#!/usr/bin/env python3
"""价量估值基线 v1：全市场 A 股等权、月度再平衡、T+1 开盘、三档滑点对照。

口径
----
- 标的：本地列存全部 A 股（沪深主板/创业板/科创板/北交所），剔除停更/退市（PIT）。
- 复权：``read_daily`` 走 ``get_stock_batch(adjustment_type='hfq')``，即**后复权**，
  全区间一致；成交与盯市使用引擎既有 ``raw_open`` / ``raw_close`` 原始价，
  分红由引擎按本地分红表计入现金。
- 信号：每月最后一个交易日收盘生成目标权重，次日（T+1）开盘成交。
- 权重：当期有行情且 close>0 的标的等权（1/n），组合级共用现金池
  （``pooled_cash=True``），持仓市值高于目标的部分在开盘减仓、低于目标的加仓。
- 约束：涨停不可买、跌停不可卖、停牌不可交易（PriceLimitTradable +
  NoSuspendedTrade + 撮合内核同日涨跌停/零成交判定），不加回撤熔断、
  行业上限、参与率闸门等额外约束。
- 成本：佣金 2.5bp 双边（最低 5 元）、印花税 0.1% 卖出、滑点按档位固定、
  冲击成本 ``0.015 × sqrt(成交额/20日均额)``（引擎既有 adv_amount 接线）。
  本口径**不含过户费**（用户指定三项成本），见报告说明。

用法
----
    .venv\\Scripts\\python.exe tools\\baseline_pv_val_equal_weight.py --slippage 10
    .venv\\Scripts\\python.exe tools\\baseline_pv_val_equal_weight.py --slippage 0 20
"""
from __future__ import annotations

import argparse
import copy
import glob
import json
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from loguru import logger

from src.backtest.costs.cost_model import CostModel
from src.backtest.engine.input_contract import (
    BAR_CONTRACT,
    BacktestInputContractError,
    validate_universe,
)
from src.backtest.engine.portfolio_backtest_engine import PortfolioBacktestEngine
from src.backtest.strategies.base_strategy import BaseStrategy
from src.core.usecases.run_backtest import (
    _load_local_dividends,
    _load_st_status,
)
from src.data.managers.data_fetcher import DataFetcher
from src.risk import NoSuspendedTrade, PriceLimitTradable, RiskEngine
from src.utils.config import Config

START = '2010-01-01'
END = '2026-11-13'
INITIAL_CAPITAL = 1e9
COMMISSION_RATE = 0.00025
MIN_COMMISSION = 5.0
REGIME_POINTS = ('2015-06', '2016-01', '2020-02')


class FlatCostModel(CostModel):
    """指定口径成本模型：佣金/印花税/滑点均为固定费率，保留引擎冲击项接线。

    与 ``CostModel`` 的差异只有两处显式口径：固定滑点（引擎默认按市值
    15/20/30bp 分档）与固定千 1 卖出印花税（引擎按历史时间表，2023-08-28
    起为 0.05%）。过户费不覆盖，沿用引擎默认（2015-08 起 0.002%，
    此前沪市 0.06%）。
    """

    def __init__(self, slippage_bps: float, impact_coef: float = 0.015,
                 commission_rate: float = 0.00025,
                 stamp_tax_rate: float = 0.001):
        super().__init__({
            'commission_rate': commission_rate,
            'min_commission': 5.0,
            'stamp_tax_rate': stamp_tax_rate,
            'slippage': slippage_bps / 10000.0,
            'impact_coef': impact_coef,
        })

    def slippage_rate(self, ts_code: str = '', adv20: float | None = None,
                      total_mv_yi: float | None = None,
                      category: str | None = None) -> float:
        return self.params['slippage']

    def _stamp_tax_rate(self, trade_date: Any, side: str, ts_code: str = '') -> float:
        if side != 'sell' or self._is_fund(ts_code):
            return 0.0
        return self.params['stamp_tax_rate']


class MonthlyTargetWeightStrategy(BaseStrategy):
    """按「信号日 → (动作, 组合级目标权重)」表输出信号；无逐 bar 状态。

    池模式下 ``target_weight`` 的权益基数是组合总权益（``AccountBook.
    total_equity``），因此这里的权重就是「该票占整个组合的比例」。
    """

    def __init__(self, signals: Dict[date, tuple[str, float]],
                 date_col: str = 'date'):
        super().__init__('ew_monthly_target')
        self.signals = signals
        self.date_col = date_col

    def __deepcopy__(self, memo: Dict[int, Any]) -> 'MonthlyTargetWeightStrategy':
        # 策略无逐 bar 状态，调度表只读，分段回测共享同一份即可
        return self

    def generate_signal(self, row: Any, index: int) -> Any:
        return None

    def generate_signals_vectorized(self, df: pl.DataFrame) -> pl.Series:
        if self.date_col not in df.columns:
            return pl.Series('signal', [None] * df.height, dtype=pl.Object)
        signals = self.signals
        out = []
        for value in df[self.date_col].to_list():
            item = signals.get(value)
            out.append(
                None if item is None
                else {'action': item[0], 'target_weight': item[1]})
        return pl.Series('signal', out, dtype=pl.Object)


def load_universe(start: str, end: str) -> Dict[str, pl.DataFrame]:
    files = glob.glob(str(ROOT / 'data/local_store/quotes/stock/**/*.parquet'),
                      recursive=True)
    codes = sorted({Path(p).stem for p in files})
    logger.info('本地列存 A 股 {} 只，开始读取 {}~{}（后复权）', len(codes), start, end)
    t0 = time.perf_counter()
    batch = DataFetcher(Config(), db_manager=None).get_stock_batch(
        codes, start, end, '1d', 'hfq')
    data = {c: df for c, df in batch.items() if df is not None and not df.is_empty()}
    logger.info('读取完成：{} 只 / {} 行 / {:.1f}s', len(data),
                sum(df.height for df in data.values()), time.perf_counter() - t0)
    return data


def drop_contract_violations(
        data: Dict[str, pl.DataFrame]) -> tuple[Dict[str, pl.DataFrame], List[Dict[str, Any]]]:
    """用引擎自己的行情契约逐标的预筛，剔除会让整场回测直接抛错的标的。

    已知场景：个券复权因子恒为 1.0（hfq 与 raw 逐值相等），引擎按
    ``price_basis='auto'`` 拒绝这种「伪复权」面板。这里不改契约、不改引擎，
    只把坏标的从标的池剔除并如实记录（数量 + 代码 + 行数 + 原因），
    由调用方在结论里作为数据异常上报。
    """
    kept: Dict[str, pl.DataFrame] = {}
    violations: List[Dict[str, Any]] = []
    for code, df in data.items():
        try:
            validate_universe({code: df})
        except BacktestInputContractError as exc:
            violations.append({
                'code': code,
                'rows': df.height,
                'start': str(df['date'].min())[:10],
                'end': str(df['date'].max())[:10],
                'reason': str(exc).replace(code, '').strip()[:120],
            })
            continue
        kept[code] = df
    if violations:
        logger.warning('契约预筛剔除 {} 只：{}', len(violations),
                       [v['code'] for v in violations[:20]])
    return kept, violations


def build_schedule(data: Dict[str, pl.DataFrame],
                   cash_buffer: float = 0.01) -> Dict[date, tuple[str, float]]:
    """构建「信号日 → (动作, 组合级等权目标)」两腿调度表。

    - 卖出腿：月末前一交易日收盘发 sell，T+1 开盘（即月末开盘）减到目标，
      先把超配部分换成现金；
    - 买入腿：月末收盘发 buy，T+1 开盘补到目标，用上一步释放的现金建仓。
    两腿各自满足「T 日收盘信号、T+1 开盘成交」；之所以拆两腿，是因为引擎
    每个标的每根 bar 只接受一个信号，同一开盘无法既减仓又加仓。

    ``cash_buffer`` 预留现金比例：满仓 1/n 时买入成本会让最后一笔（按代码序）
    资金不足被截断，形成按代码序的系统性欠配；预留 1% 后权重合计 0.99，
    现金缓冲只承担成本与整手取整，不改变等权关系。
    """
    all_dates = pl.concat([df.select('date') for df in data.values()])
    calendar: List[date] = all_dates.unique().sort('date')['date'].to_list()
    if not calendar:
        raise RuntimeError('行情日历为空')
    months: Dict[tuple[int, int], date] = {}
    for day in calendar:
        months[(day.year, day.month)] = day  # 同月后者覆盖前者 → 月末
    counts = dict(all_dates.group_by('date').len().iter_rows())
    budget = 1.0 - cash_buffer
    prev_day = {day: calendar[i - 1] for i, day in enumerate(calendar) if i}

    signals: Dict[date, tuple[str, float]] = {
        calendar[0]: ('buy', budget / counts[calendar[0]])}
    for month_end in sorted(months.values()):
        signals[month_end] = ('buy', budget / counts.get(month_end, 1))
        sell_day = prev_day.get(month_end)
        if sell_day is not None:
            signals[sell_day] = ('sell', budget / counts.get(sell_day, 1))
    return signals


def metrics(result: Dict[str, Any]) -> Dict[str, Any]:
    curve = [(to_date(r['date']), float(r['equity'])) for r in result['equity_curve']]
    curve = [(d, e) for d, e in curve if d is not None]
    if len(curve) < 2:
        raise RuntimeError('净值曲线为空')
    start_day, start_eq = curve[0]
    end_day, end_eq = curve[-1]
    years = (end_day - start_day).days / 365.25
    annual = (end_eq / start_eq) ** (1.0 / years) - 1.0 if years > 0 else 0.0

    peak, max_dd, dd_day = start_eq, 0.0, start_day
    for day, eq in curve:
        if eq > peak:
            peak = eq
        dd = eq / peak - 1.0
        if dd < max_dd:
            max_dd, dd_day = dd, day

    trades = result.get('trades') or []
    buy_notional = sum(t['price'] * t['shares'] for t in trades if t.get('signal') == 'buy')
    sell_notional = sum(t['price'] * t['shares'] for t in trades if t.get('signal') == 'sell')
    total_cost = sum(float(t.get('cost') or 0.0) for t in trades)
    avg_equity = sum(eq for _, eq in curve) / len(curve)
    notional = buy_notional + sell_notional

    gross_pnl = end_eq - start_eq + total_cost
    commission_est = sum(
        max(t['price'] * t['shares'] * COMMISSION_RATE, MIN_COMMISSION)
        for t in trades)
    return {
        'start': start_day.isoformat(), 'end': end_day.isoformat(),
        'years': round(years, 2),
        'final_equity': round(end_eq, 2),
        'annual': annual,
        'max_drawdown': max_dd,
        'max_dd_date': dd_day.isoformat(),
        'trades': len(trades),
        'buy_notional': buy_notional,
        'sell_notional': sell_notional,
        'annual_turnover_two_way': notional / avg_equity / years if years > 0 else 0.0,
        'annual_turnover_one_way': buy_notional / avg_equity / years if years > 0 else 0.0,
        'cost_total': total_cost,
        'cost_bps_of_turnover': total_cost / notional * 1e4 if notional else 0.0,
        'cost_drag_annual': total_cost / avg_equity / years if years > 0 else 0.0,
        'cost_share_of_gross': total_cost / gross_pnl if gross_pnl > 0 else None,
        'commission_est': commission_est,
        'commission_share_of_cost': commission_est / total_cost if total_cost else None,
        'gross_pnl': gross_pnl,
        'avg_equity': avg_equity,
        'expired_orders': len(result.get('expired_orders') or []),
        'rejected_orders': len(result.get('rejected_orders') or []),
        'blocked_orders': len(result.get('blocked_orders') or []),
        'fill_confidence': result.get('fill_confidence'),
        'data_quality': {
            'degraded': (result.get('data_quality') or {}).get('degraded'),
            'universe_filter': (result.get('data_quality') or {}).get('universe_filter'),
        },
    }


def to_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def regime_points(result: Dict[str, Any]) -> Dict[str, Any]:
    curve = [(to_date(r['date']), float(r['equity'])) for r in result['equity_curve']]
    curve = [(d, e) for d, e in curve if d is not None]
    out: Dict[str, Any] = {}
    peak = curve[0][1]
    peak_by_day: List[tuple[date, float]] = []
    for day, eq in curve:
        peak = max(peak, eq)
        peak_by_day.append((day, peak))
    for point in REGIME_POINTS:
        year, month = (int(part) for part in point.split('-'))
        rows = [(d, e) for d, e in curve if (d.year, d.month) == (year, month)]
        if not rows:
            out[point] = {'missing': True}
            continue
        month_ret = rows[-1][1] / rows[0][1] - 1.0
        peak_then = next(p for d, p in peak_by_day if d == rows[-1][0])
        out[point] = {
            'month_return': round(month_ret, 4),
            'dd_from_peak_at_month_end': round(rows[-1][1] / peak_then - 1.0, 4),
            'equity_end': round(rows[-1][1], 2),
        }
    return out


def expiring_breakdown(result: Dict[str, Any]) -> Dict[str, int]:
    counter: Dict[str, int] = {}
    for item in result.get('expired_orders') or []:
        reason = str(item.get('reason') or 'unknown')
        counter[reason] = counter.get(reason, 0) + 1
    return dict(sorted(counter.items(), key=lambda kv: -kv[1])[:12])


def expiring_by_year(result: Dict[str, Any]) -> Dict[str, Dict[str, int]]:
    """作废单按年 × 原因计数：判断「现金不足/不可交易」是集中在股灾月份
    还是全区间常态，是口径异常归因的必要证据（不做样本抽取）。"""
    out: Dict[str, Dict[str, int]] = {}
    for item in result.get('expired_orders') or []:
        day = to_date(item.get('date'))
        if day is None:
            continue
        bucket = out.setdefault(str(day.year), {})
        reason = str(item.get('reason') or 'unknown')
        bucket[reason] = bucket.get(reason, 0) + 1
    return dict(sorted(out.items()))


def run_one(data: Dict[str, pl.DataFrame], strategy: BaseStrategy,
            slippage_bps: float, impact_coef: float = 0.015,
            dividends: Any = None,
            st_status: Any = None) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """直接构造组合引擎：门面不透传 cost_model / pooled_cash，脚本按引擎
    真实接口接入（不改引擎、不改成本默认值）。"""
    engine = PortfolioBacktestEngine(data)
    engine.input_contract = BAR_CONTRACT
    engine.price_basis = 'auto'
    engine.risk_engine = RiskEngine([PriceLimitTradable(), NoSuspendedTrade()])
    engine.risk_gate_policy = 'adv_gate'
    engine.set_strategies({code: strategy for code in data})
    engine.set_cost_model(FlatCostModel(slippage_bps, impact_coef=impact_coef))
    t0 = time.perf_counter()
    result = engine.run_backtest(
        initial_capital=INITIAL_CAPITAL,
        lot_size=100,
        industry_map={},
        st_status=st_status,
        dividends=dividends,
        pooled_cash=True,
    )
    elapsed = time.perf_counter() - t0
    summary = metrics(result)
    summary['slippage_bps'] = slippage_bps
    summary['impact_coef'] = impact_coef
    summary['engine_seconds'] = round(elapsed, 1)
    summary['expired_breakdown'] = expiring_breakdown(result)
    summary['expired_by_year'] = expiring_by_year(result)
    summary['scaled_buy_orders'] = result.get('scaled_buy_orders', 0)
    summary['avg_buy_scale'] = result.get('avg_buy_scale')
    return result, summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--slippage', nargs='+', type=float, default=[10.0],
                        help='滑点档位（bp），可传多个，如 --slippage 0 10 20')
    parser.add_argument('--impact-coef', type=float, default=0.015)
    parser.add_argument('--impact-report', action='store_true',
                        help='对 10bp 档额外跑一次 impact_coef=0，量化冲击成本贡献')
    parser.add_argument('--start', default=START)
    parser.add_argument('--end', default=END)
    parser.add_argument('--save-equity', default=None,
                        help='把 10bp 档净值序列写到该 CSV 路径')
    args = parser.parse_args()

    data = load_universe(args.start, args.end)
    data, violations = drop_contract_violations(data)
    schedule = build_schedule(data)
    dividends = _load_local_dividends(list(data), None, None)
    st_status = _load_st_status(None, data, None, args.start, args.end, None, None)
    logger.info('信号日 {} 个（含月末买腿与前一交易日卖腿），分红 {} 只，ST 区间 {} 只',
                len(schedule), len(dividends), len(st_status or {}))
    strategy = MonthlyTargetWeightStrategy(schedule)

    summaries = []
    for slip in args.slippage:
        result, summary = run_one(
            data, strategy, slip, args.impact_coef, dividends, st_status)
        summary['regime'] = regime_points(result)
        summary['universe'] = {'used': len(data), 'excluded': len(violations)}
        summaries.append(summary)
        if args.save_equity and abs(slip - 10.0) < 1e-9:
            path = Path(args.save_equity)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open('w', encoding='utf-8', newline='') as fh:
                fh.write('date,equity\n')
                for row in result['equity_curve']:
                    fh.write(f"{to_date(row['date']).isoformat()},"
                             f"{row['equity']:.2f}\n")
            logger.info('净值序列已写 {}', path)
        print('RESULT ' + json.dumps(summary, ensure_ascii=False, default=str),
              flush=True)
        if args.impact_report and abs(slip - 10.0) < 1e-9:
            _, no_impact = run_one(
                data, strategy, slip, impact_coef=0.0,
                dividends=dividends, st_status=st_status)
            impact_cost = summary['cost_total'] - no_impact['cost_total']
            notional = summary['buy_notional'] + summary['sell_notional']
            impact = {
                'slippage_bps': slip,
                'impact_cost': impact_cost,
                'impact_bps_of_turnover': impact_cost / notional * 1e4 if notional else 0.0,
                'impact_drag_annual': impact_cost / summary['avg_equity'] / summary['years'],
                'annual_without_impact': no_impact['annual'],
            }
            print('IMPACT ' + json.dumps(impact, ensure_ascii=False), flush=True)

    print('SUMMARY ' + json.dumps(summaries, ensure_ascii=False, default=str),
          flush=True)
    if violations:
        print('EXCLUDED ' + json.dumps(violations[:200], ensure_ascii=False),
              flush=True)


if __name__ == '__main__':
    main()
