"""规格驱动的回测执行：成分构造 → 两腿信号 → 既有引擎 → 指标汇总。

引擎、成本口径与撮合全部复用 ``src/backtest`` 既有实现；本模块只做
「规格 → 引擎入参」的接线，不复制撮合、成本或下单腿算法。
"""
from __future__ import annotations

import time
from collections import Counter
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import polars as pl
from loguru import logger

from src.backtest.costs.cost_model import CostModel
from src.backtest.engine.input_contract import BAR_CONTRACT
from src.backtest.engine.portfolio_backtest_engine import PortfolioBacktestEngine
from src.backtest.strategies.base_strategy import BaseStrategy
from src.core.usecases.run_backtest import _load_local_dividends, _load_st_status
from src.risk import NoSuspendedTrade, PriceLimitTradable, RiskEngine

from . import compute
from .registry import CONDITIONS


class SpecCostModel(CostModel):
    """规格成本适配器：只覆盖费率选择，成本计算仍走引擎 ``CostModel``。

    与引擎默认的两处显式差异（由规格决定，不改引擎默认值）：
    - 滑点固定为规格 bps（引擎默认按市值 15/20/30bp 分档）；
    - ``stamp_tax_basis=fixed_0.1%`` 时卖出印花税固定千 1
      （引擎默认按历史时间表，2023-08-28 起 0.05%）。
    过户费不覆盖，沿用引擎默认。
    """

    def __init__(self, cost: Dict[str, Any]):
        params = {
            'commission_rate': cost['commission_rate'],
            'min_commission': cost['min_commission'],
            'slippage': cost['slippage_bps'] / 10000.0,
            'impact_coef': cost['impact_coef'],
        }
        if cost['stamp_tax_basis'] == 'fixed_0.1%':
            params['stamp_tax_rate'] = 0.001
        super().__init__(params)
        self.stamp_tax_basis = cost['stamp_tax_basis']

    def slippage_rate(self, ts_code: str = '', adv20: float | None = None,
                      total_mv_yi: float | None = None,
                      category: str | None = None) -> float:
        return self.params['slippage']

    def _stamp_tax_rate(self, trade_date: Any, side: str, ts_code: str = '') -> float:
        if self.stamp_tax_basis == 'historical':
            return super()._stamp_tax_rate(trade_date, side, ts_code)
        if side != 'sell' or self._is_fund(ts_code):
            return 0.0
        return self.params['stamp_tax_rate']


class MonthlyTargetWeightStrategy(BaseStrategy):
    """「信号日 → (动作, 组合级目标权重)」调度表策略，无逐 bar 状态。"""

    def __init__(self, signals: Dict[date, tuple[str, float]], date_col: str = 'date'):
        super().__init__('spec_monthly_target')
        self.signals = signals
        self.date_col = date_col

    def __deepcopy__(self, memo: Dict[str, Any]) -> MonthlyTargetWeightStrategy:
        return self

    def generate_signal(self, row: Any, index: int) -> Any:
        return None

    def generate_signals_vectorized(self, df: pl.DataFrame) -> pl.Series:
        if self.date_col not in df.columns:
            return pl.Series('signal', [None] * df.height, dtype=pl.Object)
        out = []
        for value in df[self.date_col].to_list():
            item = self.signals.get(value)
            out.append(None if item is None
                       else {'action': item[0], 'target_weight': item[1]})
        return pl.Series('signal', out, dtype=pl.Object)


# ---------------------------------------------------------------------------
# 成分构造
# ---------------------------------------------------------------------------

def build_path(ctx: compute.Context, spec: Dict[str, Any]
               ) -> Tuple[Dict[date, List[str]], Dict[str, int]]:
    """按规格顺序执行剔除链与因子分位，返回「调仓日 → 成分」及跳过计数。"""
    resolved = spec['_resolved']
    exclusions = resolved['exclusions']
    factor = resolved['factor']
    skip = Counter()
    path: Dict[date, List[str]] = {}

    for day in ctx.days:
        base: Set[str] = set(ctx.quoted[day])
        if ctx.allowed is not None:
            base &= ctx.allowed
        if not base:
            skip['no_quote'] += 1
            continue

        keep = set(base)
        skipped_day = False
        for item in exclusions:
            fn = CONDITIONS.get(item['name']).func
            dropped = fn(ctx, item['params'], day, base)
            if dropped is None:
                skip[f"{item['name']}:cross_section_short"] += 1
                skipped_day = True
                break
            keep -= dropped
        if skipped_day:
            continue

        if factor is not None:
            panel = ctx.factor(factor['name'], factor['params'])
            values = panel.get(day) or {}
            q = factor['quantile']
            buckets = compute.bucket_slices(values, q['buckets'], q['min_names'],
                                            q['direction'])
            if not buckets:
                skip['factor:cross_section_short'] += 1
                continue
            picked = set(buckets[q['bucket'] - 1])
            if q['action'] == 'hold_bucket':
                keep = picked & keep
            else:
                keep -= picked

        if not keep:
            skip['empty_members'] += 1
            continue
        path[day] = sorted(keep)

    sizes = sorted(len(v) for v in path.values())
    if sizes:
        logger.info('成分路径：{} 个调仓日，成分数中位 {}，最少 {} / 最多 {}',
                    len(path), sizes[len(sizes) // 2], sizes[0], sizes[-1])
    if skip:
        logger.info('跳过统计：{}', dict(skip))
    return path, dict(skip)


def build_schedules(path: Dict[date, List[str]], prev_day: Dict[date, date],
                    cash_buffer: float) -> Dict[str, Dict[date, Tuple[str, float]]]:
    """把成分路径转成两腿信号表：前一日收盘卖、调仓日收盘买（引擎次日开盘撮合）。"""
    schedules: Dict[str, Dict[date, Tuple[str, float]]] = {}
    prev: Set[str] = set()
    for day in sorted(path):
        codes = path[day]
        if not codes:
            continue
        weight = (1.0 - cash_buffer) / len(codes)
        sell_day = prev_day.get(day)
        current = set(codes)
        for code in codes:
            sig = schedules.setdefault(code, {})
            if sell_day is not None:
                sig[sell_day] = ('sell', weight)
            sig[day] = ('buy', weight)
        for code in prev - current:
            if sell_day is not None:
                schedules.setdefault(code, {})[sell_day] = ('sell', 0.0)
        prev = current
    return schedules


# ---------------------------------------------------------------------------
# 引擎调用
# ---------------------------------------------------------------------------

def run_engine(data: Dict[str, pl.DataFrame],
               schedules: Dict[str, Dict[date, Tuple[str, float]]],
               spec: Dict[str, Any], dividends: Any, st_status: Any
               ) -> Tuple[Dict[str, Any], int, float]:
    used = {code: data[code] for code in schedules if code in data}
    strategies = {code: MonthlyTargetWeightStrategy(sig)
                  for code, sig in schedules.items() if code in used and sig}
    used = {code: df for code, df in used.items() if code in strategies}
    portfolio = spec['_resolved']['portfolio']
    engine = PortfolioBacktestEngine(used)
    engine.input_contract = BAR_CONTRACT
    engine.price_basis = 'auto'
    engine.risk_engine = RiskEngine([PriceLimitTradable(), NoSuspendedTrade()])
    engine.risk_gate_policy = 'adv_gate'
    engine.set_strategies(strategies)
    engine.set_cost_model(SpecCostModel(spec['_resolved']['cost']))
    t0 = time.perf_counter()
    result = engine.run_backtest(
        initial_capital=portfolio['initial_capital'],
        lot_size=portfolio['lot_size'],
        industry_map={},
        st_status=st_status,
        dividends=dividends,
        pooled_cash=portfolio['pooled_cash'],
    )
    elapsed = time.perf_counter() - t0
    logger.info('回测完成：{} 只 / {:.1f}s', len(used), elapsed)
    return result, len(used), elapsed


# ---------------------------------------------------------------------------
# 指标
# ---------------------------------------------------------------------------

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


def performance_metrics(result: Dict[str, Any], spec: Dict[str, Any],
                        path_sizes: List[int]) -> Dict[str, Any]:
    period = spec['_resolved']['period']
    a = date.fromisoformat(period['start'])
    b = date.fromisoformat(period['end'])
    curve = [(d, float(e)) for d, e in
             ((to_date(r['date']), r['equity']) for r in result['equity_curve'])
             if d is not None and a <= d <= b]
    if len(curve) < 2:
        raise RuntimeError(f'净值曲线在 {a}~{b} 内不足两个点')
    start_day, start_eq = curve[0]
    end_day, end_eq = curve[-1]
    years = (end_day - start_day).days / 365.25
    annual = (end_eq / start_eq) ** (1.0 / years) - 1.0 if years > 0 else 0.0

    peak, max_dd, dd_day = start_eq, 0.0, start_day
    for day, eq in curve:
        peak = max(peak, eq)
        dd = eq / peak - 1.0
        if dd < max_dd:
            max_dd, dd_day = dd, day

    trades = [t for t in (result.get('trades') or [])
              if (d := to_date(t.get('date'))) is not None and a <= d <= b]
    buy = sum(t['price'] * t['shares'] for t in trades if t.get('signal') == 'buy')
    sell = sum(t['price'] * t['shares'] for t in trades if t.get('signal') == 'sell')
    cost = sum(float(t.get('cost') or 0.0) for t in trades)
    avg_eq = sum(eq for _, eq in curve) / len(curve)
    notional = buy + sell
    gross = end_eq - start_eq + cost
    commission_rate = spec['_resolved']['cost']['commission_rate']
    min_commission = spec['_resolved']['cost']['min_commission']
    commission_est = sum(max(t['price'] * t['shares'] * commission_rate, min_commission)
                         for t in trades)
    sizes = sorted(path_sizes)
    return {
        'start': start_day.isoformat(),
        'end': end_day.isoformat(),
        'years': round(years, 2),
        'final_equity': round(end_eq, 2),
        'annual': round(annual, 6),
        'max_drawdown': round(max_dd, 6),
        'max_dd_date': dd_day.isoformat(),
        'trades': len(trades),
        'buy_notional': round(buy, 2),
        'sell_notional': round(sell, 2),
        'turnover_two_way': round(notional / avg_eq / years, 4) if years > 0 else 0.0,
        'turnover_one_way': round(buy / avg_eq / years, 4) if years > 0 else 0.0,
        'cost_total': round(cost, 2),
        'cost_bps': round(cost / notional * 1e4, 2) if notional else None,
        'cost_share_of_gross': round(cost / gross, 4) if gross > 0 else None,
        'commission_est': round(commission_est, 2),
        'commission_share_of_cost': round(commission_est / cost, 4) if cost else None,
        'gross_pnl': round(gross, 2),
        'avg_equity': round(avg_eq, 2),
        'rebalances': len(path_sizes),
        'members_median': sizes[len(sizes) // 2] if sizes else 0,
        'members_min': sizes[0] if sizes else 0,
        'members_max': sizes[-1] if sizes else 0,
        'expired_orders': len(result.get('expired_orders') or []),
        'rejected_orders': len(result.get('rejected_orders') or []),
        'blocked_orders': len(result.get('blocked_orders') or []),
        'fill_confidence': result.get('fill_confidence'),
        'scaled_buy_orders': result.get('scaled_buy_orders', 0),
        'avg_buy_scale': result.get('avg_buy_scale'),
    }


# ---------------------------------------------------------------------------
# 编排
# ---------------------------------------------------------------------------

def execute(spec: Dict[str, Any], reporter: Any = None) -> Dict[str, Any]:
    """读规格 → 装数据 → 构造成分与信号 → 调引擎 → 指标与净值。"""
    t0 = time.perf_counter()
    resolved = spec['_resolved']
    period = resolved['period']
    data = compute.load_universe(period, resolved['universe'])
    data, violations = compute.drop_contract_violations(data)
    if not data:
        raise RuntimeError('行情为空：契约预筛后没有可用标的')

    _, month_ends, prev_day = compute.build_calendar(data)
    days = compute.rebalance_days(month_ends, resolved['rebalance']['frequency'],
                                  date.fromisoformat(resolved['rebalance']['first_date']))
    if not days:
        raise RuntimeError('调仓日为空：请检查 rebalance.first_date 与行情覆盖区间')
    quoted = compute.quoted_sets(data, days)
    st_status = _load_st_status(None, data, None, period['warmup_start'],
                                period['end'], None, None)
    allowed = None
    if resolved['universe']['mode'] == 'explicit':
        allowed = set(resolved['universe']['codes'])
    ctx = compute.Context(data=data, days=days, quoted=quoted,
                          st_status=st_status, allowed=allowed)
    path, skipped = build_path(ctx, spec)
    if not path:
        raise RuntimeError(f'成分路径为空；跳过统计：{skipped}')

    schedules = build_schedules(path, prev_day,
                                resolved['portfolio']['cash_buffer'])
    dividends = _load_local_dividends(list(data), None, None)
    if reporter is not None:
        from .progress import progress_scope
        with progress_scope(reporter):
            result, _used, engine_seconds = run_engine(
                data, schedules, spec, dividends, st_status)
    else:
        result, _used, engine_seconds = run_engine(
            data, schedules, spec, dividends, st_status)

    curve = [(d.isoformat(), float(e)) for d, e in
             ((to_date(r['date']), r['equity']) for r in result['equity_curve'])
             if d is not None]
    metrics = performance_metrics(result, spec, [len(v) for v in path.values()])
    timing = {
        'engine_seconds': round(engine_seconds, 1),
        'elapsed_seconds': round(time.perf_counter() - t0, 1),
    }
    return {
        'metrics': metrics,
        'timing': timing,
        'nav': curve,
        'skipped': skipped,
        'contract_violations': violations,
        'factor': resolved['factor'],
        'exclusions': resolved['exclusions'],
    }
