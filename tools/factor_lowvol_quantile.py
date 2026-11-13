#!/usr/bin/env python3
"""单因子分层：60 日波动率（低波因子）五分位，口径复用已冻结基线。

与 ``tools/baseline_pv_val_equal_weight.py`` 的唯一差异是「选股层」：
基线全市场等权，这里按 T 日收盘的 60 日波动率横截面排序分五档，
每档内部等权。其余全部沿用基线口径：

- 全市场 A 股，月度再平衡，2010-01-01~2026-11-13，本金 10 亿；
- T 日收盘出信号、T+1 开盘成交；涨停不可买、跌停不可卖、停牌不参与调仓
  （PriceLimitTradable + NoSuspendedTrade + 引擎撮合判定）；
- 成本：佣金万 2.5 双边（最低 5 元）、卖出印花税固定千 1、滑点 10bp、
  过户费引擎默认、冲击成本 0.015×sqrt(参与率)（引擎既有 adv_amount 接线）；
- hfq 出信号（因子也用 hfq 收盘算收益），原始价成交与盯市，分红入池；
- 组合级共享现金池，权重合计 0.99（与基线一致的 1% 现金缓冲）。

因子口径
--------
``vol = std(log(close_t / close_{t-1}), 最近 60 个交易日)``，窗口末端必须
恰好是信号日 T，且窗口内 60 个收益全部有效（不用部分窗口、不用未来数据）。
当月横截面按 vol 升序排序后按名次均分五档：Q1 最低波 … Q5 最高波。

两腿再平衡与基线一致：月末前一交易日收盘发卖腿（超配减到目标、退出票清仓），
月末收盘发买腿（次日开盘补到目标）。档位目标权重 = 0.99 / 该档成分数。

用法
----
    .venv\\Scripts\\python.exe tools\\factor_lowvol_quantile.py
"""
from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
for _p in (ROOT, ROOT / 'tools'):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from loguru import logger

import baseline_pv_val_equal_weight as base
from src.backtest.engine.input_contract import BAR_CONTRACT
from src.backtest.engine.portfolio_backtest_engine import PortfolioBacktestEngine
from src.core.usecases.run_backtest import _load_local_dividends, _load_st_status
from src.risk import NoSuspendedTrade, PriceLimitTradable, RiskEngine

VOL_WINDOW = 60
CASH_BUFFER = 0.01
QUANTILES = 5
MIN_NAMES = 50
SLIPPAGE_BPS = 10.0
IMPACT_COEF = 0.015


def build_calendar(data: Dict[str, pl.DataFrame]) -> Tuple[List[date], List[date], Dict[date, date]]:
    all_dates = pl.concat([df.select('date') for df in data.values()])
    calendar: List[date] = all_dates.unique().sort('date')['date'].to_list()
    months: Dict[Tuple[int, int], date] = {}
    for day in calendar:
        months[(day.year, day.month)] = day  # 同月后者覆盖前者 → 月末
    month_ends = sorted(months.values())
    prev_day = {day: calendar[i - 1] for i, day in enumerate(calendar) if i}
    return calendar, month_ends, prev_day


def compute_vol_panel(data: Dict[str, pl.DataFrame],
                      month_ends: List[date]) -> Dict[date, Dict[str, float]]:
    """每只票在每个月末的 60 日波动率（只用 T 日及之前数据）。"""
    me = np.array(month_ends, dtype='datetime64[D]')
    panel: Dict[date, Dict[str, float]] = {d: {} for d in month_ends}
    skipped = 0
    for code, df in data.items():
        dates = df['date'].to_numpy()
        close = df['close'].to_numpy().astype('float64')
        m = close.size
        if m <= VOL_WINDOW:
            skipped += 1
            continue
        with np.errstate(divide='ignore', invalid='ignore'):
            ret = np.diff(np.log(np.where(close > 0, close, np.nan)))
        bad = ~np.isfinite(ret)
        r = np.nan_to_num(ret, nan=0.0, posinf=0.0, neginf=0.0)
        cs1 = np.concatenate(([0.0], np.cumsum(r)))
        cs2 = np.concatenate(([0.0], np.cumsum(r * r)))
        csb = np.concatenate(([0], np.cumsum(bad)))
        idx = np.minimum(np.searchsorted(dates, me), m - 1)
        hit = np.nonzero((np.searchsorted(dates, me) < m) & (dates[idx] == me))[0]
        for j in hit:
            k = int(idx[j])
            a = k - VOL_WINDOW
            if a < 0 or csb[k] - csb[a] != 0:
                continue
            n = VOL_WINDOW
            mean = (cs1[k] - cs1[a]) / n
            var = (cs2[k] - cs2[a] - n * mean * mean) / (n - 1)
            if var <= 0:
                continue
            panel[month_ends[j]][code] = float(np.sqrt(var))
    logger.info('波动率面板：{} 个交易日末，跳过 {} 只（行数不足 {}）',
                len(panel), skipped, VOL_WINDOW)
    return panel


def quantile_members(panel: Dict[date, Dict[str, float]],
                     nq: int = QUANTILES) -> Tuple[Dict[date, List[List[str]]], List[Dict[str, Any]]]:
    members: Dict[date, List[List[str]]] = {}
    short: List[Dict[str, Any]] = []
    for day, vals in panel.items():
        items = sorted(vals.items(), key=lambda kv: kv[1])
        n = len(items)
        if n < MIN_NAMES:
            short.append({'date': day.isoformat(), 'names': n})
            continue
        buckets: List[List[str]] = [[] for _ in range(nq)]
        for rank, (code, _) in enumerate(items):
            buckets[min(nq - 1, rank * nq // n)].append(code)
        members[day] = buckets
    if short:
        logger.warning('横截面不足 {} 只而跳过的月末：{}', MIN_NAMES, short)
    return members, short


def build_quantile_signals(members: Dict[date, List[List[str]]],
                           prev_day: Dict[date, date],
                           q: int) -> Dict[str, Dict[date, Tuple[str, float]]]:
    schedules: Dict[str, Dict[date, Tuple[str, float]]] = {}
    prev: set[str] = set()
    for m in sorted(members):
        codes = members[m][q]
        if not codes:
            continue
        w = (1.0 - CASH_BUFFER) / len(codes)
        sell_day = prev_day.get(m)
        cur = set(codes)
        for code in codes:
            sig = schedules.setdefault(code, {})
            if sell_day is not None:
                sig[sell_day] = ('sell', w)
            sig[m] = ('buy', w)
        for code in prev - cur:
            if sell_day is not None:
                schedules.setdefault(code, {})[sell_day] = ('sell', 0.0)
        prev = cur
    return schedules


def run_portfolio(data: Dict[str, pl.DataFrame],
                  schedules: Dict[str, Dict[date, Tuple[str, float]]],
                  dividends: Any, st_status: Any) -> Dict[str, Any]:
    used = {code: data[code] for code in schedules if code in data}
    strategies = {code: base.MonthlyTargetWeightStrategy(sig)
                  for code, sig in schedules.items() if code in used and sig}
    used = {code: df for code, df in used.items() if code in strategies}
    engine = PortfolioBacktestEngine(used)
    engine.input_contract = BAR_CONTRACT
    engine.price_basis = 'auto'
    engine.risk_engine = RiskEngine([PriceLimitTradable(), NoSuspendedTrade()])
    engine.risk_gate_policy = 'adv_gate'
    engine.set_strategies(strategies)
    engine.set_cost_model(base.FlatCostModel(SLIPPAGE_BPS, impact_coef=IMPACT_COEF))
    t0 = time.perf_counter()
    result = engine.run_backtest(
        initial_capital=base.INITIAL_CAPITAL,
        lot_size=100,
        industry_map={},
        st_status=st_status,
        dividends=dividends,
        pooled_cash=True,
    )
    elapsed = time.perf_counter() - t0
    logger.info('回测完成：{} 只 / {:.1f}s', len(used), elapsed)
    return result


def curve_of(result: Dict[str, Any]) -> List[Tuple[date, float]]:
    curve = [(base.to_date(r['date']), float(r['equity']))
             for r in result['equity_curve']]
    return [(d, e) for d, e in curve if d is not None]


def annual_between(curve: List[Tuple[date, float]], start_day: date) -> Dict[str, Any]:
    rows = [(d, e) for d, e in curve if d >= start_day]
    if len(rows) < 2:
        return {'annual': None}
    (d0, e0), (d1, e1) = rows[0], rows[-1]
    years = (d1 - d0).days / 365.25
    return {
        'annual': (e1 / e0) ** (1.0 / years) - 1.0,
        'start': d0.isoformat(), 'end': d1.isoformat(),
        'years': round(years, 2), 'start_equity': e0, 'end_equity': e1,
    }


def baseline_annual(start_day: date) -> Dict[str, Any]:
    path = ROOT / '_staging/baseline_pv_val_equal_weight_equity.csv'
    if not path.exists():
        return {'missing': str(path)}
    curve: List[Tuple[date, float]] = []
    with path.open('r', encoding='utf-8') as fh:
        next(fh)
        for line in fh:
            day_s, eq_s = line.strip().split(',')
            curve.append((date.fromisoformat(day_s), float(eq_s)))
    return annual_between(curve, start_day)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', default=base.START)
    parser.add_argument('--end', default=base.END)
    args = parser.parse_args()

    t0 = time.perf_counter()
    data = base.load_universe(args.start, args.end)
    data, violations = base.drop_contract_violations(data)
    calendar, month_ends, prev_day = build_calendar(data)
    panel = compute_vol_panel(data, month_ends)
    members, short = quantile_members(panel)
    logger.info('数据 {} 只 / 剔除 {} 只 / 有效分层月末 {} 个 / 首末 {}~{}',
                len(data), len(violations), len(members),
                min(members).isoformat(), max(members).isoformat())

    dividends = _load_local_dividends(list(data), None, None)
    st_status = _load_st_status(None, data, None, args.start, args.end, None, None)

    counts = {q: [] for q in range(QUANTILES)}
    for m in sorted(members):
        for q in range(QUANTILES):
            counts[q].append(len(members[m][q]))

    out: List[Dict[str, Any]] = []
    first_trade_day: date | None = None
    for q in range(QUANTILES):
        schedules = build_quantile_signals(members, prev_day, q)
        result = run_portfolio(data, schedules, dividends, st_status)
        summary = base.metrics(result)
        curve = curve_of(result)
        # 组合首次建仓的交易日：净值脱离初始本金的第一天
        moved = [d for d, e in curve
                 if abs(e - base.INITIAL_CAPITAL) > 1e-3]
        start_day = moved[0] if moved else curve[0][0]
        first_trade_day = start_day if first_trade_day is None else min(first_trade_day, start_day)
        item = {
            'quantile': q + 1,
            'members_median': int(np.median(counts[q])),
            'members_min': int(min(counts[q])),
            'members_min_month': members and min(members),
            'annual_full_span': summary['annual'],
            'annual_matched': annual_between(curve, start_day),
            'max_drawdown': summary['max_drawdown'],
            'max_dd_date': summary['max_dd_date'],
            'annual_turnover_two_way': summary['annual_turnover_two_way'],
            'annual_turnover_one_way': summary['annual_turnover_one_way'],
            'cost_share_of_gross': summary['cost_share_of_gross'],
            'cost_bps_of_turnover': summary['cost_bps_of_turnover'],
            'final_equity': summary['final_equity'],
            'trades': summary['trades'],
            'engine_used': len(schedules),
            'expired': summary['expired_orders'],
            'rejected': summary['rejected_orders'],
        }
        out.append(item)
        print('QUANTILE ' + json.dumps(item, ensure_ascii=False, default=str), flush=True)
        del result, schedules
        gc.collect()

    base_window = baseline_annual(first_trade_day) if first_trade_day else {'annual': None}
    print('BASELINE ' + json.dumps(base_window, ensure_ascii=False, default=str), flush=True)
    print('SHORT ' + json.dumps(short[:40], ensure_ascii=False), flush=True)
    print('ELAPSED {:.1f}s'.format(time.perf_counter() - t0), flush=True)


if __name__ == '__main__':
    main()
