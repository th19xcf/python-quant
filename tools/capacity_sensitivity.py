#!/usr/bin/env python3
"""资金规模 × 调仓频率敏感性：v1 / v3 在 1 亿、10 亿、50 亿下的容量表现（只读）。

口径与 v3 冻结脚本一致，只改初始资金：v1 = 全市场等权；v3 = 剔 60 日波动率
最高 20% + 剔上市不满 250 交易日；各跑月频与半年频，共 12 次回测。同起点
全区间 2010-01-04~2026-11-13，预热口径不变（2009-08 起步、2009-09 首个信号）。

每档在年化/回撤/换手/成本占比之外，补三项容量指标：

- 冲击成本占毛收益比例：逐笔按引擎同一公式
  ``amount × 0.015 × sqrt(amount / adv20)`` 复算（adv20 = amount 前移 1 日
  的 20 日均额，与 execution_core 同源），分母取 seg_metrics 的毛收益；
- 成交笔数与单笔平均成交额（买卖双边合计，限指标窗口）；
- 因资金不足失效的买单笔数（``expired_orders`` 的 reason='insufficient_cash'），
  另报池现金缩放笔数与 adv_gate 等风控拒单分布作容量旁证。

用法
----
    .venv\\Scripts\\python.exe tools\\capacity_sensitivity.py
"""
from __future__ import annotations

import gc
import json
import math
import sys
import time
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
for _p in (ROOT, ROOT / 'tools'):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import factor_exclusion_combo as x
import factor_exclusion_list as e
import factor_lowvol_checks as c
import factor_lowvol_quantile as f
import factor_lowvol_variants as v
from loguru import logger

from src.backtest.engine.input_contract import BAR_CONTRACT
from src.backtest.engine.portfolio_backtest_engine import PortfolioBacktestEngine
from src.risk import NoSuspendedTrade, PriceLimitTradable, RiskEngine

WARMUP_START = '2009-08-01'
FIRST_SIGNAL = date(2009, 9, 1)
WIN = c.SEG1[0], c.SEG2[1]
CAPITALS: List[Tuple[str, float]] = [('1e8', 1e8), ('1e9', 1e9), ('5e9', 5e9)]
FREQS: Dict[str, List[int]] = {'monthly': list(range(1, 13)), 'semi': [6, 12]}
IMPACT_COEF = 0.015
OUT = ROOT / '_staging/成本与容量敏感性_2026-11-14.json'


def build_adv_cache(
        data: Dict[str, pl.DataFrame],
) -> Dict[str, Tuple[np.ndarray, np.ndarray]]:
    cache: Dict[str, Tuple[np.ndarray, np.ndarray]] = {}
    for code, df in data.items():
        if 'amount' not in df.columns:
            continue
        adv = np.asarray(
            df.select(pl.col('amount').cast(pl.Float64, strict=False)
                      .shift(1)
                      .rolling_mean(window_size=20, min_samples=1))
            .to_series().to_list(), dtype=float)
        cache[code] = (df['date'].to_numpy(), adv)
    logger.info('adv20 缓存：{} 只', len(cache))
    return cache


def run_cell(data: Dict[str, pl.DataFrame], strategies: Dict[str, Any],
             capital: float, dividends: Any, st_status: Any) -> Dict[str, Any]:
    used = {code: data[code] for code in strategies if code in data}
    engine = PortfolioBacktestEngine(used)
    engine.input_contract = BAR_CONTRACT
    engine.price_basis = 'auto'
    engine.risk_engine = RiskEngine([PriceLimitTradable(), NoSuspendedTrade()])
    engine.risk_gate_policy = 'adv_gate'
    engine.set_strategies(strategies)
    engine.set_cost_model(f.base.FlatCostModel(10.0, impact_coef=IMPACT_COEF))
    t0 = time.perf_counter()
    result = engine.run_backtest(
        initial_capital=capital, lot_size=100, industry_map={},
        st_status=st_status, dividends=dividends, pooled_cash=True)
    logger.info('回测完成：资金 {:.3g} / {} 只 / {:.1f}s',
                capital, len(used), time.perf_counter() - t0)
    return result


def window_trades(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [t for t in result['trades']
            if (d := f.base.to_date(t['date'])) is not None and WIN[0] <= d <= WIN[1]]


def impact_cost(cache: Dict[str, Tuple[np.ndarray, np.ndarray]],
                trades: List[Dict[str, Any]]) -> Tuple[float, int]:
    total, missing = 0.0, 0
    for t in trades:
        day = f.base.to_date(t['date'])
        hit = cache.get(t['stock_code'])
        if day is None or hit is None:
            missing += 1
            continue
        days, adv = hit
        i = int(np.searchsorted(days, np.datetime64(day), side='right')) - 1
        a = float(adv[i]) if 0 <= i < adv.size else float('nan')
        amount = float(t['price']) * float(t['shares'])
        if not math.isfinite(a) or a <= 0:
            missing += 1
            continue
        total += amount * IMPACT_COEF * math.sqrt(amount / a)
    return total, missing


def first_equity(result: Dict[str, Any]) -> float:
    for d, eq in f.curve_of(result):
        if d >= WIN[0]:
            return eq
    return float(result['initial_capital'])


def cell_metrics(cache: Dict[str, Tuple[np.ndarray, np.ndarray]],
                 result: Dict[str, Any], capital: float) -> Dict[str, Any]:
    base_m = c.seg_metrics(result, *WIN)
    trades = window_trades(result)
    notional = sum(float(t['price']) * float(t['shares']) for t in trades)
    cost = sum(float(t.get('cost') or 0.0) for t in trades)
    gross = base_m['final_equity'] - first_equity(result) + cost
    impact, impact_missing = impact_cost(cache, trades)
    expired = result.get('expired_reason_counts') or {}
    rejects = Counter()
    for r in result.get('rejected_orders') or []:
        day = f.base.to_date(r.get('date'))
        if day is None or not (WIN[0] <= day <= WIN[1]):
            continue
        rejects[str(r.get('rule') or 'unknown')] += 1
    return {
        'capital': capital,
        'annual': base_m['annual'],
        'max_drawdown': base_m['max_drawdown'],
        'turnover_two_way': base_m['turnover_two_way'],
        'cost_share_of_gross': base_m['cost_share_of_gross'],
        'cost_bps': base_m['cost_bps'],
        'impact_share_of_gross': round(impact / gross, 4) if gross > 0 else None,
        'impact_bps_of_turnover': (round(impact / notional * 1e4, 2)
                                   if notional else None),
        'impact_missing_trades': impact_missing,
        'trades': len(trades),
        'avg_trade_notional': round(notional / len(trades), 0) if trades else 0,
        'insufficient_cash_buys': int(expired.get('insufficient_cash', 0)),
        'scaled_buy_orders': int(result.get('scaled_buy_orders') or 0),
        'avg_buy_scale': round(float(result.get('avg_buy_scale') or 1.0), 4),
        'risk_rejects': dict(sorted(rejects.items(), key=lambda kv: -kv[1])),
    }


def build_paths(quoted: Dict[date, List[str]], members: Dict[date, Any],
                data: Dict[str, pl.DataFrame], list_dates: Dict[str, date],
                all_days: List[date], months: List[date],
                portfolio: str) -> Dict[str, Dict[date, List[str]]]:
    out: Dict[str, Dict[date, List[str]]] = {}
    for freq, mons in FREQS.items():
        days = [m for m in months if m.month in mons]
        if portfolio == 'v1':
            out[freq] = {m: sorted(quoted[m]) for m in days}
            continue
        path: Dict[date, List[str]] = {}
        for m in days:
            q = set(quoted[m])
            vol = set(members[m][4] or ()) & q
            new = x.new_listing_set(data, sorted(q), list_dates, all_days, m)
            path[m] = sorted(q - vol - new)
        out[freq] = path
    return out


def main() -> None:
    t0 = time.perf_counter()
    data = f.base.load_universe(WARMUP_START, f.base.END)
    data, _ = f.base.drop_contract_violations(data)
    _calendar, month_ends, prev_day = f.build_calendar(data)
    days_all = [d for d in month_ends if d >= FIRST_SIGNAL]
    panel = f.compute_vol_panel(data, days_all)
    members, _short = f.quantile_members(panel)
    quoted = v.quoted_sets(data, days_all)
    months = [m for m in days_all if m in members and m in quoted]
    list_dates = e.load_list_dates(data)
    all_days = e.load_trading_days()
    dividends = f._load_local_dividends(list(data), None, None)
    st_status = f._load_st_status(None, data, None, WARMUP_START,
                                  f.base.END, None, None)
    adv_cache = build_adv_cache(data)

    payload: Dict[str, Any] = {'窗口': [WIN[0].isoformat(), WIN[1].isoformat()],
                               '单元格口径': '组合|频率|资金档', '明细': {}}
    for portfolio in ('v1', 'v3'):
        paths = build_paths(quoted, members, data, list_dates, all_days,
                            months, portfolio)
        for freq, path in paths.items():
            sched = v.schedules_from_path(path, prev_day)
            strategies = {code: f.base.MonthlyTargetWeightStrategy(sig)
                          for code, sig in sched.items() if code in data and sig}
            for tag, cap in CAPITALS:
                result = run_cell(data, strategies, cap, dividends, st_status)
                m = cell_metrics(adv_cache, result, cap)
                payload['明细'][f'{portfolio}|{freq}|{tag}'] = m
                print('CELL ' + json.dumps(
                    {'portfolio': portfolio, 'freq': freq, 'tag': tag, **m},
                    ensure_ascii=False), flush=True)
                del result
                gc.collect()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=1),
                   encoding='utf-8')
    print(f'ELAPSED {time.perf_counter() - t0:.1f}s', flush=True)


if __name__ == '__main__':
    main()
