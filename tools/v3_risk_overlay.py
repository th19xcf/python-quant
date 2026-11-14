#!/usr/bin/env python3
"""v3 仓位层风险控制两项实验（只读，不改引擎/数据/成本默认值）。

在 v3（剔 60 日波动率最高 20% + 剔上市不满 250 交易日）的同一路径上，只在
调仓日缩放目标权重，其余口径（预热、后复权、月末信号 T+1 开盘成交、成本）
完全不变：

1. 波动率目标 20%：``scale = min(1, 0.20 / σ60)``，σ60 取 v3 自身净值过去
   60 个交易日的已实现波动率（年化，仅用 T 日及之前数据，因果）；
2. 指数 200 日均线过滤：中证全指 000985.SH 收盘跌破 200 日均线时
   ``scale = 0.5``，站上恢复 1.0（阈值 0.5 为本轮设定，写入口径）。

验收门槛（预先定死）：Calmar 提升且年化降幅 ≤1pp 才算通过，通过才谈 v4。

用法
----
    .venv\\Scripts\\python.exe tools\\v3_risk_overlay.py
"""
from __future__ import annotations

import gc
import json
import math
import sys
import time
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
CAPITAL = 1e9
VOL_WINDOW = 60
VOL_TARGET = 0.20
MA_WINDOW = 200
MA_SCALE = 0.5
V3_CSV = ROOT / '_staging/equity_v3_excl_vol_new_2026-11-14.csv'
INDEX = ROOT / 'data/local_store/quotes/index/sh/000985.SH.parquet'
OUT = ROOT / '_staging/仓位层实验_2026-11-14.json'
V3_BASE = {'annual': 0.091312, 'max_drawdown': -0.610778, 'calmar': 0.1495}


def v3_path(data, month_ends, prev_day) -> Dict[date, List[str]]:
    days = [d for d in month_ends if d >= FIRST_SIGNAL]
    panel = f.compute_vol_panel(data, days)
    members, _short = f.quantile_members(panel)
    quoted = v.quoted_sets(data, days)
    months = [m for m in days if m in members and m in quoted]
    list_dates = e.load_list_dates(data)
    all_days = e.load_trading_days()
    path: Dict[date, List[str]] = {}
    for m in months:
        q = set(quoted[m])
        vol = set(members[m][4] or ()) & q
        new = x.new_listing_set(data, sorted(q), list_dates, all_days, m)
        path[m] = sorted(q - vol - new)
    return path, prev_day


def v3_daily_returns() -> Tuple[List[date], np.ndarray]:
    df = pl.read_csv(V3_CSV)
    days = [date.fromisoformat(str(x)) for x in df['date'].to_list()]
    eq = df['equity'].to_numpy().astype(float)
    return days, eq[1:] / eq[:-1] - 1


def vol_scale(days: List[date], ret: np.ndarray,
              signal_days: List[date]) -> Dict[date, float]:
    out: Dict[date, float] = {}
    arr = np.array(days, dtype='datetime64[D]')
    for t in signal_days:
        k = int(np.searchsorted(arr, np.datetime64(t, 'D'), side='right'))
        if k < VOL_WINDOW:
            out[t] = 1.0
            continue
        sigma = float(np.std(ret[k - VOL_WINDOW:k], ddof=1) * math.sqrt(252))
        out[t] = round(min(1.0, VOL_TARGET / sigma), 4) if sigma > 0 else 1.0
    return out


def ma_scale(signal_days: List[date]) -> Dict[date, float]:
    df = pl.read_parquet(INDEX).select(['date', 'close']).sort('date')
    idx_days = np.array(df['date'].to_list(), dtype='datetime64[D]')
    close = df['close'].to_numpy().astype(float)
    out: Dict[date, float] = {}
    for t in signal_days:
        k = int(np.searchsorted(idx_days, np.datetime64(t, 'D'), side='right'))
        if k < MA_WINDOW:
            out[t] = 1.0
            continue
        out[t] = MA_SCALE if close[k - 1] < float(np.mean(close[k - MA_WINDOW:k])) else 1.0
    return out


def apply_scale(schedules: Dict[str, Dict[date, Tuple[str, float]]],
                scale: Dict[date, float]) -> Dict[str, Dict[date, Tuple[str, float]]]:
    out: Dict[str, Dict[date, Tuple[str, float]]] = {}
    for code, sig in schedules.items():
        new_sig: Dict[date, Tuple[str, float]] = {}
        for day, (act, w) in sig.items():
            new_sig[day] = (act, w * scale.get(day, 1.0) if act == 'buy' else w)
        out[code] = new_sig
    return out


def run(data, strategies, st_status, dividends,
        capital: float = CAPITAL) -> Dict[str, Any]:
    used = {code: data[code] for code in strategies if code in data}
    engine = PortfolioBacktestEngine(used)
    engine.input_contract = BAR_CONTRACT
    engine.price_basis = 'auto'
    engine.risk_engine = RiskEngine([PriceLimitTradable(), NoSuspendedTrade()])
    engine.risk_gate_policy = 'adv_gate'
    engine.set_strategies(strategies)
    engine.set_cost_model(f.base.FlatCostModel(10.0, impact_coef=0.015))
    return engine.run_backtest(
        initial_capital=capital, lot_size=100, industry_map={},
        st_status=st_status, dividends=dividends, pooled_cash=True)


def curve_metrics(result: Dict[str, Any]) -> Dict[str, Any]:
    curve = [(d, e) for d, e in f.curve_of(result) if WIN[0] <= d <= WIN[1]]
    days = [d for d, _ in curve]
    eq = np.array([e for _, e in curve], dtype=float)
    ret = eq[1:] / eq[:-1] - 1
    years = (days[-1] - days[0]).days / 365.25
    annual = float((eq[-1] / eq[0]) ** (1 / years) - 1)
    peak = np.maximum.accumulate(eq)
    mdd = float((eq / peak - 1).min())
    vol = float(np.std(ret, ddof=1) * math.sqrt(252))
    m = c.seg_metrics(result, *WIN)
    return {
        'annual': round(annual, 6),
        'max_drawdown': round(mdd, 6),
        'calmar': round(annual / abs(mdd), 4),
        'sharpe': round(float(np.mean(ret) / np.std(ret, ddof=1) * math.sqrt(252)), 4),
        'vol': round(vol, 4),
        'turnover_two_way': round(m['turnover_two_way'], 4),
        'cost_share_of_gross': round(m['cost_share_of_gross'], 4),
    }


def main() -> None:
    t0 = time.perf_counter()
    data = f.base.load_universe(WARMUP_START, f.base.END)
    data, _ = f.base.drop_contract_violations(data)
    _cal, month_ends, prev_day = f.build_calendar(data)
    path, prev_day = v3_path(data, month_ends, prev_day)
    dividends = f._load_local_dividends(list(data), None, None)
    st_status = f._load_st_status(None, data, None, WARMUP_START, f.base.END,
                                  None, None)
    schedules = v.schedules_from_path(path, prev_day)
    signal_days = sorted(path)
    eq_days, eq_ret = v3_daily_returns()
    scales = {
        '波动率目标20%': vol_scale(eq_days, eq_ret, signal_days),
        '200日均线过滤': ma_scale(signal_days),
    }
    out: Dict[str, Any] = {
        '口径': {
            '预热': '2009-08-01 起步、2009-09 首个信号；后复权；月末信号、次日开盘成交、月频',
            'v3基准': V3_BASE,
            '波动率目标': f'σ60 取 v3 自身过去 {VOL_WINDOW} 日已实现波动率（年化），scale=min(1,{VOL_TARGET}/σ60)',
            '均线过滤': f'中证全指 000985.SH 收盘<MA{MA_WINDOW} 时 scale={MA_SCALE}',
            '门槛': 'Calmar 提升且年化降幅 ≤1pp',
        },
        '结果': {},
    }
    for name, sc in scales.items():
        scaled = apply_scale(schedules, sc)
        strat = {code: f.base.MonthlyTargetWeightStrategy(sig)
                 for code, sig in scaled.items() if code in data and sig}
        result = run(data, strat, st_status, dividends)
        m = curve_metrics(result)
        m['scale_mean'] = round(float(np.mean(list(sc.values()))), 4)
        m['scale_min'] = round(float(np.min(list(sc.values()))), 4)
        m['vs_v3_annual'] = round(m['annual'] - V3_BASE['annual'], 6)
        m['vs_v3_calmar'] = round(m['calmar'] - V3_BASE['calmar'], 4)
        m['通过'] = bool(m['calmar'] > V3_BASE['calmar']
                        and (V3_BASE['annual'] - m['annual']) <= 0.01)
        out['结果'][name] = m
        print('OVERLAY ' + json.dumps({'name': name, **m}, ensure_ascii=False),
              flush=True)
        del result, strat
        gc.collect()
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding='utf-8')
    print(f'ELAPSED {time.perf_counter() - t0:.1f}s', flush=True)


if __name__ == '__main__':
    main()
