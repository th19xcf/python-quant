#!/usr/bin/env python3
"""v3 冻结前的市值与 beta 检验：+1.84pp 是否只是小盘/高 beta 漂移。

三项：
1. 持仓市值分布：v1 / v2 / v3 每期持仓的自由流通市值（估值表 circ_mv）中位数与
   25/75 分位，按月汇总为年均值与区间，并给 v3 相对 v1 的中枢偏移比例；
2. beta 归因：v1 与 v3 月频收益对沪深 300（``000300.SH`` 本地指数）月频收益
   做 OLS，报 beta、R²，并用「beta 差 × 基准年化收益」粗算可解释的超额；
3. 打印结论所需的干净增量（+1.84pp 减去可解释部分）。

口径与 v1 一致：月末调仓、T 日收盘信号 / T+1 开盘成交、1% 现金缓冲、10bp 滑点、
佣金万 2.5、印花税固定千 1、涨跌停停牌与复权不动、本金 10 亿；预热 2009-08，
指标窗口 2010-01-04~2026-11-13。v2 只用于市值对照，不重跑回测（市值只需成分）。

用法
----
    .venv\\Scripts\\python.exe tools\\factor_v3_cap_beta.py
"""
from __future__ import annotations

import gc
import json
import sys
import time
from bisect import bisect_right
from datetime import date
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
for _p in (ROOT, ROOT / 'tools'):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from loguru import logger

import factor_exclusion_list as e
import factor_lowvol_checks as c
import factor_lowvol_quantile as f
import factor_lowvol_variants as v

WARMUP_START = '2009-08-01'
FIRST_SIGNAL = date(2009, 9, 1)
WIN = c.SEG1[0], c.SEG2[1]
VALUATION = ROOT / 'data/local_store/meta/financials/valuation_daily.parquet'
BENCH = ROOT / 'data/local_store/quotes/index/sh/000300.SH.parquet'
EXCESS_PP = 0.018439  # 月频 v3 相对 v1 的年化增量（已测）


def load_cap_arrays() -> Dict[str, Tuple[np.ndarray, np.ndarray]]:
    """估值表 circ_mv（万元）按代码切片，仅取 >= 预热起点。"""
    df = pl.scan_parquet(VALUATION).select(['ts_code', 'trade_date', 'circ_mv']).filter(
        (pl.col('trade_date') >= date(2009, 8, 1)) & pl.col('circ_mv').is_not_null()).collect()
    df = df.sort(['ts_code', 'trade_date'])
    codes = df['ts_code'].to_numpy()
    starts = np.flatnonzero(np.r_[True, codes[1:] != codes[:-1]])
    ends = np.r_[starts[1:], len(codes)]
    dts = df['trade_date'].to_numpy()
    caps = df['circ_mv'].to_numpy()
    out: Dict[str, Tuple[np.ndarray, np.ndarray]] = {}
    for s, t in zip(starts, ends):
        out[str(codes[s])] = (dts[s:t], caps[s:t])
    logger.info('估值表 circ_mv：{} 只 / {} 行', len(out), len(codes))
    return out


def cap_of(pair: Tuple[np.ndarray, np.ndarray] | None, day: date) -> float | None:
    if pair is None:
        return None
    dts, vals = pair
    k = int(np.searchsorted(dts, np.datetime64(day), side='right')) - 1
    if k < 0:
        return None
    x = float(vals[k])
    return x if np.isfinite(x) and x > 0 else None


def month_returns(curve: List[Tuple[date, float]]) -> Dict[Tuple[int, int], float]:
    """日频权益曲线 → 月末净值 → 月度收益。"""
    ends: Dict[Tuple[int, int], float] = {}
    for d, eq in curve:
        ends[(d.year, d.month)] = eq
    keys = sorted(ends)
    out: Dict[Tuple[int, int], float] = {}
    for a, b in zip(keys, keys[1:]):
        out[b] = ends[b] / ends[a] - 1.0
    return out


def bench_returns(days: List[date]) -> Dict[Tuple[int, int], float]:
    df = pl.read_parquet(BENCH, columns=['date', 'close']).sort('date')
    ends: Dict[Tuple[int, int], float] = {}
    for d, close in zip(df['date'].to_list(), df['close'].to_list()):
        day = date.fromisoformat(str(d)[:10]) if not isinstance(d, date) else d
        if day in days and close:
            ends[(day.year, day.month)] = float(close)
    keys = sorted(ends)
    return {b: ends[b] / ends[a] - 1.0 for a, b in zip(keys, keys[1:])}


def ols(y: np.ndarray, x: np.ndarray) -> Tuple[float, float, float]:
    """返回 (beta, alpha, R²)。"""
    xm, ym = x.mean(), y.mean()
    sxx = float(((x - xm) ** 2).sum())
    beta = float(((x - xm) * (y - ym)).sum() / sxx)
    alpha = float(ym - beta * xm)
    resid = y - (alpha + beta * x)
    ss_tot = float(((y - ym) ** 2).sum())
    r2 = 1.0 - float((resid ** 2).sum()) / ss_tot if ss_tot else 0.0
    return beta, alpha, r2


def main() -> None:
    t0 = time.perf_counter()
    data = f.base.load_universe(WARMUP_START, f.base.END)
    data, _ = f.base.drop_contract_violations(data)
    calendar, month_ends, prev_day = f.build_calendar(data)
    months = [m for m in month_ends if m >= FIRST_SIGNAL]
    panel = f.compute_vol_panel(data, month_ends)
    members, _short = f.quantile_members(panel)
    quoted = v.quoted_sets(data, month_ends)
    dividends = f._load_local_dividends(list(data), None, None)
    st_status = f._load_st_status(None, data, None, WARMUP_START,
                                  f.base.END, None, None) or {}
    list_dates = e.load_list_dates(data)
    all_days = e.load_trading_days()
    cap_arrays = load_cap_arrays()

    months_ok = [m for m in months if m in members and m in quoted]
    paths: Dict[str, Dict[date, List[str]]] = {'v1': {}, 'v2': {}, 'v3': {}}
    for m in months_ok:
        q = set(quoted[m])
        vol = set(members[m][4] or ()) & q
        new = set()
        idx_m = bisect_right(all_days, m)
        for code in sorted(q):
            ld = list_dates.get(code)
            if ld is None:
                ld = date.fromisoformat(str(e.np_dates(data[code])[0]))
            if idx_m - bisect_right(all_days, ld) < e.MIN_HISTORY:
                new.add(code)
        paths['v1'][m] = sorted(q)
        paths['v2'][m] = sorted(q - vol)
        paths['v3'][m] = sorted(q - vol - new)

    # 1) 市值分布（成分口径，每月取自由流通市值分位；circ_mv 单位为元 → 亿元）
    series: Dict[str, Dict[str, List[float]]] = {
        t: {'med': [], 'q25': [], 'q75': []} for t in paths}
    ratio_v3: List[float] = []
    ratio_v2: List[float] = []
    by_year: Dict[int, List[float]] = {}
    for m in months_ok:
        stats: Dict[str, Tuple[float, float, float]] = {}
        keep = True
        for tag, path in paths.items():
            vals = [cap_of(cap_arrays.get(x), m) for x in path[m]]
            arr = np.array([x for x in vals if x is not None], dtype=float)
            if arr.size < 50:
                keep = False
                break
            p25, p50, p75 = np.percentile(arr, [25, 50, 75]) / 1e8
            stats[tag] = (p25, p50, p75)
        if not keep:
            continue
        for tag, (p25, p50, p75) in stats.items():
            series[tag]['q25'].append(p25)
            series[tag]['med'].append(p50)
            series[tag]['q75'].append(p75)
        ratio_v3.append(stats['v3'][1] / stats['v1'][1] - 1.0)
        ratio_v2.append(stats['v2'][1] / stats['v1'][1] - 1.0)
        by_year.setdefault(m.year, []).append(stats['v3'][1] / stats['v1'][1] - 1.0)
    cap_stats: Dict[str, Dict[str, float]] = {}
    for tag, s in series.items():
        cap_stats[tag] = {
            'median_yi': round(float(np.mean(s['med'])), 2),
            'q25_yi': round(float(np.mean(s['q25'])), 2),
            'q75_yi': round(float(np.mean(s['q75'])), 2),
            'median_min': round(float(np.min(s['med'])), 2),
            'median_max': round(float(np.max(s['med'])), 2),
        }
    cap_out = {
        **cap_stats,
        'v3_vs_v1_median_offset': {
            'mean': round(float(np.mean(ratio_v3)), 4),
            'min': round(float(np.min(ratio_v3)), 4),
            'max': round(float(np.max(ratio_v3)), 4),
            'by_year': {str(y): round(float(np.mean(vs)), 4)
                        for y, vs in sorted(by_year.items())},
        },
        'v2_vs_v1_median_offset': {
            'mean': round(float(np.mean(ratio_v2)), 4),
            'min': round(float(np.min(ratio_v2)), 4),
            'max': round(float(np.max(ratio_v2)), 4),
        },
    }
    print('CAP ' + json.dumps(cap_out, ensure_ascii=False), flush=True)

    # 2) beta：v1 与 v3 月频收益对沪深 300
    curves: Dict[str, Dict[Tuple[int, int], float]] = {}
    for tag in ('v1', 'v3'):
        sched = v.schedules_from_path(paths[tag], prev_day)
        result = f.run_portfolio(data, sched, dividends, st_status)
        curves[tag] = month_returns([(d, eq) for d, eq in f.curve_of(result)
                                     if WIN[0] <= d <= WIN[1]])
        logger.info('{} 月度收益 {} 个', tag, len(curves[tag]))
        print('RUN ' + json.dumps({'tag': tag, 'months': len(curves[tag])}), flush=True)
        del result, sched
        gc.collect()

    days = set(calendar)
    bmk = bench_returns(days)
    keys = sorted(set(curves['v1']) & set(curves['v3']) & set(bmk))
    y1 = np.array([curves['v1'][k] for k in keys])
    y3 = np.array([curves['v3'][k] for k in keys])
    x = np.array([bmk[k] for k in keys])
    b1, a1, r1 = ols(y1, x)
    b3, a3, r3 = ols(y3, x)
    years = len(keys) / 12.0
    mkt_cagr = float(np.prod(1.0 + x) ** (1.0 / years) - 1.0)
    mkt_mean = float(x.mean() * 12.0)
    explained = (b3 - b1) * mkt_cagr
    beta_out = {
        'bench': '000300.SH',
        'months': len(keys),
        'period': [str(keys[0]), str(keys[-1])],
        'beta_v1': round(b1, 4), 'r2_v1': round(r1, 4), 'alpha_v1_m': round(a1, 5),
        'beta_v3': round(b3, 4), 'r2_v3': round(r3, 4), 'alpha_v3_m': round(a3, 5),
        'delta_beta': round(b3 - b1, 4),
        'mkt_cagr': round(mkt_cagr, 4), 'mkt_mean_annual': round(mkt_mean, 4),
        'explained_pp_cagr': round(explained * 100, 3),
        'explained_pp_mean': round((b3 - b1) * mkt_mean * 100, 3),
        'excess_pp': round(EXCESS_PP * 100, 3),
        'clean_pp_cagr': round((EXCESS_PP - explained) * 100, 3),
        'clean_pp_mean': round((EXCESS_PP - (b3 - b1) * mkt_mean) * 100, 3),
    }
    print('BETA ' + json.dumps(beta_out, ensure_ascii=False), flush=True)
    print('ELAPSED {:.1f}s'.format(time.perf_counter() - t0), flush=True)


if __name__ == '__main__':
    main()
