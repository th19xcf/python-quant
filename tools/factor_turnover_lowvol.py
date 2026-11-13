#!/usr/bin/env python3
"""低换手率因子：先做与低波的相关性筛选，通过后再回测两种用法。

第一步（便宜）
--------------
月末横截面上，20 日平均换手率（``factors/stock_turnover.parquet`` 的 ``turn``）
与 60 日波动率的 Spearman 相关：全区间月均 + 月度分布分位。以及
「换手率最低 20%」与「波动率最低 20%」两个集合的月均 Jaccard。
Jaccard > 0.30 判为重复下注，直接停（只打印 STOP，不跑回测）。

第二步（仅当第一步通过）
------------------------
- 多头端：换手率最低 20% 等权；
- 剔除端：剔除换手率最高 20%，其余等权；
- 各跑月频与半年频；口径、成本、涨跌停停牌、复权与 v1 完全一致；
- 剔除端过三条门槛（增量 ≥0.5pp、两段皆正、换手 ≤2.5x）时，再测与 v3
  （剔高波 20% + 剔次新）的叠加。

换手率取不到或不足 20 日的标的不进当期截面，数量如实打印。

用法
----
    .venv\\Scripts\\python.exe tools\\factor_turnover_lowvol.py
"""
from __future__ import annotations

import gc
import json
import sys
import time
from bisect import bisect_right
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

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
TURNOVER = ROOT / 'data/local_store/factors/stock_turnover.parquet'
TURN_WINDOW = 20
STALE_DAYS = 30
JACCARD_STOP = 0.30
FREQS: Dict[str, List[int]] = {'monthly': list(range(1, 13)), 'semi': [6, 12]}
# 同频基线（已测，直接引用）
BASE = {
    'monthly': {
        'v1': {'annual': 0.072873, 'seg1': 0.138803, 'seg2': 0.027529},
        'v3': {'annual': 0.091312, 'seg1': 0.148970, 'seg2': 0.051322},
    },
    'semi': {
        'v1': {'annual': 0.071166, 'seg1': 0.133333, 'seg2': 0.028259},
        'v3': {'annual': 0.087883, 'seg1': 0.144780, 'seg2': 0.048461},
    },
}


def load_turnover() -> Dict[str, Tuple[np.ndarray, np.ndarray]]:
    df = (pl.scan_parquet(TURNOVER)
          .select(['ts_code', 'trade_date', 'turn'])
          .filter(pl.col('trade_date') >= date(2009, 7, 1))
          .collect().sort(['ts_code', 'trade_date']))
    codes = df['ts_code'].to_numpy()
    starts = np.flatnonzero(np.r_[True, codes[1:] != codes[:-1]])
    ends = np.r_[starts[1:], len(codes)]
    dts = df['trade_date'].to_numpy()
    vals = df['turn'].to_numpy().astype('float64')
    out = {str(codes[s]): (dts[s:t], vals[s:t]) for s, t in zip(starts, ends)}
    logger.info('换手率：{} 只 / {} 行', len(out), len(codes))
    return out


def turn20(pair: Tuple[np.ndarray, np.ndarray] | None, day: date) -> float | None:
    """截至月末的 20 日均换手率；不足 20 日或末值过旧视为不进截面。"""
    if pair is None:
        return None
    dts, vals = pair
    k = int(np.searchsorted(dts, np.datetime64(day), side='right')) - 1
    if k + 1 < TURN_WINDOW:
        return None
    window = vals[k - TURN_WINDOW + 1:k + 1]
    if not np.isfinite(window).all():
        return None
    last = dts[k].astype('datetime64[D]').astype(date)
    if last < day - timedelta(days=STALE_DAYS):
        return None
    return float(window.mean())


def avg_rank(x: np.ndarray) -> np.ndarray:
    order = np.argsort(x, kind='mergesort')
    ranks = np.empty(x.size, dtype=float)
    ranks[order] = np.arange(1, x.size + 1, dtype=float)
    sx = x[order]
    i = 0
    while i < x.size:
        j = i
        while j + 1 < x.size and sx[j + 1] == sx[i]:
            j += 1
        if j > i:
            ranks[order[i:j + 1]] = (i + j) / 2.0 + 1.0
        i = j + 1
    return ranks


def spearman(a: np.ndarray, b: np.ndarray) -> float:
    ra, rb = avg_rank(a), avg_rank(b)
    ra -= ra.mean()
    rb -= rb.mean()
    den = float(np.sqrt((ra ** 2).sum() * (rb ** 2).sum()))
    return float((ra * rb).sum() / den) if den else float('nan')


def main() -> None:
    t0 = time.perf_counter()
    data = f.base.load_universe(WARMUP_START, f.base.END)
    data, _ = f.base.drop_contract_violations(data)
    calendar, month_ends, prev_day = f.build_calendar(data)
    panel = f.compute_vol_panel(data, month_ends)
    members, _short = f.quantile_members(panel)
    quoted = v.quoted_sets(data, month_ends)
    dividends = f._load_local_dividends(list(data), None, None)
    st_status = f._load_st_status(None, data, None, WARMUP_START,
                                  f.base.END, None, None) or {}
    list_dates = e.load_list_dates(data)
    all_days = e.load_trading_days()
    turnover = load_turnover()
    months = [m for m in month_ends if m >= FIRST_SIGNAL and m in members and m in quoted]

    # 月末换手率缓存
    t20: Dict[date, Dict[str, float]] = {}
    missing: List[int] = []
    for m in months:
        vals = {}
        for code in quoted[m]:
            x = turn20(turnover.get(code), m)
            if x is not None:
                vals[code] = x
        t20[m] = vals
        missing.append(len(quoted[m]) - len(vals))
    logger.info('截面换手率覆盖：缺口中位 {} 只，最大 {} 只',
                int(np.median(missing)), int(max(missing)))

    # 第一步：相关性 + Jaccard（同一截面口径：有波动率且有换手率）
    corrs: List[float] = []
    jacs: List[float] = []
    for m in months:
        common = [x for x in t20[m] if x in panel[m]]
        if len(common) < 200:
            continue
        tv = np.array([t20[m][x] for x in common])
        vv = np.array([panel[m][x] for x in common])
        corrs.append(spearman(tv, vv))
        n = len(common)
        nq = max(1, n // 5)
        low_turn = {common[i] for i in np.argsort(tv, kind='mergesort')[:nq]}
        low_vol = {common[i] for i in np.argsort(vv, kind='mergesort')[:nq]}
        jacs.append(len(low_turn & low_vol) / len(low_turn | low_vol))
    corr_desc = {
        'months': len(corrs),
        'mean': round(float(np.mean(corrs)), 4),
        'p10': round(float(np.percentile(corrs, 10)), 4),
        'p25': round(float(np.percentile(corrs, 25)), 4),
        'p50': round(float(np.percentile(corrs, 50)), 4),
        'p75': round(float(np.percentile(corrs, 75)), 4),
        'p90': round(float(np.percentile(corrs, 90)), 4),
        'share_positive': round(float(np.mean(np.array(corrs) > 0)), 4),
        'jaccard_low_turn_low_vol': round(float(np.mean(jacs)), 4),
        'jaccard_p90': round(float(np.percentile(jacs, 90)), 4),
        'pool_median': int(np.median([len(x) for x in t20.values()])),
        'missing_median': int(np.median(missing)),
        'missing_max': int(max(missing)),
    }
    print('STAGE1 ' + json.dumps(corr_desc, ensure_ascii=False), flush=True)
    if corr_desc['jaccard_low_turn_low_vol'] > JACCARD_STOP:
        print('STOP jaccard>{:.2f}，判为重复下注，不跑第二步'.format(JACCARD_STOP),
              flush=True)
        print('ELAPSED {:.1f}s'.format(time.perf_counter() - t0), flush=True)
        return

    # 第二步：两种用法 × 两档频率
    results: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for freq, mons in FREQS.items():
        days = [m for m in months if m.month in mons]
        paths: Dict[str, Dict[date, List[str]]] = {'long': {}, 'excl': {}}
        for m in days:
            codes = sorted(t20[m], key=lambda k: t20[m][k])
            nq = max(1, len(codes) // 5)
            low = set(codes[:nq])
            high = set(codes[-nq:])
            paths['long'][m] = sorted(low)
            paths['excl'][m] = sorted(set(codes) - high)
        for tag, path in paths.items():
            sched = v.schedules_from_path(path, prev_day)
            result = f.run_portfolio(data, sched, dividends, st_status)
            item = {
                'freq': freq, 'obj': tag, 'rebalances': len(days),
                'members_median': int(np.median([len(x) for x in path.values()])),
                'full': c.seg_metrics(result, *WIN),
                'seg1': c.seg_metrics(result, *c.SEG1),
                'seg2': c.seg_metrics(result, *c.SEG2),
            }
            results[(freq, tag)] = item
            print('RUN ' + json.dumps(item, ensure_ascii=False), flush=True)
            del result, sched
            gc.collect()

    gates: Dict[str, Any] = {}
    for (freq, tag), it in results.items():
        b1, b3 = BASE[freq]['v1'], BASE[freq]['v3']
        a = it['full']['annual']
        g = {
            'freq': freq, 'obj': tag,
            'annual': round(a, 6),
            'dd': round(it['full']['max_drawdown'], 6),
            'turn': round(it['full']['turnover_two_way'], 4),
            'cost_share': round(it['full']['cost_share_of_gross'], 4),
            'vs_v1': round(a - b1['annual'], 6),
            'vs_v3': round(a - b3['annual'], 6),
            'seg1_v1': round(it['seg1']['annual'] - b1['seg1'], 6),
            'seg2_v1': round(it['seg2']['annual'] - b1['seg2'], 6),
            'seg1_v3': round(it['seg1']['annual'] - b3['seg1'], 6),
            'seg2_v3': round(it['seg2']['annual'] - b3['seg2'], 6),
        }
        g['both_pos'] = g['seg1_v1'] > 0 and g['seg2_v1'] > 0
        g['turn_ok'] = g['turn'] <= 2.5
        g['passed'] = g['vs_v1'] >= 0.005 and g['both_pos'] and g['turn_ok']
        gates['{}_{}'.format(freq, tag)] = g
    print('GATE ' + json.dumps(gates, ensure_ascii=False, indent=2), flush=True)

    # 剔除端过门槛时，测与 v3 的叠加（剔高波 20% + 剔次新 + 剔高换手 20%）
    hit = [freq for freq in FREQS if gates['{}_{}'.format(freq, 'excl')]['passed']]
    stack_out: Dict[str, Any] = {}
    for freq in hit:
        days = [m for m in months if m.month in FREQS[freq]]
        path: Dict[date, List[str]] = {}
        for m in days:
            base = set(t20[m]) & set(quoted[m])
            vol_top = set(members[m][4] or ()) & base
            idx = bisect_right(all_days, m)
            new = set()
            for code in base:
                ld = list_dates.get(code)
                if ld is None:
                    ld = date.fromisoformat(str(e.np_dates(data[code])[0]))
                if idx - bisect_right(all_days, ld) < e.MIN_HISTORY:
                    new.add(code)
            codes = sorted(base - vol_top - new, key=lambda k: t20[m][k])
            nq = max(1, len(codes) // 5)
            path[m] = codes[:-nq] if nq else codes
        sched = v.schedules_from_path(path, prev_day)
        result = f.run_portfolio(data, sched, dividends, st_status)
        it = {
            'freq': freq, 'obj': 'stack_v3_turn',
            'rebalances': len(days),
            'members_median': int(np.median([len(x) for x in path.values()])),
            'full': c.seg_metrics(result, *WIN),
            'seg1': c.seg_metrics(result, *c.SEG1),
            'seg2': c.seg_metrics(result, *c.SEG2),
        }
        b1, b3 = BASE[freq]['v1'], BASE[freq]['v3']
        stack_out[freq] = {
            'annual': round(it['full']['annual'], 6),
            'dd': round(it['full']['max_drawdown'], 6),
            'turn': round(it['full']['turnover_two_way'], 4),
            'cost_share': round(it['full']['cost_share_of_gross'], 4),
            'vs_v1': round(it['full']['annual'] - b1['annual'], 6),
            'vs_v3': round(it['full']['annual'] - b3['annual'], 6),
            'seg1_v1': round(it['seg1']['annual'] - b1['seg1'], 6),
            'seg2_v1': round(it['seg2']['annual'] - b1['seg2'], 6),
            'seg1_v3': round(it['seg1']['annual'] - b3['seg1'], 6),
            'seg2_v3': round(it['seg2']['annual'] - b3['seg2'], 6),
        }
        print('RUN ' + json.dumps(it, ensure_ascii=False), flush=True)
        del result, sched
        gc.collect()
    print('STACK ' + json.dumps(stack_out, ensure_ascii=False, indent=2), flush=True)
    print('ELAPSED {:.1f}s'.format(time.perf_counter() - t0), flush=True)


if __name__ == '__main__':
    main()
