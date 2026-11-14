#!/usr/bin/env python3
"""只读检验：把「业绩预减」类公告当剔除条件，看能否增强 v3。

条件：信号日 T 往前 60 个自然日内该标的发布过 ``event_type='业绩预减'``
公告（本地表已按 G-072 把预减/略减/首亏/续亏折叠进该类型），则 T 日不进池；
其余口径完全沿用 v3（月频/半年频、剔 60 日波动率最高 20% + 剔上市不满
250 交易日、预热 2009-08、T 收盘信号 / T+1 开盘成交、成本与 v3 相同）。

先报两项诊断：① 覆盖度——每月截面命中数与占比的中位/最小/最大、2010–2014
与 2015 之后对比、零覆盖月份清单；② 与 v3 已剔除集合（高波 20% ∪ 次新）的
月度 Jaccard 均值，``>= 0.3`` 判为重复下注，直接停手。

零覆盖月份不自动停：事件表 2024-12-19 后只有 2025 年 em_yjyg 的 2,041 条、
2026 年无数据，因此主判区间截止 2024-12-31，全区间只作辅证。三门槛
（增量 ≥0.5pp、两段皆正、年换手 ≤2.5x）一律按主判区间判定，两段为
2010-01-04~2016-12-30 与 2017-01-03~2024-12-31。

只读：不改引擎、不改 data/local_store、不改成本默认值。

用法
----
    .venv\\Scripts\\python.exe tools\\factor_event_exclusion.py
"""
from __future__ import annotations

import gc
import json
import sys
import time
from bisect import bisect_left, bisect_right
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

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

WARMUP_START = '2009-08-01'
FIRST_SIGNAL = date(2009, 9, 1)
WIN = c.SEG1[0], c.SEG2[1]
# 事件表业绩预告覆盖到 2024-12-19（tushare）+ 2025 全年（em_yjyg，仅 2,041 条）；
# 2026 年无数据，最后一段该条件等于空转。主判区间截止 2024-12-31，
# 全区间指标只作辅证；不放宽 60 日窗口。
MAIN = date(2010, 1, 4), date(2024, 12, 31)
SEG2_MAIN = date(2017, 1, 3), date(2024, 12, 31)
EVENTS = ROOT / 'data/local_store/meta/events/events.parquet'
BAD_TYPE = '业绩预减'
LOOKBACK_DAYS = 60
JACCARD_STOP = 0.30
FREQS: Dict[str, List[int]] = {'monthly': list(range(1, 13)), 'semi': [6, 12]}
# v3 冻结值（同脚本复跑只作核对，判定用复跑值）
V3_FROZEN = {'monthly': 0.091312, 'semi': 0.087883}


def load_bad_events() -> Dict[str, List[date]]:
    df = (pl.scan_parquet(EVENTS)
          .filter(pl.col('event_type') == BAD_TYPE)
          .select(['ts_code', 'publish_time'])
          .collect())
    out: Dict[str, List[date]] = {}
    for code, ts in zip(df['ts_code'].to_list(),
                        df['publish_time'].to_list(), strict=True):
        out.setdefault(code, []).append(ts.date())
    for days in out.values():
        days.sort()
    logger.info('预减公告：{} 只 / {} 条', len(out), df.height)
    return out


def event_set(bad: Dict[str, List[date]], codes: Set[str],
              day: date) -> Set[str]:
    lo = day - timedelta(days=LOOKBACK_DAYS)
    hit: Set[str] = set()
    for code in codes:
        days = bad.get(code)
        if not days:
            continue
        i = bisect_left(days, lo)
        if i < len(days) and days[i] <= day:
            hit.add(code)
    return hit


def stack_path(quoted: Dict[date, List[str]], members: Dict[date, Any],
               data: Dict[str, Any], list_dates: Dict[str, date],
               all_days: List[date], bad: Dict[str, List[date]],
               days: List[date]) -> Tuple[Dict[date, List[str]], Dict[date, Set[str]]]:
    path: Dict[date, List[str]] = {}
    ev_by_month: Dict[date, Set[str]] = {}
    for m in days:
        q = set(quoted[m])
        vol = set(members[m][4] or ()) & q
        new = x.new_listing_set(data, sorted(q), list_dates, all_days, m)
        ev = event_set(bad, q, m)
        ev_by_month[m] = ev
        path[m] = sorted(q - vol - new - ev)
    return path, ev_by_month


def main() -> None:
    t0 = time.perf_counter()
    data = f.base.load_universe(WARMUP_START, f.base.END)
    data, _ = f.base.drop_contract_violations(data)
    _calendar, month_ends, prev_day = f.build_calendar(data)
    days_all = [d for d in month_ends if d >= FIRST_SIGNAL]
    panel = f.compute_vol_panel(data, days_all)
    members, _short = f.quantile_members(panel)
    quoted = v.quoted_sets(data, days_all)
    months_ok = [m for m in days_all if m in members and m in quoted]
    list_dates = e.load_list_dates(data)
    all_days = e.load_trading_days()
    bad = load_bad_events()
    dividends = f._load_local_dividends(list(data), None, None)
    st_status = f._load_st_status(None, data, None, WARMUP_START,
                                  f.base.END, None, None)

    # 诊断：覆盖度 + 与 v3 剔除集合的重叠
    counts: List[int] = []
    jacs: List[float] = []
    cover_months: List[Tuple[date, int, int]] = []
    for m in months_ok:
        q = set(quoted[m])
        vol = set(members[m][4] or ()) & q
        new = x.new_listing_set(data, sorted(q), list_dates, all_days, m)
        excl = vol | new
        ev = event_set(bad, q, m)
        counts.append(len(ev))
        cover_months.append((m, len(ev), len(q)))
        union = ev | excl
        if union:
            jacs.append(len(ev & excl) / len(union))
    m_old = [n for (m, n, _) in cover_months if m.year <= 2014]
    m_new = [n for (m, n, _) in cover_months if m.year >= 2015]
    pool_old = [p for (m, _, p) in cover_months if m.year <= 2014]
    pool_new = [p for (m, _, p) in cover_months if m.year >= 2015]
    diag = {
        'months': len(cover_months),
        'count_median': int(np.median(counts)),
        'count_min': int(min(counts)),
        'count_max': int(max(counts)),
        'zero_months': [m.isoformat() for (m, n, _) in cover_months if n == 0],
        'share_median': round(float(np.median(
            [n / p for (_, n, p) in cover_months])), 4),
        'pre2015': {
            'months': len(m_old),
            'count_median': round(float(np.median(m_old)), 1),
            'count_mean': round(float(np.mean(m_old)), 1),
            'count_min': int(min(m_old)),
            'share_median': round(float(np.median(
                [a / b for a, b in zip(m_old, pool_old, strict=True)])), 4),
        },
        'post2015': {
            'months': len(m_new),
            'count_median': round(float(np.median(m_new)), 1),
            'count_mean': round(float(np.mean(m_new)), 1),
            'count_min': int(min(m_new)),
            'share_median': round(float(np.median(
                [a / b for a, b in zip(m_new, pool_new, strict=True)])), 4),
        },
        'jaccard_mean': round(float(np.mean(jacs)), 4),
        'jaccard_p90': round(float(np.percentile(jacs, 90)), 4),
    }
    print('DIAG ' + json.dumps(diag, ensure_ascii=False), flush=True)

    if diag['jaccard_mean'] >= JACCARD_STOP:
        print('STOP jaccard_mean={:.4f}>=0.30，判为重复下注，不跑回测'.format(
            diag['jaccard_mean']), flush=True)
        print(f'ELAPSED {time.perf_counter() - t0:.1f}s', flush=True)
        return

    results: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for freq, mons in FREQS.items():
        days = [m for m in months_ok if m.month in mons]
        path_v3, _ = stack_path(quoted, members, data, list_dates, all_days,
                                {}, days)
        path_ev, _ = stack_path(quoted, members, data, list_dates, all_days,
                                bad, days)
        for tag, path in (('v3', path_v3), ('v3_ev', path_ev)):
            sched = v.schedules_from_path(path, prev_day)
            result = f.run_portfolio(data, sched, dividends, st_status)
            item = {
                'freq': freq, 'obj': tag, 'rebalances': len(days),
                'members_median': int(np.median([len(z) for z in path.values()])),
                'full': c.seg_metrics(result, *WIN),
                'main': c.seg_metrics(result, *MAIN),
                'seg1': c.seg_metrics(result, *c.SEG1),
                'seg2': c.seg_metrics(result, *SEG2_MAIN),
                'v3_frozen': V3_FROZEN[freq],
            }
            results[(freq, tag)] = item
            print('RUN ' + json.dumps(item, ensure_ascii=False), flush=True)
            del result, sched
            gc.collect()

    gates: Dict[str, Any] = {}
    for freq in FREQS:
        b = results[(freq, 'v3')]
        it = results[(freq, 'v3_ev')]
        a = it['main']['annual']
        g = {
            'freq': freq,
            'v3_annual': round(b['main']['annual'], 6),
            'ev_annual': round(a, 6),
            'vs_v3': round(a - b['main']['annual'], 6),
            'annual': round(a, 6),
            'dd': round(it['main']['max_drawdown'], 6),
            'turn': round(it['main']['turnover_two_way'], 4),
            'cost_share': round(it['main']['cost_share_of_gross'], 4),
            'seg1_v3': round(it['seg1']['annual'] - b['seg1']['annual'], 6),
            'seg2_v3': round(it['seg2']['annual'] - b['seg2']['annual'], 6),
            'full_v3': round(b['full']['annual'], 6),
            'full_ev': round(it['full']['annual'], 6),
            'full_delta': round(it['full']['annual'] - b['full']['annual'], 6),
        }
        g['both_pos'] = g['seg1_v3'] > 0 and g['seg2_v3'] > 0
        g['turn_ok'] = g['turn'] <= 2.5
        g['passed'] = g['vs_v3'] >= 0.005 and g['both_pos'] and g['turn_ok']
        gates[freq] = g
    print('GATE ' + json.dumps(gates, ensure_ascii=False, indent=2), flush=True)
    print(f'ELAPSED {time.perf_counter() - t0:.1f}s', flush=True)


if __name__ == '__main__':
    main()
