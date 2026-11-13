#!/usr/bin/env python3
"""叠加剔除组合：剔 60 日波动率最高 20% + 剔上市不满 250 交易日次新，其余等权。

只叠加上一轮通过三条门槛的两项（vol / new），月频与半年频各跑一次：

- monthly：月末调仓（v1 基线 7.2873% 与 v2 8.466% 已有同口径结果，直接引用）；
- semi：半年末（6/12 月）调仓，同频基线在本脚本重跑一次作为增量参照。

口径与 v1 完全一致：T 日收盘信号 / T+1 开盘成交、月末（半年末）两腿再平衡、
1% 现金缓冲、10bp 滑点、佣金万 2.5、印花税固定千 1、涨跌停停牌与复权不动、
本金 10 亿；预热 2009-08，指标切 2010-01-04~2026-11-13。

另补两项核对：vol|new 月度平均 Jaccard；ST 识别覆盖度（月度 ST 数量分布、
占当期股票池比例、整年为空的年份）。

用法
----
    .venv\\Scripts\\python.exe tools/factor_exclusion_combo.py
"""
from __future__ import annotations

import gc
import json
import sys
import time
from bisect import bisect_right
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import numpy as np

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
FREQS: Dict[str, List[int]] = {'monthly': list(range(1, 13)), 'semi': [6, 12]}
# 月频全市场等权基线已有同口径结果，不重跑
SKIP_BASE = {'monthly'}


def new_listing_set(data, quoted_day: List[str], list_dates, all_days: List[date],
                    day: date) -> set:
    """上市不满 250 个交易日（缺 list_date 时用面板首日，按全历史交易日计数）。"""
    idx_m = bisect_right(all_days, day)
    out = set()
    for code in quoted_day:
        ld = list_dates.get(code)
        if ld is None:
            arr = e.np_dates(data[code])
            ld = date.fromisoformat(str(arr[0]))
        if idx_m - bisect_right(all_days, ld) < e.MIN_HISTORY:
            out.add(code)
    return out


def main() -> None:
    t0 = time.perf_counter()
    data = f.base.load_universe(WARMUP_START, f.base.END)
    data, _ = f.base.drop_contract_violations(data)
    calendar, month_ends, prev_day = f.build_calendar(data)
    dividends = f._load_local_dividends(list(data), None, None)
    st_status = f._load_st_status(None, data, None, WARMUP_START,
                                  f.base.END, None, None) or {}
    list_dates = e.load_list_dates(data)
    all_days = e.load_trading_days()
    logger.info('st_status 覆盖 {} 只 / 行情 {} 只', len(st_status), len(data))

    results: Dict[tuple, Dict[str, Any]] = {}
    jac_vol_new: List[float] = []
    st_counts: List[int] = []
    st_ratios: List[float] = []
    st_by_year: Dict[int, List[int]] = {}

    for freq, months in FREQS.items():
        days = [d for d in month_ends if d.month in months and d >= FIRST_SIGNAL]
        panel = f.compute_vol_panel(data, days)
        members, _short = f.quantile_members(panel)
        quoted = v.quoted_sets(data, days)
        months_ok = [m for m in days if m in members and m in quoted]
        logger.info('{}：调仓日 {} 个，有效 {} 个（{}~{}）', freq, len(days),
                    len(months_ok), months_ok[0].isoformat(), months_ok[-1].isoformat())

        paths: Dict[str, Dict[date, List[str]]] = {'combo': {}}
        if freq not in SKIP_BASE:
            paths['base'] = {}
        for m in months_ok:
            q = set(quoted[m])
            vol = set(members[m][4] or ()) & q
            new = new_listing_set(data, sorted(q), list_dates, all_days, m)
            paths['combo'][m] = sorted(q - vol - new)
            if 'base' in paths:
                paths['base'][m] = sorted(q)
            if freq == 'monthly':
                union = len(vol | new)
                if union:
                    jac_vol_new.append(len(vol & new) / union)
                st = {code for code in q if e.st_at(st_status.get(code), m.isoformat())}
                st_counts.append(len(st))
                st_ratios.append(len(st) / len(q) if q else 0.0)
                st_by_year.setdefault(m.year, []).append(len(st))

        for tag in ('base', 'combo'):
            if tag not in paths:
                continue
            path = paths[tag]
            sizes = [len(x) for x in path.values()]
            sched = v.schedules_from_path(path, prev_day)
            result = f.run_portfolio(data, sched, dividends, st_status)
            item = {
                'freq': freq, 'obj': tag, 'rebalances': len(months_ok),
                'members_median': int(np.median(sizes)),
                'full': c.seg_metrics(result, *WIN),
                'seg1': c.seg_metrics(result, *c.SEG1),
                'seg2': c.seg_metrics(result, *c.SEG2),
            }
            results[(freq, tag)] = item
            logger.info('{} {} 完成（成分中位 {}）', freq, tag, item['members_median'])
            print('RUN ' + json.dumps(item, ensure_ascii=False), flush=True)
            del result, sched, path, sizes
            gc.collect()
        del panel, members, quoted, paths
        gc.collect()

    # 增量：月频对 v1/v2 用已有同口径值；半年频对同频基线
    V1 = {'annual': 0.072873, 'seg1': 0.138803, 'seg2': 0.027529,
          'dd': -0.639740, 'turn': 0.8166, 'cost_share': 0.0273}
    V2 = {'annual': 0.084660, 'seg1': 0.144983, 'seg2': 0.042937,
          'dd': -0.612190, 'turn': 2.0870, 'cost_share': 0.0706}
    deltas: Dict[str, Any] = {}
    for freq in FREQS:
        combo = results[(freq, 'combo')]
        ref_v1 = (results[(freq, 'base')]['full']['annual']
                  if (freq, 'base') in results else V1['annual'])
        d = {
            'annual': combo['full']['annual'],
            'dd': combo['full']['max_drawdown'],
            'turn': combo['full']['turnover_two_way'],
            'cost_share': combo['full']['cost_share_of_gross'],
            'members_median': combo['members_median'],
            'vs_freq_base': round(combo['full']['annual'] - ref_v1, 6),
            'vs_v1': round(combo['full']['annual'] - V1['annual'], 6),
            'vs_v2': round(combo['full']['annual'] - V2['annual'], 6),
            'd_v1_seg1': round(combo['seg1']['annual'] - V1['seg1'], 6),
            'd_v1_seg2': round(combo['seg2']['annual'] - V1['seg2'], 6),
        }
        if (freq, 'base') in results:
            b = results[(freq, 'base')]
            d['d_base_seg1'] = round(combo['seg1']['annual'] - b['seg1']['annual'], 6)
            d['d_base_seg2'] = round(combo['seg2']['annual'] - b['seg2']['annual'], 6)
            d['base'] = {k: b['full'][k] for k in
                         ('annual', 'max_drawdown', 'turnover_two_way',
                          'cost_share_of_gross')}
        d['both_pos_v1'] = d['d_v1_seg1'] > 0 and d['d_v1_seg2'] > 0
        d['turn_ok'] = d['turn'] <= 2.5
        d['pass_v1'] = (d['vs_v1'] >= 0.005 and d['both_pos_v1'] and d['turn_ok'])
        deltas[freq] = d
    print('DELTA ' + json.dumps(deltas, ensure_ascii=False, indent=2), flush=True)

    years_empty = sorted(y for y, cs in st_by_year.items() if max(cs) == 0)
    st_cov = {
        'codes_with_status': len(st_status),
        'universe_codes': len(data),
        'months': len(st_counts),
        'count_median': int(np.median(st_counts)),
        'count_min': int(min(st_counts)),
        'count_max': int(max(st_counts)),
        'share_median': round(float(np.median(st_ratios)), 4),
        'share_min': round(float(min(st_ratios)), 4),
        'share_max': round(float(max(st_ratios)), 4),
        'years_empty': years_empty,
        'year_median': {str(y): int(np.median(cs)) for y, cs in sorted(st_by_year.items())},
    }
    print('STCOV ' + json.dumps(st_cov, ensure_ascii=False), flush=True)
    print('JACCARD_VOL_NEW {:.4f}'.format(float(np.mean(jac_vol_new))), flush=True)
    print('ELAPSED {:.1f}s'.format(time.perf_counter() - t0), flush=True)


if __name__ == '__main__':
    main()
