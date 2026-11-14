#!/usr/bin/env python3
"""叠加剔除检验：剔 BP 最贵 20% + 剔 60 日波动率最高 20% + 剔上市不满 250 交易日。

月频与半年频各一次，档内等权；成本、复权、成交腿、现金缓冲、股票池全部沿用
v3 与既有基线，不改任何参数。同一脚本内重跑 v3（剔高波 + 剔次新）作为对照，
使增量与分段增量在同一日历、同一份数据上对齐。

BP 最贵 20% = 正 BP 截面按值升序的第一档（BP 最低 = 最贵），与
``tools/factor_value_quantile.py`` 的 Q1 同一构造；PB<=0 不参与排序。

用法
----
    .venv\\Scripts\\python.exe tools\\factor_value_excl_combo.py
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
import factor_value_quantile as fv

WARMUP_START = '2009-08-01'
FIRST_SIGNAL = date(2009, 9, 1)
WIN = c.SEG1[0], c.SEG2[1]
FREQS: Dict[str, List[int]] = {'monthly': list(range(1, 13)), 'semi': [6, 12]}
DOC = ROOT / 'docs/估值因子_EP_BP_分层与剔除_2026-11-14.json'


def new_listing_set(data, quoted_day: List[str], list_dates, all_days,
                    day: date) -> set:
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
    val = fv.load_valuation()

    out: Dict[str, Any] = {}
    for freq, months in FREQS.items():
        days = [d for d in month_ends if d.month in months and d >= FIRST_SIGNAL]
        quoted = v.quoted_sets(data, days)
        panel = f.compute_vol_panel(data, days)
        members, _short = f.quantile_members(panel)
        bpp = fv.build_factor_panels(val, quoted, days)['bp']
        paths: Dict[str, Dict[date, List[str]]] = {'v3': {}, 'combo': {}}
        drops: List[int] = []
        for m in days:
            if m not in members or m not in quoted:
                continue
            q = set(quoted[m])
            hv = set(members[m][4] or ()) & q
            new = new_listing_set(data, sorted(q), list_dates, all_days, m)
            buckets = fv.ranked_buckets(bpp[m])
            if not buckets:
                continue
            exp = set(buckets[0]) & q
            v3set = q - hv - new
            if not v3set or not (v3set - exp):
                continue
            paths['v3'][m] = sorted(v3set)
            paths['combo'][m] = sorted(v3set - exp)
            drops.append(len(v3set & exp))
        logger.info('{}：有效调仓 {} 个，叠加端额外剔除 BP 最贵中位 {} 只', freq,
                    len(paths['combo']), int(np.median(drops)) if drops else 0)
        for tag in ('v3', 'combo'):
            path = paths[tag]
            sched = v.schedules_from_path(path, prev_day)
            result = f.run_portfolio(data, sched, dividends, st_status)
            item = {
                'freq': freq, 'object': tag,
                'rebalances': len(path),
                'members_median': int(np.median([len(x) for x in path.values()])),
                'full': c.seg_metrics(result, *WIN),
                'seg1': c.seg_metrics(result, *c.SEG1),
                'seg2': c.seg_metrics(result, *c.SEG2),
            }
            out[f'{freq}|{tag}'] = item
            print('RUN ' + json.dumps(item, ensure_ascii=False), flush=True)
            del result, sched, path
            gc.collect()
        out[f'{freq}|dropped_median'] = int(np.median(drops)) if drops else 0
        del quoted, panel, members, bpp, paths
        gc.collect()

    V1 = {'annual': 0.072873, 'seg1': 0.138803, 'seg2': 0.027529}
    V2 = {'annual': 0.084660, 'seg1': 0.144983, 'seg2': 0.042937}
    V3 = {'annual': 0.091312}
    BP_EXCL_M = 0.081440
    deltas: Dict[str, Any] = {}
    for freq in FREQS:
        combo, v3 = out[f'{freq}|combo'], out[f'{freq}|v3']
        d = {
            'annual': combo['full']['annual'],
            'max_drawdown': combo['full']['max_drawdown'],
            'turnover_two_way': combo['full']['turnover_two_way'],
            'cost_share_of_gross': combo['full']['cost_share_of_gross'],
            'vs_v3_full': round(combo['full']['annual'] - v3['full']['annual'], 6),
            'vs_v3_seg1': round(combo['seg1']['annual'] - v3['seg1']['annual'], 6),
            'vs_v3_seg2': round(combo['seg2']['annual'] - v3['seg2']['annual'], 6),
            'vs_v1': round(combo['full']['annual'] - V1['annual'], 6),
            'vs_v2': round(combo['full']['annual'] - V2['annual'], 6),
            'vs_v3_frozen': round(combo['full']['annual'] - V3['annual'], 6),
            'v3_same_run': {k: v3['full'][k] for k in
                            ('annual', 'max_drawdown', 'turnover_two_way',
                             'cost_share_of_gross')},
            'v3_seg': [v3['seg1']['annual'], v3['seg2']['annual']],
            'dropped_from_v3_median': out[f'{freq}|dropped_median'],
        }
        if freq == 'monthly':
            d['vs_bp_only_excl'] = round(combo['full']['annual'] - BP_EXCL_M, 6)
        d['both_pos'] = d['vs_v3_seg1'] > 0 and d['vs_v3_seg2'] > 0
        d['turn_ok'] = d['turnover_two_way'] <= 2.5
        d['pass'] = (d['vs_v3_full'] >= 0.005 and d['both_pos'] and d['turn_ok'])
        deltas[freq] = d
    print('DELTA ' + json.dumps(deltas, ensure_ascii=False, indent=2), flush=True)

    doc = json.loads(DOC.read_text(encoding='utf-8'))
    doc['combo_bp_excl_v3_2026-11-14'] = {
        'def': '剔 BP 最贵 20%（正 BP 截面最低档）+ 剔 60 日波动率最高 20% + 剔上市不满 250 交易日，档内等权；其余口径同 v3',
        'command': '.venv\\\\Scripts\\\\python.exe tools\\\\factor_value_excl_combo.py',
        'script': 'tools/factor_value_excl_combo.py',
        'jaccard_bp_exp_vs_v3': 0.1171,
        'results': {k: out[k] for k in out},
        'deltas_vs_v3': deltas,
        'verdict': '',
    }
    DOC.write_text(json.dumps(doc, ensure_ascii=False, indent=2) + '\n',
                   encoding='utf-8')
    print('WROTE', DOC.name, DOC.stat().st_size, flush=True)
    print('ELAPSED {:.1f}s'.format(time.perf_counter() - t0), flush=True)


if __name__ == '__main__':
    main()
