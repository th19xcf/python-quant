#!/usr/bin/env python3
"""低波两对象的调仓频率敏感性：月频 / 季频 / 半年频。

对象两个，均复用 ``factor_lowvol_quantile`` 与 ``factor_lowvol_variants`` 的
同一份因子值与两腿再平衡实现：

- Q1 低波多头：横截面 60 日波动率升序，最低 20% 等权；
- v2 剔除式：剔除波动率最高 20%，其余等权（未排名的保留）。

频率三档：月末（月频）、季末（3/6/9/12 月）、半年末（6/12 月）。唯一变化
是再平衡日历，其余口径（T 日收盘信号 / T+1 开盘成交、涨跌停停牌、成本、
复权、本金 10 亿、1% 现金缓冲）一律不动。

数据从 2009-08-01 读入作为预热，保证三档频率在期初都已建仓；指标统一切
2010-01-04~2026-11-13（与 factor_lowvol_checks 的 full 段一致）。月频的
v2 与全市场等权基线已冻结/已测（8.466% / 7.287%），本脚本直接跳过不重跑。

用法
----
    .venv\\Scripts\\python.exe tools/factor_lowvol_freq.py
"""
from __future__ import annotations

import gc
import json
import sys
import time
from datetime import date
from pathlib import Path
from typing import Dict, List

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
for _p in (ROOT, ROOT / 'tools'):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from loguru import logger

import factor_lowvol_checks as c
import factor_lowvol_quantile as f
import factor_lowvol_variants as v

WARMUP_START = '2009-08-01'
FIRST_SIGNAL = date(2009, 9, 1)
WIN = c.SEG1[0], c.SEG2[1]

FREQS: Dict[str, List[int]] = {
    'monthly': list(range(1, 13)),
    'quarterly': [3, 6, 9, 12],
    'semi': [6, 12],
}

# 月频的 v2 与全市场等权已有同口径结果，不重跑
SKIP = {('monthly', 'v2'), ('monthly', 'base')}


def rebalance_days(month_ends: List[date], months: List[int]) -> List[date]:
    return [d for d in month_ends if d.month in months and d >= FIRST_SIGNAL]


def main() -> None:
    t0 = time.perf_counter()
    data = f.base.load_universe(WARMUP_START, f.base.END)
    data, _ = f.base.drop_contract_violations(data)
    calendar, month_ends, prev_day = f.build_calendar(data)
    dividends = f._load_local_dividends(list(data), None, None)
    st_status = f._load_st_status(None, data, None, WARMUP_START,
                                  f.base.END, None, None)

    for freq, months in FREQS.items():
        days = rebalance_days(month_ends, months)
        panel = f.compute_vol_panel(data, days)
        members, _short = f.quantile_members(panel)
        quoted = v.quoted_sets(data, days)
        months_ok = [m for m in days if m in members]
        logger.info('{}：调仓日 {} 个，有效 {} 个（{}~{}）', freq, len(days),
                    len(months_ok), months_ok[0].isoformat(),
                    months_ok[-1].isoformat())

        paths = {
            'q1': {m: list(members[m][0]) for m in months_ok},
            'v2': v.variant_exclude(panel, members, quoted, months_ok),
            'base': {m: sorted(quoted[m]) for m in months_ok},
        }
        for tag in ('base', 'q1', 'v2'):
            if (freq, tag) in SKIP:
                continue
            path = paths[tag]
            sizes = [len(x) for x in path.values()]
            logger.info('{} {}：成分中位 {}', freq, tag, int(np.median(sizes)))
            sched = v.schedules_from_path(path, prev_day)
            result = f.run_portfolio(data, sched, dividends, st_status)
            item = {
                'freq': freq,
                'obj': tag,
                'rebalances': len(months_ok),
                'members_median': int(np.median(sizes)),
                'members_min': int(min(sizes)),
                **c.seg_metrics(result, *WIN),
            }
            print('RUN ' + json.dumps(item, ensure_ascii=False), flush=True)
            del result, sched, path, sizes
            gc.collect()
        del panel, members, quoted, paths
        gc.collect()

    print('ELAPSED {:.1f}s'.format(time.perf_counter() - t0), flush=True)


if __name__ == '__main__':
    main()
