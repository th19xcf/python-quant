#!/usr/bin/env python3
"""把 v1/v2/v3 三个冻结组合的日净值落盘为 CSV（只读重跑，不改任何参数）。

三版各自沿用其冻结时的构造脚本，逐字复刻：

- v1：``tools/baseline_pv_val_equal_weight.py`` 全市场等权月频（2010-01 起）；
- v2：``tools/factor_lowvol_checks.py`` 每月剔 60 日波动率最高 20%；
- v3：``tools/factor_exclusion_combo.py`` 月频剔高波 20% + 剔上市不满 250
  交易日次新（2009-08 预热，指标窗口 2010-01-04~2026-11-13）。

每版单独一个进程（``--case``），避免三份全市场面板同时驻留内存。输出两列
``date,equity``，净值按各自统计窗口首日归一为 10 亿（只做比例平移，不改变
年化、回撤与换手）。

用法
----
    .venv\\Scripts\\python.exe tools/factor_equity_dump.py --case v1
    .venv\\Scripts\\python.exe tools/factor_equity_dump.py --case v2
    .venv\\Scripts\\python.exe tools/factor_equity_dump.py --case v3
"""
from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from bisect import bisect_right
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
for _p in (ROOT, ROOT / 'tools'):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from loguru import logger

import baseline_pv_val_equal_weight as base
import factor_exclusion_combo as x
import factor_exclusion_list as e
import factor_lowvol_checks as c
import factor_lowvol_quantile as f
import factor_lowvol_variants as v

WARMUP_START = '2009-08-01'
FIRST_SIGNAL = date(2009, 9, 1)
WIN = c.SEG1[0], c.SEG2[1]
CAPITAL = 1e9
OUT_DIR = ROOT / '_staging'
NAMES = {
    'v1': 'equity_v1_pv_val_equal_weight_2026-11-14.csv',
    'v2': 'equity_v2_excl_high_vol_2026-11-14.csv',
    'v3': 'equity_v3_excl_vol_new_2026-11-14.csv',
}
FROZEN = {'v1': 0.0728243, 'v2': 0.08466, 'v3': 0.091312}


def write_csv(path: Path, curve: List[Tuple[date, float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8', newline='') as fh:
        fh.write('date,equity\n')
        for day, eq in curve:
            fh.write(f'{day.isoformat()},{eq:.2f}\n')


def run_v1() -> Tuple[Dict[str, Any], float, float]:
    data = base.load_universe(base.START, base.END)
    data, _ = base.drop_contract_violations(data)
    schedule = base.build_schedule(data)
    dividends = f._load_local_dividends(list(data), None, None)
    st_status = f._load_st_status(None, data, None, base.START, base.END, None, None)
    result, summary = base.run_one(
        data, base.MonthlyTargetWeightStrategy(schedule), 10.0, 0.015,
        dividends, st_status)
    return result, summary['annual'], summary['max_drawdown']


def run_v2() -> Tuple[Dict[str, Any], float, float]:
    # 冻结值来自 factor_lowvol_checks.py：2009-08 预热、指标窗口 2010-01-04 起
    data = f.base.load_universe(WARMUP_START, f.base.END)
    data, _ = f.base.drop_contract_violations(data)
    _calendar, month_ends, prev_day = f.build_calendar(data)
    panel = f.compute_vol_panel(data, month_ends)
    members, _short = f.quantile_members(panel)
    quoted = v.quoted_sets(data, month_ends)
    months = [m for m in month_ends if m >= FIRST_SIGNAL]
    dividends = f._load_local_dividends(list(data), None, None)
    st_status = f._load_st_status(None, data, None, WARMUP_START,
                                  f.base.END, None, None)
    path = v.variant_exclude(panel, members, quoted, months)
    sched = v.schedules_from_path(path, prev_day)
    result = f.run_portfolio(data, sched, dividends, st_status)
    m = c.seg_metrics(result, *WIN)
    return result, m['annual'], m['max_drawdown']


def run_v3() -> Tuple[Dict[str, Any], float, float]:
    data = f.base.load_universe(WARMUP_START, f.base.END)
    data, _ = f.base.drop_contract_violations(data)
    _calendar, month_ends, prev_day = f.build_calendar(data)
    days = [d for d in month_ends if d >= FIRST_SIGNAL]
    panel = f.compute_vol_panel(data, days)
    members, _short = f.quantile_members(panel)
    quoted = v.quoted_sets(data, days)
    months_ok = [m for m in days if m in members and m in quoted]
    list_dates = e.load_list_dates(data)
    all_days = e.load_trading_days()
    dividends = f._load_local_dividends(list(data), None, None)
    st_status = f._load_st_status(None, data, None, WARMUP_START,
                                  f.base.END, None, None)
    path: Dict[date, List[str]] = {}
    for m in months_ok:
        q = set(quoted[m])
        vol = set(members[m][4] or ()) & q
        new = x.new_listing_set(data, sorted(q), list_dates, all_days, m)
        path[m] = sorted(q - vol - new)
    sched = v.schedules_from_path(path, prev_day)
    result = f.run_portfolio(data, sched, dividends, st_status)
    m = c.seg_metrics(result, *WIN)
    return result, m['annual'], m['max_drawdown']


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--case', required=True, choices=['v1', 'v2', 'v3'])
    args = parser.parse_args()
    t0 = time.perf_counter()

    result, annual, drawdown = {'v1': run_v1, 'v2': run_v2, 'v3': run_v3}[args.case]()

    curve = f.curve_of(result)
    if args.case in ('v2', 'v3'):
        curve = [(d, e) for d, e in curve if WIN[0] <= d <= WIN[1]]
    raw_first = float(curve[0][1])
    scale = CAPITAL / raw_first
    curve = [(d, e * scale) for d, e in curve]
    out = OUT_DIR / NAMES[args.case]
    write_csv(out, curve)
    years = (curve[-1][0] - curve[0][0]).days / 365.25
    dumped_annual = (curve[-1][1] / curve[0][1]) ** (1.0 / years) - 1.0
    print('DUMP ' + json.dumps({
        'case': args.case,
        'path': str(out.relative_to(ROOT)),
        'rows': len(curve),
        'start': curve[0][0].isoformat(),
        'end': curve[-1][0].isoformat(),
        'raw_first_equity': round(raw_first, 2),
        'rebase_scale': round(scale, 8),
        'annual': round(annual, 6),
        'annual_from_csv': round(dumped_annual, 6),
        'frozen': FROZEN[args.case],
        'diff_pp': round((annual - FROZEN[args.case]) * 100, 4),
        'max_drawdown': round(drawdown, 6),
        'bytes': out.stat().st_size,
    }, ensure_ascii=False), flush=True)
    print('ELAPSED {:.1f}s'.format(time.perf_counter() - t0), flush=True)
    del result
    gc.collect()


if __name__ == '__main__':
    main()
