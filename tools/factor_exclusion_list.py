#!/usr/bin/env python3
"""剔除清单实验：对 v1（全市场等权、月频）逐个施加单一剔除条件。

条件（每次只加一条，除组合项外）：
1. 剔除 60 日波动率最高 20%（= 已冻结 v2，直接引用，不在本脚本重跑）；
2. 剔除信号日处于 ST / 退市风险状态的标的（st_status 区间，按当日状态）；
3. 剔除上市不满 250 个交易日（stock_basic.list_date + 交易日历）；
4. 剔除 20 日平均成交额最低 20%（成交额取 amount，不足 20 日不参与排序）；
5. 剔除 PE 为负或高于截面 95 分位（估值表 valuation_daily.pe，取 T 日或之前最近值）；
6. 条件 2 + 3 组合。

口径与 v1 完全一致：月末月频、T 日收盘信号 / T+1 开盘成交、1% 现金缓冲、
10bp 滑点、佣金万 2.5、印花税固定千 1、涨跌停停牌与复权不动、本金 10 亿；
数据预热 2009-08，指标切 2010-01-04~2026-11-13。另报两项重叠度（按月
Jaccard 平均）。

用法
----
    .venv\\Scripts\\python.exe tools/factor_exclusion_list.py
"""
from __future__ import annotations

import gc
import json
import math
import sys
import time
from bisect import bisect_right
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import polars as pl

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
CONDS = ('st', 'new', 'liq', 'pe', 'st_new')
MIN_HISTORY = 250
LIQ_WINDOW = 20
VALUATION = ROOT / 'data/local_store/meta/financials/valuation_daily.parquet'
STOCK_BASIC = ROOT / 'data/local_store/meta/stock_basic.parquet'


def st_at(entry: Any, day: str) -> bool:
    """按区间列表判定信号日是否 ST（区间外按非 ST 处理）。"""
    if not entry:
        return False
    if isinstance(entry, bool):
        return entry
    for seg in entry:
        if seg['start_date'] <= day <= seg['end_date']:
            return bool(seg['is_st'])
    return False


def np_dates(df: pl.DataFrame, col: str = 'date') -> np.ndarray:
    return np.asarray(df[col].to_numpy(), dtype='datetime64[D]')


def last_index(arr: np.ndarray, day: date) -> int:
    """arr 升序，返回 <= day 的最后一个位置（无则 -1）。"""
    return int(np.searchsorted(arr, np.datetime64(day), side='right')) - 1


def load_list_dates(data: Dict[str, pl.DataFrame]) -> Dict[str, date]:
    basic = pl.read_parquet(STOCK_BASIC, columns=['ts_code', 'list_date'])
    out: Dict[str, date] = {}
    for code, ld in basic.filter(pl.col('list_date').is_not_null()).select(
            ['ts_code', 'list_date']).iter_rows():
        out[code] = ld
    return out


def load_pe_arrays(data: Dict[str, pl.DataFrame]) -> Dict[str, Tuple[np.ndarray, np.ndarray]]:
    df = pl.scan_parquet(VALUATION).select(['ts_code', 'trade_date', 'pe']).filter(
        (pl.col('trade_date') >= date(2009, 8, 1)) & pl.col('pe').is_not_null()).collect()
    df = df.sort(['ts_code', 'trade_date'])
    codes = df['ts_code'].to_numpy()
    starts = np.flatnonzero(np.r_[True, codes[1:] != codes[:-1]])
    ends = np.r_[starts[1:], len(codes)]
    out: Dict[str, Tuple[np.ndarray, np.ndarray]] = {}
    dts = df['trade_date'].to_numpy()
    pes = df['pe'].to_numpy()
    for s, e in zip(starts, ends):
        out[str(codes[s])] = (dts[s:e], pes[s:e])
    logger.info('估值表：{} 只 / {} 行', len(out), len(codes))
    return out


def load_trading_days() -> List[date]:
    """全历史 A 股交易日（估值表日期即交易日），用于上市天数计数。"""
    df = pl.scan_parquet(VALUATION).select('trade_date').unique().collect()
    return sorted(df['trade_date'].to_list())


def main() -> None:
    t0 = time.perf_counter()
    data = f.base.load_universe(WARMUP_START, f.base.END)
    data, _ = f.base.drop_contract_violations(data)
    calendar, month_ends, prev_day = f.build_calendar(data)
    months = [m for m in month_ends if m >= FIRST_SIGNAL]
    logger.info('月末 {} 个（{}~{}）', len(months), months[0], months[-1])

    panel = f.compute_vol_panel(data, month_ends)
    members, _ = f.quantile_members(panel)
    quoted = v.quoted_sets(data, month_ends)
    dividends = f._load_local_dividends(list(data), None, None)
    st_status = f._load_st_status(None, data, None, WARMUP_START, f.base.END, None, None) or {}
    list_dates = load_list_dates(data)
    pe_arrays = load_pe_arrays(data)

    all_days = load_trading_days()
    amount_arrays: Dict[str, Tuple[np.ndarray, np.ndarray]] = {}
    for code, df in data.items():
        if 'amount' not in df.columns:
            continue
        amount_arrays[code] = (np_dates(df), df['amount'].to_numpy())

    months_ok = [m for m in months if m in quoted]
    excl: Dict[str, Dict[date, set]] = {k: {} for k in ('vol',) + CONDS}
    pe_missing: List[int] = []
    liq_missing: List[int] = []
    for m in months_ok:
        q = set(quoted[m])
        qlist = sorted(q)
        # 1) 波动率最高 20%
        q5 = members.get(m, (None,) * 5)[4]
        excl['vol'][m] = set(q5 or ()) & q
        # 2) ST
        day = m.isoformat()
        st_set = {code for code in qlist if st_at(st_status.get(code), day)}
        excl['st'][m] = st_set
        # 3) 上市不满 250 个交易日（缺 list_date 用面板首日，按全历史交易日计数）
        idx_m = bisect_right(all_days, m)
        new_set = set()
        for code in qlist:
            ld = list_dates.get(code)
            if ld is None:
                arr = np_dates(data[code])
                ld = date.fromisoformat(str(arr[0]))
            if idx_m - bisect_right(all_days, ld) < MIN_HISTORY:
                new_set.add(code)
        excl['new'][m] = new_set
        # 4) 20 日平均成交额最低 20%
        amt: List[Tuple[str, float]] = []
        for code in qlist:
            pair = amount_arrays.get(code)
            if pair is None:
                continue
            dts, vals = pair
            k = last_index(dts, m)
            if k >= LIQ_WINDOW - 1:
                window = vals[k - LIQ_WINDOW + 1:k + 1]
                window = window[np.isfinite(window)]
                if len(window) == LIQ_WINDOW:
                    amt.append((code, float(window.mean())))
        liq_missing.append(len(qlist) - len(amt))
        amt.sort(key=lambda x: x[1])
        liq_set = {code for code, _ in amt[:int(len(amt) * 0.2)]}
        excl['liq'][m] = liq_set
        # 5) PE 为负或 > 截面 95 分位
        pe_vals: List[Tuple[str, float]] = []
        for code in qlist:
            pair = pe_arrays.get(code)
            if pair is None:
                continue
            dts, pes = pair
            k = last_index(dts, m)
            if k >= 0 and math.isfinite(pes[k]):
                pe_vals.append((code, float(pes[k])))
        pe_missing.append(len(qlist) - len(pe_vals))
        pos = np.array([x for _, x in pe_vals if x > 0])
        p95 = float(np.quantile(pos, 0.95)) if pos.size else math.inf
        excl['pe'][m] = {code for code, x in pe_vals if x < 0 or x > p95}
        # 6) ST + 次新
        excl['st_new'][m] = st_set | new_set
        if m == months_ok[-1]:
            logger.info('{}：剔 ST {}、次新 {}、低流动 {}、估值 {}（105 分位 {:.1f}）',
                        m, len(excl['st'][m]), len(excl['new'][m]),
                        len(excl['liq'][m]), len(excl['pe'][m]), p95)

    logger.info('月度均值：PE 缺失 {:.1f} 只，成交额不足 {:.1f} 只',
                np.mean(pe_missing), np.mean(liq_missing))

    # 重叠度：按月 Jaccard 平均
    keys = ('vol',) + CONDS
    jac: Dict[str, List[float]] = {}
    for i, a in enumerate(keys):
        for b in keys[i + 1:]:
            vals = []
            for m in months_ok:
                sa, sb = excl[a][m], excl[b][m]
                union = len(sa | sb)
                if union:
                    vals.append(len(sa & sb) / union)
            jac[f'{a}|{b}'] = round(float(np.mean(vals)), 4) if vals else None
    print('OVERLAP ' + json.dumps(jac, ensure_ascii=False), flush=True)

    def metrics(result) -> Dict[str, Any]:
        return {'full': c.seg_metrics(result, *WIN),
                'seg1': c.seg_metrics(result, *c.SEG1),
                'seg2': c.seg_metrics(result, *c.SEG2)}

    base_path = {m: sorted(quoted[m]) for m in months_ok}
    out: Dict[str, Any] = {}
    for tag in ('base',) + CONDS:
        path = base_path if tag == 'base' else {
            m: sorted(quoted[m] - excl[tag][m]) for m in months_ok}
        sizes = [len(x) for x in path.values()]
        sched = v.schedules_from_path(path, prev_day)
        result = f.run_portfolio(data, sched, dividends, st_status)
        item = {'members_median': int(np.median(sizes)), **metrics(result)}
        out[tag] = item
        logger.info('{} 完成（成分中位 {}）', tag, item['members_median'])
        print('RUN ' + json.dumps({'tag': tag, **item}, ensure_ascii=False), flush=True)
        del result, sched, path
        gc.collect()

    b = out['base']
    delta: Dict[str, Any] = {}
    for tag in CONDS:
        r = out[tag]
        delta[tag] = {
            'full_annual': round(r['full']['annual'] - b['full']['annual'], 6),
            'seg1': round(r['seg1']['annual'] - b['seg1']['annual'], 6),
            'seg2': round(r['seg2']['annual'] - b['seg2']['annual'], 6),
        }
        delta[tag]['both_pos'] = delta[tag]['seg1'] > 0 and delta[tag]['seg2'] > 0
        delta[tag]['turn_over_2p5'] = r['full']['turnover_two_way'] > 2.5
        delta[tag]['pass'] = (delta[tag]['full_annual'] >= 0.005
                              and delta[tag]['both_pos']
                              and not delta[tag]['turn_over_2p5'])
    print('DELTA ' + json.dumps(delta, ensure_ascii=False, indent=2), flush=True)
    print('ELAPSED {:.1f}s'.format(time.perf_counter() - t0), flush=True)


if __name__ == '__main__':
    main()
