#!/usr/bin/env python3
"""估值因子 EP（1/PE_TTM）与 BP（1/PB）的分层与剔除检验。

前置检查见 ``tools/factor_value_precheck.py``（覆盖率、负值单列、重复暴露），
本脚本只在前置检查通过后运行。口径与已冻结的价量估值等权基线完全一致：
hfq 出信号 / 原始价成交与盯市 / 分红入池；月末前一交易日发卖腿、月末收盘发
买腿、T+1 开盘成交；涨停不可买、跌停不可卖、停牌不参与调仓；佣金万 2.5 双边
（最低 5 元）、卖出印花税固定千 1、滑点 10bp、冲击 0.015×sqrt(参与率)；本金
10 亿、权重合计 0.99（1% 现金缓冲）、pooled_cash=True。

因子口径
--------
- EP = 1 / PE_TTM，BP = 1 / PB（valuation_daily.pe 为 PE_TTM、pb 为 PB_MRQ），
  按 trade_date 向后 as-of 取信号日 T 或之前最近一条，不做任何填补；
- PE<=0 / PB<=0 单列独立组 ``loss``，不参与分位排序，不当最便宜一档；
- 正因子截面按值升序分五档：Q1 最低（EP 最低=最贵 / BP 最低=最贵），
  Q5 最高（EP 最高=最便宜 / BP 最高=最便宜）；
- 多头端 = Q5（最便宜 20%）；剔除端 = 全截面剔除 Q1（最贵 20%），未参与排序
  的标的（亏损组与估值缺失）按 v2 的「未排名保留」惯例留在大池里；
- ``base_pos`` = 正因子截面等权（Q1~Q5 合并），用于把「丢掉亏损组」的影响与
  「剔除最贵 20%」的影响分开；
- ``base`` = 当日有行情标的全等权（同频基准，用于增量门槛判定）。

频率：月频（每月末）与半年频（6/12 月末），只此两档。

用法
----
    .venv\\Scripts\\python.exe tools\\factor_value_quantile.py
    .venv\\Scripts\\python.exe tools\\factor_value_quantile.py --factors ep --freqs monthly --objects base,q5
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
from typing import Any, Dict, List, Set, Tuple

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
SEG1 = c.SEG1
SEG2 = c.SEG2
WIN = c.SEG1[0], c.SEG2[1]
QUANTILES = 5
MIN_NAMES = 50
FREQS: Dict[str, List[int]] = {'monthly': list(range(1, 13)), 'semi': [6, 12]}
VALUATION = ROOT / 'data/local_store/meta/financials/valuation_daily.parquet'
BASELINE_JSON = ROOT / 'docs/基线_价量估值等权_2026-11-13.json'


def load_valuation() -> Dict[str, Tuple[np.ndarray, np.ndarray, np.ndarray]]:
    df = pl.scan_parquet(VALUATION).select(
        ['ts_code', 'trade_date', 'pe', 'pb']).filter(
        (pl.col('trade_date') >= date(2008, 1, 1))
        & (pl.col('trade_date') <= date.fromisoformat(f.base.END))
        & (pl.col('pe').is_not_null() | pl.col('pb').is_not_null())).collect()
    df = df.sort(['ts_code', 'trade_date'])
    codes = df['ts_code'].to_numpy()
    starts = np.flatnonzero(np.r_[True, codes[1:] != codes[:-1]])
    ends = np.r_[starts[1:], len(codes)]
    dts = df['trade_date'].to_numpy()
    pe = df['pe'].to_numpy().astype('float64')
    pb = df['pb'].to_numpy().astype('float64')
    out: Dict[str, Tuple[np.ndarray, np.ndarray, np.ndarray]] = {}
    for a, b in zip(starts, ends):
        out[str(codes[a])] = (dts[a:b], pe[a:b], pb[a:b])
    logger.info('估值表：{} 只 / {} 行', len(out), df.height)
    return out


def build_factor_panels(val: Dict[str, Tuple[np.ndarray, np.ndarray, np.ndarray]],
                        quoted: Dict[date, Set[str]],
                        days: List[date]) -> Dict[str, Dict[date, Any]]:
    """各调仓日的正因子截面（EP/BP）与亏损组（PE<=0 / PB<=0）。"""
    ep: Dict[date, Dict[str, float]] = {}
    bp: Dict[date, Dict[str, float]] = {}
    ep_loss: Dict[date, Set[str]] = {}
    bp_loss: Dict[date, Set[str]] = {}
    for m in days:
        me = np.datetime64(m)
        epv: Dict[str, float] = {}
        bpv: Dict[str, float] = {}
        epl: Set[str] = set()
        bpl: Set[str] = set()
        for code in quoted[m]:
            pair = val.get(code)
            if pair is None:
                continue
            dts, pes, pbs = pair
            k = bisect_right(dts, me) - 1
            if k < 0:
                continue
            p_, b_ = pes[k], pbs[k]
            if np.isfinite(p_):
                if p_ > 0:
                    epv[code] = 1.0 / float(p_)
                else:
                    epl.add(code)
            if np.isfinite(b_):
                if b_ > 0:
                    bpv[code] = 1.0 / float(b_)
                else:
                    bpl.add(code)
        ep[m], bp[m], ep_loss[m], bp_loss[m] = epv, bpv, epl, bpl
    return {'ep': ep, 'bp': bp, 'ep_loss': ep_loss, 'bp_loss': bp_loss}


def ranked_buckets(vals: Dict[str, float], nq: int = QUANTILES
                   ) -> List[List[str]]:
    items = sorted(vals.items(), key=lambda kv: kv[1])
    n = len(items)
    if n < MIN_NAMES:
        return []
    buckets: List[List[str]] = [[] for _ in range(nq)]
    for i, (code, _) in enumerate(items):
        buckets[min(nq - 1, i * nq // n)].append(code)
    return buckets


def build_paths(quoted: Dict[date, Set[str]], days: List[date],
                vals: Dict[date, Dict[str, float]],
                losses: Dict[date, Set[str]]) -> Tuple[Dict[str, Dict[date, List[str]]],
                                                       List[date]]:
    """返回 {对象: {调仓日: 成分}}，只保留可有效排序的调仓日。"""
    paths: Dict[str, Dict[date, List[str]]] = {
        'base': {}, **{f'q{q + 1}': {} for q in range(QUANTILES)},
        'excl': {}, 'base_pos': {}, 'loss': {}}
    months_ok: List[date] = []
    for m in days:
        buckets = ranked_buckets(vals[m])
        if not buckets:
            continue
        months_ok.append(m)
        q = set(quoted[m])
        for i in range(QUANTILES):
            paths[f'q{i + 1}'][m] = list(buckets[i])
        paths['base'][m] = sorted(q)
        paths['base_pos'][m] = sorted(vals[m].keys() & q)
        paths['excl'][m] = sorted(q - set(buckets[0]))
        paths['loss'][m] = sorted(losses[m] & q)
    return paths, months_ok


def run_path(data, path: Dict[date, List[str]], prev_day, dividends, st_status):
    sched = v.schedules_from_path(path, prev_day)
    return f.run_portfolio(data, sched, dividends, st_status)


def summarize(result, members: List[int]) -> Dict[str, Any]:
    return {
        'rebalances': len(members),
        'members_median': int(np.median(members)) if members else 0,
        'members_min': int(min(members)) if members else 0,
        'full': c.seg_metrics(result, *WIN),
        'seg1': c.seg_metrics(result, *SEG1),
        'seg2': c.seg_metrics(result, *SEG2),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--factors', default='ep,bp')
    parser.add_argument('--freqs', default='monthly,semi')
    parser.add_argument('--objects', default='')
    args = parser.parse_args()
    factors = [x.strip() for x in args.factors.split(',') if x.strip()]
    freqs = [x.strip() for x in args.freqs.split(',') if x.strip()]
    only = {x.strip() for x in args.objects.split(',') if x.strip()}

    t0 = time.perf_counter()
    data = f.base.load_universe(WARMUP_START, f.base.END)
    data, _ = f.base.drop_contract_violations(data)
    calendar, month_ends, prev_day = f.build_calendar(data)
    quoted = v.quoted_sets(data, month_ends)
    val = load_valuation()
    dividends = f._load_local_dividends(list(data), None, None)
    st_status = f._load_st_status(None, data, None, WARMUP_START,
                                 f.base.END, None, None) or {}

    out: Dict[str, Any] = {}
    for freq in freqs:
        days = [m for m in month_ends
                if m.month in FREQS[freq] and m >= FIRST_SIGNAL]
        panels = build_factor_panels(val, quoted, days)
        for fac in factors:
            paths, months_ok = build_paths(
                quoted, days, panels[fac], panels[f'{fac}_loss'])
            logger.info('{} {}：调仓日 {} / 有效 {}（{}~{}）', fac, freq,
                        len(days), len(months_ok),
                        months_ok[0].isoformat(), months_ok[-1].isoformat())
            tag_prefix = f'{fac}|{freq}'
            keys = ['base', 'q1', 'q2', 'q3', 'q4', 'q5', 'excl', 'base_pos']
            if freq == 'monthly':
                keys.append('loss')
            if only:
                keys = [k for k in keys if k in only]
            for obj in keys:
                path = paths[obj]
                members = [len(path[m]) for m in sorted(path)]
                result = run_path(data, path, prev_day, dividends, st_status)
                item = summarize(result, members)
                item['object'] = obj
                item['factor'] = fac
                item['freq'] = freq
                out[f'{tag_prefix}|{obj}'] = item
                print('RUN ' + json.dumps(item, ensure_ascii=False), flush=True)
                del result
                gc.collect()
        del panels
        gc.collect()

    # 增量与门槛判定：同因子同频 base 为基准
    delta: Dict[str, Any] = {}
    for key, item in out.items():
        fac, freq, obj = key.split('|')
        if obj == 'base':
            continue
        base = out.get(f'{fac}|{freq}|base')
        if not base:
            continue
        d = {
            'full_annual_delta': round(item['full']['annual'] - base['full']['annual'], 6),
            'seg1_annual_delta': round(item['seg1']['annual'] - base['seg1']['annual'], 6),
            'seg2_annual_delta': round(item['seg2']['annual'] - base['seg2']['annual'], 6),
            'annual': item['full']['annual'],
            'max_drawdown': item['full']['max_drawdown'],
            'turnover_two_way': item['full']['turnover_two_way'],
            'cost_share_of_gross': item['full']['cost_share_of_gross'],
        }
        d['both_pos'] = d['seg1_annual_delta'] > 0 and d['seg2_annual_delta'] > 0
        d['turn_ok'] = d['turnover_two_way'] <= 2.5
        d['pass'] = (d['full_annual_delta'] >= 0.005 and d['both_pos']
                     and d['turn_ok'])
        delta[key] = d
    print('DELTA ' + json.dumps(delta, ensure_ascii=False, indent=2), flush=True)
    if BASELINE_JSON.exists():
        frozen = json.loads(BASELINE_JSON.read_text(encoding='utf-8'))
        print('FROZEN_V1 ' + json.dumps({
            'annual': frozen.get('metrics_10bp', {}).get('annual'),
            'max_drawdown': frozen.get('metrics_10bp', {}).get('max_drawdown'),
            'turnover_two_way': frozen.get('metrics_10bp', {}).get('turnover_two_way'),
        }, ensure_ascii=False), flush=True)
    print('ELAPSED {:.1f}s'.format(time.perf_counter() - t0), flush=True)


if __name__ == '__main__':
    main()
