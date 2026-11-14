#!/usr/bin/env python3
"""估值因子（EP=1/PE_TTM、BP=1/PB）前置检查：覆盖率、负值与异常值、重复暴露。

只读脚本：不写 ``data/``，不改引擎与成本默认值，不跑回测。口径与已冻结的
价量估值等权基线一致（同一标的池、同一交易日历、同一月末截面），因此：

- 标的池：``tools/baseline_pv_val_equal_weight.py`` 的 ``load_universe`` +
  ``drop_contract_violations``（hfq 出信号 / 原始价成交口径不变）；
- 交易日历：由行情面板取并集（与基线 ``build_schedule`` 同一口径）；
- 估值来源：``data/local_store/meta/financials/valuation_daily.parquet``，
  ``pe`` 即 PE_TTM（baostock peTTM / akshare PE(TTM) / local fin_ttm），
  ``pb`` 即 PB_MRQ；按 ``trade_date`` 向后 as-of 取 T 日或之前最近一条，
  不做行业均值、上一期值或任何填补；
- 波动率：20 日 hfq 对数收益标准差，窗口末端恰为信号日 T；
- 换手率：``volume × 100 / 流通股本``（缺自由流通股本回退流通股本）的
  20 日均值，窗口末端恰为信号日 T。

三项检查
--------
1) 覆盖率：PE_TTM / PB 在每个月末横截面（当日有行情的标的）里的可用只数、
   缺失只数、非正值只数，报中位/最小/最大；
2) 负值与异常值：PE<=0、PB<=0 单列为独立组（不参与分位排序）；极端值给出
   月度正截面 1%/99% winsorize 的做法与影响行数；
3) 重复暴露：EP/BP 与 20 日波动率、20 日均换手率的月度横截面 Spearman 均值；
   便宜 20% / 最贵 20% 与波动率最高 20% 等集合的月度 Jaccard 均值。

判据（用户口径）：|Spearman| > 0.5 或 Jaccard > 0.3 即判同一暴露，该项停止
回测，只留一行重复结论。

用法
----
    .venv\\Scripts\\python.exe tools\\factor_value_precheck.py
"""
from __future__ import annotations

import json
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

import factor_lowvol_quantile as f
import factor_lowvol_variants as v

WARMUP_START = '2009-08-01'
FIRST_SIGNAL = date(2009, 9, 1)
MIN_NAMES = 50
VOL_WINDOW = 20
TURN_WINDOW = 20
VALUATION = ROOT / 'data/local_store/meta/financials/valuation_daily.parquet'
SHARES = ROOT / 'data/local_store/meta/factors/stock_shares.parquet'


def vol_panel(data: Dict[str, pl.DataFrame], month_ends: List[date],
              window: int = VOL_WINDOW) -> Dict[date, Dict[str, float]]:
    """每只票在每个月末的 window 日 hfq 对数收益标准差（只用 T 日及之前）。"""
    me = np.array(month_ends, dtype='datetime64[D]')
    panel: Dict[date, Dict[str, float]] = {d: {} for d in month_ends}
    skipped = 0
    for code, df in data.items():
        dates = df['date'].to_numpy()
        close = df['close'].to_numpy().astype('float64')
        m = close.size
        if m <= window:
            skipped += 1
            continue
        with np.errstate(divide='ignore', invalid='ignore'):
            ret = np.diff(np.log(np.where(close > 0, close, np.nan)))
        bad = ~np.isfinite(ret)
        r = np.nan_to_num(ret, nan=0.0, posinf=0.0, neginf=0.0)
        cs1 = np.concatenate(([0.0], np.cumsum(r)))
        cs2 = np.concatenate(([0.0], np.cumsum(r * r)))
        csb = np.concatenate(([0], np.cumsum(bad)))
        raw = np.searchsorted(dates, me)
        idx = np.minimum(raw, m - 1)
        hit = np.nonzero((raw < m) & (dates[idx] == me))[0]
        for j in hit:
            k = int(idx[j])
            a = k - window
            if a < 0 or csb[k] - csb[a] != 0:
                continue
            n = window
            mean = (cs1[k] - cs1[a]) / n
            var = (cs2[k] - cs2[a] - n * mean * mean) / (n - 1)
            if var <= 0:
                continue
            panel[month_ends[j]][code] = float(np.sqrt(var))
    logger.info('{} 日波动率面板：跳过 {} 只（行数不足）', window, skipped)
    return panel


def load_share_arrays() -> Dict[str, Tuple[np.ndarray, np.ndarray]]:
    """流通股本（缺自由流通回退流通）按 effective_date 升序，供 as-of 取值。"""
    df = pl.read_parquet(SHARES).select(
        ['ts_code', 'effective_date', 'float_share', 'free_float_share']).sort(
        ['ts_code', 'effective_date'])
    df = df.with_columns(
        pl.coalesce([pl.col('free_float_share'), pl.col('float_share')]).alias('shares'))
    codes = df['ts_code'].to_numpy()
    starts = np.flatnonzero(np.r_[True, codes[1:] != codes[:-1]])
    ends = np.r_[starts[1:], len(codes)]
    ed = df['effective_date'].to_numpy()
    sv = df['shares'].to_numpy().astype('float64')
    out: Dict[str, Tuple[np.ndarray, np.ndarray]] = {}
    for a, b in zip(starts, ends):
        out[str(codes[a])] = (ed[a:b], sv[a:b])
    logger.info('股本表：{} 只', len(out))
    return out


def turnover_panel(data: Dict[str, pl.DataFrame], month_ends: List[date],
                   window: int = TURN_WINDOW) -> Dict[date, Dict[str, float]]:
    """每只票在每个月末的 window 日均换手率（volume×100/流通股本）。"""
    shares = load_share_arrays()
    me = np.array(month_ends, dtype='datetime64[D]')
    panel: Dict[date, Dict[str, float]] = {d: {} for d in month_ends}
    no_share = 0
    for code, df in data.items():
        pair = shares.get(code)
        if pair is None:
            no_share += 1
            continue
        sdates, sval = pair
        dates = df['date'].to_numpy()
        m = dates.size
        if m < window:
            continue
        pos = np.searchsorted(sdates, dates, side='right') - 1
        sv = np.where(pos >= 0, sval[np.maximum(pos, 0)], np.nan)
        vol = df['volume'].to_numpy().astype('float64') if 'volume' in df.columns \
            else np.full(m, np.nan)
        with np.errstate(divide='ignore', invalid='ignore'):
            turn = np.where((sv > 0) & (vol > 0), vol * 100.0 / sv, np.nan)
        raw = np.searchsorted(dates, me)
        idx = np.minimum(raw, m - 1)
        hit = np.nonzero((raw < m) & (dates[idx] == me))[0]
        for j in hit:
            k = int(idx[j])
            if k + 1 < window:
                continue
            win = turn[k - window + 1:k + 1]
            if not np.all(np.isfinite(win)):
                continue
            panel[month_ends[j]][code] = float(win.mean())
    logger.info('{} 日换手率面板：无股本 {} 只', window, no_share)
    return panel


def load_valuation() -> Dict[str, Tuple[np.ndarray, np.ndarray, np.ndarray]]:
    """估值表按票切分：trade_date 升序 + pe(TTM) + pb。"""
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
    logger.info('估值表：{} 只 / {} 行（2008 起）', len(out), df.height)
    return out


def rank_quantile_members(vals: Dict[str, float], nq: int = 5
                          ) -> Tuple[List[List[str]], Dict[str, int]]:
    """按值升序分 nq 档：档 0 最低、档 nq-1 最高。返回各档成员与名次字典。"""
    items = sorted(vals.items(), key=lambda kv: kv[1])
    n = len(items)
    if n < MIN_NAMES:
        return [], {}
    buckets: List[List[str]] = [[] for _ in range(nq)]
    rank: Dict[str, int] = {}
    for i, (code, _) in enumerate(items):
        q = min(nq - 1, i * nq // n)
        buckets[q].append(code)
        rank[code] = i
    return buckets, rank


def spearman(a: List[float], b: List[float]) -> float | None:
    if len(a) < 30:
        return None
    df = pl.DataFrame({'a': a, 'b': b})
    val = df.select(pl.corr('a', 'b', method='spearman')).item()
    return None if val is None or not np.isfinite(val) else float(val)


def jaccard(x: set, y: set) -> float | None:
    union = len(x | y)
    return len(x & y) / union if union else None


def stat3(series: List[float]) -> Dict[str, Any]:
    if not series:
        return {}
    arr = np.array(series, dtype='float64')
    return {'median': round(float(np.median(arr)), 4),
            'min': round(float(arr.min()), 4),
            'max': round(float(arr.max()), 4)}


def main() -> None:
    t0 = time.perf_counter()
    data = f.base.load_universe(WARMUP_START, f.base.END)
    data, _ = f.base.drop_contract_violations(data)
    calendar, month_ends, prev_day = f.build_calendar(data)
    months = [m for m in month_ends if m >= FIRST_SIGNAL]
    quoted = v.quoted_sets(data, month_ends)
    months_ok = [m for m in months if len(quoted[m]) >= MIN_NAMES]
    logger.info('月末 {} 个（{}~{}），有效 {} 个', len(months), months[0],
                months[-1], len(months_ok))

    vol = vol_panel(data, months_ok)
    turn = turnover_panel(data, months_ok)
    val = load_valuation()
    cal = np.array(calendar, dtype='datetime64[D]')

    cov: Dict[str, List[float]] = {k: [] for k in (
        'cross_section', 'pe_ok', 'pe_missing', 'pe_nonpos', 'pb_ok',
        'pb_missing', 'pb_nonpos', 'pe_stale_days', 'pb_stale_days')}
    wins: Dict[str, Any] = {'ep': {'clipped': 0, 'total': 0, 'p1': [], 'p99': [],
                                   'max': [], 'months': 0},
                            'bp': {'clipped': 0, 'total': 0, 'p1': [], 'p99': [],
                                   'max': [], 'months': 0}}
    sp: Dict[str, List[float]] = {k: [] for k in (
        'ep_pos_vol', 'ep_pos_turn', 'ep_all_vol', 'ep_all_turn',
        'bp_pos_vol', 'bp_pos_turn', 'bp_all_vol', 'bp_all_turn')}
    jac: Dict[str, List[float]] = {k: [] for k in (
        'ep_cheap_highvol', 'ep_low_highvol', 'bp_cheap_highvol',
        'bp_low_highvol', 'ep_cheap_highturn', 'bp_cheap_highturn')}
    stale_bad = 0

    for m in months_ok:
        names = sorted(quoted[m])
        pe_pos: Dict[str, float] = {}
        pb_pos: Dict[str, float] = {}
        ep_all: Dict[str, float] = {}
        bp_all: Dict[str, float] = {}
        pe_ok, pb_ok, pe_nonpos, pb_nonpos = 0, 0, 0, 0
        pe_stale, pb_stale = [], []
        im = bisect_right(calendar, m)
        for code in names:
            pair = val.get(code)
            if pair is None:
                continue
            dts, pes, pbs = pair
            k = bisect_right(dts, np.datetime64(m)) - 1
            if k < 0:
                continue
            p_, b_ = pes[k], pbs[k]
            if np.isfinite(p_):
                pe_ok += 1
                pe_stale.append(im - bisect_right(calendar, dts[k].item()))
                if p_ > 0:
                    pe_pos[code] = float(p_)
                    ep_all[code] = 1.0 / float(p_)
                else:
                    pe_nonpos += 1
                    ep_all[code] = float('nan')
            if np.isfinite(b_):
                pb_ok += 1
                pb_stale.append(im - bisect_right(calendar, dts[k].item()))
                if b_ > 0:
                    pb_pos[code] = float(b_)
                    bp_all[code] = 1.0 / float(b_)
                else:
                    pb_nonpos += 1
                    bp_all[code] = float('nan')
        cov['cross_section'].append(len(names))
        cov['pe_ok'].append(pe_ok)
        cov['pe_missing'].append(len(names) - pe_ok)
        cov['pe_nonpos'].append(pe_nonpos)
        cov['pb_ok'].append(pb_ok)
        cov['pb_missing'].append(len(names) - pb_ok)
        cov['pb_nonpos'].append(pb_nonpos)
        if pe_stale:
            cov['pe_stale_days'].append(float(np.median(pe_stale)))
        if pb_stale:
            cov['pb_stale_days'].append(float(np.median(pb_stale)))

        # 正截面极端值：1%/99% winsorize（只影响均值报告，不改变名次分档）
        for tag, pos in (('ep', pe_pos), ('bp', pb_pos)):
            arr = np.array([1.0 / x for x in pos.values()], dtype='float64')
            if arr.size < MIN_NAMES:
                continue
            p1, p99 = np.quantile(arr, [0.01, 0.99])
            w = wins[tag]
            w['clipped'] += int((arr < p1).sum() + (arr > p99).sum())
            w['total'] += int(arr.size)
            w['p1'].append(float(p1))
            w['p99'].append(float(p99))
            w['max'].append(float(arr.max()))
            w['months'] += 1

        # Spearman（正截面 / 含非正值两种口径）
        ep_pos_v = {c: 1.0 / x for c, x in pe_pos.items()}
        bp_pos_v = {c: 1.0 / x for c, x in pb_pos.items()}
        pairs = {
            'ep_pos_vol': (ep_pos_v, vol[m]), 'ep_pos_turn': (ep_pos_v, turn[m]),
            'bp_pos_vol': (bp_pos_v, vol[m]), 'bp_pos_turn': (bp_pos_v, turn[m]),
            'ep_all_vol': (ep_all, vol[m]), 'ep_all_turn': (ep_all, turn[m]),
            'bp_all_vol': (bp_all, vol[m]), 'bp_all_turn': (bp_all, turn[m]),
        }
        for key, (fa, fb) in pairs.items():
            common = [c for c in fa if c in fb
                      and np.isfinite(fa[c]) and np.isfinite(fb[c])]
            val_sp = spearman([fa[c] for c in common], [fb[c] for c in common])
            if val_sp is not None:
                sp[key].append(val_sp)

        # Jaccard：便宜/最贵 20% vs 波动率最高 20% / 换手率最高 20%
        vol_buckets, _ = rank_quantile_members(vol[m])
        turn_buckets, _ = rank_quantile_members(turn[m])
        ep_buckets, _ = rank_quantile_members(ep_pos_v)
        bp_buckets, _ = rank_quantile_members(bp_pos_v)
        if not vol_buckets or not ep_buckets or not bp_buckets:
            continue
        highvol, highturn = set(vol_buckets[4]), set(turn_buckets[4])
        for key, s in (('ep_cheap_highvol', set(ep_buckets[4])),
                       ('ep_low_highvol', set(ep_buckets[0])),
                       ('bp_cheap_highvol', set(bp_buckets[4])),
                       ('bp_low_highvol', set(bp_buckets[0])),
                       ('ep_cheap_highturn', set(ep_buckets[4])),
                       ('bp_cheap_highturn', set(bp_buckets[4]))):
            other = highturn if key.endswith('highturn') else highvol
            jv = jaccard(s, other)
            if jv is not None:
                jac[key].append(jv)

    cover_out = {k: stat3(vals) for k, vals in cov.items()}
    win_out = {}
    for tag, w in wins.items():
        if not w['months']:
            continue
        win_out[tag] = {
            'months': w['months'],
            'clipped_name_months': w['clipped'],
            'ranked_name_months': w['total'],
            'clipped_share': round(w['clipped'] / w['total'], 6) if w['total'] else None,
            'p1_median': round(float(np.median(w['p1'])), 6),
            'p99_median': round(float(np.median(w['p99'])), 8),
            'max_median': round(float(np.median(w['max'])), 4),
        }
    sp_out = {k: {'mean': round(float(np.mean(vs)), 4),
                  'median': round(float(np.median(vs)), 4),
                  'abs_mean': round(float(np.mean(np.abs(vs))), 4),
                  'months': len(vs)}
              for k, vs in sp.items() if vs}
    jac_out = {k: {'mean': round(float(np.mean(vs)), 4),
                   'median': round(float(np.median(vs)), 4),
                   'p90': round(float(np.percentile(vs, 90)), 4),
                   'months': len(vs)}
               for k, vs in jac.items() if vs}

    verdict = {
        'spearman_threshold': 0.5,
        'jaccard_threshold': 0.3,
        'ep_same_exposure': bool(
            abs(sp_out.get('ep_pos_vol', {}).get('mean', 0.0)) > 0.5
            or jac_out.get('ep_cheap_highvol', {}).get('mean', 0.0) > 0.3),
        'bp_same_exposure': bool(
            abs(sp_out.get('bp_pos_vol', {}).get('mean', 0.0)) > 0.5
            or jac_out.get('bp_cheap_highvol', {}).get('mean', 0.0) > 0.3),
    }
    print('COVERAGE ' + json.dumps(cover_out, ensure_ascii=False), flush=True)
    print('WINSOR ' + json.dumps(win_out, ensure_ascii=False), flush=True)
    print('SPEARMAN ' + json.dumps(sp_out, ensure_ascii=False), flush=True)
    print('JACCARD ' + json.dumps(jac_out, ensure_ascii=False), flush=True)
    print('VERDICT ' + json.dumps(verdict, ensure_ascii=False), flush=True)
    print('ELAPSED {:.1f}s'.format(time.perf_counter() - t0), flush=True)


if __name__ == '__main__':
    main()
