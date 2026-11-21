#!/usr/bin/env python3
"""历史 ST 状态 + 退市风险过滤：对照实验（2026-11-21）。

三个阶段（--phase st / diag / ab）：
  st   : 用 tushare ``namechange`` 重建逐段 ST 区间，与现有 ``meta/st/st_intervals.parquet``
         做月频对拍，回答「现有回测用的是静态当前名单还是历史状态」。
  diag : 把「实际买过的退市股」和「买入后 12 个月内转 ST」的样本拉出来，
         看买入时点的负债率 / 连续亏损 / 净资产 / 市值 / 成交额分位 / 股价有无共同特征。
  ab   : 原策略 vs 加过滤（买入当天可得），同口径各跑一遍。

token 只从环境变量 ``TS_TOKEN`` 读取，不落盘。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import numpy as np
import polars as pl
from loguru import logger

ROOT = Path(__file__).resolve().parents[1]
for _p in (ROOT, ROOT / 'tools'):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import alpha_authenticity_check as aac
import baseline_pv_val_equal_weight as base
import cross_section_score as css
import factor_lowvol_quantile as f
import factor_lowvol_variants as v

OUT = ROOT / 'work' / 'ST退市过滤_2026-11-21'
RUN_DIR = ROOT / 'data/local_store/backtest_runs/st_filter_20261121'
STORE = ROOT / 'data/local_store'
ST_DIR = STORE / 'meta/st'
LOCAL_ST = ST_DIR / 'st_intervals.parquet'
RAW_NC = ST_DIR / 'raw/namechange_tushare_20261121.parquet'
TS_ST = ST_DIR / 'st_tushare_intervals_20261121.parquet'


# ---------------------------------------------------------------------------
# 阶段 1：tushare namechange → ST 区间 + 与现有表对拍
# ---------------------------------------------------------------------------

def build_tushare_intervals() -> pl.DataFrame:
    df = pl.read_parquet(RAW_NC).with_columns(
        pl.col('start_date').str.strptime(pl.Date, '%Y%m%d'),
        pl.col('end_date').str.strptime(pl.Date, '%Y%m%d', strict=False),
    )
    st = df.filter(pl.col('name').str.to_uppercase().str.contains('ST')).select(
        ['ts_code', 'name', 'start_date', 'end_date', 'change_reason'])
    st = st.rename({'start_date': 'start_date', 'end_date': 'end_date'}).with_columns(
        pl.lit('tushare_namechange').alias('source'),
        pl.col('name').str.to_uppercase().str.contains(r'^\*ST').alias('is_star'))
    st.write_parquet(TS_ST)
    return st


def compare_with_local(months: List[date]) -> Dict[str, Any]:
    local = pl.read_parquet(LOCAL_ST)
    ts = pl.read_parquet(TS_ST)

    def to_map(d: pl.DataFrame) -> Dict[str, List[Tuple[date, date]]]:
        out: Dict[str, List[Tuple[date, date]]] = {}
        for c, s, e in zip(d['ts_code'].to_list(), d['start_date'].to_list(),
                           d['end_date'].to_list()):
            out.setdefault(c, []).append((s, e or date(2099, 12, 31)))
        return out

    lm, tm = to_map(local), to_map(ts)
    agree = only_ts = only_local = both_bad = 0
    codes = set(lm) | set(tm)
    for m in months:
        for c in codes:
            in_l = any(s <= m <= e for s, e in lm.get(c, []))
            in_t = any(s <= m <= e for s, e in tm.get(c, []))
            if in_l and in_t:
                both_bad += 1
            elif in_t:
                only_ts += 1
            elif in_l:
                only_local += 1
            else:
                agree += 1
    return {
        'local_intervals': local.height, 'local_codes': local['ts_code'].n_unique(),
        'ts_intervals': ts.height, 'ts_codes': ts['ts_code'].n_unique(),
        'months': len(months),
        'both_st_code_months': both_bad,
        'only_tushare_st': only_ts,
        'only_local_st': only_local,
        'neither': agree,
        'local_coverage_start_note': 'manifest: 沪市/北交所 safe_start 2005-01-01',
    }


# ---------------------------------------------------------------------------
# 特征表：买入当天可得
# ---------------------------------------------------------------------------

def st_sets(months: List[date], path: Path) -> Dict[date, Set[str]]:
    d = pl.read_parquet(path)
    rows = list(zip(d['ts_code'].to_list(), d['start_date'].to_list(),
                    d['end_date'].to_list()))
    out: Dict[date, Set[str]] = {m: set() for m in months}
    for c, s, e in rows:
        e = e or date(2099, 12, 31)
        for m in months:
            if s <= m <= e:
                out[m].add(c)
    return out


def financial_features(months: List[date]) -> Dict[str, Dict[date, Dict[str, float]]]:
    """年报口径：最近 2 个已公告年度的净利润、净资产、负债率（ann_date asof，无未来函数）。"""
    inc = pl.read_parquet(css.FIN / 'income.parquet',
                          columns=['ts_code', 'end_date', 'ann_date', 'net_profit'])
    inc = inc.filter(pl.col('end_date').dt.month() == 12).sort(['ts_code', 'ann_date'])
    bal = pl.read_parquet(css.FIN / 'balance.parquet',
                          columns=['ts_code', 'end_date', 'ann_date', 'total_equity', 'debt_ratio'])
    bal = bal.filter(pl.col('end_date').dt.month() == 12).sort(['ts_code', 'ann_date'])

    def pack(df: pl.DataFrame, col: str) -> Dict[str, Tuple[np.ndarray, np.ndarray, np.ndarray]]:
        out = {}
        codes = df['ts_code'].to_numpy()
        ad = np.array(df['ann_date'].to_list(), dtype='datetime64[D]')
        ed = np.array(df['end_date'].to_list(), dtype='datetime64[D]')
        val = df[col].to_numpy().astype('float64')
        starts = np.flatnonzero(np.r_[True, codes[1:] != codes[:-1]])
        ends = np.r_[starts[1:], len(codes)]
        for a, b in zip(starts, ends):
            out[str(codes[a])] = (ad[a:b], ed[a:b], val[a:b])
        return out

    np_ = pack(inc, 'net_profit')
    eq_ = pack(bal, 'total_equity')
    dr_ = pack(bal, 'debt_ratio')
    me = np.array(months, dtype='datetime64[D]')
    out = {k: {m: {} for m in months} for k in ('np1', 'np2', 'eq', 'debt')}

    def fill(store, key, kth: int) -> None:
        for code, (ad, ed, val) in store.items():
            idx = np.searchsorted(ad, me, side='right') - 1
            for j, m in enumerate(months):
                i = idx[j]
                if i < 0:
                    continue
                if kth == 1:
                    v = val[i]
                else:
                    if i - 1 < 0 or ed[i - 1] == ed[i]:
                        continue
                    v = val[i - 1]
                if np.isfinite(v):
                    out[key][m][code] = float(v)

    fill(np_, 'np1', 1)
    fill(np_, 'np2', 2)
    fill(eq_, 'eq', 1)
    fill(dr_, 'debt', 1)
    return out


def diag(runs: List[str]) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    data, _ = aac.load_universe_with_delisted()
    _, month_ends, _ = f.build_calendar(data)
    months = [m for m in month_ends if css.SIGNAL_START <= m <= date.fromisoformat(css.END)]
    sb = pl.read_parquet(STORE / 'meta/stock_basic.parquet', columns=['ts_code', 'status'])
    delisted = set(sb.filter(pl.col('status').is_not_null() & (pl.col('status') != 'L')
                             )['ts_code'].to_list())
    ts_map = st_sets(months, TS_ST)
    ts_iv = _all_intervals(TS_ST)
    fin = financial_features(months)
    mv = aac.load_circ_mv(months)

    rows = []
    for run in runs:
        t = pl.read_parquet(RUN_DIR.parent / 'alpha_auth_20261121' / run / 'trades.parquet')
        ccol = 'stock_code' if 'stock_code' in t.columns else 'ts_code'
        pcol = next((c for c in ('price', 'trade_price', 'open') if c in t.columns), None)
        buys = t.filter(pl.col('signal') == 'buy')
        for code, d, px in zip(buys[ccol].to_list(), buys['date'].to_list(),
                               buys[pcol].to_list() if pcol else [None] * buys.height):
            if not isinstance(d, date):
                d = d.date()
            sig = None
            for m in reversed(months):
                if m < d:
                    sig = m
                    break
            if sig is None:
                continue
            rawp = None
            q = data.get(code)
            if q is not None:
                i = int(np.searchsorted(q['date'].to_numpy(), np.datetime64(sig), 'right')) - 1
                if i >= 0:
                    rawp = float(q['raw_close'].to_numpy()[i])
            st_after = any(sig < s <= sig + timedelta(days=365)
                           for s, e in ts_iv.get(code, []))
            rows.append({
                'run': run, 'code': code, 'signal': sig.isoformat(), 'trade_date': d.isoformat(),
                'price': rawp,
                'is_delisted': code in delisted,
                'st_now': code in ts_map.get(sig, set()),
                'st_12m': st_after,
                'np1': fin['np1'][sig].get(code), 'np2': fin['np2'][sig].get(code),
                'equity': fin['eq'][sig].get(code), 'debt': fin['debt'][sig].get(code),
                'circ_mv': mv[sig].get(code),
            })
    df = pl.DataFrame(rows)
    df = df.with_columns(
        (pl.col('np1') < 0).alias('loss1'),
        ((pl.col('np1') < 0) & (pl.col('np2') < 0)).alias('loss2y'),
        (pl.col('equity') < 0).alias('neg_equity'),
        (pl.col('price') < 1.5).alias('px_lt_1_5'),
    )
    df.write_csv(OUT / 'diag_buys.csv')

    def rate(sub: pl.DataFrame, col: str) -> str:
        v = sub[col]
        n = v.len() - v.null_count()
        return f'{v.sum()}/{n}'

    bad = df.filter(pl.col('is_delisted') | pl.col('st_12m'))
    good = df.filter(~(pl.col('is_delisted') | pl.col('st_12m')))
    lines = [f'买入样本 {df.height}，其中退市/买入后 12 个月内转 ST：{bad.height}', '']
    for col in ('st_now', 'loss1', 'loss2y', 'neg_equity', 'px_lt_1_5'):
        lines.append(f'{col}: 风险样本 {rate(bad, col)} ｜ 其余 {rate(good, col)}')
    for col in ('np1', 'debt', 'equity', 'circ_mv', 'price'):
        lines.append(f'{col} 中位数: 风险 {bad[col].drop_nulls().median()} ｜ 其余 {good[col].drop_nulls().median()}')
    (OUT / 'diag_summary.txt').write_text('\n'.join(lines), encoding='utf-8')
    print('\n'.join(lines))


def _all_intervals(path: Path) -> Dict[str, List[Tuple[date, date]]]:
    d = pl.read_parquet(path)
    out: Dict[str, List[Tuple[date, date]]] = {}
    for c, s, e in zip(d['ts_code'].to_list(), d['start_date'].to_list(),
                       d['end_date'].to_list()):
        out.setdefault(c, []).append((s, e or date(2099, 12, 31)))
    return out


def _intervals(path: Path, code: str) -> List[Tuple[date, date]]:
    d = pl.read_parquet(path).filter(pl.col('ts_code') == code)
    return [(s, e or date(2099, 12, 31)) for s, e in zip(d['start_date'].to_list(),
                                                          d['end_date'].to_list())]


# ---------------------------------------------------------------------------
# 阶段 3：A/B
# ---------------------------------------------------------------------------

def pick_filtered(scores: Dict[str, Dict[str, float]], dims: List[str], top_n: int,
                  drop: Set[str]) -> List[str]:
    cand = []
    for code, sc in scores.items():
        if code in drop:
            continue
        parts = [sc.get(d) for d in dims]
        if any(x is None for x in parts):
            continue
        cand.append((sum(parts) / len(parts), code))
    cand.sort(key=lambda x: (-x[0], x[1]))
    return sorted(c for _, c in cand[:top_n])


def evaluate_run(name: str, path: Dict[date, List[str]], data, dividends, st_status,
                 prev_day, bench_curve, run_dir: Path) -> Dict[str, Any]:
    sched = v.schedules_from_path(path, prev_day)
    res = css.run_backtest(data, sched, dividends, st_status)
    curve = f.curve_of(res)
    trades = res.get('trades') or []
    row: Dict[str, Any] = {'run': name}
    for tag, (a, b) in css.SEGS.items():
        hold = [len(vv) for m, vv in path.items() if a <= m <= b]
        for k, val in css.stats(curve, trades, a, b, hold).items():
            row[f'{tag}_{k}' if tag != 'full' else k] = val
    row['yearly'] = aac.yearly_returns(curve)
    row['underwater'] = aac.longest_underwater_months(curve)
    if bench_curve:
        row['excess_monthly'], row['excess_hit'] = aac.monthly_excess(curve, bench_curve)
    out = run_dir / name
    out.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'date': [d for d, _ in curve],
                  'equity': [e for _, e in curve]}).write_parquet(out / 'equity.parquet')
    if trades:
        pl.DataFrame(trades, infer_schema_length=None).write_parquet(out / 'trades.parquet')
    logger.info('{} => annual={} mdd={} sharpe={} segs={}/{}/{}', name,
                row.get('annual'), row.get('max_drawdown'), row.get('sharpe'),
                row.get('seg1_annual'), row.get('seg2_annual'), row.get('seg3_annual'))
    return row


def bench_curve_of(root: Path) -> List[Tuple[date, float]]:
    d = pl.read_parquet(root / 'bench_sup_ew' / 'equity.parquet')
    return list(zip(d['date'].to_list(), d['equity'].to_list()))


def ab() -> None:
    data, _ = aac.load_universe_with_delisted()
    calendar, month_ends, prev_day = f.build_calendar(data)
    months = [m for m in month_ends if css.SIGNAL_START <= m <= date.fromisoformat(css.END)]
    quoted = v.quoted_sets(data, months)
    mom, bars = css.load_momentum(data, months)
    panel = css.load_panel_factors(months)
    quality = css.load_quality(months)
    st_local = css.load_st_sets(months)
    st_ts = st_sets(months, TS_ST)
    univ = {m: [c for c in quoted[m]
                if bars[m].get(c, 0) >= css.MIN_BARS and c not in st_local[m]]
            for m in months}
    signals = {m: css.month_scores(m, univ[m], panel, quality, mom) for m in months}
    sig_ep = aac.ep_only_signals(months, univ, panel)
    fin = financial_features(months)

    def drop_sets(kind: str) -> Dict[date, Set[str]]:
        out = {}
        for m in months:
            codes = set()
            if kind in ('st', 'all'):
                codes |= st_ts[m]
            for c in univ[m]:
                if kind in ('fund', 'all'):
                    np1, np2 = fin['np1'][m].get(c), fin['np2'][m].get(c)
                    eq = fin['eq'][m].get(c)
                    if (np1 is not None and np2 is not None and np1 < 0 and np2 < 0) or \
                            (eq is not None and eq < 0):
                        codes.add(c)
                if kind in ('px', 'all'):
                    d = data.get(c)
                    if d is not None:
                        i = int(np.searchsorted(d['date'].to_numpy(), np.datetime64(m), 'right')) - 1
                        if i >= 0 and d['raw_close'].to_numpy()[i] < 1.5:
                            codes.add(c)
            out[m] = codes
        return out

    dividends = aac._load_local_dividends(list(data), None, None)
    st_status = aac._load_st_status(None, data, None, css.WARMUP_START, css.END, None, None)
    bench_curve = bench_curve_of(RUN_DIR.parent / 'alpha_auth_20261121')

    rows = []
    cases = [('combo_vq_n30_flt_st', ['value', 'quality'], 'st', signals),
             ('combo_vq_n30_flt_fund', ['value', 'quality'], 'fund+px', signals),
             ('combo_vq_n30_flt_all', ['value', 'quality'], 'all', signals),
             ('value_n30_flt_all', ['value'], 'all', signals)]
    ds_cache: Dict[str, Dict[date, Set[str]]] = {}
    for name, dims, kind, sigs in cases:
        for k in (kind.split('+') if kind != 'all' else ['all']):
            if k not in ds_cache:
                ds_cache[k] = drop_sets(k)
        if kind == 'st':
            path = {m: pick_filtered(sigs[m], dims, 30, ds_cache['st'][m]) for m in months}
        elif kind == 'fund+px':
            path = {m: pick_filtered(sigs[m], dims, 30,
                                     ds_cache['fund'][m] | ds_cache['px'][m]) for m in months}
        else:
            path = {m: pick_filtered(sigs[m], dims, 30, ds_cache['all'][m]) for m in months}
        r = evaluate_run(name, path, data, dividends, st_status, prev_day,
                         bench_curve, RUN_DIR)
        r.update({'group': name, 'dims': '+'.join(dims), 'filter': kind})
        rows.append(r)
        logger.info('{} done', name)
    aac._rows_to_csv(rows, OUT / 'ab.csv')


def month_grid() -> List[date]:
    out = []
    y, mo = css.SIGNAL_START.year, css.SIGNAL_START.month
    end = date.fromisoformat(css.END)
    while date(y, mo, 1) <= end:
        nxt = date(y + (mo == 12), mo % 12 + 1, 1)
        day = nxt - timedelta(days=1)
        if day >= css.SIGNAL_START:
            out.append(day)
        y, mo = nxt.year, nxt.month
    return out


def reab() -> None:
    """用补后的权威 ST 表重跑三组：EP+质量 / 仅估值 / 全市场等权基准。"""
    data, _ = aac.load_universe_with_delisted()
    _, month_ends, prev_day = f.build_calendar(data)
    months = [m for m in month_ends if css.SIGNAL_START <= m <= date.fromisoformat(css.END)]
    quoted = v.quoted_sets(data, months)
    mom, bars = css.load_momentum(data, months)
    panel = css.load_panel_factors(months)
    quality = css.load_quality(months)
    st_new = css.load_st_sets(months)   # 已由 st_gapfill 补过的权威表
    univ = {m: [c for c in quoted[m]
                if bars[m].get(c, 0) >= css.MIN_BARS and c not in st_new[m]]
            for m in months}
    logger.info('补后 ST 表：{} 个月末，剔 ST 后股票数 {}/{}', len(months),
                sum(len(univ[m]) for m in months), sum(len(quoted[m]) for m in months))
    signals = {m: css.month_scores(m, univ[m], panel, quality, mom) for m in months}
    dividends = aac._load_local_dividends(list(data), None, None)
    st_status = aac._load_st_status(None, data, None, css.WARMUP_START, css.END, None, None)
    run_dir = RUN_DIR.parent / 'st_reab_20261121'

    rows: List[Dict[str, Any]] = []
    brow = evaluate_run('bench_reab', {m: sorted(univ[m]) for m in months},
                        data, dividends, st_status, prev_day, None, run_dir)
    rows.append(brow)
    bd = pl.read_parquet(run_dir / 'bench_reab' / 'equity.parquet')
    bench_curve = list(zip(bd['date'].to_list(), bd['equity'].to_list()))

    for name, dims in (('combo_vq_n30_reab', ['value', 'quality']),
                       ('value_n30_reab', ['value'])):
        path = css.build_paths(months, signals, dims, 30)
        rows.append(evaluate_run(name, path, data, dividends, st_status, prev_day,
                                 bench_curve, run_dir))
    aac._rows_to_csv(rows, OUT / 'reab.csv')

    old = pl.read_csv(aac.ROOT / 'work' / 'alpha真实性核查_2026-11-21' / 'compare_final.csv',
                      infer_schema_length=None)
    omap = {r['run']: r for r in old.to_dicts()}
    nmap = {r['run']: r for r in rows}
    comp = []
    for new, old_name, label in (('combo_vq_n30_reab', 'combo_vq_n30_sup', 'EP+质量'),
                                 ('value_n30_reab', 'value_n30_sup', '仅估值'),
                                 ('bench_reab', 'bench_sup_ew', '基准等权')):
        o, n = omap[old_name], nmap[new]
        comp.append({
            'group': label,
            'annual_old': o['annual'], 'annual_new': n['annual'],
            'd_annual_pp': round((n['annual'] - o['annual']) * 100, 3),
            'mdd_old': o['max_drawdown'], 'mdd_new': n['max_drawdown'],
            'd_mdd_pp': round((n['max_drawdown'] - o['max_drawdown']) * 100, 3),
            'sharpe_old': o['sharpe'], 'sharpe_new': n['sharpe'],
            'd_sharpe': round(n['sharpe'] - o['sharpe'], 4),
            'excess_old_pp': round((o.get('excess_vs_bench_monthly') or 0) * 100, 3),
            'excess_new_pp': round((n.get('excess_monthly') or 0) * 100, 3),
            'seg1_new': n['seg1_annual'], 'seg2_new': n['seg2_annual'],
            'seg3_new': n['seg3_annual'],
        })
    aac._rows_to_csv(comp, OUT / 'reab_compare.csv')
    for c in comp:
        print(c)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--phase', default='st', choices=['st', 'diag', 'ab', 'reab'])
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    logger.remove()
    logger.add(sys.stderr, level='INFO')
    logger.add(OUT / 'run.log', level='INFO', encoding='utf-8')

    if args.phase == 'st':
        build_tushare_intervals()
        months = month_grid()
        stats = compare_with_local(months)
        (OUT / 'st_compare.json').write_text(json.dumps(stats, ensure_ascii=False, indent=2),
                                             encoding='utf-8')
        print(json.dumps(stats, ensure_ascii=False, indent=2))
    elif args.phase == 'diag':
        diag(['value_n30_sup', 'combo_vq_n30_sup', 'bench_sup_ew'])
    elif args.phase == 'reab':
        reab()
    else:
        ab()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
