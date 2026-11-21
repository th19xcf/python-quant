#!/usr/bin/env python3
"""事件驱动买点研究 —— 以事件日为买点，测绝对净收益 / 超额 / 组合层。

事件
- PEAD：财报首次公告日（SUE 可算），T+1 开盘买入
- 指数调入：调整生效日，T+1 开盘买入；调出作对照
- 除权除息：除权日，T+1 开盘买入

口径
- 全 A，2005 起；剔 ST、上市不足 60 交易日
- 后复权（close/open × hfq_factor）
- 涨跌停 / 停牌不可买剔除
- 持有 3/5/10/20/60 日，往返成本 0.13%
- 超额基准：全 A 等权（按日截面等权，开盘->收盘 + 收盘->收盘复合）
- t 值按信号日聚类，BH-FDR 校正

只读 data/，产物写 work/事件驱动买点_2026-11-21/。
"""
from __future__ import annotations

import json
import math
import sys
import time
from pathlib import Path

import numpy as np
import polars as pl

ROOT = Path(r'e:\code\python\quant')
DATA = ROOT / 'data' / 'local_store'
OUT = ROOT / 'work' / '事件驱动买点_2026-11-21'
OUT.mkdir(parents=True, exist_ok=True)
LOG = open(OUT / 'run.log', 'a', encoding='utf-8')

HORIZONS = (3, 5, 10, 20, 60)
COST = 0.0013
START = np.datetime64('2005-01-01')
MAXHOLD = 20
ERAS = [('2005-2014', 2005, 2014), ('2015-2019', 2015, 2019), ('2020-2026', 2020, 2026)]


def log(*a):
    s = ' '.join(str(x) for x in a)
    print(s, flush=True)
    LOG.write(s + '\n')
    LOG.flush()


# ------------------------------------------------------------------ SUE
def build_sue() -> pl.DataFrame:
    inc = (
        pl.read_parquet(DATA / 'meta' / 'financials' / 'income.parquet',
                        columns=['ts_code', 'end_date', 'ann_date', 'ann_date_source',
                                 'parent_net_profit'])
        .filter(pl.col('parent_net_profit').is_not_null()
                & pl.col('ann_date').is_not_null())
        .with_columns([pl.col('end_date').dt.year().alias('_y'),
                       pl.col('end_date').dt.month().alias('_m')])
        .sort(['ts_code', 'end_date', 'ann_date'])
        .group_by(['ts_code', 'end_date'], maintain_order=True)
        .agg([pl.col('ann_date').first(), pl.col('ann_date_source').first(),
              pl.col('parent_net_profit').first(),
              pl.col('_y').first(), pl.col('_m').first()])
        .sort(['ts_code', 'end_date'])
    )
    cols = {k: [] for k in ('code', 'end_date', 'ann_date', 'sue', 'raw')}
    for (code,), g in inc.group_by(['ts_code'], maintain_order=True):
        ann = g['ann_date'].to_numpy().astype('datetime64[D]')
        pnp = g['parent_net_profit'].to_numpy().astype(float)
        ys = g['_y'].to_numpy()
        ms = g['_m'].to_numpy()
        n = len(g)
        single = np.full(n, np.nan)
        for i in range(n):
            if ms[i] == 3:
                single[i] = pnp[i]
            elif (i > 0 and ms[i-1] == ms[i] - 3 and ys[i-1] == ys[i]
                  and ann[i-1] <= ann[i] and np.isfinite(single[i-1])):
                single[i] = pnp[i] - pnp[i-1]
        idx_by_ym = {(ys[i], ms[i]): i for i in range(n)}
        diff = np.full(n, np.nan)
        for i in range(n):
            j = idx_by_ym.get((ys[i] - 1, ms[i]))
            if (j is not None and np.isfinite(single[i]) and np.isfinite(single[j])
                    and ann[j] <= ann[i]):
                diff[i] = single[i] - single[j]
        sue = np.full(n, np.nan)
        raw = np.full(n, np.nan)
        hist: list[tuple[np.datetime64, float]] = []
        for i in range(n):
            cand = [v for (a, v) in hist if a <= ann[i]]
            if np.isfinite(diff[i]) and len(cand) >= 6:
                sd = float(np.std(cand[-8:], ddof=1))
                if sd > 0:
                    sue[i] = diff[i] / sd
            if np.isfinite(diff[i]):
                hist.append((ann[i], float(diff[i])))
            # 原始超预期幅度：季节差分 / |去年同期单季净利|（不做波动标准化）
            j = idx_by_ym.get((ys[i] - 1, ms[i]))
            if j is not None and np.isfinite(diff[i]) and np.isfinite(single[j]) and single[j] != 0:
                raw[i] = diff[i] / abs(single[j])
        cols['code'].extend([code] * n)
        cols['end_date'].extend(g['end_date'].to_list())
        cols['ann_date'].extend(g['ann_date'].to_list())
        cols['sue'].extend(sue.tolist())
        cols['raw'].extend(raw.tolist())
    return pl.DataFrame({
        'ts_code': cols['code'],
        'end_date': pl.Series(cols['end_date'], dtype=pl.Date),
        'event_date': pl.Series(cols['ann_date'], dtype=pl.Date),
        'sue': pl.Series(cols['sue'], dtype=pl.Float64),
        'raw': pl.Series(cols['raw'], dtype=pl.Float64),
    })


# ------------------------------------------------------------------ 事件表
def build_events(sue: pl.DataFrame) -> pl.DataFrame:
    # 两种超预期定义并行：SUE（标准化）与 RAW（原始幅度）
    pead = (sue.filter(pl.col('sue').is_finite())
            .select([pl.col('ts_code'), pl.col('event_date'),
                     pl.col('sue').alias('meta1'),
                     pl.lit(0, dtype=pl.Int8).alias('etype'),
                     pl.lit('').alias('meta2')]))
    pead_raw = (sue.filter(pl.col('raw').is_finite())
                .select([pl.col('ts_code'), pl.col('event_date'),
                         pl.col('raw').alias('meta1'),
                         pl.lit(4, dtype=pl.Int8).alias('etype'),
                         pl.lit('').alias('meta2')]))

    reb = (
        pl.read_parquet(ROOT / '2026-11-19' / 'index-rebalance-event-study' / 'outputs' / 'events_detail.parquet')
        .filter(pl.col('k') == 10)
        .select([
            pl.col('ts_code'),
            pl.col('effective_date').alias('event_date'),
            pl.lit(0.0).alias('meta1'),
            pl.lit(1, dtype=pl.Int8).alias('etype'),
            pl.col('index_code').cast(pl.Utf8).alias('meta2'),
        ])
        .filter((pl.col('event_date') >= pl.date(2005, 1, 1)) & pl.col('event_date').is_not_null())
        .unique(['ts_code', 'event_date', 'meta2'])
    )

    # 调出对照：由成分区间 out_date 构造
    mem = (
        pl.read_parquet(DATA / 'meta' / 'index_members' / 'membership_intervals.parquet')
        .filter(pl.col('out_date_known') & pl.col('out_date').is_not_null()
                & (pl.col('out_date') >= pl.date(2005, 1, 1)))
        .select([pl.col('ts_code'), pl.col('out_date').alias('event_date'),
                 pl.lit(0.0).alias('meta1'), pl.lit(3, dtype=pl.Int8).alias('etype'),
                 pl.col('index_code').cast(pl.Utf8).alias('meta2')])
        .unique(['ts_code', 'event_date', 'meta2'])
    )

    dv = (
        pl.read_parquet(DATA / 'factors' / 'stock_dividend.parquet',
                        columns=['ts_code', 'ex_date', 'cash_div', 'share_div', 'no_ex_rights'])
        .filter(pl.col('ex_date').is_not_null() & (pl.col('ex_date') >= pl.date(2005, 1, 1))
                & (~pl.col('no_ex_rights').fill_null(False))
                & (pl.col('cash_div').fill_null(0) + pl.col('share_div').fill_null(0) > 0))
        .select([pl.col('ts_code'), pl.col('ex_date').alias('event_date'),
                 (pl.col('cash_div').fill_null(0) + pl.col('share_div').fill_null(0)).alias('meta1'),
                 pl.lit(2, dtype=pl.Int8).alias('etype'),
                 pl.lit('').alias('meta2')])
        .unique(['ts_code', 'event_date'])
    )

    ev = pl.concat([pead, pead_raw, reb, mem, dv], how='vertical_relaxed')
    ev = ev.filter((pl.col('event_date') >= pl.date(2005, 1, 1))
                   & (pl.col('event_date') <= pl.date(2026, 11, 20)))
    ev = ev.sort(['ts_code', 'event_date', 'etype'])
    ev = ev.with_row_index('eid')
    return ev


# ------------------------------------------------------------------ 行情状态
def load_st() -> dict[str, list[tuple[int, int]]]:
    st = pl.read_parquet(DATA / 'meta' / 'st' / 'st_intervals.parquet',
                         columns=['ts_code', 'start_date', 'end_date'])
    out: dict[str, list[tuple[int, int]]] = {}
    for code, s, e in zip(st['ts_code'].to_list(), st['start_date'].to_list(),
                          st['end_date'].to_list()):
        si = int(np.datetime64(s).astype('int64'))
        ei = int(np.datetime64(e).astype('int64')) if e is not None else 10**9
        out.setdefault(code, []).append((si, ei))
    return out


def load_susp() -> dict[str, np.ndarray]:
    sp = (pl.read_parquet(DATA / 'meta' / 'suspension' / 'suspension_daily.parquet',
                          columns=['ts_code', 'trade_date', 'asset_class', 'is_suspended'])
          .filter((pl.col('asset_class') == 'stock') & pl.col('is_suspended')))
    out: dict[str, np.ndarray] = {}
    for (code,), g in sp.group_by(['ts_code'], maintain_order=True):
        out[code] = g['trade_date'].to_numpy().astype('datetime64[D]').astype('int64')
    return out


# ------------------------------------------------------------------ 主流程
def main() -> None:
    t0 = time.time()
    log(f'[start] {time.strftime("%Y-%m-%d %H:%M:%S")}')

    cal = (pl.read_parquet(DATA / 'quotes' / 'index' / 'sh' / '000300.SH.parquet',
                           columns=['date'])
           .filter(pl.col('date') >= pl.date(2005, 1, 1))['date']
           .to_numpy().astype('datetime64[D]'))
    cal = np.unique(cal)
    ncal = len(cal)
    log(f'[cal] {cal[0]} ~ {cal[-1]} n={ncal}')

    log('[sue] building ...')
    sue = build_sue()
    ev = build_events(sue)
    for et, name in [(0, 'PEAD_SUE'), (4, 'PEAD原始幅度'), (1, '指数调入'), (2, '除权除息'), (3, '指数调出')]:
        sub = ev.filter(pl.col('etype') == et)
        if sub.height:
            log(f'[events] {name}: n={sub.height} '
                f'{sub["event_date"].min()} ~ {sub["event_date"].max()}')
        else:
            log(f'[events] {name}: n=0')

    st_map = load_st()
    susp_map = load_susp()

    evs = ev.to_dict(as_series=False)
    eid = np.asarray(evs['eid'])
    codes = evs['ts_code']
    edates = np.asarray(pl.Series(evs['event_date']).cast(pl.Date).to_numpy()).astype('datetime64[D]')
    etypes = np.asarray(evs['etype'])
    meta1 = np.asarray(evs['meta1'], dtype=float)

    by_code: dict[str, list[int]] = {}
    for i, c in enumerate(codes):
        by_code.setdefault(c, []).append(i)

    n_ev = len(eid)
    entry_cal = np.full(n_ev, -1, dtype=np.int32)
    exit_cal = {h: np.full(n_ev, -1, dtype=np.int32) for h in HORIZONS}
    ret = {h: np.full(n_ev, np.nan) for h in HORIZONS}
    entry_limit = np.zeros(n_ev, dtype=bool)
    n_prev = np.zeros(n_ev, dtype=np.int32)
    st_hit = np.zeros(n_ev, dtype=bool)
    susp_days = np.zeros(n_ev, dtype=np.int32)
    pre20 = np.full(n_ev, np.nan)
    divy = np.full(n_ev, np.nan)
    paths: list[np.ndarray | None] = [None] * n_ev

    bsum = np.zeros(ncal); bcnt = np.zeros(ncal)
    bisum = np.zeros(ncal); bicnt = np.zeros(ncal)

    files = sorted((DATA / 'quotes' / 'stock').glob('*/*.parquet'))
    log(f'[pass] stock files={len(files)} events={n_ev}')
    done = 0
    for f in files:
        code = f.stem
        df = (pl.read_parquet(f, columns=['date', 'open', 'high', 'low', 'close',
                                          'pct_chg', 'hfq_factor'])
              .sort('date'))
        if df.height < 2:
            continue
        d = df['date'].to_numpy().astype('datetime64[D]')
        fac = df['hfq_factor'].to_numpy().astype(float)
        o_raw = df['open'].to_numpy().astype(float)
        c_raw = df['close'].to_numpy().astype(float)
        o = o_raw * fac
        c = c_raw * fac
        h = df['high'].to_numpy().astype(float) * fac
        lo = df['low'].to_numpy().astype(float) * fac
        pct = df['pct_chg'].to_numpy().astype(float)
        n = len(d)

        # ---- 基准：全 A 等权日收益（与事件同一可交易域：上市满 60 日、非 ST、无长停牌跳空）
        cnt = np.arange(1, n + 1)
        bm_ok = cnt >= 60
        di_all = d.astype('int64')
        for (si, ei) in st_map.get(code, []):
            bm_ok &= ~((di_all >= si) & (di_all <= ei))
        rr_full = np.full(n, np.nan)
        intra_full = np.full(n, np.nan)
        if n > 1:
            rr_full[1:] = c[1:] / c[:-1] - 1.0
            gap_days = (d[1:] - d[:-1]).astype('int64')
            rr_full[1:][gap_days > 10] = np.nan
        with np.errstate(invalid='ignore', divide='ignore'):
            intra_full = np.where(o > 0, c / o - 1.0, np.nan)

        dp = np.searchsorted(cal, d)
        ok = (dp < ncal)
        ok[ok] = cal[dp[ok]] == d[ok]
        sel = ok & bm_ok
        if sel.any():
            idx = dp[sel]
            fin = np.isfinite(rr_full[sel])
            bsum[idx[fin]] += rr_full[sel][fin]
            bcnt[idx[fin]] += 1
            fini = np.isfinite(intra_full[sel])
            bisum[idx[fini]] += intra_full[sel][fini]
            bicnt[idx[fini]] += 1

        idxs = by_code.get(code)
        if not idxs:
            continue
        done += 1

        for i in idxs:
            ent = int(np.searchsorted(d, edates[i], side='right'))
            if ent >= n:
                continue
            entry_cal[i] = int(np.searchsorted(cal, d[ent]))
            n_prev[i] = ent
            entry_limit[i] = (h[ent] == lo[ent]) and (pct[ent] >= 4.8)
            pre20[i] = (c[ent-1] / c[ent-21] - 1.0) if ent >= 21 else np.nan
            if etypes[i] == 2:
                divy[i] = meta1[i] / c_raw[ent-1] if ent >= 1 and c_raw[ent-1] > 0 else np.nan

            last = ent
            for hz in HORIZONS:
                ex = ent + hz - 1
                if ex < n:
                    ret[hz][i] = c[ex] / o[ent] - 1.0
                    exit_cal[hz][i] = int(np.searchsorted(cal, d[ex]))
                    last = max(last, ex)

            di = int(d[ent].astype('int64'))
            dj = int(d[last].astype('int64'))
            for (si, ei) in st_map.get(code, []):
                if si <= dj and ei >= di:
                    st_hit[i] = True
                    break
            sd = susp_map.get(code)
            if sd is not None:
                k0 = np.searchsorted(sd, di, side='left')
                k1 = np.searchsorted(sd, dj, side='right')
                susp_days[i] = int(k1 - k0)

            # 组合层盯市路径（按交易日历对齐 + 前向填充）
            span_end = max((int(exit_cal[hz][i]) for hz in HORIZONS), default=-1)
            if span_end >= 0:
                L = int(span_end - entry_cal[i] + 1)
                p = np.full(L, np.nan, dtype=np.float32)
                seg = c[ent:last + 1] / o[ent] - 1.0
                seg_d = d[ent:last + 1]
                pos = np.searchsorted(cal, seg_d) - entry_cal[i]
                m = (pos >= 0) & (pos < L)
                p[pos[m]] = seg[m]
                # 前向填充
                lastv = np.nan
                for k in range(L):
                    if np.isfinite(p[k]):
                        lastv = p[k]
                    else:
                        p[k] = lastv if np.isfinite(lastv) else 0.0
                paths[i] = p

    log(f'[pass] done stocks={done} elapsed={time.time()-t0:.0f}s')

    # ---------------------------------------------------------- 基准序列
    b = np.where(bcnt > 0, bsum / np.maximum(bcnt, 1), 0.0)
    bi = np.where(bicnt > 0, bisum / np.maximum(bicnt, 1), 0.0)
    logc = np.log1p(np.clip(b, -0.5, 0.5))
    cumlog = np.concatenate([[0.0], np.cumsum(logc)])
    level = np.exp(cumlog[1:])
    # 牛熊震荡
    ma200 = np.full(ncal, np.nan)
    for i in range(199, ncal):
        ma200[i] = level[i-199:i+1].mean()
    slope = np.full(ncal, np.nan)
    slope[20:] = ma200[20:] - ma200[:-20]
    regime = np.full(ncal, 'range', dtype=object)
    regime[(level > ma200) & (slope > 0)] = 'bull'
    regime[(level < ma200) & (slope < 0)] = 'bear'
    vol20 = np.full(ncal, np.nan)
    for i in range(20, ncal):
        vol20[i] = b[i-20:i].std(ddof=1) * math.sqrt(252)
    vq = np.full(ncal, 'mid', dtype=object)
    fin_v = np.isfinite(vol20)
    if fin_v.any():
        q1, q2 = np.nanpercentile(vol20[fin_v], [33.3, 66.7])
        vq[fin_v & (vol20 <= q1)] = 'low_vol'
        vq[fin_v & (vol20 > q2)] = 'high_vol'

    def bench_ret(e_cal: int, x_cal: int) -> float:
        if e_cal < 0 or x_cal < 0 or x_cal < e_cal:
            return np.nan
        return float(np.exp(cumlog[x_cal + 1] - cumlog[e_cal]) * (1.0 + bi[e_cal]) - 1.0)

    # ---------------------------------------------------------- 事件宽表
    recs = {
        'eid': eid, 'ts_code': codes, 'etype': etypes, 'event_date': edates,
        'meta1': meta1, 'meta2': np.asarray(evs['meta2']),
        'entry_cal': entry_cal,
        'entry_day': np.where(entry_cal >= 0,
                              cal[np.clip(entry_cal, 0, ncal - 1)].astype('int64'), -1), 'n_prev': n_prev, 'entry_limit': entry_limit,
        'st_hit': st_hit, 'susp_days': susp_days, 'pre20': pre20, 'divy': divy,
    }
    for hz in HORIZONS:
        recs[f'ret{hz}'] = ret[hz]
        recs[f'bench{hz}'] = np.array([bench_ret(entry_cal[i], exit_cal[hz][i]) for i in range(n_ev)])
        recs[f'exit{hz}'] = exit_cal[hz]

    df = pl.DataFrame(recs)
    df = df.with_columns([
        pl.col('event_date').cast(pl.Date),
        (pl.col('event_date').dt.year()).alias('year'),
        pl.col('entry_day').cast(pl.Int32).cast(pl.Date).alias('entry_date'),
        pl.col('entry_cal').map_elements(
            lambda i: regime[i] if 0 <= i < ncal else None, return_dtype=pl.Utf8).alias('regime'),
        pl.col('entry_cal').map_elements(
            lambda i: vq[i] if 0 <= i < ncal else None, return_dtype=pl.Utf8).alias('vol_regime'),
    ])

    # 市值
    val = (pl.read_parquet(DATA / 'meta' / 'financials' / 'valuation_daily.parquet',
                           columns=['ts_code', 'trade_date', 'total_mv'])
           .filter(pl.col('trade_date') >= pl.date(2005, 1, 1))
           .rename({'trade_date': 'entry_date', 'total_mv': 'total_mv'}))
    df = df.join(val, on=['ts_code', 'entry_date'], how='left')
    # total_mv 单位：元；50 亿=5e9，200 亿=2e10
    df = df.with_columns(
        pl.when(pl.col('total_mv') < 5e9).then(pl.lit('small'))
        .when(pl.col('total_mv') < 2e10).then(pl.lit('mid'))
        .when(pl.col('total_mv').is_not_null()).then(pl.lit('large'))
        .otherwise(pl.lit('unknown')).alias('cap_group'))

    df.write_parquet(OUT / 'events_wide.parquet')
    log(f'[wide] rows={df.height} cols={df.width}')

    # ---------------------------------------------------------- 统计
    def cluster_t(x) -> tuple[float, float, int]:
        v = np.asarray(x, dtype=float)
        v = v[np.isfinite(v)]
        if v.size < 3:
            return np.nan, np.nan, int(v.size)
        t = v.mean() / (v.std(ddof=1) / math.sqrt(v.size))
        p = math.erfc(abs(t) / math.sqrt(2))
        return float(t), float(p), int(v.size)

    base_cond = (
        pl.col('entry_cal') >= 0)
    filt = base_cond & (pl.col('n_prev') >= 60) & (~pl.col('st_hit')) & (~pl.col('entry_limit'))

    rows = []

    def add_block(sub: pl.DataFrame, et: int, hz: int, dim: str, grp: str, n_extra: dict | None = None):
        r = np.asarray(sub[f'ret{hz}'], dtype=float)
        bm = np.asarray(sub[f'bench{hz}'], dtype=float)
        if r.size < 10:
            return
        net = (1.0 + r) * (1.0 - COST) - 1.0
        exc = net - bm
        # 按信号日聚类
        g = sub.select(['event_date', pl.Series('exc', exc)]).group_by('event_date').agg(
            pl.col('exc').mean())
        t, p, nd = cluster_t(g['exc'].to_numpy())
        yr = sub['year'].to_numpy()
        era_stats = {}
        for name, y0, y1 in ERAS:
            m = (yr >= y0) & (yr <= y1)
            if m.sum() >= 20:
                era_stats[name] = float(np.nanmean(exc[m]) * 100)
            else:
                era_stats[name] = np.nan
        row = {
            'event': {0: 'PEAD_SUE', 4: 'PEAD原始幅度', 1: '指数调入', 2: '除权除息', 3: '指数调出'}[et],
            'horizon': hz, 'dim': dim, 'group': grp,
            'n': int(r.size),
            'n_signal_days': nd,
            'abs_net_mean_pct': float(np.nanmean(net) * 100),
            'abs_net_median_pct': float(np.nanmedian(net) * 100),
            'abs_net_win_pct': float(np.nanmean(net > 0) * 100),
            'excess_mean_pct': float(np.nanmean(exc) * 100),
            'excess_median_pct': float(np.nanmedian(exc) * 100),
            'excess_win_pct': float(np.nanmean(exc > 0) * 100),
            't_cluster': t, 'p_value': p,
            'era_2005_2014_pct': era_stats.get('2005-2014', np.nan),
            'era_2015_2019_pct': era_stats.get('2015-2019', np.nan),
            'era_2020_2026_pct': era_stats.get('2020-2026', np.nan),
        }
        if n_extra:
            row.update(n_extra)
        rows.append(row)

    for et in (0, 4, 1, 2, 3):
        sub = df.filter((pl.col('etype') == et) & filt)
        if sub.height == 0:
            continue
        for hz in HORIZONS:
            add_block(sub, et, hz, 'overall', 'all')
            for name, y0, y1 in ERAS:
                add_block(sub.filter((pl.col('year') >= y0) & (pl.col('year') <= y1)),
                          et, hz, 'era', name)
            for g in ('small', 'mid', 'large'):
                add_block(sub.filter(pl.col('cap_group') == g), et, hz, 'cap', g)
            for g in ('bull', 'bear', 'range'):
                add_block(sub.filter(pl.col('regime') == g), et, hz, 'regime', g)
            for g in ('low_vol', 'mid', 'high_vol'):
                add_block(sub.filter(pl.col('vol_regime') == g), et, hz, 'vol', g)
            yrs = sorted(set(sub['year'].to_list()))
            for y in yrs:
                add_block(sub.filter(pl.col('year') == y), et, hz, 'year', str(y))
            if et in (0, 4):
                tag = 'SUE' if et == 0 else 'RAW'
                q = np.nanpercentile(sub['meta1'].to_numpy(), [33.3, 66.7])
                add_block(sub.filter(pl.col('meta1') <= q[0]), et, hz,
                          f'{tag}_tercile', 'low')
                add_block(sub.filter((pl.col('meta1') > q[0]) & (pl.col('meta1') <= q[1])),
                          et, hz, f'{tag}_tercile', 'mid')
                add_block(sub.filter(pl.col('meta1') > q[1]), et, hz,
                          f'{tag}_tercile', 'high')
                # 超跌 × 超预期：超跌 = 公告前 20 日涨幅最低 20%
                p20 = np.nanpercentile(sub['pre20'].to_numpy(), 20)
                oversold = sub.filter(pl.col('pre20') <= p20)
                add_block(oversold, et, hz, 'oversold', 'oversold_only')
                hi_cut = np.nanpercentile(oversold['meta1'].to_numpy(), 66.7)
                add_block(oversold.filter(pl.col('meta1') > hi_cut), et, hz,
                          'oversold', 'oversold_high')
                add_block(oversold.filter(pl.col('meta1') <= hi_cut), et, hz,
                          'oversold', 'oversold_low')
                add_block(sub.filter((pl.col('pre20') <= p20) & (pl.col('meta1') > hi_cut)),
                          et, hz, 'oversold', 'oversold_high_global')
            if et in (1, 3):
                for ic in sorted(set(sub['meta2'].to_list())):
                    add_block(sub.filter(pl.col('meta2') == ic), et, hz, 'index', str(ic))
            if et == 2:
                dv_ = np.nanpercentile(sub['divy'].to_numpy(), [33.3, 66.7])
                add_block(sub.filter(pl.col('divy') <= dv_[0]), et, hz, 'div_yield', 'low')
                add_block(sub.filter((pl.col('divy') > dv_[0]) & (pl.col('divy') <= dv_[1])),
                          et, hz, 'div_yield', 'mid')
                add_block(sub.filter(pl.col('divy') > dv_[1]), et, hz, 'div_yield', 'high')

    lb = pl.DataFrame(rows)
    # BH-FDR
    pv = lb['p_value'].to_numpy()
    qv = np.full(pv.size, np.nan)
    ok = np.isfinite(pv)
    if ok.any():
        idx = np.argsort(pv[ok])
        p_sorted = pv[ok][idx]
        m = p_sorted.size
        q_sorted = np.minimum.accumulate((p_sorted * m / np.arange(1, m + 1))[::-1])[::-1]
        q_sorted = np.clip(q_sorted, 0, 1)
        tmp = np.full(m, np.nan)
        tmp[idx] = q_sorted
        qv[ok] = tmp
    lb = lb.with_columns(pl.Series('q_fdr', qv))
    lb.write_csv(OUT / 'leaderboard.csv')
    log(f'[leaderboard] rows={lb.height}')

    # ---------------------------------------------------------- 组合层
    port_rows = []
    trade_rows = []
    for et, name in [(0, 'PEAD_SUE'), (4, 'PEAD原始幅度'), (1, '指数调入'), (2, '除权除息'), (3, '指数调出')]:
        sub = df.filter((pl.col('etype') == et) & filt)
        if sub.height < 50:
            continue
        for hz in HORIZONS:
            okm = (np.asarray(sub[f'exit{hz}']) >= 0)
            s = sub.filter(pl.Series(okm))
            if s.height < 50:
                continue
            ec = np.asarray(s['entry_cal'])
            xc = np.asarray(s[f'exit{hz}'])
            pidx = np.asarray(s['eid'])
            order = np.argsort(ec, kind='stable')
            ec, xc, pidx = ec[order], xc[order], pidx[order]
            by_entry: dict[int, list[int]] = {}
            for k, t in enumerate(ec):
                by_entry.setdefault(int(t), []).append(k)
            start_t = int(ec.min()); end_t = int(xc.max())
            nav = np.ones(end_t + 2)
            open_pos: list[dict] = []
            held: set[str] = set()
            codes_arr = np.asarray(codes)
            w = 1.0 / MAXHOLD
            turnover = 0.0
            days = 0
            trades = []
            pos_sum = 0
            for t in range(start_t, end_t + 1):
                dret = 0.0
                # 现有持仓当日收益（按目标权重 1/MAXHOLD，日度等权再平衡）
                for pos in open_pos:
                    p = pos['path']
                    k = min(t - pos['entry'], len(p) - 1)
                    if t == pos['entry']:
                        dret += w * float(p[0])
                    else:
                        kp = min(t - 1 - pos['entry'], len(p) - 1)
                        den = 1.0 + float(p[kp])
                        dret += w * (((1.0 + float(p[k])) / den - 1.0) if den > 1e-6 else -1.0)
                # 平仓
                still = []
                for pos in open_pos:
                    if pos['exit'] == t:
                        rel = pos['path'][min(t - pos['entry'], len(pos['path']) - 1)]
                        trades.append((1.0 + float(rel)) * (1.0 - COST) - 1.0)
                        dret -= w * COST
                        held.discard(pos['code'])
                    else:
                        still.append(pos)
                open_pos = still
                # 开仓
                for k in by_entry.get(t, []):
                    if len(open_pos) >= MAXHOLD:
                        break
                    code = codes_arr[pidx[k]]
                    if code in held:
                        continue
                    p = paths[pidx[k]]
                    open_pos.append({'entry': t, 'exit': int(xc[k]),
                                     'path': p, 'code': code})
                    held.add(code)
                    turnover += w
                    dret += w * float(p[0])
                nav[t] = (nav[t - 1] if t > start_t else 1.0) * (1.0 + max(dret, -0.999))
                days += 1
                pos_sum += len(open_pos)

            navs = nav[start_t:end_t + 1]
            navs = navs[np.isfinite(navs)]
            if navs.size < 30:
                continue
            ann = float(navs[-1] ** (252.0 / navs.size) - 1.0)
            dret = navs[1:] / navs[:-1] - 1.0
            vol = float(np.std(dret, ddof=1) * math.sqrt(252))
            sharpe = float(np.mean(dret) * 252 / vol) if vol > 0 else np.nan
            peak = np.maximum.accumulate(navs)
            dd = navs / peak - 1.0
            mdd = float(dd.min())
            calmar = float(ann / abs(mdd)) if mdd < 0 else np.nan
            # 月度
            mv = navs[::21]
            mret = mv[1:] / mv[:-1] - 1.0
            streak = 0; worst = 0
            for x in mret:
                if x < 0:
                    streak += 1; worst = max(worst, streak)
                else:
                    streak = 0
            # 逐年
            dates = cal[start_t:end_t + 1][:navs.size]
            yrs = np.array([int(str(x)[:4]) for x in dates])
            yr_ret = {}
            for y in sorted(set(yrs)):
                m = yrs == y
                if m.sum() > 20:
                    sub_nav = navs[m]
                    yr_ret[y] = sub_nav[-1] / sub_nav[0] - 1.0
            bench_window = float(np.exp(cumlog[end_t + 1] - cumlog[start_t]) - 1.0)
            bench_ann = float((1 + bench_window) ** (252.0 / navs.size) - 1.0)
            eff = sum(1 for y, v in yr_ret.items() if v > 0)
            trades_arr = np.asarray(trades)
            gains = trades_arr[trades_arr > 0].sum()
            losses = -trades_arr[trades_arr < 0].sum()
            pf = float(gains / losses) if losses > 0 else np.nan
            port_rows.append({
                'event': name, 'horizon': hz,
                'start': str(dates[0]), 'end': str(dates[-1]),
                'n_trades': int(trades_arr.size),
                'annualized_pct': ann * 100,
                'bench_annualized_pct': bench_ann * 100,
                'excess_annualized_pct': (ann - bench_ann) * 100,
                'max_drawdown_pct': mdd * 100,
                'volatility_pct': vol * 100,
                'sharpe': sharpe, 'calmar': calmar,
                'profit_factor': pf,
                'longest_losing_months': worst,
                'year_valid': f'{eff}/{len(yr_ret)}',
                'turnover_annual_x': turnover / max(days, 1) * 252 * 100,
                'capital_utilization_pct': pos_sum / max(days, 1) / MAXHOLD * 100,
                'avg_positions': pos_sum / max(days, 1),
                'avg_trade_ret_pct': float(np.mean(trades_arr) * 100) if trades_arr.size else np.nan,
                'win_rate_pct': float((trades_arr > 0).mean() * 100) if trades_arr.size else np.nan,
            })
            for y, v in yr_ret.items():
                trade_rows.append({'event': name, 'horizon': hz, 'year': y,
                                   'ret_pct': v * 100})

    pf_df = pl.DataFrame(port_rows)
    pf_df.write_csv(OUT / '组合层.csv')
    pl.DataFrame(trade_rows).write_csv(OUT / '组合层_逐年.csv')
    log(f'[portfolio] rows={pf_df.height}')

    meta = {
        'elapsed_sec': round(time.time() - t0, 1),
        'cal_range': [str(cal[0]), str(cal[-1])],
        'events': {name: int(df.filter(pl.col('etype') == et).height)
                   for et, name in [(0, 'PEAD_SUE'), (4, 'PEAD原始幅度'), (1, '指数调入'),
                                    (2, '除权除息'), (3, '指数调出')]},
        'sample_after_filter': int(df.filter(filt).height),
    }
    (OUT / 'meta.json').write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')
    log(f'[done] {meta}')


if __name__ == '__main__':
    main()
