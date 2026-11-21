#!/usr/bin/env python3
"""全 A 等权基准四种口径对账（2015-01-30~2026-10-30）。

口径：
  ① daily   每日收盘等权再平衡（不做任何过滤、不扣成本）
  ② monthly 每月末等权再平衡，月内持有（不做过滤、不扣成本）
  ③ hold    2015-01-30 等权买入后不调仓（不做过滤、不扣成本）
  ④ monthly_delisted  同 ②，但把本地已有但被 DataFetcher 跳过的退市股补回
  ②filtered_net  同 ②，但剔 ST + 上市不足 60 日 + 扣 0.13% 往返（= 已发布的引擎基准）

同时把另一条线的 work/超跌止跌买点_2026-11-21/market.parquet 在同一窗口复算，
验证它报的 17.3% 是哪种口径。

输出：work/横截面打分_2026-11-21/benchmark_reconcile.csv
"""
from __future__ import annotations

import glob
import json
import sys
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
for _p in (ROOT, ROOT / 'tools'):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import baseline_pv_val_equal_weight as base

STORE = ROOT / 'data/local_store'
OUT = ROOT / 'work' / '横截面打分_2026-11-21'
END = '2026-10-31'
START = np.datetime64('2015-01-30')
LAST = np.datetime64('2026-10-30')


def build_matrix(series: dict) -> tuple[np.ndarray, list]:
    """series: code -> (dates, close) → (dates_ref, codes, 价格矩阵, 月末位置)。"""
    dates = sorted({d for ds, _ in series.values() for d in ds})
    dates = np.array([d for d in dates if START <= d <= LAST], dtype='datetime64[D]')
    codes = sorted(series)
    px = np.full((dates.size, len(codes)), np.nan, dtype='float32')
    for j, code in enumerate(codes):
        ds, cl = series[code]
        ds = np.asarray(ds, dtype='datetime64[D]')
        hit = np.searchsorted(dates, ds)
        ok = (hit < dates.size) & (dates[np.minimum(hit, dates.size - 1)] == ds)
        px[hit[ok], j] = cl[ok]
    return dates, codes, px


def metrics(eq: np.ndarray, dates: np.ndarray) -> dict:
    years = (dates[-1] - dates[0]).astype('timedelta64[D]').astype(int) / 365.25
    annual = (eq[-1] / eq[0]) ** (1 / years) - 1
    mdd = float(np.min(eq / np.maximum.accumulate(eq) - 1))
    r = eq[1:] / eq[:-1] - 1
    return {'annual': round(float(annual), 4), 'max_drawdown': round(mdd, 4),
            'vol': round(float(np.std(r, ddof=1) * np.sqrt(252)), 4),
            'sharpe': round(float(annual / (np.std(r, ddof=1) * np.sqrt(252))), 3)}


def daily_ew(px):
    r = px[1:] / px[:-1] - 1
    m = np.isfinite(r)
    return np.r_[1.0, np.cumprod(1 + np.where(m, r, 0).sum(1) / np.maximum(m.sum(1), 1))]


def rebal(px, dates, every: int | None):
    """every=None → 每日等权；every=k → 每 k 个交易日重建等权（月频用月末索引）。"""
    n, k = px.shape
    eq = np.ones(n)
    w = np.zeros(k)
    last_px = px[0].copy()
    for t in range(n):
        p = px[t]
        live = np.isfinite(p) & (p > 0)
        last_px[live] = p[live]
        if t == 0:
            w = live / live.sum()
        elif every is not None and t % every == 0:
            w = live * (eq[t - 1] / live.sum())
        else:
            grow = np.isfinite(p) & (last_px > 0)
            w = np.where(grow, w * (np.where(grow, p, last_px) / np.where(np.isfinite(px[t - 1]), px[t - 1], last_px)), w)
        eq[t] = w.sum()
    return eq


def buy_hold(px):
    p0 = px[0]
    live = np.isfinite(p0) & (p0 > 0)
    w = live / live.sum()
    last = px.copy()
    for t in range(1, px.shape[0]):
        live_t = np.isfinite(px[t]) & (px[t] > 0)
        last[t][live_t] = px[t][live_t]
        last[t][~live_t & np.isfinite(last[t - 1])] = last[t - 1][~live_t & np.isfinite(last[t - 1])]
    val = np.nansum(np.where(np.isfinite(last), w * last / np.where(p0 > 0, p0, np.nan), np.nan), axis=1)
    val[0] = 1.0
    return val / val[0]


def load_extra(codes: list, ref: dict) -> dict:
    """补回 DataFetcher 跳过的退市股；复权方式用重叠标的自动对位后确定。"""
    files = {Path(f).stem: Path(f)
             for f in glob.glob(str(STORE / 'quotes/stock/*/*.parquet'))}

    def read(code: str, mode: str):
        df = pl.read_parquet(files[code], columns=['date', 'close', 'hfq_factor'])
        df = df.filter((pl.col('date') >= pl.lit(START)) & (pl.col('date') <= pl.lit(LAST)))
        c = df['close'].to_numpy().astype('float64')
        if mode == 'hfq':
            c = c * df['hfq_factor'].to_numpy().astype('float64')
        return df['date'].to_numpy(), c

    probe = next((c for c in ('000001.SZ', '600000.SH') if c in files and c in ref), None)
    mode = 'hfq'
    if probe:
        ds, c = read(probe, 'hfq')
        rds, rcl = ref[probe]
        idx = np.searchsorted(rds, ds)
        ok = (idx < rds.size) & (rds[np.minimum(idx, rds.size - 1)] == ds)
        diff = np.abs(c[ok] / rcl[idx[ok]] - 1).max() if ok.any() else 9
        if diff > 1e-3:
            mode = 'raw'
            ds, c = read(probe, 'raw')
            diff = np.abs(c[ok] / rcl[idx[ok]] - 1).max()
        print(f'复权对位 {probe} mode={mode} 最大相对误差={diff:.2e}', flush=True)

    out = {}
    for code in codes:
        try:
            ds, c = read(code, mode)
        except Exception:
            continue
        if ds.size:
            out[code] = (ds, c)
    return out


def rebal_filtered(px, codes, steps, min_bars=60):
    """月频等权，调仓日只买「当日有成交 & 上市足 min_bars & 非 ST」的票。"""
    st = pl.read_parquet(STORE / 'meta/st/st_intervals.parquet',
                         columns=['ts_code', 'start_date', 'end_date'])
    st_rows = st.rows()
    bars = np.cumsum(np.isfinite(px) & (px > 0), axis=0)
    n, k = px.shape
    eq = np.ones(n)
    w = np.zeros(k)
    last_px = px[0].copy()
    for t in range(n):
        p = px[t]
        live = np.isfinite(p) & (p > 0)
        last_px = np.where(live, p, last_px)
        if t == 0:
            w = live / live.sum()
        elif t in steps:
            m = np.datetime64(str(np.datetime_as_string(px_dates[t])[:10]))
            bad = {c for c, s, e in st_rows if s is not None and s <= m and (e is None or e >= m)}
            elig = live & (bars[t] >= min_bars) & np.array([c not in bad for c in codes])
            w = elig * (eq[t - 1] / max(elig.sum(), 1))
        else:
            prev = np.where(np.isfinite(px[t - 1]) & (px[t - 1] > 0), px[t - 1], np.nan)
            w = np.where((w > 0) & np.isfinite(prev), w * last_px / prev, w)
        eq[t] = w.sum()
    return eq


def main() -> int:
    data = base.load_universe('2013-06-01', END)
    data, violations = base.drop_contract_violations(data)
    series = {c: (df['date'].to_numpy(), df['close'].to_numpy().astype('float64'))
              for c, df in data.items()}
    dates, codes, px = build_matrix(series)
    print('主矩阵', px.shape, dates[0], dates[-1], flush=True)
    global px_dates
    px_dates = dates

    rows = []
    # ① 每日等权
    eq = daily_ew(px)
    rows.append({'scheme': 'daily_ew', **metrics(eq, dates)})
    # ② 每月末等权（月内不调）
    me_idx = [i for i in range(1, len(dates)) if dates[i].astype('datetime64[M]') != dates[i - 1].astype('datetime64[M]')]
    steps = set(me_idx)
    rows.append({'scheme': 'monthly_ew', **metrics(rebal_simple(px, steps), dates)})
    rows.append({'scheme': 'monthly_ew_filtered_noCost', **metrics(
        rebal_filtered(px, codes, steps), dates)})
    # ③ 买入持有
    rows.append({'scheme': 'buy_hold', **metrics(buy_hold(px), dates)})

    # ④ 补回退市股后的月频
    store_codes = {p.stem.split('.')[0] + '.' + p.stem.split('.')[1]
                   for p in map(Path, glob.glob(str(STORE / 'quotes/stock/*/*.parquet')))}
    viol_codes = {v['code'] if isinstance(v, dict) else str(v) for v in violations}
    extra = load_extra(sorted(c for c in store_codes
                              if c not in series and c not in viol_codes), series)
    print('补回退市/未纳入标的', len(extra), flush=True)
    if extra:
        dates2, codes2, px2 = build_matrix({**series, **extra})
        idx = np.searchsorted(dates2, dates)
        px2 = px2[idx]
        rows.append({'scheme': 'monthly_ew_delisted', **metrics(
            rebal_simple(px2, steps), dates)})

    # 对照：另一条线的 market.parquet
    mp = ROOT / 'work' / '超跌止跌买点_2026-11-21' / 'market.parquet'
    if mp.exists():
        mk = pl.read_parquet(mp)
        print('market.parquet 列:', mk.columns, mk.shape, flush=True)
        dcol = 'trade_date' if 'trade_date' in mk.columns else 'date'
        rcol = next((c for c in ('ew_ret', 'mkt_ret', 'ret') if c in mk.columns), None)
        if rcol:
            r_all = mk[rcol].to_numpy().astype('float64')
            if 'ordinal' in mk.columns:
                dts_all = (np.datetime64('1990-01-01')
                           + mk['ordinal'].to_numpy().astype('timedelta64[D]'))
            else:
                dts_all = mk['date'].to_numpy().astype('datetime64[D]')
            k = (dts_all >= START) & (dts_all <= LAST) & np.isfinite(r_all)
            r, dts = r_all[k], dts_all[k]
            k2 = (dts_all <= LAST) & np.isfinite(r_all)
            r2, dts2 = r_all[k2], dts_all[k2]
            yrs = (dts[-1] - dts[0]).astype('timedelta64[D]').astype(int) / 365.25
            yrs2 = (dts2[-1] - dts2[0]).astype('timedelta64[D]').astype(int) / 365.25
            eqm = np.cumprod(1 + r)
            eq2 = np.cumprod(1 + r2)
            ann = float(eqm[-1] ** (1 / yrs) - 1)
            sd = float(np.std(r, ddof=1) * np.sqrt(252))
            rows.append({'scheme': 'other_line_market_2015_2026', 'annual': round(ann, 4),
                         'max_drawdown': round(float(np.min(eqm / np.maximum.accumulate(eqm) - 1)), 4),
                         'vol': round(sd, 4), 'sharpe': round(ann / sd, 3)})
            rows.append({'scheme': 'other_line_market_full_range',
                         'annual': round(float(eq2[-1] ** (1 / yrs2) - 1), 4),
                         'max_drawdown': round(float(np.min(eq2 / np.maximum.accumulate(eq2) - 1)), 4),
                         'vol': None, 'sharpe': None})
    # 已发布的引擎基准（含 ST/60 日过滤 + 0.13% 往返）
    rows.append({'scheme': 'monthly_ew_filtered_net_engine', 'annual': 0.0641,
                 'max_drawdown': -0.6381, 'vol': 0.2630, 'sharpe': 0.244})

    df = pl.DataFrame(rows)
    df.write_csv(OUT / 'benchmark_reconcile.csv')
    print(df)


def rebal_simple(px, steps):
    n, k = px.shape
    eq = np.ones(n)
    w = np.zeros(k)
    last_px = px[0].copy()
    for t in range(n):
        p = px[t]
        live = np.isfinite(p) & (p > 0)
        last_px = np.where(live, p, last_px)
        if t == 0:
            w = live / live.sum()
        elif t in steps:
            w = live * (eq[t - 1] / live.sum())
        else:
            prev = np.where(np.isfinite(px[t - 1]) & (px[t - 1] > 0), px[t - 1], np.nan)
            w = np.where((w > 0) & np.isfinite(prev), w * last_px / prev, w)
        eq[t] = w.sum()
    return eq


if __name__ == '__main__':
    main()
