#!/usr/bin/env python3
"""全 A 市场估值分位择时（2026-11-21）。

信号：本地 valuation_daily 逐月截面聚合的市场估值
- pe_agg / pb_agg：总市值加权（正盈利/正净资产成分，等价于整体法）
- pe_med / pb_med：等权中位数（仅正值）
分位：滚动 60 月（5 年）与 120 月（10 年），只用当时可得历史。
标的：510300 / 510500 / 512100 / 510880（真实 ETF 日线）+ 等权全 A（合成月度指数）。
执行：月末信号、次日开盘（等权全 A 用月末收盘），13bp 往返，空仓 1.5%/年。
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))

from broad_beta_probe import (  # noqa: E402
    BUY_COST, MAX_INVEST, OUT, RULES, SELL_COST, load_etf, month_ends,
    sim_single, stats, timing_targets,
)

MKT = OUT / 'market_valuation_monthly.parquet'
SIG = OUT / 'market_valuation_signal.csv'
MET = OUT / 'market_timing_metrics.csv'
SUB = OUT / 'market_timing_subperiods.csv'
CORR = OUT / 'market_signal_corr.csv'

FULL = (date(2015, 1, 1), date(2026, 10, 31))
SUBPERIODS = [
    ('2015-2019', date(2015, 1, 1), date(2019, 12, 31)),
    ('2020-2023', date(2020, 1, 1), date(2023, 12, 31)),
    ('2024-2026', date(2024, 1, 1), date(2026, 10, 31)),
]
TARGETS = ['510300.SH', '510500.SH', '512100.SH', '510880.SH', 'EW_ALL_A']
CORE_RULES = ['bin30', 'bin50']
EXTRA_RULES = ['tier30_70', 'tier20_60']
SERIES = ['pe_agg', 'pe_med', 'pb_agg', 'pb_med']


def add_pct(df: pl.DataFrame) -> pl.DataFrame:
    for m in SERIES:
        vals = df[m].to_list()
        for win, minn in ((60, 36), (120, 60)):
            out: List[float | None] = []
            for i, v in enumerate(vals):
                if v is None or not np.isfinite(v):
                    out.append(None)
                    continue
                h = [x for x in vals[max(0, i - win + 1): i + 1]
                     if x is not None and np.isfinite(x)]
                out.append(round(sum(1 for x in h if x <= v) / len(h), 4)
                           if len(h) >= minn else None)
            df = df.with_columns(pl.Series(f'{m}_pct_{win}', out, dtype=pl.Float64))
    return df


def sim_monthly(df: pl.DataFrame, targets: Dict[date, float],
                a: date, b: date) -> Tuple[List[date], np.ndarray, Dict]:
    """等权全 A 月频模拟：月末信号，仓位作用于下一月收益（无 T+1 开盘）。"""
    rows = df.filter((pl.col('trade_date') >= a) & (pl.col('trade_date') <= b)
                     & pl.col('ew_ret').is_not_null()).sort('trade_date')
    ds = rows['trade_date'].to_list()
    rs = rows['ew_ret'].to_list()
    if len(ds) < 6:
        return [], np.array([]), {}
    eq = 1.0
    pos = 0.0
    curve = []
    trades = 0
    for i, d in enumerate(ds):
        if i > 0:
            cm = (1 + 0.015) ** (1 / 12) - 1
            eq *= 1 + pos * rs[i] + (1 - pos) * cm
        tgt = targets.get(d)
        if tgt is not None:
            dw = tgt - pos
            if abs(dw) > 1e-9:
                eq *= 1 - abs(dw) * (BUY_COST if dw > 0 else SELL_COST)
                trades += 1
            pos = tgt
        curve.append(eq)
    return ds, np.array(curve), {'trades': trades}


def market_curve_and_stats(df: pl.DataFrame, targets: Dict[date, float],
                           a: date, b: date) -> Tuple[List[date], np.ndarray, Dict]:
    return sim_monthly(df, targets, a, b)


def main() -> int:
    mkt = pl.read_parquet(MKT).sort('trade_date')
    mkt = mkt.filter(pl.col('trade_date') >= date(2000, 1, 1))
    sig = add_pct(mkt)
    sigt = sig.rename({'trade_date': 'signal_date'})
    sig.write_csv(SIG)
    print('signal rows', sig.height, sig['trade_date'].min(), sig['trade_date'].max())
    print('cov_pe min', sig['cov_pe'].min(), 'cov_pb min', sig['cov_pb'].min())

    etfs = {c: load_etf(c) for c in TARGETS if c != 'EW_ALL_A'}

    rows, subs = [], []
    for tgt in TARGETS:
        if tgt == 'EW_ALL_A':
            cal = sig['trade_date'].to_list()
            df = sig.select(['trade_date', 'ew_ret'])
            runner = market_curve_and_stats
        else:
            df = etfs[tgt]
            cal = df['date'].to_list()
            runner = sim_single

        bh_targets = {d: MAX_INVEST for d in month_ends(cal, FULL[0], FULL[1])}
        bh_d, bh_eq, _ = runner(df, bh_targets, FULL[0], FULL[1])
        bh = {w: stats(bh_d, bh_eq, a, b, base=1.0 if bh_d else None)
              for w, a, b in [('full', FULL[0], FULL[1])]}

        for metric in SERIES:
            for win in (60, 120):
                for rule in CORE_RULES + EXTRA_RULES:
                    tg, avg = timing_targets(sigt, cal, FULL[0], FULL[1],
                                             metric, str(win), rule)
                    ds, eq, info = runner(df, tg, FULL[0], FULL[1])
                    cds, ceq, _ = runner(
                        df, {d: avg for d in tg}, FULL[0], FULL[1])
                    variant = f'{metric}_{win}_{rule}'
                    st = stats(ds, eq, FULL[0], FULL[1],
                               base=1.0 if ds and ds[0] <= FULL[0] else None)
                    cst = stats(cds, ceq, FULL[0], FULL[1],
                                base=1.0 if cds and cds[0] <= FULL[0] else None)
                    rows.append({'target': tgt, 'variant': variant,
                                 'metric': metric, 'pct_win': win, 'rule': rule,
                                 'kind': 'timing', 'avg_position': avg,
                                 **st})
                    rows.append({'target': tgt, 'variant': variant,
                                 'metric': metric, 'pct_win': win, 'rule': rule,
                                 'kind': 'control', 'avg_position': avg,
                                 **cst})
                    rows.append({'target': tgt, 'variant': variant,
                                 'metric': metric, 'pct_win': win, 'rule': rule,
                                 'kind': 'buyhold', 'avg_position': MAX_INVEST,
                                 **bh['full']})
                    for pname, pa, pb in SUBPERIODS:
                        s2 = stats(ds, eq, pa, pb)
                        c2 = stats(cds, ceq, pa, pb)
                        subs.append({'target': tgt, 'variant': variant,
                                     'period': pname,
                                     'annual': s2['annual'],
                                     'mdd': s2['max_drawdown'],
                                     'sharpe': s2['sharpe'],
                                     'control_annual': c2['annual'],
                                     'control_mdd': c2['max_drawdown'],
                                     'control_sharpe': c2['sharpe']})
        print('done', tgt, flush=True)

    m = pl.DataFrame(rows, infer_schema_length=None,
                     schema_overrides={'rule': pl.Utf8})
    m.write_csv(MET)
    s = pl.DataFrame(subs, infer_schema_length=None)
    s.write_csv(SUB)

    # 与红利 PB 5 年分位信号的相关/重合度
    div = pl.read_csv(OUT / 'official_valuation_monthly.csv').with_columns(
        pl.col('signal_date').str.to_date())
    div = div.filter(pl.col('index_code') == '000015.SH')
    j = (sig.select(['trade_date', 'pb_agg_pct_60'])
         .join(div.select(['signal_date', 'pb_pct_60']),
               left_on='trade_date', right_on='signal_date', how='inner')
         .drop_nulls())
    j = j.rename({'pb_pct_60': 'div_pb_pct_60',
                  'pb_agg_pct_60': 'mkt_pb_pct_60'})
    corr = float(np.corrcoef(j['mkt_pb_pct_60'], j['div_pb_pct_60'])[0, 1])
    same_full = float(((j['mkt_pb_pct_60'] <= 0.3) &
                       (j['div_pb_pct_60'] <= 0.3)).mean())
    both_high = float(((j['mkt_pb_pct_60'] > 0.3) &
                       (j['div_pb_pct_60'] > 0.3)).mean())
    same_bucket = float((((j['mkt_pb_pct_60'] <= 0.3) &
                          (j['div_pb_pct_60'] <= 0.3)) |
                         ((j['mkt_pb_pct_60'] > 0.3) &
                          (j['div_pb_pct_60'] > 0.3))).mean())
    pl.DataFrame([{'months': j.height, 'corr_pct60': round(corr, 4),
                   'both_full': round(same_full, 4),
                   'both_reduced': round(both_high, 4),
                   'same_bucket': round(same_bucket, 4)}]).write_csv(CORR)
    print('corr vs dividend', round(corr, 4), 'same bucket', round(same_bucket, 4))

    core = m.filter(pl.col('kind') == 'timing')
    for tgt in TARGETS:
        t = core.filter(pl.col('target') == tgt)
        b = m.filter((pl.col('target') == tgt) & (pl.col('kind') == 'buyhold'))['sharpe'][0]
        c = m.filter((pl.col('target') == tgt) & (pl.col('kind') == 'control'))
        tm = t.select(pl.col('sharpe').median(), pl.col('annual').median(),
                      pl.col('max_drawdown').median())
        best = t.sort('sharpe', descending=True).head(1)
        print(tgt, 'median', round(float(tm['sharpe'][0]), 4),
              round(float(tm['annual'][0]), 4), round(float(tm['max_drawdown'][0]), 4),
              '| best', best['variant'][0], round(float(best['sharpe'][0]), 4),
              round(float(best['annual'][0]), 4),
              round(float(best['max_drawdown'][0]), 4),
              '| beat bh', round(float((t['sharpe'] > b).mean()), 3),
              '| beat ctrl', round(float((t['sharpe'] > c['sharpe']).mean()), 3),
              flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
