#!/usr/bin/env python3
"""红利指数官方口径择时（2026-11-21）。

用 tushare ``index_weight`` 的月度官方成分权重（PIT）+ ``daily_basic`` 的个股 pe/pe_ttm/pb，
按权重调和均值重建 000015（上证红利）与 000922（中证红利）的官方口径估值序列，
再用同一套引擎重跑红利 ETF 510880 的 16 变体择时，并与自建代理版对照。

token 只从环境变量 ``TS_TOKEN`` 读取，不落盘。

用法
----
    .venv\\Scripts\\python.exe tools/dividend_official_probe.py
"""
from __future__ import annotations

import bisect
import os
import sys
import time
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))

from broad_beta_probe import (  # noqa: E402
    MAX_INVEST, OUT, RULES, load_etf, month_ends, sim_single, stats,
)

TOKEN = os.environ.get('TS_TOKEN', '')   # 占位：运行时由环境变量注入
W_CACHE = OUT / 'official_index_weight.parquet'
D_CACHE = OUT / 'official_daily_basic_monthend.parquet'

IDX = {'000015.SH': 'official_000015', '000922.CSI': 'official_000922'}
START = {'000015.SH': 2012, '000922.CSI': 2008}
END = 2026
FULL = (date(2015, 1, 1), date(2026, 10, 31))
RECENT = (date(2021, 1, 1), date(2026, 10, 31))
SUBPERIODS = [
    ('2015-2017', date(2015, 1, 1), date(2017, 12, 31)),
    ('2018-2021', date(2018, 1, 1), date(2021, 12, 31)),
    ('2022-2026', date(2022, 1, 1), date(2026, 10, 31)),
]


def _pro():
    import tushare as ts
    if not TOKEN:
        raise RuntimeError('环境变量 TS_TOKEN 未设置')
    return ts.pro_api(TOKEN)


def fetch_weights(pro) -> pl.DataFrame:
    frames = []
    for code in IDX:
        for y in range(START[code], END + 1):
            for attempt in range(2):
                try:
                    df = pro.index_weight(index_code=code,
                                          start_date=f'{y}0101', end_date=f'{y}1231')
                    break
                except Exception as e:
                    if attempt == 1:
                        print('weight ERR', code, y, str(e)[:120])
                        df = None
                    else:
                        time.sleep(0.5)
            if df is not None and df.shape[0]:
                frames.append(df)
    w = pl.from_pandas(__import__('pandas').concat(frames, ignore_index=True))
    return (w.with_columns([
        pl.col('trade_date').str.strptime(pl.Date, '%Y%m%d').alias('d'),
        pl.col('weight').cast(pl.Float64),
    ]).select(['index_code', 'con_code', 'd', 'weight'])
     .unique(subset=['index_code', 'con_code', 'd'], keep='last')
     .sort(['index_code', 'd', 'con_code']))


def fetch_daily(pro, dates: List[date], codes: List[str]) -> pl.DataFrame:
    """逐日 daily_basic。按 <=150 次/分钟节流，失败重试 3 次（指数退避）。"""
    cset = set(codes)
    frames = []
    for i, d in enumerate(dates):
        ds = d.strftime('%Y%m%d')
        df = None
        for attempt in range(3):
            try:
                df = pro.daily_basic(trade_date=ds, fields=(
                    'ts_code,trade_date,pe,pe_ttm,pb,dv_ratio,total_mv'))
                break
            except Exception as e:
                if attempt == 2:
                    print('daily_basic ERR', ds, str(e)[:120], flush=True)
                else:
                    time.sleep(2.0 * (attempt + 1))
        time.sleep(0.4)
        if df is None or df.shape[0] == 0:
            continue
        df = df[df['ts_code'].isin(cset)]
        if df.shape[0]:
            frames.append(df)
        if (i + 1) % 20 == 0:
            print('daily_basic', i + 1, '/', len(dates), flush=True)
    return (pl.from_pandas(__import__('pandas').concat(frames, ignore_index=True))
            .with_columns(pl.col('trade_date').str.strptime(pl.Date, '%Y%m%d').alias('d'))
            .select(['ts_code', 'd', 'pe', 'pe_ttm', 'pb', 'dv_ratio', 'total_mv'])
            .sort(['ts_code', 'd']))


def harmonic(v: np.ndarray, w: np.ndarray) -> Optional[float]:
    m = np.isfinite(v) & (v > 0)
    if not m.any() or w[m].sum() <= 0:
        return None
    return float(w[m].sum() / (w[m] / v[m]).sum())


def build_valuation(w: pl.DataFrame, db: pl.DataFrame) -> pl.DataFrame:
    rows = []
    for code, label in IDX.items():
        wi = w.filter(pl.col('index_code') == code)
        for (d,), g in wi.group_by(['d']):
            x = g.join(db.filter(pl.col('d') == d), left_on='con_code',
                       right_on='ts_code', how='inner')
            if x.height < 10:
                continue
            wt = x['weight'].to_numpy()
            wt = wt / wt.sum()
            cov_pe = float(wt[np.isfinite(x['pe_ttm'].to_numpy()) &
                             (x['pe_ttm'].to_numpy() > 0)].sum())
            dv = x['dv_ratio'].to_numpy()
            m_dv = np.isfinite(dv)
            rows.append({
                'index_code': code, 'label': label, 'signal_date': d,
                'n': x.height, 'wsum': round(float(g['weight'].sum()), 2),
                'pe1': harmonic(x['pe'].to_numpy(), wt),
                'pe2': harmonic(x['pe_ttm'].to_numpy(), wt),
                'pb': harmonic(x['pb'].to_numpy(), wt),
                # 指数股息率 = Σ权重×个股股息率（算术加权），不是调和均值
                'dv': round(float((wt[m_dv] * dv[m_dv]).sum() / wt[m_dv].sum()), 4)
                if m_dv.any() else None,
                'pe_cov': round(cov_pe, 4),
            })
    out = pl.DataFrame(rows, infer_schema_length=None, schema_overrides={
        'pe1': pl.Float64, 'pe2': pl.Float64, 'pb': pl.Float64, 'dv': pl.Float64}).sort(
        ['index_code', 'signal_date'])
    out = out.with_columns(pl.col('signal_date').cast(pl.Date))
    res = []
    for code in IDX:
        sub = out.filter(pl.col('index_code') == code).sort('signal_date')
        vals = {c: sub[c].to_list() for c in ('pe2', 'pb')}
        add = []
        for i in range(sub.height):
            r = {}
            for col in ('pe2', 'pb'):
                v = vals[col][i]
                if v is None:
                    r[f'{col}_pct_exp'] = r[f'{col}_pct_60'] = None
                    continue
                hist = [x for x in vals[col][:i + 1] if x is not None]
                r[f'{col}_pct_exp'] = (round(sum(1 for x in hist if x <= v) / len(hist), 4)
                                       if len(hist) >= 24 else None)
                win = [x for x in vals[col][max(0, i - 59):i + 1] if x is not None]
                r[f'{col}_pct_60'] = (round(sum(1 for x in win if x <= v) / len(win), 4)
                                      if len(win) >= 36 else None)
            add.append(r)
        res.append(sub.hstack(pl.DataFrame(add, infer_schema_length=None, schema_overrides={
            k: pl.Float64 for k in ('pe2_pct_exp', 'pe2_pct_60', 'pb_pct_exp', 'pb_pct_60')})))
    return pl.concat(res).sort(['index_code', 'signal_date'])


def lookup(dates: List[date], vals: List[Optional[float]], t: date) -> Optional[float]:
    i = bisect.bisect_right(dates, t) - 1
    return vals[i] if i >= 0 else None


def run_grid(etf, source: str, val: pl.DataFrame, pct_map: Dict[str, str],
             eval_months: List[date]) -> Tuple[pl.DataFrame, pl.DataFrame]:
    p_dates = val['signal_date'].to_list()
    rows, subs = [], []
    for metric, col in pct_map.items():
        pv = val[col].to_list()
        for rule, fn in RULES.items():
            targets, pos = {}, []
            for d in eval_months:
                p = lookup(p_dates, pv, d)
                w = MAX_INVEST if p is None else MAX_INVEST * fn(p)
                targets[d] = w
                pos.append(w)
            avg = round(float(np.mean(pos)), 4)
            ds, eq, info = sim_single(etf, targets, FULL[0], FULL[1])
            variant = f'{source}_{rule}'
            pl.DataFrame({'date': [str(x) for x in ds], 'equity': eq}).write_csv(
                OUT / f'equity_official_{variant}.csv')
            ds2, eq2, _ = sim_single(etf, {d: avg for d in eval_months}, FULL[0], FULL[1])
            for tag, (a, b) in (('full', FULL), ('recent', RECENT)):
                rows.append({'source': source, 'variant': variant, 'window': tag,
                             'metric': metric,
                             **stats(ds, eq, a, b, base=1.0 if a <= ds[0] else None),
                             'avg_position': avg, 'trades': info.get('trades')})
                rows.append({'source': source, 'variant': variant, 'window': tag,
                             'rule': f'const@{avg}', 'metric': metric,
                             **stats(ds2, eq2, a, b, base=1.0 if a <= ds2[0] else None),
                             'avg_position': avg})
            for pname, pa, pb in SUBPERIODS:
                st = stats(ds, eq, pa, pb)
                subs.append({'source': source, 'variant': variant, 'period': pname,
                             'annual': st['annual'], 'mdd': st['max_drawdown'],
                             'sharpe': st['sharpe'], 'avg_position': avg})
    t = pl.DataFrame(rows, infer_schema_length=None, schema_overrides={'rule': pl.Utf8})
    return t, pl.DataFrame(subs)


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    pro = _pro()
    if W_CACHE.exists():
        w = pl.read_parquet(W_CACHE)
    else:
        w = fetch_weights(pro)
        w.write_parquet(W_CACHE)
    print('weights', w.height, w['d'].min(), w['d'].max(),
          w.group_by('index_code').agg(pl.len()).to_dicts())

    dates = sorted(w['d'].unique().to_list())
    codes = w['con_code'].unique().to_list()
    if D_CACHE.exists():
        db = pl.read_parquet(D_CACHE)
        have = set(db['d'].unique().to_list())
        miss = [d for d in dates if d not in have]
        print('daily_basic cache', db.height, 'rows,', len(have), 'dates, missing', len(miss))
        if miss:
            add = fetch_daily(pro, miss, codes)
            if add.height:
                db = pl.concat([db, add]).unique(
                    subset=['ts_code', 'd'], keep='last').sort(['ts_code', 'd'])
                db.write_parquet(D_CACHE)
                print('daily_basic refilled', add.height, 'rows ->', db.height)
    else:
        db = fetch_daily(pro, dates, codes)
        db.write_parquet(D_CACHE)
    print('daily_basic', db.height, db['d'].n_unique(), 'dates')

    val = build_valuation(w, db)
    val.write_csv(OUT / 'official_valuation_monthly.csv')
    print(val.group_by('index_code').agg([pl.len(), pl.col('pe2').is_not_null().mean().alias('pe_ok'),
                                          pl.col('pb').is_not_null().mean().alias('pb_ok'),
                                          pl.col('pe_cov').min().alias('cov_min')]).sort('index_code'))

    cal = pl.read_csv(OUT / 'official_000015_valuation_recent20.csv', try_parse_dates=True)
    for code, fn in (('000015.SH', 'official_000015_valuation_recent20.csv'),
                     ('000922.CSI', 'official_000922_valuation_recent20.csv')):
        off = pl.read_csv(OUT / fn, try_parse_dates=True)
        day = off.filter(pl.col('日期') == date(2026, 10, 30))
        recon = val.filter((pl.col('index_code') == code) & (pl.col('signal_date') == date(2026, 10, 30)))
        if day.height and recon.height:
            # 中证口径：市盈率1 = TTM，市盈率2 = 最近年报（静态）；与直觉相反，按实测对应
            p1, p2 = float(day['市盈率1'][0]), float(day['市盈率2'][0])
            r1, r2 = float(recon['pe2'][0]), float(recon['pe1'][0])
            print('CALIB', code, 'CSI PE1(TTM)/PE2(静态)', round(p1, 2), round(p2, 2),
                  '| recon ttm/static', round(r1, 2), round(r2, 2),
                  '| diff ttm', round(r1 / p1 - 1, 4), 'static', round(r2 / p2 - 1, 4),
                  '| PB', recon['pb'][0], '| DV', recon['dv'][0],
                  'CSI DV1', float(day['股息率1'][0]), flush=True)

    etf = load_etf('510880.SH')
    eval_months = month_ends(etf['date'].to_list(), FULL[0], FULL[1])

    pct = {'pe': 'pe2_pct_exp', 'pe60': 'pe2_pct_60',
           'pb': 'pb_pct_exp', 'pb60': 'pb_pct_60'}
    # 变体命名保持 metric 维度：pe/pb × exp/60
    maps = {'pe_exp': {'pe': 'pe2_pct_exp'}, 'pe_60': {'pe': 'pe2_pct_60'},
            'pb_exp': {'pb': 'pb_pct_exp'}, 'pb_60': {'pb': 'pb_pct_60'}}

    all_m, all_s = [], []
    for code, label in (('000015.SH', 'official_000015'), ('000922.CSI', 'official_000922')):
        sub = val.filter(pl.col('index_code') == code)
        for name, pm in maps.items():
            t, s = run_grid(etf, f'{label}_{name}', sub, pm, eval_months)
            all_m.append(t)
            all_s.append(s)

    # 代理版同窗口对照
    proxy = pl.read_csv(OUT / 'dividend_proxy_valuation_monthly.csv', try_parse_dates=True)
    pmaps = {'pe_exp': {'pe': 'pe_pct_exp'}, 'pe_60': {'pe': 'pe_pct_60'},
             'pb_exp': {'pb': 'pb_pct_exp'}, 'pb_60': {'pb': 'pb_pct_60'}}
    for name, pm in pmaps.items():
        t, s = run_grid(etf, f'proxy_{name}', proxy, pm, eval_months)
        all_m.append(t)
        all_s.append(s)

    tg_b = {eval_months[0]: MAX_INVEST}
    ds_b, eq_b, _ = sim_single(etf, tg_b, FULL[0], FULL[1])
    pl.DataFrame({'date': [str(x) for x in ds_b], 'equity': eq_b}).write_csv(
        OUT / 'equity_official_buyhold.csv')
    bh_rows, bh_subs = [], []
    for tag, (a, b) in (('full', FULL), ('recent', RECENT)):
        bh_rows.append({'source': 'buyhold', 'variant': 'buyhold', 'window': tag,
                        **stats(ds_b, eq_b, a, b, base=1.0 if a <= ds_b[0] else None),
                        'avg_position': MAX_INVEST})
    for pname, pa, pb in SUBPERIODS:
        st = stats(ds_b, eq_b, pa, pb)
        bh_subs.append({'source': 'buyhold', 'variant': 'buyhold', 'period': pname,
                        'annual': st['annual'], 'mdd': st['max_drawdown'],
                        'sharpe': st['sharpe'], 'avg_position': MAX_INVEST})

    tm = pl.concat([pl.concat(all_m, how='diagonal'),
                    pl.DataFrame(bh_rows, infer_schema_length=None,
                                 schema_overrides={'rule': pl.Utf8})], how='diagonal')
    tm.write_csv(OUT / 'official_timing_metrics.csv')
    pl.concat([pl.concat(all_s, how='diagonal'), pl.DataFrame(bh_subs)]).write_csv(
        OUT / 'official_timing_subperiods.csv')

    # 稳健性：按 source 汇总
    rob = []
    for src in sorted(set(tm['source'].drop_nulls().to_list())):
        for tag in ('full', 'recent'):
            g = tm.filter((pl.col('source') == src) & (pl.col('window') == tag))
            rules = g.filter((pl.col('rule').is_null()) | (~pl.col('rule').str.starts_with('const')))
            if rules.height == 0:
                continue
            cons = g.filter(pl.col('rule').is_not_null() & pl.col('rule').str.starts_with('const'))
            pairs = []
            for r in rules.iter_rows(named=True):
                c = cons.filter(pl.col('variant') == r['variant'])
                pairs.append((r['sharpe'], c['sharpe'][0] if c.height else None))
            sh = [x[0] for x in pairs]
            bh = tm.filter((pl.col('source') == 'buyhold') & (pl.col('window') == tag))
            rob.append({'source': src, 'window': tag, 'n': rules.height,
                        'sharpe_median': round(float(np.median(sh)), 4),
                        'sharpe_min': round(float(np.min(sh)), 4),
                        'sharpe_max': round(float(np.max(sh)), 4),
                        'annual_median': round(float(np.median(rules['annual'].to_list())), 4),
                        'mdd_median': round(float(np.median(rules['max_drawdown'].to_list())), 4),
                        'beat_buyhold': round(float(np.mean([x > bh['sharpe'][0] for x in sh])), 2),
                        'beat_control': round(float(np.mean(
                            [p[1] is not None and p[0] > p[1] for p in pairs])), 2),
                        'buyhold_sharpe': bh['sharpe'][0] if bh.height else None})
    rob_df = pl.DataFrame(rob)
    rob_df.write_csv(OUT / 'official_timing_robustness.csv')
    print(rob_df.select(['source', 'window', 'sharpe_median', 'sharpe_min', 'sharpe_max',
                         'annual_median', 'mdd_median', 'beat_buyhold', 'beat_control']))

    # 代理 vs 官方估值序列差异（同月份对齐）
    cmp_rows = []
    for code in IDX:
        off = val.filter(pl.col('index_code') == code).select([
            'signal_date', pl.col('pe2').alias('pe_off'),
            pl.col('pb').alias('pb_off'),
            pl.col('pe2_pct_exp').alias('pe_off_pct_exp'),
            pl.col('pe2_pct_60').alias('pe_off_pct_60'),
            pl.col('pb_pct_exp').alias('pb_off_pct_exp'),
            pl.col('pb_pct_60').alias('pb_off_pct_60')])
        j = proxy.join(off, on='signal_date', how='inner')
        for metric, pc, pc60, oc, oc60, pe_, oe_ in (
                ('pe', 'pe', 'pe_pct_60', 'pe_off', 'pe_off_pct_60',
                 'pe_pct_exp', 'pe_off_pct_exp'),
                ('pb', 'pb', 'pb_pct_60', 'pb_off', 'pb_off_pct_60',
                 'pb_pct_exp', 'pb_off_pct_exp')):
            jj = j.drop_nulls([pc, oc])
            if jj.height < 12:
                continue
            diff = (jj[oe_] - jj[pe_]).abs()
            j6 = j.drop_nulls([pc60, oc60])
            cmp_rows.append({
                'index_code': code, 'metric': metric, 'months': jj.height,
                'corr': round(float(np.corrcoef(jj[pc], jj[oc])[0, 1]), 4),
                'level_ratio_median': round(float((jj[oc] / jj[pc]).median()), 4),
                'mape_median': round(float(((jj[oc] - jj[pc]).abs() / jj[pc]).median()), 4),
                'pct_mae_exp': round(float(diff.median()), 4),
                'pct_gt20_share': round(float((diff > 0.2).mean()), 3),
                'pct_mae_60': round(float((j6[oc60] - j6[pc60]).abs().median()), 4)
                if j6.height >= 12 else None,
            })
    pl.DataFrame(cmp_rows, infer_schema_length=None).write_csv(
        OUT / 'proxy_vs_official_valuation.csv')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
