#!/usr/bin/env python3
"""红利指数（上证红利 000015）估值分位择时——自建 PIT 近似（2026-11-21 补测）。

缺口与解法
----------
000015/000922 不在 ``index_weight_monthly``，本地没有 PIT 成分权重，也没有官方 PE/PB。
本地 ``valuation_daily`` 的 pe/pb 在 2015-01 与 2018-01 换过数据源，对高股息篮子会
产生 +24%（PE）/ +36%（PB）的伪跳变（同一时点全市场中位数只动 3%/-10%），直接拿来做
分位会把「源切换」当成「估值突变」。所以本脚本 **不用** 这两列，改为：

- 成分（近似）：沪市 A 股（剔除 900 开头 B 股），月末要求 股息率>0、收盘价>0，
  流通市值不低于当日沪市 A 股中位数（官方按成交额前 80% 筛流动性），
  按股息率降序取前 50，按股息率加权（官方有单只权重上限，此处未复刻）；
- 个股 PE = 总市值 / 最近一期已公告的 TTM 归母前净利润（``income.parquet`` 按 ann_date PIT，
  TTM = 上年年报 + 本期累计 − 上年同期累计）；
- 个股 PB = 总市值 / 最近一期已公告的总权益（``balance.parquet`` 按 ann_date PIT）；
- 指数 PE/PB = 上述个股的正值按股息率加权调和均值。

这样 2006-2026 全程同一套定义，没有源切换断点；代价是 PE 只纳入正盈利成分、
绝对水平与中证官方口径不同，只能比历史分位。

股息率：``dv_ratio``，2015~2017 缺失段用 ``valuation_pre2018_patch.parquet`` 的 ``dp`` 补
（依据 ``meta/research_inputs/manifest.json`` 的 basis.dividend_patch：dp→dv_ratio，同量纲）。

已知口径差
----------
1. 无历史 ST 快照（``st_status_history`` 只有 2026-11 截面），未剔除历史 ST；
2. 未复刻「过去三年连续分红」条件，只用当日股息率 > 0 近似；
3. 用流通市值中位数替代官方成交额前 80% 的流动性筛选，未复刻单只权重上限；
4. PE 只纳入正盈利成分；与官方指数估值不可比绝对水平，只比分位。

覆盖与窗口
----------
代理估值：2006-01 ~ 2026-10（月度 250 期）。
择时评估：2010-01-01 ~ 2026-10-31（16.8 年，所有变体都有 >=36 个月分位历史）；
另给 2021-01-01 ~ 2026-10-31（5.8 年）作为近年窗口。

用法
----
    .venv\\Scripts\\python.exe tools/dividend_timing_probe.py
"""
from __future__ import annotations

import bisect
import sys
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

VAL_PATH = ROOT / 'data/local_store/meta/financials/valuation_daily.parquet'
PATCH_PATH = ROOT / 'data/local_store/meta/financials/valuation_pre2018_patch.parquet'
INC_PATH = ROOT / 'data/local_store/meta/financials/income.parquet'
BAL_PATH = ROOT / 'data/local_store/meta/financials/balance.parquet'
IDX_PATH = ROOT / 'data/local_store/quotes/index/sh/000015.SH.parquet'
ETF_CODE = '510880.SH'
HIST_START = date(2006, 1, 1)
FULL = (date(2010, 1, 1), date(2026, 10, 31))
RECENT = (date(2021, 1, 1), date(2026, 10, 31))
SUBPERIODS = [
    ('2010-2013', date(2010, 1, 1), date(2013, 12, 31)),
    ('2014-2017', date(2014, 1, 1), date(2017, 12, 31)),
    ('2018-2021', date(2018, 1, 1), date(2021, 12, 31)),
    ('2022-2026', date(2022, 1, 1), date(2026, 10, 31)),
]


# ---------------------------------------------------------------------------
# 数据装载
# ---------------------------------------------------------------------------

def load_snapshot(sig_days: List[date]) -> pl.DataFrame:
    """月末截面：股息率（patch 补 2015-2017）、流通市值、总市值、原始 pe/pb（仅作校验）。"""
    val = (pl.scan_parquet(VAL_PATH)
           .filter(pl.col('ts_code').str.ends_with('.SH')
                   & ~pl.col('ts_code').str.starts_with('900')
                   & pl.col('trade_date').is_in(sig_days))
           .select(['ts_code', 'trade_date', 'close', 'total_mv', 'circ_mv',
                    'dv_ratio', 'pe', 'pb'])
           .collect())
    patch = pl.read_parquet(PATCH_PATH).select(
        ['ts_code', 'trade_date', 'dp']).rename({'dp': 'dp_patch'})
    return val.join(patch, on=['ts_code', 'trade_date'], how='left').with_columns(
        pl.coalesce(['dv_ratio', 'dp_patch']).alias('dv'))


def _dedup_reports(df: pl.DataFrame, value: str) -> pl.DataFrame:
    """同一 (code, end_date) 只用最新公告日那一版的数值。"""
    return (df.drop_nulls(['ann_date', value])
            .sort(['ts_code', 'end_date', 'ann_date'])
            .group_by(['ts_code', 'end_date'])
            .agg([pl.col('ann_date').last(), pl.col(value).last()]))


def load_fundamentals() -> pl.DataFrame:
    """每期报表的 TTM 净利润与总权益，拼成一张可按 ann_date 做 asof 的表。"""
    inc = (pl.read_parquet(INC_PATH, columns=['ts_code', 'end_date', 'ann_date', 'net_profit'])
           .filter(pl.col('ts_code').str.ends_with('.SH')
                   & ~pl.col('ts_code').str.starts_with('900')))
    inc = _dedup_reports(inc, 'net_profit')
    f = inc.with_columns([
        pl.col('end_date').dt.year().alias('y'),
        pl.col('end_date').dt.month().alias('m'),
    ])
    annual = (f.filter(pl.col('m') == 12)
              .select(['ts_code', 'y', pl.col('net_profit').alias('np_annual')]))
    # 非年报期只能用「上一年」的年报：把 annual 的年份 +1 后按当期行 join
    prev_annual = annual.select(['ts_code', (pl.col('y') + 1).alias('y'),
                                 pl.col('np_annual').alias('np_prev_annual')])
    prev = (f.select(['ts_code', (pl.col('y') + 1).alias('y'), 'm',
                      pl.col('net_profit').alias('np_prev')]))

    bal = (pl.read_parquet(BAL_PATH, columns=['ts_code', 'end_date', 'ann_date', 'total_equity'])
           .filter(pl.col('ts_code').str.ends_with('.SH')
                   & ~pl.col('ts_code').str.starts_with('900')))
    bal = _dedup_reports(bal, 'total_equity').select(
        ['ts_code', 'end_date', 'total_equity'])

    out = (f.join(prev_annual, on=['ts_code', 'y'], how='left')
           .join(prev, on=['ts_code', 'y', 'm'], how='left')
           .join(bal, on=['ts_code', 'end_date'], how='left')
           .with_columns(
               pl.when(pl.col('m') == 12).then(pl.col('net_profit'))
               .otherwise(pl.col('np_prev_annual') + pl.col('net_profit') - pl.col('np_prev'))
               .alias('np_ttm'))
           .select(['ts_code', 'ann_date', 'end_date', 'np_ttm', 'total_equity'])
           .drop_nulls('ann_date')
           # asof join 要求每个 (code, ann_date) 只有一行：同日披露多期时取最新报告期
           .sort(['ts_code', 'ann_date', 'end_date'])
           .group_by(['ts_code', 'ann_date'])
           .agg([pl.col('np_ttm').last(), pl.col('total_equity').last()])
           .sort(['ts_code', 'ann_date']))
    return out


def harmonic(values: np.ndarray, weights: np.ndarray) -> Optional[float]:
    m = np.isfinite(values) & (values > 0)
    if not m.any() or weights[m].sum() <= 0:
        return None
    return float(weights[m].sum() / (weights[m] / values[m]).sum())


# ---------------------------------------------------------------------------
# 代理组合与估值序列
# ---------------------------------------------------------------------------

def build_proxy(snap: pl.DataFrame, fund: pl.DataFrame,
                sig_days: List[date]) -> pl.DataFrame:
    by_date = {k[0]: v for k, v in snap.partition_by('trade_date', as_dict=True).items()}
    pairs = snap.select(['ts_code', 'trade_date']).sort(['ts_code', 'trade_date'])
    j = pairs.join_asof(fund, left_on='trade_date', right_on='ann_date',
                        by='ts_code', strategy='backward')

    rows = []
    for t in sig_days:
        day = by_date.get(t)
        if day is None:
            continue
        day = day.filter((pl.col('dv') > 0) & (pl.col('close') > 0)
                         & pl.col('circ_mv').is_not_null()
                         & pl.col('total_mv').is_not_null())
        if day.height < 100:
            continue
        day = day.filter(pl.col('circ_mv') >= float(day['circ_mv'].quantile(0.5)))
        top = day.sort('dv', descending=True).head(50)
        if top.height < 20:
            continue
        f = j.filter(pl.col('trade_date') == t).select(
            ['ts_code', 'np_ttm', 'total_equity'])
        top = top.join(f, on='ts_code', how='left')
        w = top['dv'].to_numpy()
        w = w / w.sum()
        mv = top['total_mv'].to_numpy()
        pe_i = np.where(top['np_ttm'].to_numpy() > 0,
                        mv / top['np_ttm'].to_numpy(), np.nan)
        pb_i = np.where(top['total_equity'].to_numpy() > 0,
                        mv / top['total_equity'].to_numpy(), np.nan)
        pe_mkt = top['pe'].to_numpy()
        pb_mkt = top['pb'].to_numpy()
        rows.append({
            'signal_date': t, 'n_constituents': top.height,
            'pe': harmonic(pe_i, w), 'pb': harmonic(pb_i, w),
            'pe_weight_cov': round(float(w[np.isfinite(pe_i) & (pe_i > 0)].sum()), 4),
            'pb_weight_cov': round(float(w[np.isfinite(pb_i) & (pb_i > 0)].sum()), 4),
            'pe_mkt': harmonic(pe_mkt, w), 'pb_mkt': harmonic(pb_mkt, w),
            'top_dv': round(float(top['dv'][0]), 4),
            'median_dv': round(float(top['dv'].median()), 4),
            'min_circ_mv_yi': round(float(top['circ_mv'].min()) / 1e8, 2),
        })
    out = pl.DataFrame(rows, infer_schema_length=None, schema_overrides={
        'pe': pl.Float64, 'pb': pl.Float64,
        'pe_mkt': pl.Float64, 'pb_mkt': pl.Float64}).sort('signal_date')

    res = []
    series = {c: out[c].to_list() for c in ('pe', 'pb')}
    for i in range(out.height):
        r = {'pe_pct_exp': None, 'pe_pct_60': None,
             'pb_pct_exp': None, 'pb_pct_60': None}
        for col in ('pe', 'pb'):
            vals = series[col]
            v = vals[i]
            if v is None:
                continue
            hist = [x for x in vals[:i + 1] if x is not None]
            if len(hist) >= 24:
                r[f'{col}_pct_exp'] = round(
                    sum(1 for x in hist if x <= v) / len(hist), 4)
            win = [x for x in vals[max(0, i - 59):i + 1] if x is not None]
            if len(win) >= 36:
                r[f'{col}_pct_60'] = round(
                    sum(1 for x in win if x <= v) / len(win), 4)
        res.append(r)
    out = out.hstack(pl.DataFrame(res, infer_schema_length=None, schema_overrides={
        'pe_pct_exp': pl.Float64, 'pe_pct_60': pl.Float64,
        'pb_pct_exp': pl.Float64, 'pb_pct_60': pl.Float64}))
    return out


def validate(snap: pl.DataFrame, sig_days: List[date]) -> Tuple[pl.DataFrame, dict]:
    """代理组合的月度价格收益 vs 000015 指数月收益。"""
    by_date = {k[0]: v for k, v in snap.partition_by('trade_date', as_dict=True).items()}
    idx = pl.read_parquet(IDX_PATH, columns=['date', 'close']).sort('date')
    imap = dict(zip(idx['date'].to_list(), idx['close'].to_list()))
    rows = []
    for i in range(len(sig_days) - 1):
        t0, t1 = sig_days[i], sig_days[i + 1]
        if t0 < HIST_START or t1 > FULL[1]:
            continue
        d0, d1 = by_date.get(t0), by_date.get(t1)
        if d0 is None or d1 is None:
            continue
        day = d0.filter((pl.col('dv') > 0) & (pl.col('close') > 0)
                        & pl.col('circ_mv').is_not_null())
        if day.height < 100:
            continue
        day = day.filter(pl.col('circ_mv') >= float(day['circ_mv'].quantile(0.5)))
        b = day.sort('dv', descending=True).head(50).select(['ts_code', 'close', 'dv'])
        p1 = d1.select(['ts_code', 'close']).rename({'close': 'close1'})
        jj = b.join(p1, on='ts_code').filter(
            (pl.col('close') > 0) & (pl.col('close1') > 0))
        if jj.height < 40 or t0 not in imap or t1 not in imap:
            continue
        w = jj['dv'].to_numpy()
        w = w / w.sum()
        r = float((w * (jj['close1'].to_numpy() / jj['close'].to_numpy() - 1)).sum())
        rows.append({'date': t1, 'proxy_ret': r, 'index_ret': imap[t1] / imap[t0] - 1})
    d = pl.DataFrame(rows)
    if d.height < 12:
        return d, {}
    pr, ir = d['proxy_ret'].to_numpy(), d['index_ret'].to_numpy()
    return d, {
        'months': d.height,
        'corr_monthly': round(float(np.corrcoef(pr, ir)[0, 1]), 4),
        'te_ann': round(float(np.std(pr - ir, ddof=1) * np.sqrt(12)), 4),
        'proxy_ann': round(float((1 + pr).prod() ** (12 / d.height) - 1), 4),
        'index_ann': round(float((1 + ir).prod() ** (12 / d.height) - 1), 4),
        'excess_ann': round(float((1 + pr).prod() ** (12 / d.height)
                                  - (1 + ir).prod() ** (12 / d.height)), 4),
    }


def pe_crosscheck(proxy: pl.DataFrame) -> dict:
    """自建 PE/PB 与本地市场 PE/PB（2018 起单一源）的秩相关。"""
    d = proxy.filter(pl.col('signal_date') >= date(2018, 1, 1)).drop_nulls(
        ['pe', 'pe_mkt', 'pb', 'pb_mkt'])
    if d.height < 24:
        return {}
    def rank_corr(a: str, b: str) -> float:
        x = d[a].rank().to_numpy()
        y = d[b].rank().to_numpy()
        return round(float(np.corrcoef(x, y)[0, 1]), 4)
    return {'months': d.height,
            'pe_rank_corr': rank_corr('pe', 'pe_mkt'),
            'pb_rank_corr': rank_corr('pb', 'pb_mkt'),
            'pe_median_self': round(float(d['pe'].median()), 3),
            'pe_median_mkt': round(float(d['pe_mkt'].median()), 3),
            'pb_median_self': round(float(d['pb'].median()), 3),
            'pb_median_mkt': round(float(d['pb_mkt'].median()), 3)}


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def lookup_pct(series_dates: List[date], vals: List[Optional[float]],
               target: date) -> Optional[float]:
    i = bisect.bisect_right(series_dates, target) - 1
    return vals[i] if i >= 0 else None


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    etf = load_etf(ETF_CODE)
    idx_dates = pl.read_parquet(IDX_PATH, columns=['date'])['date'].to_list()
    sig = month_ends(idx_dates, HIST_START, FULL[1])

    snap = load_snapshot(sig)
    fund = load_fundamentals()
    proxy = build_proxy(snap, fund, sig)
    proxy.write_csv(OUT / 'dividend_proxy_valuation_monthly.csv')
    print('proxy months', proxy.height, proxy['signal_date'][0], proxy['signal_date'][-1])

    vd, vsum = validate(snap, sig)
    vd.write_csv(OUT / 'dividend_proxy_validation.csv')
    print('validation', vsum)
    print('pe crosscheck', pe_crosscheck(proxy))

    cal = etf['date'].to_list()
    first = max(FULL[0], etf['date'][0])
    eval_months = month_ends(cal, first, FULL[1])
    p_dates = proxy['signal_date'].to_list()

    rows = []
    sub_rows = []
    for metric in ('pe', 'pb'):
        for mode in ('exp', '60'):
            col = f'{metric}_pct_{mode}'
            pv = proxy[col].to_list()
            for rule in RULES:
                fn = RULES[rule]
                targets, pos = {}, []
                for d in eval_months:
                    p = lookup_pct(p_dates, pv, d)
                    w = MAX_INVEST if p is None else MAX_INVEST * fn(p)
                    targets[d] = w
                    pos.append(w)
                avg = float(np.mean(pos))
                avg_r = round(avg, 3)
                ds, eq, info = sim_single(etf, targets, first, FULL[1])
                variant = f'{metric}_{mode}_{rule}'
                pl.DataFrame({'date': [str(x) for x in ds], 'equity': eq}).write_csv(
                    OUT / f'equity_divtiming_{variant}.csv')

                tg_c = {d: avg_r for d in eval_months}
                ds2, eq2, _ = sim_single(etf, tg_c, first, FULL[1])

                for tag, (a, b) in (('full', FULL), ('recent', RECENT)):
                    rows.append({'variant': variant, 'metric': metric, 'pct_mode': mode,
                                 'rule': rule, 'window': tag,
                                 **stats(ds, eq, a, b, base=1.0 if a <= ds[0] else None),
                                 'avg_position': round(avg, 4),
                                 'trades': info.get('trades'),
                                 'cost_pct': info.get('cost_pct')})
                    rows.append({'variant': variant, 'metric': metric, 'pct_mode': mode,
                                 'rule': f'const@{avg_r:.3f}', 'window': tag,
                                 **stats(ds2, eq2, a, b,
                                         base=1.0 if a <= ds2[0] else None),
                                 'avg_position': avg_r, 'trades': None})
                for pname, pa, pb in SUBPERIODS:
                    st = stats(ds, eq, pa, pb)
                    sub_rows.append({'variant': variant, 'period': pname,
                                     'annual': st['annual'], 'mdd': st['max_drawdown'],
                                     'sharpe': st['sharpe'], 'avg_position': round(avg, 4)})

    tg_b = {eval_months[0]: MAX_INVEST}
    ds_b, eq_b, _ = sim_single(etf, tg_b, first, FULL[1])
    pl.DataFrame({'date': [str(x) for x in ds_b], 'equity': eq_b}).write_csv(
        OUT / 'equity_divtiming_buyhold.csv')
    for tag, (a, b) in (('full', FULL), ('recent', RECENT)):
        rows.append({'variant': 'buyhold', 'metric': '', 'pct_mode': '', 'rule': 'buyhold',
                     'window': tag,
                     **stats(ds_b, eq_b, a, b, base=1.0 if a <= ds_b[0] else None),
                     'avg_position': MAX_INVEST, 'trades': 1})
    for pname, pa, pb in SUBPERIODS:
        st = stats(ds_b, eq_b, pa, pb)
        sub_rows.append({'variant': 'buyhold', 'period': pname,
                         'annual': st['annual'], 'mdd': st['max_drawdown'],
                         'sharpe': st['sharpe'], 'avg_position': MAX_INVEST})

    tm = pl.DataFrame(rows)
    tm.write_csv(OUT / 'dividend_timing_metrics.csv')
    pl.DataFrame(sub_rows).write_csv(OUT / 'dividend_timing_subperiods.csv')

    is_rule = (~pl.col('rule').is_in(['buyhold'])) & (~pl.col('rule').str.starts_with('const@'))
    rob = []
    for tag in ('full', 'recent'):
        g = tm.filter(pl.col('window') == tag)
        rules = g.filter(is_rule)
        bh = g.filter(pl.col('rule') == 'buyhold')
        pairs = []
        for r in rules.iter_rows(named=True):
            c = g.filter((pl.col('variant') == r['variant'])
                         & pl.col('rule').str.starts_with('const@'))
            pairs.append((r['sharpe'], c['sharpe'][0] if c.height else None))
        sh = [p[0] for p in pairs]
        rob.append({
            'window': tag, 'n_variants': rules.height,
            'sharpe_median': round(float(np.median(sh)), 4),
            'sharpe_min': round(float(np.min(sh)), 4),
            'sharpe_max': round(float(np.max(sh)), 4),
            'annual_median': round(float(np.median(rules['annual'].to_list())), 4),
            'mdd_median': round(float(np.median(rules['max_drawdown'].to_list())), 4),
            'share_beat_buyhold': round(float(np.mean([p[0] > bh['sharpe'][0] for p in pairs])), 2),
            'share_beat_control': round(float(np.mean(
                [p[1] is not None and p[0] > p[1] for p in pairs])), 2),
            'buyhold_annual': bh['annual'][0], 'buyhold_mdd': bh['max_drawdown'][0],
            'buyhold_sharpe': bh['sharpe'][0],
        })
    rob_df = pl.DataFrame(rob)
    rob_df.write_csv(OUT / 'dividend_timing_robustness.csv')
    print(rob_df)

    for tag in ('full', 'recent'):
        best = (tm.filter(is_rule & (pl.col('window') == tag))
                .sort('sharpe', descending=True).head(3))
        print('best', tag, best.select(['variant', 'annual', 'max_drawdown',
                                        'sharpe', 'avg_position']))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
