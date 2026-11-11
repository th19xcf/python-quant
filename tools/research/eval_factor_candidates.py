#!/usr/bin/env python
"""候选因子增量评估（只读 data/，只写 _staging/ 与 reports/）。

目的：在现有 28 个在用因子之外，实算第一批主流候选因子，对 28 因子做
逐日横截面正交化，给出 h=5/20/60 的 raw IC、残差 IC、增量 ΔR²，并按增量
信息排序分档。

口径：
- universe / PIT / 过滤沿用 ``_staging/factor_oos_verify_20261111/run_oos.py``：
  全 A（剔北交所）、剔 ST/退、上市满 250 交易日、停牌不可交易、T+1 开盘
  涨跌停不可成交；标签为 T+1 开盘 -> T+1+h 收盘。
- 收益区间 IS 2015-01~2021-12，OOS 2022-01~2026-10。
- 现有 28 因子 = 16 legacy（读 panel_cache）+ 12 extended（本次补算，
  抄生产 ``compute_extended_candidates`` 口径）。
- 候选因子全部只用当日及以前数据；财务因子严格 ann_date 可见性 + TTM 校验。

运行：
    .venv\\Scripts\\python.exe tools/research/eval_factor_candidates.py --stage all
    ... --stage price --max-stocks 300   # 小样本自检
"""
from __future__ import annotations

import argparse
import glob
import json
import math
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path

import numpy as np
import polars as pl
from scipy.stats import rankdata, spearmanr
from scipy.stats import t as t_dist

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
# 中间产物（分块 parquet / progress.json）落 _staging，不进仓库、不进任何在跑链路
OUT = ROOT / '_staging' / 'factor_candidates_20261111'
OUT.mkdir(parents=True, exist_ok=True)
PARTS = OUT / 'price_parts'
REPORTS = ROOT / 'reports' / 'factor_candidates_20261111'
REPORTS.mkdir(parents=True, exist_ok=True)

from src.advice.extended_factors import compute_extended_candidates  # noqa: E402

WARMUP_START = date(2013, 1, 1)
EVAL_START = date(2015, 1, 5)
END = date(2026, 10, 31)
IS_END = date(2021, 12, 31)
HORIZONS = (5, 20, 60)
MIN_NAMES = 100
MIN_FACTORS_PRESENT = 20  # 正交化时每只股票至少有多少个现有因子非空

LEGACY_16 = [
    'momentum_1m', 'momentum_3m', 'momentum_6m', 'momentum_12m',
    'trend_slope_60', 'ma20_gap', 'rsi_14', 'sharpe_ratio_20',
    'volatility_20', 'max_drawdown_20', 'atr_pct', 'volume_momentum_20',
    'turnover_20', 'ff_div_yield', 'ff_value_bp', 'ff_sales_yield',
]
EXTENDED_12 = [
    'qlib_rsqr20', 'qlib_rsqr10', 'gtja_003', 'gtja_024', 'a101_024',
    'gtja_010', 'a101_012', 'ff_amihud_60', 'a101_020', 'a101_042',
    'gtja_012', 'gtja_041',
]
EXISTING = LEGACY_16 + EXTENDED_12

# 候选定义：列名 -> (族, 构造, 数据依赖)
CANDIDATE_DEFS: dict[str, dict[str, str]] = {
    # 质量
    'q_roe_ttm': {'family': '质量', 'formula': '归母净利TTM / 期末归母净资产',
                      'dep': 'income.parent_net_profit + balance.total_equity，PIT ann_date'},
    'q_roa_ttm': {'family': '质量', 'formula': '归母净利TTM / 期末总资产',
                      'dep': 'income.parent_net_profit + balance.total_assets，PIT'},
    'q_gross_margin': {'family': '质量', 'formula': '毛利TTM / 营收TTM',
                           'dep': 'income.gross_profit/revenue，PIT；金融业毛利缺失'},
    'q_asset_turnover': {'family': '质量', 'formula': '营收TTM / 期末总资产',
                             'dep': 'income.revenue + balance.total_assets，PIT'},
    'q_accruals': {'family': '质量', 'formula': '(归母净利TTM - 经营现金流TTM) / 总资产',
                       'dep': 'income + cashflow.operating_cf + balance，PIT'},
    'q_ocf_to_profit': {'family': '质量', 'formula': '经营现金流TTM / 归母净利TTM（净利<=0 记空）',
                            'dep': 'cashflow.operating_cf + income.parent_net_profit，PIT'},
    # 成长
    'g_revenue_yoy': {'family': '成长', 'formula': '本期累计营收 / 上年同期累计 - 1',
                          'dep': 'income.revenue，上年同期 ann_date 必须早于本期'},
    'g_profit_yoy': {'family': '成长', 'formula': '(归母净利 - 上年同期) / |上年同期|',
                         'dep': 'income.parent_net_profit，上年同期 PIT 可见'},
    'g_sue': {'family': '成长',
                  'formula': '(单季净利 - 去年同季) / std(过去8季季节差分)，PIT 校验',
                  'dep': 'income.parent_net_profit 累计值，至少 6 个季度'},
    # 价值
    'v_ep': {'family': '价值', 'formula': '归母净利TTM / 当日总市值',
                 'dep': 'income + valuation_daily.total_mv'},
    'v_cfp': {'family': '价值', 'formula': '经营现金流TTM / 当日总市值',
                  'dep': 'cashflow.operating_cf + valuation_daily.total_mv'},
    'v_sp': {'family': '价值', 'formula': '营收TTM / 当日总市值',
                 'dep': 'income.revenue + valuation_daily.total_mv'},
    # 波动与彩票
    'v_ivol_60': {'family': '波动/彩票',
                      'formula': 'CAPM 残差收益（60日滚动 beta）的 60 日标准差',
                      'dep': '个股日收益 + 000001.SH 指数日收益'},
    'v_max_5d_21': {'family': '波动/彩票',
                        'formula': '过去 21 日窗口内 5 日收益的最大值',
                        'dep': '个股日线'},
    'v_downside_vol_60': {'family': '波动/彩票',
                              'formula': '60 日内负收益半方差开方',
                              'dep': '个股日线'},
    'v_skew_60': {'family': '波动/彩票', 'formula': '60 日收益偏度', 'dep': '个股日线'},
    # 流动性
    'l_amihud_20': {'family': '流动性', 'formula': 'mean(|日收益| / 成交额)，20 日',
                        'dep': '个股日线 amount；已有 ff_amihud_60 作对照'},
    # 动量与反转
    'm_mom_12_1': {'family': '动量/反转', 'formula': 'close[t-21] / close[t-252] - 1（跳过近一月）',
                       'dep': '个股日线，252 日预热'},
    'm_resid_mom_12_1': {'family': '动量/反转',
                             'formula': '120日滚动 beta 的 CAPM 残差收益，过去 252 日剔除近 21 日累加',
                             'dep': '个股日线 + 指数日线'},
    'm_dist_52w_high': {'family': '动量/反转', 'formula': 'close / 252日最高价 - 1',
                            'dep': '个股日线'},
    'm_reversal_5': {'family': '动量/反转', 'formula': 'close / close[t-5] - 1',
                         'dep': '个股日线'},
}
PRICE_CANDIDATES = ['v_ivol_60', 'v_max_5d_21', 'v_downside_vol_60', 'v_skew_60',
                    'l_amihud_20', 'm_mom_12_1', 'm_resid_mom_12_1',
                    'm_dist_52w_high', 'm_reversal_5']
FIN_CANDIDATES = ['q_roe_ttm', 'q_roa_ttm', 'q_gross_margin', 'q_asset_turnover',
                  'q_accruals', 'q_ocf_to_profit', 'g_revenue_yoy', 'g_profit_yoy',
                  'g_sue', 'v_ep', 'v_cfp', 'v_sp']
CANDIDATES = list(CANDIDATE_DEFS)


def log(msg: str) -> None:
    print(f'[{datetime.now():%H:%M:%S}] {msg}', flush=True)


def _progress(stage: str, status: str, **kw) -> None:
    p = OUT / 'progress.json'
    data = json.loads(p.read_text(encoding='utf-8')) if p.exists() else {}
    data.setdefault(stage, {})
    data[stage].update({'status': status, 'ts': datetime.now().isoformat(timespec='seconds'), **kw})
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')


# ---------------------------------------------------------------------------
# 1. 价格类因子
# ---------------------------------------------------------------------------
def market_returns() -> pl.DataFrame:
    idx = pl.read_parquet(ROOT / 'data/local_store/quotes/index/sh/000001.SH.parquet',
                          columns=['date', 'close', 'pre_close'])
    return idx.with_columns(
        (pl.col('close') / pl.col('pre_close') - 1.0).alias('mkt_ret')
    ).select(['date', 'mkt_ret'])


def compute_price_candidates(df: pl.DataFrame) -> pl.DataFrame:
    """单批股票的价格类候选（内部按 ts_code 分组滚动）。"""
    g = 'ts_code'
    raw_close = pl.col('close')
    df = df.with_columns([
        (pl.col('open') * pl.col('hfq_factor')).alias('open'),
        (pl.col('high') * pl.col('hfq_factor')).alias('high'),
        (pl.col('low') * pl.col('hfq_factor')).alias('low'),
        (pl.col('close') * pl.col('hfq_factor')).alias('close'),
        (pl.col('pre_close') * pl.col('hfq_factor')).alias('pre_close'),
        raw_close.alias('raw_close'),
    ]).drop('hfq_factor')

    # 12 个在用 extended 因子（生产同口径）
    df = compute_extended_candidates(df, columns=tuple(EXTENDED_12))
    df = df.sort([g, 'date'])

    r = pl.col('close') / pl.col('close').shift(1).over(g) - 1.0
    m = pl.col('mkt_ret')
    df = df.with_columns([r.alias('_r'), m.alias('_m')])

    mean_r = pl.col('_r').rolling_mean(window_size=60, min_samples=40).over(g)
    mean_m = pl.col('_m').rolling_mean(window_size=60, min_samples=40).over(g)
    cov = (pl.col('_r') * pl.col('_m')).rolling_mean(window_size=60, min_samples=40).over(g) \
        - mean_r * mean_m
    var_m = (pl.col('_m') * pl.col('_m')).rolling_mean(window_size=60, min_samples=40).over(g) \
        - mean_m * mean_m
    beta60 = cov / var_m
    resid60 = pl.col('_r') - beta60 * pl.col('_m')
    ivol = resid60.rolling_std(window_size=60, min_samples=40).over(g)

    mean_r120 = pl.col('_r').rolling_mean(window_size=120, min_samples=80).over(g)
    mean_m120 = pl.col('_m').rolling_mean(window_size=120, min_samples=80).over(g)
    cov120 = (pl.col('_r') * pl.col('_m')).rolling_mean(window_size=120, min_samples=80).over(g) \
        - mean_r120 * mean_m120
    var_m120 = (pl.col('_m') * pl.col('_m')).rolling_mean(window_size=120, min_samples=80).over(g) \
        - mean_m120 * mean_m120
    resid120 = pl.col('_r') - (cov120 / var_m120) * pl.col('_m')

    ret5 = pl.col('close') / pl.col('close').shift(5).over(g) - 1.0
    dr2 = pl.when(pl.col('_r') < 0).then(pl.col('_r') * pl.col('_r')).otherwise(0.0)
    amihud = pl.when(pl.col('amount') > 0).then(
        pl.col('_r').abs() / pl.col('amount')).otherwise(None)

    df = df.with_columns([
        ivol.alias('v_ivol_60'),
        ret5.rolling_max(window_size=21, min_samples=15).over(g).alias('v_max_5d_21'),
        dr2.rolling_mean(window_size=60, min_samples=40).over(g).sqrt()
        .alias('v_downside_vol_60'),
        pl.col('_r').rolling_skew(window_size=60).over(g).alias('v_skew_60'),
        amihud.rolling_mean(window_size=20, min_samples=15).over(g).alias('l_amihud_20'),
        (pl.col('close').shift(21).over(g) / pl.col('close').shift(252).over(g) - 1.0)
        .alias('m_mom_12_1'),
        resid120.rolling_sum(window_size=231, min_samples=200).over(g).shift(21).over(g)
        .alias('m_resid_mom_12_1'),
        (pl.col('close') / pl.col('close').rolling_max(window_size=252, min_samples=200)
         .over(g) - 1.0).alias('m_dist_52w_high'),
        ret5.alias('m_reversal_5'),
    ])
    return df


def stage_price(max_stocks: int | None = None, batch: int = 300) -> None:
    _progress('price', 'running')
    files = sorted(glob.glob(str(ROOT / 'data/local_store/quotes/stock/*/*.parquet')))
    files = [f for f in files if not f.endswith('.BJ.parquet')]
    if max_stocks:
        files = files[:max_stocks]
    mkt = market_returns()

    # 调仓日：评估区间内每 5 个交易日
    base = pl.read_parquet(OUT.parent / 'factor_oos_verify_20261111/panel_cache.parquet',
                           columns=['date']).unique().sort('date')
    all_dates = [d for d in base['date'].to_list() if EVAL_START <= d <= END]
    rebal5 = set(all_dates[::5])
    log(f'价格阶段：{len(files)} 只，调仓日 {len(rebal5)} 个')

    t0 = time.time()
    for i in range(0, len(files), batch):
        part = PARTS / f'part_{i // batch:04d}.parquet'
        if part.exists():
            continue
        frames = []
        for fp in files[i:i + batch]:
            try:
                d = pl.read_parquet(fp)
            except Exception as e:  # noqa: BLE001
                log(f'  skip {os.path.basename(fp)}: {e}')
                continue
            d = d.filter(pl.col('date') >= WARMUP_START)
            if d.is_empty():
                continue
            frames.append(d.select([
                'date', 'ts_code', 'open', 'high', 'low', 'close', 'pre_close',
                'volume', 'amount', 'hfq_factor',
            ]))
        if not frames:
            continue
        df = pl.concat(frames).sort(['ts_code', 'date'])
        df = df.join(mkt, on='date', how='left').sort(['ts_code', 'date'])
        df = compute_price_candidates(df)
        df = df.filter(pl.col('date').is_in(list(rebal5))).select(
            ['date', 'ts_code', *(EXTENDED_12 + PRICE_CANDIDATES)])
        df.write_parquet(part)
        log(f'  part {i // batch + 1}: {df.height:,} 行，累计 {time.time() - t0:.0f}s')
    _progress('price', 'done', parts=len(list(PARTS.glob('part_*.parquet'))))
    log('价格阶段完成')


# ---------------------------------------------------------------------------
# 2. 财务类因子（PIT）
# ---------------------------------------------------------------------------
def first_period(frame: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    """同一 (ts_code, end_date) 只保留 ann_date 最早的一版。"""
    return (
        frame.filter(pl.col('ann_date').is_not_null())
        .sort(['ts_code', 'end_date', 'ann_date'])
        .group_by(['ts_code', 'end_date'], maintain_order=True)
        .agg([pl.col('ann_date').first(), *(pl.col(c).first() for c in cols)])
    )


def ttm(frame: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    """年初至今累计 -> TTM；严格 PIT：所需历史公告日不得晚于本期公告日。"""
    cur = first_period(frame, cols)
    prev_q = cur.select([
        'ts_code', pl.col('end_date').alias('_prev_end'),
        pl.col('ann_date').alias('_prev_ann'),
        *[pl.col(c).alias(f'{c}__prev_q') for c in cols]])
    merged = cur.with_columns(
        pl.col('end_date').dt.offset_by('-1y').alias('_prev_end')
    ).join(prev_q, on=['ts_code', '_prev_end'], how='left')

    annual = (
        cur.filter(pl.col('end_date').dt.month() == 12)
        .select(['ts_code', pl.col('end_date').alias('_annual_end'),
                 pl.col('ann_date').alias('_annual_ann'),
                 *[pl.col(c).alias(f'{c}__annual') for c in cols]])
        .with_columns((pl.col('_annual_end').dt.year() + 1).alias('_fy'))
        .select(['ts_code', '_fy', '_annual_ann',
                 *[f'{c}__annual' for c in cols]]))
    merged = merged.with_columns(pl.col('end_date').dt.year().alias('_fy')) \
        .join(annual, on=['ts_code', '_fy'], how='left')

    usable = (pl.col('_prev_ann') <= pl.col('ann_date')) \
        & (pl.col('_annual_ann') <= pl.col('ann_date'))
    expr = [
        pl.when(pl.col('end_date').dt.month() == 12).then(pl.col(c))
        .otherwise(pl.when(usable).then(
            pl.col(c) + pl.col(f'{c}__annual') - pl.col(f'{c}__prev_q'))
            .otherwise(None))
        .alias(f'{c}__ttm') for c in cols]
    return merged.with_columns(expr).select(
        ['ts_code', 'ann_date', 'end_date', *[f'{c}__ttm' for c in cols]])


def build_financial_reports() -> pl.DataFrame:
    store = ROOT / 'data/local_store/meta/financials'
    inc = pl.read_parquet(store / 'income.parquet', columns=[
        'ts_code', 'end_date', 'ann_date', 'revenue', 'gross_profit',
        'parent_net_profit']).rename({'ann_date': 'ann_inc'})
    bal = pl.read_parquet(store / 'balance.parquet', columns=[
        'ts_code', 'end_date', 'ann_date', 'total_assets', 'total_equity',
    ]).rename({'ann_date': 'ann_bal'})
    cf = pl.read_parquet(store / 'cashflow.parquet', columns=[
        'ts_code', 'end_date', 'ann_date', 'operating_cf',
    ]).rename({'ann_date': 'ann_cf'})

    m = inc.join(bal, on=['ts_code', 'end_date'], how='left') \
           .join(cf, on=['ts_code', 'end_date'], how='left')
    m = m.with_columns(
        pl.max_horizontal([pl.col(c).cast(pl.Int32).fill_null(-719162)
                           for c in ('ann_inc', 'ann_bal', 'ann_cf')])
        .cast(pl.Date).alias('ann_date')
    ).drop(['ann_inc', 'ann_bal', 'ann_cf'])

    cols = ['revenue', 'gross_profit', 'parent_net_profit', 'operating_cf']
    tt = ttm(m, cols)
    base = first_period(m, cols + ['total_assets', 'total_equity'])
    rep = tt.join(base, on=['ts_code', 'end_date', 'ann_date'], how='left')

    # 上年同期累计（同比），要求上年同期公告日 <= 本期公告日
    prev = base.select([
        'ts_code', pl.col('end_date').alias('_prev_end'),
        pl.col('ann_date').alias('_prev_ann'),
        pl.col('revenue').alias('_rev_prev'),
        pl.col('parent_net_profit').alias('_np_prev'),
    ])
    rep = rep.with_columns(pl.col('end_date').dt.offset_by('-1y').alias('_prev_end')) \
             .join(prev, on=['ts_code', '_prev_end'], how='left')
    yoy_ok = pl.col('_prev_ann') <= pl.col('ann_date')
    rep = rep.with_columns([
        pl.when(yoy_ok & (pl.col('_rev_prev') > 0))
        .then(pl.col('revenue') / pl.col('_rev_prev') - 1.0).otherwise(None)
        .alias('g_revenue_yoy'),
        pl.when(yoy_ok & (pl.col('_np_prev').abs() > 1e-6))
        .then((pl.col('parent_net_profit') - pl.col('_np_prev'))
              / pl.col('_np_prev').abs()).otherwise(None)
        .alias('g_profit_yoy'),
    ])

    # 单季净利（累计差分），用于 SUE
    rep = rep.sort(['ts_code', 'end_date'])
    cum = pl.col('parent_net_profit')
    prev_cum = cum.shift(1).over('ts_code')
    same_fy = (pl.col('end_date').dt.year()
               == pl.col('end_date').shift(1).over('ts_code').dt.year())
    single = pl.when((pl.col('end_date').dt.month() != 3) & same_fy & prev_cum.is_not_null()) \
        .then(cum - prev_cum).otherwise(cum).alias('_single_q')
    rep = rep.with_columns(single)
    diff = pl.col('_single_q') - pl.col('_single_q').shift(4).over('ts_code')
    rep = rep.with_columns(diff.alias('_sdiff'))
    sd8 = pl.col('_sdiff').rolling_std(window_size=8, min_samples=6).over('ts_code')
    ann_max8 = pl.col('ann_date').cast(pl.Int32).rolling_max(
        window_size=8, min_samples=1).over('ts_code')
    rep = rep.with_columns([
        pl.when((ann_max8 <= pl.col('ann_date').cast(pl.Int32)) & (sd8 > 0))
        .then(pl.col('_sdiff') / sd8).otherwise(None).alias('g_sue'),
        # 质量 / 价值
        pl.when(pl.col('total_equity') > 0)
        .then(pl.col('parent_net_profit__ttm') / pl.col('total_equity')).otherwise(None)
        .alias('q_roe_ttm'),
        pl.when(pl.col('total_assets') > 0)
        .then(pl.col('parent_net_profit__ttm') / pl.col('total_assets')).otherwise(None)
        .alias('q_roa_ttm'),
        pl.when(pl.col('revenue__ttm') > 0)
        .then(pl.col('gross_profit__ttm') / pl.col('revenue__ttm')).otherwise(None)
        .alias('q_gross_margin'),
        pl.when(pl.col('total_assets') > 0)
        .then(pl.col('revenue__ttm') / pl.col('total_assets')).otherwise(None)
        .alias('q_asset_turnover'),
        pl.when(pl.col('total_assets') > 0)
        .then((pl.col('parent_net_profit__ttm') - pl.col('operating_cf__ttm'))
              / pl.col('total_assets')).otherwise(None).alias('q_accruals'),
        pl.when(pl.col('parent_net_profit__ttm') > 0)
        .then(pl.col('operating_cf__ttm') / pl.col('parent_net_profit__ttm'))
        .otherwise(None).alias('q_ocf_to_profit'),
    ])
    keep = ['ts_code', 'ann_date', 'end_date', 'g_revenue_yoy', 'g_profit_yoy', 'g_sue',
            'q_roe_ttm', 'q_roa_ttm', 'q_gross_margin', 'q_asset_turnover',
            'q_accruals', 'q_ocf_to_profit',
            'parent_net_profit__ttm', 'revenue__ttm', 'operating_cf__ttm']
    rep = rep.select(keep).filter(pl.col('ann_date').is_not_null()) \
             .sort(['ts_code', 'ann_date']) \
             .unique(subset=['ts_code', 'ann_date'], keep='last') \
             .sort(['ts_code', 'ann_date'])
    log(f'财务报告表 {rep.height:,} 行 / {rep["ts_code"].n_unique()} 只')
    return rep


def stage_financial(base_cs: pl.DataFrame) -> None:
    _progress('financial', 'running')
    rep = build_financial_reports()
    rep.write_parquet(OUT / 'financial_reports.parquet')

    cross = base_cs.select(['ts_code', 'date']).unique().sort(['ts_code', 'date'])
    j = cross.join_asof(rep, left_on='date', right_on='ann_date', by='ts_code',
                        strategy='backward')

    val = pl.read_parquet(
        ROOT / 'data/local_store/meta/financials/valuation_daily.parquet',
        columns=['ts_code', 'trade_date', 'total_mv']
    ).filter((pl.col('trade_date') >= EVAL_START) & (pl.col('trade_date') <= END)) \
     .sort(['ts_code', 'trade_date'])
    j = j.sort(['ts_code', 'date']).join_asof(
        val, left_on='date', right_on='trade_date', by='ts_code', strategy='backward')

    j = j.with_columns([
        pl.when(pl.col('total_mv') > 0)
        .then(pl.col('parent_net_profit__ttm') / pl.col('total_mv')).otherwise(None).alias('v_ep'),
        pl.when(pl.col('total_mv') > 0)
        .then(pl.col('operating_cf__ttm') / pl.col('total_mv')).otherwise(None).alias('v_cfp'),
        pl.when(pl.col('total_mv') > 0)
        .then(pl.col('revenue__ttm') / pl.col('total_mv')).otherwise(None).alias('v_sp'),
    ])
    out = j.select(['date', 'ts_code', *FIN_CANDIDATES])
    out.write_parquet(OUT / 'financial_cross.parquet')
    log(f'财务截面 {out.height:,} 行')
    _progress('financial', 'done', rows=out.height)


# ---------------------------------------------------------------------------
# 3. 合并 + 过滤
# ---------------------------------------------------------------------------
def load_st_intervals() -> pl.DataFrame:
    return pl.read_parquet(ROOT / 'data/local_store/meta/name_history.parquet') \
        .select(['ts_code', 'name', 'start_date', 'end_date'])


def st_exclusions(rebal: list[date], nh: pl.DataFrame) -> pl.DataFrame:
    pairs = []
    for d in rebal:
        sub = nh.filter(
            (pl.col('start_date') <= d)
            & (pl.col('end_date').is_null() | (pl.col('end_date') >= d))
            & (pl.col('name').str.contains('ST|退')))
        pairs.extend((d, c) for c in sub['ts_code'].to_list())
    if not pairs:
        return pl.DataFrame(schema={'date': pl.Date, 'ts_code': pl.Utf8})
    return pl.DataFrame(pairs, schema={'date': pl.Date, 'ts_code': pl.Utf8}, orient='row')


def limit_threshold(code: str, d: date) -> float:
    prefix = code.split('.')[0]
    if prefix.startswith('688'):
        return 0.195
    if prefix.startswith(('300', '301')):
        return 0.195 if d >= date(2020, 8, 24) else 0.095
    return 0.095


def build_base() -> pl.DataFrame:
    p = OUT / 'base_rebal.parquet'
    if p.exists():
        return pl.read_parquet(p)
    base = pl.read_parquet(OUT.parent / 'factor_oos_verify_20261111/panel_cache.parquet')
    dates = sorted(base['date'].unique().to_list())
    rebal5 = [d for d in dates if EVAL_START <= d <= END][::5]
    base = base.filter(pl.col('date').is_in(rebal5))
    parts = sorted(PARTS.glob('part_*.parquet'))
    price = pl.concat([pl.read_parquet(f) for f in parts])
    n0 = base.height
    base = base.join(price, on=['date', 'ts_code'], how='left')
    assert base.height == n0, 'price join changed row count'
    fin = pl.read_parquet(OUT / 'financial_cross.parquet')
    base = base.join(fin, on=['date', 'ts_code'], how='left')
    assert base.height == n0, 'financial join changed row count'
    base.write_parquet(p)
    log(f'合并截面 {base.height:,} 行 × {len(base.columns)} 列')
    return base


def filtered_h(base: pl.DataFrame, h: int, all_dates: list[date]) -> pl.DataFrame:
    step = h // 5
    rebal = all_dates[::step]
    nh = load_st_intervals()
    sub = base.filter(pl.col('date').is_in(rebal))
    n0 = sub.height
    sub = sub.filter(pl.col('cum_count') >= 250)
    n1 = sub.height
    sub = sub.filter(pl.col('volume') > 0)
    n2 = sub.height
    sub = sub.join(st_exclusions(rebal, nh), on=['date', 'ts_code'], how='anti')
    n3 = sub.height
    codes = sub['ts_code'].to_list()
    ds = sub['date'].to_list()
    lim = np.array([limit_threshold(c, d) for c, d in zip(codes, ds, strict=False)])
    sub = sub.with_columns(pl.Series('_lim', lim)).filter(
        (pl.col('next_open_ret') < pl.col('_lim'))
        & (pl.col('next_open_ret') > -pl.col('_lim'))).drop('_lim')
    log(f'h={h}: {n0:,} -> 上市250 {n1:,} -> 停牌 {n2:,} -> ST {n3:,} -> 涨跌停 {sub.height:,}')
    return sub


# ---------------------------------------------------------------------------
# 4. 正交化与统计
# ---------------------------------------------------------------------------
def _centered_rank(x: np.ndarray, n_valid: int) -> np.ndarray:
    out = np.zeros(len(x))
    idx = np.where(np.isfinite(x))[0]
    if len(idx) == 0:
        return out
    r = rankdata(x[idx])
    out[idx] = (r - 0.5) / len(idx) - 0.5
    return out


def _nw_t(x: np.ndarray, lag: int = 1) -> float:
    x = np.asarray(x, float)
    n = len(x)
    if n < 3:
        return float('nan')
    e = x - x.mean()
    var = float((e * e).mean())
    for lag_i in range(1, min(lag, n - 1) + 1):
        w = 1 - lag_i / (lag + 1)
        var += 2 * w * float((e[lag_i:] * e[:-lag_i]).mean())
    se = math.sqrt(max(var, 1e-18) / n)
    return float(x.mean() / se)


def evaluate(base: pl.DataFrame, h: int) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:  # noqa: C901
    all_dates = sorted(base['date'].unique().to_list())
    xs = filtered_h(base, h, all_dates)
    fwd_col = f'fwd_{h}'
    groups = xs.partition_by('date', maintain_order=True)
    recs: dict[str, list[dict]] = {f: [] for f in CANDIDATES}
    corrs: dict[str, list[np.ndarray]] = {f: [] for f in CANDIDATES}

    for g in groups:
        n = g.height
        if n < MIN_NAMES:
            continue
        arr = {c: g[c].to_numpy().astype(float) for c in CANDIDATES + EXISTING}
        fwd = g[fwd_col].to_numpy().astype(float)
        X = np.zeros((n, len(EXISTING)))
        present = np.zeros(n, dtype=int)
        for j, f in enumerate(EXISTING):
            x = arr[f]
            ok = np.isfinite(x)
            present += ok
            X[:, j] = _centered_rank(x, int(ok.sum()))
        fwd_rank = _centered_rank(fwd, int(np.isfinite(fwd).sum()))
        base_mask = np.isfinite(fwd) & (present >= MIN_FACTORS_PRESENT)
        if base_mask.sum() < MIN_NAMES:
            continue
        d0 = g['date'][0]

        for f in CANDIDATES:
            x = arr[f]
            m = base_mask & np.isfinite(x)
            k = int(m.sum())
            rec = {'date': d0, 'cov': k, 'elig': int(base_mask.sum())}
            if k < MIN_NAMES:
                rec.update({'ic': np.nan})
                recs[f].append(rec)
                continue
            y = _centered_rank(x[m], k)
            y = y - y.mean()
            Xm = np.column_stack([np.ones(k), X[m]])
            yf = fwd_rank[m]
            # 基准模型必须在与候选相同的样本上重估，否则缺失样本会虚增 ΔR²
            beta_bm, *_ = np.linalg.lstsq(Xm, yf, rcond=None)
            resid_bm = yf - Xm @ beta_bm
            ssr_bm = float(resid_bm @ resid_bm)

            Xf = np.column_stack([Xm, y])
            beta_f, *_ = np.linalg.lstsq(Xf, yf, rcond=None)
            resid_f = yf - Xf @ beta_f
            ssr_f = float(resid_f @ resid_f)
            sst_f = float(((yf - yf.mean()) ** 2).sum())
            rec['delta'] = (ssr_bm - ssr_f) / sst_f if sst_f > 0 else np.nan

            b, *_ = np.linalg.lstsq(Xm, y, rcond=None)
            res = y - Xm @ b
            rec['ic'] = float(spearmanr(res, fwd[m]).statistic) if np.ptp(res) > 0 else np.nan
            rec['raw_ic'] = float(spearmanr(x[m], fwd[m]).statistic)
            cs = np.full(len(EXISTING), np.nan)
            for j in range(X.shape[1]):
                if np.ptp(Xm[:, j + 1]) == 0:
                    continue
                cs[j] = float(np.corrcoef(Xm[:, j + 1], y)[0, 1])
            corrs[f].append(cs)
            recs[f].append(rec)

    rows, corr_rows = [], []
    for f in CANDIDATES:
        df = pl.DataFrame([
            {'date': r['date'], 'ic': r.get('ic', np.nan),
             'raw_ic': r.get('raw_ic', np.nan), 'delta': r.get('delta', np.nan),
             'cov': r['cov'], 'elig': r['elig']}
            for r in recs[f] if 'ic' in r and np.isfinite(r['ic'])
        ])
        if df.is_empty():
            continue
        is_ = df.filter(pl.col('date') <= IS_END)
        oos = df.filter(pl.col('date') > IS_END)

        def pack(x: pl.DataFrame) -> dict:
            if x.height == 0:
                return {'ic': np.nan, 't': np.nan, 'icir': np.nan, 'n': 0,
                        'delta': np.nan, 'sd': np.nan}
            v = x['ic'].to_numpy()
            sd = float(v.std(ddof=1)) if len(v) > 1 else np.nan
            return {'ic': float(v.mean()), 't': _nw_t(v, 1),
                    'icir': float(v.mean() / sd) if sd and sd > 0 else np.nan,
                    'n': len(v), 'delta': float(np.nanmean(x['delta'].to_numpy())),
                    'sd': sd}

        s_is, s_oos = pack(is_), pack(oos)
        rows.append({
            'candidate': f, 'horizon': h,
            'n_dates': df.height,
            'raw_ic_oos': float(np.nanmean(oos['raw_ic'].to_numpy())) if oos.height else np.nan,
            'resid_ic_is': s_is['ic'], 'resid_t_is': s_is['t'], 'resid_icir_is': s_is['icir'],
            'resid_ic_oos': s_oos['ic'], 'resid_t_oos': s_oos['t'],
            'resid_icir_oos': s_oos['icir'], 'resid_ic_std_oos': s_oos['sd'],
            'n_oos': s_oos['n'],
            'delta_r2_is_pct': s_is['delta'] * 100 if np.isfinite(s_is['delta']) else np.nan,
            'delta_r2_oos_pct': s_oos['delta'] * 100 if np.isfinite(s_oos['delta']) else np.nan,
            'coverage_oos': float(df.filter(pl.col('date') > IS_END)['cov'].sum()
                                  / max(df.filter(pl.col('date') > IS_END)['elig'].sum(), 1)),
            'coverage_all': float(df['cov'].sum() / max(df['elig'].sum(), 1)),
            'max_abs_rho': float(np.nanmax(np.abs(np.vstack(corrs[f]))))
            if corrs[f] else np.nan,
        })
        if corrs[f]:
            cm = np.nanmean(np.vstack(corrs[f]), axis=0)
            for j, ef in enumerate(EXISTING):
                if np.isfinite(cm[j]):
                    corr_rows.append({'candidate': f, 'existing_factor': ef,
                                      'mean_rho': float(cm[j])})
    return pl.DataFrame(rows), pl.DataFrame(corr_rows), xs


def evaluate_all() -> None:
    base = build_base()
    all_rows, all_corr = [], []
    for h in HORIZONS:
        _progress('eval', 'running', horizon=h)
        rows, corr, _ = evaluate(base, h)
        all_rows.append(rows)
        all_corr.append(corr)
        log(f'h={h} 评估完成：{rows.height} 个候选')
    out = pl.concat(all_rows)
    out.write_csv(REPORTS / 'residual_ic.csv')
    pl.concat(all_corr).write_csv(REPORTS / 'factor_correlation.csv')
    _progress('eval', 'done', rows=out.height)


# ---------------------------------------------------------------------------
# 5. 覆盖率 / 分档 / Bonferroni
# ---------------------------------------------------------------------------
def coverage_table(base: pl.DataFrame) -> pl.DataFrame:
    xs = filtered_h(base, 20, sorted(base['date'].unique().to_list()))
    yearly = xs.with_columns(pl.col('date').dt.year().alias('year'))
    rows = []
    for f in EXISTING + CANDIDATES:
        for (yr,), g in yearly.group_by(['year'], maintain_order=True):
            nn = int(g[f].is_not_null().sum())
            rows.append({'factor': f, 'year': int(yr), 'rows': g.height, 'nonnull': nn,
                         'coverage': round(nn / g.height, 4)})
    return pl.DataFrame(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--stage', default='all',
                    choices=['price', 'financial', 'eval', 'coverage', 'all'])
    ap.add_argument('--max-stocks', type=int, default=None)
    ap.add_argument('--batch', type=int, default=300)
    args = ap.parse_args()

    if args.stage in ('price', 'all'):
        stage_price(args.max_stocks, args.batch)
    if args.stage in ('financial', 'all'):
        base = pl.read_parquet(OUT.parent / 'factor_oos_verify_20261111/panel_cache.parquet',
                               columns=['date', 'ts_code'])
        dates = sorted(base['date'].unique().to_list())
        rebal5 = [d for d in dates if EVAL_START <= d <= END][::5]
        stage_financial(base.filter(pl.col('date').is_in(rebal5)))
    if args.stage in ('eval', 'coverage', 'all'):
        if args.stage == 'coverage':
            cov = coverage_table(build_base())
            cov.write_csv(REPORTS / 'factor_coverage_by_year.csv')
            log('覆盖率表完成')
        else:
            evaluate_all()
            cov = coverage_table(build_base())
            cov.write_csv(REPORTS / 'factor_coverage_by_year.csv')
            log('覆盖率表完成')


if __name__ == '__main__':
    main()
