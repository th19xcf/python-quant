#!/usr/bin/env python3
"""研究候选因子（已实现，未启用）。

本模块只提供计算函数，**不注册**到 ``FACTOR_REGISTRY``，也不被
``src/alpha/factors/__init__.py`` 导入，因此不会进入任何在跑链路
（``factor_sets`` / 评分 / 回测）。所有候选的 ``direction`` 一律为 ``'?'``，
方向需在启用前用样本外数据另行确认。

数据口径：
- 价格类：输入面板需已做前复权，列为 ``date, ts_code, open, high, low,
  close, pre_close, volume, amount``；``amount`` 为成交额（元）。
- 财务类：输入为 ``meta/financials`` 的 income / balance / cashflow 三张表
  （按 ``ann_date`` 做 PIT），估值用 ``valuation_daily.total_mv``。
- 财务 TTM 严格校验：用到上年年报或上年同期累计时，其 ``ann_date`` 必须
  不晚于本期 ``ann_date``，否则记空，不做任何回填。
"""
from __future__ import annotations

import polars as pl

# 覆盖率与 PIT 起点来自 reports/factor_candidates_20261111/coverage_summary.csv
# （2015-01~2026-10 评估口径；coverage_all 为剔除 ST/停牌/涨跌停后的加权覆盖）
CANDIDATE_METADATA: dict[str, dict] = {
    'q_roe_ttm': {
        'family': '质量', 'direction': '?',
        'definition': '归母净利TTM / 期末归母净资产',
        'data_deps': 'income.parent_net_profit + balance.total_equity，PIT ann_date',
        'pit_start': '2015-01（财务报表 2010 起；2015 起覆盖稳定）',
        'coverage_all': 0.9821, 'coverage_oos': 0.9933,
    },
    'q_roa_ttm': {
        'family': '质量', 'direction': '?',
        'definition': '归母净利TTM / 期末总资产',
        'data_deps': 'income.parent_net_profit + balance.total_assets，PIT',
        'pit_start': '2015-01（财务报表 2010 起）',
        'coverage_all': 0.9835, 'coverage_oos': 0.9944,
    },
    'q_gross_margin': {
        'family': '质量', 'direction': '?',
        'definition': '毛利TTM / 营收TTM',
        'data_deps': 'income.gross_profit / revenue，PIT；金融业毛利缺失',
        'pit_start': '2015-01（财务报表 2010 起）',
        'coverage_all': 0.9731, 'coverage_oos': 0.9772,
    },
    'q_asset_turnover': {
        'family': '质量', 'direction': '?',
        'definition': '营收TTM / 期末总资产',
        'data_deps': 'income.revenue + balance.total_assets，PIT',
        'pit_start': '2015-01（财务报表 2010 起）',
        'coverage_all': 0.9866, 'coverage_oos': 0.9966,
    },
    'q_accruals': {
        'family': '质量', 'direction': '?',
        'definition': '(归母净利TTM - 经营现金流TTM) / 期末总资产',
        'data_deps': 'income + cashflow.operating_cf + balance.total_assets，PIT',
        'pit_start': '2015-01（财务报表 2010 起）',
        'coverage_all': 0.9835, 'coverage_oos': 0.9944,
    },
    'q_ocf_to_profit': {
        'family': '质量', 'direction': '?',
        'definition': '经营现金流TTM / 归母净利TTM（归母净利<=0 记空）',
        'data_deps': 'cashflow.operating_cf + income.parent_net_profit，PIT',
        'pit_start': '2015-01（财务报表 2010 起）',
        'coverage_all': 0.8097, 'coverage_oos': 0.7668,
    },
    'g_revenue_yoy': {
        'family': '成长', 'direction': '?',
        'definition': '本期累计营收 / 上年同期累计营收 - 1',
        'data_deps': 'income.revenue；上年同期 ann_date 必须早于本期',
        'pit_start': '2015-01（财务报表 2010 起）',
        'coverage_all': 0.9956, 'coverage_oos': 0.9984,
    },
    'g_profit_yoy': {
        'family': '成长', 'direction': '?',
        'definition': '(归母净利 - 上年同期) / |上年同期|；上年同期为 0 记空',
        'data_deps': 'income.parent_net_profit；上年同期 PIT 可见',
        'pit_start': '2015-01（财务报表 2010 起）',
        'coverage_all': 0.9930, 'coverage_oos': 0.9963,
    },
    'g_sue': {
        'family': '成长', 'direction': '?',
        'definition': '(单季净利 - 去年同季) / std(过去 8 季季节差分)，至少 6 季',
        'data_deps': 'income.parent_net_profit 累计差分；PIT 公告日校验',
        'pit_start': '2015-01（需 6 个季度以上报表）',
        'coverage_all': 0.9814, 'coverage_oos': 0.9939,
    },
    'v_ep': {
        'family': '价值', 'direction': '?',
        'definition': '归母净利TTM / 当日总市值',
        'data_deps': 'income + valuation_daily.total_mv',
        'pit_start': '2015-01（total_mv 全历史；与 pe 口径不同）',
        'coverage_all': 0.9916, 'coverage_oos': 0.9966,
    },
    'v_cfp': {
        'family': '价值', 'direction': '?',
        'definition': '经营现金流TTM / 当日总市值',
        'data_deps': 'cashflow.operating_cf + valuation_daily.total_mv',
        'pit_start': '2015-01（total_mv 全历史）',
        'coverage_all': 0.9867, 'coverage_oos': 0.9970,
    },
    'v_sp': {
        'family': '价值', 'direction': '?',
        'definition': '营收TTM / 当日总市值',
        'data_deps': 'income.revenue + valuation_daily.total_mv',
        'pit_start': '2015-01（total_mv 全历史）',
        'coverage_all': 0.9946, 'coverage_oos': 0.9989,
    },
    'v_ivol_60': {
        'family': '波动/彩票', 'direction': '?',
        'definition': 'CAPM 残差收益（60 日滚动 beta）的 60 日标准差',
        'data_deps': '个股日收益 + 000001.SH 指数日收益',
        'pit_start': '2014-01（2013 起 60 日预热）',
        'coverage_all': 1.0, 'coverage_oos': 1.0,
    },
    'v_max_5d_21': {
        'family': '波动/彩票', 'direction': '?',
        'definition': '过去 21 日窗口内 5 日收益的最大值',
        'data_deps': '个股日线',
        'pit_start': '2013-07（26 个交易日预热）',
        'coverage_all': 1.0, 'coverage_oos': 1.0,
    },
    'v_downside_vol_60': {
        'family': '波动/彩票', 'direction': '?',
        'definition': '60 日内负收益半方差开方（正收益日记 0）',
        'data_deps': '个股日线',
        'pit_start': '2014-01（60 日预热）',
        'coverage_all': 1.0, 'coverage_oos': 1.0,
    },
    'v_skew_60': {
        'family': '波动/彩票', 'direction': '?',
        'definition': '60 日收益偏度',
        'data_deps': '个股日线',
        'pit_start': '2014-01（60 日预热）',
        'coverage_all': 1.0, 'coverage_oos': 1.0,
    },
    'l_amihud_20': {
        'family': '流动性', 'direction': '?',
        'definition': 'mean(|日收益| / 成交额)，20 日',
        'data_deps': '个股日线 amount；已有 ff_amihud_60 作对照',
        'pit_start': '2013-07（20 日预热）',
        'coverage_all': 0.99998, 'coverage_oos': 0.99999,
    },
    'm_mom_12_1': {
        'family': '动量/反转', 'direction': '?',
        'definition': 'close[t-21] / close[t-252] - 1（跳过最近一月）',
        'data_deps': '个股日线，252 日预热',
        'pit_start': '2014-01（252 日预热）',
        'coverage_all': 0.99918, 'coverage_oos': 0.99933,
    },
    'm_resid_mom_12_1': {
        'family': '动量/反转', 'direction': '?',
        'definition': '120 日滚动 beta 的 CAPM 残差收益，过去 252 日剔除近 21 日累加',
        'data_deps': '个股日线 + 指数日线',
        'pit_start': '2014-07（120+252 日预热）',
        'coverage_all': 0.9857, 'coverage_oos': 0.9882,
    },
    'm_dist_52w_high': {
        'family': '动量/反转', 'direction': '?',
        'definition': 'close / 252 日最高价 - 1',
        'data_deps': '个股日线',
        'pit_start': '2014-01（252 日预热）',
        'coverage_all': 1.0, 'coverage_oos': 1.0,
    },
    'm_reversal_5': {
        'family': '动量/反转', 'direction': '?',
        'definition': 'close / close[t-5] - 1',
        'data_deps': '个股日线',
        'pit_start': '2013-07（5 日预热）',
        'coverage_all': 1.0, 'coverage_oos': 1.0,
    },
}

PRICE_CANDIDATES = ['v_ivol_60', 'v_max_5d_21', 'v_downside_vol_60', 'v_skew_60',
                    'l_amihud_20', 'm_mom_12_1', 'm_resid_mom_12_1',
                    'm_dist_52w_high', 'm_reversal_5']
FINANCIAL_CANDIDATES = ['q_roe_ttm', 'q_roa_ttm', 'q_gross_margin',
                        'q_asset_turnover', 'q_accruals', 'q_ocf_to_profit',
                        'g_revenue_yoy', 'g_profit_yoy', 'g_sue',
                        'v_ep', 'v_cfp', 'v_sp']

#: 财务时效守卫（方案 B24）：截面日 t 匹配到的报告 ``end_date`` 距 t 超过该
#: 月数即判陈旧，相关财务候选与 TTM 原始值一律置 null，不用旧值冒充当期。
#: 置 ``None`` 关闭守卫（仅用于反证用例与历史口径复现）。
MAX_FIN_STALENESS_MONTHS = 24

#: 守卫作用对象：9 个财务候选 + 3 个 TTM 原始值（v_ep/v_cfp/v_sp 由它们派生）。
_STALE_GUARD_COLS = [
    'q_roe_ttm', 'q_roa_ttm', 'q_gross_margin', 'q_asset_turnover',
    'q_accruals', 'q_ocf_to_profit', 'g_revenue_yoy', 'g_profit_yoy', 'g_sue',
    'parent_net_profit__ttm', 'revenue__ttm', 'operating_cf__ttm',
]


# ---------------------------------------------------------------------------
# 价格类
# ---------------------------------------------------------------------------
def compute_price_candidates(panel: pl.DataFrame,
                             market_returns: pl.DataFrame) -> pl.DataFrame:
    """计算 9 个价格类候选。

    Args:
        panel: 已前复权的日线面板，需含 date/ts_code/open/high/low/close/
            pre_close/volume/amount。
        market_returns: 含 date/mkt_ret 的市场收益（000001.SH 口径）。

    Returns:
        date, ts_code + PRICE_CANDIDATES。
    """
    g = 'ts_code'
    df = panel.select(['date', 'ts_code', 'open', 'high', 'low', 'close',
                       'pre_close', 'volume', 'amount']) \
        .join(market_returns.select(['date', 'mkt_ret']), on='date', how='left') \
        .sort([g, 'date'])

    r = pl.col('close') / pl.col('close').shift(1).over(g) - 1.0
    m = pl.col('mkt_ret')
    df = df.with_columns([r.alias('_r'), m.alias('_m')])

    mean_r = pl.col('_r').rolling_mean(window_size=60, min_samples=40).over(g)
    mean_m = pl.col('_m').rolling_mean(window_size=60, min_samples=40).over(g)
    cov = (pl.col('_r') * pl.col('_m')).rolling_mean(
        window_size=60, min_samples=40).over(g) - mean_r * mean_m
    var_m = (pl.col('_m') * pl.col('_m')).rolling_mean(
        window_size=60, min_samples=40).over(g) - mean_m * mean_m
    beta60 = pl.when(var_m > 0).then(cov / var_m).otherwise(0.0)
    resid60 = pl.col('_r') - beta60 * pl.col('_m')
    ivol = resid60.rolling_std(window_size=60, min_samples=40).over(g)

    mean_r120 = pl.col('_r').rolling_mean(window_size=120, min_samples=80).over(g)
    mean_m120 = pl.col('_m').rolling_mean(window_size=120, min_samples=80).over(g)
    cov120 = (pl.col('_r') * pl.col('_m')).rolling_mean(
        window_size=120, min_samples=80).over(g) - mean_r120 * mean_m120
    var_m120 = (pl.col('_m') * pl.col('_m')).rolling_mean(
        window_size=120, min_samples=80).over(g) - mean_m120 * mean_m120
    beta120 = pl.when(var_m120 > 0).then(cov120 / var_m120).otherwise(0.0)
    resid120 = pl.col('_r') - beta120 * pl.col('_m')

    ret5 = pl.col('close') / pl.col('close').shift(5).over(g) - 1.0
    dr2 = pl.when(pl.col('_r') < 0).then(
        pl.col('_r') * pl.col('_r')).otherwise(0.0)
    amihud = pl.when(pl.col('amount') > 0).then(
        pl.col('_r').abs() / pl.col('amount')).otherwise(None)

    return df.with_columns([
        ivol.alias('v_ivol_60'),
        ret5.rolling_max(window_size=21, min_samples=15).over(g)
        .alias('v_max_5d_21'),
        dr2.rolling_mean(window_size=60, min_samples=40).over(g).sqrt()
        .alias('v_downside_vol_60'),
        pl.col('_r').rolling_skew(window_size=60).over(g).alias('v_skew_60'),
        amihud.rolling_mean(window_size=20, min_samples=15).over(g)
        .alias('l_amihud_20'),
        (pl.col('close').shift(21).over(g) / pl.col('close').shift(252).over(g) - 1.0)
        .alias('m_mom_12_1'),
        resid120.rolling_sum(window_size=231, min_samples=200).over(g)
        .shift(21).over(g).alias('m_resid_mom_12_1'),
        (pl.col('close') / pl.col('close').rolling_max(
            window_size=252, min_samples=200).over(g) - 1.0)
        .alias('m_dist_52w_high'),
        ret5.alias('m_reversal_5'),
    ]).select(['date', 'ts_code', *PRICE_CANDIDATES])


# ---------------------------------------------------------------------------
# 财务类（PIT）
# ---------------------------------------------------------------------------
_REQUIRED_FIN_COLS = ['revenue', 'gross_profit', 'parent_net_profit', 'operating_cf']


def _first_period(frame: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    """同一 (ts_code, end_date) 只保留 ``ann_date`` 最新的一版（更正/重述覆盖旧版）。

    PIT 口径：任意时点 T 可见的是 ``ann_date <= T`` 中公告日最新的一版；
    ``ann_date`` 同时是可见性门槛，故取最新版不会引入前视。与
    ``store.latest_announced_as_of``、``fundamental_factors._dedupe_period``、
    ``build_new_factors.first_period`` 一致（函数名 ``first`` 为历史命名，
    实际语义是「时点可见的最新版」）。
    """
    return (
        frame.filter(pl.col('ann_date').is_not_null())
        .sort(['ts_code', 'end_date', 'ann_date'])
        .group_by(['ts_code', 'end_date'], maintain_order=True)
        .agg([pl.col('ann_date').last(), *(pl.col(c).last() for c in cols)])
    )


def _ttm(frame: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    cur = _first_period(frame, cols)
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


def build_pit_reports(income: pl.DataFrame,
                      balance: pl.DataFrame,
                      cashflow: pl.DataFrame) -> pl.DataFrame:
    """把三张表合成带 PIT 公告日的报告级特征表。

    输入列要求：
    - income: ts_code, end_date, ann_date, revenue, gross_profit, parent_net_profit
    - balance: ts_code, end_date, ann_date, total_assets, total_equity
    - cashflow: ts_code, end_date, ann_date, operating_cf

    Returns:
        ts_code, ann_date, end_date, 12 个财务候选 + 3 个 TTM 原始值。
    """
    inc = income.select(['ts_code', 'end_date', 'ann_date', 'revenue',
                         'gross_profit', 'parent_net_profit']) \
        .rename({'ann_date': 'ann_inc'})
    bal = balance.select(['ts_code', 'end_date', 'ann_date', 'total_assets',
                          'total_equity']).rename({'ann_date': 'ann_bal'})
    cf = cashflow.select(['ts_code', 'end_date', 'ann_date', 'operating_cf']) \
        .rename({'ann_date': 'ann_cf'})

    m = inc.join(bal, on=['ts_code', 'end_date'], how='left') \
           .join(cf, on=['ts_code', 'end_date'], how='left')
    m = m.with_columns(
        pl.max_horizontal([pl.col(c).cast(pl.Int32).fill_null(-719162)
                           for c in ('ann_inc', 'ann_bal', 'ann_cf')])
        .cast(pl.Date).alias('ann_date')
    ).drop(['ann_inc', 'ann_bal', 'ann_cf'])

    tt = _ttm(m, _REQUIRED_FIN_COLS)
    base = _first_period(m, _REQUIRED_FIN_COLS + ['total_assets', 'total_equity'])
    rep = tt.join(base, on=['ts_code', 'end_date', 'ann_date'], how='left')

    prev = base.select([
        'ts_code', pl.col('end_date').alias('_prev_end'),
        pl.col('ann_date').alias('_prev_ann'),
        pl.col('revenue').alias('_rev_prev'),
        pl.col('parent_net_profit').alias('_np_prev')])
    rep = rep.with_columns(
        pl.col('end_date').dt.offset_by('-1y').alias('_prev_end')
    ).join(prev, on=['ts_code', '_prev_end'], how='left')
    yoy_ok = pl.col('_prev_ann') <= pl.col('ann_date')
    rep = rep.with_columns([
        pl.when(yoy_ok & (pl.col('_rev_prev') > 0))
        .then(pl.col('revenue') / pl.col('_rev_prev') - 1.0).otherwise(None)
        .alias('g_revenue_yoy'),
        pl.when(yoy_ok & (pl.col('_np_prev').abs() > 1e-6))
        .then((pl.col('parent_net_profit') - pl.col('_np_prev'))
              / pl.col('_np_prev').abs()).otherwise(None).alias('g_profit_yoy'),
    ])

    rep = rep.sort(['ts_code', 'end_date'])
    cum = pl.col('parent_net_profit')
    prev_cum = cum.shift(1).over('ts_code')
    same_fy = (pl.col('end_date').dt.year()
               == pl.col('end_date').shift(1).over('ts_code').dt.year())
    rep = rep.with_columns(
        pl.when((pl.col('end_date').dt.month() != 3) & same_fy & prev_cum.is_not_null())
        .then(cum - prev_cum).otherwise(cum).alias('_single_q'))
    rep = rep.with_columns(
        (pl.col('_single_q') - pl.col('_single_q').shift(4).over('ts_code'))
        .alias('_sdiff'))
    sd8 = pl.col('_sdiff').rolling_std(window_size=8, min_samples=6).over('ts_code')
    ann_max8 = pl.col('ann_date').cast(pl.Int32).rolling_max(
        window_size=8, min_samples=1).over('ts_code')
    rep = rep.with_columns([
        pl.when((ann_max8 <= pl.col('ann_date').cast(pl.Int32)) & (sd8 > 0))
        .then(pl.col('_sdiff') / sd8).otherwise(None).alias('g_sue'),
        pl.when(pl.col('total_equity') > 0)
        .then(pl.col('parent_net_profit__ttm') / pl.col('total_equity'))
        .otherwise(None).alias('q_roe_ttm'),
        pl.when(pl.col('total_assets') > 0)
        .then(pl.col('parent_net_profit__ttm') / pl.col('total_assets'))
        .otherwise(None).alias('q_roa_ttm'),
        pl.when(pl.col('revenue__ttm') > 0)
        .then(pl.col('gross_profit__ttm') / pl.col('revenue__ttm'))
        .otherwise(None).alias('q_gross_margin'),
        pl.when(pl.col('total_assets') > 0)
        .then(pl.col('revenue__ttm') / pl.col('total_assets'))
        .otherwise(None).alias('q_asset_turnover'),
        pl.when(pl.col('total_assets') > 0)
        .then((pl.col('parent_net_profit__ttm') - pl.col('operating_cf__ttm'))
              / pl.col('total_assets')).otherwise(None).alias('q_accruals'),
        pl.when(pl.col('parent_net_profit__ttm') > 0)
        .then(pl.col('operating_cf__ttm') / pl.col('parent_net_profit__ttm'))
        .otherwise(None).alias('q_ocf_to_profit'),
    ])
    return rep.select(['ts_code', 'ann_date', 'end_date',
                       'g_revenue_yoy', 'g_profit_yoy', 'g_sue',
                       'q_roe_ttm', 'q_roa_ttm', 'q_gross_margin',
                       'q_asset_turnover', 'q_accruals', 'q_ocf_to_profit',
                       'parent_net_profit__ttm', 'revenue__ttm',
                       'operating_cf__ttm']) \
        .filter(pl.col('ann_date').is_not_null()) \
        .sort(['ts_code', 'ann_date']) \
        .unique(subset=['ts_code', 'ann_date'], keep='last') \
        .sort(['ts_code', 'ann_date'])


def compute_financial_candidates(cross: pl.DataFrame,
                                 reports: pl.DataFrame,
                                 valuation: pl.DataFrame,
                                 max_staleness_months: int | None
                                 = MAX_FIN_STALENESS_MONTHS) -> pl.DataFrame:
    """把报告级财务候选按 PIT 对齐到 (date, ts_code) 截面。

    Args:
        cross: 含 date, ts_code 的截面索引。
        reports: ``build_pit_reports`` 的输出。
        valuation: 含 ts_code, trade_date, total_mv 的日估值。
        max_staleness_months: 报告 ``end_date`` 距截面日超过该月数即置 null；
            ``None`` 关闭守卫（仅反证用例使用）。

    Returns:
        date, ts_code + FINANCIAL_CANDIDATES。
    """
    c = cross.select(['ts_code', 'date']).unique().sort(['ts_code', 'date'])
    j = c.join_asof(reports, left_on='date', right_on='ann_date', by='ts_code',
                    strategy='backward')
    if max_staleness_months is not None:
        stale = pl.col('end_date').is_null() | (
            pl.col('date')
            > pl.col('end_date').dt.offset_by(f'{max_staleness_months}mo'))
        j = j.with_columns([
            pl.when(stale).then(None).otherwise(pl.col(name)).alias(name)
            for name in _STALE_GUARD_COLS])
    val = valuation.select(['ts_code', 'trade_date', 'total_mv']) \
        .sort(['ts_code', 'trade_date'])
    j = j.sort(['ts_code', 'date']).join_asof(
        val, left_on='date', right_on='trade_date', by='ts_code',
        strategy='backward')
    j = j.with_columns([
        pl.when(pl.col('total_mv') > 0)
        .then(pl.col('parent_net_profit__ttm') / pl.col('total_mv'))
        .otherwise(None).alias('v_ep'),
        pl.when(pl.col('total_mv') > 0)
        .then(pl.col('operating_cf__ttm') / pl.col('total_mv'))
        .otherwise(None).alias('v_cfp'),
        pl.when(pl.col('total_mv') > 0)
        .then(pl.col('revenue__ttm') / pl.col('total_mv'))
        .otherwise(None).alias('v_sp'),
    ])
    return j.select(['date', 'ts_code', *FINANCIAL_CANDIDATES])


def compute_all_candidates(panel: pl.DataFrame,
                           market_returns: pl.DataFrame,
                           cross: pl.DataFrame,
                           reports: pl.DataFrame,
                           valuation: pl.DataFrame,
                           max_staleness_months: int | None
                           = MAX_FIN_STALENESS_MONTHS) -> pl.DataFrame:
    """价格类 + 财务类候选合并（调用方自行决定是否使用）。"""
    price = compute_price_candidates(panel, market_returns)
    fin = compute_financial_candidates(cross, reports, valuation,
                                       max_staleness_months)
    return price.join(fin, on=['date', 'ts_code'], how='left')
