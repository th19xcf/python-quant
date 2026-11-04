#!/usr/bin/env python3

"""股本 / 财务 / 估值类因子（规模、价值、投资、真实换手）。

数据来源（全部只读，不写、不改数据）：
- ``data/local_store/meta/factors/stock_shares.parquet``：股本区间事实，
  按 ``effective_date`` forward-fill（``shares_as_of`` 同一语义）；
- ``data/local_store/meta/financials/{income,balance}.parquet``：财务三表，
  **可见性一律以 ``ann_date`` 判定**，同报告期多条公告取最新一版；
- ``data/local_store/meta/financials/valuation_daily.parquet``：日频估值，
  按 ``trade_date`` 向后 as-of 取值（停牌日取最近一个交易日，不偷看未来）。

T TM 口径：财报表为「年初至今累计」，故
``TTM = 本期累计 + 上年年报 - 上年同期累计``；年报期（end_date 为 12-31）
TTM 即本期值。投资因子用「总资产同比」（同报告期 vs 上年同期）。

所有因子都无量纲或在截面上可比（规模取对数、比率类取比值），
缺失一律保留 null，由调用方按可用权重归一，绝不填 0。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable

import polars as pl

from src.data.financials.store import read_table
from src.data.local_store.share_capital import read_shares


@dataclass(frozen=True)
class FundamentalCandidate:
    """基本面候选因子：列名 + 风格 + 公式 + 经济方向。"""

    column: str
    style: str
    expression: str
    direction: float
    note: str = ''


#: 由股本/财务/估值解锁的候选（原先在 factor_lib 里登记为「不可算」）
FUNDAMENTAL_CANDIDATES: tuple[FundamentalCandidate, ...] = (
    FundamentalCandidate('ff_size', 'size', 'ln(流通市值)', -1.0,
                         '小市值溢价；流通市值缺失时用总市值'),
    FundamentalCandidate('ff_value_bp', 'value', '净资产 / 总市值', 1.0,
                         '最新已公告资产负债表权益，PB 的倒数口径'),
    FundamentalCandidate('ff_value_ep', 'value', '归母净利润TTM / 总市值', 1.0,
                         '盈利收益率，亏损公司保留负值'),
    FundamentalCandidate('ff_investment', 'value', '总资产同比', -1.0,
                         'Fama-French CMA 方向：扩张越快的公司后续收益越低'),
    FundamentalCandidate('ff_sales_yield', 'value', '营业收入TTM / 总市值', 1.0,
                         '营收收益率（PS 的倒数口径）'),
    FundamentalCandidate('ff_div_yield', 'value', '近12个月股息率', 1.0,
                         'valuation_daily.dv_ratio（百分数归一为比值）'),
    FundamentalCandidate('ext_turnover_20', 'volume', '20日均真实换手率', -1.0,
                         'volume×100 / 流通股本，20日均值；高换手偏空'),
)

FUNDAMENTAL_COLUMNS: tuple[str, ...] = tuple(
    item.column for item in FUNDAMENTAL_CANDIDATES)

_INCOME_COLUMNS = ('revenue', 'parent_net_profit')
_BALANCE_COLUMNS = ('total_assets', 'total_equity')


def _dedupe_period(frame: pl.DataFrame, columns: Iterable[str]) -> pl.DataFrame:
    """同一报告期只保留 ``ann_date`` 最新的一版（更正/重述以最新为准）。"""
    values = list(columns)
    return (
        frame.filter(pl.col('ann_date').is_not_null())
        .sort(['ts_code', 'end_date', 'ann_date'])
        .group_by(['ts_code', 'end_date'], maintain_order=True)
        .agg([pl.col('ann_date').last(), *(pl.col(name).last() for name in values)])
    )


def ttm_from_cumulative(frame: pl.DataFrame, columns: Iterable[str]) -> pl.DataFrame:
    """把年初至今累计的报表值转成 TTM（按 ``end_date``，不做可见性判断）。

    Args:
        frame: 至少含 ``ts_code / end_date / ann_date`` 与待转换列。
    Returns:
        含 ``ann_date`` 与 ``<col>__ttm`` 的帧（一行一个报告期）。
    """
    values = list(columns)
    current = _dedupe_period(frame, values)
    previous = current.select([
        'ts_code',
        pl.col('end_date').alias('_prev_end'),
        *[pl.col(name).alias(f'{name}__prev_q') for name in values],
    ])
    merged = current.with_columns(
        pl.col('end_date').dt.offset_by('-1y').alias('_prev_end')
    ).join(previous, on=['ts_code', '_prev_end'], how='left')

    annual = (
        current.filter(pl.col('end_date').dt.month() == 12)
        .select(['ts_code', pl.col('end_date').alias('_annual_end'),
                 *[pl.col(name).alias(f'{name}__annual') for name in values]])
        .with_columns((pl.col('_annual_end').dt.year() + 1).alias('_fy'))
        .select(['ts_code', '_fy', *[f'{name}__annual' for name in values]])
    )
    merged = merged.with_columns(pl.col('end_date').dt.year().alias('_fy'))
    merged = merged.join(annual, on=['ts_code', '_fy'], how='left')

    is_annual_period = pl.col('end_date').dt.month() == 12
    exprs = [
        pl.when(is_annual_period).then(pl.col(name)).otherwise(
            pl.col(name) + pl.col(f'{name}__annual') - pl.col(f'{name}__prev_q')
        ).alias(f'{name}__ttm')
        for name in values
    ]
    merged = merged.with_columns(exprs)
    return merged.select([
        'ts_code', 'ann_date', 'end_date', *[f'{name}__ttm' for name in values]])


def balance_metrics(frame: pl.DataFrame) -> pl.DataFrame:
    """最新已公告的净资产 + 总资产同比（同报告期 vs 上年同期）。"""
    current = _dedupe_period(frame, _BALANCE_COLUMNS)
    previous = current.select([
        'ts_code',
        pl.col('end_date').alias('_prev_end'),
        pl.col('total_assets').alias('_assets_prev'),
    ])
    merged = current.with_columns(
        pl.col('end_date').dt.offset_by('-1y').alias('_prev_end')
    ).join(previous, on=['ts_code', '_prev_end'], how='left')
    yoy = pl.when(pl.col('_assets_prev') > 0).then(
        pl.col('total_assets') / pl.col('_assets_prev') - 1.0).otherwise(None)
    return merged.select([
        'ts_code', 'ann_date', 'end_date', 'total_equity', yoy.alias('assets_yoy')])


def _asof_join(left: pl.DataFrame, right: pl.DataFrame, left_on: str,
               right_on: str) -> pl.DataFrame:
    """按 (ts_code, 日期) 向后 as-of 连接；右侧列不可见未来值。"""
    if right.is_empty():
        return left
    return left.join_asof(
        right.sort(['ts_code', right_on]),
        left_on=left_on, right_on=right_on, by='ts_code', strategy='backward',
        check_sortedness=False)


def build_fundamentals(panel: pl.DataFrame, valuation: pl.DataFrame,
                       shares: pl.DataFrame, income: pl.DataFrame,
                       balance: pl.DataFrame) -> pl.DataFrame:
    """纯计算入口（数据帧由调用方提供，便于单测与离线复算）。

    Args:
        panel: 至少含 ``ts_code / date / volume``；
        valuation: ``ts_code / trade_date / total_mv / circ_mv / dv_ratio``；
        shares: ``ts_code / effective_date / float_share / total_share``；
        income / balance: 财务三表原始行（含 ``ann_date``）。
    Returns:
        原面板 + :data:`FUNDAMENTAL_COLUMNS`（顺序与输入一致）。
    """
    base = (panel.select(['ts_code', 'date', 'volume'])
            .with_row_index('_row').sort(['ts_code', 'date']))

    if not valuation.is_empty():
        base = _asof_join(
            base,
            valuation.select(['ts_code', pl.col('trade_date'), 'total_mv',
                              'circ_mv', 'dv_ratio']),
            'date', 'trade_date')

    if not shares.is_empty():
        share_frame = shares.select(['ts_code', 'effective_date', 'float_share'])
        share_frame = share_frame.filter(
            pl.col('float_share').is_not_null() & (pl.col('float_share') > 0))
        base = _asof_join(base, share_frame, 'date', 'effective_date')
        base = base.with_columns(
            pl.col('float_share').cast(pl.Float64).alias('_float_share'))
    else:
        base = base.with_columns(
            pl.lit(None, dtype=pl.Float64).alias('_float_share'))

    income_view = ttm_from_cumulative(income, _INCOME_COLUMNS) if not income.is_empty() \
        else pl.DataFrame()
    balance_view = balance_metrics(balance) if not balance.is_empty() \
        else pl.DataFrame()
    base = _asof_join(base, income_view, 'date', 'ann_date')
    base = _asof_join(base, balance_view, 'date', 'ann_date')

    required = ('total_mv', 'circ_mv', 'dv_ratio', 'total_equity', 'assets_yoy',
                'revenue__ttm', 'parent_net_profit__ttm')
    base = base.with_columns([
        pl.lit(None, dtype=pl.Float64).alias(name)
        for name in required if name not in base.columns])

    market_cap = pl.col('total_mv')
    circ_cap = pl.when(pl.col('circ_mv') > 0).then(pl.col('circ_mv')) \
        .otherwise(pl.col('total_mv'))
    valid_cap = market_cap.is_not_null() & (market_cap > 0)

    size = pl.when(circ_cap > 0).then(circ_cap.log()).otherwise(None)
    bp = pl.when(valid_cap & (pl.col('total_equity') > 0)).then(
        pl.col('total_equity') / market_cap).otherwise(None)
    ep = pl.when(valid_cap).then(
        pl.col('parent_net_profit__ttm') / market_cap).otherwise(None)
    sales_yield = pl.when(valid_cap).then(
        pl.col('revenue__ttm') / market_cap).otherwise(None)
    investment = pl.col('assets_yoy')
    div_yield = pl.when(pl.col('dv_ratio').is_not_null()).then(
        pl.col('dv_ratio') / 100.0).otherwise(None)

    turnover = pl.when((pl.col('_float_share') > 0) & (pl.col('volume') > 0)).then(
        pl.col('volume') * 100.0 / pl.col('_float_share')).otherwise(None)
    turnover_20 = turnover.rolling_mean(window_size=20, min_samples=10).over('ts_code')

    out = base.with_columns([
        size.alias('ff_size'),
        bp.alias('ff_value_bp'),
        ep.alias('ff_value_ep'),
        investment.alias('ff_investment'),
        sales_yield.alias('ff_sales_yield'),
        div_yield.alias('ff_div_yield'),
        turnover_20.alias('ext_turnover_20'),
    ])
    out = out.sort('_row').drop('_row')
    return panel.with_columns([out[name] for name in FUNDAMENTAL_COLUMNS])


def compute_fundamental_factors(panel: pl.DataFrame,
                                columns: Iterable[str] | None = None,
                                store_root: Any = None) -> pl.DataFrame:
    """读取本地股本/财务/估值并追加基本面因子列（只读，不写任何文件）。"""
    if panel.is_empty():
        return panel
    codes = panel['ts_code'].unique().to_list()
    dates = panel['date'].unique().sort().to_list()
    start: date = dates[0]
    end: date = dates[-1]

    valuation = read_table('valuation', store_root, codes=codes,
                           start=start, end=end)
    income = read_table('income', store_root, codes=codes)
    balance = read_table('balance', store_root, codes=codes)
    shares = read_shares(columns=['ts_code', 'effective_date', 'float_share'])
    if shares is None:
        shares = pl.DataFrame(schema={'ts_code': pl.Utf8,
                                      'effective_date': pl.Date,
                                      'float_share': pl.Int64})
    shares = shares.filter(pl.col('ts_code').is_in(codes))

    result = build_fundamentals(panel, valuation, shares, income, balance)
    wanted = set(columns or FUNDAMENTAL_COLUMNS)
    keep = [name for name in FUNDAMENTAL_COLUMNS if name in wanted]
    return result.select(list(panel.columns) + keep)


__all__ = [
    'FUNDAMENTAL_CANDIDATES',
    'FUNDAMENTAL_COLUMNS',
    'FundamentalCandidate',
    'balance_metrics',
    'build_fundamentals',
    'compute_fundamental_factors',
    'ttm_from_cumulative',
]
