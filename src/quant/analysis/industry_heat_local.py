"""行业热度状态：本地数据装配与 CLI 入口（只读，不写 data/，不引入外部数据源）。

装配口径见 ``docs/行业热度判定方法_2026-11-15.md``：

- 行业归属只用申万 PIT 区间表 ``meta/industry_pit/industry_pit_sw.parquet``；
  2021-07-30 的 L1 拆分（210000 采掘 → 740000 煤炭 / 750000 石油石化）不做历史拼接；
- 行业日收益用后复权价等权聚合，剔除停牌（``volume == 0``）与触板样本；
- 数值只落到内存，不落盘、不写 manifest。

重量级聚合（个股行情 → 行业日频面板）在本模块完成；打分与标签在
``industry_heat_state.py``。
"""

from __future__ import annotations

import argparse
import time
from datetime import date, datetime
from pathlib import Path

import polars as pl
from loguru import logger

from src.data.local_store.store import resolve_store_root
from src.quant.analysis.industry_heat_state import (
    evaluate_industry_heat,
    format_heat_detail,
)
from src.utils.config import get_config

#: 申万 2014 版树生效日；此前为 2003 版代码，行业归属不可回溯
SW_TREE_EFFECTIVE_DATE = date(2014, 2, 21)
#: 估值与景气度回看缓冲（自然日），保障 250 日滚动分位有满窗样本
LOOKBACK_BUFFER = '2012-01-01'


def _default_root() -> Path:
    return resolve_store_root(get_config())


def _load_sw_pit(base: Path) -> pl.DataFrame:
    """申万 L1 区间表，按代码取最新名称作为展示名（2021-07 名称未变）。"""
    pit = (
        pl.read_parquet(
            base / 'meta' / 'industry_pit' / 'industry_pit_sw.parquet',
            columns=['ts_code', 'industry_l1', 'industry_l1_name', 'in_date', 'out_date'],
        )
        .filter(pl.col('industry_l1_name').is_not_null())
        .with_columns(
            pl.col('in_date').cast(pl.Date),
            pl.col('out_date').cast(pl.Date),
            pl.col('industry_l1').alias('industry_code'),
        )
    )
    canonical = (
        pit.sort(['industry_l1', 'in_date'])
        .group_by('industry_l1')
        .agg(pl.col('industry_l1_name').last().alias('industry_name'))
    )
    return pit.join(canonical, on='industry_l1', how='left').select(
        ['ts_code', 'industry_code', 'industry_name', 'in_date', 'out_date']
    )


def _attach_industry(frame: pl.DataFrame, pit: pl.DataFrame) -> pl.DataFrame:
    """按 (ts_code, date) 做 as-of 连接，并剔除落在区间外的行。"""
    right = pit.sort(['ts_code', 'in_date'])
    joined = frame.sort(['ts_code', 'date']).join_asof(
        right,
        left_on='date',
        right_on='in_date',
        by='ts_code',
        strategy='backward',
        check_sortedness=False,
    )
    return joined.filter(
        pl.col('industry_code').is_not_null()
        & (pl.col('out_date').is_null() | (pl.col('date') <= pl.col('out_date')))
    )


def _load_quotes(base: Path) -> pl.DataFrame:
    """个股日线 → 后复权价、日收益、MA60 与 250 日新高/新低。"""
    quotes = (
        pl.scan_parquet(str(base / 'quotes' / 'stock' / '**' / '*.parquet'))
        .select(['date', 'ts_code', 'close', 'volume', 'amount', 'pct_chg', 'hfq_factor'])
        .filter(pl.col('date') >= pl.lit(LOOKBACK_BUFFER).str.to_date())
        .collect()
        .sort(['ts_code', 'date'])
        .with_columns((pl.col('close') * pl.col('hfq_factor')).alias('adj'))
        .with_columns(pl.col('adj').shift(1).over('ts_code').alias('adj_prev'))
        .with_columns((pl.col('adj') / pl.col('adj_prev') - 1).alias('ret'))
        .with_columns(
            pl.col('adj').rolling_mean(60, min_samples=60).over('ts_code').alias('ma60'),
            pl.col('adj').rolling_max(250, min_samples=250).over('ts_code').alias('hi250'),
            pl.col('adj').rolling_min(250, min_samples=250).over('ts_code').alias('lo250'),
        )
    )
    limit = (
        pl.when(pl.col('ts_code').str.ends_with('.BJ')).then(0.295)
        .when(pl.col('ts_code').str.slice(0, 3).is_in(['300', '301', '688', '689'])).then(0.195)
        .otherwise(0.095)
    )
    return quotes.with_columns(limit.alias('_limit')).with_columns(
        (pl.col('volume') > 0).alias('_traded'),
        (pl.col('pct_chg').abs() < pl.col('_limit') * 100 * 0.98).alias('_not_limit'),
    )


def _load_float_share(base: Path) -> pl.DataFrame:
    return (
        pl.read_parquet(
            base / 'meta' / 'factors' / 'stock_shares.parquet',
            columns=['ts_code', 'effective_date', 'float_share'],
        )
        .with_columns(pl.col('effective_date').cast(pl.Date))
        .filter(pl.col('float_share') > 0)
        .sort(['ts_code', 'effective_date'])
    )


def _market_daily(quotes: pl.DataFrame) -> pl.DataFrame:
    """全 A 等权日收益（与行业聚合同一套过滤口径）。"""
    valid = quotes.filter(
        pl.col('_traded') & pl.col('_not_limit') & pl.col('ret').is_not_null()
    )
    return (
        valid.group_by('date')
        .agg(
            pl.col('ret').mean().alias('mkt_ret'),
            pl.col('amount').sum().alias('mkt_amount'),
        )
        .sort('date')
        .with_columns((1 + pl.col('mkt_ret')).cum_prod().alias('mkt_nav'))
        .with_columns(
            (pl.col('mkt_nav') / pl.col('mkt_nav').shift(20) - 1).alias('mkt_ret20'),
            (pl.col('mkt_nav') / pl.col('mkt_nav').shift(60) - 1).alias('mkt_ret60'),
        )
    )


def _industry_daily(quotes: pl.DataFrame, pit: pl.DataFrame) -> pl.DataFrame:
    """个股 → 行业日频聚合（等权收益、量能、广度），不依赖 breadth 派生表。"""
    valid = quotes.filter(
        pl.col('_traded') & pl.col('_not_limit') & pl.col('ret').is_not_null()
    )
    attached = _attach_industry(valid, pit)
    with_flags = attached.with_columns(
        (pl.col('adj') > pl.col('ma60')).cast(pl.Int8).alias('_above60'),
        (pl.col('adj') >= pl.col('hi250')).cast(pl.Int8).alias('_new_high'),
        (pl.col('adj') <= pl.col('lo250')).cast(pl.Int8).alias('_new_low'),
        (pl.col('ret') > 0).cast(pl.Int8).alias('_up'),
        (pl.col('ret') < 0).cast(pl.Int8).alias('_down'),
        (pl.col('volume') * 100).alias('_shares'),
    )
    return (
        with_flags.group_by(['date', 'industry_code'])
        .agg(
            pl.col('industry_name').last().alias('industry_name'),
            pl.col('ret').mean().alias('ret'),
            pl.col('amount').sum().alias('amount'),
            pl.len().alias('n_stocks'),
            pl.col('_up').sum().alias('_up_n'),
            pl.col('_down').sum().alias('_down_n'),
            (pl.col('_above60').mean() * 100).alias('above60'),
            (pl.col('_new_high').mean() * 100 - pl.col('_new_low').mean() * 100).alias('highlow_net'),
        )
        .with_columns(
            (pl.col('_up_n') / (pl.col('_up_n') + pl.col('_down_n')) * 100).alias('up_ratio'),
        )
        .drop(['_up_n', '_down_n'])
        .sort(['industry_code', 'date'])
    )


def _attach_turnover(
    industry: pl.DataFrame,
    quotes: pl.DataFrame,
    shares: pl.DataFrame,
    pit: pl.DataFrame,
) -> pl.DataFrame:
    """行业换手率 = Σ成交股数 / Σ流通股本（流通股本按 effective_date PIT 取值）。"""
    traded = quotes.filter(pl.col('_traded')).select(['date', 'ts_code', 'volume'])
    with_share = (
        traded.sort(['ts_code', 'date'])
        .join_asof(
            shares,
            left_on='date',
            right_on='effective_date',
            by='ts_code',
            strategy='backward',
            check_sortedness=False,
        )
        .filter(pl.col('float_share').is_not_null())
        .with_columns((pl.col('volume') * 100.0 / pl.col('float_share')).alias('turn'))
    )
    attached = _attach_industry(with_share, pit)
    per_industry = (
        attached.group_by(['date', 'industry_code'])
        .agg(pl.col('turn').mean().alias('turn'))
        .sort(['industry_code', 'date'])
    )
    return industry.join(per_industry, on=['date', 'industry_code'], how='left')


def _valuation_daily(base: Path, pit: pl.DataFrame) -> pl.DataFrame:
    """行业 PE/PB 中位数与全 A PE 中位数（亏损样本剔除）。"""
    valuation = (
        pl.read_parquet(
            base / 'meta' / 'financials' / 'valuation_daily.parquet',
            columns=['ts_code', 'trade_date', 'pe', 'pb'],
        )
        .filter(pl.col('trade_date') >= pl.lit(LOOKBACK_BUFFER).str.to_date())
        .rename({'trade_date': 'date'})
        .with_columns(
            pl.when(pl.col('pe') > 0).then(pl.col('pe')).otherwise(None).alias('pe'),
            pl.when(pl.col('pb') > 0).then(pl.col('pb')).otherwise(None).alias('pb'),
        )
    )
    market_pe = (
        valuation.group_by('date')
        .agg(pl.col('pe').median().alias('market_pe'))
        .sort('date')
    )
    attached = _attach_industry(valuation, pit)
    per_industry = (
        attached.group_by(['date', 'industry_code'])
        .agg(
            pl.col('pe').median().alias('pe_med'),
            pl.col('pb').median().alias('pb_med'),
        )
        .sort(['industry_code', 'date'])
    )
    return per_industry.join(market_pe, on='date', how='left')


def _fundamental_daily(base: Path, pit: pl.DataFrame) -> pl.DataFrame:
    """行业营收/净利同比中位数：每只股票取 ann_date<=t 的最新一期，按公告日 PIT。"""
    income = (
        pl.read_parquet(
            base / 'meta' / 'financials' / 'income.parquet',
            columns=['ts_code', 'end_date', 'ann_date', 'revenue_yoy', 'net_profit_yoy'],
        )
        .filter(pl.col('ann_date').is_not_null())
        .sort(['ts_code', 'ann_date', 'end_date'])
        .unique(subset=['ts_code', 'ann_date'], keep='last')
        .rename({'ann_date': 'date'})
    )
    attached = _attach_industry(income, pit)
    return (
        attached.group_by(['date', 'industry_code'])
        .agg(
            pl.col('revenue_yoy').median().alias('rev_yoy_med'),
            pl.col('net_profit_yoy').median().alias('np_yoy_med'),
        )
        .sort(['industry_code', 'date'])
    )


def _apply_fundamental_asof(industry: pl.DataFrame, fundamental: pl.DataFrame) -> pl.DataFrame:
    """把季频景气度前值延续到日频（公告日之后才生效）。"""
    left = industry.sort(['industry_code', 'date'])
    right = fundamental.sort(['industry_code', 'date'])
    return left.join_asof(
        right,
        left_on='date',
        right_on='date',
        by='industry_code',
        strategy='backward',
        check_sortedness=False,
    )


def build_industry_daily(base: Path) -> pl.DataFrame:
    """装配行业日频面板（原始指标，不含打分）。"""
    pit = _load_sw_pit(base)

    quotes = _load_quotes(base)
    market = _market_daily(quotes)
    industry = _industry_daily(quotes, pit)
    industry = _attach_turnover(industry, quotes, _load_float_share(base), pit)

    industry = industry.join(
        market.select(['date', 'mkt_ret', 'mkt_amount', 'mkt_ret20', 'mkt_ret60']),
        on='date',
        how='left',
    )
    valuation = _valuation_daily(base, pit)
    industry = industry.join(valuation, on=['date', 'industry_code'], how='left')

    fundamental = _fundamental_daily(base, pit)
    industry = _apply_fundamental_asof(industry, fundamental)

    industry = (
        industry.sort(['industry_code', 'date'])
        .with_columns((1 + pl.col('ret')).cum_prod().over('industry_code').alias('nav'))
        .with_columns(
            (pl.col('nav') / pl.col('nav').shift(20).over('industry_code') - 1).alias('ind_ret20'),
            (pl.col('nav') / pl.col('nav').shift(60).over('industry_code') - 1).alias('ind_ret60'),
            pl.col('ret').rolling_std(60, min_samples=60).over('industry_code').alias('vol60'),
        )
        .with_columns(
            (pl.col('ind_ret20') - pl.col('mkt_ret20')).alias('rs20'),
            (pl.col('ind_ret60') - pl.col('mkt_ret60')).alias('rs60'),
            (pl.col('ind_ret60') / pl.col('vol60')).alias('mom60'),
            (pl.col('nav') / pl.col('nav').rolling_mean(60, min_samples=60).over('industry_code') - 1).alias('dev60'),
            pl.col('amount').rolling_mean(20, min_samples=20).over('industry_code').alias('_amount20'),
        )
        .with_columns(
            (pl.col('_amount20') / pl.col('mkt_amount').rolling_mean(20, min_samples=20)).alias('share20'),
            (pl.col('ret').rolling_sum(5, min_samples=5).over('industry_code')).alias('_ret5'),
        )
        .with_columns(
            pl.col('turn').rolling_mean(20, min_samples=20).over('industry_code').alias('turnover20'),
            (pl.col('amount') / pl.col('_amount20') * pl.col('_ret5').sign()).alias('amount_confirm'),
        )
        .with_columns(
            (pl.col('share20') / pl.col('share20').rolling_mean(250, min_samples=250).over('industry_code') - 1).alias('conc'),
            (pl.col('pe_med') / pl.col('market_pe') - 1).alias('premium'),
        )
        .filter(pl.col('date') >= pl.lit(SW_TREE_EFFECTIVE_DATE))
    )
    return industry.select([
        'date', 'industry_code', 'industry_name', 'n_stocks',
        'ret', 'rs20', 'rs60', 'mom60', 'dev60',
        'share20', 'turnover20', 'amount_confirm', 'conc', 'premium',
        'above60', 'up_ratio', 'highlow_net',
        'pe_med', 'pb_med', 'rev_yoy_med', 'np_yoy_med',
    ])


def load_local_panel(root: Path | str | None = None) -> dict[str, pl.DataFrame]:
    """读取并装配判定所需的本地表；不新增外部采集。"""
    base = Path(root) if root is not None else _default_root()
    started = time.time()
    industry_daily = build_industry_daily(base)
    logger.debug(
        '行业日频面板装配完成：{} 行 / {} 个行业 / 耗时 {:.1f}s',
        industry_daily.height,
        industry_daily['industry_code'].n_unique(),
        time.time() - started,
    )
    return {'industry_daily': industry_daily}


def run_cli(
    asof: str | None = None,
    panel_root: str | None = None,
    top: int | None = None,
) -> None:
    target = datetime.strptime(asof, '%Y-%m-%d').date() if asof else None
    state = evaluate_industry_heat(load_local_panel(panel_root), asof=target)
    if top and top > 0:
        hottest = state.industries[:top]
        coldest = state.industries[-top:]
        from src.quant.analysis.industry_heat_state import format_industry_line

        logger.info('行业热度（{}）最热 {} 个：', state.asof, len(hottest))
        for heat in hottest:
            logger.info(format_industry_line(heat))
        logger.info('行业热度（{}）最冷 {} 个：', state.asof, len(coldest))
        for heat in reversed(coldest):
            logger.info(format_industry_line(heat))
        for note in state.notes:
            logger.info('说明：{}', note)
        return
    for line in format_heat_detail(state).splitlines():
        logger.info(line)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='行业热度状态判定（只读本地数据）')
    parser.add_argument('--asof', help='判定截止日，格式 YYYY-MM-DD')
    parser.add_argument('--panel-root', help='本地列存根目录，默认按配置解析')
    parser.add_argument('--top', type=int, help='只输出最热/最冷各 N 个行业')
    args = parser.parse_args(argv)
    run_cli(asof=args.asof, panel_root=args.panel_root, top=args.top)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
