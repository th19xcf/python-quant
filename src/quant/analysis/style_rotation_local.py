"""风格轮动状态：本地数据装配与 CLI 入口（只读，不写 data/，不引入外部数据源）。

装配口径见 ``docs/风格轮动判定方法_2026-11-16.md``：

- 分组只用当日可得数据：规模用 ``close × total_share``（``effective_date`` PIT），
  价值用当日 PB（``valuation_daily.parquet``），波动率用截至 t−1 的 60 日已实现波动率；
- 只用非停牌（``volume > 0``）且未触板的样本，组内等权；
- 收益用后复权价计算，市值用未复权收盘价；
- 数值只落到内存，不落盘、不写 manifest。

重量级聚合（个股行情 → 风格日频面板）在本模块完成；打分与标签在
``style_rotation_state.py``。
"""

from __future__ import annotations

import argparse
import time
from datetime import date, datetime
from pathlib import Path

import polars as pl
from loguru import logger

from src.data.local_store.store import resolve_store_root
from src.quant.analysis.style_rotation_state import (
    evaluate_style_rotation,
    format_style_detail,
)
from src.utils.config import get_config

#: 回看缓冲（自然日），保障 250 日滚动分位有满窗样本
LOOKBACK_BUFFER = '2012-01-01'
#: 面板输出起点；此前只作为滚动窗口缓冲
PANEL_START_DATE = date(2013, 1, 1)
#: 分组边界：低 30% / 中 40% / 高 30%
LOW_QUANTILE = 0.30
HIGH_QUANTILE = 0.70


def _default_root() -> Path:
    return resolve_store_root(get_config())


def suspension_aware_ret(quotes: pl.DataFrame) -> pl.DataFrame:
    """在个股日线上补 ``adj`` / ``adj_prev`` / ``ret``（停牌感知）。

    口径与 ``tools/research/prepare_monthly_samples.py`` 的停牌处理一致：

    - 占位 bar 判据：``volume`` 为空或 ``<= 0``；
    - 交易日序取本帧全部标的的日期并集（本模块扫描全市场行情，等价于市场日历）；
    - 只有「当前 bar 与上一 bar 都真实成交，且两者在交易日序上相邻」时才计算
      单日收益；跨停牌（缺行或占位 bar）一律记缺失，绝不把停牌前后压成一根
      单日收益（如 ``000622.SZ`` 2006→2013、2025 长期停牌复牌会被记成 +369% /
      −89%），也不 forward-fill 价格、不拿缺口当 0 收益。
    """
    days = quotes.select('date').unique().sort('date').with_columns(
        pl.int_range(0, pl.len()).cast(pl.Int64).alias('_tidx'))
    frame = quotes.join(days, on='date', how='left').sort(['ts_code', 'date'])
    frame = frame.with_columns(
        (pl.col('close') * pl.col('hfq_factor')).alias('adj'),
        (pl.col('volume').is_null() | (pl.col('volume') <= 0)).alias('_ph'),
    ).with_columns(
        pl.col('adj').shift(1).over('ts_code').alias('adj_prev'),
        pl.col('_tidx').shift(1).over('ts_code').alias('_tidx_prev'),
        pl.col('_ph').shift(1).over('ts_code').alias('_ph_prev'),
    )
    adjacent = (
        (pl.col('_tidx') == pl.col('_tidx_prev') + 1)
        & ~pl.col('_ph') & ~pl.col('_ph_prev')
        & pl.col('adj').is_not_null() & pl.col('adj_prev').is_not_null()
    )
    return (
        frame.with_columns(
            pl.when(adjacent).then(pl.col('adj') / pl.col('adj_prev') - 1.0)
            .otherwise(None).alias('ret'),
        )
        .drop(['_tidx', '_tidx_prev', '_ph', '_ph_prev'])
    )


def _load_quotes(base: Path) -> pl.DataFrame:
    """个股日线 → 后复权收益、60 日已实现波动率与停牌/触板标记。"""
    quotes = (
        pl.scan_parquet(str(base / 'quotes' / 'stock' / '**' / '*.parquet'))
        .select(['date', 'ts_code', 'close', 'volume', 'amount', 'pct_chg', 'hfq_factor'])
        .filter(pl.col('date') >= pl.lit(LOOKBACK_BUFFER).str.to_date())
        .collect()
    )
    quotes = suspension_aware_ret(quotes).with_columns(
        pl.col('ret').rolling_std(60, min_samples=60).over('ts_code').alias('vol60'),
    ).with_columns(
        pl.col('vol60').shift(1).over('ts_code').alias('vol60_prev'),
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


def _load_shares(base: Path) -> pl.DataFrame:
    """总股本与流通股本（按 effective_date PIT 取值）。"""
    return (
        pl.read_parquet(
            base / 'meta' / 'factors' / 'stock_shares.parquet',
            columns=['ts_code', 'effective_date', 'total_share', 'float_share'],
        )
        .with_columns(pl.col('effective_date').cast(pl.Date))
        .filter(pl.col('total_share') > 0)
        .sort(['ts_code', 'effective_date'])
    )


def _load_valuation(base: Path) -> pl.DataFrame:
    """当日 PB（<=0 视为缺失）。"""
    return (
        pl.read_parquet(
            base / 'meta' / 'financials' / 'valuation_daily.parquet',
            columns=['ts_code', 'trade_date', 'pb'],
        )
        .filter(pl.col('trade_date') >= pl.lit(LOOKBACK_BUFFER).str.to_date())
        .rename({'trade_date': 'date'})
        .with_columns(
            pl.when(pl.col('pb') > 0).then(pl.col('pb')).otherwise(None).alias('pb'),
        )
    )


def _load_np_yoy(base: Path) -> pl.DataFrame:
    """净利润同比：每只股票取 ann_date<=t 的最新一期（公告日 PIT）。"""
    return (
        pl.read_parquet(
            base / 'meta' / 'financials' / 'income.parquet',
            columns=['ts_code', 'end_date', 'ann_date', 'net_profit_yoy'],
        )
        .filter(pl.col('ann_date').is_not_null())
        .sort(['ts_code', 'ann_date', 'end_date'])
        .unique(subset=['ts_code', 'ann_date'], keep='last')
        .rename({'ann_date': 'date'})
        .select(['ts_code', 'date', 'net_profit_yoy'])
    )


def _attach_shares(quotes: pl.DataFrame, shares: pl.DataFrame) -> pl.DataFrame:
    """按 (ts_code, date) 做 as-of 连接，得到当日总市值、流通市值与换手率。"""
    joined = quotes.sort(['ts_code', 'date']).join_asof(
        shares,
        left_on='date',
        right_on='effective_date',
        by='ts_code',
        strategy='backward',
        check_sortedness=False,
    )
    return joined.with_columns(
        (pl.col('close') * pl.col('total_share')).alias('mcap'),
        (pl.col('volume') * 100.0 / pl.col('float_share')).alias('turn'),
    )


def _rank_pct(column: str) -> pl.Expr:
    """当日横截面百分位排名（0~1）；该列缺失的样本不参与排名。"""
    valid = pl.col(column).is_not_null()
    return (
        pl.when(valid)
        .then(pl.col(column).rank('ordinal').over('date') / valid.sum().over('date'))
        .otherwise(None)
    )


def _bucket(rank: pl.Expr, low_label: str, high_label: str) -> pl.Expr:
    """按百分位分成 低 30% / 中 40% / 高 30%；排名缺失保持空。"""
    return (
        pl.when(rank.is_null()).then(pl.lit(None, dtype=pl.Utf8))
        .when(rank <= LOW_QUANTILE).then(pl.lit(low_label))
        .when(rank >= HIGH_QUANTILE).then(pl.lit(high_label))
        .otherwise(pl.lit('mid'))
    )


def _daily_panel(valid: pl.DataFrame) -> pl.DataFrame:
    """个股 → 风格日频面板：组内等权收益、成交占比、估值与景气度中位数。"""
    frame = valid.with_columns(
        _bucket(_rank_pct('mcap'), 'small', 'large').alias('size_group'),
        _bucket(_rank_pct('pb'), 'value', 'growth').alias('value_group'),
        _bucket(_rank_pct('vol60_prev'), 'low', 'high').alias('vol_group'),
    )

    def mean_where(group: str, label: str) -> pl.Expr:
        return pl.col('ret').filter(pl.col(group) == label).mean()

    def combo(size: str, value: str) -> pl.Expr:
        mask = (pl.col('size_group') == size) & (pl.col('value_group') == value)
        return pl.col('ret').filter(mask).mean()

    return (
        frame.group_by('date')
        .agg(
            pl.col('ret').mean().alias('ret_market'),
            pl.col('amount').sum().alias('_amount_total'),
            mean_where('size_group', 'small').alias('ret_small'),
            mean_where('size_group', 'mid').alias('ret_mid'),
            mean_where('size_group', 'large').alias('ret_large'),
            mean_where('value_group', 'value').alias('ret_value'),
            mean_where('value_group', 'growth').alias('ret_growth'),
            mean_where('vol_group', 'high').alias('ret_highvol'),
            mean_where('vol_group', 'low').alias('ret_lowvol'),
            combo('large', 'value').alias('ret_large_value'),
            combo('large', 'growth').alias('ret_large_growth'),
            combo('small', 'value').alias('ret_small_value'),
            combo('small', 'growth').alias('ret_small_growth'),
            pl.col('amount').filter(pl.col('size_group') == 'small').sum().alias('_amount_small'),
            pl.col('turn').filter(pl.col('size_group') == 'small').mean().alias('_turn_small'),
            pl.col('turn').filter(pl.col('size_group') == 'large').mean().alias('_turn_large'),
            pl.col('pb').filter(pl.col('value_group') == 'value').median().alias('pb_med_value'),
            pl.col('pb').filter(pl.col('value_group') == 'growth').median().alias('pb_med_growth'),
            pl.col('net_profit_yoy').filter(pl.col('value_group') == 'value').median().alias('np_yoy_value'),
            pl.col('net_profit_yoy').filter(pl.col('value_group') == 'growth').median().alias('np_yoy_growth'),
            pl.col('vol60').filter(pl.col('vol_group') == 'high').median().alias('vol60_high'),
            pl.col('vol60').filter(pl.col('vol_group') == 'low').median().alias('vol60_low'),
        )
        .sort('date')
    )


_RETURN_KEYS = (
    'small', 'mid', 'large', 'value', 'growth', 'highvol', 'lowvol',
    'large_value', 'large_growth', 'small_value', 'small_growth',
)


def _add_indicators(daily: pl.DataFrame) -> pl.DataFrame:
    """净值 → 20/60 日收益 → 三组维度指标。"""
    frame = daily
    for key in _RETURN_KEYS:
        frame = frame.with_columns(
            (1 + pl.col(f'ret_{key}').fill_null(0.0)).cum_prod().alias(f'nav_{key}'),
        )
    for key in _RETURN_KEYS:
        nav = pl.col(f'nav_{key}')
        frame = frame.with_columns(
            (nav / nav.shift(20) - 1).alias(f'r20_{key}'),
            (nav / nav.shift(60) - 1).alias(f'r60_{key}'),
        )
    return (
        frame.with_columns(
            (pl.col('r20_small') - pl.col('r20_large')).alias('size_rs20'),
            (pl.col('r60_small') - pl.col('r60_large')).alias('size_rs60'),
            (pl.col('_amount_small') / pl.col('_amount_total')).alias('_amount_share'),
            (pl.col('_turn_small') / pl.col('_turn_large')).alias('_turn_ratio'),
            (pl.col('r20_value') - pl.col('r20_growth')).alias('vg_rs20'),
            (pl.col('r60_value') - pl.col('r60_growth')).alias('vg_rs60'),
            (pl.col('pb_med_growth') / pl.col('pb_med_value')).alias('vg_pb_gap'),
            (pl.col('np_yoy_value') - pl.col('np_yoy_growth')).alias('vg_earn_gap'),
            (pl.col('r20_highvol') - pl.col('r20_lowvol')).alias('vol_rs20'),
            (pl.col('r60_highvol') - pl.col('r60_lowvol')).alias('vol_rs60'),
            (pl.col('vol60_high') / pl.col('vol60_low')).alias('vol_disp'),
        )
        .with_columns(
            pl.col('_amount_share').rolling_mean(20, min_samples=20).alias('size_amount'),
            pl.col('_turn_ratio').rolling_mean(20, min_samples=20).alias('size_turn'),
        )
        .filter(pl.col('date') >= pl.lit(PANEL_START_DATE))
        .select([
            'date', 'ret_market',
            'ret_small', 'ret_mid', 'ret_large', 'ret_value', 'ret_growth',
            'ret_highvol', 'ret_lowvol',
            'ret_large_value', 'ret_large_growth', 'ret_small_value', 'ret_small_growth',
            'size_rs20', 'size_rs60', 'size_amount', 'size_turn',
            'vg_rs20', 'vg_rs60', 'vg_pb_gap', 'vg_earn_gap',
            'vol_rs20', 'vol_rs60', 'vol_disp',
        ])
    )


def build_style_daily(base: Path) -> pl.DataFrame:
    """装配风格日频面板（原始指标，不含打分）。"""
    quotes = _load_quotes(base)
    quotes = _attach_shares(quotes, _load_shares(base))
    valuation = _load_valuation(base)
    income = _load_np_yoy(base)

    valid = quotes.filter(
        pl.col('_traded') & pl.col('_not_limit') & pl.col('ret').is_not_null()
        & pl.col('mcap').is_not_null()
    )
    valid = valid.join(valuation, on=['ts_code', 'date'], how='left').sort(['ts_code', 'date'])
    valid = valid.join_asof(
        income,
        left_on='date',
        right_on='date',
        by='ts_code',
        strategy='backward',
        check_sortedness=False,
    )
    daily = _daily_panel(valid)
    return _add_indicators(daily)


def load_local_panel(root: Path | str | None = None) -> dict[str, pl.DataFrame]:
    """读取并装配判定所需的本地表；不新增外部采集。"""
    base = Path(root) if root is not None else _default_root()
    started = time.time()
    style_daily = build_style_daily(base)
    logger.debug(
        '风格日频面板装配完成：{} 行 / {} 个交易日 / 耗时 {:.1f}s',
        style_daily.height,
        style_daily['date'].n_unique(),
        time.time() - started,
    )
    return {'style_daily': style_daily}


def run_cli(
    asof: str | None = None,
    panel_root: str | None = None,
    top: int | None = None,
) -> None:
    target = datetime.strptime(asof, '%Y-%m-%d').date() if asof else None
    state = evaluate_style_rotation(load_local_panel(panel_root), asof=target)
    if top and top > 0:
        logger.info('风格轮动（{}）：{}，分化强度 {:.2f}', state.asof, state.direction, state.total_score)
        for dimension in state.dimensions:
            from src.quant.analysis.style_rotation_state import format_dimension_line

            logger.info(format_dimension_line(dimension))
        for portfolio in state.portfolios[:top]:
            from src.quant.analysis.style_rotation_state import format_portfolio_line

            logger.info(format_portfolio_line(portfolio))
        for note in state.notes:
            logger.info('说明：{}', note)
        return
    for line in format_style_detail(state).splitlines():
        logger.info(line)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='风格轮动状态判定（只读本地数据）')
    parser.add_argument('--asof', help='判定截止日，格式 YYYY-MM-DD')
    parser.add_argument('--panel-root', help='本地列存根目录，默认按配置解析')
    parser.add_argument('--top', type=int, help='只输出前 N 条代表组合')
    args = parser.parse_args(argv)
    run_cli(asof=args.asof, panel_root=args.panel_root, top=args.top)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
