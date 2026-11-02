#!/usr/bin/env python3

"""
大势型指标计算模块
包含：ABI, ADL, ADR, OBOS等
用于分析整体市场走势
"""

from datetime import datetime, timedelta
from typing import Any

import polars as pl
from loguru import logger

from ..utils import to_float32


class MarketBreadthUnavailableError(RuntimeError):
    """缺少市场涨跌家数数据（未注入 db_manager 且未直接传入 market_breadth）。"""


def get_market_breadth_from_db(
    start_date: str | None = None,
    end_date: str | None = None,
    *,
    db_manager: Any = None,
) -> pl.DataFrame | None:
    """
    从数据库获取市场涨跌家数数据

    计算层不再自行构造 DatabaseManager：调用方必须注入 db_manager
    （或直接把 market_breadth 数据传给指标函数）。缺依赖时记录 error 并返回 None。

    Args:
        start_date: 开始日期，格式YYYY-MM-DD，默认30天前
        end_date: 结束日期，格式YYYY-MM-DD，默认今天
        db_manager: 由调用方注入的数据库管理器

    Returns:
        polars DataFrame: 包含 trade_date, up_count, down_count, flat_count, total_count
    """
    if db_manager is None:
        logger.error("缺少 db_manager 依赖，无法读取市场涨跌家数数据")
        return None

    session = None
    try:
        from sqlalchemy import select

        from src.database.models.stock import MarketBreadth

        session = db_manager.get_session()
        if session is None:
            logger.error("db_manager 未返回数据库会话，无法读取市场涨跌家数数据")
            return None

        query: Any = select(
            MarketBreadth.trade_date,
            MarketBreadth.up_count,
            MarketBreadth.down_count,
            MarketBreadth.flat_count,
            MarketBreadth.total_count,
        )
        if start_date:
            query = query.where(MarketBreadth.trade_date >= start_date)
        if end_date:
            query = query.where(MarketBreadth.trade_date <= end_date)
        query = query.order_by(MarketBreadth.trade_date)

        rows = session.execute(query).all()

        if not rows:
            logger.warning("数据库中没有市场涨跌家数数据")
            return None

        return pl.DataFrame(
            rows,
            schema=['trade_date', 'up_count', 'down_count', 'flat_count', 'total_count'],
            orient='row',
        )

    # 兜底理由：数据库取数失败返回 None 由调用方显式失败，不再用全 0 降级
    except Exception as e:  # noqa: BLE001
        logger.error(f"从数据库获取市场涨跌家数数据失败: {e}")
        return None
    finally:
        if session is not None:
            session.close()



def calculate_abi(
    lazy_df: pl.LazyFrame,
    period: int = 10,
    *,
    db_manager: Any = None,
    market_breadth: pl.DataFrame | None = None,
) -> pl.LazyFrame:
    """
    计算绝对广量指标 (Absolute Breadth Index, ABI)
    用于衡量市场涨跌的广度，不考虑涨跌幅度

    公式: ABI = |上涨家数 - 下跌家数| / 总成交股票数 * 100

    Args:
        lazy_df: Polars LazyFrame，需包含 up_count, down_count 字段
        period: 计算周期，默认10
        db_manager: 由调用方注入的数据库管理器（缺 up_count/down_count 时用于取数）
        market_breadth: 直接传入的市场涨跌家数 DataFrame（优先于数据库取数）

    Returns:
        pl.LazyFrame: 包含ABI指标的LazyFrame
    """
    lazy_df = _ensure_market_breadth_columns(
        lazy_df, db_manager=db_manager, market_breadth=market_breadth)

    total = pl.col('up_count') + pl.col('down_count')
    diff = (pl.col('up_count') - pl.col('down_count')).abs()

    return lazy_df.with_columns(
        to_float32((diff / total * 100).fill_nan(0).replace(float('inf'), 0).alias(f'abi{period}'))
    )


def calculate_adl(
    lazy_df: pl.LazyFrame,
    *,
    db_manager: Any = None,
    market_breadth: pl.DataFrame | None = None,
) -> pl.LazyFrame:
    """
    计算腾落指标 (Advance Decline Line, ADL)
    累计上涨家数与下跌家数的差值

    公式: ADL = 累计(上涨家数 - 下跌家数)

    Args:
        lazy_df: Polars LazyFrame，需包含 up_count, down_count 字段
        db_manager: 由调用方注入的数据库管理器（缺 up_count/down_count 时用于取数）
        market_breadth: 直接传入的市场涨跌家数 DataFrame（优先于数据库取数）

    Returns:
        pl.LazyFrame: 包含ADL指标的LazyFrame
    """
    lazy_df = _ensure_market_breadth_columns(
        lazy_df, db_manager=db_manager, market_breadth=market_breadth)

    adl_value = (pl.col('up_count') - pl.col('down_count')).cum_sum()

    return lazy_df.with_columns(
        to_float32(adl_value.alias('adl'))
    )


def calculate_adr(
    lazy_df: pl.LazyFrame,
    period: int = 10,
    *,
    db_manager: Any = None,
    market_breadth: pl.DataFrame | None = None,
) -> pl.LazyFrame:
    """
    计算涨跌比率 (Advance Decline Ratio, ADR)
    上涨家数与下跌家数的比率

    公式: ADR = 上涨N日平均家数 / 下跌N日平均家数 * 100

    Args:
        lazy_df: Polars LazyFrame，需包含 up_count, down_count 字段
        period: 计算周期，默认10
        db_manager: 由调用方注入的数据库管理器（缺 up_count/down_count 时用于取数）
        market_breadth: 直接传入的市场涨跌家数 DataFrame（优先于数据库取数）

    Returns:
        pl.LazyFrame: 包含ADR指标的LazyFrame
    """
    lazy_df = _ensure_market_breadth_columns(
        lazy_df, db_manager=db_manager, market_breadth=market_breadth)

    avg_up = pl.col('up_count').rolling_mean(window_size=period, min_samples=period)
    avg_down = pl.col('down_count').rolling_mean(window_size=period, min_samples=period)

    adr_value = (avg_up / avg_down * 100).fill_nan(0).replace(float('inf'), 0)

    return lazy_df.with_columns(
        to_float32(adr_value.alias(f'adr{period}'))
    )


def calculate_obos(
    lazy_df: pl.LazyFrame,
    period: int = 10,
    *,
    db_manager: Any = None,
    market_breadth: pl.DataFrame | None = None,
) -> pl.LazyFrame:
    """
    计算超买超卖指标 (Overbought/Oversold, OBOS)
    类似于ADR但使用累计差值

    公式: OBOS = 上涨N日平均 - 下跌N日平均

    Args:
        lazy_df: Polars LazyFrame，需包含 up_count, down_count 字段
        period: 计算周期，默认10
        db_manager: 由调用方注入的数据库管理器（缺 up_count/down_count 时用于取数）
        market_breadth: 直接传入的市场涨跌家数 DataFrame（优先于数据库取数）

    Returns:
        pl.LazyFrame: 包含OBOS指标的LazyFrame
    """
    lazy_df = _ensure_market_breadth_columns(
        lazy_df, db_manager=db_manager, market_breadth=market_breadth)

    avg_up = pl.col('up_count').rolling_mean(window_size=period, min_samples=period)
    avg_down = pl.col('down_count').rolling_mean(window_size=period, min_samples=period)

    obos_value = avg_up - avg_down

    return lazy_df.with_columns(
        to_float32(obos_value.alias(f'obos{period}'))
    )


def _ensure_market_breadth_columns(
    lazy_df: pl.LazyFrame,
    *,
    db_manager: Any = None,
    market_breadth: pl.DataFrame | None = None,
) -> pl.LazyFrame:
    """
    确保DataFrame包含市场涨跌家数列

    Args:
        lazy_df: Polars LazyFrame
        db_manager: 由调用方注入的数据库管理器
        market_breadth: 直接传入的市场涨跌家数 DataFrame

    Returns:
        pl.LazyFrame: 包含市场涨跌家数数据的LazyFrame

    Raises:
        MarketBreadthUnavailableError: 既无涨跌家数列，也没有可用的数据来源
    """
    if 'up_count' in lazy_df.columns and 'down_count' in lazy_df.columns:
        return lazy_df

    market_data = market_breadth
    if market_data is None:
        start_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
        end_date = datetime.now().strftime('%Y-%m-%d')
        market_data = get_market_breadth_from_db(
            start_date, end_date, db_manager=db_manager)

    if market_data is None or market_data.is_empty():
        # 不用全 0 占位：伪造的 up_count/down_count 会让 ABI/ADL/ADR/OBOS 失真
        logger.error(
            "缺少市场涨跌家数数据，拒绝用全 0 补齐；请注入 db_manager 或直接传入 market_breadth")
        raise MarketBreadthUnavailableError(
            "缺少市场涨跌家数数据：请注入 db_manager 或直接传入 market_breadth")

    market_data = market_data.rename({'trade_date': 'date'})

    if 'date' in lazy_df.columns:
        lazy_df = lazy_df.join(
            market_data.lazy(),
            on='date',
            how='left'
        )
    else:
        lazy_df = market_data.lazy()

    logger.info("成功加载市场涨跌家数数据")
    return lazy_df


def calculate_market_breadth_indicators(
    lazy_df: pl.LazyFrame,
    indicator_types: list,
    *,
    db_manager: Any = None,
    market_breadth: pl.DataFrame | None = None,
    **params,
) -> pl.LazyFrame | None:
    """
    计算大势型指标

    Args:
        lazy_df: Polars LazyFrame
        indicator_types: 指标类型列表，如 ['abi', 'adl', 'adr', 'obos']
        db_manager: 由调用方注入的数据库管理器
        market_breadth: 直接传入的市场涨跌家数 DataFrame
        params: 参数字典

    Returns:
        pl.LazyFrame: 包含指定指标的LazyFrame；
        请求了大势型指标但缺涨跌家数数据时返回 None（不伪造全 0 数据列）
    """
    period = params.get('period', 10)

    breadth_types = [name for name in ('abi', 'adl', 'adr', 'obos')
                     if name in indicator_types]
    if not breadth_types:
        return lazy_df

    try:
        lazy_df = _ensure_market_breadth_columns(
            lazy_df, db_manager=db_manager, market_breadth=market_breadth)
    except MarketBreadthUnavailableError:
        logger.error(
            f"市场涨跌家数数据不可用，跳过大势型指标 {breadth_types}（不使用全 0 占位）")
        return None

    if 'abi' in indicator_types:
        lazy_df = calculate_abi(lazy_df, period)

    if 'adl' in indicator_types:
        lazy_df = calculate_adl(lazy_df)

    if 'adr' in indicator_types:
        lazy_df = calculate_adr(lazy_df, period)

    if 'obos' in indicator_types:
        lazy_df = calculate_obos(lazy_df, period)

    return lazy_df
