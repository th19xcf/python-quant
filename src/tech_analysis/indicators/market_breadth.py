#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
大势型指标计算模块
包含：ABI, ADL, ADR, OBOS等
用于分析整体市场走势
"""

import polars as pl
import numpy as np
from datetime import datetime, timedelta
from loguru import logger
from ..utils import to_float32


def get_market_breadth_from_db(start_date: str = None, end_date: str = None):
    """
    从数据库获取市场涨跌家数数据

    Args:
        start_date: 开始日期，格式YYYY-MM-DD，默认30天前
        end_date: 结束日期，格式YYYY-MM-DD，默认今天

    Returns:
        polars DataFrame: 包含 trade_date, up_count, down_count, flat_count, total_count
    """
    try:
        from src.database.db_manager import DatabaseManager

        db_manager = DatabaseManager()
        session = db_manager.get_session()

        from src.database.models.stock import MarketBreadth
        from sqlalchemy import select

        query = select(MarketBreadth)
        if start_date:
            query = query.where(MarketBreadth.trade_date >= start_date)
        if end_date:
            query = query.where(MarketBreadth.trade_date <= end_date)
        query = query.order_by(MarketBreadth.trade_date)

        result = session.execute(query)
        rows = result.fetchall()

        if not rows:
            logger.warning("数据库中没有市场涨跌家数数据")
            return None

        data = []
        for row in rows:
            data.append({
                'trade_date': row[1],
                'up_count': row[2],
                'down_count': row[3],
                'flat_count': row[4],
                'total_count': row[5]
            })

        session.close()
        return pl.DataFrame(data)

    except Exception as e:
        logger.warning(f"从数据库获取市场涨跌家数数据失败: {e}")
        return None


def calculate_abi(lazy_df: pl.LazyFrame, period: int = 10) -> pl.LazyFrame:
    """
    计算绝对广量指标 (Absolute Breadth Index, ABI)
    用于衡量市场涨跌的广度，不考虑涨跌幅度

    公式: ABI = |上涨家数 - 下跌家数| / 总成交股票数 * 100

    Args:
        lazy_df: Polars LazyFrame，需包含 up_count, down_count 字段
        period: 计算周期，默认10

    Returns:
        pl.LazyFrame: 包含ABI指标的LazyFrame
    """
    lazy_df = _ensure_market_breadth_columns(lazy_df)

    total = pl.col('up_count') + pl.col('down_count')
    diff = (pl.col('up_count') - pl.col('down_count')).abs()

    return lazy_df.with_columns(
        to_float32((diff / total * 100).fill_nan(0).replace(float('inf'), 0).alias(f'abi{period}'))
    )


def calculate_adl(lazy_df: pl.LazyFrame) -> pl.LazyFrame:
    """
    计算腾落指标 (Advance Decline Line, ADL)
    累计上涨家数与下跌家数的差值

    公式: ADL = 累计(上涨家数 - 下跌家数)

    Args:
        lazy_df: Polars LazyFrame，需包含 up_count, down_count 字段

    Returns:
        pl.LazyFrame: 包含ADL指标的LazyFrame
    """
    lazy_df = _ensure_market_breadth_columns(lazy_df)

    adl_value = (pl.col('up_count') - pl.col('down_count')).cum_sum()

    return lazy_df.with_columns(
        to_float32(adl_value.alias('adl'))
    )


def calculate_adr(lazy_df: pl.LazyFrame, period: int = 10) -> pl.LazyFrame:
    """
    计算涨跌比率 (Advance Decline Ratio, ADR)
    上涨家数与下跌家数的比率

    公式: ADR = 上涨N日平均家数 / 下跌N日平均家数 * 100

    Args:
        lazy_df: Polars LazyFrame，需包含 up_count, down_count 字段
        period: 计算周期，默认10

    Returns:
        pl.LazyFrame: 包含ADR指标的LazyFrame
    """
    lazy_df = _ensure_market_breadth_columns(lazy_df)

    avg_up = pl.col('up_count').rolling_mean(window_size=period, min_periods=period)
    avg_down = pl.col('down_count').rolling_mean(window_size=period, min_periods=period)

    adr_value = (avg_up / avg_down * 100).fill_nan(0).replace(float('inf'), 0)

    return lazy_df.with_columns(
        to_float32(adr_value.alias(f'adr{period}'))
    )


def calculate_obos(lazy_df: pl.LazyFrame, period: int = 10) -> pl.LazyFrame:
    """
    计算超买超卖指标 (Overbought/Oversold, OBOS)
    类似于ADR但使用累计差值

    公式: OBOS = 上涨N日平均 - 下跌N日平均

    Args:
        lazy_df: Polars LazyFrame，需包含 up_count, down_count 字段
        period: 计算周期，默认10

    Returns:
        pl.LazyFrame: 包含OBOS指标的LazyFrame
    """
    lazy_df = _ensure_market_breadth_columns(lazy_df)

    avg_up = pl.col('up_count').rolling_mean(window_size=period, min_periods=period)
    avg_down = pl.col('down_count').rolling_mean(window_size=period, min_periods=period)

    obos_value = avg_up - avg_down

    return lazy_df.with_columns(
        to_float32(obos_value.alias(f'obos{period}'))
    )


def _ensure_market_breadth_columns(lazy_df: pl.LazyFrame) -> pl.LazyFrame:
    """
    确保DataFrame包含市场涨跌家数列，如果没有则尝试从数据库获取

    Args:
        lazy_df: Polars LazyFrame

    Returns:
        pl.LazyFrame: 包含市场涨跌家数数据的LazyFrame
    """
    if 'up_count' in lazy_df.columns and 'down_count' in lazy_df.columns:
        return lazy_df

    try:
        start_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
        end_date = datetime.now().strftime('%Y-%m-%d')

        market_data = get_market_breadth_from_db(start_date, end_date)

        if market_data is not None and not market_data.is_empty():
            market_data = market_data.rename({'trade_date': 'date'})

            if lazy_df is not None and 'date' in lazy_df.columns:
                lazy_df = lazy_df.join(
                    market_data.lazy(),
                    on='date',
                    how='left'
                )
            else:
                lazy_df = market_data.lazy()

            logger.info("成功加载市场涨跌家数数据")
        else:
            logger.debug("使用默认市场涨跌家数数据（全0）")
            lazy_df = lazy_df.with_columns([
                pl.lit(0).cast(pl.Int32).alias('up_count'),
                pl.lit(0).cast(pl.Int32).alias('down_count'),
                pl.lit(0).cast(pl.Int32).alias('flat_count'),
                pl.lit(0).cast(pl.Int32).alias('total_count')
            ])

    except Exception as e:
        logger.warning(f"获取市场涨跌家数数据失败，使用默认值: {e}")
        lazy_df = lazy_df.with_columns([
            pl.lit(0).cast(pl.Int32).alias('up_count'),
            pl.lit(0).cast(pl.Int32).alias('down_count'),
            pl.lit(0).cast(pl.Int32).alias('flat_count'),
            pl.lit(0).cast(pl.Int32).alias('total_count')
        ])

    return lazy_df


def calculate_market_breadth_indicators(lazy_df: pl.LazyFrame, indicator_types: list, **params) -> pl.LazyFrame:
    """
    计算大势型指标

    Args:
        lazy_df: Polars LazyFrame
        indicator_types: 指标类型列表，如 ['abi', 'adl', 'adr', 'obos']
        params: 参数字典

    Returns:
        pl.LazyFrame: 包含指定指标的LazyFrame
    """
    period = params.get('period', 10)

    if 'abi' in indicator_types:
        lazy_df = calculate_abi(lazy_df, period)

    if 'adl' in indicator_types:
        lazy_df = calculate_adl(lazy_df)

    if 'adr' in indicator_types:
        lazy_df = calculate_adr(lazy_df, period)

    if 'obos' in indicator_types:
        lazy_df = calculate_obos(lazy_df, period)

    return lazy_df
