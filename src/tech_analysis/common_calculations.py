#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
通用计算模块，提供指标计算中常用的通用函数
"""

import polars as pl
from .utils import to_float32


def calculate_price_change(lazy_df: pl.LazyFrame) -> pl.LazyFrame:
    """
    计算价格变化
    
    Args:
        lazy_df: Polars LazyFrame
        
    Returns:
        pl.LazyFrame: 包含价格变化的LazyFrame
    """
    return lazy_df.with_columns(
        pl.col('close').diff().alias('price_change')
    )


def calculate_moving_average(lazy_df: pl.LazyFrame, column: str, windows: list, prefix: str = '') -> pl.LazyFrame:
    """
    计算移动平均线
    
    Args:
        lazy_df: Polars LazyFrame
        column: 要计算移动平均的列名
        windows: 移动平均窗口列表
        prefix: 指标前缀
        
    Returns:
        pl.LazyFrame: 包含移动平均线的LazyFrame
    """
    for window in windows:
        lazy_df = lazy_df.with_columns(
            to_float32(pl.col(column).rolling_mean(window_size=window, min_periods=1)).alias(f'{prefix}ma{window}')
        )
    return lazy_df


def calculate_ewm(lazy_df: pl.LazyFrame, column: str, spans: list, prefix: str = '') -> pl.LazyFrame:
    """
    计算指数加权移动平均
    
    Args:
        lazy_df: Polars LazyFrame
        column: 要计算EWM的列名
        spans: EWM周期列表
        prefix: 指标前缀
        
    Returns:
        pl.LazyFrame: 包含EWM的LazyFrame
    """
    for span in spans:
        lazy_df = lazy_df.with_columns(
            to_float32(pl.col(column).ewm_mean(span=span)).alias(f'{prefix}ewm{span}')
        )
    return lazy_df


def calculate_rolling_max(lazy_df: pl.LazyFrame, column: str, windows: list, prefix: str = '') -> pl.LazyFrame:
    """
    计算滚动最大值
    
    Args:
        lazy_df: Polars LazyFrame
        column: 要计算滚动最大值的列名
        windows: 窗口列表
        prefix: 指标前缀
        
    Returns:
        pl.LazyFrame: 包含滚动最大值的LazyFrame
    """
    for window in windows:
        lazy_df = lazy_df.with_columns(
            pl.col(column).rolling_max(window_size=window, min_periods=1).alias(f'{prefix}max_{window}')
        )
    return lazy_df


def calculate_rolling_min(lazy_df: pl.LazyFrame, column: str, windows: list, prefix: str = '') -> pl.LazyFrame:
    """
    计算滚动最小值
    
    Args:
        lazy_df: Polars LazyFrame
        column: 要计算滚动最小值的列名
        windows: 窗口列表
        prefix: 指标前缀
        
    Returns:
        pl.LazyFrame: 包含滚动最小值的LazyFrame
    """
    for window in windows:
        lazy_df = lazy_df.with_columns(
            pl.col(column).rolling_min(window_size=window, min_periods=1).alias(f'{prefix}min_{window}')
        )
    return lazy_df


def calculate_rolling_std(lazy_df: pl.LazyFrame, column: str, windows: list, prefix: str = '') -> pl.LazyFrame:
    """
    计算滚动标准差
    
    Args:
        lazy_df: Polars LazyFrame
        column: 要计算滚动标准差的列名
        windows: 窗口列表
        prefix: 指标前缀
        
    Returns:
        pl.LazyFrame: 包含滚动标准差的LazyFrame
    """
    for window in windows:
        lazy_df = lazy_df.with_columns(
            to_float32(pl.col(column).rolling_std(window_size=window, min_periods=1)).alias(f'{prefix}std_{window}')
        )
    return lazy_df


def calculate_rolling_sum(lazy_df: pl.LazyFrame, column: str, windows: list, prefix: str = '') -> pl.LazyFrame:
    """
    计算滚动总和
    
    Args:
        lazy_df: Polars LazyFrame
        column: 要计算滚动总和的列名
        windows: 窗口列表
        prefix: 指标前缀
        
    Returns:
        pl.LazyFrame: 包含滚动总和的LazyFrame
    """
    for window in windows:
        lazy_df = lazy_df.with_columns(
            to_float32(pl.col(column).rolling_sum(window_size=window, min_periods=1)).alias(f'{prefix}sum_{window}')
        )
    return lazy_df


def calculate_typical_price(lazy_df: pl.LazyFrame) -> pl.LazyFrame:
    """
    计算典型价格 (H + L + C) / 3
    
    Args:
        lazy_df: Polars LazyFrame
        
    Returns:
        pl.LazyFrame: 包含典型价格的LazyFrame
    """
    return lazy_df.with_columns(
        to_float32((pl.col('high') + pl.col('low') + pl.col('close')) / 3).alias('typical_price')
    )


def calculate_gain_loss(lazy_df: pl.LazyFrame) -> pl.LazyFrame:
    """
    计算上涨和下跌变化
    
    Args:
        lazy_df: Polars LazyFrame
        
    Returns:
        pl.LazyFrame: 包含上涨和下跌变化的LazyFrame
    """
    return lazy_df.with_columns(
        pl.when(pl.col('price_change') > 0).then(pl.col('price_change')).otherwise(0).alias('gain'),
        pl.when(pl.col('price_change') < 0).then(-pl.col('price_change')).otherwise(0).alias('loss')
    )


def add_default_columns(lazy_df: pl.LazyFrame, indicator_type: str, windows: list) -> pl.LazyFrame:
    """
    添加默认列名
    
    Args:
        lazy_df: Polars LazyFrame
        indicator_type: 指标类型
        windows: 窗口列表
        
    Returns:
        pl.LazyFrame: 包含默认列名的LazyFrame
    """
    if len(windows) >= 1:
        window = windows[0]
        if indicator_type in ['ma', 'vol_ma', 'rsi', 'kdj', 'wr', 'dmi', 'cci', 'roc', 'mtm', 'vr', 'psy', 'trix', 'brar', 'emv', 'mcst', 'cyc', 'cr']:
            lazy_df = lazy_df.with_columns(
                pl.col(f'{indicator_type}{window}').alias(indicator_type)
            )
        elif indicator_type == 'boll':
            lazy_df = lazy_df.with_columns(
                pl.col(f'mb{window}').alias('mb'),
                pl.col(f'up{window}').alias('up'),
                pl.col(f'dn{window}').alias('dn')
            )
        elif indicator_type == 'kdj':
            lazy_df = lazy_df.with_columns(
                pl.col(f'k{window}').alias('k'),
                pl.col(f'd{window}').alias('d'),
                pl.col(f'j{window}').alias('j')
            )
        elif indicator_type == 'dmi':
            lazy_df = lazy_df.with_columns(
                pl.col(f'pdi_{window}').alias('pdi'),
                pl.col(f'ndi_{window}').alias('ndi'),
                pl.col(f'adx_{window}').alias('adx'),
                pl.col(f'adxr_{window}').alias('adxr')
            )
    return lazy_df
