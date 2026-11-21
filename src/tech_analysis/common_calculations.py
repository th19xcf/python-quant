#!/usr/bin/env python3

"""
通用计算模块，提供指标计算中常用的通用函数
"""

import polars as pl

from .utils import to_float32


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
        # 通达信 MA 函数文档：数据个数少于 N 时不输出（min_samples=window → 前 N-1 根为空）
        lazy_df = lazy_df.with_columns(
            to_float32(pl.col(column).rolling_mean(window_size=window, min_samples=window)).alias(f'{prefix}ma{window}')
        )
    return lazy_df


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
        if indicator_type in ['ma', 'vol_ma', 'rsi', 'kdj', 'wr', 'dmi', 'cci', 'roc', 'mtm', 'vr', 'psy', 'trix', 'brar', 'emv', 'mcst', 'cyc', 'cr', 'bias', 'mfi', 'cmf']:
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
