#!/usr/bin/env python3

"""
震荡类指标计算模块
包含：RSI, KDJ, CCI, ROC, MTM等
"""

import polars as pl

from ..common_calculations import add_default_columns
from ..utils import calculate_mad, to_float32
from .formulas import kdj_exprs, rsi_expr


def calculate_rsi(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """
    计算RSI指标

    Args:
        lazy_df: Polars LazyFrame
        windows: RSI计算窗口列表

    Returns:
        pl.LazyFrame: 包含RSI指标的LazyFrame
    """
    # 统一走共享公式层（Wilder 标准口径），窗口之外自动为 null
    for window in windows:
        rsi = to_float32(rsi_expr('close', window)).alias(f'rsi{window}')
        lazy_df = lazy_df.with_columns(rsi)

    # 添加默认列名
    lazy_df = add_default_columns(lazy_df, 'rsi', windows)
    return lazy_df


def calculate_kdj(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """
    计算KDJ指标

    Args:
        lazy_df: Polars LazyFrame
        windows: KDJ计算窗口列表

    Returns:
        pl.LazyFrame: 包含KDJ指标的LazyFrame
    """
    for window in windows:
        # 计算RSV、K、D、J（通达信口径，公式统一见 formulas.kdj_exprs）
        kdj = kdj_exprs('high', 'low', 'close', window=window)
        lazy_df = lazy_df.with_columns(
            to_float32(kdj['rsv']).alias(f'rsv_{window}')
        )

        k_expr = to_float32(kdj['k']).alias(f'k{window}')
        d_expr = to_float32(kdj['d']).alias(f'd{window}')
        j_expr = to_float32(kdj['j']).alias(f'j{window}')

        lazy_df = lazy_df.with_columns([k_expr, d_expr, j_expr])

        # 添加默认列名
        if window == 14:
            lazy_df = lazy_df.with_columns(
                pl.col(f'k{window}').alias('k'),
                pl.col(f'd{window}').alias('d'),
                pl.col(f'j{window}').alias('j')
            )

    return lazy_df


def calculate_cci(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """
    计算CCI指标

    Args:
        lazy_df: Polars LazyFrame
        windows: CCI计算窗口列表

    Returns:
        pl.LazyFrame: 包含CCI指标的LazyFrame
    """
    # 计算典型价格（TP = (H + L + C) / 3）
    tp = (pl.col('high') + pl.col('low') + pl.col('close')) / 3

    for window in windows:
        # 计算典型价格的N日移动平均值（MA_TP）
        ma_tp = tp.rolling_mean(window_size=window, min_samples=1)

        # 计算平均绝对偏差（MAD）
        mad = calculate_mad(tp, window)

        # 计算CCI = (TP - MA_TP) / (0.015 * MAD)
        cci = to_float32((tp - ma_tp) / (0.015 * mad)).alias(f'cci{window}')

        # 添加CCI指标列
        lazy_df = lazy_df.with_columns(cci)

    # 添加默认列名
    lazy_df = add_default_columns(lazy_df, 'cci', windows)
    return lazy_df


def calculate_roc(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """
    计算ROC指标

    Args:
        lazy_df: Polars LazyFrame
        windows: ROC计算窗口列表

    Returns:
        pl.LazyFrame: 包含ROC指标的LazyFrame
    """
    for window in windows:
        # 计算ROC = ((当前收盘价 - n天前收盘价) / n天前收盘价) * 100
        lazy_df = lazy_df.with_columns(
            to_float32((pl.col('close') - pl.col('close').shift(window)) /
             pl.col('close').shift(window) * 100).alias(f'roc{window}')
        )

    # 添加默认列名
    lazy_df = add_default_columns(lazy_df, 'roc', windows)
    return lazy_df


def calculate_mtm(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """
    计算MTM指标

    Args:
        lazy_df: Polars LazyFrame
        windows: MTM计算窗口列表

    Returns:
        pl.LazyFrame: 包含MTM指标的LazyFrame
    """
    for window in windows:
        # 计算MTM = 当前收盘价 - n天前收盘价
        lazy_df = lazy_df.with_columns(
            to_float32(pl.col('close') - pl.col('close').shift(window)).alias(f'mtm{window}')
        )

    # 添加默认列名
    lazy_df = add_default_columns(lazy_df, 'mtm', windows)
    return lazy_df


def calculate_oscillator_indicators(lazy_df: pl.LazyFrame, indicator_types: list, **params) -> pl.LazyFrame:
    """
    计算所有震荡类指标

    Args:
        lazy_df: Polars LazyFrame
        indicator_types: 需要计算的震荡类指标列表
        **params: 指标计算参数

    Returns:
        pl.LazyFrame: 包含所有计算震荡类指标的LazyFrame
    """
    # 计算RSI指标
    if 'rsi' in indicator_types:
        windows = params.get('rsi_windows', [14])
        lazy_df = calculate_rsi(lazy_df, windows)

    # 计算KDJ指标
    if 'kdj' in indicator_types:
        windows = params.get('kdj_windows', [14])
        lazy_df = calculate_kdj(lazy_df, windows)

    # 计算CCI指标
    if 'cci' in indicator_types:
        windows = params.get('cci_windows', [14])
        lazy_df = calculate_cci(lazy_df, windows)

    # 计算ROC指标
    if 'roc' in indicator_types:
        windows = params.get('roc_windows', [12])
        lazy_df = calculate_roc(lazy_df, windows)

    # 计算MTM指标
    if 'mtm' in indicator_types:
        windows = params.get('mtm_windows', [12])
        lazy_df = calculate_mtm(lazy_df, windows)

    return lazy_df
