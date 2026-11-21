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
    # 统一走共享公式层（通达信 SMA 口径，第 2 根 K 线起即有值）
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
        # 计算典型价格的N日移动平均值（MA_TP，通达信 MA 不足 N 根不输出）
        ma_tp = tp.rolling_mean(window_size=window, min_samples=window)

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


def calculate_bias(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """
    计算BIAS指标（乖离率，通达信口径）

    通达信原文：``BIAS := (CLOSE - MA(CLOSE,N)) / MA(CLOSE,N) * 100``
    常用周期 6 / 12 / 24；MA 不足 N 根不输出（与阶段一 MA 预热口径一致）。

    Args:
        lazy_df: Polars LazyFrame
        windows: BIAS 计算周期列表，默认 [6, 12, 24]

    Returns:
        pl.LazyFrame: 含 ``bias{N}`` 与默认列 ``bias``（首个窗口）
    """
    for window in windows:
        ma = pl.col('close').rolling_mean(window_size=window, min_samples=window)
        bias = to_float32((pl.col('close') - ma) / ma * 100).alias(f'bias{window}')
        lazy_df = lazy_df.with_columns(bias)

    return add_default_columns(lazy_df, 'bias', windows)


def calculate_skdj(lazy_df: pl.LazyFrame, n: int = 9, m: int = 3,
                   k_period: int = 3, d_period: int = 3) -> pl.LazyFrame:
    """
    计算SKDJ指标（慢速KD）

    公式来源：**公开标准公式**（``H:\\zxzq\\TCalc.dll`` 中无非 SKDJ 原文，未取证）：
        RSV = (CLOSE - LLV(LOW,N)) / (HHV(HIGH,N) - LLV(LOW,N)) * 100
        K = SMA(EMA(RSV, M), K_PERIOD, 1)
        D = SMA(K, D_PERIOD, 1)
        J = 3K - 2D

    与项目 KDJ 的区别：SKDJ 先对 RSV 做 M 日 EMA 平滑，再用 SMA(X,N,1) 二次平滑，
    因此更钝、假信号更少。

    Args:
        lazy_df: Polars LazyFrame
        n: RSV 窗口，默认 9
        m: RSV 的 EMA 平滑周期，默认 3
        k_period: K 的 SMA 周期，默认 3
        d_period: D 的 SMA 周期，默认 3

    Returns:
        pl.LazyFrame: 含 ``skdj_k`` / ``skdj_d`` / ``skdj_j``
    """
    from src.tech_analysis.indicators.formulas import sma_smooth

    low_n = pl.col('low').rolling_min(window_size=n, min_samples=n)
    high_n = pl.col('high').rolling_max(window_size=n, min_samples=n)
    rsv = (pl.col('close') - low_n) / (high_n - low_n) * 100.0

    # RSV 先做 EMA 平滑（通达信 EMA 口径 alpha=2/(M+1)）
    rsv_smooth = rsv.ewm_mean(alpha=2.0 / (m + 1), adjust=False, min_samples=1)
    k_line = sma_smooth(rsv_smooth, k_period, 1)
    d_line = sma_smooth(k_line, d_period, 1)
    j_line = 3.0 * k_line - 2.0 * d_line

    return lazy_df.with_columns([
        to_float32(k_line).alias('skdj_k'),
        to_float32(d_line).alias('skdj_d'),
        to_float32(j_line).alias('skdj_j'),
    ])


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

    # 计算BIAS指标（乖离率）
    if 'bias' in indicator_types:
        windows = params.get('bias_windows', [6, 12, 24])
        lazy_df = calculate_bias(lazy_df, windows)

    # 计算SKDJ指标（慢速KD）
    if 'skdj' in indicator_types:
        lazy_df = calculate_skdj(
            lazy_df,
            n=params.get('skdj_n', 9),
            m=params.get('skdj_m', 3),
            k_period=params.get('skdj_k', 3),
            d_period=params.get('skdj_d', 3),
        )

    return lazy_df
