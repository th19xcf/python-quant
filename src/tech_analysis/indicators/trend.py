#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
趋势类指标计算模块
包含：MA, MACD, DMI, TRIX等
"""

import polars as pl
from ..utils import to_float32


def calculate_ma(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """
    计算移动平均线指标
    
    Args:
        lazy_df: Polars LazyFrame
        windows: 移动平均窗口列表
        
    Returns:
        pl.LazyFrame: 包含移动平均线的LazyFrame
    """
    return lazy_df.with_columns(
        *[to_float32(pl.col('close').rolling_mean(window_size=window, min_periods=1)).alias(f'ma{window}')
          for window in windows]
    )


def calculate_macd(lazy_df: pl.LazyFrame, fast_period: int, slow_period: int, signal_period: int) -> pl.LazyFrame:
    """
    计算MACD指标
    
    Args:
        lazy_df: Polars LazyFrame
        fast_period: 快速EMA周期
        slow_period: 慢速EMA周期
        signal_period: 信号线EMA周期
        
    Returns:
        pl.LazyFrame: 包含MACD指标的LazyFrame
    """
    # 直接计算EMA值，不创建中间列
    ema12 = pl.col('close').ewm_mean(span=fast_period)
    ema26 = pl.col('close').ewm_mean(span=slow_period)
    
    # 计算MACD线
    macd_line = to_float32(ema12 - ema26).alias('macd')
    
    # 计算信号线
    macd_signal = to_float32(macd_line.ewm_mean(span=signal_period)).alias('macd_signal')
    
    # 计算柱状图
    macd_hist = to_float32(macd_line - macd_signal).alias('macd_hist')
    
    return lazy_df.with_columns([macd_line, macd_signal, macd_hist])


def calculate_dmi(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """
    计算DMI指标
    
    Args:
        lazy_df: Polars LazyFrame
        windows: DMI计算窗口列表
        
    Returns:
        pl.LazyFrame: 包含DMI指标的LazyFrame
    """
    # 计算前一天的最高价、最低价、收盘价
    prev_high = pl.col('high').shift(1)
    prev_low = pl.col('low').shift(1)
    prev_close = pl.col('close').shift(1)
    
    # 计算真实波幅(TR)
    tr = pl.max_horizontal(pl.col('high'), prev_close) - pl.min_horizontal(pl.col('low'), prev_close)
    
    # 计算+DM和-DM
    high_diff = pl.col('high') - prev_high
    low_diff = prev_low - pl.col('low')
    
    plus_dm = pl.when((high_diff > low_diff) & (high_diff > 0)).then(high_diff).otherwise(0.0)
    minus_dm = pl.when((low_diff > high_diff) & (low_diff > 0)).then(low_diff).otherwise(0.0)
    
    for window in windows:
        # 计算平滑的TR、+DM、-DM
        tr_sma = tr.rolling_sum(window_size=window, min_periods=1)
        pdm_sma = plus_dm.rolling_sum(window_size=window, min_periods=1)
        ndm_sma = minus_dm.rolling_sum(window_size=window, min_periods=1)
        
        # 计算+DI和-DI
        pdi = to_float32(pdm_sma / tr_sma * 100).alias(f'pdi_{window}')
        ndi = to_float32(ndm_sma / tr_sma * 100).alias(f'ndi_{window}')
        
        # 计算DX
        dx = to_float32(pl.when((pdi + ndi) == 0).then(0.0).otherwise((pdi - ndi).abs() / (pdi + ndi) * 100)).alias(f'dx_{window}')
        
        # 计算ADX
        adx = to_float32(dx.rolling_mean(window_size=window, min_periods=1)).alias(f'adx_{window}')
        
        # 计算ADXR
        adxr = to_float32((adx + adx.shift(window)) / 2).alias(f'adxr_{window}')
        
        # 添加所有DMI指标列
        lazy_df = lazy_df.with_columns([pdi, ndi, adx, adxr])
    
    # 添加默认列名
    if len(windows) >= 1:
        window = windows[0]
        lazy_df = lazy_df.with_columns(
            pl.col(f'pdi_{window}').alias('pdi'),
            pl.col(f'ndi_{window}').alias('ndi'),
            pl.col(f'adx_{window}').alias('adx'),
            pl.col(f'adxr_{window}').alias('adxr')
        )
    
    return lazy_df


def calculate_trix(lazy_df: pl.LazyFrame, windows: list, signal_period: int) -> pl.LazyFrame:
    """
    计算TRIX指标
    
    Args:
        lazy_df: Polars LazyFrame
        windows: TRIX计算窗口列表
        signal_period: TRIX信号线周期
        
    Returns:
        pl.LazyFrame: 包含TRIX指标的LazyFrame
    """
    for window in windows:
        # 1. 第一次指数平滑（EMA1）
        ema1 = pl.col('close').ewm_mean(span=window)
        # 2. 第二次指数平滑（EMA2）
        ema2 = ema1.ewm_mean(span=window)
        # 3. 第三次指数平滑（EMA3）
        ema3 = ema2.ewm_mean(span=window)
        # 4. 计算EMA3的变化率（TRIX = (EMA3 - EMA3.shift(1)) / EMA3.shift(1) * 100）
        trix = to_float32((ema3 - ema3.shift(1)) / ema3.shift(1) * 100).alias(f'trix{window}')
        # 5. 计算TRIX的信号线（TRMA = TRIX的signal_period天EMA）
        trma = to_float32(trix.ewm_mean(span=signal_period)).alias(f'trma{window}')
        
        # 6. 构建表达式列表（只添加最终结果，不添加中间EMA列，避免混乱）
        lazy_df = lazy_df.with_columns([trix, trma])
    
    # 6. 添加默认列名
    if len(windows) >= 1:
        window = windows[0]
        lazy_df = lazy_df.with_columns(
            pl.col(f'trix{window}').alias('trix'),
            pl.col(f'trma{window}').alias('trma')
        )
    
    return lazy_df


def calculate_trend_indicators(lazy_df: pl.LazyFrame, indicator_types: list, **params) -> pl.LazyFrame:
    """
    计算所有趋势类指标
    
    Args:
        lazy_df: Polars LazyFrame
        indicator_types: 需要计算的趋势类指标列表
        **params: 指标计算参数
        
    Returns:
        pl.LazyFrame: 包含所有计算趋势类指标的LazyFrame
    """
    # 计算MA指标
    if 'ma' in indicator_types:
        windows = params.get('windows', [5, 10, 20, 60])
        lazy_df = calculate_ma(lazy_df, windows)
    
    # 计算MACD指标
    if 'macd' in indicator_types:
        fast_period = params.get('fast_period', 12)
        slow_period = params.get('slow_period', 26)
        signal_period = params.get('signal_period', 9)
        lazy_df = calculate_macd(lazy_df, fast_period, slow_period, signal_period)
    
    # 计算DMI指标
    if 'dmi' in indicator_types:
        windows = params.get('dmi_windows', [14])
        lazy_df = calculate_dmi(lazy_df, windows)
    
    # 计算TRIX指标
    if 'trix' in indicator_types:
        windows = params.get('trix_windows', [12])
        signal_period = params.get('trix_signal_period', 9)
        lazy_df = calculate_trix(lazy_df, windows, signal_period)
    
    return lazy_df
