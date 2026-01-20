#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
成交量类指标计算模块
包含：OBV, VR, PSY, VOL_MA等
"""

import polars as pl
from ..utils import to_float32


def calculate_vol_ma(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """
    计算成交量移动平均线指标
    
    Args:
        lazy_df: Polars LazyFrame
        windows: 成交量移动平均窗口列表
        
    Returns:
        pl.LazyFrame: 包含成交量移动平均线的LazyFrame
    """
    return lazy_df.with_columns(
        *[to_float32(pl.col('volume').rolling_mean(window_size=window, min_periods=1)).alias(f'vol_ma{window}')
          for window in windows]
    )


def calculate_obv(lazy_df: pl.LazyFrame) -> pl.LazyFrame:
    """
    计算OBV指标
    
    Args:
        lazy_df: Polars LazyFrame
        
    Returns:
        pl.LazyFrame: 包含OBV指标的LazyFrame
    """
    # 1. 计算价格变化方向
    obv_change = to_float32(pl.when(pl.col('close') > pl.col('close').shift(1)).then(pl.col('volume')).when(pl.col('close') < pl.col('close').shift(1)).then(-pl.col('volume')).otherwise(0.0))
    
    # 2. 累积计算OBV
    return lazy_df.with_columns(
        to_float32(obv_change.cum_sum()).alias('obv')
    )


def calculate_vr(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """
    计算VR指标
    
    Args:
        lazy_df: Polars LazyFrame
        windows: VR计算窗口列表
        
    Returns:
        pl.LazyFrame: 包含VR指标的LazyFrame
    """
    # 1. 计算价格变化方向和分类成交量
    prev_close = pl.col('close').shift(1)
    up_vol = pl.when(pl.col('close') > prev_close).then(pl.col('volume')).otherwise(0.0)
    down_vol = pl.when(pl.col('close') < prev_close).then(pl.col('volume')).otherwise(0.0)
    flat_vol = pl.when(pl.col('close') == prev_close).then(pl.col('volume')).otherwise(0.0)
    
    # 2. 计算各窗口的VR值
    for window in windows:
        # 计算N日上涨、下跌、平盘成交量总和
        up_sum = to_float32(up_vol.rolling_sum(window_size=window, min_periods=1))
        down_sum = to_float32(down_vol.rolling_sum(window_size=window, min_periods=1))
        flat_sum = to_float32(flat_vol.rolling_sum(window_size=window, min_periods=1))
        
        # 计算VR值 = (上涨总和 + 1/2平盘总和) / (下跌总和 + 1/2平盘总和) * 100
        vr = to_float32((up_sum + flat_sum / 2) / 
             (down_sum + flat_sum / 2 + 0.0001) * 100).alias(f'vr{window}')
        
        lazy_df = lazy_df.with_columns(vr)
    
    # 3. 添加默认列名
    if len(windows) >= 1:
        window = windows[0]
        lazy_df = lazy_df.with_columns(
            pl.col(f'vr{window}').alias('vr')
        )
    
    return lazy_df


def calculate_psy(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """
    计算PSY指标
    
    Args:
        lazy_df: Polars LazyFrame
        windows: PSY计算窗口列表
        
    Returns:
        pl.LazyFrame: 包含PSY指标的LazyFrame
    """
    # 1. 计算上涨天数标记（上涨为1，否则为0）
    up_day = to_float32(pl.when(pl.col('close') > pl.col('close').shift(1)).then(1.0).otherwise(0.0))
    
    # 2. 计算各窗口的PSY值（N天内上涨天数百分比）
    for window in windows:
        psy_expr = to_float32(up_day.rolling_sum(window_size=window, min_periods=1) / window * 100).alias(f'psy{window}')
        lazy_df = lazy_df.with_columns(psy_expr)
    
    # 3. 添加默认列名
    if len(windows) >= 1:
        window = windows[0]
        lazy_df = lazy_df.with_columns(
            pl.col(f'psy{window}').alias('psy')
        )
    
    return lazy_df


def calculate_volume_indicators(lazy_df: pl.LazyFrame, indicator_types: list, **params) -> pl.LazyFrame:
    """
    计算所有成交量类指标
    
    Args:
        lazy_df: Polars LazyFrame
        indicator_types: 需要计算的成交量类指标列表
        **params: 指标计算参数
        
    Returns:
        pl.LazyFrame: 包含所有计算成交量类指标的LazyFrame
    """
    # 计算VOL_MA指标
    if 'vol_ma' in indicator_types:
        windows = params.get('vol_ma_windows', [5, 10])
        lazy_df = calculate_vol_ma(lazy_df, windows)
    
    # 计算OBV指标
    if 'obv' in indicator_types:
        lazy_df = calculate_obv(lazy_df)
    
    # 计算VR指标
    if 'vr' in indicator_types:
        windows = params.get('vr_windows', [26])
        lazy_df = calculate_vr(lazy_df, windows)
    
    # 计算PSY指标
    if 'psy' in indicator_types:
        windows = params.get('psy_windows', [12])
        lazy_df = calculate_psy(lazy_df, windows)
    
    return lazy_df
