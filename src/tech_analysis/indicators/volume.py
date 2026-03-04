#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
成交量类指标计算模块
包含：OBV, VR, PSY, VOL_MA等
"""

import polars as pl
from ..utils import to_float32
from ..common_calculations import (
    calculate_moving_average,
    add_default_columns
)


def calculate_vol_ma(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """
    计算成交量移动平均线指标
    
    Args:
        lazy_df: Polars LazyFrame
        windows: 成交量移动平均窗口列表
        
    Returns:
        pl.LazyFrame: 包含成交量移动平均线的LazyFrame
    """
    # 使用通用移动平均计算函数
    lazy_df = calculate_moving_average(lazy_df, 'volume', windows, 'vol_')
    # 添加默认列名
    lazy_df = add_default_columns(lazy_df, 'vol_ma', windows)
    return lazy_df


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
    lazy_df = add_default_columns(lazy_df, 'vr', windows)
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
    lazy_df = add_default_columns(lazy_df, 'psy', windows)
    return lazy_df


def calculate_cr(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """
    计算CR指标（能量指标）
    CR = (N日内上涨日成交量总和 - N日内下跌日成交量总和) / (N日内上涨日成交量总和 + N日内下跌日成交量总和) * 100
    
    Args:
        lazy_df: Polars LazyFrame
        windows: CR计算窗口列表
        
    Returns:
        pl.LazyFrame: 包含CR指标的LazyFrame
    """
    # 1. 计算价格变化方向和分类成交量
    prev_close = pl.col('close').shift(1)
    up_vol = pl.when(pl.col('close') > prev_close).then(pl.col('volume')).otherwise(0.0)
    down_vol = pl.when(pl.col('close') < prev_close).then(pl.col('volume')).otherwise(0.0)
    
    # 2. 计算各窗口的CR值
    for window in windows:
        # 计算N日上涨、下跌成交量总和
        up_sum = to_float32(up_vol.rolling_sum(window_size=window, min_periods=1))
        down_sum = to_float32(down_vol.rolling_sum(window_size=window, min_periods=1))
        
        # 计算CR值 = (上涨总和 - 下跌总和) / (上涨总和 + 下跌总和) * 100
        cr = to_float32((up_sum - down_sum) / 
             (up_sum + down_sum + 0.0001) * 100).alias(f'cr{window}')
        
        lazy_df = lazy_df.with_columns(cr)
    
    # 3. 添加默认列名
    lazy_df = add_default_columns(lazy_df, 'cr', windows)
    return lazy_df


def calculate_hsl(lazy_df: pl.LazyFrame, float_share: float = None) -> pl.LazyFrame:
    """
    计算HSL指标（换手率）
    HSL = 成交量 / 流通股本 × 100%
    
    Args:
        lazy_df: Polars LazyFrame
        float_share: 流通股本（股），如果为None则使用volume列的5日平均作为估算
        
    Returns:
        pl.LazyFrame: 包含HSL指标的LazyFrame
    """
    if float_share is not None and float_share > 0:
        # 使用提供的流通股本计算
        hsl = to_float32(pl.col('volume') / float_share * 100).alias('hsl')
    else:
        # 估算：使用60日平均成交量作为流通股本的参考
        avg_volume = pl.col('volume').rolling_mean(window_size=60, min_periods=1)
        hsl = to_float32(pl.when(avg_volume > 0).then(pl.col('volume') / avg_volume * 5).otherwise(0)).alias('hsl')
    
    # 先添加HSL列
    lazy_df = lazy_df.with_columns(hsl)
    
    # 计算HSL的5日和10日移动平均
    hsl_ma5 = to_float32(pl.col('hsl').rolling_mean(window_size=5, min_periods=1)).alias('hsl_ma5')
    hsl_ma10 = to_float32(pl.col('hsl').rolling_mean(window_size=10, min_periods=1)).alias('hsl_ma10')
    
    return lazy_df.with_columns([hsl_ma5, hsl_ma10])


def calculate_lb(lazy_df: pl.LazyFrame, period: int = 5) -> pl.LazyFrame:
    """
    计算LB指标（量比）
    LB = 当日成交量 / 过去N日平均成交量
    
    Args:
        lazy_df: Polars LazyFrame
        period: 计算平均成交量的周期，默认5日
        
    Returns:
        pl.LazyFrame: 包含LB指标的LazyFrame
    """
    # 计算过去N日平均成交量（不包含当日）
    avg_volume = pl.col('volume').shift(1).rolling_mean(window_size=period, min_periods=1)
    
    # 计算量比
    lb = to_float32(pl.when(avg_volume > 0).then(pl.col('volume') / avg_volume).otherwise(1)).alias('lb')
    
    return lazy_df.with_columns(lb)


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
    
    # 计算CR指标
    if 'cr' in indicator_types:
        windows = params.get('cr_windows', [26])
        lazy_df = calculate_cr(lazy_df, windows)
    
    # 计算HSL指标（换手率）
    if 'hsl' in indicator_types:
        float_share = params.get('float_share', None)
        lazy_df = calculate_hsl(lazy_df, float_share)
    
    # 计算LB指标（量比）
    if 'lb' in indicator_types:
        period = params.get('lb_period', 5)
        lazy_df = calculate_lb(lazy_df, period)
    
    return lazy_df
