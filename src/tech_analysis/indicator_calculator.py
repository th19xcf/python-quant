#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
指标计算工具模块，提供各种技术指标的计算方法
"""

import polars as pl
import numpy as np
import ta


def calculate_ma_polars(df, windows=[5, 10, 20, 60]):
    """
    使用Polars计算移动平均线
    
    Args:
        df: Polars DataFrame
        windows: 移动平均窗口列表
        
    Returns:
        pl.DataFrame: 包含移动平均线的DataFrame
    """
    pl_df_updated = df
    
    for window in windows:
        # 使用Polars的rolling_mean方法计算MA
        pl_df_updated = pl_df_updated.with_columns(
            pl.col('close').rolling_mean(window_size=window, min_periods=1).alias(f'ma{window}')
        )
    
    return pl_df_updated


def calculate_vol_ma_polars(df, windows=[5, 10]):
    """
    使用Polars计算成交量移动平均线
    
    Args:
        df: Polars DataFrame
        windows: 移动平均窗口列表
        
    Returns:
        pl.DataFrame: 包含成交量移动平均线的DataFrame
    """
    pl_df_updated = df
    
    for window in windows:
        # 使用Polars的rolling_mean方法计算成交量MA
        pl_df_updated = pl_df_updated.with_columns(
            pl.col('volume').rolling_mean(window_size=window, min_periods=1).alias(f'vol_ma{window}')
        )
    
    return pl_df_updated


def preprocess_data_polars(df):
    """
    使用Polars进行数据预处理
    - 检查必要列是否存在
    - 转换为数值类型
    - 处理缺失值
    
    Args:
        df: Polars DataFrame
        
    Returns:
        pl.DataFrame: 预处理后的数据
    """
    # 确保必要的列存在
    required_columns = ['open', 'high', 'low', 'close', 'volume']
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"数据中没有{col}列")
    
    # 将必要列转换为数值类型
    return df.with_columns(
        pl.col(required_columns).cast(pl.Float64, strict=False).fill_nan(0.0)
    )


def sample_data_polars(df, target_points=1000, strategy='uniform'):
    """
    对Polars数据进行采样，减少数据量，提高图表渲染速度
    
    Args:
        df: Polars DataFrame
        target_points: 目标采样点数
        strategy: 采样策略，可选值：'uniform'（均匀采样）
        
    Returns:
        pl.DataFrame: 采样后的数据
    """
    data_len = len(df)
    
    if data_len <= target_points:
        # 数据量已经满足要求，无需采样
        return df
    
    # 计算采样间隔
    sample_interval = data_len // target_points
    
    if strategy == 'uniform':
        # 均匀采样
        sampled_data = df[::sample_interval]
    else:
        raise ValueError(f"不支持的采样策略: {strategy}")
    
    # 确保包含首尾数据点
    if len(sampled_data) < 2 or len(sampled_data) < len(df):
        # 添加最后一个数据点
        last_point = df.tail(1)
        sampled_data = sampled_data.vstack(last_point)
    
    return sampled_data


def generate_cache_key(data_hash, indicator_type, *args, **kwargs):
    """
    生成唯一的缓存键
    
    Args:
        data_hash: 数据哈希值
        indicator_type: 指标类型
        *args: 位置参数
        **kwargs: 关键字参数
        
    Returns:
        int: 唯一的缓存键
    """
    return hash((data_hash, indicator_type, args, tuple(sorted(kwargs.items()))))
