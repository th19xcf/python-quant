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


def calculate_macd_polars(df, fast_period=12, slow_period=26, signal_period=9):
    """
    使用Polars计算MACD指标
    
    Args:
        df: Polars DataFrame
        fast_period: 快速EMA周期
        slow_period: 慢速EMA周期
        signal_period: 信号线EMA周期
        
    Returns:
        pl.DataFrame: 包含MACD指标的DataFrame
    """
    # 计算快速EMA和慢速EMA
    df = df.with_columns([
        pl.col('close').rolling_mean(window_size=fast_period, min_periods=1).alias(f'ema{fast_period}'),
        pl.col('close').rolling_mean(window_size=slow_period, min_periods=1).alias(f'ema{slow_period}')
    ])
    
    # 计算DIF (MACD线)
    df = df.with_columns(
        (pl.col(f'ema{fast_period}') - pl.col(f'ema{slow_period}')).alias('macd')
    )
    
    # 计算DEA (信号线)
    df = df.with_columns(
        pl.col('macd').rolling_mean(window_size=signal_period, min_periods=1).alias('macd_signal')
    )
    
    # 计算柱状图 (MACD柱状图)
    df = df.with_columns(
        (pl.col('macd') - pl.col('macd_signal')).alias('macd_hist')
    )
    
    # 清理临时列
    df = df.drop([f'ema{fast_period}', f'ema{slow_period}'])
    
    return df


def calculate_rsi_polars(df, window=14):
    """
    使用Polars计算RSI指标
    
    Args:
        df: Polars DataFrame
        window: RSI计算窗口
        
    Returns:
        pl.DataFrame: 包含RSI指标的DataFrame
    """
    # 计算价格变化
    df = df.with_columns(
        pl.col('close').diff().alias('price_change')
    )
    
    # 计算上涨和下跌变化
    df = df.with_columns([
        pl.when(pl.col('price_change') > 0).then(pl.col('price_change')).otherwise(0).alias('gain'),
        pl.when(pl.col('price_change') < 0).then(-pl.col('price_change')).otherwise(0).alias('loss')
    ])
    
    # 计算平均上涨和平均下跌
    df = df.with_columns([
        pl.col('gain').rolling_mean(window_size=window, min_periods=1).alias('avg_gain'),
        pl.col('loss').rolling_mean(window_size=window, min_periods=1).alias('avg_loss')
    ])
    
    # 计算RSI
    df = df.with_columns(
        pl.when(pl.col('avg_loss') == 0)
        .then(100.0)
        .otherwise(100.0 - (100.0 / (1.0 + (pl.col('avg_gain') / pl.col('avg_loss')))))
        .alias(f'rsi{window}')
    )
    
    # 清理临时列
    df = df.drop(['price_change', 'gain', 'loss', 'avg_gain', 'avg_loss'])
    
    return df


def calculate_kdj_polars(df, window=14):
    """
    使用Polars计算KDJ指标
    
    Args:
        df: Polars DataFrame
        window: KDJ计算窗口
        
    Returns:
        pl.DataFrame: 包含KDJ指标的DataFrame
    """
    # 计算最高价的N日最高价
    df = df.with_columns(
        pl.col('high').rolling_max(window_size=window, min_periods=1).alias('high_n'),
        pl.col('low').rolling_min(window_size=window, min_periods=1).alias('low_n')
    )
    
    # 计算RSV (未成熟随机值)
    df = df.with_columns(
        ((pl.col('close') - pl.col('low_n')) / (pl.col('high_n') - pl.col('low_n')) * 100).alias('rsv')
    )
    
    # 计算K、D、J值
    # 使用SMA计算K、D
    df = df.with_columns(
        pl.col('rsv').rolling_mean(window_size=3, min_periods=1).alias('k')
    )
    
    df = df.with_columns(
        pl.col('k').rolling_mean(window_size=3, min_periods=1).alias('d')
    )
    
    # 计算J值
    df = df.with_columns(
        (3 * pl.col('k') - 2 * pl.col('d')).alias('j')
    )
    
    # 清理临时列
    df = df.drop(['high_n', 'low_n', 'rsv'])
    
    return df


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
