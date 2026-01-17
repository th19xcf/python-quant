#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
指标计算工具模块，提供各种技术指标的计算方法
"""

import polars as pl
import numpy as np


def calculate_ma_polars(df, windows=[5, 10, 20, 60]):
    """
    使用Polars计算移动平均线
    
    Args:
        df: Polars DataFrame
        windows: 移动平均窗口列表
        
    Returns:
        pl.DataFrame: 包含移动平均线的DataFrame
    """
    return df.with_columns(
        *[pl.col('close').rolling_mean(window_size=window, min_periods=1).alias(f'ma{window}') 
          for window in windows]
    )


def calculate_vol_ma_polars(df, windows=[5, 10]):
    """
    使用Polars计算成交量移动平均线
    
    Args:
        df: Polars DataFrame
        windows: 移动平均窗口列表
        
    Returns:
        pl.DataFrame: 包含成交量移动平均线的DataFrame
    """
    return df.with_columns(
        *[pl.col('volume').rolling_mean(window_size=window, min_periods=1).alias(f'vol_ma{window}') 
          for window in windows]
    )


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
        pl.col('close').ewm_mean(span=fast_period).alias(f'ema{fast_period}'),
        pl.col('close').ewm_mean(span=slow_period).alias(f'ema{slow_period}')
    ])
    
    # 计算DIF (MACD线)
    df = df.with_columns(
        (pl.col(f'ema{fast_period}') - pl.col(f'ema{slow_period}')).alias('macd')
    )
    
    # 计算DEA (信号线)
    df = df.with_columns(
        pl.col('macd').ewm_mean(span=signal_period).alias('macd_signal')
    )
    
    # 计算柱状图 (MACD柱状图)
    df = df.with_columns(
        (pl.col('macd') - pl.col('macd_signal')).alias('macd_hist')
    )
    
    # 清理临时列
    df = df.drop([f'ema{fast_period}', f'ema{slow_period}'])
    
    return df


def calculate_rsi_polars(df, windows=None):
    """
    使用Polars批量计算RSI指标
    
    Args:
        df: Polars DataFrame
        windows: RSI计算窗口列表，默认为[14]
        
    Returns:
        pl.DataFrame: 包含RSI指标的DataFrame
    """
    if windows is None:
        windows = [14]
    
    # 计算价格变化
    if 'price_change' not in df.columns:
        df = df.with_columns(
            pl.col('close').diff().alias('price_change')
        )
    
    # 计算上涨和下跌变化
    if 'gain' not in df.columns or 'loss' not in df.columns:
        df = df.with_columns([
            pl.when(pl.col('price_change') > 0).then(pl.col('price_change')).otherwise(0).alias('gain'),
            pl.when(pl.col('price_change') < 0).then(-pl.col('price_change')).otherwise(0).alias('loss')
        ])
    
    # 批量计算所有窗口的avg_gain和avg_loss
    gain_cols = []
    loss_cols = []
    for window in windows:
        gain_cols.append(pl.col('gain').rolling_mean(window_size=window, min_periods=1).alias(f'avg_gain_{window}'))
        loss_cols.append(pl.col('loss').rolling_mean(window_size=window, min_periods=1).alias(f'avg_loss_{window}'))
    
    df = df.with_columns(*gain_cols, *loss_cols)
    
    # 批量计算所有窗口的RSI
    rsi_cols = []
    for window in windows:
        rsi_cols.append(
            pl.when(pl.col(f'avg_loss_{window}') == 0)
            .then(100.0)
            .otherwise(100.0 - (100.0 / (1.0 + (pl.col(f'avg_gain_{window}') / pl.col(f'avg_loss_{window}')))))
            .alias(f'rsi{window}')
        )
    
    df = df.with_columns(*rsi_cols)
    
    # 清理临时列
    temp_cols = ['price_change', 'gain', 'loss']
    for window in windows:
        temp_cols.extend([f'avg_gain_{window}', f'avg_loss_{window}'])
    
    return df.drop(temp_cols)


def calculate_kdj_polars(df, windows=None):
    """
    使用Polars批量计算KDJ指标
    
    Args:
        df: Polars DataFrame
        windows: KDJ计算窗口列表，默认为[14]
        
    Returns:
        pl.DataFrame: 包含KDJ指标的DataFrame
    """
    if windows is None:
        windows = [14]
    
    # 批量计算所有窗口的high_n和low_n
    high_cols = []
    low_cols = []
    for window in windows:
        high_cols.append(pl.col('high').rolling_max(window_size=window, min_periods=1).alias(f'high_n_{window}'))
        low_cols.append(pl.col('low').rolling_min(window_size=window, min_periods=1).alias(f'low_n_{window}'))
    
    df = df.with_columns(*high_cols, *low_cols)
    
    # 批量计算所有窗口的rsv
    rsv_cols = []
    for window in windows:
        rsv_cols.append(
            ((pl.col('close') - pl.col(f'low_n_{window}')) / 
             (pl.col(f'high_n_{window}') - pl.col(f'low_n_{window}')) * 100).alias(f'rsv_{window}')
        )
    
    df = df.with_columns(*rsv_cols)
    
    # 批量计算所有窗口的k、d、j值
    k_cols = []
    d_cols = []
    j_cols = []
    
    for window in windows:
        # 计算k值
        # 注意：使用不带下划线的列名格式，如k14而不是k_14
        k_col = pl.col(f'rsv_{window}').rolling_mean(window_size=3, min_periods=1).alias(f'k{window}')
        k_cols.append(k_col)
        
        # 计算d值
        d_col = k_col.rolling_mean(window_size=3, min_periods=1).alias(f'd{window}')
        d_cols.append(d_col)
        
        # 计算j值
        j_col = (3 * k_col - 2 * d_col).alias(f'j{window}')
        j_cols.append(j_col)
    
    df = df.with_columns(*k_cols, *d_cols, *j_cols)
    
    # 设置默认列名（如果只有一个窗口或第一个窗口）
    if len(windows) == 1 or windows[0] in windows:
        window = windows[0]
        df = df.with_columns([
            pl.col(f'k{window}').alias('k'),
            pl.col(f'd{window}').alias('d'),
            pl.col(f'j{window}').alias('j')
        ])
    
    # 清理临时列
    temp_cols = []
    for window in windows:
        temp_cols.extend([f'high_n_{window}', f'low_n_{window}', f'rsv_{window}'])
    
    return df.drop(temp_cols)


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
