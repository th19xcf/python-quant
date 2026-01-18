#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
指标计算工具模块，提供各种技术指标的计算方法
"""

import polars as pl
import numpy as np


def calculate_ma_polars(df, windows=[5, 10, 20, 60]):
    """
    使用Polars计算移动平均线（Lazy API优化）
    支持链式调用，返回与输入类型一致（DataFrame或LazyFrame）
    
    Args:
        df: Polars DataFrame或LazyFrame
        windows: 移动平均窗口列表
        
    Returns:
        pl.DataFrame或pl.LazyFrame: 包含移动平均线的DataFrame或LazyFrame
    """
    # 使用Lazy API计算移动平均线，支持链式调用，并确保结果为float32类型
    return df.with_columns(
        *[pl.col('close').rolling_mean(window_size=window, min_periods=1).cast(pl.Float32).alias(f'ma{window}') 
          for window in windows]
    )


def calculate_vol_ma_polars(df, windows=[5, 10]):
    """
    使用Polars计算成交量移动平均线（Lazy API优化）
    支持链式调用，返回与输入类型一致（DataFrame或LazyFrame）
    
    Args:
        df: Polars DataFrame或LazyFrame
        windows: 移动平均窗口列表
        
    Returns:
        pl.DataFrame或pl.LazyFrame: 包含成交量移动平均线的DataFrame或LazyFrame
    """
    # 使用Lazy API计算成交量移动平均线，支持链式调用，并确保结果为float32类型
    return df.with_columns(
        *[pl.col('volume').rolling_mean(window_size=window, min_periods=1).cast(pl.Float32).alias(f'vol_ma{window}') 
          for window in windows]
    )


def preprocess_data_polars(df):
    """
    使用Polars进行数据预处理
    - 检查必要列是否存在
    - 转换为高效数值类型（float32替代float64）
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
    
    # 将必要列转换为高效数值类型（float32替代float64），减少内存使用
    return df.with_columns(
        pl.col(['open', 'high', 'low', 'close']).cast(pl.Float32, strict=False).fill_nan(0.0),
        pl.col('volume').cast(pl.Float32, strict=False).fill_nan(0.0)
    )


def sample_data_polars(df, target_points=1000, strategy='adaptive'):
    """
    对Polars数据进行采样，减少数据量，提高图表渲染速度
    支持多种采样策略，包括均匀采样和自适应采样
    
    Args:
        df: Polars DataFrame
        target_points: 目标采样点数
        strategy: 采样策略，可选值：'uniform'（均匀采样）、'adaptive'（自适应采样）
        
    Returns:
        pl.DataFrame: 采样后的数据
    """
    data_len = len(df)
    
    if data_len <= target_points:
        # 数据量已经满足要求，无需采样
        return df
    
    if strategy == 'uniform':
        # 均匀采样
        sample_interval = data_len // target_points
        sampled_data = df[::sample_interval]
    elif strategy == 'adaptive':
        # 自适应采样：基于数据密度和重要性
        
        # 1. 计算数据的变化率
        df_with_change = df.with_columns(
            pl.col('close').diff().abs().alias('price_change')
        )
        
        # 2. 计算变化率的分位数，确定重要数据点
        change_quantile = df_with_change['price_change'].quantile(0.75)
        
        # 3. 标记重要数据点（变化率大于分位数的点）
        important_points = df_with_change.filter(pl.col('price_change') > change_quantile)
        
        # 4. 计算需要从非重要数据点中采样的数量
        important_count = len(important_points)
        regular_count = target_points - important_count
        
        # 5. 如果重要数据点已经足够，直接返回
        if important_count >= target_points:
            return important_points.head(target_points)
        
        # 6. 从非重要数据点中均匀采样
        regular_points = df_with_change.filter(pl.col('price_change') <= change_quantile)
        regular_sample_interval = len(regular_points) // regular_count
        sampled_regular = regular_points[::regular_sample_interval].head(regular_count)
        
        # 7. 合并重要数据点和采样的非重要数据点
        combined = important_points.vstack(sampled_regular)
        
        # 8. 按索引排序
        sampled_data = combined.sort(by=df.columns[0])
    else:
        raise ValueError(f"不支持的采样策略: {strategy}")
    
    # 确保包含首尾数据点
    if len(sampled_data) > 0:
        first_point = df.head(1)
        last_point = df.tail(1)
        
        # 检查是否已包含首尾数据点
        if sampled_data[0, df.columns[0]] != first_point[0, df.columns[0]]:
            sampled_data = first_point.vstack(sampled_data)
        
        if sampled_data[-1, df.columns[0]] != last_point[0, df.columns[0]]:
            sampled_data = sampled_data.vstack(last_point)
    
    # 确保采样数量不超过目标数量
    if len(sampled_data) > target_points:
        sampled_data = sampled_data.head(target_points)
    
    return sampled_data


def calculate_macd_polars(df, fast_period=12, slow_period=26, signal_period=9):
    """
    使用Polars计算MACD指标（Lazy API优化）
    支持链式调用，返回与输入类型一致（DataFrame或LazyFrame）
    
    Args:
        df: Polars DataFrame或LazyFrame
        fast_period: 快速EMA周期
        slow_period: 慢速EMA周期
        signal_period: 信号线EMA周期
        
    Returns:
        pl.DataFrame或pl.LazyFrame: 包含MACD指标的DataFrame或LazyFrame
    """
    # 使用Lazy API计算MACD指标，合并所有步骤为单个查询，支持链式调用，并确保结果为float32类型
    return df.with_columns([
        pl.col('close').ewm_mean(span=fast_period).alias('ema12'),
        pl.col('close').ewm_mean(span=slow_period).alias('ema26')
    ]).with_columns([
        (pl.col('ema12') - pl.col('ema26')).cast(pl.Float32).alias('macd')
    ]).with_columns([
        pl.col('macd').ewm_mean(span=signal_period).cast(pl.Float32).alias('macd_signal')
    ]).with_columns([
        (pl.col('macd') - pl.col('macd_signal')).cast(pl.Float32).alias('macd_hist')
    ]).drop(['ema12', 'ema26'])


def calculate_rsi_polars(df, windows=None):
    """
    使用Polars批量计算RSI指标（Lazy API优化）
    优化：使用EMA替代普通移动平均线计算平均上涨和下跌幅度
    支持链式调用，返回与输入类型一致（DataFrame或LazyFrame）
    
    Args:
        df: Polars DataFrame或LazyFrame
        windows: RSI计算窗口列表，默认为[14]
        
    Returns:
        pl.DataFrame或pl.LazyFrame: 包含RSI指标的DataFrame或LazyFrame
    """
    if windows is None:
        windows = [14]
    
    # 使用Lazy API计算RSI指标，合并所有步骤为单个查询，支持链式调用，并确保结果为float32类型
    return df.with_columns(
        # 计算价格变化
        pl.col('close').diff().alias('price_change')
    ).with_columns(
        # 计算上涨和下跌变化
        pl.when(pl.col('price_change') > 0).then(pl.col('price_change')).otherwise(0).alias('gain'),
        pl.when(pl.col('price_change') < 0).then(-pl.col('price_change')).otherwise(0).alias('loss')
    ).with_columns(
        # 批量计算所有窗口的avg_gain和avg_loss，使用EMA替代普通移动平均线
        *[pl.col('gain').ewm_mean(span=window).alias(f'avg_gain_{window}') for window in windows],
        *[pl.col('loss').ewm_mean(span=window).alias(f'avg_loss_{window}') for window in windows]
    ).with_columns(
        # 批量计算所有窗口的RSI，并确保结果为float32类型
        *[pl.when(pl.col(f'avg_loss_{window}') == 0)
          .then(100.0)
          .otherwise(100.0 - (100.0 / (1.0 + (pl.col(f'avg_gain_{window}') / pl.col(f'avg_loss_{window}')))))
          .cast(pl.Float32)
          .alias(f'rsi{window}') for window in windows]
    ).drop(
        # 清理临时列
        ['price_change', 'gain', 'loss'] + 
        [f'avg_gain_{window}' for window in windows] + 
        [f'avg_loss_{window}' for window in windows]
    )


def calculate_kdj_polars(df, windows=None):
    """
    使用Polars批量计算KDJ指标（Lazy API优化）
    支持链式调用，返回与输入类型一致（DataFrame或LazyFrame）
    
    Args:
        df: Polars DataFrame或LazyFrame
        windows: KDJ计算窗口列表，默认为[14]
        
    Returns:
        pl.DataFrame或pl.LazyFrame: 包含KDJ指标的DataFrame或LazyFrame
    """
    if windows is None:
        windows = [14]
    
    # 使用Lazy API计算KDJ指标，合并所有步骤为单个查询
    # 1. 准备列定义
    high_cols = []
    low_cols = []
    for window in windows:
        high_cols.append(pl.col('high').rolling_max(window_size=window, min_periods=1).alias(f'high_n_{window}'))
        low_cols.append(pl.col('low').rolling_min(window_size=window, min_periods=1).alias(f'low_n_{window}'))
    
    # 2. 准备rsv列定义
    rsv_cols = []
    for window in windows:
        rsv_cols.append(
            ((pl.col('close') - pl.col(f'low_n_{window}')) / 
             (pl.col(f'high_n_{window}') - pl.col(f'low_n_{window}')) * 100).cast(pl.Float32).alias(f'rsv_{window}')
        )
    
    # 3. 准备k、d、j列定义
    k_cols = []
    d_cols = []
    j_cols = []
    default_cols = []
    for window in windows:
        # 计算k值
        k_expr = pl.col(f'rsv_{window}').rolling_mean(window_size=3, min_periods=1).cast(pl.Float32).alias(f'k{window}')
        k_cols.append(k_expr)
        
        # 计算d值
        d_expr = k_expr.rolling_mean(window_size=3, min_periods=1).cast(pl.Float32).alias(f'd{window}')
        d_cols.append(d_expr)
        
        # 计算j值
        j_expr = (3 * k_expr - 2 * d_expr).cast(pl.Float32).alias(f'j{window}')
        j_cols.append(j_expr)
        
        # 设置默认列名
        if window == 14:
            default_cols.extend([
                pl.col(f'k{window}').alias('k'),
                pl.col(f'd{window}').alias('d'),
                pl.col(f'j{window}').alias('j')
            ])
    
    # 4. 准备临时列列表
    temp_cols = []
    for window in windows:
        temp_cols.extend([f'high_n_{window}', f'low_n_{window}', f'rsv_{window}'])
    
    # 使用Lazy API执行所有计算
    result = df.with_columns(
        *high_cols, *low_cols
    ).with_columns(
        *rsv_cols
    ).with_columns(
        *k_cols, *d_cols, *j_cols
    )
    
    # 添加默认列名
    if default_cols:
        result = result.with_columns(*default_cols)
    
    # 清理临时列
    return result.drop(temp_cols)


def calculate_boll_polars(df, windows=[20], std_dev=2.0):
    """
    使用Polars批量计算Boll指标（布林带），优化性能
    支持链式调用，返回与输入类型一致（DataFrame或LazyFrame）
    
    Args:
        df: Polars DataFrame或LazyFrame
        windows: Boll计算窗口列表，默认为[20]
        std_dev: 标准差倍数，默认为2.0
        
    Returns:
        pl.DataFrame或pl.LazyFrame: 包含Boll指标的DataFrame或LazyFrame
    """
    if windows is None:
        windows = [20]
    
    # 批量计算所有窗口的Boll指标，减少数据遍历次数，并确保结果为float32类型
    # 1. 准备所有计算表达式
    boll_exprs = []
    for window in windows:
        # 计算移动平均线（中轨线）
        ma_expr = pl.col('close').rolling_mean(window_size=window, min_periods=1).cast(pl.Float32).alias(f'mb{window}')
        # 计算上轨线和下轨线，合并标准差计算到表达式中
        up_expr = (ma_expr + pl.col('close').rolling_std(window_size=window, min_periods=1).cast(pl.Float32) * std_dev).cast(pl.Float32).alias(f'up{window}')
        dn_expr = (ma_expr - pl.col('close').rolling_std(window_size=window, min_periods=1).cast(pl.Float32) * std_dev).cast(pl.Float32).alias(f'dn{window}')
        # 添加到表达式列表
        boll_exprs.extend([ma_expr, up_expr, dn_expr])
    
    # 2. 一次性添加所有计算结果，支持链式调用
    result = df.with_columns(boll_exprs)
    
    # 3. 准备默认列名定义
    default_cols = []
    if len(windows) >= 1:
        window = windows[0]
        default_cols.extend([
            pl.col(f'mb{window}').alias('mb'),
            pl.col(f'up{window}').alias('up'),
            pl.col(f'dn{window}').alias('dn')
        ])
    
    # 4. 添加默认列名
    if default_cols:
        result = result.with_columns(default_cols)
    
    return result


def calculate_wr_polars(df, windows=None):
    """
    使用Polars批量计算WR指标（威廉指标），优化性能，模拟通达信WR(10,6)效果
    支持链式调用，返回与输入类型一致（DataFrame或LazyFrame）
    
    Args:
        df: Polars DataFrame或LazyFrame
        windows: WR计算窗口列表，默认为[10, 6]
        
    Returns:
        pl.DataFrame或pl.LazyFrame: 包含WR指标的DataFrame或LazyFrame
    """
    if windows is None:
        windows = [10, 6]  # 通达信默认使用WR10和WR6
    
    # 使用Lazy API计算WR指标，合并所有步骤为单个查询，并确保结果为float32类型
    # 1. 准备临时列定义（high_n和low_n）
    temp_cols = []
    temp_exprs = []
    
    for window in windows:
        # 计算n日内最高价和最低价
        temp_exprs.append(pl.col('high').rolling_max(window_size=window, min_periods=1).alias(f'high_n_{window}'))
        temp_exprs.append(pl.col('low').rolling_min(window_size=window, min_periods=1).alias(f'low_n_{window}'))
        temp_cols.extend([f'high_n_{window}', f'low_n_{window}'])
    
    # 2. 计算WR值
    wr_exprs = []
    for window in windows:
        wr_exprs.append(
            ((pl.col(f'high_n_{window}') - pl.col('close')) / 
             (pl.col(f'high_n_{window}') - pl.col(f'low_n_{window}')) * 100).cast(pl.Float32).alias(f'wr{window}')
        )
    
    # 3. 准备默认列名定义（兼容旧版本和通达信风格）
    default_cols = []
    if len(windows) >= 1:
        # 旧版本兼容：生成wr列
        default_cols.append(pl.col(f'wr{windows[0]}').alias('wr'))
        # 通达信风格：生成wr1列
        default_cols.append(pl.col(f'wr{windows[0]}').alias('wr1'))
    if len(windows) >= 2:
        # 通达信风格：生成wr2列
        default_cols.append(pl.col(f'wr{windows[1]}').alias('wr2'))
    
    # 4. 执行计算，分步骤进行
    result = df.with_columns(temp_exprs)
    result = result.with_columns(wr_exprs)
    
    # 5. 添加默认列名
    if default_cols:
        result = result.with_columns(default_cols)
    
    # 6. 清理临时列
    result = result.drop(temp_cols)
    
    return result


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
