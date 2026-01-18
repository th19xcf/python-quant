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
    # 调用批量计算函数，减少代码冗余
    return calculate_multiple_indicators_polars(df, ['ma'], windows=windows)


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
    # 调用批量计算函数，减少代码冗余
    return calculate_multiple_indicators_polars(df, ['vol_ma'], vol_ma_windows=windows)


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
    # 检查必要列是否存在，只检查核心列
    core_columns = ['close']  # 只检查收盘价，其他列可选
    for col in core_columns:
        if col not in df.columns:
            raise ValueError(f"数据中没有{col}列")
    
    # 确定需要转换的数值列
    numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'amount']
    columns_to_cast = [col for col in numeric_columns if col in df.columns]
    
    if not columns_to_cast:
        return df
    
    # 将数值列转换为高效数值类型（float32替代float64），减少内存使用
    # 仅转换存在的列，避免不必要的转换
    return df.with_columns(
        [pl.col(col).cast(pl.Float32, strict=False).fill_nan(0.0) for col in columns_to_cast]
    )


def sample_data_polars(df, target_points=1000, strategy='adaptive'):
    """
    对Polars数据进行采样，减少数据量，提高图表渲染速度
    支持多种采样策略，包括均匀采样和自适应采样
    
    Args:
        df: Polars DataFrame或LazyFrame
        target_points: 目标采样点数
        strategy: 采样策略，可选值：'uniform'（均匀采样）、'adaptive'（自适应采样）
        
    Returns:
        pl.DataFrame或pl.LazyFrame: 采样后的数据
    """
    # 检查是否为LazyFrame
    is_lazy = isinstance(df, pl.LazyFrame)
    
    # 1. 快速路径：如果目标点数大于等于数据行数，直接返回
    if is_lazy:
        # 对于LazyFrame，我们只能在必要时才执行计算
        # 尝试使用lazy的方式估算行数，但这可能不总是准确
        # 所以我们直接进入后续逻辑，让Polars优化器处理
        pass
    else:
        data_len = len(df)
        if data_len <= target_points:
            return df
    
    if strategy == 'uniform':
        if is_lazy:
            # 对于LazyFrame，使用nth_sample进行均匀采样，这是更高效的方式
            return df.nth_sample(target_points)
        else:
            # 对于DataFrame，使用nth_sample
            data_len = len(df)
            sample_interval = data_len // target_points
            if sample_interval < 1:
                sample_interval = 1
            return df.nth(range(0, data_len, sample_interval)).head(target_points)
    elif strategy == 'adaptive':
        if is_lazy:
            # 对于LazyFrame，返回原始数据，让Polars优化器处理
            # 自适应采样需要多次数据扫描，对于LazyFrame可能效率不高
            # 这里选择直接返回原始数据，避免触发不必要的计算
            return df.head(target_points)
        else:
            # 对于DataFrame，执行自适应采样
            data_len = len(df)
            if data_len <= target_points:
                return df
            
            # 计算价格变化
            df_with_change = df.with_columns(
                pl.col('close').diff().abs().alias('price_change')
            )
            
            # 计算变化率的分位数
            change_quantile = df_with_change['price_change'].quantile(0.75)
            
            # 标记重要数据点
            important_points = df_with_change.filter(pl.col('price_change') > change_quantile)
            important_count = len(important_points)
            
            if important_count >= target_points:
                return important_points.head(target_points)
            
            # 计算需要从非重要数据点中采样的数量
            regular_count = target_points - important_count
            
            # 从非重要数据点中采样
            regular_points = df_with_change.filter(pl.col('price_change') <= change_quantile)
            regular_len = len(regular_points)
            
            if regular_len <= regular_count:
                # 非重要数据点不足，直接合并
                combined = important_points.vstack(regular_points)
            else:
                # 均匀采样非重要数据点
                sample_interval = regular_len // regular_count
                if sample_interval < 1:
                    sample_interval = 1
                sampled_regular = regular_points.nth(range(0, regular_len, sample_interval)).head(regular_count)
                combined = important_points.vstack(sampled_regular)
            
            # 按时间排序
            sampled_data = combined.sort(by=df.columns[0])
            
            # 确保包含首尾数据点
            first_point = df.head(1)
            last_point = df.tail(1)
            
            # 检查并添加首尾数据点
            if not sampled_data.filter(pl.col(df.columns[0]) == first_point[0, df.columns[0]]).is_empty():
                first_point = None
            if not sampled_data.filter(pl.col(df.columns[0]) == last_point[0, df.columns[0]]).is_empty():
                last_point = None
            
            if first_point is not None:
                sampled_data = first_point.vstack(sampled_data)
            if last_point is not None:
                sampled_data = sampled_data.vstack(last_point)
            
            # 确保采样数量不超过目标数量
            if len(sampled_data) > target_points:
                sampled_data = sampled_data.head(target_points)
            
            return sampled_data
    else:
        raise ValueError(f"不支持的采样策略: {strategy}")


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
    # 调用批量计算函数，减少代码冗余
    return calculate_multiple_indicators_polars(df, ['macd'], 
                                              fast_period=fast_period, 
                                              slow_period=slow_period, 
                                              signal_period=signal_period)


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
    # 调用批量计算函数，减少代码冗余
    return calculate_multiple_indicators_polars(df, ['rsi'], rsi_windows=windows)


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
    # 调用批量计算函数，减少代码冗余
    return calculate_multiple_indicators_polars(df, ['kdj'], kdj_windows=windows)


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
    # 调用批量计算函数，减少代码冗余，使用与其他指标一致的参数命名
    return calculate_multiple_indicators_polars(df, ['boll'], boll_windows=windows, boll_std_dev=std_dev)


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
    # 调用批量计算函数，减少代码冗余
    return calculate_multiple_indicators_polars(df, ['wr'], wr_windows=windows)


def calculate_multiple_indicators_polars(df, indicator_types=None, **params):
    """
    统一的多指标批量计算函数，将所有指标计算合并到单个查询计划
    支持链式调用，返回与输入类型一致（DataFrame或LazyFrame）
    
    Args:
        df: Polars DataFrame或LazyFrame
        indicator_types: 指标类型列表，默认计算所有指标
        **params: 指标计算参数
        
    Returns:
        pl.DataFrame或pl.LazyFrame: 包含所有计算指标的DataFrame或LazyFrame
    """
    # 默认计算所有指标
    if indicator_types is None:
        indicator_types = ['ma', 'rsi', 'kdj', 'vol_ma', 'wr', 'boll', 'macd']
    
    # 1. 收集所有需要计算的指标和参数
    indicator_params = {
        'ma': {'windows': params.get('windows', [5, 10, 20, 60])},
        'rsi': {'windows': params.get('rsi_windows', [14])},
        'kdj': {'windows': params.get('kdj_windows', [14])},
        'vol_ma': {'windows': params.get('vol_ma_windows', [5, 10])},
        'wr': {'windows': params.get('wr_windows', [10, 6])},
        # 同时支持旧参数名（为兼容性）和新参数名（为一致性）
        'boll': {'windows': params.get('boll_windows', params.get('windows', [20])), 
                 'std_dev': params.get('boll_std_dev', params.get('std_dev', 2.0))},
        'macd': {
            'fast_period': params.get('fast_period', 12),
            'slow_period': params.get('slow_period', 26),
            'signal_period': params.get('signal_period', 9)
        }
    }
    
    # 2. 收集所有需要的窗口大小
    all_windows = set()
    for indicator in indicator_types:
        if indicator in indicator_params:
            if 'windows' in indicator_params[indicator]:
                all_windows.update(indicator_params[indicator]['windows'])
    
    # 3. 预计算共享的窗口列（最高价、最低价、收盘价的窗口统计）
    need_high_low = any(indicator in indicator_types for indicator in ['kdj', 'wr', 'boll'])
    
    # 4. 使用Lazy API构建查询，确保所有计算在单个查询计划中执行
    lazy_df = df.lazy()
    
    # 步骤1: 添加共享的窗口列
    if need_high_low:
        for window in all_windows:
            # 预计算最高价和最低价的rolling_max/min
            lazy_df = lazy_df.with_columns(
                pl.col('high').rolling_max(window_size=window, min_periods=1).alias(f'high_n_{window}'),
                pl.col('low').rolling_min(window_size=window, min_periods=window).alias(f'low_n_{window}')
            )
    
    # 步骤2: 计算MA指标
    if 'ma' in indicator_types:
        windows = indicator_params['ma']['windows']
        lazy_df = lazy_df.with_columns(
            *[pl.col('close').rolling_mean(window_size=window, min_periods=1).cast(pl.Float32).alias(f'ma{window}')
              for window in windows]
        )
    
    # 步骤3: 计算VOL_MA指标
    if 'vol_ma' in indicator_types:
        windows = indicator_params['vol_ma']['windows']
        lazy_df = lazy_df.with_columns(
            *[pl.col('volume').rolling_mean(window_size=window, min_periods=1).cast(pl.Float32).alias(f'vol_ma{window}')
              for window in windows]
        )
    
    # 步骤4: 计算RSI指标
    if 'rsi' in indicator_types:
        windows = indicator_params['rsi']['windows']
        # 计算价格变化
        lazy_df = lazy_df.with_columns(
            pl.col('close').diff().alias('price_change')
        )
        
        # 计算上涨和下跌变化
        lazy_df = lazy_df.with_columns(
            pl.when(pl.col('price_change') > 0).then(pl.col('price_change')).otherwise(0).alias('gain'),
            pl.when(pl.col('price_change') < 0).then(-pl.col('price_change')).otherwise(0).alias('loss')
        )
        
        # 计算RSI，分解步骤以确保依赖关系正确
        for window in windows:
            # 第一步：计算平均上涨和下跌幅度
            lazy_df = lazy_df.with_columns(
                pl.col('gain').ewm_mean(span=window).alias(f'avg_gain_{window}'),
                pl.col('loss').ewm_mean(span=window).alias(f'avg_loss_{window}')
            )
            
            # 第二步：计算RSI值
            lazy_df = lazy_df.with_columns(
                pl.when(pl.col(f'avg_loss_{window}') == 0)
                .then(100.0)
                .otherwise(100.0 - (100.0 / (1.0 + (pl.col(f'avg_gain_{window}') / pl.col(f'avg_loss_{window}')))))
                .cast(pl.Float32)
                .alias(f'rsi{window}')
            )
    
    # 步骤5: 计算KDJ指标
    if 'kdj' in indicator_types:
        windows = indicator_params['kdj']['windows']
        for window in windows:
            # 计算RSV值
            lazy_df = lazy_df.with_columns(
                ((pl.col('close') - pl.col(f'low_n_{window}')) / 
                 (pl.col(f'high_n_{window}') - pl.col(f'low_n_{window}')) * 100).cast(pl.Float32).alias(f'rsv_{window}')
            )
            
            # 计算k、d、j值
            k_expr = pl.col(f'rsv_{window}').rolling_mean(window_size=3, min_periods=1).cast(pl.Float32).alias(f'k{window}')
            d_expr = k_expr.rolling_mean(window_size=3, min_periods=1).cast(pl.Float32).alias(f'd{window}')
            j_expr = (3 * k_expr - 2 * d_expr).cast(pl.Float32).alias(f'j{window}')
            
            lazy_df = lazy_df.with_columns([k_expr, d_expr, j_expr])
            
            # 添加默认列名
            if window == 14:
                lazy_df = lazy_df.with_columns(
                    pl.col(f'k{window}').alias('k'),
                    pl.col(f'd{window}').alias('d'),
                    pl.col(f'j{window}').alias('j')
                )
    
    # 步骤6: 计算WR指标
    if 'wr' in indicator_types:
        windows = indicator_params['wr']['windows']
        for window in windows:
            lazy_df = lazy_df.with_columns(
                ((pl.col(f'high_n_{window}') - pl.col('close')) / 
                 (pl.col(f'high_n_{window}') - pl.col(f'low_n_{window}')) * 100).cast(pl.Float32).alias(f'wr{window}')
            )
        
        # 添加默认列名
        if len(windows) >= 1:
            lazy_df = lazy_df.with_columns(
                pl.col(f'wr{windows[0]}').alias('wr'),
                pl.col(f'wr{windows[0]}').alias('wr1')
            )
        if len(windows) >= 2:
            lazy_df = lazy_df.with_columns(
                pl.col(f'wr{windows[1]}').alias('wr2')
            )
    
    # 步骤7: 计算Boll指标
    if 'boll' in indicator_types:
        boll_params = indicator_params['boll']
        windows = boll_params['windows']
        std_dev = boll_params['std_dev']
        
        # 批量计算所有窗口的Boll指标
        for window in windows:
            # 计算移动平均线（中轨线）
            mb_expr = pl.col('close').rolling_mean(window_size=window, min_periods=1).cast(pl.Float32).alias(f'mb{window}')
            # 计算上轨线和下轨线
            up_expr = (mb_expr + pl.col('close').rolling_std(window_size=window, min_periods=1).cast(pl.Float32) * std_dev).cast(pl.Float32).alias(f'up{window}')
            dn_expr = (mb_expr - pl.col('close').rolling_std(window_size=window, min_periods=1).cast(pl.Float32) * std_dev).cast(pl.Float32).alias(f'dn{window}')
            
            # 添加到DataFrame
            lazy_df = lazy_df.with_columns([mb_expr, up_expr, dn_expr])
        
        # 添加默认列名
        if len(windows) >= 1:
            window = windows[0]
            lazy_df = lazy_df.with_columns(
                pl.col(f'mb{window}').alias('mb'),
                pl.col(f'up{window}').alias('up'),
                pl.col(f'dn{window}').alias('dn')
            )
    
    # 步骤8: 计算MACD指标
    if 'macd' in indicator_types:
        macd_params = indicator_params['macd']
        fast_period = macd_params['fast_period']
        slow_period = macd_params['slow_period']
        signal_period = macd_params['signal_period']
        
        # 1. 计算EMA12和EMA26
        lazy_df = lazy_df.with_columns(
            pl.col('close').ewm_mean(span=fast_period).alias('ema12'),
            pl.col('close').ewm_mean(span=slow_period).alias('ema26')
        )
        
        # 2. 计算MACD线
        lazy_df = lazy_df.with_columns(
            (pl.col('ema12') - pl.col('ema26')).cast(pl.Float32).alias('macd')
        )
        
        # 3. 计算信号线
        lazy_df = lazy_df.with_columns(
            pl.col('macd').ewm_mean(span=signal_period).cast(pl.Float32).alias('macd_signal')
        )
        
        # 4. 计算柱状图
        lazy_df = lazy_df.with_columns(
            (pl.col('macd') - pl.col('macd_signal')).cast(pl.Float32).alias('macd_hist')
        )
    
    # 步骤9: 清理临时列
    temp_cols = []
    # 清理RSI临时列
    if 'rsi' in indicator_types:
        windows = indicator_params['rsi']['windows']
        temp_cols.extend(['price_change', 'gain', 'loss'])
        temp_cols.extend([f'avg_gain_{window}' for window in windows])
        temp_cols.extend([f'avg_loss_{window}' for window in windows])
    
    # 清理KDJ临时列
    if 'kdj' in indicator_types:
        windows = indicator_params['kdj']['windows']
        temp_cols.extend([f'rsv_{window}' for window in windows])
    
    # 清理MACD临时列
    if 'macd' in indicator_types:
        temp_cols.extend(['ema12', 'ema26'])
    
    # 清理共享临时列
    if need_high_low:
        temp_cols.extend([f'high_n_{window}' for window in all_windows])
        temp_cols.extend([f'low_n_{window}' for window in all_windows])
    
    # 直接删除临时列，无需检查是否存在
    # Polars的drop操作会自动忽略不存在的列，因此无需先检查
    if temp_cols:
        lazy_df = lazy_df.drop(*temp_cols)
    
    # 执行计算
    if isinstance(df, pl.LazyFrame):
        return lazy_df
    else:
        return lazy_df.collect()


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
