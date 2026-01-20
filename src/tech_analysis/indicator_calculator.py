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


def calculate_dmi_polars(df, windows=[14]):
    """
    使用Polars批量计算DMI指标（趋向指标）
    支持链式调用，返回与输入类型一致（DataFrame或LazyFrame）
    
    Args:
        df: Polars DataFrame或LazyFrame
        windows: DMI计算窗口列表，默认为[14]
        
    Returns:
        pl.DataFrame或pl.LazyFrame: 包含DMI指标的DataFrame或LazyFrame
    """
    if windows is None:
        windows = [14]  # 通达信默认使用DMI14
    # 调用批量计算函数，减少代码冗余
    return calculate_multiple_indicators_polars(df, ['dmi'], dmi_windows=windows)


def calculate_cci_polars(df, windows=[14]):
    """
    使用Polars批量计算CCI指标（商品通道指标）
    支持链式调用，返回与输入类型一致（DataFrame或LazyFrame）
    
    Args:
        df: Polars DataFrame或LazyFrame
        windows: CCI计算窗口列表，默认为[14]
        
    Returns:
        pl.DataFrame或pl.LazyFrame: 包含CCI指标的DataFrame或LazyFrame
    """
    if windows is None:
        windows = [14]  # 通达信默认使用CCI14
    # 调用批量计算函数，减少代码冗余
    return calculate_multiple_indicators_polars(df, ['cci'], cci_windows=windows)


def calculate_roc_polars(df, windows=[12]):
    """
    使用Polars批量计算ROC指标（变化率指标）
    支持链式调用，返回与输入类型一致（DataFrame或LazyFrame）
    
    Args:
        df: Polars DataFrame或LazyFrame
        windows: ROC计算窗口列表，默认为[12]
        
    Returns:
        pl.DataFrame或pl.LazyFrame: 包含ROC指标的DataFrame或LazyFrame
    """
    if windows is None:
        windows = [12]  # 通达信默认使用ROC12
    # 调用批量计算函数，减少代码冗余
    return calculate_multiple_indicators_polars(df, ['roc'], roc_windows=windows)


def calculate_mtm_polars(df, windows=[12]):
    """
    使用Polars批量计算MTM指标（动量指标）
    支持链式调用，返回与输入类型一致（DataFrame或LazyFrame）
    
    Args:
        df: Polars DataFrame或LazyFrame
        windows: MTM计算窗口列表，默认为[12]
        
    Returns:
        pl.DataFrame或pl.LazyFrame: 包含MTM指标的DataFrame或LazyFrame
    """
    if windows is None:
        windows = [12]  # 通达信默认使用MTM12
    # 调用批量计算函数，减少代码冗余
    return calculate_multiple_indicators_polars(df, ['mtm'], mtm_windows=windows)


def calculate_obv_polars(df):
    """
    使用Polars计算OBV指标（能量潮）
    支持链式调用，返回与输入类型一致（DataFrame或LazyFrame）
    
    Args:
        df: Polars DataFrame或LazyFrame
        
    Returns:
        pl.DataFrame或pl.LazyFrame: 包含OBV指标的DataFrame或LazyFrame
    """
    # 调用批量计算函数，减少代码冗余
    return calculate_multiple_indicators_polars(df, ['obv'])


def calculate_vr_polars(df, windows=[26]):
    """
    使用Polars批量计算VR指标（成交量比率）
    支持链式调用，返回与输入类型一致（DataFrame或LazyFrame）
    
    Args:
        df: Polars DataFrame或LazyFrame
        windows: VR计算窗口列表，默认为[26]
        
    Returns:
        pl.DataFrame或pl.LazyFrame: 包含VR指标的DataFrame或LazyFrame
    """
    if windows is None:
        windows = [26]  # 通达信默认使用VR26
    # 调用批量计算函数，减少代码冗余
    return calculate_multiple_indicators_polars(df, ['vr'], vr_windows=windows)


def calculate_psy_polars(df, windows=[12]):
    """
    使用Polars批量计算PSY指标（心理线）
    支持链式调用，返回与输入类型一致（DataFrame或LazyFrame）
    
    Args:
        df: Polars DataFrame或LazyFrame
        windows: PSY计算窗口列表，默认为[12]
        
    Returns:
        pl.DataFrame或pl.LazyFrame: 包含PSY指标的DataFrame或LazyFrame
    """
    if windows is None:
        windows = [12]  # 通达信默认使用PSY12
    # 调用批量计算函数，减少代码冗余
    return calculate_multiple_indicators_polars(df, ['psy'], psy_windows=windows)


def calculate_trix_polars(df, windows=[12], signal_period=9):
    """
    使用Polars批量计算TRIX指标（三重指数平滑）
    支持链式调用，返回与输入类型一致（DataFrame或LazyFrame）
    
    Args:
        df: Polars DataFrame或LazyFrame
        windows: TRIX计算窗口列表，默认为[12]
        signal_period: TRIX信号线周期，默认为9
        
    Returns:
        pl.DataFrame或pl.LazyFrame: 包含TRIX指标的DataFrame或LazyFrame
    """
    if windows is None:
        windows = [12]  # 通达信默认使用TRIX12
    # 调用批量计算函数，减少代码冗余
    return calculate_multiple_indicators_polars(df, ['trix'], trix_windows=windows, trix_signal_period=signal_period)


def calculate_brar_polars(df, windows=[26]):
    """
    使用Polars批量计算BRAR指标（情绪指标）
    支持链式调用，返回与输入类型一致（DataFrame或LazyFrame）
    
    Args:
        df: Polars DataFrame或LazyFrame
        windows: BRAR计算窗口列表，默认为[26]
        
    Returns:
        pl.DataFrame或pl.LazyFrame: 包含BRAR指标的DataFrame或LazyFrame
    """
    if windows is None:
        windows = [26]  # 通达信默认使用BRAR26
    # 调用批量计算函数，减少代码冗余
    return calculate_multiple_indicators_polars(df, ['brar'], brar_windows=windows)


def calculate_asi_polars(df, signal_period=20):
    """
    使用Polars计算ASI指标（振动升降指标）
    支持链式调用，返回与输入类型一致（DataFrame或LazyFrame）
    
    Args:
        df: Polars DataFrame或LazyFrame
        signal_period: ASI信号线周期，默认为20
        
    Returns:
        pl.DataFrame或pl.LazyFrame: 包含ASI指标的DataFrame或LazyFrame
    """
    # 调用批量计算函数，减少代码冗余
    return calculate_multiple_indicators_polars(df, ['asi'], asi_signal_period=signal_period)


def calculate_emv_polars(df, windows=[14], constant=100000000):
    """
    使用Polars批量计算EMV指标（简易波动指标）
    支持链式调用，返回与输入类型一致（DataFrame或LazyFrame）
    
    Args:
        df: Polars DataFrame或LazyFrame
        windows: EMV计算窗口列表，默认为[14]
        constant: 常数，用于调整EMV的数值大小，默认为100000000
        
    Returns:
        pl.DataFrame或pl.LazyFrame: 包含EMV指标的DataFrame或LazyFrame
    """
    if windows is None:
        windows = [14]  # 通达信默认使用EMV14
    # 调用批量计算函数，减少代码冗余
    return calculate_multiple_indicators_polars(df, ['emv'], emv_windows=windows, emv_constant=constant)


def calculate_mcst_polars(df, windows=[12]):
    """
    使用Polars批量计算MCST指标（市场成本）
    支持链式调用，返回与输入类型一致（DataFrame或LazyFrame）
    
    Args:
        df: Polars DataFrame或LazyFrame
        windows: MCST计算窗口列表，默认为[12]
        
    Returns:
        pl.DataFrame或pl.LazyFrame: 包含MCST指标的DataFrame或LazyFrame
    """
    if windows is None:
        windows = [12]  # 通达信默认使用MCST12
    # 调用批量计算函数，减少代码冗余
    return calculate_multiple_indicators_polars(df, ['mcst'], mcst_windows=windows)


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
        indicator_types = ['ma', 'rsi', 'kdj', 'vol_ma', 'wr', 'boll', 'macd', 'dmi', 'cci', 'roc', 'mtm', 'obv', 'vr', 'psy', 'trix', 'brar', 'asi', 'emv', 'mcst']
    
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
        },
        'dmi': {'windows': params.get('dmi_windows', [14])},
        'cci': {'windows': params.get('cci_windows', [14])},
        'roc': {'windows': params.get('roc_windows', [12])},
        'mtm': {'windows': params.get('mtm_windows', [12])},
        'obv': {},
        'vr': {'windows': params.get('vr_windows', [26])},
        'psy': {'windows': params.get('psy_windows', [12])},
        'trix': {
            'windows': params.get('trix_windows', [12]),
            'signal_period': params.get('trix_signal_period', 9)
        },
        'brar': {'windows': params.get('brar_windows', [26])},
        'asi': {
            'signal_period': params.get('asi_signal_period', 20)
        },
        'emv': {
            'windows': params.get('emv_windows', [14]),
            'constant': params.get('emv_constant', 100000000)
        },
        'mcst': {
            'windows': params.get('mcst_windows', [12])
        }
    }
    
    # 2. 收集所有需要的窗口大小
    all_windows = set()
    for indicator in indicator_types:
        if indicator in indicator_params:
            if 'windows' in indicator_params[indicator]:
                all_windows.update(indicator_params[indicator]['windows'])
    
    # 3. 预计算共享的窗口列（最高价、最低价的窗口统计）
    need_high_low = any(indicator in indicator_types for indicator in ['kdj', 'wr', 'boll'])
    
    # 4. 使用Lazy API构建查询，确保所有计算在单个查询计划中执行
    lazy_df = df.lazy()
    
    # 步骤1: 添加共享的窗口列
    # 只创建实际需要的共享窗口列
    if need_high_low:
        # 收集实际使用的窗口大小
        used_windows = set()
        
        # 检查每个指标实际使用的窗口
        if 'kdj' in indicator_types:
            used_windows.update(indicator_params['kdj']['windows'])
        if 'wr' in indicator_types:
            used_windows.update(indicator_params['wr']['windows'])
        if 'boll' in indicator_types:
            used_windows.update(indicator_params['boll']['windows'])
        
        # 只创建实际使用的窗口列
        for window in used_windows:
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
        price_change = pl.col('close').diff()
        
        # 计算上涨和下跌变化
        gain = pl.when(price_change > 0).then(price_change).otherwise(0)
        loss = pl.when(price_change < 0).then(-price_change).otherwise(0)
        
        # 计算RSI，使用表达式别名避免创建中间列
        for window in windows:
            # 直接计算RSI值，不创建中间列
            avg_gain = gain.ewm_mean(span=window)
            avg_loss = loss.ewm_mean(span=window)
            
            rsi = pl.when(avg_loss == 0).then(100.0).otherwise(100.0 - (100.0 / (1.0 + (avg_gain / avg_loss)))).cast(pl.Float32).alias(f'rsi{window}')
            
            lazy_df = lazy_df.with_columns(rsi)
    
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
        
        # 1. 直接计算EMA值，不创建中间列
        ema12 = pl.col('close').ewm_mean(span=fast_period)
        ema26 = pl.col('close').ewm_mean(span=slow_period)
        
        # 2. 计算MACD线
        macd_line = (ema12 - ema26).cast(pl.Float32).alias('macd')
        
        # 3. 计算信号线
        macd_signal = macd_line.ewm_mean(span=signal_period).cast(pl.Float32).alias('macd_signal')
        
        # 4. 计算柱状图
        macd_hist = (macd_line - macd_signal).cast(pl.Float32).alias('macd_hist')
        
        # 添加所有MACD指标列
        lazy_df = lazy_df.with_columns([macd_line, macd_signal, macd_hist])
    
    # 步骤9: 计算DMI指标
    if 'dmi' in indicator_types:
        dmi_params = indicator_params['dmi']
        windows = dmi_params['windows']
        
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
            pdi = (pdm_sma / tr_sma * 100).cast(pl.Float32).alias(f'pdi_{window}')
            ndi = (ndm_sma / tr_sma * 100).cast(pl.Float32).alias(f'ndi_{window}')
            
            # 计算DX
            dx = pl.when((pdi + ndi) == 0).then(0.0).otherwise(((pdi - ndi).abs() / (pdi + ndi) * 100).cast(pl.Float32)).alias(f'dx_{window}')
            
            # 计算ADX
            adx = dx.rolling_mean(window_size=window, min_periods=1).cast(pl.Float32).alias(f'adx_{window}')
            
            # 计算ADXR
            adxr = ((adx + adx.shift(window)) / 2).cast(pl.Float32).alias(f'adxr_{window}')
            
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
    
    # 步骤10: 计算CCI指标
    if 'cci' in indicator_types:
        cci_params = indicator_params['cci']
        windows = cci_params['windows']
        
        # 计算典型价格（TP = (H + L + C) / 3）
        tp = (pl.col('high') + pl.col('low') + pl.col('close')) / 3
        
        for window in windows:
            # 计算典型价格的N日移动平均值（MA_TP）
            ma_tp = tp.rolling_mean(window_size=window, min_periods=1)
            
            # 计算平均绝对偏差（MAD）
            # 优化MAD计算，使用rolling_mean和rolling_std的组合方式
            tp_series = tp
            # 计算滚动平均值
            ma_tp_rolling = tp_series.rolling_mean(window_size=window, min_periods=1)
            # 计算绝对偏差
            abs_dev = (tp_series - ma_tp_rolling).abs()
            # 计算滚动平均绝对偏差
            mad = abs_dev.rolling_mean(window_size=window, min_periods=1).cast(pl.Float32)
            
            # 计算CCI = (TP - MA_TP) / (0.015 * MAD)
            cci = ((tp - ma_tp) / (0.015 * mad)).cast(pl.Float32).alias(f'cci{window}')
            
            # 添加CCI指标列
            lazy_df = lazy_df.with_columns(cci)
        
        # 添加默认列名
        if len(windows) >= 1:
            window = windows[0]
            lazy_df = lazy_df.with_columns(
                pl.col(f'cci{window}').alias('cci')
            )
    
    # 步骤11: 计算ROC指标
    if 'roc' in indicator_types:
        roc_params = indicator_params['roc']
        windows = roc_params['windows']
        
        for window in windows:
            # 计算ROC = ((当前收盘价 - n天前收盘价) / n天前收盘价) * 100
            lazy_df = lazy_df.with_columns(
                ((pl.col('close') - pl.col('close').shift(window)) / 
                 pl.col('close').shift(window) * 100).cast(pl.Float32).alias(f'roc{window}')
            )
        
        # 添加默认列名
        if len(windows) >= 1:
            window = windows[0]
            lazy_df = lazy_df.with_columns(
                pl.col(f'roc{window}').alias('roc')
            )
    
    # 步骤12: 计算MTM指标
    if 'mtm' in indicator_types:
        mtm_params = indicator_params['mtm']
        windows = mtm_params['windows']
        
        for window in windows:
            # 计算MTM = 当前收盘价 - n天前收盘价
            lazy_df = lazy_df.with_columns(
                (pl.col('close') - pl.col('close').shift(window)).cast(pl.Float32).alias(f'mtm{window}')
            )
        
        # 添加默认列名
        if len(windows) >= 1:
            window = windows[0]
            lazy_df = lazy_df.with_columns(
                pl.col(f'mtm{window}').alias('mtm')
            )
    
    # 步骤13: 计算OBV指标
    if 'obv' in indicator_types:
        # 1. 计算价格变化方向
        obv_change = pl.when(pl.col('close') > pl.col('close').shift(1)).then(pl.col('volume')).when(pl.col('close') < pl.col('close').shift(1)).then(-pl.col('volume')).otherwise(0.0).cast(pl.Float32)
        
        # 2. 累积计算OBV
        lazy_df = lazy_df.with_columns(
            obv_change.cum_sum().cast(pl.Float32).alias('obv')
        )
    
    # 步骤14: 计算VR指标
    if 'vr' in indicator_types:
        vr_params = indicator_params['vr']
        windows = vr_params['windows']
        
        # 1. 计算价格变化方向和分类成交量
        prev_close = pl.col('close').shift(1)
        up_vol = pl.when(pl.col('close') > prev_close).then(pl.col('volume')).otherwise(0.0)
        down_vol = pl.when(pl.col('close') < prev_close).then(pl.col('volume')).otherwise(0.0)
        flat_vol = pl.when(pl.col('close') == prev_close).then(pl.col('volume')).otherwise(0.0)
        
        # 2. 计算各窗口的VR值
        for window in windows:
            # 计算N日上涨、下跌、平盘成交量总和
            up_sum = up_vol.rolling_sum(window_size=window, min_periods=1).cast(pl.Float32)
            down_sum = down_vol.rolling_sum(window_size=window, min_periods=1).cast(pl.Float32)
            flat_sum = flat_vol.rolling_sum(window_size=window, min_periods=1).cast(pl.Float32)
            
            # 计算VR值 = (上涨总和 + 1/2平盘总和) / (下跌总和 + 1/2平盘总和) * 100
            vr = ((up_sum + flat_sum / 2) / 
                 (down_sum + flat_sum / 2 + 0.0001) * 100).cast(pl.Float32).alias(f'vr{window}')
            
            lazy_df = lazy_df.with_columns(vr)
        
        # 3. 添加默认列名
        if len(windows) >= 1:
            window = windows[0]
            lazy_df = lazy_df.with_columns(
                pl.col(f'vr{window}').alias('vr')
            )
    
    # 步骤15: 计算PSY指标
    if 'psy' in indicator_types:
        psy_params = indicator_params['psy']
        windows = psy_params['windows']
        
        # 1. 计算上涨天数标记（上涨为1，否则为0）
        up_day = pl.when(pl.col('close') > pl.col('close').shift(1)).then(1.0).otherwise(0.0).cast(pl.Float32)
        
        # 2. 计算各窗口的PSY值（N天内上涨天数百分比）
        for window in windows:
            psy_expr = (up_day.rolling_sum(window_size=window, min_periods=1) / window * 100).cast(pl.Float32).alias(f'psy{window}')
            lazy_df = lazy_df.with_columns(psy_expr)
        
        # 3. 添加默认列名
        if len(windows) >= 1:
            window = windows[0]
            lazy_df = lazy_df.with_columns(
                pl.col(f'psy{window}').alias('psy')
            )
    
    # 步骤16: 计算TRIX指标
    if 'trix' in indicator_types:
        trix_params = indicator_params['trix']
        windows = trix_params['windows']
        signal_period = trix_params['signal_period']
        
        for window in windows:
            # 1. 第一次指数平滑（EMA1）
            ema1 = pl.col('close').ewm_mean(span=window)
            # 2. 第二次指数平滑（EMA2）
            ema2 = ema1.ewm_mean(span=window)
            # 3. 第三次指数平滑（EMA3）
            ema3 = ema2.ewm_mean(span=window)
            # 4. 计算EMA3的变化率（TRIX = (EMA3 - EMA3.shift(1)) / EMA3.shift(1) * 100）
            trix = ((ema3 - ema3.shift(1)) / ema3.shift(1) * 100).cast(pl.Float32).alias(f'trix{window}')
            # 5. 计算TRIX的信号线（TRMA = TRIX的signal_period天EMA）
            trma = trix.ewm_mean(span=signal_period).cast(pl.Float32).alias(f'trma{window}')
            
            # 6. 构建表达式列表（只添加最终结果，不添加中间EMA列，避免混乱）
            lazy_df = lazy_df.with_columns([trix, trma])
        
        # 6. 添加默认列名
        if len(windows) >= 1:
            window = windows[0]
            lazy_df = lazy_df.with_columns(
                pl.col(f'trix{window}').alias('trix'),
                pl.col(f'trma{window}').alias('trma')
            )
    
    # 步骤17: 计算BRAR指标
    if 'brar' in indicator_types:
        brar_params = indicator_params['brar']
        windows = brar_params['windows']

        # 1. 计算必要的中间变量
        # 前一日收盘价
        prev_close = pl.col('close').shift(1).alias('prev_close')
        # AR的分子：(最高价 - 开盘价)的正值部分
        ar_up = pl.when((pl.col('high') - pl.col('open')) > 0).then(pl.col('high') - pl.col('open')).otherwise(0.0).alias('ar_up')
        # AR的分母：(开盘价 - 最低价)的正值部分
        ar_down = pl.when((pl.col('open') - pl.col('low')) > 0).then(pl.col('open') - pl.col('low')).otherwise(0.0).alias('ar_down')
        # BR的分子：(最高价 - 前一日收盘价)的正值部分
        br_up = pl.when((pl.col('high') - prev_close) > 0).then(pl.col('high') - prev_close).otherwise(0.0).alias('br_up')
        # BR的分母：(前一日收盘价 - 最低价)的正值部分
        br_down = pl.when((prev_close - pl.col('low')) > 0).then(prev_close - pl.col('low')).otherwise(0.0).alias('br_down')

        # 2. 添加中间变量到DataFrame
        lazy_df = lazy_df.with_columns([ar_up, ar_down, br_up, br_down])

        # 3. 计算各窗口的AR和BR值
        for window in windows:
            # 计算AR = (ar_up的N日和 / ar_down的N日和) * 100
            ar_expr = ((pl.col('ar_up').rolling_sum(window_size=window, min_periods=1) / (pl.col('ar_down').rolling_sum(window_size=window, min_periods=1) + 0.0001)) * 100).cast(pl.Float32).alias(f'ar{window}')
            # 计算BR = (br_up的N日和 / br_down的N日和) * 100
            br_expr = ((pl.col('br_up').rolling_sum(window_size=window, min_periods=1) / (pl.col('br_down').rolling_sum(window_size=window, min_periods=1) + 0.0001)) * 100).cast(pl.Float32).alias(f'br{window}')

            # 添加到DataFrame
            lazy_df = lazy_df.with_columns([ar_expr, br_expr])

        # 4. 添加默认列名
        if len(windows) >= 1:
            window = windows[0]
            lazy_df = lazy_df.with_columns(
                pl.col(f'ar{window}').alias('ar'),
                pl.col(f'br{window}').alias('br')
            )
    
    # 步骤18: 计算ASI指标
    if 'asi' in indicator_types:
        asi_params = indicator_params['asi']
        signal_period = asi_params['signal_period']
        
        # 1. 计算前一日的数据
        prev_high = pl.col('high').shift(1).alias('prev_high')
        prev_low = pl.col('low').shift(1).alias('prev_low')
        prev_close = pl.col('close').shift(1).alias('prev_close')
        
        # 2. 计算各种价格差
        # 今日开盘价与前一日收盘价的差
        open_diff = pl.col('open') - prev_close
        # 今日最高价与前一日收盘价的差
        high_diff = pl.col('high') - prev_close
        # 今日最低价与前一日收盘价的差
        low_diff = pl.col('low') - prev_close
        # 今日最高价与今日开盘价的差
        h_o_diff = pl.col('high') - pl.col('open')
        # 今日最低价与今日开盘价的差
        l_o_diff = pl.col('low') - pl.col('open')
        # 前一日收盘价与前一日开盘价的差
        prev_c_o_diff = prev_close - pl.col('open').shift(1)
        
        # 3. 计算真实波幅（简化计算，避免中间变量）
        tr = pl.max_horizontal(
            (pl.col('high').shift(1) - pl.col('close')).abs(),
            (pl.col('low').shift(1) - pl.col('close')).abs(),
            (pl.col('high').shift(1) - pl.col('low').shift(1)).abs()
        )

        # 4. 计算ASI的分子部分
        asi_numerator = (pl.col('open') - pl.col('close').shift(1)) + 0.5 * (pl.col('open') - pl.col('close')) + 0.25 * (pl.col('close').shift(1) - pl.col('open').shift(1))

        # 5. 计算ASI值（使用Polars条件表达式处理除以零的情况）
        asi = pl.when(tr != 0).then((asi_numerator / tr * 16).cast(pl.Float32)).otherwise(0.0).cast(pl.Float32).alias('asi')

        # 6. 计算ASI的信号线（ASI_SIG = ASI的signal_period天MA）
        asi_sig = asi.rolling_mean(window_size=signal_period, min_periods=1).alias('asi_sig')

        # 7. 添加到DataFrame
        lazy_df = lazy_df.with_columns([asi, asi_sig])
    
    # 步骤19: 计算EMV指标
    if 'emv' in indicator_types:
        emv_params = indicator_params['emv']
        windows = emv_params['windows']
        constant = emv_params['constant']

        # 1. 直接计算EMV基础值，避免中间变量和列名冲突
        # 距离差值：(最高价 + 最低价)/2 - 前一日(最高价 + 最低价)/2
        distance_diff = ((pl.col('high') + pl.col('low')) / 2) - ((pl.col('high').shift(1) + pl.col('low').shift(1)) / 2)

        # 2. 计算EMV基础值（直接在表达式中处理除以零的情况）
        # EMV = (距离差值 / (成交量 / (最高价 - 最低价))) * 常数，当最高价等于最低价时为0
        emv_base_expr = pl.when(pl.col('high') != pl.col('low')).then((distance_diff / (pl.col('volume') / (pl.col('high') - pl.col('low'))) * constant).cast(pl.Float32)).otherwise(0.0).cast(pl.Float32)

        # 3. 计算各窗口的EMV移动平均线
        for window in windows:
            emv_expr = emv_base_expr.rolling_mean(window_size=window, min_periods=1).cast(pl.Float32).alias(f'emv{window}')
            lazy_df = lazy_df.with_columns(emv_expr)

        # 4. 添加默认列名
        if len(windows) >= 1:
            window = windows[0]
            lazy_df = lazy_df.with_columns(
                pl.col(f'emv{window}').alias('emv')
            )
    
    # 步骤20: 计算MCST指标
    if 'mcst' in indicator_types:
        mcst_params = indicator_params['mcst']
        windows = mcst_params['windows']
        
        # 1. 计算中间值
        # 价格 * 成交量
        price_volume = (pl.col('close') * pl.col('volume')).alias('price_volume')
        # 累积成本：累积(价格 * 成交量)
        cumulative_cost = price_volume.cum_sum().alias('cumulative_cost')
        # 累积成交量：累积(成交量)
        cumulative_volume = pl.col('volume').cum_sum().alias('cumulative_volume')
        
        # 2. 添加中间变量到DataFrame
        lazy_df = lazy_df.with_columns([price_volume, cumulative_cost, cumulative_volume])
        
        # 3. 计算MCST（使用Polars条件表达式处理除以零的情况）
        mcst = pl.when(cumulative_volume != 0).then((cumulative_cost / cumulative_volume).cast(pl.Float32)).otherwise(pl.lit(0.0).cast(pl.Float32)).alias('mcst')
        
        # 4. 添加MCST值到DataFrame
        lazy_df = lazy_df.with_columns(mcst)
        
        # 5. 计算各窗口的MCST移动平均线
        for window in windows:
            mcst_ma_expr = mcst.rolling_mean(window_size=window, min_periods=1).cast(pl.Float32).alias(f'mcst_ma{window}')
            lazy_df = lazy_df.with_columns(mcst_ma_expr)
    
    # 步骤13: 清理临时列
    temp_cols = []
    # 清理KDJ临时列
    if 'kdj' in indicator_types:
        windows = indicator_params['kdj']['windows']
        temp_cols.extend([f'rsv_{window}' for window in windows])
    

    

    

    
    # 清理TRIX临时列
    if 'trix' in indicator_types:
        # TRIX指标不再使用临时列，不需要清理
        pass
    
    # 清理BRAR临时列
    if 'brar' in indicator_types:
        temp_cols.extend(['ar_up', 'ar_down', 'br_up', 'br_down'])
    
    # 清理ASI临时列
    if 'asi' in indicator_types:
        # ASI指标计算已优化，不再使用临时列，无需清理
        pass
    
    # 清理EMV临时列
    if 'emv' in indicator_types:
        # EMV指标计算已优化，不再使用临时列，无需清理
        pass
    
    # 清理MCST临时列
    if 'mcst' in indicator_types:
        temp_cols.extend(['price_volume', 'cumulative_cost', 'cumulative_volume'])
    
    # 清理共享临时列
    if need_high_low:
        # 只清理实际创建的共享窗口列
        used_windows = set()
        if 'kdj' in indicator_types:
            used_windows.update(indicator_params['kdj']['windows'])
        if 'wr' in indicator_types:
            used_windows.update(indicator_params['wr']['windows'])
        if 'boll' in indicator_types:
            used_windows.update(indicator_params['boll']['windows'])
        
        temp_cols.extend([f'high_n_{window}' for window in used_windows])
        temp_cols.extend([f'low_n_{window}' for window in used_windows])
    
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
