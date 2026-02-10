#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
指标计算工具模块，提供各种技术指标的计算方法
"""

import polars as pl
import numpy as np
from .utils import (
    collect_used_windows,
    collect_indicator_windows,
    merge_used_windows,
    add_default_column_names,
    get_indicator_params,
    cleanup_temp_columns,
    to_float32,
    calculate_mad
)
from .indicators import (
    calculate_trend_indicators,
    calculate_oscillator_indicators,
    calculate_volume_indicators,
    calculate_volatility_indicators
)


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
    - 保留复权价格字段
    
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
    
    # 确定需要转换的数值列（包括原始价格和复权价格）
    numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'amount',
                       'qfq_open', 'qfq_high', 'qfq_low', 'qfq_close',
                       'hfq_open', 'hfq_high', 'hfq_low', 'hfq_close']
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


def calculate_dma_polars(df, short_period=10, long_period=50, signal_period=10):
    """
    使用Polars计算DMA指标（平均线差）
    支持链式调用，返回与输入类型一致（DataFrame或LazyFrame）

    Args:
        df: Polars DataFrame或LazyFrame
        short_period: 短期MA周期，默认10
        long_period: 长期MA周期，默认50
        signal_period: DMA信号线周期，默认10

    Returns:
        pl.DataFrame或pl.LazyFrame: 包含DMA指标的DataFrame或LazyFrame
    """
    return calculate_multiple_indicators_polars(df, ['dma'],
                                               dma_short_period=short_period,
                                               dma_long_period=long_period,
                                               dma_signal_period=signal_period)


def calculate_fsl_polars(df):
    """
    使用Polars计算FSL指标（分水岭指标）
    支持链式调用，返回与输入类型一致（DataFrame或LazyFrame）

    Args:
        df: Polars DataFrame或LazyFrame

    Returns:
        pl.DataFrame或pl.LazyFrame: 包含FSL指标的DataFrame或LazyFrame
    """
    return calculate_multiple_indicators_polars(df, ['fsl'])


def calculate_sar_polars(df, af_step=0.02, max_af=0.2):
    """
    使用Polars计算SAR指标（抛物线转向指标）
    支持链式调用，返回与输入类型一致（DataFrame或LazyFrame）

    Args:
        df: Polars DataFrame或LazyFrame
        af_step: 加速因子步长，默认0.02
        max_af: 最大加速因子，默认0.2

    Returns:
        pl.DataFrame或pl.LazyFrame: 包含SAR指标的DataFrame或LazyFrame
    """
    return calculate_multiple_indicators_polars(df, ['sar'],
                                               sar_af_step=af_step,
                                               sar_max_af=max_af)


def calculate_vol_tdx_polars(df, ma_period=5):
    """
    使用Polars计算VOL-TDX指标（成交量趋势）
    支持链式调用，返回与输入类型一致（DataFrame或LazyFrame）

    Args:
        df: Polars DataFrame或LazyFrame
        ma_period: 移动平均周期，默认5

    Returns:
        pl.DataFrame或pl.LazyFrame: 包含VOL-TDX指标的DataFrame或LazyFrame
    """
    return calculate_multiple_indicators_polars(df, ['vol_tdx'],
                                               vol_tdx_ma_period=ma_period)


def calculate_cr_polars(df, windows=[26]):
    """
    使用Polars计算CR指标（能量指标）
    支持链式调用，返回与输入类型一致（DataFrame或LazyFrame）

    Args:
        df: Polars DataFrame或LazyFrame
        windows: CR计算窗口列表，默认为[26]

    Returns:
        pl.DataFrame或pl.LazyFrame: 包含CR指标的DataFrame或LazyFrame
    """
    if windows is None:
        windows = [26]
    return calculate_multiple_indicators_polars(df, ['cr'], cr_windows=windows)


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
    indicator_params = get_indicator_params(**params)
    
    # 2. 收集所有需要的窗口大小
    all_windows = collect_used_windows(indicator_types, indicator_params)
    
    # 3. 预计算共享的窗口列（最高价、最低价的窗口统计）
    need_high_low = any(indicator in indicator_types for indicator in ['kdj', 'wr', 'boll'])
    
    # 4. 使用Lazy API构建查询，确保所有计算在单个查询计划中执行
    lazy_df = df.lazy()
    
    # 步骤1: 添加共享的窗口列
    # 只创建实际需要的共享窗口列
    if need_high_low:
        # 收集实际使用的窗口大小
        indicator_windows = collect_indicator_windows(indicator_types, indicator_params)
        used_windows = merge_used_windows(indicator_windows)
        
        # 只创建实际使用的窗口列
        for window in used_windows:
            # 预计算最高价和最低价的rolling_max/min
            lazy_df = lazy_df.with_columns(
                pl.col('high').rolling_max(window_size=window, min_periods=1).alias(f'high_n_{window}'),
                pl.col('low').rolling_min(window_size=window, min_periods=window).alias(f'low_n_{window}')
            )
    
    # 步骤2: 计算趋势类指标
    lazy_df = calculate_trend_indicators(lazy_df, indicator_types, **params)
    
    # 步骤3: 计算震荡类指标
    lazy_df = calculate_oscillator_indicators(lazy_df, indicator_types, **params)
    
    # 步骤4: 计算成交量类指标
    lazy_df = calculate_volume_indicators(lazy_df, indicator_types, **params)
    
    # 步骤5: 计算波动率类指标
    lazy_df = calculate_volatility_indicators(lazy_df, indicator_types, **params)
    
    # 清理临时列
    lazy_df = cleanup_temp_columns(lazy_df, indicator_types, indicator_params)
    
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
