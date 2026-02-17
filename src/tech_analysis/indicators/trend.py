#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
趋势类指标计算模块
包含：MA, MACD, DMI, TRIX, SAR等
"""

import polars as pl
import numpy as np
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
    # 修改：使用min_periods=window，确保只有窗口满时才计算平均值
    # 这样可以避免早期的均线值为0或接近0的情况
    return lazy_df.with_columns(
        *[to_float32(pl.col('close').rolling_mean(window_size=window, min_periods=window)).alias(f'ma{window}')
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


def calculate_sar(lazy_df: pl.LazyFrame, af_step: float = 0.02, max_af: float = 0.2) -> pl.LazyFrame:
    """
    计算SAR指标（抛物线转向指标）
    由于SAR是迭代计算，使用map_batches处理

    Args:
        lazy_df: Polars LazyFrame
        af_step: 加速因子步长，默认0.02
        max_af: 最大加速因子，默认0.2

    Returns:
        pl.LazyFrame: 包含SAR指标的LazyFrame
    """
    def sar_calculation(highs, lows, closes):
        """计算SAR的numpy实现"""
        n = len(highs)
        sar = np.zeros(n)
        sar.fill(np.nan)

        if n < 2:
            return sar

        # 初始化
        ep = highs[0]  # 极点价格
        af = af_step   # 加速因子
        long = True    # 当前趋势（True=多头，False=空头）

        # 确定初始趋势
        if closes[1] > closes[0]:
            long = True
            sar[0] = lows[0]
            ep = highs[0]
        else:
            long = False
            sar[0] = highs[0]
            ep = lows[0]

        # 迭代计算
        for i in range(1, n):
            if long:
                # 多头趋势
                sar[i] = sar[i-1] + af * (ep - sar[i-1])
                # 限制SAR不超过前n周期的最低价
                if i >= 2:
                    sar[i] = max(sar[i], lows[i-1], lows[i-2])
                else:
                    sar[i] = max(sar[i], lows[i-1])

                # 检查趋势反转
                if lows[i] < sar[i]:
                    # 转为空头
                    long = False
                    sar[i] = ep
                    ep = lows[i]
                    af = af_step
                elif highs[i] > ep:
                    # 更新极点
                    ep = highs[i]
                    af = min(af + af_step, max_af)
            else:
                # 空头趋势
                sar[i] = sar[i-1] + af * (ep - sar[i-1])
                # 限制SAR不低于前n周期的最高价
                if i >= 2:
                    sar[i] = min(sar[i], highs[i-1], highs[i-2])
                else:
                    sar[i] = min(sar[i], highs[i-1])

                # 检查趋势反转
                if highs[i] > sar[i]:
                    # 转为多头
                    long = True
                    sar[i] = ep
                    ep = highs[i]
                    af = af_step
                elif lows[i] < ep:
                    # 更新极点
                    ep = lows[i]
                    af = min(af + af_step, max_af)

        return sar

    # 使用map_batches进行向量化计算
    return lazy_df.with_columns(
        pl.map_batches(
            ['high', 'low', 'close'],
            lambda cols: sar_calculation(
                cols[0].to_numpy(),
                cols[1].to_numpy(),
                cols[2].to_numpy()
            )
        ).alias('sar')
    )


def calculate_dma(lazy_df: pl.LazyFrame, short_period: int = 10, long_period: int = 50, signal_period: int = 10) -> pl.LazyFrame:
    """
    计算DMA指标（平行线差指标）
    DMA = MA(CLOSE, short_period) - MA(CLOSE, long_period)
    AMA = MA(DMA, signal_period)

    Args:
        lazy_df: Polars LazyFrame
        short_period: 短期均线周期，默认10
        long_period: 长期均线周期，默认50
        signal_period: 信号线周期，默认10

    Returns:
        pl.LazyFrame: 包含DMA和AMA指标的LazyFrame
    """
    # 计算短期和长期移动平均
    short_ma = pl.col('close').rolling_mean(window_size=short_period, min_periods=short_period)
    long_ma = pl.col('close').rolling_mean(window_size=long_period, min_periods=long_period)

    # 计算DMA
    dma = to_float32(short_ma - long_ma).alias('dma')

    # 计算AMA（DMA的移动平均）
    ama = to_float32(dma.rolling_mean(window_size=signal_period, min_periods=signal_period)).alias('ama')

    return lazy_df.with_columns([dma, ama])


def calculate_fsl(lazy_df: pl.LazyFrame) -> pl.LazyFrame:
    """
    计算FSL指标（分水岭指标）
    SWL = (最高价 + 最低价 + 收盘价) / 3
    SWS = (最高价 + 最低价 + 收盘价 + 开盘价) / 4
    
    Args:
        lazy_df: Polars LazyFrame
        
    Returns:
        pl.LazyFrame: 包含FSL指标的LazyFrame
    """
    # 计算SWL（分水岭线）
    swl = to_float32((pl.col('high') + pl.col('low') + pl.col('close')) / 3).alias('swl')
    
    # 计算SWS（分水岭线2）
    sws = to_float32((pl.col('high') + pl.col('low') + pl.col('close') + pl.col('open')) / 4).alias('sws')
    
    return lazy_df.with_columns([swl, sws])


def calculate_expma(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """
    计算EXPMA指标（指数平均线）
    EXPMA = 当日收盘价 × 2 / (N+1) + 昨日EXPMA × (N-1) / (N+1)
    
    Args:
        lazy_df: Polars LazyFrame
        windows: EXPMA计算窗口列表
        
    Returns:
        pl.LazyFrame: 包含EXPMA指标的LazyFrame
    """
    for window in windows:
        # 使用Polars的ewm_mean计算指数移动平均
        expma = to_float32(pl.col('close').ewm_mean(span=window)).alias(f'expma{window}')
        lazy_df = lazy_df.with_columns(expma)
    
    # 添加默认列名
    if len(windows) >= 1:
        window = windows[0]
        lazy_df = lazy_df.with_columns(
            pl.col(f'expma{window}').alias('expma')
        )
    
    return lazy_df


def calculate_bbi(lazy_df: pl.LazyFrame) -> pl.LazyFrame:
    """
    计算BBI指标（多空指数）
    BBI = (3日MA + 6日MA + 12日MA + 24日MA) / 4
    
    Args:
        lazy_df: Polars LazyFrame
        
    Returns:
        pl.LazyFrame: 包含BBI指标的LazyFrame
    """
    # 计算不同周期的移动平均线
    ma3 = pl.col('close').rolling_mean(window_size=3, min_periods=3)
    ma6 = pl.col('close').rolling_mean(window_size=6, min_periods=6)
    ma12 = pl.col('close').rolling_mean(window_size=12, min_periods=12)
    ma24 = pl.col('close').rolling_mean(window_size=24, min_periods=24)
    
    # 计算BBI
    bbi = to_float32((ma3 + ma6 + ma12 + ma24) / 4).alias('bbi')
    
    return lazy_df.with_columns(bbi)


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

    # 计算SAR指标
    if 'sar' in indicator_types:
        af_step = params.get('sar_af_step', 0.02)
        max_af = params.get('sar_max_af', 0.2)
        lazy_df = calculate_sar(lazy_df, af_step, max_af)

    # 计算DMA指标
    if 'dma' in indicator_types:
        short_period = params.get('dma_short_period', 10)
        long_period = params.get('dma_long_period', 50)
        signal_period = params.get('dma_signal_period', 10)
        lazy_df = calculate_dma(lazy_df, short_period, long_period, signal_period)
    
    # 计算FSL指标
    if 'fsl' in indicator_types:
        lazy_df = calculate_fsl(lazy_df)
    
    # 计算EXPMA指标
    if 'expma' in indicator_types:
        windows = params.get('expma_windows', [12, 50])
        lazy_df = calculate_expma(lazy_df, windows)
    
    # 计算BBI指标
    if 'bbi' in indicator_types:
        lazy_df = calculate_bbi(lazy_df)

    return lazy_df
