#!/usr/bin/env python3

"""
趋势类指标计算模块
包含：MA, MACD, DMI, TRIX, SAR, DMA, BBI, EXPMA, TRMA, ADX 等

# 指标计算分层说明（2026-10 梳理）
主入口：indicator_calculator.py —— TechnicalAnalyzer.calculate_indicator()
       → IndicatorRegistry.calculate_indicators() → 本文件 + oscillator/volume/...
  Polars LazyFrame 风格（df.lazy() → collect()），与 indicator_calculator.py
  的 Polars DataFrame 风格（df.with_columns(expr)）并行，同一公式两端实现。
  两者都走 Polars 内置 ewm_mean / rolling_mean / wm_mean 表达式，口径一致。

绕开注册中心的调用点（需注意）：
  chart_data_preparer.py 局部 import 本文件的 calculate_ma / calculate_dma /
  calculate_bbi，不走 IndicatorRegistry，因此拿不到 global_indicator_cache。
  这些调用量小（每张图渲染一次）、参数硬编码，暂不调整。
  后续可把 DMA / BBI 也补进 indicator_calculator.py 注册中心。

本文件保留 indicator_calculator.py 未覆盖的小众指标：
  EXPMA / TRMA / TRIX / DMA / BBI / ADX 等
"""

import numpy as np
import polars as pl
from numba import njit

from ..common_calculations import add_default_columns, calculate_moving_average
from ..utils import to_float32
from .formulas import ema_expr, macd_exprs


@njit(cache=True)
def _sar_core(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
              af_step: float, max_af: float) -> np.ndarray:
    """Numba JIT 编译的 SAR 核心迭代。

    SAR 是状态机式递推（依赖前一根 SAR / EP / AF / 趋势方向），
    无法向量化，用 njit 可把 Python 层循环降到接近 C 速度。
    """
    n = len(highs)
    sar = np.empty(n, dtype=np.float64)
    for k in range(n):
        sar[k] = np.nan

    if n < 2:
        return sar

    ep = highs[0]
    af = af_step
    long_trend = True

    if closes[1] > closes[0]:
        long_trend = True
        sar[0] = lows[0]
        ep = highs[0]
    else:
        long_trend = False
        sar[0] = highs[0]
        ep = lows[0]

    for i in range(1, n):
        if long_trend:
            sar[i] = sar[i - 1] + af * (ep - sar[i - 1])
            if i >= 2:
                limit = lows[i - 1]
                if lows[i - 2] > limit:
                    limit = lows[i - 2]
                if sar[i] < limit:
                    sar[i] = limit
            else:
                if sar[i] < lows[i - 1]:
                    sar[i] = lows[i - 1]

            if lows[i] < sar[i]:
                long_trend = False
                sar[i] = ep
                ep = lows[i]
                af = af_step
            elif highs[i] > ep:
                ep = highs[i]
                af += af_step
                if af > max_af:
                    af = max_af
        else:
            sar[i] = sar[i - 1] + af * (ep - sar[i - 1])
            if i >= 2:
                limit = highs[i - 1]
                if highs[i - 2] < limit:
                    limit = highs[i - 2]
                if sar[i] > limit:
                    sar[i] = limit
            else:
                if sar[i] > highs[i - 1]:
                    sar[i] = highs[i - 1]

            if highs[i] > sar[i]:
                long_trend = True
                sar[i] = ep
                ep = highs[i]
                af = af_step
            elif lows[i] < ep:
                ep = lows[i]
                af += af_step
                if af > max_af:
                    af = max_af

    return sar


def calculate_ma(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """
    计算移动平均线指标

    Args:
        lazy_df: Polars LazyFrame
        windows: 移动平均窗口列表

    Returns:
        pl.LazyFrame: 包含移动平均线的LazyFrame
    """
    # 使用通用移动平均计算函数
    lazy_df = calculate_moving_average(lazy_df, 'close', windows)
    # 添加默认列名
    lazy_df = add_default_columns(lazy_df, 'ma', windows)
    return lazy_df


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
    # 统一走共享公式层（标准 EMA 口径），不创建中间列
    exprs = macd_exprs('close', fast_period, slow_period, signal_period)

    macd_line = to_float32(exprs['macd']).alias('macd')
    macd_signal = to_float32(exprs['signal']).alias('macd_signal')
    macd_hist = to_float32(exprs['hist']).alias('macd_hist')

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
        tr_sma = tr.rolling_sum(window_size=window, min_samples=1)
        pdm_sma = plus_dm.rolling_sum(window_size=window, min_samples=1)
        ndm_sma = minus_dm.rolling_sum(window_size=window, min_samples=1)

        # 计算+DI和-DI
        pdi = to_float32(pdm_sma / tr_sma * 100).alias(f'pdi_{window}')
        ndi = to_float32(ndm_sma / tr_sma * 100).alias(f'ndi_{window}')

        # 计算DX
        dx = to_float32(pl.when((pdi + ndi) == 0).then(0.0).otherwise((pdi - ndi).abs() / (pdi + ndi) * 100)).alias(f'dx_{window}')

        # 计算ADX
        adx = to_float32(dx.rolling_mean(window_size=window, min_samples=1)).alias(f'adx_{window}')

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
        ema1 = ema_expr(pl.col('close'), window)
        # 2. 第二次指数平滑（EMA2）
        ema2 = ema_expr(ema1, window)
        # 3. 第三次指数平滑（EMA3）
        ema3 = ema_expr(ema2, window)
        # 4. 计算EMA3的变化率（TRIX = (EMA3 - EMA3.shift(1)) / EMA3.shift(1) * 100）
        trix_value = (ema3 - ema3.shift(1)) / ema3.shift(1) * 100
        trix = to_float32(trix_value).alias(f'trix{window}')
        # 5. 计算TRIX的信号线（TRMA = TRIX的signal_period天EMA）
        trma = to_float32(ema_expr(trix_value, signal_period)).alias(f'trma{window}')

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
        """委托给 Numba JIT 编译的 _sar_core 执行。"""
        return _sar_core(
            np.ascontiguousarray(highs, dtype=np.float64),
            np.ascontiguousarray(lows, dtype=np.float64),
            np.ascontiguousarray(closes, dtype=np.float64),
            af_step,
            max_af,
        )

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
    # 计算短期和长期移动平均，min_samples=1确保从第一个数据点开始计算
    short_ma = pl.col('close').rolling_mean(window_size=short_period, min_samples=1)
    long_ma = pl.col('close').rolling_mean(window_size=long_period, min_samples=1)

    # 计算DMA
    dma = to_float32(short_ma - long_ma).alias('dma')

    # 计算AMA（DMA的移动平均），min_samples=1确保从第一个数据点开始计算
    ama = to_float32(dma.rolling_mean(window_size=signal_period, min_samples=1)).alias('ama')

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
        # 统一走共享公式层的标准 EMA 口径
        expma = to_float32(ema_expr(pl.col('close'), window)).alias(f'expma{window}')
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
    # 计算不同周期的移动平均线，min_samples=1确保从第一个数据点开始计算
    ma3 = pl.col('close').rolling_mean(window_size=3, min_samples=1)
    ma6 = pl.col('close').rolling_mean(window_size=6, min_samples=1)
    ma12 = pl.col('close').rolling_mean(window_size=12, min_samples=1)
    ma24 = pl.col('close').rolling_mean(window_size=24, min_samples=1)

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
