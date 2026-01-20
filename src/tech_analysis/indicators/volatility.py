#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
波动率类指标计算模块
包含：BOLL, WR, BRAR, ASI, EMV, MCST等
"""

import polars as pl
from ..utils import to_float32


def calculate_boll(lazy_df: pl.LazyFrame, windows: list, std_dev: float) -> pl.LazyFrame:
    """
    计算Boll指标（布林带）
    
    Args:
        lazy_df: Polars LazyFrame
        windows: Boll计算窗口列表
        std_dev: 标准差倍数
        
    Returns:
        pl.LazyFrame: 包含Boll指标的LazyFrame
    """
    # 批量计算所有窗口的Boll指标
    for window in windows:
        # 计算移动平均线（中轨线）
        mb_expr = to_float32(pl.col('close').rolling_mean(window_size=window, min_periods=1)).alias(f'mb{window}')
        # 计算上轨线和下轨线
        up_expr = to_float32(mb_expr + to_float32(pl.col('close').rolling_std(window_size=window, min_periods=1)) * std_dev).alias(f'up{window}')
        dn_expr = to_float32(mb_expr - to_float32(pl.col('close').rolling_std(window_size=window, min_periods=1)) * std_dev).alias(f'dn{window}')
        
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
    
    return lazy_df


def calculate_wr(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """
    计算WR指标（威廉指标）
    
    Args:
        lazy_df: Polars LazyFrame
        windows: WR计算窗口列表
        
    Returns:
        pl.LazyFrame: 包含WR指标的LazyFrame
    """
    for window in windows:
        lazy_df = lazy_df.with_columns(
            to_float32(((pl.col(f'high_n_{window}') - pl.col('close')) / 
             (pl.col(f'high_n_{window}') - pl.col(f'low_n_{window}')) * 100)).alias(f'wr{window}')
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
    
    return lazy_df


def calculate_brar(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """
    计算BRAR指标（情绪指标）
    
    Args:
        lazy_df: Polars LazyFrame
        windows: BRAR计算窗口列表
        
    Returns:
        pl.LazyFrame: 包含BRAR指标的LazyFrame
    """
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
        ar_expr = to_float32((pl.col('ar_up').rolling_sum(window_size=window, min_periods=1) / (pl.col('ar_down').rolling_sum(window_size=window, min_periods=1) + 0.0001)) * 100).alias(f'ar{window}')
        # 计算BR = (br_up的N日和 / br_down的N日和) * 100
        br_expr = to_float32((pl.col('br_up').rolling_sum(window_size=window, min_periods=1) / (pl.col('br_down').rolling_sum(window_size=window, min_periods=1) + 0.0001)) * 100).alias(f'br{window}')

        # 添加到DataFrame
        lazy_df = lazy_df.with_columns([ar_expr, br_expr])

    # 4. 添加默认列名
    if len(windows) >= 1:
        window = windows[0]
        lazy_df = lazy_df.with_columns(
            pl.col(f'ar{window}').alias('ar'),
            pl.col(f'br{window}').alias('br')
        )
    
    return lazy_df


def calculate_asi(lazy_df: pl.LazyFrame, signal_period: int) -> pl.LazyFrame:
    """
    计算ASI指标（振动升降指标）
    
    Args:
        lazy_df: Polars LazyFrame
        signal_period: ASI信号线周期
        
    Returns:
        pl.LazyFrame: 包含ASI指标的LazyFrame
    """
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
    asi = to_float32(pl.when(tr != 0).then((asi_numerator / tr * 16)).otherwise(0.0)).alias('asi')

    # 6. 计算ASI的信号线（ASI_SIG = ASI的signal_period天MA）
    asi_sig = asi.rolling_mean(window_size=signal_period, min_periods=1).alias('asi_sig')

    # 7. 添加到DataFrame
    return lazy_df.with_columns([asi, asi_sig])


def calculate_emv(lazy_df: pl.LazyFrame, windows: list, constant: float) -> pl.LazyFrame:
    """
    计算EMV指标（简易波动指标）
    
    Args:
        lazy_df: Polars LazyFrame
        windows: EMV计算窗口列表
        constant: 常数，用于调整EMV的数值大小
        
    Returns:
        pl.LazyFrame: 包含EMV指标的LazyFrame
    """
    # 1. 直接计算EMV基础值，避免中间变量和列名冲突
    # 距离差值：(最高价 + 最低价)/2 - 前一日(最高价 + 最低价)/2
    distance_diff = ((pl.col('high') + pl.col('low')) / 2) - ((pl.col('high').shift(1) + pl.col('low').shift(1)) / 2)

    # 2. 计算EMV基础值（直接在表达式中处理除以零的情况）
    # EMV = (距离差值 / (成交量 / (最高价 - 最低价))) * 常数，当最高价等于最低价时为0
    emv_base_expr = to_float32(pl.when(pl.col('high') != pl.col('low')).then((distance_diff / (pl.col('volume') / (pl.col('high') - pl.col('low'))) * constant)).otherwise(0.0))

    # 3. 计算各窗口的EMV移动平均线
    for window in windows:
        emv_expr = to_float32(emv_base_expr.rolling_mean(window_size=window, min_periods=1)).alias(f'emv{window}')
        lazy_df = lazy_df.with_columns(emv_expr)

    # 4. 添加默认列名
    if len(windows) >= 1:
        window = windows[0]
        lazy_df = lazy_df.with_columns(
            pl.col(f'emv{window}').alias('emv')
        )
    
    return lazy_df


def calculate_mcst(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """
    计算MCST指标（市场成本）
    
    Args:
        lazy_df: Polars LazyFrame
        windows: MCST计算窗口列表
        
    Returns:
        pl.LazyFrame: 包含MCST指标的LazyFrame
    """
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
    mcst = to_float32(pl.when(cumulative_volume != 0).then(cumulative_cost / cumulative_volume).otherwise(pl.lit(0.0))).alias('mcst')
    
    # 4. 添加MCST值到DataFrame
    lazy_df = lazy_df.with_columns(mcst)
    
    # 5. 计算各窗口的MCST移动平均线
    for window in windows:
        mcst_ma_expr = to_float32(mcst.rolling_mean(window_size=window, min_periods=1)).alias(f'mcst_ma{window}')
        lazy_df = lazy_df.with_columns(mcst_ma_expr)
    
    return lazy_df


def calculate_volatility_indicators(lazy_df: pl.LazyFrame, indicator_types: list, **params) -> pl.LazyFrame:
    """
    计算所有波动率类指标
    
    Args:
        lazy_df: Polars LazyFrame
        indicator_types: 需要计算的波动率类指标列表
        **params: 指标计算参数
        
    Returns:
        pl.LazyFrame: 包含所有计算波动率类指标的LazyFrame
    """
    # 计算BOLL指标
    if 'boll' in indicator_types:
        windows = params.get('boll_windows', params.get('windows', [20]))
        std_dev = params.get('boll_std_dev', params.get('std_dev', 2.0))
        lazy_df = calculate_boll(lazy_df, windows, std_dev)
    
    # 计算WR指标
    if 'wr' in indicator_types:
        windows = params.get('wr_windows', [10, 6])
        lazy_df = calculate_wr(lazy_df, windows)
    
    # 计算BRAR指标
    if 'brar' in indicator_types:
        windows = params.get('brar_windows', [26])
        lazy_df = calculate_brar(lazy_df, windows)
    
    # 计算ASI指标
    if 'asi' in indicator_types:
        signal_period = params.get('asi_signal_period', 20)
        lazy_df = calculate_asi(lazy_df, signal_period)
    
    # 计算EMV指标
    if 'emv' in indicator_types:
        windows = params.get('emv_windows', [14])
        constant = params.get('emv_constant', 100000000)
        lazy_df = calculate_emv(lazy_df, windows, constant)
    
    # 计算MCST指标
    if 'mcst' in indicator_types:
        windows = params.get('mcst_windows', [12])
        lazy_df = calculate_mcst(lazy_df, windows)
    
    return lazy_df
