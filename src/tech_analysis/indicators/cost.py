#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
成本类指标计算模块
包含：CYC（成本均线）、CYS（市场盈亏）等
"""

import polars as pl
from ..utils import to_float32
from ..common_calculations import (
    add_default_columns
)


def calculate_cyc(lazy_df: pl.LazyFrame, windows: list = None) -> pl.LazyFrame:
    """
    计算CYC指标（成本均线）
    CYC = Σ(成交量 × 成交价格) / Σ(成交量)
    
    通达信版本使用分价表数据精确计算，这里使用简化版本：
    - 使用典型价格（High+Low+Close）/ 3 作为成交价格估计
    
    Args:
        lazy_df: Polars LazyFrame
        windows: CYC计算窗口列表，默认[5, 13, 34, 无穷]
        
    Returns:
        pl.LazyFrame: 包含CYC指标的LazyFrame
    """
    if windows is None:
        windows = [5, 13, 34]  # 通达信常用周期
    
    # 计算典型价格
    typical_price = (pl.col('high') + pl.col('low') + pl.col('close')) / 3
    
    # 计算成本（价格 × 成交量）
    cost = typical_price * pl.col('volume')
    
    for window in windows:
        # 计算N日累计成本和累计成交量
        cumulative_cost = cost.rolling_sum(window_size=window, min_periods=1)
        cumulative_volume = pl.col('volume').rolling_sum(window_size=window, min_periods=1)
        
        # 计算成本均线
        cyc = to_float32(
            pl.when(cumulative_volume > 0)
            .then(cumulative_cost / cumulative_volume)
            .otherwise(pl.col('close'))
        ).alias(f'cyc{window}')
        
        lazy_df = lazy_df.with_columns(cyc)
    
    # 计算无穷成本均线（CYC∞）- 使用累积值
    cumulative_cost_all = cost.cum_sum()
    cumulative_volume_all = pl.col('volume').cum_sum()
    cyc_inf = to_float32(
        pl.when(cumulative_volume_all > 0)
        .then(cumulative_cost_all / cumulative_volume_all)
        .otherwise(pl.col('close'))
    ).alias('cyc_inf')
    
    lazy_df = lazy_df.with_columns(cyc_inf)
    
    # 添加默认列名
    lazy_df = add_default_columns(lazy_df, 'cyc', windows)
    return lazy_df


def calculate_cys(lazy_df: pl.LazyFrame, cyc_window: int = 13) -> pl.LazyFrame:
    """
    计算CYS指标（市场盈亏）
    CYS = (收盘价 - CYC) / CYC × 100%
    
    反映当前价格相对于市场平均成本的位置
    CYS > 0 表示盈利，CYS < 0 表示亏损
    
    Args:
        lazy_df: Polars LazyFrame
        cyc_window: 成本均线周期，默认13日
        
    Returns:
        pl.LazyFrame: 包含CYS指标的LazyFrame
    """
    # 首先计算CYC（如果还没有计算）
    typical_price = (pl.col('high') + pl.col('low') + pl.col('close')) / 3
    cost = typical_price * pl.col('volume')
    
    cumulative_cost = cost.rolling_sum(window_size=cyc_window, min_periods=1)
    cumulative_volume = pl.col('volume').rolling_sum(window_size=cyc_window, min_periods=1)
    
    cyc = pl.when(cumulative_volume > 0).then(cumulative_cost / cumulative_volume).otherwise(pl.col('close'))
    
    # 计算CYS
    cys = to_float32(
        pl.when(cyc > 0)
        .then((pl.col('close') - cyc) / cyc * 100)
        .otherwise(0)
    ).alias('cys')
    
    # 先添加CYS列
    lazy_df = lazy_df.with_columns(cys)
    
    # 计算CYS的5日移动平均
    cys_ma5 = to_float32(pl.col('cys').rolling_mean(window_size=5, min_periods=1)).alias('cys_ma5')
    
    return lazy_df.with_columns(cys_ma5)


def calculate_cost_indicators(lazy_df: pl.LazyFrame, indicator_types: list, **params) -> pl.LazyFrame:
    """
    计算所有成本类指标
    
    Args:
        lazy_df: Polars LazyFrame
        indicator_types: 需要计算的成本类指标列表
        **params: 指标计算参数
        
    Returns:
        pl.LazyFrame: 包含所有计算成本类指标的LazyFrame
    """
    # 计算CYC指标（成本均线）
    if 'cyc' in indicator_types:
        windows = params.get('cyc_windows', [5, 13, 34])
        lazy_df = calculate_cyc(lazy_df, windows)
    
    # 计算CYS指标（市场盈亏）
    if 'cys' in indicator_types:
        cyc_window = params.get('cys_cyc_window', 13)
        lazy_df = calculate_cys(lazy_df, cyc_window)
    
    return lazy_df
