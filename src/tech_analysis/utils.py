#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
技术指标计算通用工具模块
"""

import polars as pl
from typing import List, Set, Dict, Any


def collect_used_windows(indicator_types: List[str], indicator_params: Dict[str, Dict[str, Any]]) -> Set[int]:
    """
    收集所有指标需要使用的窗口大小并去重
    
    Args:
        indicator_types: 指标类型列表
        indicator_params: 指标参数字典
        
    Returns:
        Set[int]: 去重后的窗口大小集合
    """
    all_windows = set()
    for indicator in indicator_types:
        if indicator in indicator_params:
            if 'windows' in indicator_params[indicator]:
                all_windows.update(indicator_params[indicator]['windows'])
    return all_windows


def collect_indicator_windows(indicator_types: List[str], indicator_params: Dict[str, Dict[str, Any]]) -> Dict[str, Set[int]]:
    """
    按指标类型收集需要使用的窗口大小
    
    Args:
        indicator_types: 指标类型列表
        indicator_params: 指标参数字典
        
    Returns:
        Dict[str, Set[int]]: 按指标类型分组的窗口大小字典
    """
    used_windows = {}
    
    if 'kdj' in indicator_types:
        used_windows['kdj'] = set(indicator_params['kdj']['windows'])
    if 'wr' in indicator_types:
        used_windows['wr'] = set(indicator_params['wr']['windows'])
    if 'boll' in indicator_types:
        used_windows['boll'] = set(indicator_params['boll']['windows'])
    
    return used_windows


def merge_used_windows(indicator_windows: Dict[str, Set[int]]) -> Set[int]:
    """
    合并所有指标需要使用的窗口大小
    
    Args:
        indicator_windows: 按指标类型分组的窗口大小字典
        
    Returns:
        Set[int]: 合并后的窗口大小集合
    """
    merged = set()
    for windows in indicator_windows.values():
        merged.update(windows)
    return merged


def add_default_column_names(lazy_df: pl.LazyFrame, indicator: str, window: int, columns: List[str]) -> pl.LazyFrame:
    """
    为指标添加默认列名
    
    Args:
        lazy_df: Polars LazyFrame
        indicator: 指标类型
        window: 窗口大小
        columns: 需要添加默认名的列名列表
        
    Returns:
        pl.LazyFrame: 添加了默认列名的LazyFrame
    """
    if indicator not in ['ma', 'rsi', 'kdj', 'wr', 'boll', 'macd', 'dmi', 'cci', 'roc', 'mtm', 'obv', 'vr', 'psy', 'trix', 'brar', 'asi', 'emv', 'mcst']:
        return lazy_df
    
    aliases = []
    for col in columns:
        # 根据指标类型添加不同的默认列名
        if indicator == 'kdj' and window == 14:
            if col == f'k{window}':
                aliases.append(pl.col(col).alias('k'))
            elif col == f'd{window}':
                aliases.append(pl.col(col).alias('d'))
            elif col == f'j{window}':
                aliases.append(pl.col(col).alias('j'))
        elif indicator == 'wr' and len(columns) >= 1:
            if col == columns[0]:
                aliases.append(pl.col(col).alias('wr'))
                aliases.append(pl.col(col).alias('wr1'))
            elif len(columns) >= 2 and col == columns[1]:
                aliases.append(pl.col(col).alias('wr2'))
        elif indicator == 'boll' and len(columns) >= 3:
            if col == columns[0]:
                aliases.append(pl.col(col).alias('mb'))
            elif col == columns[1]:
                aliases.append(pl.col(col).alias('up'))
            elif col == columns[2]:
                aliases.append(pl.col(col).alias('dn'))
        elif indicator in ['dmi', 'cci', 'vr', 'psy', 'trix', 'brar', 'asi', 'emv', 'mcst']:
            # 直接使用指标名作为默认列名
            aliases.append(pl.col(col).alias(col.split('_')[0] if '_' in col else col))
    
    if aliases:
        lazy_df = lazy_df.with_columns(aliases)
    
    return lazy_df


def get_indicator_params(**params: Any) -> Dict[str, Dict[str, Any]]:
    """
    获取所有指标的参数配置
    
    Args:
        **params: 指标计算参数
        
    Returns:
        Dict[str, Dict[str, Any]]: 指标参数配置字典
    """
    return {
        'ma': {'windows': params.get('windows', [5, 10, 20, 60])},
        'rsi': {'windows': params.get('rsi_windows', [14])},
        'kdj': {'windows': params.get('kdj_windows', [14])},
        'vol_ma': {'windows': params.get('vol_ma_windows', [5, 10])},
        'wr': {'windows': params.get('wr_windows', [10, 6])},
        # 同时支持旧参数名（为兼容性）和新参数名（为一致性）
        'boll': {
            'windows': params.get('boll_windows', params.get('windows', [20])),
            'std_dev': params.get('boll_std_dev', params.get('std_dev', 2.0))
        },
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


def cleanup_temp_columns(lazy_df: pl.LazyFrame, indicator_types: List[str], indicator_params: Dict[str, Dict[str, Any]]) -> pl.LazyFrame:
    """
    清理临时列
    
    Args:
        lazy_df: Polars LazyFrame
        indicator_types: 指标类型列表
        indicator_params: 指标参数字典
        
    Returns:
        pl.LazyFrame: 清理了临时列的LazyFrame
    """
    temp_cols = []
    
    # 清理KDJ临时列
    if 'kdj' in indicator_types:
        windows = indicator_params['kdj']['windows']
        temp_cols.extend([f'rsv_{window}' for window in windows])
    
    # 清理BRAR临时列
    if 'brar' in indicator_types:
        temp_cols.extend(['ar_up', 'ar_down', 'br_up', 'br_down'])
    
    # 清理MCST临时列
    if 'mcst' in indicator_types:
        temp_cols.extend(['price_volume', 'cumulative_cost', 'cumulative_volume'])
    
    # 清理共享临时列
    need_high_low = any(indicator in indicator_types for indicator in ['kdj', 'wr', 'boll'])
    if need_high_low:
        # 收集实际使用的窗口大小
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
    if temp_cols:
        lazy_df = lazy_df.drop(*temp_cols)
    
    return lazy_df


def to_float32(expr: pl.Expr) -> pl.Expr:
    """
    将表达式转换为float32类型
    
    Args:
        expr: Polars表达式
        
    Returns:
        pl.Expr: 转换为float32类型的表达式
    """
    return expr.cast(pl.Float32)


def calculate_mad(tp_series: pl.Expr, window: int) -> pl.Expr:
    """
    计算平均绝对偏差（MAD）
    
    Args:
        tp_series: 典型价格序列
        window: 窗口大小
        
    Returns:
        pl.Expr: MAD计算表达式
    """
    # 计算滚动平均值
    ma_tp_rolling = tp_series.rolling_mean(window_size=window, min_periods=1)
    # 计算绝对偏差
    abs_dev = (tp_series - ma_tp_rolling).abs()
    # 计算滚动平均绝对偏差
    return abs_dev.rolling_mean(window_size=window, min_periods=1).cast(pl.Float32)
