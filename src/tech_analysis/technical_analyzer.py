#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
技术分析器，用于计算各种技术指标
"""

# 第三方库导入
import os
from typing import List, Optional, Any, Dict, Union
import polars as pl
import pandas as pd
from loguru import logger

# 内部模块导入
from src.api.business_api import ITechnicalAnalyzer
from src.utils.event_bus import publish, subscribe, EventType
from .indicator_calculator import (
    calculate_ma_polars,
    calculate_vol_ma_polars,
    calculate_macd_polars,
    calculate_rsi_polars,
    calculate_kdj_polars,
    calculate_boll_polars,
    calculate_wr_polars,
    preprocess_data_polars,
    sample_data_polars,
    calculate_multiple_indicators_polars,
    generate_cache_key
)
from .indicator_manager import global_indicator_manager


class TechnicalAnalyzer(ITechnicalAnalyzer):
    """
    技术分析器类，提供各种技术指标的计算方法
    实现了ITechnicalAnalyzer接口
    """
    
    def __init__(self, data, plugin_manager=None):
        """
        初始化技术分析器
        
        Args:
            data: 股票数据，可以是Polars DataFrame或Pandas DataFrame
            plugin_manager: 插件管理器实例，用于加载和使用指标插件
        """
        # 只保存Polars DataFrame作为主要数据结构
        self.pl_df = None
        
        # 按需转换并缓存Pandas DataFrame
        self._pandas_cache = None
        self._pandas_cache_hash = None
        
        if hasattr(data, 'to_pandas'):
            # 输入是Polars DataFrame
            self.pl_df = data
        elif isinstance(data, pd.DataFrame):
            # 输入是Pandas DataFrame，转换为Polars
            self.pl_df = pl.from_pandas(data)
        else:
            # 输入是其他格式，转换为Polars
            self.pl_df = pl.DataFrame(data)
        
        # 使用Polars进行数据预处理
        self._preprocess_data_polars()
        
        # 保存插件管理器实例
        self.plugin_manager = plugin_manager
        
        # 添加指标计算状态跟踪
        self.calculated_indicators = {
            'ma': set(),  # 已计算的MA窗口
            'macd': False,
            'rsi': set(),  # 已计算的RSI窗口
            'kdj': set(),  # 已计算的KDJ窗口
            'vol_ma': set(),  # 已计算的成交量MA窗口
            'boll': set(),  # 已计算的Boll窗口
            'wr': set(),  # 已计算的WR窗口
            'dmi': set(),  # 已计算的DMI窗口
            'cci': set(),  # 已计算的CCI窗口
            'roc': set(),  # 已计算的ROC窗口
            'mtm': set(),  # 已计算的MTM窗口
            'obv': False,  # 已计算的OBV
            'vr': set(),  # 已计算的VR窗口
            'psy': set(),  # 已计算的PSY窗口
            'trix': set(),  # 已计算的TRIX窗口
            'brar': set(),  # 已计算的BRAR窗口
            'asi': set(),  # 已计算的ASI窗口
            'emv': set(),  # 已计算的EMV窗口
            'mcst': set(),  # 已计算的MCST窗口
            'plugin': set()  # 已计算的插件指标
        }
        
        # 添加缓存机制，避免重复计算
        self._calculate_cache = {}
        # 使用Polars原生方法计算数据哈希
        self._data_hash = self._calculate_polars_data_hash()
        
        # 初始化指标映射，便于统一管理
        self.indicator_mapping = {
            'ma': self.calculate_ma,
            'macd': self.calculate_macd,
            'rsi': self.calculate_rsi,
            'kdj': self.calculate_kdj,
            'vol_ma': self.calculate_vol_ma,
            'boll': self.calculate_boll,
            'wr': self.calculate_wr,
            'dmi': self.calculate_indicator_parallel,
            'cci': self.calculate_indicator_parallel,
            'roc': self.calculate_indicator_parallel,
            'mtm': self.calculate_indicator_parallel,
            'obv': self.calculate_indicator_parallel,
            'vr': self.calculate_indicator_parallel,
            'psy': self.calculate_indicator_parallel,
            'trix': self.calculate_indicator_parallel,
            'brar': self.calculate_indicator_parallel,
            'asi': self.calculate_indicator_parallel,
            'emv': self.calculate_indicator_parallel,
            'mcst': self.calculate_indicator_parallel,
            'dma': self.calculate_indicator_parallel,
            'fsl': self.calculate_indicator_parallel,
            'sar': self.calculate_indicator_parallel,
            'vol_tdx': self.calculate_indicator_parallel,
            'cr': self.calculate_indicator_parallel
        }
        
        # 初始化插件指标映射
        self._init_plugin_indicator_mapping()
    
    def _calculate_polars_data_hash(self):
        """
        使用Polars 1.36.1 API计算数据哈希，修复agg方法错误
        
        Returns:
            int: 唯一的数据哈希值
        """
        # 只计算关键列的摘要，避免转换为numpy数组
        # 在Polars 1.36.1中，DataFrame没有agg方法，使用select配合聚合函数
        key_cols = ['open', 'high', 'low', 'close', 'volume']
        
        # 计算关键列的统计量：均值、标准差、最小值、最大值、数量
        # 使用select方法配合聚合函数，这是Polars 1.36.1支持的API
        stats = self.pl_df.select(
            [pl.col(col).mean().alias(f'{col}_mean') for col in key_cols] +
            [pl.col(col).std().alias(f'{col}_std') for col in key_cols] +
            [pl.col(col).min().alias(f'{col}_min') for col in key_cols] +
            [pl.col(col).max().alias(f'{col}_max') for col in key_cols] +
            [pl.col(col).count().alias(f'{col}_count') for col in key_cols]
        )
        
        # 将统计结果转换为字符串进行哈希
        stats_str = str(stats.to_dicts()[0])
        return hash(stats_str)
    
    def _preprocess_data_polars(self):
        """
        使用Polars进行数据预处理
        - 检查必要列是否存在
        - 转换为数值类型
        - 处理缺失值
        """
        self.pl_df = preprocess_data_polars(self.pl_df)
    
    def _generate_cache_key(self, indicator_type, *args, **kwargs):
        """
        生成唯一的缓存键，基于数据哈希和指标参数
        
        Args:
            indicator_type: 指标类型
            *args: 位置参数
            **kwargs: 关键字参数
            
        Returns:
            int: 唯一的缓存键
        """
        # 优化缓存键生成，使用更高效的哈希组合
        # 对kwargs进行排序，确保相同参数不同顺序生成相同的键
        sorted_kwargs = tuple(sorted(kwargs.items()))
        # 组合所有参数生成唯一哈希
        return hash((self._data_hash, indicator_type, args, sorted_kwargs))
    
    def _clear_pandas_cache(self):
        """
        清除Pandas DataFrame缓存
        """
        self._pandas_cache = None
        self._pandas_cache_hash = None
    
    def _calculate_window_based_indicator(self, indicator_type, calc_func, windows):
        """
        通用窗口指标计算方法
        
        Args:
            indicator_type: 指标类型
            calc_func: 具体的计算函数
            windows: 窗口列表
            
        Returns:
            pd.DataFrame: 包含计算结果的DataFrame
        """
        # 确保windows是列表
        if not isinstance(windows, list):
            windows = [windows]
        
        # 只计算尚未计算过的窗口
        windows_to_calculate = [w for w in windows if w not in self.calculated_indicators[indicator_type]]
        
        if windows_to_calculate:
            # 调用具体的计算函数
            self.pl_df = calc_func(self.pl_df, windows_to_calculate)
            
            # 更新计算状态
            self.calculated_indicators[indicator_type].update(windows_to_calculate)
            
            # 清除转换缓存
            self._clear_pandas_cache()
        
        return self._ensure_pandas_df()
    
    def _calculate_simple_indicator(self, indicator_type, calc_func, **kwargs):
        """
        通用非窗口指标计算方法
        
        Args:
            indicator_type: 指标类型
            calc_func: 具体的计算函数
            **kwargs: 计算参数
            
        Returns:
            pd.DataFrame: 包含计算结果的DataFrame
        """
        # 检查是否已经计算过该指标
        if not self.calculated_indicators[indicator_type]:
            # 调用具体的计算函数
            self.pl_df = calc_func(self.pl_df, **kwargs)
            
            # 更新计算状态
            self.calculated_indicators[indicator_type] = True
            
            # 清除转换缓存
            self._clear_pandas_cache()
        
        return self._ensure_pandas_df()
    
    def _ensure_pandas_df(self):
        """
        确保pandas DataFrame已初始化，仅在需要时转换，并缓存结果
        
        Returns:
            pd.DataFrame: 转换后的Pandas DataFrame
        """
        if self._pandas_cache is None or self._data_hash != self._pandas_cache_hash:
            self._pandas_cache = self.pl_df.to_pandas()
            self._pandas_cache_hash = self._data_hash
        return self._pandas_cache
    
    def calculate_macd(self, fast_period=12, slow_period=26, signal_period=9):
        """
        计算MACD指标
        
        Args:
            fast_period: 快速EMA周期
            slow_period: 慢速EMA周期
            signal_period: 信号线EMA周期
            
        Returns:
            pd.DataFrame: 包含MACD指标的DataFrame
        """
        return self._calculate_simple_indicator('macd', calculate_macd_polars, fast_period=fast_period, slow_period=slow_period, signal_period=signal_period)
    
    def calculate_kdj(self, windows=14):
        """
        计算KDJ指标
        
        Args:
            windows: KDJ计算窗口，支持单个窗口或窗口列表
            
        Returns:
            pd.DataFrame: 包含KDJ指标的DataFrame
        """
        return self._calculate_window_based_indicator('kdj', calculate_kdj_polars, windows)
    
    def calculate_boll(self, windows=[20], std_dev=2.0):
        """
        计算Boll指标（布林带）
        
        Args:
            windows: Boll计算窗口，支持单个窗口或窗口列表
            std_dev: 标准差倍数，默认为2.0
            
        Returns:
            pd.DataFrame: 包含Boll指标的DataFrame
        """
        # 确保windows是列表
        if not isinstance(windows, list):
            windows = [windows]
        
        # 只计算尚未计算过的窗口
        windows_to_calculate = [w for w in windows if w not in self.calculated_indicators['boll']]
        
        if windows_to_calculate:
            # 计算Boll指标
            self.pl_df = calculate_boll_polars(self.pl_df, windows_to_calculate, std_dev)
            
            # 更新计算状态
            self.calculated_indicators['boll'].update(windows_to_calculate)
            
            # 清除转换缓存
            self._clear_pandas_cache()
        
        return self._ensure_pandas_df()
    
    def calculate_wr(self, windows=None):
        """
        计算WR指标（威廉指标）
        
        Args:
            windows: WR计算窗口，支持单个窗口或窗口列表，默认[10, 6]（通达信风格）
            
        Returns:
            pd.DataFrame: 包含WR指标的DataFrame
        """
        # 通达信默认使用WR10和WR6
        if windows is None:
            windows = [10, 6]
        
        return self._calculate_window_based_indicator('wr', calculate_wr_polars, windows)
    
    def sample_data(self, target_points=1000, strategy='uniform', return_polars=False):
        """
        对数据进行采样，减少数据量，提高图表渲染速度
        
        Args:
            target_points: 目标采样点数
            strategy: 采样策略，可选值：'uniform'（均匀采样）、'time_weighted'（时间加权采样）
            return_polars: 是否返回Polars DataFrame
            
        Returns:
            pd.DataFrame或pl.DataFrame: 采样后的数据
        """
        current_data = self.get_data(return_polars=True)
        sampled_data = sample_data_polars(current_data, target_points, strategy)
        return sampled_data if return_polars else sampled_data.to_pandas()
    
    def get_data(self, return_polars=False, sample=False, sample_params=None):
        """
        获取包含所有计算指标的数据
        
        Args:
            return_polars: 是否返回Polars DataFrame，默认返回pandas DataFrame
            sample: 是否对数据进行采样
            sample_params: 采样参数，字典类型，包含target_points和strategy
            
        Returns:
            pd.DataFrame或pl.DataFrame: 包含所有指标的数据
        """
        if return_polars:
            data = self.pl_df
        else:
            data = self._ensure_pandas_df()
        
        if sample:
            sample_params = sample_params or {}
            target_points = sample_params.get('target_points', 1000)
            strategy = sample_params.get('strategy', 'uniform')
            data = self.sample_data(target_points=target_points, strategy=strategy, return_polars=return_polars)
        
        return data
    
    def is_indicator_calculated(self, indicator_type, window=None):
        """
        检查特定指标是否已经计算
        
        Args:
            indicator_type: 指标类型，如'ma', 'macd', 'rsi', 'kdj', 'vol_ma'或插件名称
            window: 对于需要窗口的指标，指定窗口大小
            
        Returns:
            bool: 如果指标已计算返回True，否则返回False
        """
        # 检查是否为插件指标
        if self.plugin_manager and indicator_type in self.plugin_manager.get_available_indicator_plugins():
            return indicator_type in self.calculated_indicators['plugin']
        
        # 检查指标类型是否存在
        if indicator_type not in self.calculated_indicators:
            return False
        
        # 获取指标计算状态
        status = self.calculated_indicators[indicator_type]
        
        # 根据状态类型判断
        if isinstance(status, set):
            # 窗口型指标，需要检查窗口是否已计算
            return window is not None and window in status
        else:
            # 非窗口型指标，直接返回状态值
            return status
    
    def reset_calculation(self, indicator_type=None, window=None):
        """
        重置指标计算状态，可选择重置特定指标或所有指标
        
        Args:
            indicator_type: 要重置的指标类型，None表示重置所有指标
            window: 对于需要窗口的指标，指定要重置的窗口，None表示重置该类型的所有窗口
        """
        def _reset_all_indicators():
            """重置所有指标"""
            # 重新初始化计算状态字典
            self.calculated_indicators = {
                'ma': set(),
                'macd': False,
                'rsi': set(),
                'kdj': set(),
                'vol_ma': set(),
                'boll': set(),
                'wr': set(),
                'dmi': set(),
                'cci': set(),
                'roc': set(),
                'mtm': set(),
                'obv': False,
                'vr': set(),
                'psy': set(),
                'trix': set(),
                'brar': set(),
                'asi': set(),
                'emv': set(),
                'mcst': set(),
                'plugin': set()
            }
            # 重置缓存
            self._calculate_cache.clear()
            # 重置插件指标映射
            self._init_plugin_indicator_mapping()
            # 重置数据哈希
            self._data_hash = self._calculate_polars_data_hash()
            
            # 重置Polars DataFrame，只保留原始列
            original_columns = ['open', 'high', 'low', 'close', 'volume']
            self.pl_df = self.pl_df.select(original_columns)
            
            # 清除转换缓存
            self._clear_pandas_cache()
        
        def _reset_plugin_indicator(plugin_name):
            """重置特定插件指标"""
            if plugin_name in self.calculated_indicators['plugin']:
                self.calculated_indicators['plugin'].remove(plugin_name)
                # 从缓存中删除
                cache_key = self._generate_cache_key(plugin_name)
                self._calculate_cache.pop(cache_key, None)
                self._clear_pandas_cache()
        
        def _reset_window_based_indicator(ind_type, win):
            """重置基于窗口的指标"""
            columns_to_drop = []
            
            if win:
                # 重置特定窗口
                self.calculated_indicators[ind_type].discard(win)
                # 准备删除的列名
                if ind_type == 'vol_ma':
                    columns_to_drop = [f'vol_ma{win}']
                elif ind_type == 'boll':
                    columns_to_drop = [f'mb{win}', f'up{win}', f'dn{win}']
                else:
                    columns_to_drop = [f'{ind_type}{win}']
                # 从缓存中删除
                cache_key = self._generate_cache_key(ind_type, win)
                self._calculate_cache.pop(cache_key, None)
            else:
                # 重置该类型的所有窗口
                windows = list(self.calculated_indicators[ind_type])
                for w in windows:
                    if ind_type == 'vol_ma':
                        columns_to_drop.append(f'vol_ma{w}')
                    elif ind_type == 'boll':
                        columns_to_drop.extend([f'mb{w}', f'up{w}', f'dn{w}'])
                    else:
                        columns_to_drop.append(f'{ind_type}{w}')
                    # 从缓存中删除
                    cache_key = self._generate_cache_key(ind_type, w)
                    self._calculate_cache.pop(cache_key, None)
                # 清空该类型的所有窗口
                self.calculated_indicators[ind_type].clear()
                # 删除默认列名
                if ind_type == 'boll':
                    columns_to_drop.extend(['mb', 'up', 'dn'])
                elif ind_type == 'wr':
                    columns_to_drop.extend(['wr'])
            
            # 统一删除需要删除的列
            if columns_to_drop:
                columns_to_drop_existing = [col for col in columns_to_drop if col in self.pl_df.columns]
                if columns_to_drop_existing:
                    self.pl_df = self.pl_df.drop(columns_to_drop_existing)
                    self._clear_pandas_cache()
        
        def _reset_simple_indicator(ind_type):
            """重置简单指标"""
            self.calculated_indicators[ind_type] = False
            # 准备删除的列名
            if ind_type == 'macd':
                columns_to_drop = ['macd', 'macd_signal', 'macd_hist']
            elif ind_type == 'obv':
                columns_to_drop = ['obv']
            else:
                columns_to_drop = [ind_type]
            
            # 从缓存中删除
            cache_key = self._generate_cache_key(ind_type)
            self._calculate_cache.pop(cache_key, None)
            
            # 统一删除需要删除的列
            columns_to_drop_existing = [col for col in columns_to_drop if col in self.pl_df.columns]
            if columns_to_drop_existing:
                self.pl_df = self.pl_df.drop(columns_to_drop_existing)
                self._clear_pandas_cache()
        
        # 主逻辑
        if not indicator_type:
            # 重置所有指标
            _reset_all_indicators()
        elif self.plugin_manager and indicator_type in self.plugin_manager.get_available_indicator_plugins():
            # 重置特定插件指标
            _reset_plugin_indicator(indicator_type)
        elif indicator_type in self.calculated_indicators:
            if indicator_type in ['ma', 'rsi', 'kdj', 'vol_ma', 'boll', 'wr', 'dmi', 'cci', 'roc', 'mtm', 'vr', 'psy', 'trix', 'brar', 'asi', 'emv', 'mcst']:
                # 重置基于窗口的指标
                _reset_window_based_indicator(indicator_type, window)
            elif indicator_type in ['macd', 'obv']:
                # 重置简单指标
                _reset_simple_indicator(indicator_type)
            elif indicator_type == 'plugin':
                # 重置所有插件指标
                self.calculated_indicators['plugin'].clear()
                # 从缓存中删除所有插件指标
                for cache_key in list(self._calculate_cache.keys()):
                    if cache_key.startswith('plugin_'):
                        del self._calculate_cache[cache_key]
                self._clear_pandas_cache()
    
    def get_calculated_indicators(self):
        """
        获取已计算的指标信息
        
        Returns:
            dict: 包含已计算指标类型和窗口的字典
        """
        return self.calculated_indicators.copy()
    
    def calculate_ma(self, windows=[5, 10, 20, 60]):
        """
        计算移动平均线
        
        Args:
            windows: 移动平均窗口列表
            
        Returns:
            pd.DataFrame: 包含移动平均线的DataFrame
        """
        return self._calculate_window_based_indicator('ma', calculate_ma_polars, windows)
    
    def calculate_rsi(self, windows=14):
        """
        计算RSI指标
        
        Args:
            windows: RSI计算窗口，支持单个窗口或窗口列表
            
        Returns:
            pd.DataFrame: 包含RSI指标的DataFrame
        """
        return self._calculate_window_based_indicator('rsi', calculate_rsi_polars, windows)
    
    def calculate_vol_ma(self, windows=[5, 10]):
        """
        计算成交量移动平均线
        
        Args:
            windows: 移动平均窗口列表
            
        Returns:
            pd.DataFrame: 包含成交量移动平均线的DataFrame
        """
        return self._calculate_window_based_indicator('vol_ma', calculate_vol_ma_polars, windows)
    
    def _init_plugin_indicator_mapping(self):
        """
        初始化插件指标映射
        """
        if not self.plugin_manager:
            return
        
        # 获取所有可用的指标插件
        available_indicators = self.plugin_manager.get_available_indicator_plugins()
        
        # 为每个指标插件创建对应的计算方法
        for plugin_name in available_indicators:
            # 动态添加指标映射
            self.indicator_mapping[plugin_name] = lambda *args, plugin_name=plugin_name, **kwargs: self.calculate_plugin_indicator(plugin_name, *args, **kwargs)
    
    def get_available_plugin_indicators(self):
        """
        获取可用的插件指标列表
        
        Returns:
            list: 可用插件指标名称列表
        """
        if not self.plugin_manager:
            return []
        
        return list(self.plugin_manager.get_available_indicator_plugins().keys())
    
    def calculate_plugin_indicator(self, plugin_name, **kwargs):
        """
        计算插件指标
        
        Args:
            plugin_name: 插件名称
            **kwargs: 传递给插件calculate方法的参数
            
        Returns:
            pd.DataFrame: 包含插件指标的DataFrame
        """
        if not self.plugin_manager:
            raise ValueError("插件管理器未初始化")
        
        # 获取指标插件实例
        indicator_plugins = self.plugin_manager.get_available_indicator_plugins()
        if plugin_name not in indicator_plugins:
            raise ValueError(f"指标插件{plugin_name}不存在或未启用")
        
        plugin = indicator_plugins[plugin_name]
        
        # 检查插件指标是否已经计算
        if plugin_name in self.calculated_indicators['plugin']:
            return self._ensure_pandas_df()
        
        try:
            # 检查插件是否支持polars
            if hasattr(plugin, 'supports_polars') and plugin.supports_polars():
                # 直接传递polars DataFrame给插件
                result_pl = plugin.calculate_polars(self.pl_df, **kwargs)
                
                # 合并结果到主Polars DataFrame，只添加新列
                if result_pl is not None and hasattr(result_pl, 'columns'):
                    new_columns = [col for col in result_pl.columns if col not in self.pl_df.columns]
                    if new_columns:
                        self.pl_df = self.pl_df.with_columns(
                            *[result_pl[col].alias(col) for col in new_columns]
                        )
                    
                    # 更新计算状态和清除缓存
                    self.calculated_indicators['plugin'].add(plugin_name)
                    self._clear_pandas_cache()
            else:
                # 旧插件，使用pandas DataFrame
                # 只在必要时转换为Pandas DataFrame
                df_pd = self._ensure_pandas_df()
                
                # 调用插件的calculate方法
                result_df = plugin.calculate(df_pd, **kwargs)
                
                # 将结果转换回Polars并合并
                if result_df is not None and isinstance(result_df, pd.DataFrame):
                    # 转换为Polars
                    result_pl = pl.from_pandas(result_df)
                    # 合并到主Polars DataFrame，只添加新列
                    new_columns = [col for col in result_pl.columns if col not in self.pl_df.columns]
                    if new_columns:
                        self.pl_df = self.pl_df.with_columns(
                            *[result_pl[col].alias(col) for col in new_columns]
                        )
                    
                    # 更新计算状态和清除缓存
                    self.calculated_indicators['plugin'].add(plugin_name)
                    self._clear_pandas_cache()
        except Exception as e:
            raise RuntimeError(f"计算插件指标{plugin_name}失败: {str(e)}")
        
        return self._ensure_pandas_df()
    
    def calculate_indicator_parallel(self, indicator_type, *args, **kwargs):
        """
        计算特定类型的指标，利用Polars内置并行计算能力
        
        Args:
            indicator_type: 指标类型，如'ma', 'macd', 'rsi', 'kdj', 'vol_ma'或插件名称
            *args: 传递给指标计算方法的位置参数
            **kwargs: 传递给指标计算方法的关键字参数
        
        Returns:
            pd.DataFrame: 包含计算指标的DataFrame
        """
        # 检查指标类型是否支持多窗口计算
        if indicator_type in ['ma', 'rsi', 'kdj', 'vol_ma', 'dmi', 'cci', 'roc', 'mtm', 'vr', 'psy', 'trix', 'brar', 'asi', 'emv', 'mcst']:
            # 对于支持多窗口的指标
            windows = kwargs.get('windows', [14])
            if not isinstance(windows, list):
                windows = [windows]
            
            # 只计算尚未计算过的窗口
            windows_to_calculate = [w for w in windows if w not in self.calculated_indicators[indicator_type]]
            
            if windows_to_calculate:
                # 直接调用指标计算函数，利用Polars内置并行
                if indicator_type == 'ma':
                    self.pl_df = calculate_ma_polars(self.pl_df, windows_to_calculate)
                elif indicator_type == 'rsi':
                    self.pl_df = calculate_rsi_polars(self.pl_df, windows_to_calculate)
                elif indicator_type == 'kdj':
                    self.pl_df = calculate_kdj_polars(self.pl_df, windows_to_calculate)
                elif indicator_type == 'vol_ma':
                    self.pl_df = calculate_vol_ma_polars(self.pl_df, windows_to_calculate)
                # 使用批量计算函数处理所有新指标
                else:
                    # 使用calculate_multiple_indicators_polars处理所有其他指标
                    lazy_df = self.pl_df.lazy()
                    lazy_df = calculate_multiple_indicators_polars(lazy_df, [indicator_type], windows=windows_to_calculate)
                    self.pl_df = lazy_df.collect()
                
                # 更新计算状态
                self.calculated_indicators[indicator_type].update(windows_to_calculate)
                
                # 清除转换缓存
                self._clear_pandas_cache()
        elif indicator_type in ['macd', 'obv']:
            # 对于不支持多窗口的指标，直接使用批量计算函数
            lazy_df = self.pl_df.lazy()
            lazy_df = calculate_multiple_indicators_polars(lazy_df, [indicator_type])
            self.pl_df = lazy_df.collect()
            
            # 更新计算状态
            self.calculated_indicators[indicator_type] = True
            
            # 清除转换缓存
            self._clear_pandas_cache()
        else:
            # 对于插件指标，调用常规计算方法
            self.calculate_indicator(indicator_type, *args, **kwargs)
        
        # 返回转换后的Pandas DataFrame
        return self._ensure_pandas_df()
    
    def calculate_indicator(self, indicator_type, *args, **kwargs):
        """
        计算特定类型的指标
        
        Args:
            indicator_type: 指标类型，如'ma', 'macd', 'rsi', 'kdj', 'vol_ma'或插件名称
            *args: 传递给指标计算方法的位置参数
            **kwargs: 传递给指标计算方法的关键字参数
        
        Returns:
            pd.DataFrame: 包含计算指标的DataFrame
        """
        # 检查是否为插件指标
        if self.plugin_manager and indicator_type in self.plugin_manager.get_available_indicator_plugins():
            # 调用插件指标计算方法
            return self.calculate_plugin_indicator(indicator_type, **kwargs)
        
        # 使用指标管理器计算内置指标
        try:
            # 使用指标管理器计算指标
            result_df = global_indicator_manager.calculate_indicator(
                self.pl_df, indicator_type, return_polars=True, **kwargs
            )
            
            # 更新内部DataFrame
            self.pl_df = result_df
            
            # 清除转换缓存
            self._clear_pandas_cache()
            
            # 更新计算状态
            if indicator_type in self.calculated_indicators:
                if isinstance(self.calculated_indicators[indicator_type], set):
                    windows = kwargs.get('windows', [14])
                    if not isinstance(windows, list):
                        windows = [windows]
                    self.calculated_indicators[indicator_type].update(windows)
                else:
                    self.calculated_indicators[indicator_type] = True
            
            return self._ensure_pandas_df()
        except ValueError as e:
            raise ValueError(f"不支持的指标类型: {indicator_type}") from e
    
    def calculate_indicators_parallel(self, indicator_types, *args, **kwargs):
        """
        计算多个指标类型，利用Polars内置并行计算能力
        
        Args:
            indicator_types: 指标类型列表，如['ma', 'macd', 'rsi', 'kdj', 'vol_ma']或插件名称列表
            *args: 传递给指标计算方法的位置参数
            **kwargs: 传递给指标计算方法的关键字参数
        
        Returns:
            pd.DataFrame: 包含所有计算指标的DataFrame
        """
        # 分离内置指标和插件指标
        builtin_indicators = []
        plugin_indicators = []
        
        for indicator_type in indicator_types:
            if self.plugin_manager and indicator_type in self.plugin_manager.get_available_indicator_plugins():
                plugin_indicators.append(indicator_type)
            else:
                builtin_indicators.append(indicator_type)
        
        # 批量计算内置指标
        if builtin_indicators:
            try:
                # 使用指标管理器批量计算内置指标
                result_df = global_indicator_manager.calculate_indicators(
                    self.pl_df, builtin_indicators, return_polars=True, **kwargs
                )
                
                # 更新内部DataFrame
                self.pl_df = result_df
                
                # 清除转换缓存
                self._clear_pandas_cache()
                
                # 更新计算状态
                for indicator_type in builtin_indicators:
                    if indicator_type in self.calculated_indicators:
                        if isinstance(self.calculated_indicators[indicator_type], set):
                            windows = kwargs.get(f'{indicator_type}_windows', kwargs.get('windows', [14]))
                            if not isinstance(windows, list):
                                windows = [windows]
                            self.calculated_indicators[indicator_type].update(windows)
                        else:
                            self.calculated_indicators[indicator_type] = True
            except Exception as e:
                logger.error(f"计算内置指标失败: {e}")
        
        # 计算插件指标
        for plugin_name in plugin_indicators:
            try:
                self.calculate_plugin_indicator(plugin_name, **kwargs)
            except Exception as e:
                logger.error(f"计算插件指标{plugin_name}失败: {e}")
        
        return self._ensure_pandas_df()
    
    def calculate_plugin_indicators_parallel(self, plugin_names, *args, **kwargs):
        """
        计算多个插件指标，利用Polars内置并行计算能力
        
        Args:
            plugin_names: 插件名称列表
            *args: 传递给插件计算方法的位置参数
            **kwargs: 传递给插件计算方法的关键字参数
        
        Returns:
            pd.DataFrame: 包含所有计算结果的DataFrame
        """
        if not plugin_names:
            plugin_names = self.get_available_plugin_indicators()
        
        # 直接串行调用，利用Polars内部并行计算
        for plugin_name in plugin_names:
            try:
                self.calculate_plugin_indicator(plugin_name, *args, **kwargs)
            except Exception as e:
                logger.error(f"计算插件指标{plugin_name}失败: {e}")
        
        return self._ensure_pandas_df()
    
    def calculate_all_indicators(self, data: Optional[Union[pl.DataFrame, pd.DataFrame]] = None, indicator_types: Optional[List[str]] = None, **params) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        计算多个技术指标
        使用多指标批量计算框架，充分利用Polars的Lazy API和并行计算能力

        Args:
            data: 股票数据，None表示使用现有数据
            indicator_types: 指标类型列表，默认计算所有指标
            **params: 指标计算参数

        Returns:
            pl.DataFrame或pd.DataFrame: 包含所有计算结果的数据
        """
        # 保存复权列，避免在指标计算过程中丢失
        adj_columns = ['qfq_open', 'qfq_high', 'qfq_low', 'qfq_close',
                       'hfq_open', 'hfq_high', 'hfq_low', 'hfq_close',
                       'qfq_factor', 'hfq_factor']
        adj_data = {}

        # 如果传入了新数据，更新内部数据
        if data is not None:
            if hasattr(data, 'to_pandas'):
                # 输入是Polars DataFrame
                self.pl_df = data
            elif isinstance(data, pd.DataFrame):
                # 输入是Pandas DataFrame，转换为Polars
                self.pl_df = pl.from_pandas(data)
            else:
                # 输入是其他格式，转换为Polars
                self.pl_df = pl.DataFrame(data)

            # 保存复权列数据
            for col in adj_columns:
                if col in self.pl_df.columns:
                    adj_data[col] = self.pl_df[col]

            # 使用Polars进行数据预处理
            self._preprocess_data_polars()

            # 重新计算数据哈希
            self._data_hash = self._calculate_polars_data_hash()
        
        # 1. 确定要计算的指标类型
        if indicator_types is None:
            # 默认计算所有指标
            indicator_types = ['ma', 'rsi', 'kdj', 'vol_ma', 'wr', 'macd', 'dmi', 'cci', 'roc', 'mtm', 'obv', 'vr', 'psy', 'trix', 'brar', 'asi', 'emv', 'mcst']
        
        # 2. 准备需要计算的内置指标类型
        builtin_indicators = [ind for ind in indicator_types if ind in ['ma', 'rsi', 'kdj', 'vol_ma', 'wr', 'macd', 'dmi', 'cci', 'roc', 'mtm', 'obv', 'vr', 'psy', 'trix', 'brar', 'asi', 'emv', 'mcst']]
        
        # 3. 使用新的批量计算函数进行内置指标计算
        indicators_updated = False
        success = True
        error_message = ""
        
        if builtin_indicators:
            try:
                # 使用新的批量计算函数，将所有指标计算合并到单个查询计划
                lazy_df = self.pl_df.lazy()
                lazy_df = calculate_multiple_indicators_polars(lazy_df, builtin_indicators, **params)
                
                # 执行计算并更新主DataFrame
                self.pl_df = lazy_df.collect()
                
                # 4. 更新计算状态
                # 使用字典映射替代长if-elif链，减少重复代码
                indicator_update_map = {
                    'ma': {'param_key': 'windows', 'default': [5, 10, 20, 60], 'update_type': 'windows'},
                    'rsi': {'param_key': 'rsi_windows', 'default': [14], 'update_type': 'windows'},
                    'kdj': {'param_key': 'kdj_windows', 'default': [14], 'update_type': 'windows'},
                    'vol_ma': {'param_key': 'vol_ma_windows', 'default': [5, 10], 'update_type': 'windows'},
                    'wr': {'param_key': 'wr_windows', 'default': [10, 6], 'update_type': 'windows'},
                    'macd': {'update_type': 'boolean'},
                    'dmi': {'param_key': 'windows', 'default': [14], 'update_type': 'windows'},
                    'cci': {'param_key': 'windows', 'default': [14], 'update_type': 'windows'},
                    'roc': {'param_key': 'windows', 'default': [12], 'update_type': 'windows'},
                    'mtm': {'param_key': 'windows', 'default': [12], 'update_type': 'windows'},
                    'obv': {'update_type': 'boolean'},
                    'vr': {'param_key': 'windows', 'default': [24], 'update_type': 'windows'},
                    'psy': {'param_key': 'windows', 'default': [12], 'update_type': 'windows'},
                    'trix': {'param_key': 'windows', 'default': [12], 'update_type': 'windows'},
                    'brar': {'param_key': 'windows', 'default': [26], 'update_type': 'windows'},
                    'asi': {'param_key': 'windows', 'default': [14], 'update_type': 'windows'},
                    'emv': {'param_key': 'windows', 'default': [14], 'update_type': 'windows'},
                    'mcst': {'param_key': 'windows', 'default': [12], 'update_type': 'windows'}
                }
                
                for indicator in builtin_indicators:
                    if indicator in indicator_update_map:
                        update_info = indicator_update_map[indicator]
                        if update_info['update_type'] == 'windows':
                            windows = params.get(update_info['param_key'], update_info['default'])
                            self.calculated_indicators[indicator].update(windows)
                        elif update_info['update_type'] == 'boolean':
                            self.calculated_indicators[indicator] = True
                
                # 5. 清除转换缓存，因为数据已更新
                self._pandas_cache = None
                self._pandas_cache_hash = None
                indicators_updated = True
            except Exception as e:
                success = False
                error_message = f"计算内置指标失败: {str(e)}"
                logger.error(error_message)
                # 发布错误事件
                publish(
                    EventType.INDICATOR_ERROR,
                    data_type='stock',
                    indicators=builtin_indicators,
                    error=error_message
                )
        
        # 6. 计算插件指标
        plugin_indicators = self.get_available_plugin_indicators()
        for plugin_name in plugin_indicators:
            if indicator_types is None or plugin_name in indicator_types:
                try:
                    # 直接调用插件计算
                    self.calculate_plugin_indicator(plugin_name, **params)
                except Exception as e:
                    logger.error(f"计算插件指标{plugin_name}时发生错误: {str(e)}")
        
        # 7. 恢复复权列到结果中
        if adj_data:
            try:
                # 构建需要添加的列表达式列表
                columns_to_add = []
                for col, values in adj_data.items():
                    if col not in self.pl_df.columns:
                        columns_to_add.append(values.alias(col))
                
                # 一次性添加所有缺失的复权列
                if columns_to_add:
                    self.pl_df = self.pl_df.with_columns(columns_to_add)
                    logger.debug(f"恢复复权列: {list(adj_data.keys())}")
            except Exception as e:
                logger.warning(f"恢复复权列时出错: {e}")

        # 8. 发布指标计算完成事件
        publish(
            EventType.INDICATOR_CALCULATED,
            data_type='stock',
            indicators=indicator_types,
            calculated_indicators=self.calculated_indicators.copy(),
            success=success,
            error=error_message if not success else None
        )

        # 返回结果，根据参数决定返回类型
        return_polars = params.get('return_polars', False)
        return self.pl_df if return_polars else self._ensure_pandas_df()
    
    def get_supported_indicators(self) -> List[str]:
        """
        获取支持的技术指标列表
        
        Returns:
            List[str]: 支持的技术指标列表
        """
        # 内置指标
        builtin_indicators = ['ma', 'rsi', 'kdj', 'vol_ma', 'wr', 'macd', 'dmi', 'cci', 'roc', 'mtm', 'obv', 'vr', 'psy', 'trix', 'brar', 'asi', 'emv', 'mcst']
        
        # 插件指标
        plugin_indicators = self.get_available_plugin_indicators()
        
        return builtin_indicators + plugin_indicators
    
    def is_indicator_supported(self, indicator_type: str) -> bool:
        """
        检查是否支持指定的技术指标
        
        Args:
            indicator_type: 指标类型
        
        Returns:
            bool: 是否支持
        """
        supported_indicators = self.get_supported_indicators()
        return indicator_type in supported_indicators
    
    def clear_calculation_cache(self, indicator_type: Optional[str] = None) -> bool:
        """
        清除指标计算缓存
        
        Args:
            indicator_type: 指标类型，None表示清除所有指标缓存
        
        Returns:
            bool: 清除是否成功
        """
        try:
            if indicator_type:
                # 清除特定指标的缓存
                if indicator_type in self._calculate_cache:
                    del self._calculate_cache[indicator_type]
                # 重置计算状态
                self.reset_calculation(indicator_type)
            else:
                # 清除所有缓存
                self._calculate_cache.clear()
                # 重置所有计算状态
                self.reset_calculation()
            return True
        except Exception as e:
            logger.error(f"清除缓存失败: {str(e)}")
            return False

