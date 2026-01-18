#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
技术分析器，用于计算各种技术指标
"""

# 第三方库导入
import os
from typing import List, Optional, Any
import polars as pl
import pandas as pd
from loguru import logger

# 内部模块导入
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
    generate_cache_key
)


class TechnicalAnalyzer:
    """
    技术分析器类，提供各种技术指标的计算方法
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
            'wr': self.calculate_wr
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
        # 检查是否已经计算过MACD指标
        if not self.calculated_indicators['macd']:
            # 使用Polars计算MACD指标
            self.pl_df = calculate_macd_polars(self.pl_df, fast_period, slow_period, signal_period)
            
            # 更新计算状态
            self.calculated_indicators['macd'] = True
            
            # 清除转换缓存，因为数据已更新
            self._pandas_cache = None
            self._pandas_cache_hash = None
        
        # 返回转换后的Pandas DataFrame
        return self._ensure_pandas_df()
    
    def calculate_kdj(self, windows=14, parallel=False):
        """
        计算KDJ指标
        
        Args:
            windows: KDJ计算窗口，支持单个窗口或窗口列表
            parallel: 是否使用并行计算
            
        Returns:
            pd.DataFrame: 包含KDJ指标的DataFrame
        """
        # 确保windows是列表
        if not isinstance(windows, list):
            windows = [windows]
        
        # 只计算尚未计算过的窗口
        windows_to_calculate = [w for w in windows if w not in self.calculated_indicators['kdj']]
        
        if windows_to_calculate:
            # 批量计算KDJ指标（Polars已内部优化）
            self.pl_df = calculate_kdj_polars(self.pl_df, windows_to_calculate)
            
            # 更新计算状态
            for window in windows_to_calculate:
                self.calculated_indicators['kdj'].add(window)
            
            # 清除转换缓存，因为数据已更新
            self._pandas_cache = None
            self._pandas_cache_hash = None
        
        # 返回转换后的Pandas DataFrame
        return self._ensure_pandas_df()
    
    def calculate_boll(self, windows=[20], std_dev=2.0, parallel=False):
        """
        计算Boll指标（布林带）
        
        Args:
            windows: Boll计算窗口，支持单个窗口或窗口列表
            std_dev: 标准差倍数，默认为2.0
            parallel: 是否使用并行计算
            
        Returns:
            pd.DataFrame: 包含Boll指标的DataFrame
        """
        # 确保windows是列表
        if not isinstance(windows, list):
            windows = [windows]
        
        # 只计算尚未计算过的窗口
        windows_to_calculate = [w for w in windows if w not in self.calculated_indicators['boll']]
        
        if windows_to_calculate:
            # 批量计算Boll指标（Polars已内部优化）
            self.pl_df = calculate_boll_polars(self.pl_df, windows_to_calculate, std_dev)
            
            # 更新计算状态
            for window in windows_to_calculate:
                self.calculated_indicators['boll'].add(window)
            
            # 清除转换缓存，因为数据已更新
            self._pandas_cache = None
            self._pandas_cache_hash = None
        
        # 返回转换后的Pandas DataFrame
        return self._ensure_pandas_df()
    
    def calculate_wr(self, windows=None, parallel=False):
        """
        计算WR指标（威廉指标）
        
        Args:
            windows: WR计算窗口，支持单个窗口或窗口列表，默认[10, 6]（通达信风格）
            parallel: 是否使用并行计算
            
        Returns:
            pd.DataFrame: 包含WR指标的DataFrame
        """
        # 通达信默认使用WR10和WR6
        if windows is None:
            windows = [10, 6]
        # 确保windows是列表
        elif not isinstance(windows, list):
            windows = [windows]
        
        # 只计算尚未计算过的窗口
        windows_to_calculate = [w for w in windows if w not in self.calculated_indicators['wr']]
        
        if windows_to_calculate:
            # 批量计算WR指标（Polars已内部优化）
            self.pl_df = calculate_wr_polars(self.pl_df, windows_to_calculate)
            
            # 更新计算状态
            for window in windows_to_calculate:
                self.calculated_indicators['wr'].add(window)
            
            # 清除转换缓存，因为数据已更新
            self._pandas_cache = None
            self._pandas_cache_hash = None
        
        # 返回转换后的Pandas DataFrame
        return self._ensure_pandas_df()
    
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
        
        if indicator_type not in self.calculated_indicators:
            return False
        
        if indicator_type in ['ma', 'rsi', 'kdj', 'vol_ma']:
            return window in self.calculated_indicators[indicator_type]
        elif indicator_type in ['macd']:
            return self.calculated_indicators[indicator_type]
        
        return False
    
    def reset_calculation(self, indicator_type=None, window=None):
        """
        重置指标计算状态，可选择重置特定指标或所有指标，使用批量操作优化
        
        Args:
            indicator_type: 要重置的指标类型，None表示重置所有指标
            window: 对于需要窗口的指标，指定要重置的窗口，None表示重置该类型的所有窗口
        """
        # 先收集需要删除的列，再统一处理，减少对共享数据的修改次数
        columns_to_drop = []
        
        if not indicator_type:
            # 重置所有指标
            # 重新初始化计算状态字典，避免直接修改共享数据
            self.calculated_indicators = {
                'ma': set(),
                'macd': False,
                'rsi': set(),
                'kdj': set(),
                'vol_ma': set(),
                'boll': set(),
                'plugin': set()
            }
            # 重置缓存
            self._calculate_cache = {}
            # 重置插件指标映射
            self._init_plugin_indicator_mapping()
            # 重置数据哈希
            self._data_hash = self._calculate_polars_data_hash()
            
            # 重置Polars DataFrame，只保留原始列
            original_columns = ['open', 'high', 'low', 'close', 'volume']
            self.pl_df = self.pl_df.select(original_columns)
            
            # 清除转换缓存
            self._pandas_cache = None
            self._pandas_cache_hash = None
        elif self.plugin_manager and indicator_type in self.plugin_manager.get_available_indicator_plugins():
            # 重置特定插件指标
            if indicator_type in self.calculated_indicators['plugin']:
                self.calculated_indicators['plugin'].remove(indicator_type)
                # 对于插件指标，目前无法自动确定要删除的列，由插件自行管理
                # 从缓存中删除
                cache_key = self._generate_cache_key(indicator_type)
                if cache_key in self._calculate_cache:
                    del self._calculate_cache[cache_key]
                # 清除转换缓存
                self._pandas_cache = None
                self._pandas_cache_hash = None
        elif indicator_type in self.calculated_indicators:
            if indicator_type in ['ma', 'rsi', 'kdj', 'vol_ma', 'boll', 'wr']:
                if window:
                    # 重置特定窗口
                    self.calculated_indicators[indicator_type].discard(window)
                    # 准备删除的列名
                    if indicator_type == 'vol_ma':
                        column_name = f'vol_ma{window}'
                        columns_to_drop.append(column_name)
                    elif indicator_type == 'boll':
                        # Boll指标需要删除中轨线、上轨线和下轨线
                        columns_to_drop.extend([f'mb{window}', f'up{window}', f'dn{window}'])
                    else:
                        column_name = f'{indicator_type}{window}'
                        columns_to_drop.append(column_name)
                    # 从缓存中删除
                    cache_key = self._generate_cache_key(indicator_type, window)
                    if cache_key in self._calculate_cache:
                        del self._calculate_cache[cache_key]
                else:
                    # 重置该类型的所有窗口
                    windows = list(self.calculated_indicators[indicator_type])
                    for w in windows:
                        if indicator_type == 'vol_ma':
                            column_name = f'vol_ma{w}'
                            columns_to_drop.append(column_name)
                        elif indicator_type == 'boll':
                            # Boll指标需要删除中轨线、上轨线和下轨线
                            columns_to_drop.extend([f'mb{w}', f'up{w}', f'dn{w}'])
                        else:
                            column_name = f'{indicator_type}{w}'
                            columns_to_drop.append(column_name)
                        # 从缓存中删除
                        cache_key = self._generate_cache_key(indicator_type, w)
                        if cache_key in self._calculate_cache:
                            del self._calculate_cache[cache_key]
                    # 清空该类型的所有窗口
                    self.calculated_indicators[indicator_type].clear()
                    # 对于Boll指标，还需要删除默认列名
                    if indicator_type == 'boll':
                        columns_to_drop.extend(['mb', 'up', 'dn'])
                    # 对于WR指标，还需要删除默认列名
                    elif indicator_type == 'wr':
                        columns_to_drop.extend(['wr'])
            elif indicator_type in ['macd']:
                # 重置MACD指标
                self.calculated_indicators[indicator_type] = False
                # 准备删除的MACD相关列
                columns_to_drop.extend(['macd', 'macd_signal', 'macd_hist'])
                # 从缓存中删除
                cache_key = self._generate_cache_key(indicator_type)
                if cache_key in self._calculate_cache:
                    del self._calculate_cache[cache_key]
            elif indicator_type == 'plugin':
                # 重置所有插件指标
                self.calculated_indicators['plugin'].clear()
                # 对于插件指标，目前无法自动确定要删除的列，由插件自行管理
                # 从缓存中删除所有插件指标
                for cache_key in list(self._calculate_cache.keys()):
                    if cache_key.startswith('plugin_'):
                        del self._calculate_cache[cache_key]
        
        # 统一删除需要删除的列
        if columns_to_drop:
            # 从Polars DataFrame中删除列
            columns_to_drop_existing = [col for col in columns_to_drop if col in self.pl_df.columns]
            if columns_to_drop_existing:
                self.pl_df = self.pl_df.drop(columns_to_drop_existing)
                # 清除转换缓存
                self._pandas_cache = None
                self._pandas_cache_hash = None
    
    def get_calculated_indicators(self):
        """
        获取已计算的指标信息
        
        Returns:
            dict: 包含已计算指标类型和窗口的字典
        """
        return self.calculated_indicators.copy()
    
    def calculate_ma(self, windows=[5, 10, 20, 60], parallel=False):
        """
        计算移动平均线
        
        Args:
            windows: 移动平均窗口列表
            parallel: 是否使用并行计算
            
        Returns:
            pd.DataFrame: 包含移动平均线的DataFrame
        """
        # 只计算尚未计算过的窗口
        windows_to_calculate = [w for w in windows if w not in self.calculated_indicators['ma']]
        
        if windows_to_calculate:
            # 使用Polars计算移动平均线，利用其内置并行能力
            self.pl_df = calculate_ma_polars(self.pl_df, windows_to_calculate)
            
            # 更新计算状态
            for window in windows_to_calculate:
                self.calculated_indicators['ma'].add(window)
            
            # 清除转换缓存，因为数据已更新
            self._pandas_cache = None
            self._pandas_cache_hash = None
        
        # 返回转换后的Pandas DataFrame
        return self._ensure_pandas_df()
    
    def calculate_rsi(self, windows=14, parallel=False):
        """
        计算RSI指标
        
        Args:
            windows: RSI计算窗口，支持单个窗口或窗口列表
            parallel: 是否使用并行计算
            
        Returns:
            pd.DataFrame: 包含RSI指标的DataFrame
        """
        # 确保windows是列表
        if not isinstance(windows, list):
            windows = [windows]
        
        # 只计算尚未计算过的窗口
        windows_to_calculate = [w for w in windows if w not in self.calculated_indicators['rsi']]
        
        if windows_to_calculate:
            # 批量计算RSI指标（Polars已内部优化）
            self.pl_df = calculate_rsi_polars(self.pl_df, windows_to_calculate)
            
            # 更新计算状态
            for window in windows_to_calculate:
                self.calculated_indicators['rsi'].add(window)
            
            # 清除转换缓存，因为数据已更新
            self._pandas_cache = None
            self._pandas_cache_hash = None
        
        # 返回转换后的Pandas DataFrame
        return self._ensure_pandas_df()
    
    def calculate_vol_ma(self, windows=[5, 10], parallel=False):
        """
        计算成交量移动平均线
        
        Args:
            windows: 移动平均窗口列表
            parallel: 是否使用并行计算
            
        Returns:
            pd.DataFrame: 包含成交量移动平均线的DataFrame
        """
        # 只计算尚未计算过的窗口
        windows_to_calculate = [w for w in windows if w not in self.calculated_indicators['vol_ma']]
        
        if windows_to_calculate:
            # 使用Polars计算成交量移动平均线，利用其内置并行能力
            self.pl_df = calculate_vol_ma_polars(self.pl_df, windows_to_calculate)
            
            # 更新计算状态
            for window in windows_to_calculate:
                self.calculated_indicators['vol_ma'].add(window)
            
            # 清除转换缓存，因为数据已更新
            self._pandas_cache = None
            self._pandas_cache_hash = None
        
        # 返回转换后的Pandas DataFrame
        return self._ensure_pandas_df()
    
    def _init_plugin_indicator_mapping(self):
        """
        初始化插件指标映射
        """
        if not self.plugin_manager:
            return
        
        # 获取所有可用的指标插件
        available_indicators = self.plugin_manager.get_available_indicator_plugins()
        
        # 为每个指标插件创建对应的计算方法
        for plugin_name, plugin in available_indicators.items():
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
                    
                    # 更新计算状态
                    self.calculated_indicators['plugin'].add(plugin_name)
                    # 清除转换缓存
                    self._pandas_cache = None
                    self._pandas_cache_hash = None
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
                    
                    # 更新计算状态
                    self.calculated_indicators['plugin'].add(plugin_name)
                    # 清除转换缓存
                    self._pandas_cache = None
                    self._pandas_cache_hash = None
        except Exception as e:
            raise RuntimeError(f"计算插件指标{plugin_name}失败: {str(e)}")
        
        return self._ensure_pandas_df()
    
    def calculate_indicator_parallel(self, indicator_type, *args, **kwargs):
        """
        并行计算特定类型的指标
        
        Args:
            indicator_type: 指标类型，如'ma', 'macd', 'rsi', 'kdj', 'vol_ma'或插件名称
            *args: 传递给指标计算方法的位置参数
            **kwargs: 传递给指标计算方法的关键字参数，包括parallel和max_workers等
            - parallel: 是否使用并行计算，默认True
            - max_workers: 最大工作线程数，默认None（使用系统默认值）
            - batch_size: 批量处理大小，默认None
        
        Returns:
            pd.DataFrame: 包含计算指标的DataFrame
        """
        # 检查指标类型是否支持并行计算
        if indicator_type in ['ma', 'rsi', 'kdj', 'vol_ma']:
            # 对于支持多窗口的指标，使用并行计算不同窗口
            windows = kwargs.get('windows', [14])
            if not isinstance(windows, list):
                windows = [windows]
            
            # 只计算尚未计算过的窗口
            windows_to_calculate = [w for w in windows if w not in self.calculated_indicators[indicator_type]]
            
            if windows_to_calculate:
                # 处理参数，但不再使用线程池
                kwargs.pop('parallel', True)
                kwargs.pop('max_workers', None)
                
                # 直接调用指标计算函数，利用Polars内置并行
                if indicator_type == 'ma':
                    self.pl_df = calculate_ma_polars(self.pl_df, windows_to_calculate)
                elif indicator_type == 'rsi':
                    self.pl_df = calculate_rsi_polars(self.pl_df, windows_to_calculate)
                elif indicator_type == 'kdj':
                    self.pl_df = calculate_kdj_polars(self.pl_df, windows_to_calculate)
                elif indicator_type == 'vol_ma':
                    self.pl_df = calculate_vol_ma_polars(self.pl_df, windows_to_calculate)
                
                # 更新计算状态
                for window in windows_to_calculate:
                    self.calculated_indicators[indicator_type].add(window)
                
                # 清除转换缓存，因为数据已更新
                self._pandas_cache = None
                self._pandas_cache_hash = None
        else:
            # 对于不支持多窗口的指标（如MACD）或插件指标，调用常规计算方法
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
        # 移除parallel参数，因为Polars会自动处理并行
        kwargs.pop('parallel', None)
        
        if indicator_type not in self.indicator_mapping:
            # 检查是否为插件指标
            if self.plugin_manager and indicator_type in self.plugin_manager.get_available_indicator_plugins():
                # 调用插件指标计算方法
                return self.calculate_plugin_indicator(indicator_type, **kwargs)
            raise ValueError(f"不支持的指标类型: {indicator_type}")
        
        # 调用相应的指标计算方法，利用Polars内置并行
        return self.indicator_mapping[indicator_type](*args, **kwargs)
    
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
        # 移除不再需要的并行相关参数
        kwargs.pop('parallel', None)
        kwargs.pop('max_workers', None)
        
        # 直接串行调用，利用Polars内部并行计算
        for indicator_type in indicator_types:
            try:
                if indicator_type == 'macd':
                    # MACD不需要windows参数，创建新的kwargs副本并移除windows
                    macd_kwargs = kwargs.copy()
                    macd_kwargs.pop('windows', None)
                    self.calculate_indicator(indicator_type, *args, **macd_kwargs)
                else:
                    self.calculate_indicator(indicator_type, *args, **kwargs)
            except Exception as e:
                logger.error(f"计算指标{indicator_type}失败: {e}")
        
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
    
    def calculate_all_indicators(self, parallel=False):
        """
        计算所有支持的技术指标，包括内置指标和插件指标
        使用多指标批量计算框架，充分利用Polars的Lazy API和并行计算能力
        
        Args:
            parallel: 是否使用并行计算（目前已由Polars内部处理）
            
        Returns:
            pd.DataFrame: 包含所有指标的数据
        """
        # 1. 收集所有需要计算的指标和参数，优化检查逻辑
        indicator_tasks = {
            'ma': {'windows': [5, 10, 20, 60]},
            'rsi': {'windows': [14]},
            'kdj': {'windows': [14]},
            'vol_ma': {'windows': [5, 10]},
            'wr': {'windows': [10, 6]},
            'macd': {}
        }
        
        # 2. 使用Lazy API构建多指标批量计算查询
        lazy_df = self.pl_df.lazy()
        indicators_updated = False
        
        # 批量计算MA指标
        ma_windows = [w for w in indicator_tasks['ma']['windows'] if w not in self.calculated_indicators['ma']]
        if ma_windows:
            lazy_df = calculate_ma_polars(lazy_df, ma_windows)
            self.calculated_indicators['ma'].update(ma_windows)
            indicators_updated = True
        
        # 批量计算VOL_MA指标
        vol_ma_windows = [w for w in indicator_tasks['vol_ma']['windows'] if w not in self.calculated_indicators['vol_ma']]
        if vol_ma_windows:
            lazy_df = calculate_vol_ma_polars(lazy_df, vol_ma_windows)
            self.calculated_indicators['vol_ma'].update(vol_ma_windows)
            indicators_updated = True
        
        # 计算MACD指标
        if not self.calculated_indicators['macd']:
            lazy_df = calculate_macd_polars(lazy_df)
            self.calculated_indicators['macd'] = True
            indicators_updated = True
        
        # 批量计算KDJ指标
        kdj_windows = [w for w in indicator_tasks['kdj']['windows'] if w not in self.calculated_indicators['kdj']]
        if kdj_windows:
            lazy_df = calculate_kdj_polars(lazy_df, kdj_windows)
            self.calculated_indicators['kdj'].update(kdj_windows)
            indicators_updated = True
        
        # 批量计算WR指标
        wr_windows = [w for w in indicator_tasks['wr']['windows'] if w not in self.calculated_indicators['wr']]
        if wr_windows:
            lazy_df = calculate_wr_polars(lazy_df, wr_windows)
            self.calculated_indicators['wr'].update(wr_windows)
            indicators_updated = True
        
        # 批量计算RSI指标
        rsi_windows = [w for w in indicator_tasks['rsi']['windows'] if w not in self.calculated_indicators['rsi']]
        if rsi_windows:
            lazy_df = calculate_rsi_polars(lazy_df, rsi_windows)
            self.calculated_indicators['rsi'].update(rsi_windows)
            indicators_updated = True
        
        # 2.2 执行批量计算
        if indicators_updated:
            # 执行计算并更新主DataFrame
            self.pl_df = lazy_df.collect()
            
            # 3. 清除转换缓存，因为数据已更新
            self._pandas_cache = None
            self._pandas_cache_hash = None
        
        # 4. 计算所有插件指标
        plugin_indicators = self.get_available_plugin_indicators()
        if plugin_indicators:
            for plugin_name in plugin_indicators:
                try:
                    # 直接调用插件计算，不传递parallel参数（由Polars内部处理）
                    self.calculate_plugin_indicator(plugin_name)
                except Exception as e:
                    logger.error("计算插件指标" + plugin_name + "时发生错误: " + str(e))
        
        # 返回转换后的Pandas DataFrame，只有在必要时才转换
        return self._ensure_pandas_df()

