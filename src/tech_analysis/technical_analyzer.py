#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
技术分析器，用于计算各种技术指标
"""

import pandas as pd
import numpy as np
import ta
import concurrent.futures
from functools import partial
import polars as pl

from .indicator_calculator import (
    calculate_ma_polars,
    calculate_vol_ma_polars,
    calculate_macd_polars,
    calculate_rsi_polars,
    calculate_kdj_polars,
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
        # 保存原始Polars DataFrame（如果输入是Polars格式）
        self.pl_df = None
        # 仅在必要时转换为Pandas DataFrame以便使用ta库
        self.df = None
        
        if hasattr(data, 'to_pandas'):
            # 输入是Polars DataFrame
            self.pl_df = data
            # 暂不转换，按需转换
            self.df = None
        elif isinstance(data, pd.DataFrame):
            # 输入是Pandas DataFrame
            self.df = data
            # 转换为Polars以便进行高性能处理
            self.pl_df = pl.from_pandas(data)
        else:
            # 输入是其他格式，转换为Polars
            self.pl_df = pl.DataFrame(data)
            # 暂不转换为Pandas
            self.df = None
        
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
            'plugin': set()  # 已计算的插件指标
        }
        
        # 添加缓存机制，避免重复计算
        self._calculate_cache = {}
        # 数据哈希，用于检测数据变化
        self._data_hash = hash(self.pl_df.to_pandas().values.tobytes())
        
        # 初始化指标映射，便于统一管理
        self.indicator_mapping = {
            'ma': self.calculate_ma,
            'macd': self.calculate_macd,
            'rsi': self.calculate_rsi,
            'kdj': self.calculate_kdj,
            'vol_ma': self.calculate_vol_ma
        }
        
        # 初始化插件指标映射
        self._init_plugin_indicator_mapping()
    
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
        生成唯一的缓存键
        
        Args:
            indicator_type: 指标类型
            *args: 位置参数
            **kwargs: 关键字参数
            
        Returns:
            int: 唯一的缓存键
        """
        # 使用工具函数生成缓存键
        return generate_cache_key(self._data_hash, indicator_type, *args, **kwargs)
    
    def _ensure_pandas_df(self):
        """
        确保pandas DataFrame已初始化，仅在需要时转换
        """
        if self.df is None and self.pl_df is not None:
            # 仅在需要时转换为pandas DataFrame
            self.df = self.pl_df.to_pandas()
    
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
            
            # 如果pandas DataFrame已初始化，需要同步更新
            if self.df is not None:
                # 只转换新添加的MACD列
                new_macd_cols = ['macd', 'macd_signal', 'macd_hist']
                new_cols_df = self.pl_df.select(new_macd_cols).to_pandas()
                self.df = pd.concat([self.df, new_cols_df], axis=1)
        
        # 确保pandas DataFrame已初始化
        self._ensure_pandas_df()
        
        return self.df
    
    def calculate_kdj(self, window=14):
        """
        计算KDJ指标
        
        Args:
            window: KDJ计算窗口
            
        Returns:
            pd.DataFrame: 包含KDJ指标的DataFrame
        """
        # 检查是否已经计算过该窗口的KDJ指标
        if window not in self.calculated_indicators['kdj']:
            # 使用Polars计算KDJ指标
            self.pl_df = calculate_kdj_polars(self.pl_df, window)
            
            # 更新计算状态
            self.calculated_indicators['kdj'].add(window)
            
            # 如果pandas DataFrame已初始化，需要同步更新
            if self.df is not None:
                # 只转换新添加的KDJ列
                new_kdj_cols = ['k', 'd', 'j']
                new_cols_df = self.pl_df.select(new_kdj_cols).to_pandas()
                self.df = pd.concat([self.df, new_cols_df], axis=1)
        
        # 确保pandas DataFrame已初始化
        self._ensure_pandas_df()
        
        return self.df
    
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
        # 确保数据已同步
        current_data = self.get_data(return_polars=True)
        
        # 使用工具函数进行采样
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
        # 确保数据已同步
        if return_polars:
            # 如果已计算了指标，需要将pandas DataFrame转换回Polars
            if self.df is not None and self.pl_df is not None:
                # 只转换新添加的指标列
                new_columns = [col for col in self.df.columns if col not in self.pl_df.columns]
                if new_columns:
                    # 将新指标列合并到Polars DataFrame
                    new_cols_df = pl.from_pandas(self.df[new_columns])
                    self.pl_df = self.pl_df.hstack(new_cols_df)
            data = self.pl_df
        else:
            # 确保pandas DataFrame已初始化
            self._ensure_pandas_df()
            data = self.df
        
        # 如果需要采样
        if sample:
            sample_params = sample_params or {}
            target_points = sample_params.get('target_points', 1000)
            strategy = sample_params.get('strategy', 'uniform')
            
            if return_polars:
                data = self.sample_data(target_points=target_points, strategy=strategy, return_polars=True)
            else:
                data = self.sample_data(target_points=target_points, strategy=strategy, return_polars=False)
        
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
        重置指标计算状态，可选择重置特定指标或所有指标
        
        Args:
            indicator_type: 要重置的指标类型，None表示重置所有指标
            window: 对于需要窗口的指标，指定要重置的窗口，None表示重置该类型的所有窗口
        """
        if not indicator_type:
            # 重置所有指标
            self.calculated_indicators = {
                'ma': set(),
                'macd': False,
                'rsi': set(),
                'kdj': set(),
                'vol_ma': set(),
                'plugin': set()
            }
            # 重置缓存
            self._calculate_cache = {}
            # 重置插件指标映射
            self._init_plugin_indicator_mapping()
            # 重置数据哈希
            self._data_hash = hash(self.pl_df.to_pandas().values.tobytes())
        elif self.plugin_manager and indicator_type in self.plugin_manager.get_available_indicator_plugins():
            # 重置特定插件指标
            if indicator_type in self.calculated_indicators['plugin']:
                self.calculated_indicators['plugin'].remove(indicator_type)
                # 对于插件指标，目前无法自动确定要删除的列，由插件自行管理
                # 从缓存中删除
                cache_key = self._generate_cache_key(indicator_type)
                if cache_key in self._calculate_cache:
                    del self._calculate_cache[cache_key]
        elif indicator_type in self.calculated_indicators:
            if indicator_type in ['ma', 'rsi', 'kdj', 'vol_ma']:
                if window:
                    # 重置特定窗口
                    self.calculated_indicators[indicator_type].discard(window)
                    # 从DataFrame中删除该列
                    column_name = f'{indicator_type}{window}' if indicator_type != 'vol_ma' else f'vol_ma{window}'
                    if column_name in self.df.columns:
                        self.df.drop(column_name, axis=1, inplace=True)
                    # 从缓存中删除
                    cache_key = self._generate_cache_key(indicator_type, window)
                    if cache_key in self._calculate_cache:
                        del self._calculate_cache[cache_key]
                else:
                    # 重置该类型的所有窗口
                    windows = list(self.calculated_indicators[indicator_type])
                    for w in windows:
                        column_name = f'{indicator_type}{w}' if indicator_type != 'vol_ma' else f'vol_ma{w}'
                        if column_name in self.df.columns:
                            self.df.drop(column_name, axis=1, inplace=True)
                        # 从缓存中删除
                        cache_key = self._generate_cache_key(indicator_type, w)
                        if cache_key in self._calculate_cache:
                            del self._calculate_cache[cache_key]
                    self.calculated_indicators[indicator_type].clear()
            elif indicator_type in ['macd']:
                # 重置MACD指标
                self.calculated_indicators[indicator_type] = False
                # 从DataFrame中删除MACD相关列
                for col in ['macd', 'macd_signal', 'macd_hist']:
                    if col in self.df.columns:
                        self.df.drop(col, axis=1, inplace=True)
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
    
    def get_calculated_indicators(self):
        """
        获取已计算的指标信息
        
        Returns:
            dict: 包含已计算指标类型和窗口的字典
        """
        return self.calculated_indicators.copy()
    
    def _calculate_ma_window(self, window, close_data):
        """
        计算单个移动平均线窗口，用于并行计算
        
        Args:
            window: 移动平均窗口
            close_data: 收盘价数据
            
        Returns:
            tuple: (window, ma_data)
        """
        return window, ta.trend.sma_indicator(close_data, window=window, fillna=True)
    
    def _calculate_rsi_window(self, window, close_data):
        """
        计算单个RSI窗口，用于并行计算
        
        Args:
            window: RSI窗口
            close_data: 收盘价数据
            
        Returns:
            tuple: (window, rsi_data)
        """
        return window, ta.momentum.rsi(close_data, window=window, fillna=True)
    
    def _calculate_vol_ma_window(self, window, volume_data):
        """
        计算单个成交量移动平均线窗口，用于并行计算
        
        Args:
            window: 移动平均窗口
            volume_data: 成交量数据
            
        Returns:
            tuple: (window, vol_ma_data)
        """
        return window, ta.trend.sma_indicator(volume_data, window=window, fillna=True)
    
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
            
            # 如果pandas DataFrame已初始化，需要同步更新
            if self.df is not None:
                # 只转换新添加的MA列
                new_ma_cols = [f'ma{w}' for w in windows_to_calculate]
                new_cols_df = self.pl_df.select(new_ma_cols).to_pandas()
                self.df = pd.concat([self.df, new_cols_df], axis=1)
        
        # 确保pandas DataFrame已初始化
        self._ensure_pandas_df()
        
        return self.df
    
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
            # 串行计算RSI指标（Polars已内部优化）
            for window in windows_to_calculate:
                # 使用Polars计算RSI指标
                self.pl_df = calculate_rsi_polars(self.pl_df, window)
                
                # 更新计算状态
                self.calculated_indicators['rsi'].add(window)
                
                # 如果pandas DataFrame已初始化，需要同步更新
                if self.df is not None:
                    # 只转换新添加的RSI列
                    new_rsi_cols = [f'rsi{window}']
                    new_cols_df = self.pl_df.select(new_rsi_cols).to_pandas()
                    self.df = pd.concat([self.df, new_cols_df], axis=1)
        
        # 确保pandas DataFrame已初始化
        self._ensure_pandas_df()
        
        return self.df
    
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
            
            # 如果pandas DataFrame已初始化，需要同步更新
            if self.df is not None:
                # 只转换新添加的成交量MA列
                new_vol_ma_cols = [f'vol_ma{w}' for w in windows_to_calculate]
                new_cols_df = self.pl_df.select(new_vol_ma_cols).to_pandas()
                self.df = pd.concat([self.df, new_cols_df], axis=1)
        
        # 确保pandas DataFrame已初始化
        self._ensure_pandas_df()
        
        return self.df
    
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
        
        # 确保pandas DataFrame已初始化
        self._ensure_pandas_df()
        
        # 获取指标插件实例
        indicator_plugins = self.plugin_manager.get_available_indicator_plugins()
        if plugin_name not in indicator_plugins:
            raise ValueError(f"指标插件{plugin_name}不存在或未启用")
        
        plugin = indicator_plugins[plugin_name]
        
        # 检查插件指标是否已经计算
        if plugin_name in self.calculated_indicators['plugin']:
            return self.df
        
        try:
            # 调用插件的calculate方法
            result_df = plugin.calculate(self.df, **kwargs)
            
            # 更新DataFrame
            if result_df is not None and isinstance(result_df, pd.DataFrame):
                # 将插件计算的指标列合并到主DataFrame
                for col in result_df.columns:
                    if col not in self.df.columns:
                        self.df[col] = result_df[col]
                
                # 更新计算状态
                self.calculated_indicators['plugin'].add(plugin_name)
        except Exception as e:
            raise RuntimeError(f"计算插件指标{plugin_name}失败: {str(e)}")
        
        return self.df
    
    def calculate_indicator_parallel(self, indicator_type, *args, **kwargs):
        """
        并行计算特定类型的指标
        
        Args:
            indicator_type: 指标类型，如'ma', 'macd', 'rsi', 'kdj', 'vol_ma'或插件名称
            *args: 传递给指标计算方法的位置参数
            **kwargs: 传递给指标计算方法的关键字参数
            
        Returns:
            pd.DataFrame: 包含计算指标的DataFrame
        """
        # 确保pandas DataFrame已初始化
        self._ensure_pandas_df()
        
        if indicator_type not in self.indicator_mapping:
            # 检查是否为插件指标
            if self.plugin_manager and indicator_type in self.plugin_manager.get_available_indicator_plugins():
                # 设置parallel=True
                kwargs['parallel'] = True
                # 调用插件指标计算方法
                return self.calculate_plugin_indicator(indicator_type, **kwargs)
            raise ValueError(f"不支持的指标类型: {indicator_type}")
        
        # 设置parallel=True
        kwargs['parallel'] = True
        
        # 调用相应的指标计算方法
        return self.indicator_mapping[indicator_type](*args, **kwargs)
    
    def calculate_all_indicators(self, parallel=False):
        """
        计算所有支持的技术指标，包括内置指标和插件指标
        
        Args:
            parallel: 是否使用并行计算
            
        Returns:
            pd.DataFrame: 包含所有指标的数据
        """
        # 确保pandas DataFrame已初始化
        self._ensure_pandas_df()
        
        if parallel:
            # 并行计算所有指标类型
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                # 提交所有内置指标计算任务
                futures = {
                    executor.submit(self.calculate_ma, [5, 10, 20, 60], parallel=True): 'ma',
                    executor.submit(self.calculate_macd): 'macd',
                    executor.submit(self.calculate_rsi, [14], parallel=True): 'rsi',
                    executor.submit(self.calculate_kdj, 14): 'kdj',
                    executor.submit(self.calculate_vol_ma, [5, 10], parallel=True): 'vol_ma'
                }
                
                # 等待所有内置指标任务完成
                for future in concurrent.futures.as_completed(futures):
                    indicator_type = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        print(f"计算{indicator_type}指标时发生错误: {e}")
        else:
            # 串行计算所有内置指标
            # 计算移动平均线
            ma_windows = [5, 10, 20, 60]
            self.calculate_ma(ma_windows)
            
            # 计算MACD指标
            self.calculate_macd()
            
            # 计算RSI指标
            rsi_windows = [14]
            self.calculate_rsi(rsi_windows)
            
            # 计算KDJ指标
            kdj_windows = [14]
            for window in kdj_windows:
                self.calculate_kdj(window)
            
            # 计算成交量5日均线和10日均线
            vol_ma_windows = [5, 10]
            self.calculate_vol_ma(vol_ma_windows)
        
        # 计算所有插件指标
        for plugin_name in self.get_available_plugin_indicators():
            try:
                self.calculate_plugin_indicator(plugin_name, parallel=parallel)
            except Exception as e:
                print(f"计算插件指标{plugin_name}时发生错误: {e}")
        
        return self.df
