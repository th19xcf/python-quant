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


class TechnicalAnalyzer:
    """
    技术分析器类，提供各种技术指标的计算方法
    """
    
    def __init__(self, data):
        """
        初始化技术分析器
        
        Args:
            data: 股票数据，可以是Polars DataFrame或Pandas DataFrame
        """
        # 转换为Pandas DataFrame以便使用ta库
        self.df = None
        if hasattr(data, 'to_pandas'):
            self.df = data.to_pandas()
        elif isinstance(data, pd.DataFrame):
            self.df = data
        else:
            self.df = pd.DataFrame(data)
        
        # 确保必要的列存在且为数值类型
        required_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in required_columns:
            if col not in self.df.columns:
                raise ValueError(f"数据中没有{col}列")
            self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
        
        # 添加指标计算状态跟踪
        self.calculated_indicators = {
            'ma': set(),  # 已计算的MA窗口
            'macd': False,
            'rsi': set(),  # 已计算的RSI窗口
            'kdj': set(),  # 已计算的KDJ窗口
            'vol_ma': set()  # 已计算的成交量MA窗口
        }
        
        # 添加缓存机制，避免重复计算
        self._calculate_cache = {}
        
        # 初始化指标映射，便于统一管理
        self.indicator_mapping = {
            'ma': self.calculate_ma,
            'macd': self.calculate_macd,
            'rsi': self.calculate_rsi,
            'kdj': self.calculate_kdj,
            'vol_ma': self.calculate_vol_ma
        }
    
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
            self.df['macd'] = ta.trend.macd(self.df['close'], window_slow=slow_period, window_fast=fast_period, fillna=True)
            self.df['macd_signal'] = ta.trend.macd_signal(self.df['close'], window_slow=slow_period, window_fast=fast_period, window_sign=signal_period, fillna=True)
            self.df['macd_hist'] = ta.trend.macd_diff(self.df['close'], window_slow=slow_period, window_fast=fast_period, window_sign=signal_period, fillna=True)
            # 更新计算状态
            self.calculated_indicators['macd'] = True
        
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
            self.df['k'] = ta.momentum.stoch(self.df['high'], self.df['low'], self.df['close'], window=window, fillna=True)
            self.df['d'] = ta.momentum.stoch_signal(self.df['high'], self.df['low'], self.df['close'], window=window, fillna=True)
            self.df['j'] = 3 * self.df['k'] - 2 * self.df['d']
            # 更新计算状态
            self.calculated_indicators['kdj'].add(window)
        
        return self.df
    
    def get_data(self):
        """
        获取包含所有计算指标的数据
        
        Returns:
            pd.DataFrame: 包含所有指标的数据
        """
        return self.df
    
    def is_indicator_calculated(self, indicator_type, window=None):
        """
        检查特定指标是否已经计算
        
        Args:
            indicator_type: 指标类型，如'ma', 'macd', 'rsi', 'kdj', 'vol_ma'
            window: 对于需要窗口的指标，指定窗口大小
            
        Returns:
            bool: 如果指标已计算返回True，否则返回False
        """
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
                'vol_ma': set()
            }
            # 重置缓存
            self._calculate_cache = {}
        elif indicator_type in self.calculated_indicators:
            if indicator_type in ['ma', 'rsi', 'kdj', 'vol_ma']:
                if window:
                    # 重置特定窗口
                    self.calculated_indicators[indicator_type].discard(window)
                    # 从DataFrame中删除该列
                    column_name = f'{indicator_type}{window}' if indicator_type != 'vol_ma' else f'vol_ma{window}'
                    if column_name in self.df.columns:
                        self.df.drop(column_name, axis=1, inplace=True)
                else:
                    # 重置该类型的所有窗口
                    windows = list(self.calculated_indicators[indicator_type])
                    for w in windows:
                        column_name = f'{indicator_type}{w}' if indicator_type != 'vol_ma' else f'vol_ma{w}'
                        if column_name in self.df.columns:
                            self.df.drop(column_name, axis=1, inplace=True)
                    self.calculated_indicators[indicator_type].clear()
            elif indicator_type in ['macd']:
                # 重置MACD指标
                self.calculated_indicators[indicator_type] = False
                # 从DataFrame中删除MACD相关列
                for col in ['macd', 'macd_signal', 'macd_hist']:
                    if col in self.df.columns:
                        self.df.drop(col, axis=1, inplace=True)
    
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
            if parallel:
                # 并行计算MA指标
                close_data = self.df['close']
                results = []
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(windows_to_calculate), 8)) as executor:
                    # 使用partial绑定close_data参数
                    ma_func = partial(self._calculate_ma_window, close_data=close_data)
                    # 提交所有任务
                    futures = {executor.submit(ma_func, window): window for window in windows_to_calculate}
                    
                    # 收集结果
                    for future in concurrent.futures.as_completed(futures):
                        window, ma_data = future.result()
                        results.append((window, ma_data))
                
                # 将结果合并到DataFrame
                for window, ma_data in results:
                    self.df[f'ma{window}'] = ma_data
                    # 更新计算状态
                    self.calculated_indicators['ma'].add(window)
            else:
                # 串行计算MA指标
                for window in windows_to_calculate:
                    self.df[f'ma{window}'] = ta.trend.sma_indicator(self.df['close'], window=window, fillna=True)
                    # 更新计算状态
                    self.calculated_indicators['ma'].add(window)
        
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
            if parallel:
                # 并行计算RSI指标
                close_data = self.df['close']
                results = []
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(windows_to_calculate), 8)) as executor:
                    # 使用partial绑定close_data参数
                    rsi_func = partial(self._calculate_rsi_window, close_data=close_data)
                    # 提交所有任务
                    futures = {executor.submit(rsi_func, window): window for window in windows_to_calculate}
                    
                    # 收集结果
                    for future in concurrent.futures.as_completed(futures):
                        window, rsi_data = future.result()
                        results.append((window, rsi_data))
                
                # 将结果合并到DataFrame
                for window, rsi_data in results:
                    self.df[f'rsi{window}'] = rsi_data
                    # 更新计算状态
                    self.calculated_indicators['rsi'].add(window)
            else:
                # 串行计算RSI指标
                for window in windows_to_calculate:
                    self.df[f'rsi{window}'] = ta.momentum.rsi(self.df['close'], window=window, fillna=True)
                    # 更新计算状态
                    self.calculated_indicators['rsi'].add(window)
        
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
            if parallel:
                # 并行计算成交量MA指标
                volume_data = self.df['volume']
                results = []
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(windows_to_calculate), 8)) as executor:
                    # 使用partial绑定volume_data参数
                    vol_ma_func = partial(self._calculate_vol_ma_window, volume_data=volume_data)
                    # 提交所有任务
                    futures = {executor.submit(vol_ma_func, window): window for window in windows_to_calculate}
                    
                    # 收集结果
                    for future in concurrent.futures.as_completed(futures):
                        window, vol_ma_data = future.result()
                        results.append((window, vol_ma_data))
                
                # 将结果合并到DataFrame
                for window, vol_ma_data in results:
                    self.df[f'vol_ma{window}'] = vol_ma_data
                    # 更新计算状态
                    self.calculated_indicators['vol_ma'].add(window)
            else:
                # 串行计算成交量MA指标
                for window in windows_to_calculate:
                    self.df[f'vol_ma{window}'] = ta.trend.sma_indicator(self.df['volume'], window=window, fillna=True)
                    # 更新计算状态
                    self.calculated_indicators['vol_ma'].add(window)
        
        return self.df
    
    def calculate_indicator_parallel(self, indicator_type, *args, **kwargs):
        """
        并行计算特定类型的指标
        
        Args:
            indicator_type: 指标类型，如'ma', 'macd', 'rsi', 'kdj', 'vol_ma'
            *args: 传递给指标计算方法的位置参数
            **kwargs: 传递给指标计算方法的关键字参数
            
        Returns:
            pd.DataFrame: 包含计算指标的DataFrame
        """
        if indicator_type not in self.indicator_mapping:
            raise ValueError(f"不支持的指标类型: {indicator_type}")
        
        # 设置parallel=True
        kwargs['parallel'] = True
        
        # 调用相应的指标计算方法
        return self.indicator_mapping[indicator_type](*args, **kwargs)
    
    def calculate_all_indicators(self, parallel=False):
        """
        计算所有支持的技术指标
        
        Args:
            parallel: 是否使用并行计算
            
        Returns:
            pd.DataFrame: 包含所有指标的数据
        """
        if parallel:
            # 并行计算所有指标类型
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                # 提交所有指标计算任务
                futures = {
                    executor.submit(self.calculate_ma, [5, 10, 20, 60], parallel=True): 'ma',
                    executor.submit(self.calculate_macd): 'macd',
                    executor.submit(self.calculate_rsi, [14], parallel=True): 'rsi',
                    executor.submit(self.calculate_kdj, 14): 'kdj',
                    executor.submit(self.calculate_vol_ma, [5, 10], parallel=True): 'vol_ma'
                }
                
                # 等待所有任务完成
                for future in concurrent.futures.as_completed(futures):
                    indicator_type = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        print(f"计算{indicator_type}指标时发生错误: {e}")
        else:
            # 串行计算所有指标
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
        
        return self.df
