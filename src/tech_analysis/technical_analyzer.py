#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
技术分析器，用于计算各种技术指标
"""

import pandas as pd
import numpy as np
import ta


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
    
    def calculate_ma(self, windows=[5, 10, 20, 60]):
        """
        计算移动平均线
        
        Args:
            windows: 移动平均窗口列表
            
        Returns:
            pd.DataFrame: 包含移动平均线的DataFrame
        """
        for window in windows:
            self.df[f'ma{window}'] = ta.trend.sma_indicator(self.df['close'], window=window, fillna=True)
        return self.df
    
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
        self.df['macd'] = ta.trend.macd(self.df['close'], window_slow=slow_period, window_fast=fast_period, fillna=True)
        self.df['macd_signal'] = ta.trend.macd_signal(self.df['close'], window_slow=slow_period, window_fast=fast_period, window_sign=signal_period, fillna=True)
        self.df['macd_hist'] = ta.trend.macd_diff(self.df['close'], window_slow=slow_period, window_fast=fast_period, window_sign=signal_period, fillna=True)
        return self.df
    
    def calculate_kdj(self, window=14):
        """
        计算KDJ指标
        
        Args:
            window: KDJ计算窗口
            
        Returns:
            pd.DataFrame: 包含KDJ指标的DataFrame
        """
        self.df['k'] = ta.momentum.stoch(self.df['high'], self.df['low'], self.df['close'], window=window, fillna=True)
        self.df['d'] = ta.momentum.stoch_signal(self.df['high'], self.df['low'], self.df['close'], window=window, fillna=True)
        self.df['j'] = 3 * self.df['k'] - 2 * self.df['d']
        return self.df
    
    def calculate_rsi(self, window=14):
        """
        计算RSI指标
        
        Args:
            window: RSI计算窗口
            
        Returns:
            pd.DataFrame: 包含RSI指标的DataFrame
        """
        self.df[f'rsi{window}'] = ta.momentum.rsi(self.df['close'], window=window, fillna=True)
        return self.df
    
    def calculate_vol_ma(self, windows=[5, 10]):
        """
        计算成交量移动平均线
        
        Args:
            windows: 移动平均窗口列表
            
        Returns:
            pd.DataFrame: 包含成交量移动平均线的DataFrame
        """
        for window in windows:
            self.df[f'vol_ma{window}'] = ta.trend.sma_indicator(self.df['volume'], window=window, fillna=True)
        return self.df
    
    def get_data(self):
        """
        获取包含所有计算指标的数据
        
        Returns:
            pd.DataFrame: 包含所有指标的数据
        """
        return self.df
    
    def calculate_all_indicators(self):
        """
        计算所有支持的技术指标
        
        Returns:
            pd.DataFrame: 包含所有指标的数据
        """
        # 计算移动平均线
        self.calculate_ma([5, 10, 20, 60])
        
        # 计算MACD指标
        self.calculate_macd()
        
        # 计算RSI指标
        self.calculate_rsi(14)
        
        # 计算KDJ指标
        self.calculate_kdj(14)
        
        # 计算成交量5日均线和10日均线
        self.calculate_vol_ma([5, 10])
        
        return self.df
