#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
技术指标因子计算模块
"""

import polars as pl
import numpy as np


class TechnicalFactors:
    """
    技术指标因子计算类
    """
    
    def __init__(self, data):
        """
        初始化技术指标因子计算类
        
        Args:
            data: Polars DataFrame，包含价格数据
        """
        self.data = data
    
    def calculate(self, **params) -> pl.DataFrame:
        """
        计算技术指标因子
        
        Args:
            **params: 计算参数
            
        Returns:
            pl.DataFrame: 包含计算结果的DataFrame
        """
        # 计算各种技术指标因子
        self._calculate_bollinger_bands()
        self._calculate_moving_average_crossovers()
        self._calculate_relative_strength()
        self._calculate_stochastic_oscillator()
        
        return self.data
    
    def _calculate_bollinger_bands(self):
        """
        计算布林带因子
        """
        close = self.data['close'].to_numpy()
        
        # 计算20日移动平均和标准差
        ma20 = np.zeros_like(close)
        std20 = np.zeros_like(close)
        upper_band = np.zeros_like(close)
        lower_band = np.zeros_like(close)
        bollinger_width = np.zeros_like(close)
        bollinger_position = np.zeros_like(close)
        
        for i in range(20, len(close)):
            ma20[i] = np.mean(close[i-20:i])
            std20[i] = np.std(close[i-20:i])
            upper_band[i] = ma20[i] + 2 * std20[i]
            lower_band[i] = ma20[i] - 2 * std20[i]
            bollinger_width[i] = (upper_band[i] - lower_band[i]) / ma20[i]
            bollinger_position[i] = (close[i] - ma20[i]) / (upper_band[i] - lower_band[i])
        
        # 添加到DataFrame
        self.data = self.data.with_columns(
            pl.Series('bollinger_ma20', ma20),
            pl.Series('bollinger_upper', upper_band),
            pl.Series('bollinger_lower', lower_band),
            pl.Series('bollinger_width', bollinger_width),
            pl.Series('bollinger_position', bollinger_position)
        )
    
    def _calculate_moving_average_crossovers(self):
        """
        计算移动平均线交叉因子
        """
        close = self.data['close'].to_numpy()
        
        # 计算不同周期的移动平均线
        ma5 = np.zeros_like(close)
        ma10 = np.zeros_like(close)
        ma20 = np.zeros_like(close)
        ma60 = np.zeros_like(close)
        
        for i in range(60, len(close)):
            ma5[i] = np.mean(close[i-5:i])
            ma10[i] = np.mean(close[i-10:i])
            ma20[i] = np.mean(close[i-20:i])
            ma60[i] = np.mean(close[i-60:i])
        
        # 计算移动平均线交叉信号
        ma_crossover_5_20 = np.zeros_like(close)
        ma_crossover_10_60 = np.zeros_like(close)
        
        for i in range(60, len(close)):
            if ma5[i] > ma20[i] and ma5[i-1] <= ma20[i-1]:
                ma_crossover_5_20[i] = 1  # 金叉
            elif ma5[i] < ma20[i] and ma5[i-1] >= ma20[i-1]:
                ma_crossover_5_20[i] = -1  # 死叉
            
            if ma10[i] > ma60[i] and ma10[i-1] <= ma60[i-1]:
                ma_crossover_10_60[i] = 1  # 金叉
            elif ma10[i] < ma60[i] and ma10[i-1] >= ma60[i-1]:
                ma_crossover_10_60[i] = -1  # 死叉
        
        # 添加到DataFrame
        self.data = self.data.with_columns(
            pl.Series('ma5', ma5),
            pl.Series('ma10', ma10),
            pl.Series('ma20', ma20),
            pl.Series('ma60', ma60),
            pl.Series('ma_crossover_5_20', ma_crossover_5_20),
            pl.Series('ma_crossover_10_60', ma_crossover_10_60)
        )
    
    def _calculate_relative_strength(self):
        """
        计算相对强弱因子
        """
        close = self.data['close'].to_numpy()
        
        # 计算RSI
        delta = np.diff(close)
        gain = np.where(delta > 0, delta, 0)
        loss = np.where(delta < 0, -delta, 0)
        
        rsi_14 = np.zeros_like(close)
        for i in range(14, len(close)):
            avg_gain = np.mean(gain[i-14:i])
            avg_loss = np.mean(loss[i-14:i])
            if avg_loss == 0:
                rsi_14[i] = 100
            else:
                rs = avg_gain / avg_loss
                rsi_14[i] = 100 - (100 / (1 + rs))
        
        # 计算RSI动量
        rsi_momentum = np.zeros_like(close)
        for i in range(28, len(close)):
            rsi_momentum[i] = rsi_14[i] - rsi_14[i-14]
        
        # 添加到DataFrame
        self.data = self.data.with_columns(
            pl.Series('rsi_14', rsi_14),
            pl.Series('rsi_momentum', rsi_momentum)
        )
    
    def _calculate_stochastic_oscillator(self):
        """
        计算随机振荡器因子
        """
        high = self.data['high'].to_numpy()
        low = self.data['low'].to_numpy()
        close = self.data['close'].to_numpy()
        
        # 计算KDJ指标
        k = np.zeros_like(close)
        d = np.zeros_like(close)
        j = np.zeros_like(close)
        
        for i in range(14, len(close)):
            # 计算RSV
            highest = np.max(high[i-14:i])
            lowest = np.min(low[i-14:i])
            if highest == lowest:
                rsv = 0
            else:
                rsv = (close[i] - lowest) / (highest - lowest) * 100
            
            # 计算K
            if i == 14:
                k[i] = 50
            else:
                k[i] = k[i-1] * 2/3 + rsv * 1/3
            
            # 计算D
            if i == 14:
                d[i] = 50
            else:
                d[i] = d[i-1] * 2/3 + k[i] * 1/3
            
            # 计算J
            j[i] = 3 * k[i] - 2 * d[i]
        
        # 计算KDJ交叉信号
        kdj_crossover = np.zeros_like(close)
        for i in range(15, len(close)):
            if k[i] > d[i] and k[i-1] <= d[i-1]:
                kdj_crossover[i] = 1  # 金叉
            elif k[i] < d[i] and k[i-1] >= d[i-1]:
                kdj_crossover[i] = -1  # 死叉
        
        # 添加到DataFrame
        self.data = self.data.with_columns(
            pl.Series('kdj_k', k),
            pl.Series('kdj_d', d),
            pl.Series('kdj_j', j),
            pl.Series('kdj_crossover', kdj_crossover)
        )
