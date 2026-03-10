#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
价格动量因子计算模块
"""

import polars as pl
import numpy as np


class PriceMomentumFactors:
    """
    价格动量因子计算类
    """
    
    def __init__(self, data):
        """
        初始化价格动量因子计算类
        
        Args:
            data: Polars DataFrame，包含价格数据
        """
        self.data = data
    
    def calculate(self, **params) -> pl.DataFrame:
        """
        计算价格动量因子
        
        Args:
            **params: 计算参数
            
        Returns:
            pl.DataFrame: 包含计算结果的DataFrame
        """
        # 计算各种动量因子
        self._calculate_momentum()
        self._calculate_rsi_momentum()
        self._calculate_macd_momentum()
        self._calculate_stochastic_momentum()
        
        return self.data
    
    def _calculate_momentum(self):
        """
        计算动量因子
        """
        close = self.data['close'].to_numpy()
        
        # 短期动量（1个月）
        momentum_1m = np.zeros_like(close)
        for i in range(20, len(close)):
            momentum_1m[i] = (close[i] - close[i-20]) / close[i-20]
        
        # 中期动量（3个月）
        momentum_3m = np.zeros_like(close)
        for i in range(60, len(close)):
            momentum_3m[i] = (close[i] - close[i-60]) / close[i-60]
        
        # 长期动量（6个月）
        momentum_6m = np.zeros_like(close)
        for i in range(120, len(close)):
            momentum_6m[i] = (close[i] - close[i-120]) / close[i-120]
        
        # 动量反转（12个月）
        momentum_12m = np.zeros_like(close)
        for i in range(240, len(close)):
            momentum_12m[i] = (close[i] - close[i-240]) / close[i-240]
        
        # 添加到DataFrame
        self.data = self.data.with_columns(
            pl.Series('momentum_1m', momentum_1m),
            pl.Series('momentum_3m', momentum_3m),
            pl.Series('momentum_6m', momentum_6m),
            pl.Series('momentum_12m', momentum_12m)
        )
    
    def _calculate_rsi_momentum(self):
        """
        计算RSI动量因子
        """
        close = self.data['close'].to_numpy()
        
        # 计算RSI
        delta = np.diff(close)
        gain = np.where(delta > 0, delta, 0)
        loss = np.where(delta < 0, -delta, 0)
        
        # 14日RSI
        rsi_14 = np.zeros_like(close)
        for i in range(14, len(close)):
            avg_gain = np.mean(gain[i-14:i])
            avg_loss = np.mean(loss[i-14:i])
            if avg_loss == 0:
                rsi_14[i] = 100
            else:
                rs = avg_gain / avg_loss
                rsi_14[i] = 100 - (100 / (1 + rs))
        
        # 21日RSI
        rsi_21 = np.zeros_like(close)
        for i in range(21, len(close)):
            avg_gain = np.mean(gain[i-21:i])
            avg_loss = np.mean(loss[i-21:i])
            if avg_loss == 0:
                rsi_21[i] = 100
            else:
                rs = avg_gain / avg_loss
                rsi_21[i] = 100 - (100 / (1 + rs))
        
        # 添加到DataFrame
        self.data = self.data.with_columns(
            pl.Series('rsi_14', rsi_14),
            pl.Series('rsi_21', rsi_21)
        )
    
    def _calculate_macd_momentum(self):
        """
        计算MACD动量因子
        """
        close = self.data['close'].to_numpy()
        
        # 计算EMA
        def calculate_ema(values, period):
            ema = np.zeros_like(values)
            ema[period-1] = np.mean(values[:period])
            multiplier = 2 / (period + 1)
            for i in range(period, len(values)):
                ema[i] = (values[i] - ema[i-1]) * multiplier + ema[i-1]
            return ema
        
        # 计算MACD
        ema12 = calculate_ema(close, 12)
        ema26 = calculate_ema(close, 26)
        macd = ema12 - ema26
        signal = calculate_ema(macd, 9)
        histogram = macd - signal
        
        # 添加到DataFrame
        self.data = self.data.with_columns(
            pl.Series('macd', macd),
            pl.Series('macd_signal', signal),
            pl.Series('macd_hist', histogram)
        )
    
    def _calculate_stochastic_momentum(self):
        """
        计算随机动量因子
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
        
        # 添加到DataFrame
        self.data = self.data.with_columns(
            pl.Series('kdj_k', k),
            pl.Series('kdj_d', d),
            pl.Series('kdj_j', j)
        )
