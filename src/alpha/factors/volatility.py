#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
波动率因子计算模块
"""

import polars as pl
import numpy as np


class VolatilityFactors:
    """
    波动率因子计算类
    """
    
    def __init__(self, data):
        """
        初始化波动率因子计算类
        
        Args:
            data: Polars DataFrame，包含价格数据
        """
        self.data = data
    
    def calculate(self, **params) -> pl.DataFrame:
        """
        计算波动率因子
        
        Args:
            **params: 计算参数
            
        Returns:
            pl.DataFrame: 包含计算结果的DataFrame
        """
        # 计算各种波动率因子
        self._calculate_atr()
        self._calculate_historical_volatility()
        self._calculate_garman_klass_volatility()
        self._calculate_volatility_trend()
        
        return self.data
    
    def _calculate_atr(self):
        """
        计算平均真实波幅(ATR)
        """
        high = self.data['high'].to_numpy()
        low = self.data['low'].to_numpy()
        close = self.data['close'].to_numpy()
        
        # 计算真实波幅(TR)
        tr = np.zeros_like(close)
        for i in range(1, len(close)):
            tr[i] = max(high[i] - low[i], abs(high[i] - close[i-1]), abs(low[i] - close[i-1]))
        
        # 计算ATR
        atr_14 = np.zeros_like(close)
        for i in range(14, len(close)):
            atr_14[i] = np.mean(tr[i-14:i])
        
        atr_21 = np.zeros_like(close)
        for i in range(21, len(close)):
            atr_21[i] = np.mean(tr[i-21:i])
        
        # 添加到DataFrame
        self.data = self.data.with_columns(
            pl.Series('atr_14', atr_14),
            pl.Series('atr_21', atr_21)
        )
    
    def _calculate_historical_volatility(self):
        """
        计算历史波动率
        """
        close = self.data['close'].to_numpy()
        
        # 计算收益率
        returns = np.zeros_like(close)
        for i in range(1, len(close)):
            returns[i] = np.log(close[i] / close[i-1])
        
        # 计算历史波动率
        volatility_20 = np.zeros_like(close)
        for i in range(20, len(close)):
            volatility_20[i] = np.std(returns[i-20:i]) * np.sqrt(252)
        
        volatility_60 = np.zeros_like(close)
        for i in range(60, len(close)):
            volatility_60[i] = np.std(returns[i-60:i]) * np.sqrt(252)
        
        # 添加到DataFrame
        self.data = self.data.with_columns(
            pl.Series('volatility_20', volatility_20),
            pl.Series('volatility_60', volatility_60)
        )
    
    def _calculate_garman_klass_volatility(self):
        """
        计算Garman-Klass波动率
        """
        high = self.data['high'].to_numpy()
        low = self.data['low'].to_numpy()
        open_ = self.data['open'].to_numpy()
        close = self.data['close'].to_numpy()
        
        # 计算Garman-Klass波动率
        gk_volatility = np.zeros_like(close)
        for i in range(1, len(close)):
            log_high_low = np.log(high[i] / low[i])
            log_close_open = np.log(close[i] / open_[i])
            gk_volatility[i] = np.sqrt(0.5 * log_high_low**2 - (2 * np.log(2) - 1) * log_close_open**2)
        
        # 计算20日和60日移动平均
        gk_volatility_20 = np.zeros_like(close)
        for i in range(20, len(close)):
            gk_volatility_20[i] = np.mean(gk_volatility[i-20:i]) * np.sqrt(252)
        
        gk_volatility_60 = np.zeros_like(close)
        for i in range(60, len(close)):
            gk_volatility_60[i] = np.mean(gk_volatility[i-60:i]) * np.sqrt(252)
        
        # 添加到DataFrame
        self.data = self.data.with_columns(
            pl.Series('gk_volatility_20', gk_volatility_20),
            pl.Series('gk_volatility_60', gk_volatility_60)
        )
    
    def _calculate_volatility_trend(self):
        """
        计算波动率趋势因子
        """
        close = self.data['close'].to_numpy()
        
        # 计算收益率
        returns = np.zeros_like(close)
        for i in range(1, len(close)):
            returns[i] = np.log(close[i] / close[i-1])
        
        # 计算波动率趋势
        volatility_trend = np.zeros_like(close)
        for i in range(60, len(close)):
            # 计算最近20日和之前40日的波动率
            recent_vol = np.std(returns[i-20:i])
            past_vol = np.std(returns[i-60:i-20])
            if past_vol > 0:
                volatility_trend[i] = (recent_vol - past_vol) / past_vol
        
        # 添加到DataFrame
        self.data = self.data.with_columns(
            pl.Series('volatility_trend', volatility_trend)
        )
