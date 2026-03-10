#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
成交量因子计算模块
"""

import polars as pl
import numpy as np


class VolumeFactors:
    """
    成交量因子计算类
    """
    
    def __init__(self, data):
        """
        初始化成交量因子计算类
        
        Args:
            data: Polars DataFrame，包含价格和成交量数据
        """
        self.data = data
    
    def calculate(self, **params) -> pl.DataFrame:
        """
        计算成交量因子
        
        Args:
            **params: 计算参数
            
        Returns:
            pl.DataFrame: 包含计算结果的DataFrame
        """
        # 计算各种成交量因子
        self._calculate_volume_change()
        self._calculate_volume_momentum()
        self._calculate_on_balance_volume()
        self._calculate_volume_price_relationship()
        
        return self.data
    
    def _calculate_volume_change(self):
        """
        计算成交量变化因子
        """
        volume = self.data['volume'].to_numpy()
        
        # 计算成交量变化率
        volume_change = np.zeros_like(volume)
        for i in range(1, len(volume)):
            if volume[i-1] > 0:
                volume_change[i] = (volume[i] - volume[i-1]) / volume[i-1]
        
        # 计算5日和20日平均成交量变化率
        volume_change_5 = np.zeros_like(volume)
        for i in range(5, len(volume)):
            volume_change_5[i] = np.mean(volume_change[i-5:i])
        
        volume_change_20 = np.zeros_like(volume)
        for i in range(20, len(volume)):
            volume_change_20[i] = np.mean(volume_change[i-20:i])
        
        # 添加到DataFrame
        self.data = self.data.with_columns(
            pl.Series('volume_change', volume_change),
            pl.Series('volume_change_5', volume_change_5),
            pl.Series('volume_change_20', volume_change_20)
        )
    
    def _calculate_volume_momentum(self):
        """
        计算成交量动量因子
        """
        volume = self.data['volume'].to_numpy()
        
        # 计算成交量动量
        volume_momentum_10 = np.zeros_like(volume)
        for i in range(10, len(volume)):
            volume_momentum_10[i] = (volume[i] - volume[i-10]) / volume[i-10]
        
        volume_momentum_20 = np.zeros_like(volume)
        for i in range(20, len(volume)):
            volume_momentum_20[i] = (volume[i] - volume[i-20]) / volume[i-20]
        
        # 添加到DataFrame
        self.data = self.data.with_columns(
            pl.Series('volume_momentum_10', volume_momentum_10),
            pl.Series('volume_momentum_20', volume_momentum_20)
        )
    
    def _calculate_on_balance_volume(self):
        """
        计算能量潮(OBV)因子
        """
        volume = self.data['volume'].to_numpy()
        close = self.data['close'].to_numpy()
        
        # 计算OBV
        obv = np.zeros_like(volume)
        for i in range(1, len(volume)):
            if close[i] > close[i-1]:
                obv[i] = obv[i-1] + volume[i]
            elif close[i] < close[i-1]:
                obv[i] = obv[i-1] - volume[i]
            else:
                obv[i] = obv[i-1]
        
        # 计算OBV的10日和20日移动平均
        obv_ma10 = np.zeros_like(volume)
        for i in range(10, len(volume)):
            obv_ma10[i] = np.mean(obv[i-10:i])
        
        obv_ma20 = np.zeros_like(volume)
        for i in range(20, len(volume)):
            obv_ma20[i] = np.mean(obv[i-20:i])
        
        # 添加到DataFrame
        self.data = self.data.with_columns(
            pl.Series('obv', obv),
            pl.Series('obv_ma10', obv_ma10),
            pl.Series('obv_ma20', obv_ma20)
        )
    
    def _calculate_volume_price_relationship(self):
        """
        计算量价关系因子
        """
        volume = self.data['volume'].to_numpy()
        close = self.data['close'].to_numpy()
        
        # 计算价格变化
        price_change = np.zeros_like(close)
        for i in range(1, len(close)):
            price_change[i] = (close[i] - close[i-1]) / close[i-1]
        
        # 计算量价配合度
        volume_price_correlation = np.zeros_like(volume)
        for i in range(20, len(volume)):
            corr = np.corrcoef(volume[i-20:i], price_change[i-20:i])[0, 1]
            volume_price_correlation[i] = corr
        
        # 计算成交量加权价格
        volume_weighted_price = np.zeros_like(close)
        for i in range(1, len(close)):
            if np.sum(volume[max(0, i-20):i]) > 0:
                volume_weighted_price[i] = np.sum(close[max(0, i-20):i] * volume[max(0, i-20):i]) / np.sum(volume[max(0, i-20):i])
        
        # 添加到DataFrame
        self.data = self.data.with_columns(
            pl.Series('volume_price_correlation', volume_price_correlation),
            pl.Series('volume_weighted_price', volume_weighted_price)
        )
