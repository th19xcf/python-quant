#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
均值回归策略实现
"""

from typing import Dict, Any
from src.backtest.strategies.base_strategy import BaseStrategy


class MeanReversionStrategy(BaseStrategy):
    """
    均值回归策略，基于RSI超买超卖信号
    """
    
    def __init__(self):
        """
        初始化均值回归策略
        """
        super().__init__("MeanReversion")
        # 默认参数
        self.params = {
            'rsi_period': 14,
            'overbought': 70,
            'oversold': 30
        }
    
    def generate_signal(self, data: Dict[str, Any], index: int) -> str:
        """
        生成交易信号
        
        Args:
            data: 当前数据
            index: 当前数据索引
            
        Returns:
            str: 交易信号，'buy'、'sell'或'hold'
        """
        rsi_period = self.params.get('rsi_period', 14)
        overbought = self.params.get('overbought', 70)
        oversold = self.params.get('oversold', 30)
        
        # 检查是否有足够的数据
        if index < rsi_period:
            return 'hold'
        
        # 计算RSI
        rsi = self._calculate_rsi(data, index, rsi_period)
        
        # 生成信号
        if rsi < oversold:
            return 'buy'
        elif rsi > overbought:
            return 'sell'
        else:
            return 'hold'
    
    def _calculate_rsi(self, data: Dict[str, Any], index: int, period: int) -> float:
        """
        计算RSI指标
        
        Args:
            data: 当前数据
            index: 当前数据索引
            period: RSI周期
            
        Returns:
            float: RSI值
        """
        # 获取收盘价数据
        closes = []
        for i in range(max(0, index - period), index + 1):
            closes.append(data.get(f'close_{i}', data.get('close', 0)))
        
        # 计算价格变化
        deltas = []
        for i in range(1, len(closes)):
            deltas.append(closes[i] - closes[i-1])
        
        # 计算上涨和下跌
        gains = []
        losses = []
        for delta in deltas:
            if delta > 0:
                gains.append(delta)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(delta))
        
        # 计算平均上涨和下跌
        avg_gain = sum(gains) / len(gains) if gains else 0
        avg_loss = sum(losses) / len(losses) if losses else 0
        
        # 计算RSI
        if avg_loss == 0:
            return 100
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
