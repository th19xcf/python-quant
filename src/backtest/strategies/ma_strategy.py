#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
移动平均线策略
"""

from src.backtest.strategies.base_strategy import BaseStrategy
from typing import Dict, Any


class MAStrategy(BaseStrategy):
    """
    移动平均线策略
    当短期均线上穿长期均线时买入，下穿时卖出
    """
    
    def __init__(self, short_window: int = 5, long_window: int = 20):
        """
        初始化移动平均线策略
        
        Args:
            short_window: 短期均线窗口
            long_window: 长期均线窗口
        """
        super().__init__(name="移动平均线策略")
        self.params['short_window'] = short_window
        self.params['long_window'] = long_window
        self.short_ma = []
        self.long_ma = []
    
    def generate_signal(self, data: Dict[str, Any], index: int) -> str:
        """
        生成交易信号
        
        Args:
            data: 当前数据
            index: 当前数据索引
            
        Returns:
            str: 交易信号，'buy'、'sell'或'hold'
        """
        # 计算移动平均线
        close_price = data.get('close', 0)
        
        # 更新短期均线
        self.short_ma.append(close_price)
        if len(self.short_ma) > self.params['short_window']:
            self.short_ma.pop(0)
        
        # 更新长期均线
        self.long_ma.append(close_price)
        if len(self.long_ma) > self.params['long_window']:
            self.long_ma.pop(0)
        
        # 检查是否有足够的数据计算均线
        if len(self.short_ma) < self.params['short_window'] or len(self.long_ma) < self.params['long_window']:
            return 'hold'
        
        # 计算均线值
        short_ma_value = sum(self.short_ma) / len(self.short_ma)
        long_ma_value = sum(self.long_ma) / len(self.long_ma)
        
        # 生成信号
        if short_ma_value > long_ma_value:
            return 'buy'
        elif short_ma_value < long_ma_value:
            return 'sell'
        else:
            return 'hold'
