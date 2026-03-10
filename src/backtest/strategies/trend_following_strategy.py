#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
趋势跟踪策略实现
"""

from typing import Dict, Any
from src.backtest.strategies.base_strategy import BaseStrategy


class TrendFollowingStrategy(BaseStrategy):
    """
    趋势跟踪策略，基于移动平均线交叉信号
    """
    
    def __init__(self):
        """
        初始化趋势跟踪策略
        """
        super().__init__("TrendFollowing")
        # 默认参数
        self.params = {
            'short_window': 10,
            'long_window': 20
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
        short_window = self.params.get('short_window', 10)
        long_window = self.params.get('long_window', 20)
        
        # 检查是否有足够的数据
        if index < long_window:
            return 'hold'
        
        # 计算移动平均线
        short_ma = 0
        long_ma = 0
        
        # 计算短期移动平均
        short_sum = 0
        for i in range(max(0, index - short_window + 1), index + 1):
            short_sum += data.get(f'close_{i}', data.get('close', 0))
        short_ma = short_sum / min(short_window, index + 1)
        
        # 计算长期移动平均
        long_sum = 0
        for i in range(max(0, index - long_window + 1), index + 1):
            long_sum += data.get(f'close_{i}', data.get('close', 0))
        long_ma = long_sum / min(long_window, index + 1)
        
        # 计算前一期的移动平均线
        prev_short_ma = 0
        prev_long_ma = 0
        
        if index > 0:
            # 计算前一期的短期移动平均
            prev_short_sum = 0
            for i in range(max(0, index - short_window), index):
                prev_short_sum += data.get(f'close_{i}', data.get('close', 0))
            prev_short_ma = prev_short_sum / min(short_window, index)
            
            # 计算前一期的长期移动平均
            prev_long_sum = 0
            for i in range(max(0, index - long_window), index):
                prev_long_sum += data.get(f'close_{i}', data.get('close', 0))
            prev_long_ma = prev_long_sum / min(long_window, index)
        
        # 生成信号
        if short_ma > long_ma and prev_short_ma <= prev_long_ma:
            return 'buy'
        elif short_ma < long_ma and prev_short_ma >= prev_long_ma:
            return 'sell'
        else:
            return 'hold'
