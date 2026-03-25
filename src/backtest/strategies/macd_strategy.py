#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MACD策略
"""

from src.backtest.strategies.base_strategy import BaseStrategy
from typing import Dict, Any


class MACDStrategy(BaseStrategy):
    """
    MACD策略
    当MACD线上穿信号线时买入，下穿时卖出
    """
    
    def __init__(self, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9):
        """
        初始化MACD策略
        
        Args:
            fast_period: 快速EMA周期
            slow_period: 慢速EMA周期
            signal_period: 信号线EMA周期
        """
        super().__init__(name="MACD策略")
        self.params['fast_period'] = fast_period
        self.params['slow_period'] = slow_period
        self.params['signal_period'] = signal_period
        self.close_prices = []
        self.fast_ema = []
        self.slow_ema = []
        self.macd = []
        self.signal = []
    
    def _calculate_ema(self, prices, period):
        """
        计算指数移动平均线
        
        Args:
            prices: 价格序列
            period: 周期
            
        Returns:
            float: EMA值
        """
        if len(prices) < period:
            return sum(prices) / len(prices)
        
        alpha = 2 / (period + 1)
        ema = prices[0]
        for price in prices[1:]:
            ema = alpha * price + (1 - alpha) * ema
        return ema
    
    def generate_signal(self, data: Dict[str, Any], index: int) -> str:
        """
        生成交易信号
        
        Args:
            data: 当前数据
            index: 当前数据索引
            
        Returns:
            str: 交易信号，'buy'、'sell'或'hold'
        """
        # 获取收盘价
        close_price = data.get('close', 0)
        
        # 更新价格序列
        self.close_prices.append(close_price)
        
        # 计算EMA
        if len(self.close_prices) >= self.params['slow_period']:
            # 计算快速EMA
            fast_ema = self._calculate_ema(self.close_prices[-self.params['fast_period']:], self.params['fast_period'])
            self.fast_ema.append(fast_ema)
            
            # 计算慢速EMA
            slow_ema = self._calculate_ema(self.close_prices[-self.params['slow_period']:], self.params['slow_period'])
            self.slow_ema.append(slow_ema)
            
            # 计算MACD
            macd = fast_ema - slow_ema
            self.macd.append(macd)
            
            # 计算信号线
            if len(self.macd) >= self.params['signal_period']:
                signal = self._calculate_ema(self.macd[-self.params['signal_period']:], self.params['signal_period'])
                self.signal.append(signal)
                
                # 生成信号
                if len(self.signal) > 1:
                    prev_macd = self.macd[-2]
                    prev_signal = self.signal[-2]
                    current_macd = self.macd[-1]
                    current_signal = self.signal[-1]
                    
                    # 金叉买入
                    if prev_macd < prev_signal and current_macd > current_signal:
                        return 'buy'
                    # 死叉卖出
                    elif prev_macd > prev_signal and current_macd < current_signal:
                        return 'sell'
        
        return 'hold'
