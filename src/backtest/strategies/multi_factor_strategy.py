#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
多因子策略
"""

from src.backtest.strategies.base_strategy import BaseStrategy
from typing import Dict, Any, List


class MultiFactorStrategy(BaseStrategy):
    """
    多因子策略
    结合多个技术指标来生成交易信号
    """
    
    def __init__(self, factors: List[str] = None, weights: Dict[str, float] = None):
        """
        初始化多因子策略
        
        Args:
            factors: 使用的因子列表
            weights: 因子权重字典
        """
        super().__init__(name="多因子策略")
        self.params['factors'] = factors or ['ma', 'macd', 'rsi']
        self.params['weights'] = weights or {'ma': 0.3, 'macd': 0.4, 'rsi': 0.3}
        
        # 初始化因子数据
        self.ma_data = {
            'short_ma': [],
            'long_ma': []
        }
        self.macd_data = {
            'close_prices': [],
            'fast_ema': [],
            'slow_ema': [],
            'macd': [],
            'signal': []
        }
        self.rsi_data = {
            'close_prices': [],
            'gains': [],
            'losses': []
        }
    
    def _calculate_ma(self, data: Dict[str, Any]):
        """
        计算移动平均线因子
        
        Args:
            data: 当前数据
            
        Returns:
            float: 因子值，1为买入信号，-1为卖出信号，0为持有
        """
        close_price = data.get('close', 0)
        
        # 更新短期均线
        self.ma_data['short_ma'].append(close_price)
        if len(self.ma_data['short_ma']) > 5:
            self.ma_data['short_ma'].pop(0)
        
        # 更新长期均线
        self.ma_data['long_ma'].append(close_price)
        if len(self.ma_data['long_ma']) > 20:
            self.ma_data['long_ma'].pop(0)
        
        # 检查是否有足够的数据计算均线
        if len(self.ma_data['short_ma']) < 5 or len(self.ma_data['long_ma']) < 20:
            return 0
        
        # 计算均线值
        short_ma_value = sum(self.ma_data['short_ma']) / len(self.ma_data['short_ma'])
        long_ma_value = sum(self.ma_data['long_ma']) / len(self.ma_data['long_ma'])
        
        # 生成因子值
        if short_ma_value > long_ma_value:
            return 1
        elif short_ma_value < long_ma_value:
            return -1
        else:
            return 0
    
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
    
    def _calculate_macd(self, data: Dict[str, Any]):
        """
        计算MACD因子
        
        Args:
            data: 当前数据
            
        Returns:
            float: 因子值，1为买入信号，-1为卖出信号，0为持有
        """
        close_price = data.get('close', 0)
        
        # 更新价格序列
        self.macd_data['close_prices'].append(close_price)
        
        # 计算EMA
        if len(self.macd_data['close_prices']) >= 26:
            # 计算快速EMA
            fast_ema = self._calculate_ema(self.macd_data['close_prices'][-12:], 12)
            self.macd_data['fast_ema'].append(fast_ema)
            
            # 计算慢速EMA
            slow_ema = self._calculate_ema(self.macd_data['close_prices'][-26:], 26)
            self.macd_data['slow_ema'].append(slow_ema)
            
            # 计算MACD
            macd = fast_ema - slow_ema
            self.macd_data['macd'].append(macd)
            
            # 计算信号线
            if len(self.macd_data['macd']) >= 9:
                signal = self._calculate_ema(self.macd_data['macd'][-9:], 9)
                self.macd_data['signal'].append(signal)
                
                # 生成因子值
                if len(self.macd_data['signal']) > 1:
                    prev_macd = self.macd_data['macd'][-2]
                    prev_signal = self.macd_data['signal'][-2]
                    current_macd = self.macd_data['macd'][-1]
                    current_signal = self.macd_data['signal'][-1]
                    
                    # 金叉买入
                    if prev_macd < prev_signal and current_macd > current_signal:
                        return 1
                    # 死叉卖出
                    elif prev_macd > prev_signal and current_macd < current_signal:
                        return -1
        
        return 0
    
    def _calculate_rsi(self, data: Dict[str, Any]):
        """
        计算RSI因子
        
        Args:
            data: 当前数据
            
        Returns:
            float: 因子值，1为买入信号，-1为卖出信号，0为持有
        """
        close_price = data.get('close', 0)
        
        # 更新价格序列
        self.rsi_data['close_prices'].append(close_price)
        
        # 计算收益率
        if len(self.rsi_data['close_prices']) > 1:
            previous_price = self.rsi_data['close_prices'][-2]
            current_price = close_price
            change = current_price - previous_price
            
            if change > 0:
                self.rsi_data['gains'].append(change)
                self.rsi_data['losses'].append(0)
            else:
                self.rsi_data['gains'].append(0)
                self.rsi_data['losses'].append(abs(change))
            
            # 限制窗口大小为14
            if len(self.rsi_data['gains']) > 14:
                self.rsi_data['gains'].pop(0)
            if len(self.rsi_data['losses']) > 14:
                self.rsi_data['losses'].pop(0)
            
            # 计算RSI
            if len(self.rsi_data['gains']) == 14:
                avg_gain = sum(self.rsi_data['gains']) / 14
                avg_loss = sum(self.rsi_data['losses']) / 14
                
                if avg_loss == 0:
                    rsi = 100
                else:
                    rs = avg_gain / avg_loss
                    rsi = 100 - (100 / (1 + rs))
                
                # 生成因子值
                if rsi < 30:
                    return 1  # 超卖，买入
                elif rsi > 70:
                    return -1  # 超买，卖出
        
        return 0
    
    def generate_signal(self, data: Dict[str, Any], index: int) -> str:
        """
        生成交易信号
        
        Args:
            data: 当前数据
            index: 当前数据索引
            
        Returns:
            str: 交易信号，'buy'、'sell'或'hold'
        """
        # 计算各因子值
        factor_values = {}
        
        if 'ma' in self.params['factors']:
            factor_values['ma'] = self._calculate_ma(data)
        
        if 'macd' in self.params['factors']:
            factor_values['macd'] = self._calculate_macd(data)
        
        if 'rsi' in self.params['factors']:
            factor_values['rsi'] = self._calculate_rsi(data)
        
        # 计算加权得分
        score = 0
        for factor, value in factor_values.items():
            if factor in self.params['weights']:
                score += value * self.params['weights'][factor]
        
        # 生成信号
        if score > 0.3:
            return 'buy'
        elif score < -0.3:
            return 'sell'
        else:
            return 'hold'
