#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
信号生成器，用于根据技术指标生成交易信号
"""

from typing import Dict, Any, List
import polars as pl
import numpy as np
from loguru import logger


class SignalGenerator:
    """
    信号生成器类，提供各种策略的信号生成方法
    """
    
    def generate_trend_following_signals(self, data: pl.DataFrame, **params) -> Dict[str, Any]:
        """
        生成趋势跟踪策略信号
        
        Args:
            data: 包含技术指标的数据
            **params: 策略参数
            
        Returns:
            Dict[str, Any]: 信号字典
        """
        # 默认参数
        ma_short = params.get('ma_short', 10)
        ma_long = params.get('ma_long', 20)
        
        # 确保MA指标已计算
        if f'ma{ma_short}' not in data.columns or f'ma{ma_long}' not in data.columns:
            logger.error(f"缺少MA指标数据，需要ma{ma_short}和ma{ma_long}")
            return {}
        
        # 计算信号
        short_ma = data[f'ma{ma_short}'].to_numpy()
        long_ma = data[f'ma{ma_long}'].to_numpy()
        close = data['close'].to_numpy()
        dates = data['date'].to_numpy()
        
        signals = []
        positions = []
        
        for i in range(1, len(short_ma)):
            # 金叉信号
            if short_ma[i] > long_ma[i] and short_ma[i-1] <= long_ma[i-1]:
                signals.append({'date': dates[i], 'signal': 'buy', 'price': close[i], 'reason': 'MA金叉'})
                positions.append(1)
            # 死叉信号
            elif short_ma[i] < long_ma[i] and short_ma[i-1] >= long_ma[i-1]:
                signals.append({'date': dates[i], 'signal': 'sell', 'price': close[i], 'reason': 'MA死叉'})
                positions.append(0)
            else:
                # 保持之前的仓位
                positions.append(positions[-1] if positions else 0)
        
        return {
            'signals': signals,
            'positions': positions,
            'params': params
        }
    
    def generate_mean_reversion_signals(self, data: pl.DataFrame, **params) -> Dict[str, Any]:
        """
        生成均值回归策略信号
        
        Args:
            data: 包含技术指标的数据
            **params: 策略参数
            
        Returns:
            Dict[str, Any]: 信号字典
        """
        # 默认参数
        rsi_period = params.get('rsi_period', 14)
        overbought = params.get('overbought', 70)
        oversold = params.get('oversold', 30)
        
        # 确保RSI指标已计算
        if f'rsi{rsi_period}' not in data.columns:
            logger.error(f"缺少RSI指标数据，需要rsi{rsi_period}")
            return {}
        
        # 计算信号
        rsi = data[f'rsi{rsi_period}'].to_numpy()
        close = data['close'].to_numpy()
        dates = data['date'].to_numpy()
        
        signals = []
        positions = []
        
        for i in range(len(rsi)):
            # 超卖信号
            if rsi[i] < oversold:
                signals.append({'date': dates[i], 'signal': 'buy', 'price': close[i], 'reason': 'RSI超卖'})
                positions.append(1)
            # 超买信号
            elif rsi[i] > overbought:
                signals.append({'date': dates[i], 'signal': 'sell', 'price': close[i], 'reason': 'RSI超买'})
                positions.append(0)
            else:
                # 保持之前的仓位
                positions.append(positions[-1] if positions else 0)
        
        return {
            'signals': signals,
            'positions': positions,
            'params': params
        }
    
    def generate_momentum_signals(self, data: pl.DataFrame, **params) -> Dict[str, Any]:
        """
        生成动量策略信号
        
        Args:
            data: 包含技术指标的数据
            **params: 策略参数
            
        Returns:
            Dict[str, Any]: 信号字典
        """
        # 默认参数
        momentum_period = params.get('momentum_period', 12)
        
        # 计算动量
        close = data['close'].to_numpy()
        dates = data['date'].to_numpy()
        
        momentum = np.zeros_like(close)
        for i in range(momentum_period, len(close)):
            momentum[i] = (close[i] - close[i - momentum_period]) / close[i - momentum_period] * 100
        
        signals = []
        positions = []
        
        for i in range(momentum_period, len(momentum)):
            # 动量为正且增加
            if momentum[i] > 0 and momentum[i] > momentum[i-1]:
                signals.append({'date': dates[i], 'signal': 'buy', 'price': close[i], 'reason': '动量上升'})
                positions.append(1)
            # 动量为负且减少
            elif momentum[i] < 0 and momentum[i] < momentum[i-1]:
                signals.append({'date': dates[i], 'signal': 'sell', 'price': close[i], 'reason': '动量下降'})
                positions.append(0)
            else:
                # 保持之前的仓位
                positions.append(positions[-1] if positions else 0)
        
        return {
            'signals': signals,
            'positions': positions,
            'params': params
        }
    
    def generate_volatility_breakout_signals(self, data: pl.DataFrame, **params) -> Dict[str, Any]:
        """
        生成波动率突破策略信号
        
        Args:
            data: 包含技术指标的数据
            **params: 策略参数
            
        Returns:
            Dict[str, Any]: 信号字典
        """
        # 默认参数
        window = params.get('window', 20)
        multiplier = params.get('multiplier', 2.0)
        
        # 计算ATR
        high = data['high'].to_numpy()
        low = data['low'].to_numpy()
        close = data['close'].to_numpy()
        dates = data['date'].to_numpy()
        
        tr = np.zeros_like(close)
        for i in range(1, len(close)):
            tr[i] = max(high[i] - low[i], abs(high[i] - close[i-1]), abs(low[i] - close[i-1]))
        
        atr = np.zeros_like(close)
        for i in range(window, len(tr)):
            atr[i] = np.mean(tr[i-window:i])
        
        # 计算突破水平
        upper_band = close + multiplier * atr
        lower_band = close - multiplier * atr
        
        signals = []
        positions = []
        
        for i in range(window, len(close)):
            # 突破上轨
            if close[i] > upper_band[i-1]:
                signals.append({'date': dates[i], 'signal': 'buy', 'price': close[i], 'reason': '突破上轨'})
                positions.append(1)
            # 突破下轨
            elif close[i] < lower_band[i-1]:
                signals.append({'date': dates[i], 'signal': 'sell', 'price': close[i], 'reason': '突破下轨'})
                positions.append(0)
            else:
                # 保持之前的仓位
                positions.append(positions[-1] if positions else 0)
        
        return {
            'signals': signals,
            'positions': positions,
            'params': params
        }
