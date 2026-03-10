#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
策略可视化工具，用于展示策略分析结果和信号
"""

from typing import Dict, Any, Optional
import polars as pl
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np


class StrategyVisualizer:
    """
    策略可视化类，提供策略结果的可视化功能
    """
    
    def visualize_strategy(self, data: pl.DataFrame, signals: Dict[str, Any], strategy_type: str, **params) -> go.Figure:
        """
        可视化策略结果
        
        Args:
            data: 包含价格和指标数据
            signals: 策略信号
            strategy_type: 策略类型
            **params: 可视化参数
            
        Returns:
            go.Figure: 可视化图表
        """
        # 创建子图
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                          subplot_titles=(f'{strategy_type}策略信号', '成交量'),
                          row_heights=[0.7, 0.3])
        
        # 绘制价格线
        dates = data['date'].to_numpy()
        close = data['close'].to_numpy()
        fig.add_trace(go.Scatter(x=dates, y=close, name='收盘价', line=dict(color='blue')),
                     row=1, col=1)
        
        # 绘制信号点
        signal_data = signals.get('signals', [])
        buy_dates = [s['date'] for s in signal_data if s['signal'] == 'buy']
        buy_prices = [s['price'] for s in signal_data if s['signal'] == 'buy']
        sell_dates = [s['date'] for s in signal_data if s['signal'] == 'sell']
        sell_prices = [s['price'] for s in signal_data if s['signal'] == 'sell']
        
        fig.add_trace(go.Scatter(x=buy_dates, y=buy_prices, name='买入信号', 
                               mode='markers', marker=dict(color='green', size=10, symbol='triangle-up')),
                     row=1, col=1)
        
        fig.add_trace(go.Scatter(x=sell_dates, y=sell_prices, name='卖出信号', 
                               mode='markers', marker=dict(color='red', size=10, symbol='triangle-down')),
                     row=1, col=1)
        
        # 绘制成交量
        volume = data['volume'].to_numpy()
        fig.add_trace(go.Bar(x=dates, y=volume, name='成交量', marker=dict(color='gray', opacity=0.5)),
                     row=2, col=1)
        
        # 绘制指标
        self._add_indicators(fig, data, strategy_type, **params)
        
        # 更新布局
        fig.update_layout(
            title=f'{strategy_type}策略分析',
            xaxis_title='日期',
            yaxis_title='价格',
            yaxis2_title='成交量',
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            height=800
        )
        
        return fig
    
    def _add_indicators(self, fig: go.Figure, data: pl.DataFrame, strategy_type: str, **params):
        """
        添加指标到图表
        
        Args:
            fig: 图表对象
            data: 包含指标数据
            strategy_type: 策略类型
            **params: 可视化参数
        """
        dates = data['date'].to_numpy()
        
        if strategy_type == 'trend_following':
            # 添加MA指标
            ma_short = params.get('ma_short', 10)
            ma_long = params.get('ma_long', 20)
            if f'ma{ma_short}' in data.columns:
                fig.add_trace(go.Scatter(x=dates, y=data[f'ma{ma_short}'].to_numpy(), 
                                       name=f'MA{ma_short}', line=dict(color='orange', width=1)),
                             row=1, col=1)
            if f'ma{ma_long}' in data.columns:
                fig.add_trace(go.Scatter(x=dates, y=data[f'ma{ma_long}'].to_numpy(), 
                                       name=f'MA{ma_long}', line=dict(color='purple', width=1)),
                             row=1, col=1)
        
        elif strategy_type == 'mean_reversion':
            # 添加RSI指标
            rsi_period = params.get('rsi_period', 14)
            if f'rsi{rsi_period}' in data.columns:
                # 创建RSI子图
                fig.add_trace(go.Scatter(x=dates, y=data[f'rsi{rsi_period}'].to_numpy(), 
                                       name=f'RSI{rsi_period}', line=dict(color='purple')),
                             row=1, col=1)
                # 添加超买超卖线
                overbought = params.get('overbought', 70)
                oversold = params.get('oversold', 30)
                fig.add_hline(y=overbought, line=dict(color='red', dash='dash'), row=1, col=1)
                fig.add_hline(y=oversold, line=dict(color='green', dash='dash'), row=1, col=1)
        
        elif strategy_type == 'momentum':
            # 添加动量指标
            momentum_period = params.get('momentum_period', 12)
            close = data['close'].to_numpy()
            momentum = np.zeros_like(close)
            for i in range(momentum_period, len(close)):
                momentum[i] = (close[i] - close[i - momentum_period]) / close[i - momentum_period] * 100
            
            # 创建动量子图
            fig.add_trace(go.Scatter(x=dates, y=momentum, name='动量', line=dict(color='green')),
                         row=1, col=1)
            fig.add_hline(y=0, line=dict(color='gray', dash='dash'), row=1, col=1)
        
        elif strategy_type == 'volatility_breakout':
            # 添加波动率突破指标
            window = params.get('window', 20)
            multiplier = params.get('multiplier', 2.0)
            
            high = data['high'].to_numpy()
            low = data['low'].to_numpy()
            close = data['close'].to_numpy()
            
            # 计算ATR
            tr = np.zeros_like(close)
            for i in range(1, len(close)):
                tr[i] = max(high[i] - low[i], abs(high[i] - close[i-1]), abs(low[i] - close[i-1]))
            
            atr = np.zeros_like(close)
            for i in range(window, len(tr)):
                atr[i] = np.mean(tr[i-window:i])
            
            # 计算突破水平
            upper_band = close + multiplier * atr
            lower_band = close - multiplier * atr
            
            fig.add_trace(go.Scatter(x=dates, y=upper_band, name='上轨', line=dict(color='red', dash='dash')),
                         row=1, col=1)
            fig.add_trace(go.Scatter(x=dates, y=lower_band, name='下轨', line=dict(color='green', dash='dash')),
                         row=1, col=1)
    
    def visualize_multi_indicator(self, data: pl.DataFrame, indicators: list, weights: Optional[dict] = None) -> go.Figure:
        """
        可视化多指标分析结果
        
        Args:
            data: 包含指标数据
            indicators: 指标列表
            weights: 指标权重
            
        Returns:
            go.Figure: 可视化图表
        """
        # 创建子图
        fig = make_subplots(rows=len(indicators) + 1, cols=1, shared_xaxes=True, 
                          subplot_titles=['收盘价'] + indicators)
        
        dates = data['date'].to_numpy()
        close = data['close'].to_numpy()
        
        # 绘制收盘价
        fig.add_trace(go.Scatter(x=dates, y=close, name='收盘价', line=dict(color='blue')),
                     row=1, col=1)
        
        # 绘制各个指标
        for i, indicator in enumerate(indicators, start=2):
            if indicator in data.columns:
                values = data[indicator].to_numpy()
                fig.add_trace(go.Scatter(x=dates, y=values, name=indicator),
                             row=i, col=1)
        
        # 更新布局
        fig.update_layout(
            title='多指标分析',
            xaxis_title='日期',
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            height=300 * (len(indicators) + 1)
        )
        
        return fig
