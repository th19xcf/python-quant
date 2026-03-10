#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
回测结果可视化工具，用于展示回测结果的图表
"""

from typing import Dict, Any
import plotly.graph_objects as go
from plotly.subplots import make_subplots


class BacktestVisualizer:
    """
    回测结果可视化类，提供回测结果的可视化功能
    """
    
    def visualize_backtest(self, backtest_results: Dict[str, Any]) -> go.Figure:
        """
        可视化回测结果
        
        Args:
            backtest_results: 回测结果
            
        Returns:
            go.Figure: 可视化图表
        """
        # 创建子图
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                          subplot_titles=('权益曲线', '交易信号'),
                          row_heights=[0.7, 0.3])
        
        # 提取数据
        equity_curve = backtest_results.get('equity_curve', [])
        trades = backtest_results.get('trades', [])
        strategy_name = backtest_results.get('strategy_name', 'Strategy')
        
        if equity_curve:
            # 绘制权益曲线
            dates = [item['date'] for item in equity_curve]
            equities = [item['equity'] for item in equity_curve]
            
            fig.add_trace(go.Scatter(x=dates, y=equities, name='权益', line=dict(color='blue')),
                         row=1, col=1)
        
        if trades:
            # 绘制交易信号
            buy_dates = []
            buy_prices = []
            sell_dates = []
            sell_prices = []
            
            for trade in trades:
                if trade['signal'] == 'buy':
                    buy_dates.append(trade['date'])
                    buy_prices.append(trade['price'])
                elif trade['signal'] == 'sell':
                    sell_dates.append(trade['date'])
                    sell_prices.append(trade['price'])
            
            fig.add_trace(go.Scatter(x=buy_dates, y=buy_prices, name='买入信号', 
                                   mode='markers', marker=dict(color='green', size=10, symbol='triangle-up')),
                         row=2, col=1)
            
            fig.add_trace(go.Scatter(x=sell_dates, y=sell_prices, name='卖出信号', 
                                   mode='markers', marker=dict(color='red', size=10, symbol='triangle-down')),
                         row=2, col=1)
        
        # 更新布局
        fig.update_layout(
            title=f'{strategy_name} 回测结果',
            xaxis_title='日期',
            yaxis_title='权益',
            yaxis2_title='价格',
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            height=800
        )
        
        return fig
    
    def visualize_performance(self, performance: Dict[str, Any]) -> go.Figure:
        """
        可视化绩效指标
        
        Args:
            performance: 绩效指标
            
        Returns:
            go.Figure: 可视化图表
        """
        # 创建饼图和柱状图
        fig = make_subplots(rows=1, cols=2, 
                          subplot_titles=('绩效指标', '交易统计'))
        
        # 提取绩效指标
        metrics = [
            {'name': '总收益率', 'value': performance.get('total_return', 0), 'unit': '%'},
            {'name': '年化收益率', 'value': performance.get('annual_return', 0), 'unit': '%'},
            {'name': '夏普比率', 'value': performance.get('sharpe_ratio', 0), 'unit': ''},
            {'name': '最大回撤', 'value': performance.get('max_drawdown', 0), 'unit': '%'},
            {'name': '波动率', 'value': performance.get('volatility', 0), 'unit': '%'}
        ]
        
        # 绘制绩效指标柱状图
        fig.add_trace(go.Bar(x=[m['name'] for m in metrics], 
                           y=[m['value'] for m in metrics],
                           text=[f"{m['value']:.2f}{m['unit']}" for m in metrics],
                           textposition='auto'),
                     row=1, col=1)
        
        # 绘制交易统计图
        trade_metrics = [
            {'name': '交易次数', 'value': performance.get('trades_count', 0)},
            {'name': '胜率', 'value': performance.get('winning_rate', 0), 'unit': '%'},
            {'name': '盈亏比', 'value': performance.get('average_profit_loss', 0), 'unit': ''}
        ]
        
        fig.add_trace(go.Bar(x=[m['name'] for m in trade_metrics], 
                           y=[m['value'] for m in trade_metrics],
                           text=[f"{m['value']:.2f}{m.get('unit', '')}" for m in trade_metrics],
                           textposition='auto'),
                     row=1, col=2)
        
        # 更新布局
        fig.update_layout(
            title='绩效分析',
            height=500
        )
        
        return fig
