#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
绩效分析器，用于计算回测的各种绩效指标
"""

from typing import List, Dict, Any
import numpy as np


class PerformanceAnalyzer:
    """
    绩效分析器类，提供回测绩效指标计算功能
    """
    
    def analyze(self, equity_curve: List[Dict[str, Any]], trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        分析回测绩效
        
        Args:
            equity_curve: 权益曲线
            trades: 交易记录
            
        Returns:
            Dict[str, Any]: 绩效分析结果
        """
        if not equity_curve:
            return {}
        
        # 提取权益数据
        equities = [item['equity'] for item in equity_curve]
        dates = [item['date'] for item in equity_curve]
        
        # 计算基本指标
        initial_equity = equities[0]
        final_equity = equities[-1]
        total_return = (final_equity - initial_equity) / initial_equity * 100
        
        # 计算年化收益率
        days = len(equities)
        annual_return = (pow((final_equity / initial_equity), 252 / days) - 1) * 100
        
        # 计算收益率序列
        returns = []
        for i in range(1, len(equities)):
            daily_return = (equities[i] - equities[i-1]) / equities[i-1]
            returns.append(daily_return)
        
        # 计算风险指标
        if returns:
            sharpe_ratio = self._calculate_sharpe_ratio(returns)
            max_drawdown = self._calculate_max_drawdown(equities)
            volatility = np.std(returns) * np.sqrt(252) * 100
            winning_rate = self._calculate_winning_rate(trades)
            average_profit_loss = self._calculate_average_profit_loss(trades)
        else:
            sharpe_ratio = 0
            max_drawdown = 0
            volatility = 0
            winning_rate = 0
            average_profit_loss = 0
        
        return {
            'total_return': total_return,
            'annual_return': annual_return,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'volatility': volatility,
            'winning_rate': winning_rate,
            'average_profit_loss': average_profit_loss,
            'trades_count': len(trades) // 2  # 每笔完整交易包含买入和卖出
        }
    
    def _calculate_sharpe_ratio(self, returns: List[float], risk_free_rate: float = 0) -> float:
        """
        计算夏普比率
        
        Args:
            returns: 收益率序列
            risk_free_rate: 无风险利率
            
        Returns:
            float: 夏普比率
        """
        if not returns:
            return 0
        
        avg_return = np.mean(returns)
        std_return = np.std(returns)
        
        if std_return == 0:
            return 0
        
        # 年化处理
        sharpe_ratio = (avg_return - risk_free_rate / 252) / std_return * np.sqrt(252)
        return sharpe_ratio
    
    def _calculate_max_drawdown(self, equities: List[float]) -> float:
        """
        计算最大回撤
        
        Args:
            equities: 权益序列
            
        Returns:
            float: 最大回撤（百分比）
        """
        if not equities:
            return 0
        
        max_drawdown = 0
        peak = equities[0]
        
        for equity in equities:
            if equity > peak:
                peak = equity
            drawdown = (peak - equity) / peak * 100
            if drawdown > max_drawdown:
                max_drawdown = drawdown
        
        return max_drawdown
    
    def _calculate_winning_rate(self, trades: List[Dict[str, Any]]) -> float:
        """
        计算胜率
        
        Args:
            trades: 交易记录
            
        Returns:
            float: 胜率（百分比）
        """
        if not trades:
            return 0
        
        winning_trades = 0
        for i in range(0, len(trades), 2):
            if i + 1 < len(trades):
                buy_trade = trades[i]
                sell_trade = trades[i+1]
                if sell_trade['price'] > buy_trade['price']:
                    winning_trades += 1
        
        return (winning_trades / (len(trades) // 2)) * 100 if len(trades) >= 2 else 0
    
    def _calculate_average_profit_loss(self, trades: List[Dict[str, Any]]) -> float:
        """
        计算平均盈亏比
        
        Args:
            trades: 交易记录
            
        Returns:
            float: 平均盈亏比
        """
        if not trades:
            return 0
        
        profits = []
        losses = []
        
        for i in range(0, len(trades), 2):
            if i + 1 < len(trades):
                buy_trade = trades[i]
                sell_trade = trades[i+1]
                profit = (sell_trade['price'] - buy_trade['price']) * buy_trade['shares']
                if profit > 0:
                    profits.append(profit)
                else:
                    losses.append(abs(profit))
        
        avg_profit = np.mean(profits) if profits else 0
        avg_loss = np.mean(losses) if losses else 1
        
        return avg_profit / avg_loss if avg_loss > 0 else 0
