#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
高级绩效分析器，用于计算回测的各种绩效指标和风险评估
"""

from typing import List, Dict, Any, Optional
import numpy as np
import pandas as pd


class AdvancedPerformanceAnalyzer:
    """
    高级绩效分析器类，提供更全面的回测绩效指标计算功能
    """
    
    def __init__(self, risk_free_rate: float = 0.02):
        """
        初始化高级绩效分析器
        
        Args:
            risk_free_rate: 无风险利率
        """
        self.risk_free_rate = risk_free_rate
    
    def analyze(self, equity_curve: List[Dict[str, Any]], trades: List[Dict[str, Any]], benchmark_returns: Optional[List[float]] = None) -> Dict[str, Any]:
        """
        分析回测绩效
        
        Args:
            equity_curve: 权益曲线
            trades: 交易记录
            benchmark_returns: 基准收益率序列
            
        Returns:
            Dict[str, Any]: 详细的绩效分析结果
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
            sortino_ratio = self._calculate_sortino_ratio(returns)
            calmar_ratio = self._calculate_calmar_ratio(returns, equities)
            max_drawdown = self._calculate_max_drawdown(equities)
            drawdown_details = self._calculate_drawdown_details(equities, dates)
            volatility = np.std(returns) * np.sqrt(252) * 100
            downside_risk = self._calculate_downside_risk(returns)
            winning_rate = self._calculate_winning_rate(trades)
            average_profit_loss = self._calculate_average_profit_loss(trades)
            profit_factor = self._calculate_profit_factor(trades)
            expectancy = self._calculate_expectancy(trades)
            sharpe_ratio_1y = self._calculate_sharpe_ratio(returns[-252:]) if len(returns) >= 252 else 0
            sharpe_ratio_6m = self._calculate_sharpe_ratio(returns[-126:]) if len(returns) >= 126 else 0
            
            # 计算阿尔法和贝塔（如果提供了基准收益率）
            alpha = 0
            beta = 0
            information_ratio = 0
            if benchmark_returns and len(benchmark_returns) == len(returns):
                alpha, beta = self._calculate_alpha_beta(returns, benchmark_returns)
                information_ratio = self._calculate_information_ratio(returns, benchmark_returns)
        else:
            sharpe_ratio = 0
            sortino_ratio = 0
            calmar_ratio = 0
            max_drawdown = 0
            drawdown_details = {}
            volatility = 0
            downside_risk = 0
            winning_rate = 0
            average_profit_loss = 0
            profit_factor = 0
            expectancy = 0
            sharpe_ratio_1y = 0
            sharpe_ratio_6m = 0
            alpha = 0
            beta = 0
            information_ratio = 0
        
        # 计算交易相关指标
        trade_count = len(trades) // 2  # 每笔完整交易包含买入和卖出
        avg_holding_period = self._calculate_average_holding_period(trades, dates)
        turnover_rate = self._calculate_turnover_rate(trades, initial_equity)
        
        # 构建详细的分析结果
        analysis_result = {
            # 基本指标
            'total_return': total_return,
            'annual_return': annual_return,
            'trade_count': trade_count,
            
            # 风险调整收益指标
            'sharpe_ratio': sharpe_ratio,
            'sortino_ratio': sortino_ratio,
            'calmar_ratio': calmar_ratio,
            'information_ratio': information_ratio,
            'sharpe_ratio_1y': sharpe_ratio_1y,
            'sharpe_ratio_6m': sharpe_ratio_6m,
            
            # 风险指标
            'volatility': volatility,
            'downside_risk': downside_risk,
            'max_drawdown': max_drawdown,
            'drawdown_details': drawdown_details,
            'alpha': alpha,
            'beta': beta,
            
            # 交易指标
            'winning_rate': winning_rate,
            'average_profit_loss': average_profit_loss,
            'profit_factor': profit_factor,
            'expectancy': expectancy,
            'avg_holding_period': avg_holding_period,
            'turnover_rate': turnover_rate
        }
        
        return analysis_result
    
    def _calculate_sharpe_ratio(self, returns: List[float]) -> float:
        """
        计算夏普比率
        
        Args:
            returns: 收益率序列
            
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
        sharpe_ratio = (avg_return - self.risk_free_rate / 252) / std_return * np.sqrt(252)
        return sharpe_ratio
    
    def _calculate_sortino_ratio(self, returns: List[float]) -> float:
        """
        计算索提诺比率
        
        Args:
            returns: 收益率序列
            
        Returns:
            float: 索提诺比率
        """
        if not returns:
            return 0
        
        avg_return = np.mean(returns)
        
        # 计算下行风险
        downside_returns = [r for r in returns if r < 0]
        if not downside_returns:
            return 0
        
        downside_std = np.std(downside_returns)
        
        if downside_std == 0:
            return 0
        
        # 年化处理
        sortino_ratio = (avg_return - self.risk_free_rate / 252) / downside_std * np.sqrt(252)
        return sortino_ratio
    
    def _calculate_calmar_ratio(self, returns: List[float], equities: List[float]) -> float:
        """
        计算卡马比率
        
        Args:
            returns: 收益率序列
            equities: 权益序列
            
        Returns:
            float: 卡马比率
        """
        if not returns:
            return 0
        
        # 计算年化收益率
        annual_return = (pow(equities[-1] / equities[0], 252 / len(equities)) - 1) * 100
        
        # 计算最大回撤
        max_drawdown = self._calculate_max_drawdown(equities)
        
        if max_drawdown == 0:
            return 0
        
        # 卡马比率 = 年化收益率 / 最大回撤
        calmar_ratio = annual_return / max_drawdown
        return calmar_ratio
    
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
    
    def _calculate_drawdown_details(self, equities: List[float], dates: List[Any]) -> Dict[str, Any]:
        """
        计算回撤详细信息
        
        Args:
            equities: 权益序列
            dates: 日期序列
            
        Returns:
            Dict[str, Any]: 回撤详细信息
        """
        if not equities or not dates:
            return {}
        
        drawdowns = []
        peak = equities[0]
        peak_date = dates[0]
        
        for i, equity in enumerate(equities):
            if equity > peak:
                peak = equity
                peak_date = dates[i]
            else:
                drawdown = (peak - equity) / peak * 100
                drawdowns.append({
                    'drawdown': drawdown,
                    'peak_date': peak_date,
                    'trough_date': dates[i]
                })
        
        if not drawdowns:
            return {
                'max_drawdown': 0,
                'peak_date': None,
                'trough_date': None,
                'recovery_date': None,
                'drawdown_duration': 0,
                'recovery_duration': 0
            }
        
        # 找到最大回撤
        max_drawdown_info = max(drawdowns, key=lambda x: x['drawdown'])
        
        # 计算回撤持续时间和恢复时间（简化处理）
        drawdown_duration = 0
        recovery_duration = 0
        recovery_date = None
        
        # 尝试找到恢复日期
        peak_value = equities[dates.index(max_drawdown_info['peak_date'])]
        for i in range(dates.index(max_drawdown_info['trough_date']), len(equities)):
            if equities[i] >= peak_value:
                recovery_date = dates[i]
                recovery_duration = (recovery_date - dates[i-1]).days
                break
        
        if max_drawdown_info['peak_date'] and max_drawdown_info['trough_date']:
            drawdown_duration = (max_drawdown_info['trough_date'] - max_drawdown_info['peak_date']).days
        
        return {
            'max_drawdown': max_drawdown_info['drawdown'],
            'peak_date': max_drawdown_info['peak_date'],
            'trough_date': max_drawdown_info['trough_date'],
            'recovery_date': recovery_date,
            'drawdown_duration': drawdown_duration,
            'recovery_duration': recovery_duration
        }
    
    def _calculate_downside_risk(self, returns: List[float]) -> float:
        """
        计算下行风险
        
        Args:
            returns: 收益率序列
            
        Returns:
            float: 下行风险（年化）
        """
        if not returns:
            return 0
        
        # 计算下行偏差
        downside_returns = [r for r in returns if r < 0]
        if not downside_returns:
            return 0
        
        downside_std = np.std(downside_returns)
        
        # 年化处理
        annual_downside_risk = downside_std * np.sqrt(252) * 100
        return annual_downside_risk
    
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
    
    def _calculate_profit_factor(self, trades: List[Dict[str, Any]]) -> float:
        """
        计算盈利因子
        
        Args:
            trades: 交易记录
            
        Returns:
            float: 盈利因子
        """
        if not trades:
            return 0
        
        total_profit = 0
        total_loss = 0
        
        for i in range(0, len(trades), 2):
            if i + 1 < len(trades):
                buy_trade = trades[i]
                sell_trade = trades[i+1]
                profit = (sell_trade['price'] - buy_trade['price']) * buy_trade['shares']
                if profit > 0:
                    total_profit += profit
                else:
                    total_loss += abs(profit)
        
        return total_profit / total_loss if total_loss > 0 else 0
    
    def _calculate_expectancy(self, trades: List[Dict[str, Any]]) -> float:
        """
        计算期望值
        
        Args:
            trades: 交易记录
            
        Returns:
            float: 期望值
        """
        if not trades:
            return 0
        
        total_profit = 0
        trade_count = len(trades) // 2
        
        for i in range(0, len(trades), 2):
            if i + 1 < len(trades):
                buy_trade = trades[i]
                sell_trade = trades[i+1]
                profit = (sell_trade['price'] - buy_trade['price']) * buy_trade['shares']
                total_profit += profit
        
        return total_profit / trade_count if trade_count > 0 else 0
    
    def _calculate_alpha_beta(self, returns: List[float], benchmark_returns: List[float]) -> tuple:
        """
        计算阿尔法和贝塔
        
        Args:
            returns: 策略收益率序列
            benchmark_returns: 基准收益率序列
            
        Returns:
            tuple: (alpha, beta)
        """
        if not returns or not benchmark_returns or len(returns) != len(benchmark_returns):
            return 0, 0
        
        # 计算贝塔
        covariance = np.cov(returns, benchmark_returns)[0, 1]
        benchmark_variance = np.var(benchmark_returns)
        
        if benchmark_variance == 0:
            return 0, 0
        
        beta = covariance / benchmark_variance
        
        # 计算阿尔法
        avg_return = np.mean(returns)
        avg_benchmark_return = np.mean(benchmark_returns)
        alpha = (avg_return - self.risk_free_rate / 252) - beta * (avg_benchmark_return - self.risk_free_rate / 252)
        
        # 年化处理
        annual_alpha = alpha * 252 * 100
        
        return annual_alpha, beta
    
    def _calculate_information_ratio(self, returns: List[float], benchmark_returns: List[float]) -> float:
        """
        计算信息比率
        
        Args:
            returns: 策略收益率序列
            benchmark_returns: 基准收益率序列
            
        Returns:
            float: 信息比率
        """
        if not returns or not benchmark_returns or len(returns) != len(benchmark_returns):
            return 0
        
        # 计算超额收益
        excess_returns = [r - b for r, b in zip(returns, benchmark_returns)]
        
        # 计算超额收益均值和标准差
        avg_excess_return = np.mean(excess_returns)
        std_excess_return = np.std(excess_returns)
        
        if std_excess_return == 0:
            return 0
        
        # 年化处理
        information_ratio = avg_excess_return / std_excess_return * np.sqrt(252)
        return information_ratio
    
    def _calculate_average_holding_period(self, trades: List[Dict[str, Any]], dates: List[Any]) -> float:
        """
        计算平均持有周期
        
        Args:
            trades: 交易记录
            dates: 日期序列
            
        Returns:
            float: 平均持有周期（天）
        """
        if not trades or len(trades) < 2:
            return 0
        
        holding_periods = []
        
        for i in range(0, len(trades), 2):
            if i + 1 < len(trades):
                buy_date = trades[i]['date']
                sell_date = trades[i+1]['date']
                # 计算持有天数
                try:
                    holding_days = (sell_date - buy_date).days
                    holding_periods.append(holding_days)
                except:
                    pass
        
        return np.mean(holding_periods) if holding_periods else 0
    
    def _calculate_turnover_rate(self, trades: List[Dict[str, Any]], initial_capital: float) -> float:
        """
        计算换手率
        
        Args:
            trades: 交易记录
            initial_capital: 初始资金
            
        Returns:
            float: 换手率（百分比）
        """
        if not trades or initial_capital == 0:
            return 0
        
        # 计算总交易量
        total_trade_value = 0
        for trade in trades:
            if 'price' in trade and 'shares' in trade:
                total_trade_value += trade['price'] * trade['shares']
        
        # 换手率 = 总交易量 / (2 * 初始资金) * 100
        # 除以2是因为每笔交易包含买入和卖出
        turnover_rate = (total_trade_value / (2 * initial_capital)) * 100
        return turnover_rate
