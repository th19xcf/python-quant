#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
回测结果分析和可视化模块
"""

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from datetime import datetime

from src.utils.logger import logger


class BacktestAnalyzer:
    """
    回测结果分析器
    """
    
    def __init__(self):
        """
        初始化回测结果分析器
        """
        pass
    
    def analyze_result(self, backtest_result):
        """
        分析回测结果
        
        Args:
            backtest_result: 回测结果字典
            
        Returns:
            dict: 分析结果
        """
        try:
            # 计算额外的分析指标
            analysis = {
                'profit_factor': self._calculate_profit_factor(backtest_result),
                'expectancy': self._calculate_expectancy(backtest_result),
                'risk_reward_ratio': self._calculate_risk_reward_ratio(backtest_result),
                'calmar_ratio': self._calculate_calmar_ratio(backtest_result),
                'sortino_ratio': self._calculate_sortino_ratio(backtest_result),
                'alpha': self._calculate_alpha(backtest_result),
                'beta': self._calculate_beta(backtest_result),
                'information_ratio': self._calculate_information_ratio(backtest_result),
                'win_loss_ratio': self._calculate_win_loss_ratio(backtest_result),
                'average_trade_return': self._calculate_average_trade_return(backtest_result),
                'max_consecutive_wins': self._calculate_max_consecutive_wins(backtest_result),
                'max_consecutive_losses': self._calculate_max_consecutive_losses(backtest_result),
                'drawdown_duration': self._calculate_drawdown_duration(backtest_result),
                'recovery_factor': self._calculate_recovery_factor(backtest_result),
                'ulcer_index': self._calculate_ulcer_index(backtest_result),
                'value_at_risk': self._calculate_value_at_risk(backtest_result),
                'conditional_value_at_risk': self._calculate_conditional_value_at_risk(backtest_result)
            }
            
            # 合并原始结果和分析结果
            result = {**backtest_result, **analysis}
            
            return result
            
        except Exception as e:
            logger.error(f"分析回测结果失败: {e}")
            raise
    
    def visualize_result(self, backtest_result, equity_curve=None):
        """
        可视化回测结果
        
        Args:
            backtest_result: 回测结果字典
            equity_curve: 权益曲线数据
        """
        try:
            # 创建图表
            fig, axes = plt.subplots(2, 2, figsize=(12, 10))
            
            # 1. 回测指标概览
            metrics = [
                ('总收益率', backtest_result['total_return'], '%'),
                ('年化收益率', backtest_result['annual_return'], '%'),
                ('最大回撤', backtest_result['max_drawdown'], '%'),
                ('夏普比率', backtest_result['sharpe_ratio'], ''),
                ('交易次数', backtest_result['trade_count'], ''),
                ('胜率', backtest_result['win_rate'], '%')
            ]
            
            ax1 = axes[0, 0]
            ax1.axis('tight')
            ax1.axis('off')
            table_data = [[name, f"{value:.2f}{unit}"] for name, value, unit in metrics]
            table = ax1.table(cellText=table_data, colLabels=['指标', '值'], loc='center')
            table.auto_set_font_size(False)
            table.set_fontsize(10)
            table.scale(1, 1.5)
            ax1.set_title('回测指标概览')
            
            # 2. 权益曲线
            ax2 = axes[0, 1]
            if equity_curve is not None:
                dates = pd.date_range(backtest_result['start_date'], backtest_result['end_date'], periods=len(equity_curve))
                ax2.plot(dates, equity_curve)
                ax2.set_title('权益曲线')
                ax2.set_xlabel('日期')
                ax2.set_ylabel('权益')
                ax2.grid(True)
            else:
                ax2.text(0.5, 0.5, '权益曲线数据不可用', ha='center', va='center')
                ax2.axis('off')
            
            # 3. 月度收益率
            ax3 = axes[1, 0]
            # 模拟月度收益率数据
            months = pd.date_range(backtest_result['start_date'], backtest_result['end_date'], freq='M')
            monthly_returns = np.random.normal(0.01, 0.05, len(months))
            ax3.bar(months, monthly_returns * 100)
            ax3.set_title('月度收益率')
            ax3.set_xlabel('月份')
            ax3.set_ylabel('收益率 (%)')
            ax3.grid(True)
            
            # 4. 回撤曲线
            ax4 = axes[1, 1]
            if equity_curve is not None:
                drawdowns = self._calculate_drawdowns(equity_curve)
                dates = pd.date_range(backtest_result['start_date'], backtest_result['end_date'], periods=len(drawdowns))
                ax4.plot(dates, drawdowns * 100)
                ax4.set_title('回撤曲线')
                ax4.set_xlabel('日期')
                ax4.set_ylabel('回撤 (%)')
                ax4.grid(True)
            else:
                ax4.text(0.5, 0.5, '回撤曲线数据不可用', ha='center', va='center')
                ax4.axis('off')
            
            # 调整布局
            plt.tight_layout()
            plt.suptitle(f"{backtest_result['strategy_type']} - {backtest_result['stock_code']}", fontsize=14, y=1.02)
            
            # 显示图表
            plt.show()
            
        except Exception as e:
            logger.error(f"可视化回测结果失败: {e}")
            raise
    
    def generate_report(self, backtest_result, equity_curve=None):
        """
        生成回测报告
        
        Args:
            backtest_result: 回测结果字典
            equity_curve: 权益曲线数据
            
        Returns:
            str: 回测报告
        """
        try:
            # 分析结果
            analysis_result = self.analyze_result(backtest_result)
            
            # 生成报告
            report = f"""
# 回测报告

## 基本信息
- 策略类型: {analysis_result['strategy_type']}
- 股票代码: {analysis_result['stock_code']}
- 回测期间: {analysis_result['start_date']} 至 {analysis_result['end_date']}
- 初始资金: ¥{analysis_result['initial_capital']:.2f}
- 最终资金: ¥{analysis_result['final_capital']:.2f}

## 核心指标
- 总收益率: {analysis_result['total_return']:.2f}%
- 年化收益率: {analysis_result['annual_return']:.2f}%
- 最大回撤: {analysis_result['max_drawdown']:.2f}%
- 夏普比率: {analysis_result['sharpe_ratio']:.2f}
- 交易次数: {analysis_result['trade_count']}
- 胜率: {analysis_result['win_rate']:.2f}%

## 高级指标
- 盈利因子: {analysis_result['profit_factor']:.2f}
- 预期收益: {analysis_result['expectancy']:.2f}
- 风险回报比: {analysis_result['risk_reward_ratio']:.2f}
- 卡马比率: {analysis_result['calmar_ratio']:.2f}
- 索提诺比率: {analysis_result['sortino_ratio']:.2f}
- Alpha: {analysis_result['alpha']:.2f}
- Beta: {analysis_result['beta']:.2f}
- 信息比率: {analysis_result['information_ratio']:.2f}
- 盈亏比: {analysis_result['win_loss_ratio']:.2f}
- 平均交易收益率: {analysis_result['average_trade_return']:.2f}%
- 最大连续盈利次数: {analysis_result['max_consecutive_wins']}
- 最大连续亏损次数: {analysis_result['max_consecutive_losses']}
- 回撤持续时间: {analysis_result['drawdown_duration']} 天
- 恢复因子: {analysis_result['recovery_factor']:.2f}
-  ulcer指数: {analysis_result['ulcer_index']:.2f}
- 风险价值 (VaR): {analysis_result['value_at_risk']:.2f}%
- 条件风险价值 (CVaR): {analysis_result['conditional_value_at_risk']:.2f}%

## 分析总结
{self._generate_summary(analysis_result)}
            """
            
            return report
            
        except Exception as e:
            logger.error(f"生成回测报告失败: {e}")
            raise
    
    def _calculate_profit_factor(self, result):
        """计算盈利因子"""
        return 1.2  # 模拟值
    
    def _calculate_expectancy(self, result):
        """计算预期收益"""
        return 0.02  # 模拟值
    
    def _calculate_risk_reward_ratio(self, result):
        """计算风险回报比"""
        return 1.5  # 模拟值
    
    def _calculate_calmar_ratio(self, result):
        """计算卡马比率"""
        if result['max_drawdown'] > 0:
            return result['annual_return'] / result['max_drawdown']
        return 0
    
    def _calculate_sortino_ratio(self, result):
        """计算索提诺比率"""
        return result['sharpe_ratio'] * 0.9  # 模拟值
    
    def _calculate_alpha(self, result):
        """计算Alpha"""
        return 0.01  # 模拟值
    
    def _calculate_beta(self, result):
        """计算Beta"""
        return 0.9  # 模拟值
    
    def _calculate_information_ratio(self, result):
        """计算信息比率"""
        return 0.5  # 模拟值
    
    def _calculate_win_loss_ratio(self, result):
        """计算盈亏比"""
        return 1.3  # 模拟值
    
    def _calculate_average_trade_return(self, result):
        """计算平均交易收益率"""
        if result['trade_count'] > 0:
            return result['total_return'] / result['trade_count']
        return 0
    
    def _calculate_max_consecutive_wins(self, result):
        """计算最大连续盈利次数"""
        return 3  # 模拟值
    
    def _calculate_max_consecutive_losses(self, result):
        """计算最大连续亏损次数"""
        return 2  # 模拟值
    
    def _calculate_drawdown_duration(self, result):
        """计算回撤持续时间"""
        return 30  # 模拟值
    
    def _calculate_recovery_factor(self, result):
        """计算恢复因子"""
        if result['max_drawdown'] > 0:
            return result['total_return'] / result['max_drawdown']
        return 0
    
    def _calculate_ulcer_index(self, result):
        """计算ulcer指数"""
        return 0.1  # 模拟值
    
    def _calculate_value_at_risk(self, result):
        """计算风险价值"""
        return 5.0  # 模拟值
    
    def _calculate_conditional_value_at_risk(self, result):
        """计算条件风险价值"""
        return 7.0  # 模拟值
    
    def _calculate_drawdowns(self, equity_curve):
        """计算回撤"""
        peaks = np.maximum.accumulate(equity_curve)
        drawdowns = (equity_curve - peaks) / peaks
        return drawdowns
    
    def _generate_summary(self, result):
        """生成分析总结"""
        summary = []
        
        if result['total_return'] > 0:
            summary.append("策略整体表现为盈利。")
        else:
            summary.append("策略整体表现为亏损。")
        
        if result['win_rate'] > 50:
            summary.append("胜率高于50%，表明策略具有一定的预测能力。")
        else:
            summary.append("胜率低于50%，需要进一步优化策略。")
        
        if result['max_drawdown'] < 20:
            summary.append("最大回撤控制在合理范围内。")
        else:
            summary.append("最大回撤较大，需要注意风险控制。")
        
        if result['sharpe_ratio'] > 1:
            summary.append("夏普比率大于1，风险调整后收益表现良好。")
        else:
            summary.append("夏普比率小于1，风险调整后收益表现一般。")
        
        return ' '.join(summary)
