#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
组合回测引擎，用于执行多股票组合策略回测
"""

from typing import Dict, Any, List, Optional
import polars as pl
import pandas as pd
import numpy as np
from loguru import logger

from src.backtest.strategies.base_strategy import BaseStrategy
from src.backtest.costs.cost_model import CostModel
from src.backtest.performance.performance_analyzer import PerformanceAnalyzer
from src.backtest.visualization.backtest_visualizer import BacktestVisualizer


class PortfolioBacktestEngine:
    """
    组合回测引擎类，提供多股票组合策略回测功能
    """
    
    def __init__(self, data_dict: Dict[str, Any]):
        """
        初始化组合回测引擎
        
        Args:
            data_dict: 股票数据字典，键为股票代码，值为Polars DataFrame、LazyFrame或Pandas DataFrame
        """
        # 转换为Polars DataFrame
        self.data_dict = {}
        for stock_code, data in data_dict.items():
            if isinstance(data, pd.DataFrame):
                self.data_dict[stock_code] = pl.from_pandas(data)
            elif hasattr(data, 'to_pandas'):
                self.data_dict[stock_code] = data
            else:
                self.data_dict[stock_code] = pl.DataFrame(data)
        
        # 初始化策略字典
        self.strategy_dict = {}
        # 初始化成本模型
        self.cost_model = CostModel()
        # 初始化性能分析器
        self.performance_analyzer = PerformanceAnalyzer()
        # 初始化可视化工具
        self.visualizer = BacktestVisualizer()
        
        # 存储回测结果
        self.backtest_results = {}
        # 存储交易记录
        self.trades = []
        # 存储资金曲线
        self.equity_curve = []
        # 存储各股票持仓
        self.positions = {}
        # 存储各股票策略
        self.strategies = {}
    
    def set_strategy(self, stock_code: str, strategy: BaseStrategy):
        """
        为特定股票设置策略
        
        Args:
            stock_code: 股票代码
            strategy: 策略对象
        """
        self.strategies[stock_code] = strategy
    
    def set_cost_model(self, cost_model: CostModel):
        """
        设置成本模型
        
        Args:
            cost_model: 成本模型对象
        """
        self.cost_model = cost_model
    
    def run_backtest(self, initial_capital: float = 1000000.0, allocation_strategy: str = 'equal', **params) -> Dict[str, Any]:
        """
        运行组合回测
        
        Args:
            initial_capital: 初始资金
            allocation_strategy: 资金分配策略，可选值：'equal'（等权分配）、'value'（价值权重）
            **params: 回测参数
            
        Returns:
            Dict[str, Any]: 回测结果
        """
        if not self.strategies:
            logger.error("未设置策略")
            return {}
        
        # 重置回测结果
        self.trades = []
        self.equity_curve = []
        self.positions = {stock: 0 for stock in self.data_dict.keys()}
        
        # 初始化回测参数
        current_capital = initial_capital
        stock_capitals = {}
        
        # 根据分配策略分配资金
        if allocation_strategy == 'equal':
            # 等权分配
            stock_count = len(self.data_dict)
            for stock in self.data_dict.keys():
                stock_capitals[stock] = initial_capital / stock_count
        elif allocation_strategy == 'value':
            # 价值权重分配（这里简化处理，实际应该基于市值）
            stock_count = len(self.data_dict)
            for stock in self.data_dict.keys():
                stock_capitals[stock] = initial_capital / stock_count
        else:
            # 默认等权分配
            stock_count = len(self.data_dict)
            for stock in self.data_dict.keys():
                stock_capitals[stock] = initial_capital / stock_count
        
        # 获取所有股票的日期范围
        all_dates = set()
        for stock, data in self.data_dict.items():
            dates = data['date'].to_list()
            all_dates.update(dates)
        
        # 按日期排序
        sorted_dates = sorted(all_dates)
        
        # 执行回测
        for date in sorted_dates:
            daily_trades = []
            
            # 处理每个股票
            for stock_code, data in self.data_dict.items():
                # 检查该股票在当天是否有数据
                stock_data = data.filter(pl.col('date') == date)
                if stock_data.is_empty():
                    continue
                
                # 获取当前数据
                current_data = stock_data.to_dict(as_series=False)
                current_data = {k: v[0] for k, v in current_data.items()}
                
                # 生成信号
                strategy = self.strategies.get(stock_code)
                if strategy:
                    signal = strategy.generate_signal(current_data, 0)  # 简化处理，实际应该传递正确的索引
                    
                    # 执行交易
                    if signal == 'buy' and self.positions[stock_code] == 0:
                        # 买入
                        current_price = current_data['close']
                        shares = stock_capitals[stock_code] / current_price
                        self.positions[stock_code] = shares
                        stock_capitals[stock_code] = 0
                        
                        # 记录交易
                        trade = {
                            'date': date,
                            'stock_code': stock_code,
                            'signal': 'buy',
                            'price': current_price,
                            'shares': shares,
                            'capital': stock_capitals[stock_code],
                            'position': self.positions[stock_code]
                        }
                        daily_trades.append(trade)
                    elif signal == 'sell' and self.positions[stock_code] > 0:
                        # 卖出
                        sell_price = current_data['close']
                        stock_capitals[stock_code] = self.positions[stock_code] * sell_price
                        # 扣除交易成本
                        cost = self.cost_model.calculate_cost(self.positions[stock_code], current_data['close'], sell_price)
                        stock_capitals[stock_code] -= cost
                        
                        # 记录交易
                        trade = {
                            'date': date,
                            'stock_code': stock_code,
                            'signal': 'sell',
                            'price': sell_price,
                            'shares': self.positions[stock_code],
                            'capital': stock_capitals[stock_code],
                            'position': 0,
                            'cost': cost
                        }
                        daily_trades.append(trade)
                        
                        self.positions[stock_code] = 0
            
            # 记录当日交易
            self.trades.extend(daily_trades)
            
            # 计算当前权益
            current_equity = current_capital
            for stock_code, position in self.positions.items():
                if position > 0:
                    # 获取当日收盘价
                    stock_data = self.data_dict[stock_code].filter(pl.col('date') == date)
                    if not stock_data.is_empty():
                        close_price = stock_data['close'][0]
                        current_equity += position * close_price
            
            # 加上各股票的可用资金
            current_equity += sum(stock_capitals.values())
            
            # 记录权益曲线
            self.equity_curve.append({
                'date': date,
                'equity': current_equity
            })
        
        # 计算回测结果
        final_equity = self.equity_curve[-1]['equity'] if self.equity_curve else initial_capital
        total_return = (final_equity - initial_capital) / initial_capital * 100
        
        # 计算性能指标
        performance = self.performance_analyzer.analyze(self.equity_curve, self.trades)
        
        # 存储回测结果
        self.backtest_results = {
            'initial_capital': initial_capital,
            'final_equity': final_equity,
            'total_return': total_return,
            'trades': self.trades,
            'equity_curve': self.equity_curve,
            'performance': performance,
            'strategy_names': {stock: strategy.name for stock, strategy in self.strategies.items()}
        }
        
        logger.info(f"组合回测完成: 总收益率 = {total_return:.2f}%")
        return self.backtest_results
    
    def get_backtest_results(self) -> Dict[str, Any]:
        """
        获取回测结果
        
        Returns:
            Dict[str, Any]: 回测结果
        """
        return self.backtest_results
    
    def visualize_backtest(self) -> Any:
        """
        可视化回测结果
        
        Returns:
            Any: 可视化结果
        """
        if not self.backtest_results:
            logger.error("尚未运行回测")
            return None
        
        return self.visualizer.visualize_backtest(self.backtest_results)
