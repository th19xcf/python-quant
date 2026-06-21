#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
组合回测引擎，用于执行多股票组合策略回测
"""

from typing import Dict, Any
import polars as pl
from loguru import logger

from src.backtest.engine.base_engine import BaseBacktestEngine
from src.backtest.strategies.base_strategy import BaseStrategy


class PortfolioBacktestEngine(BaseBacktestEngine):
    """
    组合回测引擎类，提供多股票组合策略回测功能
    """

    def __init__(self, data_dict: Dict[str, Any]):
        """
        初始化组合回测引擎

        Args:
            data_dict: 股票数据字典，键为股票代码，值为Polars DataFrame、LazyFrame或Pandas DataFrame
        """
        super().__init__()
        self.data_dict = {
            stock_code: self.convert_to_polars(data)
            for stock_code, data in data_dict.items()
        }
        self.strategy_dict = {}
        self.positions = {}
        self.strategies = {}

    def set_strategy(self, stock_code: str, strategy: BaseStrategy):
        """
        为特定股票设置策略

        Args:
            stock_code: 股票代码
            strategy: 策略对象
        """
        self.strategies[stock_code] = strategy
    
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
        
        self.trades = []
        self.equity_curve = []
        self.positions = {stock: 0 for stock in self.data_dict.keys()}
        
        current_capital = initial_capital
        stock_capitals = {}
        
        stock_count = len(self.data_dict)
        for stock in self.data_dict.keys():
            stock_capitals[stock] = initial_capital / stock_count
        
        all_dates = set()
        for stock, data in self.data_dict.items():
            dates = data['date'].to_list()
            all_dates.update(dates)
        
        sorted_dates = sorted(all_dates)
        
        for date in sorted_dates:
            daily_trades = []
            
            for stock_code, data in self.data_dict.items():
                stock_data = data.filter(pl.col('date') == date)
                if stock_data.is_empty():
                    continue
                
                current_data = stock_data.to_dict(as_series=False)
                current_data = {k: v[0] for k, v in current_data.items()}
                
                strategy = self.strategies.get(stock_code)
                if strategy:
                    signal = strategy.generate_signal(current_data, 0)
                    
                    if signal == 'buy' and self.positions[stock_code] == 0:
                        current_price = current_data['close']
                        shares = stock_capitals[stock_code] / current_price
                        self.positions[stock_code] = shares
                        stock_capitals[stock_code] = 0
                        
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
                        sell_price = current_data['close']
                        stock_capitals[stock_code] = self.positions[stock_code] * sell_price
                        cost = self.cost_model.calculate_cost(self.positions[stock_code], current_data['close'], sell_price)
                        stock_capitals[stock_code] -= cost
                        
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
            
            self.trades.extend(daily_trades)
            
            current_equity = current_capital
            for stock_code, position in self.positions.items():
                if position > 0:
                    stock_data = self.data_dict[stock_code].filter(pl.col('date') == date)
                    if not stock_data.is_empty():
                        close_price = stock_data['close'][0]
                        current_equity += position * close_price
            
            current_equity += sum(stock_capitals.values())
            
            self.equity_curve.append({
                'date': date,
                'equity': current_equity
            })
        
        final_equity = self.equity_curve[-1]['equity'] if self.equity_curve else initial_capital
        total_return = (final_equity - initial_capital) / initial_capital * 100
        
        performance = self.performance_analyzer.analyze(self.equity_curve, self.trades)
        
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