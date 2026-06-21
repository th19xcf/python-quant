#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
优化的回测引擎，用于执行策略回测，提高执行效率
"""

from typing import Dict, Any, List
import polars as pl
from loguru import logger
import concurrent.futures
from functools import lru_cache

from src.backtest.engine.base_engine import BaseBacktestEngine


class OptimizedBacktestEngine(BaseBacktestEngine):
    """
    优化的回测引擎类，提供策略回测功能，提高执行效率
    """

    def __init__(self, data):
        """
        初始化优化的回测引擎

        Args:
            data: 股票数据，可以是Polars DataFrame、LazyFrame或Pandas DataFrame
        """
        super().__init__()
        self.pl_df = self.convert_to_polars(data)

        if 'date' in self.pl_df.columns:
            self.pl_df = self.pl_df.sort('date')

    @lru_cache(maxsize=1000)
    def _get_signal(self, data_tuple, index):
        """
        缓存策略信号生成结果
        
        Args:
            data_tuple: 数据元组，用于缓存键
            index: 数据索引
            
        Returns:
            str: 交易信号
        """
        data = {}
        columns = ['open', 'high', 'low', 'close', 'volume']
        for i, value in enumerate(data_tuple):
            data[columns[i]] = value
        
        return self.strategy.generate_signal(data, index)
    
    def run_backtest(self, initial_capital: float = 1000000.0, **params) -> Dict[str, Any]:
        """
        运行回测
        
        Args:
            initial_capital: 初始资金
            **params: 回测参数
            
        Returns:
            Dict[str, Any]: 回测结果
        """
        if not self.strategy:
            logger.error("未设置策略")
            return {}
        
        self.trades = []
        self.equity_curve = []
        
        current_capital = initial_capital
        position = 0
        current_price = 0
        
        data = self.pl_df.to_dict(as_series=False)
        dates = data['date']
        close_prices = data['close']
        
        data_tuples = []
        columns = ['open', 'high', 'low', 'close', 'volume']
        for i in range(len(dates)):
            data_tuple = tuple(data[col][i] for col in columns if col in data)
            data_tuples.append(data_tuple)
        
        for i in range(len(dates)):
            signal = self._get_signal(data_tuples[i], i)
            
            if signal == 'buy' and position == 0:
                current_price = close_prices[i]
                shares = current_capital / current_price
                position = shares
                current_capital = 0
                
                self.trades.append({
                    'date': dates[i],
                    'signal': 'buy',
                    'price': current_price,
                    'shares': shares,
                    'capital': current_capital,
                    'position': position
                })
            elif signal == 'sell' and position > 0:
                sell_price = close_prices[i]
                current_capital = position * sell_price
                cost = self.cost_model.calculate_cost(position, current_price, sell_price)
                current_capital -= cost
                
                self.trades.append({
                    'date': dates[i],
                    'signal': 'sell',
                    'price': sell_price,
                    'shares': position,
                    'capital': current_capital,
                    'position': 0,
                    'cost': cost
                })
                
                position = 0
            
            if position > 0:
                current_equity = position * close_prices[i]
            else:
                current_equity = current_capital
            
            self.equity_curve.append({
                'date': dates[i],
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
            'strategy_name': self.strategy.name
        }
        
        logger.info(f"回测完成: 总收益率 = {total_return:.2f}%")
        return self.backtest_results
    
    def run_parallel_backtest(self, param_combinations: List[Dict[str, Any]], initial_capital: float = 1000000.0, max_workers: int = 4) -> List[Dict[str, Any]]:
        """
        并行运行回测
        
        Args:
            param_combinations: 参数组合列表
            initial_capital: 初始资金
            max_workers: 最大工作线程数
            
        Returns:
            List[Dict[str, Any]]: 回测结果列表
        """
        results = []
        
        def run_single_backtest(params):
            strategy_copy = self.strategy.__class__()
            strategy_copy.set_params(params)
            
            engine_copy = OptimizedBacktestEngine(self.pl_df)
            engine_copy.set_strategy(strategy_copy)
            engine_copy.set_cost_model(self.cost_model)
            
            result = engine_copy.run_backtest(initial_capital)
            result['params'] = params
            return result
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_param = {executor.submit(run_single_backtest, params): params for params in param_combinations}
            
            for future in concurrent.futures.as_completed(future_to_param):
                try:
                    result = future.result()
                    results.append(result)
                except (OSError, RuntimeError, ValueError) as exc:
                    logger.error(f"参数组合 {future_to_param[future]} 回测失败: {exc}")
        
        return results
    
    def optimize_strategy(self, param_ranges: Dict[str, List[float]], initial_capital: float = 1000000.0, objective: str = 'total_return', max_workers: int = 4) -> Dict[str, Any]:
        """
        优化策略参数
        
        Args:
            param_ranges: 参数范围
            initial_capital: 初始资金
            objective: 优化目标
            max_workers: 并行计算的最大工作线程数
            
        Returns:
            Dict[str, Any]: 优化结果
        """
        if not self.strategy:
            logger.error("未设置策略")
            return {}
        
        param_combinations = self._generate_param_combinations(param_ranges)
        results = self.run_parallel_backtest(param_combinations, initial_capital, max_workers)
        best_params, best_value = self._select_best_params(results, objective)
        
        logger.info(f"参数优化完成: 最佳参数 = {best_params}, 最佳{objective} = {best_value:.4f}")
        
        self.strategy.set_params(best_params)
        
        return {
            'best_params': best_params,
            'best_value': best_value,
            'optimization_results': results,
            'objective': objective
        }