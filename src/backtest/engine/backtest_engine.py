#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
回测引擎，用于执行策略回测
"""

from typing import Dict, Any, List
import polars as pl
from loguru import logger

from src.backtest.engine.base_engine import BaseBacktestEngine


class BacktestEngine(BaseBacktestEngine):
    """
    回测引擎类，提供策略回测功能
    """

    def __init__(self, data):
        """
        初始化回测引擎

        Args:
            data: 股票数据，可以是Polars DataFrame、LazyFrame或Pandas DataFrame
        """
        super().__init__()
        self.pl_df = self.convert_to_polars(data)

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
        
        for i in range(len(dates)):
            current_data = {}
            for col in data:
                current_data[col] = data[col][i]
            
            signal = self.strategy.generate_signal(current_data, i)
            
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
        results = self._run_parallel_backtests(param_combinations, initial_capital, max_workers)
        best_params, best_value = self._select_best_params(results, objective)
        
        logger.info(f"参数优化完成: 最佳参数 = {best_params}, 最佳{objective} = {best_value:.4f}")
        
        self.strategy.set_params(best_params)
        
        return {
            'best_params': best_params,
            'best_value': best_value,
            'optimization_results': results,
            'objective': objective
        }
    
    def _run_parallel_backtests(self, param_combinations: List[Dict[str, float]], initial_capital: float, max_workers: int) -> List[Dict[str, Any]]:
        """
        并行运行回测
        
        Args:
            param_combinations: 参数组合列表
            initial_capital: 初始资金
            max_workers: 最大工作线程数
            
        Returns:
            List[Dict[str, Any]]: 回测结果列表
        """
        import concurrent.futures
        
        results = []
        
        def run_single_backtest(params):
            strategy_copy = self.strategy.__class__()
            strategy_copy.set_params(params)
            
            engine_copy = BacktestEngine(self.pl_df)
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
    
    def analyze_sensitivity(self, param_ranges: Dict[str, List[float]], initial_capital: float = 1000000.0, max_workers: int = 4) -> Dict[str, Any]:
        """
        分析参数敏感性
        
        Args:
            param_ranges: 参数范围
            initial_capital: 初始资金
            max_workers: 并行计算的最大工作线程数
            
        Returns:
            Dict[str, Any]: 敏感性分析结果
        """
        if not self.strategy:
            logger.error("未设置策略")
            return {}
        
        param_combinations = self._generate_param_combinations(param_ranges)
        results = self._run_parallel_backtests(param_combinations, initial_capital, max_workers)
        sensitivity_analysis = self._calculate_sensitivity(results, list(param_ranges.keys()))
        
        return {
            'sensitivity_analysis': sensitivity_analysis,
            'param_ranges': param_ranges,
            'results': results
        }
    
    def _calculate_sensitivity(self, results: List[Dict[str, Any]], param_names: List[str]) -> Dict[str, Any]:
        """
        计算参数敏感性
        
        Args:
            results: 回测结果列表
            param_names: 参数名称列表
            
        Returns:
            Dict[str, Any]: 敏感性分析结果
        """
        import numpy as np
        
        sensitivity = {}
        
        for param_name in param_names:
            param_values = []
            total_returns = []
            sharpe_ratios = []
            max_drawdowns = []
            
            for result in results:
                params = result.get('params', {})
                if param_name in params:
                    param_values.append(params[param_name])
                    total_returns.append(result.get('total_return', 0))
                    sharpe_ratios.append(result.get('performance', {}).get('sharpe_ratio', 0))
                    max_drawdowns.append(result.get('performance', {}).get('max_drawdown', 0))
            
            if param_values and len(param_values) > 1:
                sensitivity[param_name] = {
                    'values': param_values,
                    'total_return_correlation': np.corrcoef(param_values, total_returns)[0, 1] if len(param_values) > 1 else 0,
                    'sharpe_ratio_correlation': np.corrcoef(param_values, sharpe_ratios)[0, 1] if len(param_values) > 1 else 0,
                    'max_drawdown_correlation': np.corrcoef(param_values, max_drawdowns)[0, 1] if len(param_values) > 1 else 0,
                    'total_return_std': np.std(total_returns) if total_returns else 0,
                    'sharpe_ratio_std': np.std(sharpe_ratios) if sharpe_ratios else 0,
                    'max_drawdown_std': np.std(max_drawdowns) if max_drawdowns else 0
                }
        
        return sensitivity
    
    def visualize_parameter_impact(self, optimization_results: Dict[str, Any]) -> Any:
        """
        可视化参数对策略性能的影响
        
        Args:
            optimization_results: 优化结果
            
        Returns:
            Any: 可视化结果
        """
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
        
        results = optimization_results.get('optimization_results', [])
        if not results:
            logger.error("没有优化结果可可视化")
            return None
        
        param_names = list(results[0].get('params', {}).keys()) if results else []
        
        if param_names:
            fig = make_subplots(rows=len(param_names), cols=3, 
                              subplot_titles=[f'{param} vs 总收益率' for param in param_names] + 
                                            [f'{param} vs 夏普比率' for param in param_names] + 
                                            [f'{param} vs 最大回撤' for param in param_names])
            
            for i, param_name in enumerate(param_names):
                x_values = []
                y_return = []
                y_sharpe = []
                y_drawdown = []
                
                for result in results:
                    params = result.get('params', {})
                    if param_name in params:
                        x_values.append(params[param_name])
                        y_return.append(result.get('total_return', 0))
                        y_sharpe.append(result.get('performance', {}).get('sharpe_ratio', 0))
                        y_drawdown.append(result.get('performance', {}).get('max_drawdown', 0))
                
                fig.add_trace(go.Scatter(x=x_values, y=y_return, mode='markers', name=param_name), 
                             row=i+1, col=1)
                
                fig.add_trace(go.Scatter(x=x_values, y=y_sharpe, mode='markers', name=param_name), 
                             row=i+1, col=2)
                
                fig.add_trace(go.Scatter(x=x_values, y=y_drawdown, mode='markers', name=param_name), 
                             row=i+1, col=3)
            
            fig.update_layout(height=400*len(param_names), width=1200, title_text="参数对策略性能的影响")
            return fig
        
        return None