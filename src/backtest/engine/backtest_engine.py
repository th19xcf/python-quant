#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
回测引擎，用于执行策略回测
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


class BacktestEngine:
    """
    回测引擎类，提供策略回测功能
    """
    
    def __init__(self, data):
        """
        初始化回测引擎
        
        Args:
            data: 股票数据，可以是Polars DataFrame、LazyFrame或Pandas DataFrame
        """
        # 转换为Polars DataFrame
        if isinstance(data, pd.DataFrame):
            self.pl_df = pl.from_pandas(data)
        elif hasattr(data, 'to_pandas'):
            self.pl_df = data
        else:
            self.pl_df = pl.DataFrame(data)
        
        # 初始化策略
        self.strategy = None
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
    
    def set_strategy(self, strategy: BaseStrategy):
        """
        设置策略
        
        Args:
            strategy: 策略对象
        """
        self.strategy = strategy
    
    def set_cost_model(self, cost_model: CostModel):
        """
        设置成本模型
        
        Args:
            cost_model: 成本模型对象
        """
        self.cost_model = cost_model
    
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
        
        # 重置回测结果
        self.trades = []
        self.equity_curve = []
        
        # 初始化回测参数
        current_capital = initial_capital
        position = 0
        current_price = 0
        
        # 执行回测
        data = self.pl_df.to_dict(as_series=False)
        dates = data['date']
        close_prices = data['close']
        
        for i in range(len(dates)):
            # 获取当前数据
            current_data = {}
            for col in data:
                current_data[col] = data[col][i]
            
            # 生成信号
            signal = self.strategy.generate_signal(current_data, i)
            
            # 执行交易
            if signal == 'buy' and position == 0:
                # 买入
                current_price = close_prices[i]
                shares = current_capital / current_price
                position = shares
                current_capital = 0
                
                # 记录交易
                self.trades.append({
                    'date': dates[i],
                    'signal': 'buy',
                    'price': current_price,
                    'shares': shares,
                    'capital': current_capital,
                    'position': position
                })
            elif signal == 'sell' and position > 0:
                # 卖出
                sell_price = close_prices[i]
                current_capital = position * sell_price
                # 扣除交易成本
                cost = self.cost_model.calculate_cost(position, current_price, sell_price)
                current_capital -= cost
                
                # 记录交易
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
            
            # 计算当前权益
            if position > 0:
                current_equity = position * close_prices[i]
            else:
                current_equity = current_capital
            
            # 记录权益曲线
            self.equity_curve.append({
                'date': dates[i],
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
            'strategy_name': self.strategy.name
        }
        
        logger.info(f"回测完成: 总收益率 = {total_return:.2f}%")
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
    
    def optimize_strategy(self, param_ranges: Dict[str, List[float]], initial_capital: float = 1000000.0, objective: str = 'total_return', max_workers: int = 4) -> Dict[str, Any]:
        """
        优化策略参数
        
        Args:
            param_ranges: 参数范围
            initial_capital: 初始资金
            objective: 优化目标，可选值：'total_return'（总收益率）、'sharpe_ratio'（夏普比率）、'max_drawdown'（最大回撤）
            max_workers: 并行计算的最大工作线程数
            
        Returns:
            Dict[str, Any]: 优化结果
        """
        if not self.strategy:
            logger.error("未设置策略")
            return {}
        
        # 生成参数组合
        param_combinations = self._generate_param_combinations(param_ranges)
        
        # 并行运行回测
        results = self._run_parallel_backtests(param_combinations, initial_capital, max_workers)
        
        # 选择最佳参数
        best_params, best_value = self._select_best_params(results, objective)
        
        logger.info(f"参数优化完成: 最佳参数 = {best_params}, 最佳{objective} = {best_value:.4f}")
        
        # 设置最佳参数
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
            # 创建策略副本
            strategy_copy = self.strategy.__class__()
            strategy_copy.set_params(params)
            
            # 创建回测引擎副本
            engine_copy = BacktestEngine(self.pl_df)
            engine_copy.set_strategy(strategy_copy)
            engine_copy.set_cost_model(self.cost_model)
            
            # 运行回测
            result = engine_copy.run_backtest(initial_capital)
            result['params'] = params
            return result
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_param = {executor.submit(run_single_backtest, params): params for params in param_combinations}
            
            for future in concurrent.futures.as_completed(future_to_param):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as exc:
                    logger.error(f"参数组合 {future_to_param[future]} 回测失败: {exc}")
        
        return results
    
    def _select_best_params(self, results: List[Dict[str, Any]], objective: str) -> tuple:
        """
        选择最佳参数
        
        Args:
            results: 回测结果列表
            objective: 优化目标
            
        Returns:
            tuple: (最佳参数, 最佳值)
        """
        if not results:
            return {}, 0
        
        best_params = {}
        if objective == 'total_return':
            best_value = -float('inf')
            for result in results:
                value = result.get('total_return', -float('inf'))
                if value > best_value:
                    best_value = value
                    best_params = result.get('params', {})
        elif objective == 'sharpe_ratio':
            best_value = -float('inf')
            for result in results:
                sharpe = result.get('performance', {}).get('sharpe_ratio', -float('inf'))
                if sharpe > best_value:
                    best_value = sharpe
                    best_params = result.get('params', {})
        elif objective == 'max_drawdown':
            best_value = float('inf')
            for result in results:
                drawdown = result.get('performance', {}).get('max_drawdown', float('inf'))
                if drawdown < best_value:
                    best_value = drawdown
                    best_params = result.get('params', {})
        else:
            best_value = -float('inf')
            for result in results:
                value = result.get('total_return', -float('inf'))
                if value > best_value:
                    best_value = value
                    best_params = result.get('params', {})
        
        return best_params, best_value
    
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
        
        # 生成参数组合
        param_combinations = self._generate_param_combinations(param_ranges)
        
        # 并行运行回测
        results = self._run_parallel_backtests(param_combinations, initial_capital, max_workers)
        
        # 分析敏感性
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
            
            # 计算相关性
            if param_values and len(param_values) > 1:
                import numpy as np
                
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
        
        # 提取参数名
        if results:
            param_names = list(results[0].get('params', {}).keys())
        else:
            param_names = []
        
        # 创建子图
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
                
                # 总收益率图
                fig.add_trace(go.Scatter(x=x_values, y=y_return, mode='markers', name=param_name), 
                             row=i+1, col=1)
                
                # 夏普比率图
                fig.add_trace(go.Scatter(x=x_values, y=y_sharpe, mode='markers', name=param_name), 
                             row=i+1, col=2)
                
                # 最大回撤图
                fig.add_trace(go.Scatter(x=x_values, y=y_drawdown, mode='markers', name=param_name), 
                             row=i+1, col=3)
            
            fig.update_layout(height=400*len(param_names), width=1200, title_text="参数对策略性能的影响")
            return fig
        
        return None
    
    def _generate_param_combinations(self, param_ranges: Dict[str, List[float]]) -> List[Dict[str, float]]:
        """
        生成参数组合
        
        Args:
            param_ranges: 参数范围
            
        Returns:
            List[Dict[str, float]]: 参数组合列表
        """
        import itertools
        
        # 提取参数名和值列表
        param_names = list(param_ranges.keys())
        param_values = [param_ranges[name] for name in param_names]
        
        # 生成所有组合
        combinations = list(itertools.product(*param_values))
        
        # 转换为字典列表
        param_combinations = []
        for combo in combinations:
            param_dict = {}
            for i, name in enumerate(param_names):
                param_dict[name] = combo[i]
            param_combinations.append(param_dict)
        
        return param_combinations
