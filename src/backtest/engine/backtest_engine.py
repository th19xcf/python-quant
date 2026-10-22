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

    def run_backtest(self, initial_capital: float = 1000000.0, lot_size: int = 100, limit_pct: float = 0.1,
                     ts_code: str = '', **params) -> Dict[str, Any]:
        """
        运行回测

        交易规则（遵循A股制度）：
        1. t日信号延迟到t+1日以开盘价成交（避免收盘价未来函数）
        2. 买入股数按lot_size（默认100股）整数倍取整，买入时即扣除买方成本
        3. 一字涨停日不可买入、一字跌停日不可卖出、停牌日（成交量为0）不可交易
        4. 涨跌停幅度按 ts_code 自动推导（创业板/科创板 20%、ST 5%、北交所 30%）
        """
        if not self.strategy:
            logger.error("未设置策略")
            return {}

        self.ts_code = ts_code or self.ts_code
        self.trades = []
        self.equity_curve = []
        
        # 指标预计算：根据策略参数一次性向量化算好，避免每根K线在策略里 O(n²) 循环
        self.pl_df = self._precompute_indicators(self.pl_df, self.strategy)
        
        current_capital = initial_capital
        position = 0
        pending_signal = None
        
        data = self.pl_df.to_dict(as_series=False)
        dates = data['date']
        close_prices = data['close']
        open_prices = data.get('open', close_prices)
        high_prices = data.get('high')
        low_prices = data.get('low')
        volumes = data.get('volume')
        
        for i in range(len(dates)):
            # 1. 执行前一交易日生成的信号（以今日开盘价成交，订单当日有效）
            if pending_signal == 'buy' and position == 0:
                if self._is_tradable(i, 'buy', close_prices, high_prices, low_prices, volumes,
                                     limit_pct, ts_code=self.ts_code):
                    exec_price = open_prices[i]
                    # 预留买入成本后按整手取整
                    cost_rate = self.cost_model.get_buy_cost_rate()
                    affordable = current_capital / (exec_price * (1 + cost_rate))
                    shares = int(affordable // lot_size) * lot_size
                    if shares > 0:
                        buy_cost = self.cost_model.calculate_buy_cost(shares, exec_price)
                        total_pay = shares * exec_price + buy_cost
                        if total_pay <= current_capital:
                            position = shares
                            current_capital -= total_pay
                            self.trades.append({
                                'date': dates[i],
                                'signal': 'buy',
                                'price': exec_price,
                                'shares': shares,
                                'capital': current_capital,
                                'position': position,
                                'cost': buy_cost
                            })
                pending_signal = None
            elif pending_signal == 'sell' and position > 0:
                if self._is_tradable(i, 'sell', close_prices, high_prices, low_prices, volumes,
                                     limit_pct, ts_code=self.ts_code):
                    sell_price = open_prices[i]
                    sell_cost = self.cost_model.calculate_sell_cost(position, sell_price)
                    # 卖出所得应叠加到剩余现金，不能覆盖买入后剩余的现金
                    current_capital += position * sell_price - sell_cost
                    
                    self.trades.append({
                        'date': dates[i],
                        'signal': 'sell',
                        'price': sell_price,
                        'shares': position,
                        'capital': current_capital,
                        'position': 0,
                        'cost': sell_cost
                    })
                    position = 0
                pending_signal = None
            
            # 2. 基于当日数据生成信号，延迟到次日执行
            current_data = {}
            for col in data:
                current_data[col] = data[col][i]
            
            signal = self.strategy.generate_signal(current_data, i)
            if signal in ('buy', 'sell'):
                pending_signal = signal
            
            # 3. 按收盘价对持仓盯市
            if position > 0:
                current_equity = current_capital + position * close_prices[i]
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
        import os
        import pickle

        from src.backtest.engine._parallel_worker import run_backtest_worker

        results = []

        # 检查策略、数据、成本模型是否可 pickle，决定能否使用多进程
        try:
            pickle.dumps((self.strategy, self.pl_df, self.cost_model))
            use_process_pool = True
        except (pickle.PicklingError, TypeError, AttributeError) as exc:
            logger.warning(f"策略/数据/成本模型不可 pickle（{exc}），回退到 ThreadPoolExecutor 并行回测")
            use_process_pool = False

        if use_process_pool:
            # 纯 Python 回测循环受 GIL 限制，线程无法并行 CPU 计算，改用进程池利用多核
            workers = max(1, min(max_workers, len(param_combinations), os.cpu_count() or 1))
            with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
                future_to_param = {
                    executor.submit(run_backtest_worker, BacktestEngine, self.strategy,
                                    self.pl_df, self.cost_model, params, initial_capital): params
                    for params in param_combinations
                }

                for future in concurrent.futures.as_completed(future_to_param):
                    try:
                        result = future.result()
                        results.append(result)
                    except (OSError, RuntimeError, ValueError) as exc:
                        logger.error(f"参数组合 {future_to_param[future]} 回测失败: {exc}")

            return results

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