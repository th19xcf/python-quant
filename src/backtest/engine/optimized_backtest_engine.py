#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
优化的回测引擎，用于执行策略回测，提高执行效率
"""

from typing import Dict, Any, List
import polars as pl
from loguru import logger
import concurrent.futures

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

    def run_backtest(self, initial_capital: float = 1000000.0, lot_size: int = 100, limit_pct: float = 0.1,
                     ts_code: str = '', **params) -> Dict[str, Any]:
        """
        运行回测

        交易规则（遵循A股制度）：
        1. t日信号延迟到t+1日以开盘价成交（避免收盘价未来函数）
        2. 买入股数按lot_size（默认100股）整数倍取整，买入时即扣除买方成本
        3. 一字涨停日不可买入、一字跌停日不可卖出、停牌日（成交量为0）不可交易
        4. 涨跌停幅度按 ts_code 自动推导（创业板/科创板 20%、ST 5%、北交所 30%）

        Args:
            initial_capital: 初始资金
            lot_size: 最小交易单位（股），A股买入为100股整数倍
            limit_pct: 涨跌停幅度阈值，用于识别一字板
            ts_code: 股票代码，用于自适应涨跌停幅度
            **params: 回测参数

        Returns:
            Dict[str, Any]: 回测结果
        """
        if not self.strategy:
            logger.error("未设置策略")
            return {}

        self.ts_code = ts_code or self.ts_code
        self.trades = []
        self.equity_curve = []

        # 指标预计算：向量化算好策略依赖的列，避免每根 K 线重复计算
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
                                     open_prices=open_prices, limit_pct=limit_pct, ts_code=self.ts_code):
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
                                     open_prices=open_prices, limit_pct=limit_pct, ts_code=self.ts_code):
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
                    executor.submit(run_backtest_worker, OptimizedBacktestEngine, self.strategy,
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