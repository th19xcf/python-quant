#!/usr/bin/env python3

"""
回测引擎（单票），撮合逻辑全部委托给 execution_core 统一内核
"""

from typing import Any, Dict, List

from loguru import logger

from src.backtest.engine.base_engine import BaseBacktestEngine
from src.backtest.engine.execution_core import EventBacktestRunner


class BacktestEngine(BaseBacktestEngine):
    """
    回测引擎类，提供单只股票的策略回测功能

    交易规则（遵循A股制度，实现见 execution_core）：
    1. t日信号延迟到t+1日以开盘价成交（避免收盘价未来函数）
    2. 买入股数按lot_size（默认100股）整数倍取整，买入时即扣除买方成本
    3. 开盘触及涨停不可买入、触及跌停不可卖出、停牌日（成交量为0）不可交易
    4. 涨跌停幅度按 ts_code 自动推导（创业板/科创板 20%、ST 5%、北交所 30%）

    风控层（可选）：
    - 设置 self.risk_engine 后，buy/sell 前会先过 gate()
    - 被拒的单子记进 self.rejected_orders，包含规则名和原因
    """

    def __init__(self, data):
        """
        初始化回测引擎

        Args:
            data: 股票数据，可以是Polars DataFrame、LazyFrame或Pandas DataFrame
        """
        super().__init__()
        self.pl_df = self.convert_to_polars(data)

    def run_backtest(self, initial_capital: float = 1000000.0, lot_size: int = 100,
                     ts_code: str = '', industry: str = '', **params) -> Dict[str, Any]:
        """
        运行回测

        Args:
            initial_capital: 初始资金
            lot_size: 最小交易单位（股），A股买入为100股整数倍
            ts_code: 股票代码，涨跌停幅度据此自适应推导
            industry: 行业标签（风控 MaxIndustryWeight 用）
            **params: 其他回测参数

        Returns:
            Dict[str, Any]: 回测结果
        """
        if not self.strategy:
            logger.error("未设置策略")
            return {}

        self.ts_code = ts_code or self.ts_code
        self.rejected_orders = []

        # 指标预计算：根据策略参数一次性向量化算好，避免每根K线在策略里 O(n²) 循环
        self.pl_df = self._precompute_indicators(self.pl_df, self.strategy)

        # 单票 = 单标的账户，初始资金全部归属该标的
        code = self.ts_code or '__default__'

        runner = EventBacktestRunner(
            cost_model=self.cost_model,
            performance_analyzer=self.performance_analyzer,
            risk_engine=self.risk_engine,
        )
        result = runner.run(
            universe={code: self.pl_df},
            strategies={code: self.strategy},
            initial_capital=initial_capital,
            lot_size=lot_size,
            industry_map={code: industry} if industry else {},
            result_meta={'strategy_name': self.strategy.name},
        )

        self.trades = result.get('trades', [])
        self.equity_curve = result.get('equity_curve', [])
        self.rejected_orders = result.get('rejected_orders', [])
        self.backtest_results = result
        return result

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

        # 检查策略、数据、成本模型、风控引擎是否可 pickle，决定能否使用多进程
        try:
            pickle.dumps((self.strategy, self.pl_df, self.cost_model, self.risk_engine))
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
                                    self.pl_df, self.cost_model, params, initial_capital,
                                    self.risk_engine): params
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
            engine_copy.risk_engine = self.risk_engine

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
