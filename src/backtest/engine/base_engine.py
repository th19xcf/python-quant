#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
回测引擎基类，提供数据转换和通用组件初始化的统一逻辑
"""

from typing import Any, Union, Dict, List
import polars as pl
import pandas as pd
import itertools
from loguru import logger

from src.backtest.costs.cost_model import CostModel
from src.backtest.performance.performance_analyzer import PerformanceAnalyzer
from src.backtest.visualization.backtest_visualizer import BacktestVisualizer
from src.backtest.strategies.base_strategy import BaseStrategy


DataFrameType = Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame, Any]


class BaseBacktestEngine:
    """
    回测引擎基类，封装数据转换、组件初始化等通用逻辑。
    子类仅需实现各自的回测主体逻辑。
    """

    def __init__(self):
        """初始化通用组件（不含数据）。子类应先调用此方法，再处理数据。"""
        self.strategy = None
        self.cost_model = CostModel()
        self.performance_analyzer = PerformanceAnalyzer()
        self.visualizer = BacktestVisualizer()
        self.backtest_results = {}
        self.trades = []
        self.equity_curve = []

    @staticmethod
    def convert_to_polars(data: DataFrameType) -> pl.DataFrame:
        """
        统一的数据转换方法：接受多种类型并返回 Polars DataFrame。

        Args:
            data: Polars DataFrame / LazyFrame / Pandas DataFrame / 其他可转换对象

        Returns:
            pl.DataFrame: 转换后的 Polars DataFrame
        """
        if isinstance(data, pd.DataFrame):
            return pl.from_pandas(data)
        if isinstance(data, pl.LazyFrame):
            return data.collect()
        if isinstance(data, pl.DataFrame):
            return data
        return pl.DataFrame(data)

    def init_common_components(self) -> None:
        """初始化通用组件（供子类在 __init__ 末尾调用）。"""
        self.cost_model = CostModel()
        self.performance_analyzer = PerformanceAnalyzer()
        self.visualizer = BacktestVisualizer()
        self.backtest_results = {}
        self.trades = []
        self.equity_curve = []

    def set_strategy(self, strategy: BaseStrategy) -> None:
        """
        设置策略

        Args:
            strategy: 策略对象
        """
        self.strategy = strategy

    def set_cost_model(self, cost_model: CostModel) -> None:
        """
        设置成本模型

        Args:
            cost_model: 成本模型对象
        """
        self.cost_model = cost_model

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

    def _generate_param_combinations(self, param_ranges: Dict[str, List[float]]) -> List[Dict[str, float]]:
        """
        生成参数组合

        Args:
            param_ranges: 参数范围

        Returns:
            List[Dict[str, float]]: 参数组合列表
        """
        param_names = list(param_ranges.keys())
        param_values = [param_ranges[name] for name in param_names]

        combinations = list(itertools.product(*param_values))

        param_combinations = []
        for combo in combinations:
            param_dict = {}
            for i, name in enumerate(param_names):
                param_dict[name] = combo[i]
            param_combinations.append(param_dict)

        return param_combinations

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
