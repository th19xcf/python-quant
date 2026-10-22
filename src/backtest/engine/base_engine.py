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

    @staticmethod
    def _is_tradable(index: int, direction: str, close_prices, high_prices, low_prices, volumes, limit_pct: float = 0.1) -> bool:
        """
        检查指定交易日是否可交易（A股交易制度校验）

        Args:
            index: 交易日索引
            direction: 交易方向，'buy'或'sell'
            close_prices: 收盘价序列
            high_prices: 最高价序列（可为None）
            low_prices: 最低价序列（可为None）
            volumes: 成交量序列（可为None）
            limit_pct: 涨跌停幅度阈值

        Returns:
            bool: 是否可交易。停牌（成交量为0）不可交易；
                  一字涨停（high==low且涨幅达限）不可买入；
                  一字跌停（high==low且跌幅达限）不可卖出
        """
        # 停牌日（成交量为0）不可交易
        if volumes is not None and volumes[index] is not None and volumes[index] <= 0:
            return False
        # 一字涨跌停板不可交易
        if high_prices is not None and low_prices is not None and index > 0:
            prev_close = close_prices[index - 1]
            if prev_close and prev_close > 0 and high_prices[index] == low_prices[index]:
                pct = close_prices[index] / prev_close - 1
                if direction == 'buy' and pct >= limit_pct - 0.005:
                    return False
                if direction == 'sell' and pct <= -(limit_pct - 0.005):
                    return False
        return True

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

    # ------------------------------------------------------------------
    # 指标预计算：根据策略参数一次性向量化算好，避免策略内 O(n²) 循环
    # ------------------------------------------------------------------

    def _precompute_indicators(self, df: pl.DataFrame, strategy: BaseStrategy) -> pl.DataFrame:
        """
        向量化预计算策略会用到的指标列，结果写入 DataFrame 供策略按列名读取

        约定策略直接读以下列（如存在）：
            ma_{short_window}, ma_{long_window}     趋势跟踪 / 多因子 MA
            macd, macd_signal, macd_hist            多因子 / MACD 策略
            rsi                                     均值回归 / 多因子
            rsi_{period}                            通用 RSI
        """
        close = pl.col('close')
        high = pl.col('high')
        low = pl.col('low')

        short_w = int(strategy.params.get('short_window', 10))
        long_w = int(strategy.params.get('long_window', 20))
        rsi_w = int(strategy.params.get('rsi_period', 14))

        exprs = []

        # 移动平均线（多策略共用）
        exprs.append(close.rolling_mean(window_size=short_w).alias(f'ma_{short_w}'))
        exprs.append(close.rolling_mean(window_size=long_w).alias(f'ma_{long_w}'))

        # EMA / MACD
        ema12 = close.ewm_mean(span=12)
        ema26 = close.ewm_mean(span=26)
        macd = ema12 - ema26
        signal = macd.ewm_mean(span=9)
        exprs.extend([
            macd.alias('macd'),
            signal.alias('macd_signal'),
            (macd - signal).alias('macd_hist'),
        ])

        # RSI（支持自定义 period）
        delta = close.diff()
        gain = pl.when(delta > 0).then(delta).otherwise(0.0)
        loss = pl.when(delta < 0).then(-delta).otherwise(0.0)
        avg_gain = gain.rolling_mean(window_size=rsi_w)
        avg_loss = loss.rolling_mean(window_size=rsi_w)
        rs = avg_gain / (avg_loss + 1e-10)
        rsi_expr = 100 - (100 / (1 + rs))
        exprs.extend([
            rsi_expr.alias('rsi'),
            rsi_expr.alias(f'rsi_{rsi_w}'),
        ])

        # ATR（多因子可能用到）
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        tr = pl.max_horizontal([tr1, tr2, tr3])
        exprs.append(tr.rolling_mean(window_size=14).alias('atr'))

        if not exprs:
            return df
        return df.with_columns(exprs)

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
