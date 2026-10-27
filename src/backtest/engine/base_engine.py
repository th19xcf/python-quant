#!/usr/bin/env python3

"""
回测引擎基类，提供数据转换和通用组件初始化的统一逻辑
"""

import itertools
from typing import Any, Dict, List, Union

import pandas as pd
import polars as pl
from loguru import logger

from src.backtest.costs.cost_model import CostModel
from src.backtest.performance.performance_analyzer import PerformanceAnalyzer
from src.backtest.strategies.base_strategy import BaseStrategy
from src.backtest.visualization.backtest_visualizer import BacktestVisualizer
from src.risk import RiskEngine
from src.tech_analysis.indicators.formulas import atr_expr, macd_exprs, rsi_expr

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
        self.ts_code = ''

        # 风控层：None 表示不启用（所有订单直接过）；设置后 gate() 拦截
        self.risk_engine: RiskEngine | None = None
        self.rejected_orders: List[Dict[str, Any]] = []   # 风控拒单日志

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

    def set_strategy(self, strategy: BaseStrategy, code: str = '') -> None:
        """
        设置策略（基类 1 参数契约：只传 strategy）。
        内部委托 set_strategies，便于子类统一主入口。
        """
        self.set_strategies({code: strategy} if code else {'__default__': strategy})

    def set_strategies(self, mapping: Dict[str, BaseStrategy]) -> None:
        """
        主入口：一次性设置多标的策略映射。
        单标的引擎取 mapping 中唯一或首个值存入 self.strategy；
        组合引擎按 code 存入 self.strategies。
        """
        if not mapping:
            self.strategy = None
            return
        # 单标的场景：只关心策略对象本身，code 在 run_backtest 里由 ts_code 补位
        self.strategy = next(iter(mapping.values()))

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

        short_w = int(strategy.params.get('short_window', 10))
        long_w = int(strategy.params.get('long_window', 20))
        rsi_w = int(strategy.params.get('rsi_period', 14))

        exprs = []

        # 移动平均线（多策略共用），min_samples=window 使窗口未满的前 window-1 根
        # 为 null，策略读到 None 即 hold，避免 ma_20 = close[0] 之类的暖机污染值产生虚假信号
        exprs.append(close.rolling_mean(window_size=short_w, min_samples=short_w).alias(f'ma_{short_w}'))
        exprs.append(close.rolling_mean(window_size=long_w, min_samples=long_w).alias(f'ma_{long_w}'))

        # EMA / MACD：统一走共享公式层（标准 EMA 口径）
        macd_dict = macd_exprs('close', 12, 26, 9)
        exprs.extend([
            macd_dict['macd'].alias('macd'),
            macd_dict['signal'].alias('macd_signal'),
            macd_dict['hist'].alias('macd_hist'),
        ])

        # RSI（支持自定义 period）：统一走共享公式层（Wilder 标准口径）
        rsi = rsi_expr('close', rsi_w)
        exprs.extend([
            rsi.alias('rsi'),
            rsi.alias(f'rsi_{rsi_w}'),
        ])

        # ATR（多因子可能用到）：统一走共享公式层
        exprs.append(atr_expr('high', 'low', 'close', 14).alias('atr'))

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
