#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
回测引擎基类，提供数据转换和通用组件初始化的统一逻辑
"""

from typing import Any, Union, Dict, List, Optional
import polars as pl
import pandas as pd
import itertools
from loguru import logger

from src.backtest.costs.cost_model import CostModel
from src.backtest.engine.execution_core import ExecutionRules
from src.backtest.performance.performance_analyzer import PerformanceAnalyzer
from src.backtest.visualization.backtest_visualizer import BacktestVisualizer
from src.backtest.strategies.base_strategy import BaseStrategy
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
        self.risk_engine: Optional[RiskEngine] = None
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

    def set_strategy(self, strategy: BaseStrategy) -> None:
        """
        设置策略

        Args:
            strategy: 策略对象
        """
        self.strategy = strategy

    @staticmethod
    def _get_limit_pct(ts_code: str = '', is_st: bool = False) -> float:
        """
        按证券代码与 ST 状态推导涨跌停幅度阈值。

        Args:
            ts_code: 股票代码，形如 '000001.SZ' 或 '300001.SZ'
            is_st: 是否 ST/*ST 股票（可由数据源或行情表 name 字段判定）

        Returns:
            float: 涨跌停幅度（如 0.10 表示 10%）

        规则（按上交所/深交所现行制度）：
            ST / *ST                 → 5%
            创业板 300xx / 301xx     → 20%
            科创板 68xxx / 688xxx    → 20%
            北交所 8/4/9 开头代码   → 30%
            主板默认                 → 10%
        """
        return ExecutionRules.resolve_limit_pct(ts_code, is_st, default=0.10)

    @staticmethod
    def _is_tradable(index: int, direction: str, close_prices, high_prices, low_prices, volumes,
                     open_prices=None, limit_pct: float = 0.1, ts_code: str = '', is_st: bool = False) -> bool:
        """
        检查指定交易日是否可交易（A股交易制度校验）

        Args:
            index: 交易日索引
            direction: 交易方向，'buy'或'sell'
            close_prices: 收盘价序列
            high_prices: 最高价序列（可为None）
            low_prices: 最低价序列（可为None）
            volumes: 成交量序列（可为None）
            open_prices: 开盘价序列（可为None；传入后可识别"开盘即涨跌停但盘中打开"的不可成交情形）
            limit_pct: 涨跌停幅度阈值（若传入 ts_code 则会被覆盖）
            ts_code: 股票代码，用于自适应涨跌停幅度
            is_st: 是否 ST/*ST 股票

        Returns:
            bool: 是否可交易。停牌（成交量为0）不可交易；
                  开盘价触及涨停（含一字涨停、开盘封板后打开）不可买入；
                  开盘价触及跌停（含一字跌停、开盘封板后打开）不可卖出
        """
        rules = ExecutionRules(limit_pct=limit_pct)
        prev_close = close_prices[index - 1] if index > 0 else None
        return rules.is_tradable(
            direction,
            prev_close=prev_close,
            open_price=open_prices[index] if open_prices is not None else None,
            high=high_prices[index] if high_prices is not None else None,
            low=low_prices[index] if low_prices is not None else None,
            close=close_prices[index],
            volume=volumes[index] if volumes is not None else None,
            ts_code=ts_code,
            is_st=is_st,
        )

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
