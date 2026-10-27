#!/usr/bin/env python3

"""
组合回测引擎，撮合逻辑全部委托给 execution_core 统一内核
"""

from typing import Any, Dict

from loguru import logger

from src.backtest.engine.base_engine import BaseBacktestEngine
from src.backtest.engine.execution_core import EventBacktestRunner
from src.backtest.strategies.base_strategy import BaseStrategy


class PortfolioBacktestEngine(BaseBacktestEngine):
    """
    组合回测引擎类，提供多股票组合策略回测功能

    交易规则（遵循A股制度，实现见 execution_core）：
    1. t日信号延迟到该股票下一交易日以开盘价成交（避免收盘价未来函数）
    2. 买入股数按lot_size（默认100股）整数倍取整，买入时即扣除买方成本
    3. 开盘触及涨停不可买入、触及跌停不可卖出、停牌（成交量为0）不可交易
    4. 初始资金按标的等权分配

    风控层（可选）：
    - 设置 self.risk_engine 后，每笔 buy/sell 先过 gate()
    - 被拒的单子记进 self.rejected_orders，包含规则名和原因
    """

    def __init__(self, data_dict: Dict[str, Any]):
        """
        初始化组合回测引擎

        Args:
            data_dict: 股票数据字典，键为股票代码，值为Polars DataFrame、LazyFrame或Pandas DataFrame
        """
        super().__init__()
        self.data_dict = {
            stock_code: self.convert_to_polars(data)
            for stock_code, data in data_dict.items()
        }
        self.positions: Dict[str, int] = {}
        self.strategies: Dict[str, BaseStrategy] = {}

    def set_strategy(self, stock_code: str = '', strategy: BaseStrategy | None = None, *args) -> None:
        """
        为特定股票设置策略。

        同时支持两种调用风格（消除 LSP 违反）：
            旧位置参数：engine.set_strategy('600000.SH', MAStrategy(...))
            基类单参数：engine.set_strategy(MAStrategy(...))    # stock_code 自动补位

        stock_code 留空时：
          - 多标的 Portfolio 需靠自动检测补位；
          - 单标的 Portfolio 自动取唯一标的。
          多标的且 stock_code 为空时，自动推断取 args[0]（若 args 第一个元素是 BaseStrategy），
          否则抛 ValueError 提示歧义。
        """
        # 动态识别调用风格：
        #   set_strategy(MAStrategy(...))                  → stock_code 是 BaseStrategy（单参数基类风格）
        #   set_strategy('600000.SH', MAStrategy(...))     → 正常两参数位置风格
        #   set_strategy(strategy=MAStrategy(...))         → keyword 风格
        if isinstance(stock_code, BaseStrategy) and strategy is None:
            # 基类 1 参数契约：把 strategy 从 stock_code 位置取出
            strategy = stock_code
            stock_code = ''
        elif args and strategy is None and stock_code:
            # 旧两参数位置风格：set_strategy(code, strat)
            strategy = args[0]

        if strategy is None:
            raise TypeError("set_strategy 必须传入策略对象")

        if not stock_code:
            codes = list(self.data_dict.keys())
            if len(codes) != 1:
                raise ValueError(
                    f"PortfolioBacktestEngine.set_strategy 未传 stock_code，"
                    f"但当前有 {len(codes)} 个标的 {codes}，歧义"
                )
            stock_code = codes[0]
        self.set_strategies({stock_code: strategy})

    def set_strategies(self, mapping: Dict[str, BaseStrategy]) -> None:
        """主入口：一次性设置多标的策略映射。"""
        self.strategies.update(mapping)

    def run_backtest(self, initial_capital: float = 1000000.0, allocation_strategy: str = 'equal', lot_size: int = 100,
                     industry_map: Dict[str, str] = None,
                     **params) -> Dict[str, Any]:
        """
        运行组合回测

        Args:
            initial_capital: 初始资金
            allocation_strategy: 资金分配方式（当前统一为等权，保留参数以兼容既有调用）
            lot_size: 最小交易单位（股）
            industry_map: 行业映射 {ts_code: industry}

        Returns:
            Dict[str, Any]: 回测结果
        """
        if not self.strategies:
            logger.error("未设置策略")
            return {}

        self.rejected_orders = []

        # 指标预计算：每只标的只算一次（策略读取 ma_* / macd / rsi 等预计算列，
        # 若缺失则 generate_signal 恒返回 hold，回测会静默空跑）
        prepared_data = {}
        for stock_code, data in self.data_dict.items():
            strategy = self.strategies.get(stock_code)
            prepared_data[stock_code] = (
                self._precompute_indicators(data, strategy) if strategy else data
            )

        runner = EventBacktestRunner(
            cost_model=self.cost_model,
            performance_analyzer=self.performance_analyzer,
            risk_engine=self.risk_engine,
        )
        result = runner.run(
            universe=prepared_data,
            strategies=self.strategies,
            initial_capital=initial_capital,
            lot_size=lot_size,
            industry_map=industry_map or {},
            result_meta={
                'strategy_names': {stock: strategy.name for stock, strategy in self.strategies.items()},
            },
        )

        self.trades = result.get('trades', [])
        self.equity_curve = result.get('equity_curve', [])
        self.rejected_orders = result.get('rejected_orders', [])
        self.positions = dict.fromkeys(self.data_dict, 0)
        for trade in self.trades:
            self.positions[trade['stock_code']] = trade['position']

        self.backtest_results = result
        return result
