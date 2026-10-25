#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
组合回测引擎，撮合逻辑全部委托给 execution_core 统一内核
"""

from typing import Dict, Any
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

    def set_strategy(self, stock_code: str, strategy: BaseStrategy):
        """
        为特定股票设置策略

        Args:
            stock_code: 股票代码
            strategy: 策略对象
        """
        self.strategies[stock_code] = strategy

    def run_backtest(self, initial_capital: float = 1000000.0, allocation_strategy: str = 'equal', lot_size: int = 100, limit_pct: float = 0.1,
                     industry_map: Dict[str, str] = None,
                     **params) -> Dict[str, Any]:
        """
        运行组合回测

        Args:
            initial_capital: 初始资金
            allocation_strategy: 资金分配方式（当前统一为等权，保留参数以兼容既有调用）
            lot_size: 最小交易单位（股）
            limit_pct: 涨跌停幅度阈值（无代码时使用）
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
            limit_pct=limit_pct,
            industry_map=industry_map or {},
            result_meta={
                'strategy_names': {stock: strategy.name for stock, strategy in self.strategies.items()},
            },
        )

        self.trades = result.get('trades', [])
        self.equity_curve = result.get('equity_curve', [])
        self.rejected_orders = result.get('rejected_orders', [])
        self.positions = {stock: 0 for stock in self.data_dict}
        for trade in self.trades:
            self.positions[trade['stock_code']] = trade['position']

        self.backtest_results = result
        return result
