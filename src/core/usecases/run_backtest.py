#!/usr/bin/env python3

"""
run_backtest —— 回测唯一用例入口。

CLI / GUI / 服务端 都应该调用这个函数，不要各自 new BacktestEngine 然后手写循环。
风控闸门通过 risk_engine 参数注入——不注入就是 no-op（所有订单放行）。

Usage:
    from src.core.usecases import run_backtest
    from src.risk import RiskEngine, MaxPositionWeight, MaxDrawdownCircuit
    from src.backtest.strategies.ma_strategy import MAStrategy

    risk = RiskEngine([MaxPositionWeight(0.10), MaxDrawdownCircuit(0.15)])
    result = run_backtest(
        ts_code='000001.SZ',
        start_date='2023-01-01',
        end_date='2024-01-01',
        strategy=MAStrategy(),
        risk_engine=risk,
        initial_capital=1_000_000,
    )
    print(result['risk_summary'])   # 风控拒单情况
    print(result['total_return'])
"""

from __future__ import annotations

from typing import Any, Dict, Union

import polars as pl
from loguru import logger

from src.backtest.engine.backtest_engine import BacktestEngine
from src.backtest.engine.portfolio_backtest_engine import PortfolioBacktestEngine
from src.backtest.strategies.base_strategy import BaseStrategy
from src.risk import RiskEngine


def run_backtest(
    ts_code: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    data: Union[pl.DataFrame, Dict[str, pl.DataFrame]] | None = None,
    strategy: BaseStrategy | None = None,
    strategies: Dict[str, BaseStrategy] | None = None,
    risk_engine: RiskEngine | None = None,
    initial_capital: float = 1_000_000.0,
    lot_size: int = 100,
    industry: str = '',
    industry_map: Dict[str, str] | None = None,
    st_status: Dict[str, Any] | None = None,
    db_manager: Any = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    运行回测（单票 或 组合）。

    两种模式：
        1. 单票回测：传 ts_code + strategy，或 data（单票 DataFrame）+ strategy
        2. 组合回测：传 data（dict of DataFrame）+ strategies（dict of Strategy）

    Args:
        ts_code:       单票代码（'000001.SZ'）。传了就会用 DataFetcher 取数。
        start_date:    '2023-01-01' 字符串。
        end_date:      '2024-01-01' 字符串。
        data:          直接传入数据（跳过取数）。单票=pl.DataFrame，组合=Dict[str, pl.DataFrame]。
        strategy:      单票策略实例。
        strategies:    组合策略字典 {ts_code: BaseStrategy}。
        risk_engine:   风控引擎。None 表示不启用。
        initial_capital: 初始资金。
        lot_size:      最小交易单位（A股 100 股）。
        industry:      单票行业标签（风控 MaxIndustryWeight 用）。
        industry_map:  组合行业映射 {ts_code: industry}。
        st_status:     ST 状态映射，值可为 bool / None / 区间列表。
                       不传时若持有 db_manager 会自动从 ST 时间维表加载。
        db_manager:    可选的数据库管理器；传入后即使 data 已提供也会加载
                       ST 时间维表。为 None 且需要取数时自动创建。

    Returns:
        dict —— BacktestEngine.run_backtest() 的返回值，包含：
            - initial_capital, final_equity, total_return
            - trades, equity_curve, performance
            - risk_summary（如有风控）、rejected_orders（如有风控）
    """
    # ---- 数据准备 ----
    db = None
    owns_db = False
    if data is None and ts_code:
        # 按需 import 避免循环依赖
        from src.data.managers.data_fetcher import DataFetcher
        from src.database.db_manager import DatabaseManager
        from src.utils.config import Config

        config = Config()
        db = db_manager
        owns_db = db is None
        if db is None:
            db = DatabaseManager(config)
        db.connect()
        fetcher = DataFetcher(config, db_manager=db)
        data = fetcher.get_stock_data(
            ts_code, start_date or '', end_date or '',
            adjustment_type='hfq',
        )

    # ---- ST 时间维表：未显式传入时按标的与日期范围加载 ----
    if st_status is None:
        st_db = db if db is not None else db_manager
        if st_db is not None and data is not None:
            if isinstance(data, dict):
                codes = list(data.keys())
            else:
                codes = [ts_code] if ts_code else []
            if codes:
                try:
                    from src.data.services.st_status_service import StStatusService

                    st_status = StStatusService(db_manager=st_db).get_status_intervals(
                        codes, start_date, end_date,
                    )
                # 兜底理由：ST 维表加载失败不影响回测，按未知处理
                except Exception as e:  # noqa: BLE001
                    logger.warning(f"加载 ST 时间维表失败，按未知处理: {e}")
                    st_status = None

    if owns_db and db is not None:
        db.disconnect()

    if data is None:
        raise ValueError("run_backtest: 必须传 data 或 ts_code")

    # ---- 分支：单票 vs 组合 ----
    if isinstance(data, dict):
        # 组合回测
        strategies = strategies or {}
        if not strategies:
            raise ValueError("组合回测需要 strategies dict")

        engine = PortfolioBacktestEngine(data)
        engine.risk_engine = risk_engine
        engine.set_strategies(strategies)

        # 显式 st_status 参数优先；调用方 kwargs 里已有的同名参数不覆盖
        if st_status:
            kwargs.setdefault('st_status', st_status)
        return engine.run_backtest(
            initial_capital=initial_capital,
            lot_size=lot_size,
            industry_map=industry_map or {},
            **kwargs,
        )

    else:
        # 单票回测
        if strategy is None:
            raise ValueError("单票回测需要 strategy")

        engine = BacktestEngine(data)
        engine.risk_engine = risk_engine
        engine.set_strategy(strategy)

        if 'is_st' not in kwargs:
            kwargs['is_st'] = st_status.get(ts_code) if st_status and ts_code else None

        return engine.run_backtest(
            initial_capital=initial_capital,
            lot_size=lot_size,
            ts_code=ts_code or '',
            industry=industry,
            **kwargs,
        )
