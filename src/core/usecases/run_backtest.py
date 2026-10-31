#!/usr/bin/env python3

"""
run_backtest —— 回测唯一用例入口。

CLI / GUI / 服务端 都应该调用这个函数，不要各自 new BacktestEngine 然后手写循环。
风控闸门默认装配安全规则集（涨跌停不可成交 + 停牌不可交易，与撮合内核
既有判定同源，不改变成交结果）；需要权重/行业/回撤等规则时显式传 risk_engine，
需要完全关闭风控时传 enable_default_risk=False。

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

import threading
from typing import Any, Dict, Union

import polars as pl
from loguru import logger

from src.backtest.engine.backtest_engine import BacktestEngine
from src.backtest.engine.portfolio_backtest_engine import PortfolioBacktestEngine
from src.backtest.strategies.base_strategy import BaseStrategy
from src.risk import RiskEngine, default_risk_engine

# 长驻取数资源：CLI / GUI 会反复调用 run_backtest，若每次新建
# DatabaseManager 与 DataFetcher，连接池与线程池随之反复创建，
# 线程数随调用次数单调增长。这里进程内只保留一份，跨调用复用。
_shared_lock = threading.Lock()
_shared_fetcher: Any = None
_shared_db_manager: Any = None


def _get_shared_data_fetcher() -> tuple[Any, Any]:
    """获取（或懒创建）进程内长驻的 ``(DataFetcher, DatabaseManager)``。"""
    global _shared_fetcher, _shared_db_manager
    with _shared_lock:
        if _shared_fetcher is None or _shared_db_manager is None:
            from src.data.managers.data_fetcher import DataFetcher
            from src.database.db_manager import DatabaseManager
            from src.utils.config import Config

            db = DatabaseManager(Config())
            db.connect()
            _shared_db_manager = db
            _shared_fetcher = DataFetcher(db.config, db_manager=db)
        return _shared_fetcher, _shared_db_manager


def close_shared_data_fetcher() -> None:
    """关闭进程内长驻取数资源（测试清理 / 进程退出钩子使用），可重复调用。"""
    global _shared_fetcher, _shared_db_manager
    with _shared_lock:
        fetcher, db = _shared_fetcher, _shared_db_manager
        _shared_fetcher = None
        _shared_db_manager = None
    if fetcher is not None:
        fetcher.close()
    if db is not None:
        db.disconnect()


def _assemble_risk_engine(risk_engine: RiskEngine | None,
                          enable_default_risk: bool) -> RiskEngine | None:
    """风控装配：显式传入优先；否则按开关装配默认安全规则集（False 表示完全关闭）。"""
    if risk_engine is not None:
        return risk_engine
    return default_risk_engine() if enable_default_risk else None


def run_backtest(
    ts_code: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    data: Union[pl.DataFrame, Dict[str, pl.DataFrame]] | None = None,
    strategy: BaseStrategy | None = None,
    strategies: Dict[str, BaseStrategy] | None = None,
    risk_engine: RiskEngine | None = None,
    enable_default_risk: bool = True,
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
        risk_engine:   风控引擎。None 时按 enable_default_risk 装配默认安全规则集。
        enable_default_risk: 默认 True——装配「涨跌停不可成交 + 停牌不可交易」，
                       与撮合内核既有判定同源、不改变成交结果；False 表示完全关闭风控。
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
    # ---- 风控装配：生产路径不允许"无风控即全放行" ----
    risk_engine = _assemble_risk_engine(risk_engine, enable_default_risk)

    # ---- 数据准备 ----
    db = db_manager
    if data is None and ts_code:
        # 按需 import 避免循环依赖
        from src.data.managers.data_fetcher import DataFetcher

        if db is None:
            # 默认路径复用进程内长驻实例；本次调用不负责关闭它
            fetcher, db = _get_shared_data_fetcher()
        else:
            # 调用方显式持有 db_manager，连接复用，避免每次回测新建连接池
            if not db.is_connected():
                db.connect()
            fetcher = DataFetcher(db.config, db_manager=db)
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

        single_engine = BacktestEngine(data)
        single_engine.risk_engine = risk_engine
        single_engine.set_strategy(strategy)

        if 'is_st' not in kwargs:
            kwargs['is_st'] = st_status.get(ts_code) if st_status and ts_code else None

        return single_engine.run_backtest(
            initial_capital=initial_capital,
            lot_size=lot_size,
            ts_code=ts_code or '',
            industry=industry,
            **kwargs,
        )
