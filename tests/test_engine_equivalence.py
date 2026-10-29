"""单票引擎与单标的组合引擎的撮合等价性测试。

两个引擎都委托 execution_core.EventBacktestRunner，本文件把这种结构上的
"同源"固化为可执行断言：同一份数据、同样的初始资金与参数，必须产生完全
一致的成交、资金曲线与最终持仓。任何一方私自新增撮合分支都会在此暴露。
"""

import numpy as np
import polars as pl
import pytest

from src.backtest.engine.backtest_engine import BacktestEngine
from src.backtest.engine.portfolio_backtest_engine import PortfolioBacktestEngine
from src.backtest.strategies.ma_strategy import MAStrategy

TS_CODE = "600000.SH"


def _make_df() -> pl.DataFrame:
    """确定性的震荡上行序列，保证 MA 策略能产生多次买卖信号。"""
    import pandas as pd

    n = 150
    idx = np.arange(n, dtype=float)
    close = 100.0 + 8.0 * np.sin(idx / 9.0) + 0.05 * idx
    dates = pd.date_range("2023-01-02", periods=n, freq="B")

    return pl.DataFrame({
        "date": dates,
        "open": close * 0.999,
        "high": close * 1.004,
        "low": close * 0.996,
        "close": close,
        "volume": np.full(n, 1_000_000),
    })


def _run_single(df: pl.DataFrame) -> dict:
    # 策略实例必须每个引擎各建一份：策略可能持有状态，复用会污染结果
    engine = BacktestEngine(df)
    engine.set_strategy(MAStrategy(short_window=5, long_window=20))
    return engine.run_backtest(initial_capital=1_000_000.0, ts_code=TS_CODE)


def _run_portfolio(df: pl.DataFrame) -> dict:
    engine = PortfolioBacktestEngine({TS_CODE: df})
    engine.set_strategy(TS_CODE, MAStrategy(short_window=5, long_window=20))
    return engine.run_backtest(initial_capital=1_000_000.0)


def test_single_equals_one_stock_portfolio():
    df = _make_df()

    single_result = _run_single(df)
    portfolio_result = _run_portfolio(df)

    # 先确保用例本身有效：没有成交的等价性没有意义
    assert len(single_result["trades"]) > 0, "测试数据未触发任何成交，等价性断言无意义"
    assert len(single_result["trades"]) % 2 == 0, "成交应成对出现（买入后卖出）"

    assert single_result["trades"] == portfolio_result["trades"]
    assert single_result["equity_curve"] == portfolio_result["equity_curve"]
    assert single_result["positions"] == portfolio_result["positions"]
    assert single_result["final_equity"] == pytest.approx(portfolio_result["final_equity"])
    assert single_result["total_return"] == pytest.approx(portfolio_result["total_return"])
    assert single_result["rejected_orders"] == portfolio_result["rejected_orders"]
    assert single_result["expired_orders"] == portfolio_result["expired_orders"]
