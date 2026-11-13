#!/usr/bin/env python3
"""H1（过期单现金泄漏）账本守恒回归测试。

网页模型第二轮把「H1 未验证」列为正确性缺口；本地实测（2026-11-06）表明
**当前实现现金守恒成立**：扣款只发生在 `AccountBook.buy()` 成功路径，
`_append_expired` 之前没有任何冻结/预扣。本文件把该结论固化为不变量，
防止后续改动（尤其是显式冻结资金、订单状态机重构）悄悄引入泄漏。

不变量：订单全部作废的回测，其期末权益必须与「从未下单」逐分相等。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import polars as pl
import pytest

from src.backtest.engine.execution_core import AccountBook
from src.backtest.engine.portfolio_backtest_engine import PortfolioBacktestEngine
from src.backtest.strategies.base_strategy import BaseStrategy

CODE = "600000.SH"
PRICE = 10.0


class _FixedSignalStrategy(BaseStrategy):
    def __init__(self, signals: dict[int, str]):
        super().__init__(name="守恒测试固定信号")
        self.signals = dict(signals)

    def generate_signal(self, data, index):
        return self.signals.get(index, "hold")


def _panel(days: int = 8, volume: float = 1_000_000.0) -> dict[str, pl.DataFrame]:
    idx = pd.date_range("2023-01-02", periods=days, freq="B")
    return {CODE: pl.DataFrame({
        "date": idx,
        "open": np.full(days, PRICE),
        "high": np.full(days, PRICE * 1.005),
        "low": np.full(days, PRICE * 0.995),
        "close": np.full(days, PRICE),
        "volume": np.full(days, volume),
    })}


def _run(signals: dict[int, str], capital: float, data=None,
         risk_gate_policy: str = "adv_gate"):
    data = data or _panel()
    engine = PortfolioBacktestEngine(data)
    engine.set_strategies({CODE: _FixedSignalStrategy(signals)})
    engine.risk_gate_policy = risk_gate_policy
    return engine.run_backtest(initial_capital=capital)


def _assert_conserved(result, capital: float, expect_reasons: set[str] | None = None):
    assert result["trades"] == []
    assert result["final_equity"] == pytest.approx(capital, abs=1e-6)
    assert result["equity_curve"], "权益曲线不应为空"
    reasons = {o["reason"] for o in result["expired_orders"]}
    assert all(o["reason"] for o in result["expired_orders"])
    if expect_reasons:
        assert expect_reasons <= reasons, (expect_reasons, reasons)


def test_no_signal_is_exact_baseline():
    result = _run({0: "hold"}, 100_000.0)
    _assert_conserved(result, 100_000.0)


def test_expired_insufficient_cash_releases_nothing():
    """买不起 → 3 笔作废，现金与从未下单完全一致。"""
    result = _run({0: "buy", 2: "buy", 4: "buy"}, 100.0)
    _assert_conserved(result, 100.0, {"insufficient_cash"})
    assert len(result["expired_orders"]) == 3


def test_expired_suspended_orders_do_not_leak():
    """停牌（volume=0）→ 3 笔作废，现金与从未下单完全一致。

    整段停牌时 ADV 恒为 0，A1 的 ADV 闸门无从判定；本用例只验证账本守恒，
    因此显式声明 risk_gate_policy='off'（停牌由撮合层逐笔拒单，不需要风控）。
    """
    result = _run({0: "buy", 2: "buy", 4: "buy"}, 100_000.0,
                  _panel(volume=0.0), risk_gate_policy="off")
    _assert_conserved(result, 100_000.0, {"not_tradable"})


def test_ledger_write_failure_does_not_leak(monkeypatch):
    """账本 buy 抛 ValueError 时该单作废，不得中断回测也不得扣款。"""
    def reject(self, code, price, shares, cost):  # noqa: ANN001, ARG001
        raise ValueError("injected ledger failure")

    monkeypatch.setattr(AccountBook, "buy", reject)
    result = _run({0: "buy"}, 100_000.0)
    _assert_conserved(result, 100_000.0, {"ledger_rejected"})


def test_successful_trades_account_for_every_yuan():
    """对照：正常成交时权益减少量必须等于逐笔费用之和（价格恒定，无盯市差额）。"""
    capital = 100_000.0
    result = _run({0: "buy", 4: "sell"}, capital)
    assert len(result["trades"]) == 2
    total_cost = sum(t["cost"] for t in result["trades"])
    assert result["final_equity"] == pytest.approx(capital - total_cost, abs=1e-6)
    assert total_cost > 0


def test_pooled_cash_reads_and_writes_single_pool():
    """池模式：任意标的读同一池，卖出回款可被其它标的立即使用。"""
    book = AccountBook(100_000.0, ["A", "B"], pooled_cash=True)
    assert book.cash.get("A") == pytest.approx(100_000.0)
    assert book.cash.get("B") == pytest.approx(100_000.0)

    book.buy("A", 10.0, 1000, 0.0)
    book.sell("A", 11.0, 1000, 0.0)
    assert book.cash.get("B") == pytest.approx(101_000.0)
    assert book.total_cash == pytest.approx(101_000.0)
    assert book.total_equity() == pytest.approx(101_000.0)

    # 对照：默认逐票桶模式不串桶，B 桶仍为初始份额的一半
    buckets = AccountBook(100_000.0, ["A", "B"])
    buckets.buy("A", 10.0, 1000, 0.0)
    buckets.sell("A", 11.0, 1000, 0.0)
    assert buckets.cash.get("A") == pytest.approx(51_000.0)
    assert buckets.cash.get("B") == pytest.approx(50_000.0)
    assert buckets.total_cash == pytest.approx(101_000.0)


def test_pooled_cash_conservation_end_to_end():
    """池模式资金守恒：现金 + 持仓市值 = 初始资金 + 盈亏 − 成本。

    现金由成交流水独立复算（不读账本内部状态），持仓市值按期末收盘价重估，
    因此任何「回款落入未被计入的桶」「重复扣费」都会被这条等式抓到。
    """
    codes = ["600000.SH", "000001.SZ", "300001.SZ"]
    idx = pd.date_range("2023-01-02", periods=16, freq="B")
    data = {}
    for code in codes:
        close = np.array([10.0 + 0.02 * i for i in range(len(idx))])
        data[code] = pl.DataFrame({
            "date": idx,
            "open": close,
            "high": close * 1.001,
            "low": close * 0.999,
            "close": close,
            "volume": np.full(len(idx), 1_000_000.0),
        })

    script = {0: {"action": "buy", "target_weight": 0.32},
              8: {"action": "buy", "target_weight": 0.32},
              12: {"action": "sell", "target_weight": 0.1}}
    engine = PortfolioBacktestEngine(data)
    engine.set_strategies({code: _FixedSignalStrategy(script) for code in codes})
    capital = 1_000_000.0
    result = engine.run_backtest(initial_capital=capital, pooled_cash=True)

    trades = result["trades"]
    buy_notional = sum(t["price"] * t["shares"] for t in trades if t["signal"] == "buy")
    sell_notional = sum(t["price"] * t["shares"] for t in trades if t["signal"] == "sell")
    total_cost = sum(t["cost"] for t in trades)
    market_value = sum(qty * float(data[code]["close"][-1])
                       for code, qty in result["positions"].items())

    cash = capital + sell_notional - buy_notional - total_cost
    pnl = market_value + sell_notional - buy_notional

    assert sell_notional > 0 and total_cost > 0
    assert result["final_equity"] == pytest.approx(cash + market_value, abs=1e-6)
    assert cash + market_value == pytest.approx(capital + pnl - total_cost, abs=1e-6)

