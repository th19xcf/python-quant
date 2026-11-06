#!/usr/bin/env python3
"""性能回归门禁（2026-11-06 替换原「零断言」脚本）。

原实现两个测试只 ``return dict``，pytest 9 报 ``PytestReturnNotNoneWarning``，
性能退化没有任何拦截；且 ``test_data_fetch_performance`` 依赖真实 MySQL，
离线环境结果不可比，还含无副作用的 ``np.mean(diff)``。

本版改造：
- 全程零依赖合成面板（固定 seed），不连数据库、不读本地行情；
- 每个规模点跑 ``REPEAT`` 次取中位数，降低 CI 机器噪声；
- 两点拟合 ``T(n) = a + b*n``，分别约束**每次运行的固定开销 a**与
  **每 bar 开销 b**。P0 审计字段带来的固定成本由 a 监控，撮合热路径退化由 b 监控；
- 同时断言结果结构正确，避免「跑得快但结果为空」的假通过。

独立、更细粒度的基准见 ``reports/audit_2026-11-06/benchmark.py``。
"""
from __future__ import annotations

import statistics
import sys
import time
from typing import Any

import numpy as np
import pandas as pd
import polars as pl
import pytest

from src.backtest.engine.portfolio_backtest_engine import PortfolioBacktestEngine
from src.backtest.strategies.ma_strategy import MAStrategy

SEED = 20261106
REPEAT = 3
SMALL_CODES, LARGE_CODES, DAYS = 2, 20, 1000
SMALL_BARS, LARGE_BARS = SMALL_CODES * DAYS, LARGE_CODES * DAYS

#: 阈值取自 2026-11-06 本地实测（venv Python 3.14.9）：
#: 小负载 0.0124s / 2k bar、大负载 0.1014s / 20k bar，
#: a≈0.0025s、b≈4.95 µs/bar。绝对阈值留 3~20 倍余量，
#: 更敏感的跨机器判定交给下方「每 bar 开销 / 校准循环」比值。
MAX_FIXED_OVERHEAD_S = 0.05
MAX_US_PER_BAR = 15.0
MAX_WALL_SECONDS = 3.0
MAX_BAR_TO_CALIBRATION_RATIO = 300.0

#: 全量 ``pytest tests --cov=src`` 会话里被测代码明显变慢（实测 b 从
#: 单独运行的 4.24~4.67 → 全量会话的 16.87~18.38 µs/bar）。放大不只来自
#: coverage：单独跑 ``--cov`` 时仍只有 4.67 µs/bar，说明主因是长会话累积的
#: 进程状态（GC/缓存/内存），插桩只是叠加因素。因此对**两条绝对阈值**做
#: 有限归一，系数取 2.0 而非按放大倍数给足（收紧，不放水）：
#:   - 无插桩：15 µs/bar、0.05s（对 4.24 µs/bar 有 3.5× 余量）
#:   - 插桩：30 µs/bar、0.10s（对 17.35 µs/bar 有 1.7× 余量）
#: 另两条判据（墙钟上限、每 bar / 校准循环比值）**不乘系数**，保持严格。
#: 实测三种环境下注入 +150ms 固定开销与 +20 µs/bar 均会被拦截，见
#: reports/audit_2026-11-06/verify_perf_gate.py 与同目录注入探针。
#:
#: 检测方式必须覆盖两种插桩实现：coverage 7.x 在 Python 3.14 默认使用
#: ``sys.monitoring`` (SysMonitor)，不再调用 ``sys.settrace``，此时
#: ``sys.gettrace()`` 仍为 ``None``；旧版本 / 其他 profile 工具才走 trace 钩子。
TRACE_SLOWDOWN_FACTOR = 2.0


def _trace_factor() -> float:
    """返回插桩环境的耗时归一系数。

    ``coverage.Coverage.current()`` 是 coverage 官方 API，返回当前线程正在
    收集数据的实例，对 CTracer / PyTracer / SysMonitor 三种内核都有效。
    """
    if sys.gettrace() is not None:
        return TRACE_SLOWDOWN_FACTOR
    try:
        import coverage

        return (TRACE_SLOWDOWN_FACTOR
                if coverage.Coverage.current() is not None else 1.0)
    except (ImportError, AttributeError):
        # 未安装 coverage（无 --cov 的最小环境）或版本过旧无此 API：视为无插桩
        return 1.0


def _calibration_per_iteration(iterations: int = 200_000) -> float:
    """同进程纯 Python 循环校准：机器变慢时分子分母同比例变化。"""
    acc = 0.0
    start = time.perf_counter()
    for i in range(iterations):
        acc += (i % 7) * 0.125
    elapsed = time.perf_counter() - start
    if acc < 0:  # 防止循环被优化掉
        raise AssertionError("校准循环未执行")
    return elapsed / iterations


def _make_panel(ncodes: int, days: int, seed: int) -> dict[str, pl.DataFrame]:
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2020-01-02", periods=days, freq="B")
    panel: dict[str, pl.DataFrame] = {}
    for i in range(ncodes):
        close = np.maximum(10.0 + np.cumsum(rng.normal(0, 0.08, days)), 1.0)
        open_ = close * (1 + rng.normal(0, 0.004, days))
        high = np.maximum(open_, close) * (1 + np.abs(rng.normal(0, 0.003, days)))
        low = np.minimum(open_, close) * (1 - np.abs(rng.normal(0, 0.003, days)))
        volume = np.full(days, 1_000_000.0)
        panel[f"{600000 + i:06d}.SH"] = pl.DataFrame({
            "date": dates, "open": open_, "high": high, "low": low,
            "close": close, "volume": volume,
        })
    return panel


def _run_once(data: dict[str, pl.DataFrame]) -> dict[str, Any]:
    engine = PortfolioBacktestEngine(data)
    engine.set_strategies({code: MAStrategy(5, 20) for code in data})
    return engine.run_backtest(initial_capital=1_000_000.0)


def _median_seconds(data: dict[str, pl.DataFrame]) -> float:
    samples = []
    for _ in range(REPEAT):
        start = time.perf_counter()
        result = _run_once(data)
        elapsed = time.perf_counter() - start
        assert result["equity_curve"], "回测结果不得为空"
        assert isinstance(result["trades"], list)
        assert "cost_inputs" in result
        assert result["final_equity"] > 0
        samples.append(elapsed)
    return statistics.median(samples)


@pytest.mark.perf
def test_backtest_fixed_and_per_bar_cost_gate():
    """分别约束固定开销 a 与每 bar 开销 b；任一退化都会让本用例失败。"""
    small = _make_panel(SMALL_CODES, DAYS, SEED)
    large = _make_panel(LARGE_CODES, DAYS, SEED + 1)

    factor = _trace_factor()
    t_small = _median_seconds(small)
    t_large = _median_seconds(large)
    # 墙钟上限保留严格值：实测插桩全量会话仅 0.35s，距 3.0s 有 8× 余量
    assert t_large < MAX_WALL_SECONDS, f"大负载退化: {t_large:.4f}s"

    per_bar_s = (t_large - t_small) / (LARGE_BARS - SMALL_BARS)
    fixed_s = t_small - per_bar_s * SMALL_BARS
    per_bar_us = per_bar_s * 1e6

    assert per_bar_us <= MAX_US_PER_BAR * factor, (
        f"每 bar 开销退化: {per_bar_us:.2f} µs/bar > {MAX_US_PER_BAR * factor}")
    assert fixed_s <= MAX_FIXED_OVERHEAD_S * factor, (
        f"每次运行固定开销退化: {fixed_s:.4f}s > {MAX_FIXED_OVERHEAD_S * factor}")
    ratio = per_bar_s / _calibration_per_iteration()
    # 相对判据不乘系数：单独运行时比值约 180~210，注入 +20 µs/bar 会升到
    # 1000+ 被拦下；全量会话比值基线偏低（约 67），此时由上面的 30 µs/bar
    # 绝对阈值兜底。两种环境互补，避免插桩模式把门禁变成摆设。
    assert ratio <= MAX_BAR_TO_CALIBRATION_RATIO, (
        f"相对机器算力的每 bar 开销退化: {ratio:.1f} > "
        f"{MAX_BAR_TO_CALIBRATION_RATIO}")
    assert per_bar_us > 0, "测量无效：大负载与小负载耗时未分离"


@pytest.mark.perf
def test_result_shape_guard():
    """门禁不能只测时间：结果字段结构必须完整（防止快速但空结果的假通过）。"""
    result = _run_once(_make_panel(2, 60, SEED + 2))
    assert result["equity_curve"]
    assert "cost_inputs" in result
    assert "risk_gate" in result
    assert set(result) >= {"trades", "expired_orders", "rejected_orders",
                           "final_equity", "performance", "risk_summary"}
