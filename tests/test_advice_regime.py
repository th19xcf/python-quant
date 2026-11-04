"""市场状态规则的无未来函数与 T+1 生效测试。"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import numpy as np
import polars as pl
import pytest

from src.advice.regime import (
    RegimeParams,
    breadth_position,
    executed_position,
    ma_position,
    oracle_position,
    overlay_curve,
    rolling_percentile,
    vol_target_position,
)


def _index_frame(days: int = 260) -> pl.DataFrame:
    rng = np.random.default_rng(7)
    dates = [date(2023, 1, 2) + timedelta(days=offset) for offset in range(days)]
    close = 3_000.0 * np.cumprod(1.0 + rng.normal(0.0004, 0.012, days))
    return pl.DataFrame({'date': dates, 'close': close})


def _breadth_frame(days: int = 320) -> pl.DataFrame:
    rng = np.random.default_rng(11)
    dates = [date(2022, 1, 4) + timedelta(days=offset) for offset in range(days)]
    up = rng.integers(800, 4_000, days).astype(float)
    down = rng.integers(800, 4_000, days).astype(float)
    return pl.DataFrame(
        {'trade_date': dates, 'up_count': up, 'down_count': down})


class _ZeroCost:
    """零成本桩，仅用于验证仓位时序。"""

    def calculate_buy_cost(self, shares: float, price: float,
                           trade_date: Any = None, ts_code: str = '') -> float:
        return 0.0

    def calculate_sell_cost(self, shares: float, price: float,
                            trade_date: Any = None, ts_code: str = '') -> float:
        return 0.0


class _FlatCost:
    """买卖各 1% 的成本桩，用于验证调仓成本作用在变动当日。"""

    def calculate_buy_cost(self, shares: float, price: float,
                           trade_date: Any = None, ts_code: str = '') -> float:
        return shares * price * 0.01

    def calculate_sell_cost(self, shares: float, price: float,
                            trade_date: Any = None, ts_code: str = '') -> float:
        return shares * price * 0.01


def _assert_prefix_equal(full: pl.DataFrame, prefix: pl.DataFrame) -> None:
    assert full.head(prefix.height).equals(prefix), '截断前缀结果不一致'


def test_ma_position_has_no_lookahead() -> None:
    frame = _index_frame()
    params = RegimeParams()
    full = ma_position(frame, params)
    prefix = ma_position(frame.head(180), params)
    _assert_prefix_equal(full, prefix)


def test_breadth_position_has_no_lookahead() -> None:
    breadth = _breadth_frame()
    params = RegimeParams()
    full = breadth_position(breadth, params)
    prefix = breadth_position(breadth.head(220), params)
    _assert_prefix_equal(full, prefix)


def test_vol_target_position_has_no_lookahead() -> None:
    frame = _index_frame()
    params = RegimeParams()
    full = vol_target_position(frame, params)
    prefix = vol_target_position(frame.head(180), params)
    _assert_prefix_equal(full, prefix)


def test_rolling_percentile_is_causal() -> None:
    rng = np.random.default_rng(3)
    values = pl.Series('value', rng.normal(size=200))
    window = 60
    full = rolling_percentile(values, window)
    prefix = rolling_percentile(values.head(120), window)
    assert full.head(120).equals(prefix)


def test_executed_position_is_t_plus_one() -> None:
    dates = [date(2023, 1, 2) + timedelta(days=offset) for offset in range(5)]
    position = pl.DataFrame({
        'date': dates,
        'position': [1.0, 0.0, 1.0, 1.0, 0.5],
    })
    executed = executed_position(position)
    # 第 t 行执行的是 t-1 日收盘确定的仓位
    assert executed['exec_position'][1:].to_list() == [1.0, 0.0, 1.0, 1.0]
    # 调仓变化发生在执行当日
    assert executed['delta'][2:].to_list() == [-1.0, 1.0, 0.0]


def test_overlay_uses_previous_day_position() -> None:
    dates = [date(2023, 1, 2) + timedelta(days=offset) for offset in range(5)]
    equity = 1_000_000.0 * np.array([1.0, 1.10, 1.045, 1.0659, 1.076559])
    curve = pl.DataFrame({'date': dates, 'equity': equity})
    position = pl.DataFrame({
        'date': dates,
        'position': [1.0, 0.0, 1.0, 1.0, 0.5],
    })
    overlay = overlay_curve(curve, position, _ZeroCost())
    # t1 用 p0=1 吃满 +10%；t2 用 p1=0，不应吃到 -5%
    assert overlay['net_return'][1] == pytest.approx(0.10)
    assert overlay['net_return'][2] == pytest.approx(0.0)
    assert overlay['net_return'][3] == pytest.approx(0.02)
    assert overlay['equity_overlay'][-1] == pytest.approx(
        1_000_000.0 * 1.10 * 1.0 * 1.02 * 1.01)


def test_overlay_charges_cost_on_turnover_day() -> None:
    dates = [date(2023, 1, 2) + timedelta(days=offset) for offset in range(4)]
    curve = pl.DataFrame({
        'date': dates,
        'equity': 1_000_000.0 * np.array([1.0, 1.10, 1.045, 1.0659]),
    })
    position = pl.DataFrame({
        'date': dates,
        'position': [1.0, 0.0, 1.0, 1.0],
    })
    overlay = overlay_curve(curve, position, _FlatCost())
    # t2：清仓（delta=-1）成本 1%，当天毛收益 -5% 被 0 仓位屏蔽
    assert overlay['overlay_cost'][2] == pytest.approx(0.01)
    assert overlay['net_return'][2] == pytest.approx(-0.01)
    # t3：加回（delta=+1）成本 1%，毛收益 +2%
    assert overlay['overlay_cost'][3] == pytest.approx(0.01)
    assert overlay['net_return'][3] == pytest.approx(0.01)


def test_oracle_position_uses_future_returns_and_drops_tail() -> None:
    returns = pl.Series('ret', [0.02, -0.01, 0.03, -0.02, 0.01, 0.02])
    position = oracle_position(returns, horizon=2)
    expected = [1.0, 1.0, 1.0, 0.0, None, None]
    actual = position.to_list()
    for value, target in zip(actual, expected, strict=True):
        if target is None:
            assert value is None or value != value  # NaN
        else:
            assert value == pytest.approx(target)
