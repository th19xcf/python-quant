"""策略试算 Sharpe 口径：CAGR ÷ 年化波动率（ddof=1 × √252），含边界。"""
import math
from datetime import date, timedelta

import numpy as np
import pytest

from src.quant.strategy_lab.backtest import MIN_SHARPE_SAMPLES, _cagr_sharpe


def _curve(values, start=date(2020, 1, 1)):
    return [(start + timedelta(days=i), float(v)) for i, v in enumerate(values)]


def _zigzag(n=200, step=0.01):
    """确定性的上下震荡序列，保证波动率非零。"""
    out, value = [], 1.0
    for i in range(n):
        value *= 1.0 + (step if i % 2 == 0 else -step * 0.8)
        out.append(value)
    return out


def test_matches_cagr_over_annualized_volatility():
    values = _zigzag()
    curve = _curve(values)
    annual = 0.08
    returns = np.diff(values) / np.asarray(values[:-1])
    vol = float(np.std(returns, ddof=1) * math.sqrt(252))
    assert _cagr_sharpe(curve, annual) == pytest.approx(round(annual / vol, 4))


def test_differs_from_arithmetic_daily_sharpe():
    """旧口径（均值/标准差×√252）与新口径（CAGR/波动）在同一条净值上不同。"""
    values = _zigzag()
    returns = np.diff(values) / np.asarray(values[:-1])
    old = float(np.mean(returns) / np.std(returns) * math.sqrt(252))
    new = _cagr_sharpe(_curve(values), 0.08)
    assert new != pytest.approx(old, abs=1e-6)
    assert math.isfinite(new)


def test_zero_volatility_returns_zero():
    flat = [1.0] * 100
    assert _cagr_sharpe(_curve(flat), 0.05) == 0.0


def test_insufficient_samples_returns_zero():
    assert _cagr_sharpe(_curve([1.0, 1.1, 1.2]), 0.05) == 0.0
    assert len(_curve([1.0] * (MIN_SHARPE_SAMPLES - 1))) < MIN_SHARPE_SAMPLES + 1


def test_nan_and_non_positive_equity_filtered():
    values = _zigzag()
    values[10] = float('nan')
    values[20] = 0.0
    values[30] = -1.0
    result = _cagr_sharpe(_curve(values), 0.08)
    assert math.isfinite(result)


def test_negative_cagr_keeps_negative_sharpe():
    assert _cagr_sharpe(_curve(_zigzag()), -0.05) < 0


def test_never_returns_nan_or_inf():
    cases = [([], 0.1), (_curve([1.0]), 0.1), (_curve([1.0] * 50), 0.1),
             (_curve([float('inf')] + _zigzag()), 0.1),
             (_curve(_zigzag()), float('nan'))]
    for curve, annual in cases:
        assert math.isfinite(_cagr_sharpe(curve, annual))
