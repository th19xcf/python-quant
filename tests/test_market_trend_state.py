"""市场趋势状态纯函数测试：标签、缺失处理、asof 对齐与分位窗口。"""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import polars as pl
import pytest

from src.quant.analysis.market_trend_state import (
    _label_for,
    _rolling_percentile,
    evaluate_market_trend,
)


def _dates(count: int) -> list[date]:
    start = date(2024, 1, 1)
    return [start + timedelta(days=offset) for offset in range(count)]


def _panel(close: list[float], with_optional: bool = True) -> dict[str, pl.DataFrame]:
    dates = _dates(len(close))
    panel = {
        'index': pl.DataFrame({
            'date': dates,
            'close': close,
            'amount': [1.0e12] * len(close),
        }),
    }
    if not with_optional:
        return panel
    panel['breadth_ma'] = pl.DataFrame({
        'date': dates,
        'pct_above_ma60': [70.0] * len(close),
        'pct_above_ma120': [65.0] * len(close),
    })
    panel['breadth'] = pl.DataFrame({
        'trade_date': dates,
        'up_count': [3000] * len(close),
        'down_count': [1000] * len(close),
        'total_count': [4200] * len(close),
        'limit_up_count': [60] * len(close),
        'limit_down_count': [5] * len(close),
        'new_high_250': [120] * len(close),
        'new_low_250': [20] * len(close),
    })
    panel['state_vector'] = pl.DataFrame({
        'date': dates,
        'valuation_erp_q': [80.0] * len(close),
        'val_earn_yield_q': [75.0] * len(close),
        'lev_margin_buy_ratio_q': [70.0] * len(close),
    })
    panel['margin'] = pl.DataFrame({
        'date': dates + dates,
        'market': ['sh'] * len(close) + ['sz'] * len(close),
        'fin_balance': [1.0e12 + index * 1.0e9 for index in range(len(close))] * 2,
    })
    return panel


def test_rolling_percentile_requires_full_window() -> None:
    values = np.arange(100, dtype=float)
    result = _rolling_percentile(values, window=250, min_samples=250)
    assert np.isnan(result).all()

    values = np.arange(250, dtype=float)
    result = _rolling_percentile(values, window=250, min_samples=250)
    assert result[-1] == pytest.approx(99.8)
    assert np.isnan(result[-2])


def test_label_thresholds_are_monotonic() -> None:
    assert _label_for(0.8) == '强趋势上行'
    assert _label_for(0.3) == '弱上行'
    assert _label_for(0.0) == '震荡'
    assert _label_for(-0.3) == '弱势下行'
    assert _label_for(-0.8) == '强下行'


def test_uptrend_scores_positive() -> None:
    close = [100.0] * 260 + [100.0 * (1.006 ** (index + 1)) for index in range(60)]
    state = evaluate_market_trend(_panel(close))
    assert state.total_score > 0
    trend = next(layer for layer in state.layers if layer.key == 'trend')
    assert trend.score is not None and trend.score > 0
    assert state.label in {'弱上行', '强趋势上行'}


def test_downtrend_scores_negative() -> None:
    close = [100.0] * 260 + [100.0 * (0.994 ** (index + 1)) for index in range(60)]
    state = evaluate_market_trend(_panel(close))
    assert state.total_score < 0
    trend = next(layer for layer in state.layers if layer.key == 'trend')
    assert trend.score is not None and trend.score < 0


def test_missing_optional_panels_renormalizes_weights() -> None:
    close = [100.0] * 260 + [100.0 * (1.006 ** (index + 1)) for index in range(60)]
    state = evaluate_market_trend(_panel(close, with_optional=False))
    breadth = next(layer for layer in state.layers if layer.key == 'breadth')
    value = next(layer for layer in state.layers if layer.key == 'value')
    assert breadth.score is None and breadth.contribution is None
    assert value.score is None and value.contribution is None
    available = [layer for layer in state.layers if layer.score is not None]
    assert sum(layer.weight for layer in available) == pytest.approx(1.0)
    assert any('未参与总分' in note for note in state.notes)


def test_asof_snaps_to_last_trading_day() -> None:
    close = [100.0] * 260 + [100.0 * (1.001 ** (index + 1)) for index in range(60)]
    panel = _panel(close)
    last = list(panel['index']['date'])[-1]
    state = evaluate_market_trend(panel, asof=last + timedelta(days=3))
    assert state.asof == last
