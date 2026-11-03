"""QuantStats 报告模块单测：净值校验、边界报错与指标口径（无网络依赖）。"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

pytest.importorskip('quantstats')

from src.quant.reports.quantstats_report import (  # noqa: E402
    build_quantstats_metrics,
    equity_curve_to_series,
    generate_quantstats_report,
)


def _equity_series(periods: int = 120, seed: int = 11) -> pd.Series:
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range('2023-01-03', periods=periods)
    values = 100_000 * np.cumprod(1 + rng.normal(0.0004, 0.012, periods))
    return pd.Series(values, index=dates, name='equity')


def test_list_records_are_sorted_to_datetime_index():
    records = [
        {'date': '2024-01-03', 'equity': 101.0},
        {'date': '2024-01-02', 'equity': 100.0},
        {'date': '2024-01-04', 'equity': 102.0},
    ]
    series = equity_curve_to_series(records)
    assert isinstance(series.index, pd.DatetimeIndex)
    assert series.index.is_monotonic_increasing
    assert list(series.to_numpy()) == [100.0, 101.0, 102.0]


def test_dataframe_input_with_custom_columns():
    frame = pd.DataFrame({
        'trade_date': pd.to_datetime(['2024-01-02', '2024-01-03']),
        'nav': [100.0, 103.0],
    })
    series = equity_curve_to_series(frame, date_col='trade_date', equity_col='nav')
    assert series.name == 'nav'
    assert series.iloc[-1] == pytest.approx(103.0)


def test_empty_and_single_point_raise():
    with pytest.raises(ValueError, match='为空'):
        equity_curve_to_series([])
    with pytest.raises(ValueError, match='至少需要 2 个'):
        equity_curve_to_series([{'date': '2024-01-02', 'equity': 100.0}])


def test_duplicate_dates_raise():
    with pytest.raises(ValueError, match='重复日期'):
        equity_curve_to_series([
            {'date': '2024-01-02', 'equity': 100.0},
            {'date': '2024-01-02', 'equity': 101.0},
            {'date': '2024-01-03', 'equity': 102.0},
        ])


def test_invalid_dates_and_non_positive_nav_raise():
    with pytest.raises(ValueError, match='无法解析'):
        equity_curve_to_series([
            {'date': 'not-a-date', 'equity': 100.0},
            {'date': '2024-01-03', 'equity': 101.0},
        ])
    with pytest.raises(ValueError, match='严格为正'):
        equity_curve_to_series([
            {'date': '2024-01-02', 'equity': 100.0},
            {'date': '2024-01-03', 'equity': 0.0},
        ])
    with pytest.raises(ValueError, match='NaN/Inf'):
        equity_curve_to_series([
            {'date': '2024-01-02', 'equity': 100.0},
            {'date': '2024-01-03', 'equity': float('nan')},
        ])


def test_numeric_series_index_is_rejected():
    series = pd.Series([100.0, 101.0, 102.0])  # RangeIndex
    with pytest.raises(ValueError, match='DatetimeIndex'):
        equity_curve_to_series(series)


def test_missing_columns_raise():
    with pytest.raises(ValueError, match='缺少列'):
        equity_curve_to_series(pd.DataFrame({'date': ['2024-01-02']}))


def test_metrics_are_decimal_and_consistent():
    equity = _equity_series()
    returns = equity.pct_change().dropna()
    metrics = build_quantstats_metrics(equity)

    expected_total = equity.iloc[-1] / equity.iloc[0] - 1
    assert metrics['total_return'] == pytest.approx(expected_total)
    assert metrics['max_drawdown'] > 0  # 已取绝对值，正数小数口径
    assert metrics['n_periods'] == len(returns)
    assert metrics['start_date'] == '2023-01-03'
    assert {
        'annual_return', 'sharpe', 'sortino', 'volatility', 'calmar',
        'win_rate', 'profit_factor', 'ulcer_index',
    } <= set(metrics)


def test_generate_report_writes_html(tmp_path):
    equity = _equity_series()
    output = tmp_path / 'quantstats_demo.html'
    result = generate_quantstats_report(equity, output_path=output, title='单测绩效报告')

    assert output.exists()
    assert output.stat().st_size > 10_000
    assert result['report_path'] == str(output)
    assert set(result['metrics']) >= {'annual_return', 'sharpe', 'max_drawdown'}
    assert len(result['returns']) == len(equity) - 1
    html = output.read_text(encoding='utf-8')
    assert '<html' in html.lower()
    assert '单测绩效报告' in html
