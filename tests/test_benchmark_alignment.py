"""基准对齐公共函数的单元测试。"""

from __future__ import annotations

import datetime as dt

import polars as pl

from tools.benchmark_alignment import (
    aligned_benchmark_curve,
    daily_excess_stats,
    equal_weight_daily,
    same_start_excess_annualized,
    unaligned_benchmark_curve,
)


def _frame(returns: list[float]) -> pl.DataFrame:
    rows = []
    for offset, value in enumerate(returns):
        day = dt.date(2024, 1, 2) + dt.timedelta(days=offset)
        for code in ('000001.SZ', '600000.SH'):
            rows.append({'date': day, 'ts_code': code, 'ret': value})
    return pl.DataFrame(rows)


def test_equal_weight_daily_averages_codes():
    frame = pl.DataFrame({
        'date': [dt.date(2024, 1, 2)] * 3,
        'ts_code': ['a', 'b', 'c'],
        'ret': [0.02, 0.04, 0.06],
    })
    daily = equal_weight_daily(frame)
    assert daily.height == 1
    assert abs(float(daily['ret'][0]) - 0.04) < 1e-12


def test_aligned_curve_skips_first_day_and_starts_at_one():
    frame = _frame([0.10, -0.20, 0.05])
    curve = aligned_benchmark_curve(frame)
    assert curve.height == 2
    assert abs(float(curve['equity'][0]) - 1.0) < 1e-12
    assert abs(float(curve['equity'][1]) - 1.05) < 1e-12
    unaligned = unaligned_benchmark_curve(frame)
    assert unaligned.height == 3
    assert abs(float(unaligned['equity'][0]) - 1.10) < 1e-12
    assert abs(float(unaligned['equity'][-1]) - 0.924) < 1e-12


def test_aligned_and_unaligned_differ_by_overnight_gap():
    frame = _frame([0.10, 0.10, 0.05])
    aligned = aligned_benchmark_curve(frame)
    unaligned = unaligned_benchmark_curve(frame)
    # 修正后从 d1 收盘起算，只保留 d1 之后的收益（10%→5% 中的 5%）
    assert float(unaligned['equity'][-1]) > float(aligned['equity'][-1])
    assert abs(float(unaligned['equity'][-1]) - 1.2705) < 1e-12
    assert abs(float(aligned['equity'][-1]) - 1.05) < 1e-12


def test_daily_excess_stats_identical_series():
    curve = pl.DataFrame({
        'date': [dt.date(2024, 1, 2) + dt.timedelta(days=i) for i in range(6)],
        'equity': [1.0, 1.02, 1.01, 1.04, 1.03, 1.05],
    })
    stats = daily_excess_stats(curve, curve)
    assert abs(stats['mean_daily_excess']) < 1e-12
    assert abs(stats['beta'] - 1.0) < 1e-12
    assert abs(stats['excess_t']) < 1e-12


def test_same_start_excess_annualized():
    value = same_start_excess_annualized(
        {'net_return': 0.21}, {'net_return': 0.10}, 252)
    assert abs(value - 0.10) < 1e-12
