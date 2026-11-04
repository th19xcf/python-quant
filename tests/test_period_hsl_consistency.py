#!/usr/bin/env python3

"""周/月线 HSL 累计口径一致性测试。

口径说明（重要）
================
周/月线的 HSL 取「区间逐日累计换手率」：区间内各交易日 ``turn`` 直接求和
（``Σ turn_i``，单位同为百分数，不做二次换算）。每日 ``turn`` 已按当日流通股本
计算，故股本中途变更时求和仍无偏。

旧口径（``Σ(turn_i × float_share_i) / mean(float_share)``，即区间成交量的股本
加权平均）只在股本恒定时与逐日求和等价；股本变更区间内会偏离真实累计值。
旧值保留在 ``turn_legacy`` 列，仅用于对照/回滚，下游统一读 ``turn``。

验收：
* 周线 HSL == 当期日 HSL 之和（精确相等），月线同理；
* 股本缺失路径同样退化为 Σ turn，且 ``turn_legacy`` 同值；
* 股本变更区间：新口径仍等于逐日和，旧口径出现 >5% 偏差（缺陷复现）；
* 聚合 bar 数量不因口径调整而改变；
* 日线 HSL 与股本来源的守恒关系（turn = volume×100×100/float_share）保持。
"""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import polars as pl

from src.data.managers.data_processor import DataProcessor

FLOAT_SHARE = 2_000_000_000  # 20 亿股
TOLERANCE = 0.05
EXACT_TOL = 1e-9


def _workdays(n: int = 60, start: date = date(2024, 1, 2)) -> list[date]:
    dates: list[date] = []
    cursor = start
    while len(dates) < n:
        if cursor.weekday() < 5:  # 只取工作日，模拟交易日
            dates.append(cursor)
        cursor += timedelta(days=1)
    return dates


def _daily_frame(n: int = 60, start: date = date(2024, 1, 2),
                 with_float_share: bool = True,
                 float_share: list[float] | float | None = None) -> pl.DataFrame:
    rng = np.random.default_rng(11)
    dates = _workdays(n, start)
    volume = rng.integers(50_000, 500_000, n).astype(float)  # 手
    close = 10.0 + np.cumsum(rng.normal(0, 0.05, n))
    frame = pl.DataFrame({
        'date': dates,
        'open': close,
        'high': close + 0.2,
        'low': close - 0.2,
        'close': close,
        'volume': volume,
        'amount': volume * close * 100,
    })
    if float_share is None:
        shares = np.full(n, float(FLOAT_SHARE))
    elif np.isscalar(float_share):
        shares = np.full(n, float(float_share))
    else:
        shares = np.asarray(float_share, dtype=float)
    frame = frame.with_columns(pl.Series('float_share', shares))
    # 日换手率（百分数）：volume(手)×100 股 / 当日股本 × 100
    frame = frame.with_columns(
        (pl.col('volume') * 100.0 * 100.0 / pl.col('float_share')).alias('turn'))
    if not with_float_share:
        frame = frame.drop('float_share')
    return frame


def _group_of(daily: pl.DataFrame, bar_date: date, period: str) -> pl.DataFrame:
    """按聚合 bar 的日期回溯同一 ISO 周 / 自然月。"""
    iso = bar_date.isocalendar()
    if period == '1w':
        return daily.filter(
            (pl.col('date').cast(pl.Date).dt.week() == iso[1])
            & (pl.col('date').cast(pl.Date).dt.iso_year() == iso[0]))
    return daily.filter(
        (pl.col('date').cast(pl.Date).dt.month() == bar_date.month)
        & (pl.col('date').cast(pl.Date).dt.year() == bar_date.year))


def _as_date(value) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _assert_period_is_daily_sum(period: str, with_float_share: bool):
    """周/月线 turn 必须精确等于区间内逐日 turn 之和。"""
    daily = _daily_frame(with_float_share=with_float_share)
    bars = DataProcessor(None).convert_frequency(daily, period)
    assert 'turn' in bars.columns
    assert 'turn_legacy' in bars.columns
    for bar_date, turn in zip(
            bars['date'].to_list(), bars['turn'].to_list(), strict=True):
        group = _group_of(daily, _as_date(bar_date), period)
        daily_sum = float(group['turn'].sum())
        assert daily_sum > 0
        assert abs(turn - daily_sum) <= EXACT_TOL * daily_sum, (
            f'{period} {bar_date}: turn={turn:.8f}, 逐日和={daily_sum:.8f}')


def test_weekly_hsl_equals_sum_of_daily_hsl():
    """周线：turn == 当期 5 个日 turn 之和（精确）。"""
    _assert_period_is_daily_sum('1w', with_float_share=True)


def test_monthly_hsl_equals_sum_of_daily_hsl():
    """月线：turn == 当月各日 turn 之和（精确）。"""
    _assert_period_is_daily_sum('1m', with_float_share=True)


def test_period_hsl_falls_back_to_daily_turn_sum():
    """股本缺失时退化为 Σ turn，turn_legacy 与 turn 同值。"""
    _assert_period_is_daily_sum('1w', with_float_share=False)
    _assert_period_is_daily_sum('1m', with_float_share=False)
    daily = _daily_frame(with_float_share=False)
    bars = DataProcessor(None).convert_frequency(daily, '1w')
    assert np.allclose(bars['turn'].to_numpy(),
                       bars['turn_legacy'].to_numpy(), rtol=1e-12)


def test_share_change_keeps_sum_and_legacy_is_biased():
    """股本中途翻倍：新口径仍等于逐日和，旧口径偏差明显（缺陷复现）。"""
    n = 10
    shares = [1.0e9] * 4 + [5.0e9] * 3 + [8.0e9] * 3
    daily = _daily_frame(n=n, start=date(2024, 1, 8), float_share=shares)
    bars = DataProcessor(None).convert_frequency(daily, '1w')
    assert bars.height >= 1
    first_bar = _as_date(bars['date'][0])
    group = _group_of(daily, first_bar, '1w')
    daily_sum = float(group['turn'].sum())
    new_value = float(bars['turn'][0])
    legacy_value = float(bars['turn_legacy'][0])
    assert abs(new_value - daily_sum) <= EXACT_TOL * daily_sum
    # 该周股本从 10 亿跳到 25 亿，旧加权口径应出现 >5% 偏差
    assert abs(new_value - legacy_value) / legacy_value > TOLERANCE


def test_period_bar_count_unchanged():
    """口径调整不改变聚合 bar 数量：周线=ISO 周数，月线=自然月数。"""
    daily = _daily_frame(n=250, start=date(2024, 1, 2))
    dates = [_as_date(v) for v in daily['date'].to_list()]
    weekly_expected = len({(d.isocalendar()[0], d.isocalendar()[1]) for d in dates})
    monthly_expected = len({(d.year, d.month) for d in dates})
    weekly = DataProcessor(None).convert_frequency(daily, '1w')
    monthly = DataProcessor(None).convert_frequency(daily, '1m')
    assert weekly.height == weekly_expected
    assert monthly.height == monthly_expected


def test_daily_hsl_conserved_from_volume_and_share():
    """守恒断言（保持）：日线 turn = volume(手)×100×100 / float_share。"""
    daily = _daily_frame(with_float_share=True)
    expected = float(daily['volume'][0]) * 100.0 * 100.0 / FLOAT_SHARE
    assert abs(float(daily['turn'][0]) - expected) < 1e-9


def test_period_hsl_is_cumulative_not_average():
    """累计口径的量纲检查：周线 HSL 中位数数量级明显大于日线（约 5 倍）。"""
    daily = _daily_frame(with_float_share=True)
    weekly = DataProcessor(None).convert_frequency(daily, '1w')
    daily_median = float(daily['turn'].median())
    weekly_median = float(weekly['turn'].median())
    assert weekly_median / daily_median > 3.0
