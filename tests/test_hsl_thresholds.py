#!/usr/bin/env python3

"""HSL 分位阈值测试（120 日滚动 60%/85% 分位 + 板块固定值回退）。

覆盖：
* 分位阈值落点（与 numpy.quantile 对齐）
* 有效样本不足 60 时按板块回退固定值（主板 / 创业板·科创 / 北交所）
* 周/月线累计口径下，温启动期固定回退值按周期放大
* 滚动阈值无未来函数：第 i 根只使用截至当日的 trailing window
* UI 着色（标签文本）与纯函数口径一致
"""

from __future__ import annotations

import numpy as np

from src.ui.event_handlers.indicator_display import _render_hsl
from src.utils.hsl_threshold import (
    HSL_HIGH_QUANTILE,
    HSL_LOW_QUANTILE,
    PERIOD_SCALES,
    board_of,
    fixed_thresholds,
    percentile_thresholds,
    period_of,
    rolling_thresholds,
)


def test_board_classification():
    assert board_of('600000.SH') == 'main'
    assert board_of('000001.SZ') == 'main'
    assert board_of('300750.SZ') == 'growth'
    assert board_of('301029.SZ') == 'growth'
    assert board_of('688981.SH') == 'growth'
    assert board_of('920001.BJ') == 'bj'
    assert board_of('830799.BJ') == 'bj'
    assert board_of(None) == 'main'


def test_percentile_thresholds_match_numpy_quantile():
    values = np.linspace(0.2, 6.0, 150)
    low, high = percentile_thresholds(values, '600000.SH')
    # 只用最新 120 个样本
    window = values[-120:]
    want_low = float(np.quantile(window, HSL_LOW_QUANTILE))
    want_high = float(np.quantile(window, HSL_HIGH_QUANTILE))
    assert abs(low - want_low) < 1e-9
    assert abs(high - want_high) < 1e-9


def test_insufficient_samples_fall_back_by_board():
    # 30 个有效样本 < 60 → 固定阈值
    values = np.linspace(0.1, 0.9, 30)
    assert percentile_thresholds(values, '600000.SH') == (1.0, 3.0)
    assert percentile_thresholds(values, '300750.SZ') == (3.0, 8.0)
    assert percentile_thresholds(values, '688981.SH') == (3.0, 8.0)
    assert percentile_thresholds(values, '920001.BJ') == (2.0, 6.0)
    assert fixed_thresholds('600000.SH') == (1.0, 3.0)


def test_nan_values_do_not_count_as_samples():
    values = np.array([0.5] * 59 + [np.nan] * 2)
    # 59 个有效样本 < 60 → 回退
    assert percentile_thresholds(values, '600000.SH') == (1.0, 3.0)
    values = np.array([0.5] * 60 + [np.nan] * 5)
    low, high = percentile_thresholds(values, '600000.SH')
    # 常量窗口：阈值退化保护
    assert high > low


def test_period_alias_normalization():
    assert period_of(None) == 'day'
    assert period_of('日线') == 'day'
    assert period_of('1w') == 'week'
    assert period_of('周线') == 'week'
    assert period_of('1m') == 'month'
    assert period_of('月线') == 'month'
    assert period_of('unknown') == 'day'


def test_fixed_thresholds_scale_by_period():
    """周/月累计口径的温启动固定值 = 日线值 × 实测倍数（周 5.0、月 22.9）。"""
    week, month = PERIOD_SCALES['week'], PERIOD_SCALES['month']
    assert fixed_thresholds('600000.SH') == (1.0, 3.0)
    assert fixed_thresholds('600000.SH', '日线') == (1.0, 3.0)
    assert fixed_thresholds('600000.SH', '1w') == (1.0 * week, 3.0 * week)
    assert fixed_thresholds('600000.SH', '1m') == (1.0 * month, 3.0 * month)
    assert fixed_thresholds('300750.SZ', '月线') == (3.0 * month, 8.0 * month)
    assert fixed_thresholds('920001.BJ', '周线') == (2.0 * week, 6.0 * week)


def test_insufficient_samples_fall_back_scaled_by_period():
    """样本不足时周/月线回退值必须按周期放大，否则累计值会被误判为高换手。"""
    values = np.linspace(0.1, 0.9, 30)  # < 60 个有效样本
    assert percentile_thresholds(values, '600000.SH', period='1w') == (
        1.0 * PERIOD_SCALES['week'], 3.0 * PERIOD_SCALES['week'])
    assert percentile_thresholds(values, '600000.SH', period='1m') == (
        1.0 * PERIOD_SCALES['month'], 3.0 * PERIOD_SCALES['month'])
    low, high = rolling_thresholds(values, '300750.SZ', period='1w')
    assert np.allclose(low, 3.0 * PERIOD_SCALES['week'])
    assert np.allclose(high, 8.0 * PERIOD_SCALES['week'])


def test_quantile_path_is_period_independent():
    """分位阈值自适应各周期自身分布，不应随 period 缩放。"""
    rng = np.random.default_rng(3)
    daily = rng.uniform(0.1, 1.0, 200)
    weekly = daily * PERIOD_SCALES['week']
    low_d, high_d = percentile_thresholds(daily, '600000.SH', period='日线')
    low_w, high_w = percentile_thresholds(weekly, '600000.SH', period='周线')
    assert abs(low_w / low_d - PERIOD_SCALES['week']) < 1e-6
    assert abs(high_w / high_d - PERIOD_SCALES['week']) < 1e-6


def test_rolling_thresholds_use_trailing_window_only():
    rng = np.random.default_rng(7)
    values = rng.uniform(0.1, 8.0, 200)
    low, high = rolling_thresholds(values, '600000.SH')

    assert low.shape == values.shape == high.shape
    # 前 59 根样本不足 → 固定值
    assert np.allclose(low[:59], 1.0)
    assert np.allclose(high[:59], 3.0)

    for i in (59, 120, 199):
        window = values[max(0, i - 119):i + 1]
        want_low = float(np.quantile(window, HSL_LOW_QUANTILE))
        want_high = float(np.quantile(window, HSL_HIGH_QUANTILE))
        assert abs(low[i] - want_low) < 1e-9
        assert abs(high[i] - want_high) < 1e-9


def _fmt(value) -> str:
    return f'{value:.2f}'


def test_ui_hsl_label_colors_match_rolling_thresholds():
    # 60 个递增样本 + 最后一根显著放量：应为红色
    base = list(np.linspace(0.0, 1.0, 60))
    red = base + [0.9]
    assert "color='#FF0000'" in _render_hsl({'hsl': red}, len(red) - 1, _fmt, '600000.SH')

    # 最后一根处在 60%~85% 分位之间：橙色
    orange = base + [0.7]
    assert "color='#FFA500'" in _render_hsl({'hsl': orange}, len(orange) - 1, _fmt,
                                            '600000.SH')

    # 最后一根低于 60% 分位：蓝色
    blue = base + [0.3]
    assert "color='#00BFFF'" in _render_hsl({'hsl': blue}, len(blue) - 1, _fmt,
                                           '600000.SH')


def test_ui_hsl_label_falls_back_by_board():
    # 30 个样本 < 60：主板 1/3、创业板 3/8、北交所 2/6
    values = [0.5] * 29 + [2.0]
    assert "color='#FFA500'" in _render_hsl({'hsl': values}, 29, _fmt, '600000.SH')
    assert "color='#00BFFF'" in _render_hsl({'hsl': values}, 29, _fmt, '300750.SZ')
    values = [0.5] * 29 + [3.0]
    assert "color='#FFA500'" in _render_hsl({'hsl': values}, 29, _fmt, '920001.BJ')


def test_ui_hsl_label_falls_back_scaled_by_period():
    """周线累计值 3.0% 在主板日线属中区，但周线回退阈值已放大到 5/15 → 蓝色。"""
    values = [0.5] * 29 + [3.0]
    assert "color='#FFA500'" in _render_hsl(
        {'hsl': values}, 29, _fmt, '600000.SH', '日线')
    assert "color='#00BFFF'" in _render_hsl(
        {'hsl': values}, 29, _fmt, '600000.SH', '周线')
