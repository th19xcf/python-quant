"""beta 匹配对照脚本的单元测试（纯函数，不跑数据）。"""

from __future__ import annotations

import datetime as dt

import numpy as np
import polars as pl

from tools.beta_matched_baseline import (
    index_window_curve,
    matched_beta,
    weight_from_beta,
    weighted_curve,
    window_net,
)


def _curve(equities: list[float]) -> pl.DataFrame:
    start = dt.date(2024, 1, 2)
    return pl.DataFrame({
        'date': [start + dt.timedelta(days=i) for i in range(len(equities))],
        'equity': equities,
    })


def test_weighted_curve_scales_daily_returns():
    curve = _curve([1.0, 1.10, 1.21])
    combo = weighted_curve(curve, 0.55)
    assert abs(float(combo['equity'][0]) - 1.0) < 1e-12
    assert abs(float(combo['equity'][1]) - 1.055) < 1e-12
    assert abs(float(combo['equity'][2]) - 1.055 * 1.055) < 1e-12


def test_weighted_curve_keeps_beta():
    curve = _curve([1.0, 1.10, 0.99, 1.089])
    combo = weighted_curve(curve, 0.55)
    rs, rb = _daily(combo, curve)
    beta = float(np.cov(rs, rb, ddof=1)[0, 1] / float(rb.var(ddof=1)))
    assert abs(beta - 0.55) < 1e-10


def _daily(left: pl.DataFrame, right: pl.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    a = left.select('date', (pl.col('equity') / pl.col('equity').shift(1) - 1.0)
                    .alias('rs'))
    b = right.select('date', (pl.col('equity') / pl.col('equity').shift(1) - 1.0)
                     .alias('rb'))
    joined = a.join(b, on='date', how='inner').drop_nulls(['rs', 'rb'])
    return joined['rs'].to_numpy(), joined['rb'].to_numpy()


def test_matched_beta_recovers_scale():
    start = dt.date(2024, 1, 2)
    rets = [0.01, -0.02, 0.015, -0.005, 0.02, -0.01]
    values = [1.0]
    for value in rets:
        values.append(values[-1] * (1.0 + value))
    index = pl.DataFrame({
        'date': [start + dt.timedelta(days=i + 1) for i in range(len(rets))],
        'equity': values[1:],
    })
    strategy = weighted_curve(_curve(values), 0.55)
    beta = matched_beta([strategy], [index])
    assert abs(beta - 0.55) < 1e-10


def test_beta_weight_is_clamped_to_reasonable_range():
    assert weight_from_beta(0.55) == 0.55
    assert weight_from_beta(1.4) == 1.0
    assert weight_from_beta(-0.2) == 0.0


def test_window_net_is_first_point_normalized():
    curve = _curve([2.0, 2.2, 2.0])
    assert abs(window_net(curve) - 0.0) < 1e-12


def test_index_window_curve_drops_d0_and_starts_at_one():
    start = dt.date(2024, 1, 2)
    prices = pl.DataFrame({
        'date': [start + dt.timedelta(days=i) for i in range(5)],
        'close': [100.0, 110.0, 121.0, 133.1, 146.41],
    })
    window = {'test_start': start + dt.timedelta(days=1),
              'test_end': start + dt.timedelta(days=5)}
    curve = index_window_curve(prices, window, '000300.SH')
    # d0 在窗外，d1 归一为 1.0，其后只累乘 d2 起的收益
    assert curve.height == 3
    assert abs(float(curve['equity'][0]) - 1.0) < 1e-12
    assert abs(float(curve['equity'][-1]) - 1.21) < 1e-12
