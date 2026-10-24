#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
指标公式一致性测试

验证「共享公式层」（src/tech_analysis/indicators/formulas.py）收口后，
回测引擎、GPU 加速路径、Alpha 因子库与技术指标权威实现数值一致。
全部使用构造数据，不依赖数据库与网络。
"""

import numpy as np
import polars as pl

from src.backtest.engine.backtest_engine import BacktestEngine
from src.tech_analysis.gpu_acceleration import GPUAccelerator
from src.tech_analysis.indicator_calculator import calculate_multiple_indicators_polars
from src.tech_analysis.indicators.formulas import macd_exprs, rsi_expr
from src.alpha.factors.price_momentum import PriceMomentumFactors


def _make_ohlcv(n: int = 300) -> pl.DataFrame:
    """构造 300 根稳定的 OHLCV 数据（np.random.seed(0)）"""
    np.random.seed(0)
    close = 100.0 + np.cumsum(np.random.randn(n))
    high = close + np.abs(np.random.randn(n))
    low = close - np.abs(np.random.randn(n))
    open_ = np.clip(close + np.random.randn(n) * 0.5, low, high)
    volume = np.random.randint(100_000, 1_000_000, n).astype(float)
    return pl.DataFrame({
        'open': open_,
        'high': high,
        'low': low,
        'close': close,
        'volume': volume,
    })


class _FakeStrategy:
    """只提供 params 字典的最小策略替身，供 _precompute_indicators 使用"""

    def __init__(self, params: dict):
        self.params = params


def _assert_close(a, b, rtol, atol, name: str) -> None:
    np.testing.assert_allclose(
        np.asarray(a, dtype=float),
        np.asarray(b, dtype=float),
        rtol=rtol,
        atol=atol,
        equal_nan=True,
        err_msg=f'{name} 数值不一致',
    )


def test_backtest_engine_matches_authoritative_impl():
    """回测引擎预计算的 MACD / RSI 必须与权威实现一致（权威为 float32）"""
    df = _make_ohlcv()
    engine = BacktestEngine.__new__(BacktestEngine)
    strategy = _FakeStrategy({
        'short_window': 10,
        'long_window': 20,
        'rsi_period': 14,
    })
    precomputed = engine._precompute_indicators(df, strategy)

    authoritative = calculate_multiple_indicators_polars(df, ['macd', 'rsi'])

    for col in ('macd', 'macd_signal', 'macd_hist', 'rsi'):
        _assert_close(
            precomputed[col].to_numpy(),
            authoritative[col].to_numpy(),
            rtol=1e-4,
            atol=1e-6,
            name=col,
        )


def test_gpu_path_matches_shared_formulas():
    """GPU 加速路径的 MACD / RSI 必须与共享公式完全一致"""
    close = _make_ohlcv()['close'].to_numpy()
    accelerator = GPUAccelerator()

    macd_result = accelerator.calculate_macd_gpu(close, 12, 26, 9)
    exprs = macd_exprs('close', 12, 26, 9)
    expected = pl.DataFrame({'close': close}).with_columns([
        exprs['macd'].alias('macd'),
        exprs['signal'].alias('macd_signal'),
        exprs['hist'].alias('macd_hist'),
    ])
    for key in ('macd', 'macd_signal', 'macd_hist'):
        _assert_close(
            macd_result[key],
            expected[key].to_numpy(),
            rtol=1e-6,
            atol=1e-9,
            name=f'gpu_{key}',
        )

    rsi_result = accelerator.calculate_rsi_gpu(close, 14)
    expected_rsi = (
        pl.DataFrame({'close': close})
        .with_columns(rsi_expr('close', 14).alias('rsi'))['rsi']
        .to_numpy()
    )
    _assert_close(rsi_result, expected_rsi, rtol=1e-6, atol=1e-9, name='gpu_rsi')


def test_rsi_seed_and_value_range():
    """RSI 前 13 行为 null，种子在索引 13；值域 0~100"""
    df = _make_ohlcv()
    rsi = df.select(rsi_expr('close', 14).alias('rsi'))['rsi']

    assert rsi[:13].is_null().all(), 'RSI 前 13 行应为 null'
    assert rsi[13:].is_not_null().all(), 'RSI 从索引 13 起应非 null'

    values = rsi.drop_nulls().to_numpy()
    assert values.min() >= 0.0 and values.max() <= 100.0, 'RSI 值域应为 0~100'


def test_alpha_macd_uses_shared_formula():
    """Alpha 因子层的 MACD 列直接复用共享公式（保持原有不做 shift 的语义）"""
    df = _make_ohlcv()
    factors = PriceMomentumFactors(df)
    factors._calculate_macd_momentum()
    result = factors.data

    exprs = macd_exprs('close', 12, 26, 9)
    expected = df.with_columns([
        exprs['macd'].fill_null(0.0).alias('macd'),
        exprs['signal'].fill_null(0.0).alias('macd_signal'),
        exprs['hist'].fill_null(0.0).alias('macd_hist'),
    ])

    for col in ('macd', 'macd_signal', 'macd_hist'):
        _assert_close(
            result[col].to_numpy(),
            expected[col].to_numpy(),
            rtol=1e-4,
            atol=1e-6,
            name=f'alpha_{col}',
        )


def test_kdj_matches_tdx_recursive_sma():
    """图表 KDJ 的 K/D 应为通达信 SMA(X,3,1) 递推（α=1/3），而非 3 周期简单均值"""
    df = _make_ohlcv()
    result = calculate_multiple_indicators_polars(df, ['kdj'], kdj_windows=[14])

    close = df['close'].to_numpy()
    high = df['high'].to_numpy()
    low = df['low'].to_numpy()

    # 参考实现：RSV 含当日（与图表 high_n_14 / low_n_14 一致）
    rsv = []
    for i in range(len(close)):
        hh = high[max(0, i - 13):i + 1].max()
        ll = low[max(0, i - 13):i + 1].min()
        rsv.append(0.0 if hh == ll else (close[i] - ll) / (hh - ll) * 100.0)

    # K(t) = (RSV(t) + 2*K(t-1)) / 3，首值取 RSV 本身；D 为 K 的同参数递推
    k_ref, d_ref = [], []
    for i, value in enumerate(rsv):
        k_value = value if i == 0 else (value + 2 * k_ref[-1]) / 3.0
        d_value = k_value if i == 0 else (k_value + 2 * d_ref[-1]) / 3.0
        k_ref.append(k_value)
        d_ref.append(d_value)
    j_ref = [3 * k - 2 * d for k, d in zip(k_ref, d_ref)]

    for col, ref in (('k', k_ref), ('d', d_ref), ('j', j_ref)):
        _assert_close(
            result[col].to_numpy(),
            np.array(ref),
            rtol=1e-4,
            atol=1e-3,
            name=f'kdj_{col}',
        )


def test_alpha_kdj_matches_chart():
    """Alpha 因子层的 KDJ 与图表同源（RSV 含当日 + SMA(3,1) 递推）"""
    from src.alpha.factors.price_momentum import PriceMomentumFactors
    from src.alpha.factors.technical import TechnicalFactors

    df = _make_ohlcv()
    chart = calculate_multiple_indicators_polars(df, ['kdj'], kdj_windows=[14])

    price_momentum = PriceMomentumFactors(df)
    price_momentum._calculate_stochastic_momentum()

    technical = TechnicalFactors(df)
    technical._calculate_stochastic_oscillator()

    for factor_col, chart_col in (('kdj_k', 'k'), ('kdj_d', 'd'), ('kdj_j', 'j')):
        _assert_close(
            price_momentum.data[factor_col].to_numpy(),
            chart[chart_col].to_numpy(),
            rtol=1e-4,
            atol=1e-3,
            name=f'price_momentum_{factor_col}',
        )
        _assert_close(
            technical.data[factor_col].to_numpy(),
            chart[chart_col].to_numpy(),
            rtol=1e-4,
            atol=1e-3,
            name=f'technical_{factor_col}',
        )
