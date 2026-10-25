#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
横截面（多标的）因子计算回归测试

验证 calculate_factors_cross_sectional：
  1. 与逐标的循环调用 AlphaCalculator 的结果逐格一致（正确性）
  2. 输入乱序/非连续时输出行序与输入完全一致（行序还原）
  3. 进程池并行结果与串行完全一致（并行确定性）
  4. 缺少标的代码列时抛出 ValueError（边界）
"""

import numpy as np
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from src.alpha.calculator.alpha_calculator import (
    AlphaCalculator,
    calculate_factors_cross_sectional,
)

ALL_FACTORS = ['price_momentum', 'volatility', 'volume', 'technical', 'risk']

# (代码, 行数, 随机种子)：刻意使用不等长历史
STOCKS = [('000001.SZ', 300, 1), ('600000.SH', 260, 2), ('430047.BJ', 120, 3)]


def _make_stock(code: str, n: int, seed: int) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    close = np.maximum(10 + np.cumsum(rng.normal(0, 0.05, n)), 1.0)
    high = close * (1 + rng.uniform(0, 0.02, n))
    low = close * (1 - rng.uniform(0, 0.02, n))
    dates = pl.date_range(
        pl.date(2022, 1, 3), pl.date(2022, 1, 3) + pl.duration(days=n - 1),
        interval='1d', eager=True)
    return pl.DataFrame({
        'ts_code': [code] * n,
        'date': dates,
        'open': low + (high - low) * rng.uniform(0, 1, n),
        'high': high,
        'low': low,
        'close': close,
        'volume': rng.uniform(1e6, 5e6, n),
    })


@pytest.fixture(scope='module')
def panel() -> pl.DataFrame:
    """三只标的不等长历史拼成长表，并确定性打乱行序（非连续、非排序）。"""
    df = pl.concat([_make_stock(c, n, s) for c, n, s in STOCKS])
    return df.sample(fraction=1.0, seed=42, shuffle=True)


def _per_stock_reference(df: pl.DataFrame) -> pl.DataFrame:
    """逐标的循环调用 AlphaCalculator 的参照结果（保持输入行序）。"""
    parts = []
    for code in df['ts_code'].unique(maintain_order=True):
        sub = df.filter(pl.col('ts_code') == code)
        parts.append(AlphaCalculator(sub).calculate_factors(ALL_FACTORS))
    merged = pl.concat(parts, how='vertical_relaxed')
    # 参照结果按 (ts_code, 原始行序) 重排以对齐输入
    return df[['ts_code', 'date']].join(
        merged, on=['ts_code', 'date'], how='left')


class TestCrossSectionalCorrectness:
    """横截面结果与逐标的参照逐格一致"""

    def test_serial_matches_per_stock_reference(self, panel):
        result = calculate_factors_cross_sectional(
            panel, ALL_FACTORS, n_workers=1)
        reference = _per_stock_reference(panel)
        assert_frame_equal(
            result.sort(['ts_code', 'date']),
            reference.sort(['ts_code', 'date']))

    def test_parallel_matches_serial(self, panel):
        serial = calculate_factors_cross_sectional(
            panel, ALL_FACTORS, n_workers=1)
        parallel = calculate_factors_cross_sectional(
            panel, ALL_FACTORS, n_workers=2)
        assert_frame_equal(serial, parallel)

    def test_subset_only_computes_requested(self, panel):
        result = calculate_factors_cross_sectional(
            panel, ['price_momentum'], n_workers=1)
        assert 'momentum_1m' in result.columns
        assert 'atr_14' not in result.columns
        assert 'obv' not in result.columns


class TestCrossSectionalRowOrder:
    """输出行序与输入完全一致（允许乱序输入）"""

    def test_row_order_preserved(self, panel):
        result = calculate_factors_cross_sectional(
            panel, ['price_momentum'], n_workers=1)
        assert_frame_equal(result.select(['ts_code', 'date']),
                           panel.select(['ts_code', 'date']))

    def test_parallel_row_order_preserved(self, panel):
        result = calculate_factors_cross_sectional(
            panel, ['price_momentum'], n_workers=2)
        assert_frame_equal(result.select(['ts_code', 'date']),
                           panel.select(['ts_code', 'date']))


class TestCrossSectionalBoundary:
    def test_missing_code_column_raises(self, panel):
        with pytest.raises(ValueError, match='ts_code'):
            calculate_factors_cross_sectional(
                panel.drop('ts_code'), ['price_momentum'], n_workers=1)

    def test_empty_factor_types_returns_input(self, panel):
        result = calculate_factors_cross_sectional(panel, [], n_workers=1)
        assert_frame_equal(result, panel)

    def test_custom_code_column(self):
        df = _make_stock('A', 50, 7).rename({'ts_code': 'stock_code'})
        result = calculate_factors_cross_sectional(
            df, ['price_momentum'], code_col='stock_code', n_workers=1)
        assert 'momentum_1m' in result.columns
