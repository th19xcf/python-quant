#!/usr/bin/env python3

"""
横截面（多标的）因子计算回归测试

验证 calculate_factors_cross_sectional：
  1. 与逐标的循环调用 AlphaCalculator 的结果 bit 级一致（正确性）
  2. 输出行序与输入完全一致（分区不重排行）
  3. 按需子集只计算请求的因子
  4. 缺标的代码列时抛 ValueError（边界）

第 1 条是本次全向量化改造的核心防线：只要任一因子漏包 .over()，
跨标的窗口就会串值，逐格比较必然失败。
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

# (代码, 行数, 随机种子)：刻意使用不等长历史，覆盖分组长短不一的情形
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
    """逐标的循环调用 AlphaCalculator 的参照结果。"""
    parts = []
    for code in df['ts_code'].unique(maintain_order=True):
        sub = df.filter(pl.col('ts_code') == code)
        parts.append(AlphaCalculator(sub).calculate_factors(ALL_FACTORS))
    merged = pl.concat(parts, how='vertical_relaxed')
    # 按 (ts_code, date) 对齐，避免依赖两侧行序
    return df[['ts_code', 'date']].join(
        merged, on=['ts_code', 'date'], how='left')


class TestCrossSectionalCorrectness:
    """横截面结果与逐标的参照逐格一致"""

    def test_matches_per_stock_reference(self, panel):
        result = calculate_factors_cross_sectional(panel, ALL_FACTORS)
        reference = _per_stock_reference(panel)
        assert_frame_equal(
            result.sort(['ts_code', 'date']),
            reference.sort(['ts_code', 'date']))

    def test_subset_only_computes_requested(self, panel):
        result = calculate_factors_cross_sectional(panel, ['price_momentum'])
        assert 'momentum_1m' in result.columns
        assert 'atr_14' not in result.columns
        assert 'obv' not in result.columns

    def test_partition_matches_single_stock_call(self):
        """同一标的数据单独计算与放进横截面计算，结果必须一致。"""
        single = _make_stock('000001.SZ', 200, 9)
        other = _make_stock('600519.SH', 150, 10)
        panel = pl.concat([single, other])

        merged = calculate_factors_cross_sectional(panel, ALL_FACTORS)
        standalone = AlphaCalculator(single).calculate_factors(ALL_FACTORS)

        got = merged.filter(pl.col('ts_code') == '000001.SZ')
        assert_frame_equal(got, standalone)


class TestCrossSectionalRowOrder:
    """输出行序与输入完全一致（允许乱序输入）"""

    def test_row_order_preserved(self, panel):
        result = calculate_factors_cross_sectional(panel, ['price_momentum'])
        assert_frame_equal(result.select(['ts_code', 'date']),
                           panel.select(['ts_code', 'date']))

    def test_all_columns_preserved(self, panel):
        result = calculate_factors_cross_sectional(panel, ['risk'])
        assert result.columns[:len(panel.columns)] == panel.columns


class TestCrossSectionalBoundary:
    def test_missing_code_column_raises(self, panel):
        with pytest.raises(ValueError, match='ts_code'):
            calculate_factors_cross_sectional(
                panel.drop('ts_code'), ['price_momentum'])

    def test_empty_factor_types_returns_input(self, panel):
        result = calculate_factors_cross_sectional(panel, [])
        assert_frame_equal(result, panel)

    def test_custom_code_column(self):
        df = _make_stock('A', 60, 7).rename({'ts_code': 'stock_code'})
        result = calculate_factors_cross_sectional(
            df, ['price_momentum'], code_col='stock_code')
        assert 'momentum_1m' in result.columns
