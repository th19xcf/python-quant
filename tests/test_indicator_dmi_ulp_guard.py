#!/usr/bin/env python3
"""DMI 逐级物化改造的位级守卫。

20 指标合并口径下，DMI 多出的投影节点会让 ``volume`` 列的运行时 chunk 数
从 88 变成 176，导致下游 float32 累加的 ``obv/psy/psy12`` 累加顺序改变。
固定样本实测：psy/psy12 各 3~8 行差 ≤1 ULP；obv 因逐行累加、漂移沿序列累积，
70~96 行差 ≤2 ULP。这是已接受的收口口径：
  * 除这三列外，所有列必须与改动前的 DMI 实现**位级全等**（``Series.equals``）；
  * 这三列按 ``ULP_BUDGET`` 允许 ULP 级差异（按该值自身的 float32 ULP 计），
    超出即失败；不使用 rtol/普通浮点近似。

固定样本：301008.SZ / 688067.SH，2016-11-21 ~ 2026-11-20；数据缺失时跳过。
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from loguru import logger  # noqa: E402

logger.remove()

from src.tech_analysis.indicator_calculator import preprocess_data_polars  # noqa: E402
from src.tech_analysis.indicator_manager import IndicatorManager  # noqa: E402
from src.tech_analysis.indicators import trend as trd  # noqa: E402
from src.tech_analysis.utils import to_float32  # noqa: E402

QUOTE_ROOT = ROOT / 'data' / 'local_store' / 'quotes' / 'stock'
START, END = '2016-11-21', '2026-11-20'
FIXED_SAMPLE = ('301008.SZ', '688067.SH')
ALL20 = ['ma', 'vol_ma', 'macd', 'rsi', 'kdj', 'wr', 'boll', 'dmi', 'cci', 'roc',
         'mtm', 'bias', 'obv', 'vr', 'mfi', 'cmf', 'ema', 'skdj', 'psy', 'trix']
# 已接受口径：只这三列允许 ULP 级差异，其余列必须位级全等。
# 预算按本固定样本实测最大偏差设定（ULP 计数，非 rtol/atol 近似）：
#   psy / psy12 = 1 ULP；obv 是逐行累加，单行 1 ULP 漂移会沿序列累积，实测最大 2 ULP。
ULP_BUDGET = {'obv': 2, 'psy': 1, 'psy12': 1}


def _legacy_dmi(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """改造前的 DMI 实现（表达式内联互相引用），仅作为位级基准。"""
    prev_high = pl.col('high').shift(1)
    prev_low = pl.col('low').shift(1)
    prev_close = pl.col('close').shift(1)
    tr = pl.max_horizontal(pl.col('high'), prev_close) - pl.min_horizontal(pl.col('low'), prev_close)
    high_diff = pl.col('high') - prev_high
    low_diff = prev_low - pl.col('low')
    plus_dm = pl.when((high_diff > low_diff) & (high_diff > 0)).then(high_diff).otherwise(0.0)
    minus_dm = pl.when((low_diff > high_diff) & (low_diff > 0)).then(low_diff).otherwise(0.0)
    for window in windows:
        tr_sma = tr.rolling_sum(window_size=window, min_samples=1)
        pdm_sma = plus_dm.rolling_sum(window_size=window, min_samples=1)
        ndm_sma = minus_dm.rolling_sum(window_size=window, min_samples=1)
        pdi = to_float32(pl.when(tr_sma == 0).then(0.0)
                         .otherwise(pdm_sma / tr_sma * 100)).alias(f'pdi_{window}')
        ndi = to_float32(pl.when(tr_sma == 0).then(0.0)
                         .otherwise(ndm_sma / tr_sma * 100)).alias(f'ndi_{window}')
        dx = to_float32(pl.when((pdi + ndi) == 0).then(0.0)
                        .otherwise((pdi - ndi).abs() / (pdi + ndi) * 100)).alias(f'dx_{window}')
        adx = to_float32(dx.rolling_mean(window_size=window, min_samples=window)).alias(f'adx_{window}')
        adxr = to_float32((adx + adx.shift(window)) / 2).alias(f'adxr_{window}')
        lazy_df = lazy_df.with_columns([pdi, ndi, adx, adxr])
    window = windows[0]
    return lazy_df.with_columns(
        pl.col(f'pdi_{window}').alias('pdi'), pl.col(f'ndi_{window}').alias('ndi'),
        pl.col(f'adx_{window}').alias('adx'), pl.col(f'adxr_{window}').alias('adxr'))


def _assert_within_ulp(old: pl.Series, new: pl.Series, ctx: str, budget: int) -> None:
    """同一 dtype、同一 null/NaN 分布，且逐点 |diff| ≤ budget × 该点 float32 的 1 ULP。"""
    assert old.dtype == new.dtype, f'{ctx}: dtype {old.dtype} -> {new.dtype}'
    assert old.is_null().equals(new.is_null()), f'{ctx}: null 分布不同'
    a = old.to_numpy(allow_copy=True).astype(np.float32, copy=False)
    b = new.to_numpy(allow_copy=True).astype(np.float32, copy=False)
    nan_a, nan_b = np.isnan(a), np.isnan(b)
    assert np.array_equal(nan_a, nan_b), f'{ctx}: NaN 分布不同'
    valid = ~nan_a
    diff = np.abs(a[valid].astype(np.float64) - b[valid].astype(np.float64))
    tol = np.spacing(np.abs(a[valid])).astype(np.float64) * budget
    bad = np.flatnonzero(diff > tol)
    assert bad.size == 0, (f'{ctx}: {bad.size} 处超出 {budget} ULP，'
                           f'首处 index={int(bad[0])} old={a[valid][bad[0]]!r} '
                           f'new={b[valid][bad[0]]!r}')


def _load(code: str) -> pl.DataFrame:
    path = next(QUOTE_ROOT.glob(f'*/{code}.parquet'), None)
    if path is None:
        pytest.skip(f'缺少样本数据 {code}（{QUOTE_ROOT}）')
    return preprocess_data_polars(pl.read_parquet(path).filter(
        (pl.col('date') >= pl.lit(START).str.to_date())
        & (pl.col('date') <= pl.lit(END).str.to_date())))


@pytest.mark.parametrize('code', FIXED_SAMPLE)
def test_dmi_materialized_columns_bit_identical_except_1ulp_downstream(code: str) -> None:
    mgr = IndicatorManager(cache_enabled=False)
    df = _load(code)
    baseline_fn = trd.calculate_dmi
    try:
        trd.calculate_dmi = _legacy_dmi
        baseline = mgr.calculate_indicators(df, ALL20, return_polars=True)
    finally:
        trd.calculate_dmi = baseline_fn
    current = mgr.calculate_indicators(df, ALL20, return_polars=True)

    assert baseline.columns == current.columns, '列集合/顺序发生变化'
    for col in baseline.columns:
        old, new = baseline[col], current[col]
        if col in ULP_BUDGET:
            _assert_within_ulp(old, new, f'{code}/{col}', ULP_BUDGET[col])
        else:
            assert old.dtype == new.dtype, f'{code}/{col}: dtype 变化'
            assert old.equals(new), f'{code}/{col}: 非 ULP 列出现位级差异'
