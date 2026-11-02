#!/usr/bin/env python3

"""指标入口一致性回归：参数生效 + calculator/manager/analyzer 三入口对拍。

覆盖三类此前被静默丢弃/口径分裂的问题：
1. 窗口参数按指标真实名称生效（dmi_windows / ma_windows / vr 默认 26）；
2. 默认指标集合以注册表为唯一真源（vol_tdx 已注销）；
3. 指标管理器按声明列名抽取缓存列（dmi 可缓存、ma 不吞 macd）。
"""

import numpy as np
import polars as pl
import pytest

from src.tech_analysis import indicator_calculator
from src.tech_analysis.analyzer import parallel_ops
from src.tech_analysis.indicator_cache import global_indicator_cache
from src.tech_analysis.indicator_calculator import (
    calculate_multiple_indicators_polars,
    get_default_indicator_types,
    preprocess_data_polars,
)
from src.tech_analysis.indicator_manager import (
    IndicatorManager,
    ensure_indicators_registered,
    global_indicator_manager,
)
from src.tech_analysis.indicator_registry import global_indicator_registry
from src.tech_analysis.technical_analyzer import TechnicalAnalyzer

_DMI_WINDOW_COLUMNS = {'pdi_5', 'ndi_5', 'adx_5', 'adxr_5'}


def _ohlcv(n: int = 160) -> pl.DataFrame:
    """构造稳定的 OHLCV 数据（date 为 pl.Date）。"""
    rng = np.random.default_rng(7)
    close = 100.0 + np.cumsum(rng.standard_normal(n))
    dates = pl.date_range(
        pl.date(2024, 1, 1), pl.date(2024, 12, 31), interval='1d', eager=True
    ).head(n)
    return pl.DataFrame({
        'date': dates,
        'open': close + rng.standard_normal(n) * 0.2,
        'high': close + np.abs(rng.standard_normal(n)),
        'low': close - np.abs(rng.standard_normal(n)),
        'close': close,
        'volume': rng.integers(100_000, 1_000_000, n).astype(float),
    })


@pytest.fixture(autouse=True)
def _clean_indicator_cache():
    global_indicator_cache.clear()
    yield
    global_indicator_cache.clear()


@pytest.fixture
def frame():
    return _ohlcv()


def _new_indicator_columns(df: pl.DataFrame, base_columns) -> set:
    """返回单指标计算结果中新增的指标列。"""
    return set(df.columns) - set(base_columns)


# ----------------------------------------------------------------------
# 默认集合 / 注册表委托
# ----------------------------------------------------------------------


def test_registry_is_single_source_of_truth(frame):
    """默认指标集合与支持清单都来自注册表，vol_tdx 已注销。"""
    ensure_indicators_registered()
    registry = global_indicator_registry.get_supported_indicators()

    assert len(registry) == len(set(registry))
    assert 'vol_tdx' not in registry
    assert get_default_indicator_types() == registry

    analyzer = TechnicalAnalyzer(frame)
    assert set(analyzer.get_supported_indicators()) == set(registry)
    for name in ('boll', 'dma', 'bbi', 'cr', 'expma'):
        assert analyzer.is_indicator_supported(name) is True
    assert analyzer.is_indicator_supported('vol_tdx') is False
    assert not hasattr(indicator_calculator, 'calculate_vol_tdx_polars')


# ----------------------------------------------------------------------
# 参数生效：dmi 自定义窗口 / ma_windows / vr 默认值
# ----------------------------------------------------------------------


def test_dmi_custom_window_is_effective_in_all_entries(frame):
    """dmi 自定义窗口在批量、管理器、TechnicalAnalyzer 三个入口都生效。"""
    calc = calculate_multiple_indicators_polars(
        frame.lazy(), ['dmi'], dmi_windows=[5]
    ).collect()
    assert _DMI_WINDOW_COLUMNS <= set(calc.columns)
    assert 'pdi_14' not in calc.columns

    single = global_indicator_manager.calculate_indicator(
        frame, 'dmi', windows=[5], return_polars=True
    )
    assert _DMI_WINDOW_COLUMNS <= set(single.columns)
    assert 'pdi_14' not in single.columns

    batch = global_indicator_manager.calculate_indicators(
        frame, ['dmi'], dmi_windows=[5], return_polars=True
    )
    assert _DMI_WINDOW_COLUMNS <= set(batch.columns)
    assert 'pdi_14' not in batch.columns

    analyzer = TechnicalAnalyzer(frame)
    parallel = analyzer.calculate_indicator_parallel('dmi', windows=[5])
    assert _DMI_WINDOW_COLUMNS <= set(parallel.columns)
    assert 'pdi_14' not in parallel.columns
    # 状态登记的是用户窗口，而不是被静默套用的默认 [14]
    assert analyzer.calculated_indicators['dmi'] == {5}

    # 真实参数名与通用 windows 等价
    aliased = TechnicalAnalyzer(frame).calculate_indicator_parallel(
        'dmi', dmi_windows=[3]
    )
    assert {'pdi_3', 'ndi_3', 'adx_3', 'adxr_3'} <= set(aliased.columns)


def test_ma_windows_alias_is_effective(frame):
    """ma 同时接受 ma_windows 与通用 windows，且不残留默认窗口列。"""
    batch = calculate_multiple_indicators_polars(
        frame.lazy(), ['ma'], ma_windows=[7]
    ).collect()
    assert {'ma', 'ma7'} <= set(batch.columns)
    assert 'ma5' not in batch.columns

    # 通用 windows 仍作为 ma 的兼容参数
    generic = calculate_multiple_indicators_polars(
        frame.lazy(), ['ma'], windows=[7]
    ).collect()
    assert {'ma', 'ma7'} <= set(generic.columns)
    assert 'ma5' not in generic.columns

    single = global_indicator_manager.calculate_indicator(
        frame, 'ma', windows=[7], return_polars=True
    )
    assert {'ma', 'ma7'} <= set(single.columns)
    assert 'ma5' not in single.columns

    batch_alias = global_indicator_manager.calculate_indicators(
        frame, ['ma'], ma_windows=[7], return_polars=True
    )
    assert {'ma', 'ma7'} <= set(batch_alias.columns)
    assert 'ma5' not in batch_alias.columns

    analyzer = TechnicalAnalyzer(frame)
    parallel = analyzer.calculate_indicator_parallel('ma', windows=[7])
    assert {'ma', 'ma7'} <= set(parallel.columns)
    assert analyzer.calculated_indicators['ma'] == {7}


def test_vr_default_window_is_26(frame):
    """vr 的登记默认值与实际计算默认窗口都改为 26。"""
    assert parallel_ops._INDICATOR_UPDATE_MAP['vr']['default'] == [26]

    calc = calculate_multiple_indicators_polars(frame.lazy(), ['vr']).collect()
    assert {'vr', 'vr26'} <= set(calc.columns)
    assert 'vr24' not in calc.columns

    manager = global_indicator_manager.calculate_indicators(
        frame, ['vr'], return_polars=True
    )
    assert {'vr', 'vr26'} <= set(manager.columns)
    assert 'vr24' not in manager.columns

    analyzer = TechnicalAnalyzer(frame)
    parallel = analyzer.calculate_indicator_parallel('vr')
    assert {'vr', 'vr26'} <= set(parallel.columns)
    assert analyzer.calculated_indicators['vr'] == {26}


# ----------------------------------------------------------------------
# 三入口数值对拍
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    'indicator',
    ['ma', 'macd', 'rsi', 'kdj', 'boll', 'dmi', 'wr', 'cci'],
)
def test_three_entry_numeric_parity(indicator, frame):
    """同一份公式下，calculator / manager / TechnicalAnalyzer 三入口数值一致。"""
    base_columns = frame.columns

    calc = calculate_multiple_indicators_polars(frame.lazy(), [indicator]).collect()
    calc_cols = _new_indicator_columns(calc, base_columns)
    assert calc_cols

    manager = IndicatorManager(cache_enabled=False).calculate_indicators(
        frame, [indicator], return_polars=True
    )
    manager_cols = _new_indicator_columns(manager, base_columns)
    assert manager_cols == calc_cols

    analyzer = TechnicalAnalyzer(frame)
    analyzer.calculate_indicator_parallel(indicator)
    analyzer_cols = _new_indicator_columns(analyzer.pl_df, base_columns)
    assert analyzer_cols == calc_cols

    for col in sorted(calc_cols):
        np.testing.assert_allclose(
            calc[col].to_numpy(),
            manager[col].to_numpy(),
            rtol=1e-4,
            atol=1e-4,
            equal_nan=True,
            err_msg=f'{indicator}.{col} calculator vs manager 不一致',
        )
        np.testing.assert_allclose(
            calc[col].to_numpy(),
            analyzer.pl_df[col].to_numpy(),
            rtol=1e-4,
            atol=1e-4,
            equal_nan=True,
            err_msg=f'{indicator}.{col} calculator vs TechnicalAnalyzer 不一致',
        )


# ----------------------------------------------------------------------
# 缓存列抽取：按指标声明列名
# ----------------------------------------------------------------------


def test_manager_cache_stores_dmi_columns(frame):
    """dmi 按 pdi/ndi/adx/adxr 声明列名进缓存，不再永不命中。"""
    global_indicator_manager.calculate_indicators(frame, ['dmi'], return_polars=True)
    cached = global_indicator_cache.get(preprocess_data_polars(frame), 'dmi')

    assert cached is not None
    assert {'pdi', 'ndi', 'adx', 'adxr'} <= set(cached.columns)


def test_manager_cache_does_not_mix_ma_and_macd(frame):
    """ma 的缓存只含 ma 列，不会把同批计算的 macd 列一起吞进缓存。"""
    global_indicator_manager.calculate_indicators(
        frame, ['ma', 'macd'], return_polars=True
    )
    pl_data = preprocess_data_polars(frame)

    ma_cached = global_indicator_cache.get(pl_data, 'ma')
    macd_cached = global_indicator_cache.get(pl_data, 'macd')

    assert ma_cached is not None
    assert {'ma', 'ma5', 'ma10', 'ma20', 'ma60'} <= set(ma_cached.columns)
    assert not {'macd', 'macd_signal', 'macd_hist'} & set(ma_cached.columns)

    assert macd_cached is not None
    assert {'macd', 'macd_signal', 'macd_hist'} <= set(macd_cached.columns)
