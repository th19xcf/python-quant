#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pandas↔Polars 转换开销治理的回归测试

覆盖：
- P0: ensure_indicator_columns 渲染前批量补齐（indicator_renderer）
- P1: return_polars 默认值翻转（IndicatorManager / TechnicalAnalyzer）
- P2: preprocess_data_polars 幂等快速路径
- P3: 遗留 pandas 插件路径列裁剪与共享源帧
"""

import datetime

import numpy as np
import pandas as pd
import polars as pl
import pytest

from src.tech_analysis.indicator_calculator import preprocess_data_polars
from src.tech_analysis.indicator_manager import IndicatorManager
from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
from src.plugin.plugin_base import IndicatorPlugin


def _make_ohlcv(rows=80, extra_cols=False):
    """构造稳定的 OHLCV 测试数据（全 Float64 价格/成交量列）"""
    base = np.arange(rows, dtype=np.float64) + 10.0
    data = {
        'date': [datetime.date(2024, 1, 1) + datetime.timedelta(days=i) for i in range(rows)],
        'open': base + 0.1,
        'high': base + 1.0,
        'low': base - 1.0,
        'close': base,
        'volume': np.full(rows, 1_000_000.0),
    }
    if extra_cols:
        # 额外的非基列，用于验证插件路径的列裁剪
        data['amount'] = base * 100.0
    return pl.DataFrame(data)


# ---------------------------------------------------------------------------
# P0: ensure_indicator_columns 渲染前批量补齐
# ---------------------------------------------------------------------------

class TestEnsureIndicatorColumns:
    """渲染前批量补齐指标列（单个 Polars 查询计划）"""

    @pytest.fixture(autouse=True)
    def _import_renderer(self):
        pytest.importorskip('pyqtgraph')
        from src.ui.chart_components import indicator_renderer
        self.mod = indicator_renderer

    def test_missing_indicator_columns_are_computed(self):
        df = _make_ohlcv()
        assert 'k' not in df.columns and 'macd' not in df.columns

        result = self._ensure(df, ['KDJ', 'MACD'])

        for col in ('k', 'd', 'macd', 'macd_signal'):
            assert col in result.columns, f"缺少指标列 {col}"

    def test_already_present_columns_skip_calculation(self):
        df = _make_ohlcv()
        df = self._ensure(df, ['KDJ'])
        assert 'k' in df.columns

        # 列已齐备时应原样返回（同一对象，未触发任何计算）
        assert self._ensure(df, ['KDJ']) is df

    def test_unknown_and_passthrough_names(self):
        df = _make_ohlcv()
        # VOL 直接绘制 volume 列，VOL-TDX 无批量实现，未知名忽略 —— 均应原样返回
        assert self._ensure(df, ['VOL']) is df
        assert self._ensure(df, ['VOL-TDX']) is df
        assert self._ensure(df, ['NOT_EXIST']) is df
        assert self._ensure(df, []) is df
        assert self._ensure(None, ['KDJ']) is None

    def _ensure(self, df, names):
        return self.mod.ensure_indicator_columns(df, names)


# ---------------------------------------------------------------------------
# P1: return_polars 默认值翻转
# ---------------------------------------------------------------------------

class TestReturnPolarsDefault:
    """默认返回 Polars，避免全量 to_pandas 转换开销"""

    def test_manager_calculate_indicator_default_polars(self):
        manager = IndicatorManager(cache_enabled=False)
        df = _make_ohlcv()

        result_default = manager.calculate_indicator(df, 'ma', windows=[5])
        result_pandas = manager.calculate_indicator(df, 'ma', windows=[5], return_polars=False)

        assert isinstance(result_default, pl.DataFrame)
        assert isinstance(result_pandas, pd.DataFrame)
        assert 'ma5' in result_default.columns

    def test_manager_calculate_indicators_default_polars(self):
        manager = IndicatorManager(cache_enabled=False)
        df = _make_ohlcv()

        result_default = manager.calculate_indicators(df, ['ma', 'rsi'])
        result_pandas = manager.calculate_indicators(df, ['ma', 'rsi'], return_polars=False)

        assert isinstance(result_default, pl.DataFrame)
        assert isinstance(result_pandas, pd.DataFrame)

    def test_analyzer_calculate_all_default_polars(self):
        analyzer = TechnicalAnalyzer(_make_ohlcv())

        result_default = analyzer.calculate_all_indicators(indicator_types=['ma'])
        assert isinstance(result_default, pl.DataFrame)

        analyzer2 = TechnicalAnalyzer(_make_ohlcv())
        result_pandas = analyzer2.calculate_all_indicators(
            indicator_types=['ma'], return_polars=False)
        assert isinstance(result_pandas, pd.DataFrame)


# ---------------------------------------------------------------------------
# P2: preprocess_data_polars 幂等快速路径
# ---------------------------------------------------------------------------

class TestPreprocessIdempotent:
    """已预处理数据直接原样返回，跳过重复全量优化"""

    def test_already_preprocessed_returns_same_object(self):
        df = _make_ohlcv().with_columns(
            pl.Series('ma5', np.arange(80, dtype=np.float32))
        )
        assert preprocess_data_polars(df) is df

    def test_nan_in_float32_column_goes_full_path(self):
        values = np.arange(80, dtype=np.float32)
        values[3] = np.nan
        df = _make_ohlcv().with_columns(pl.Series('ma5', values))

        result = preprocess_data_polars(df)

        assert result is not df
        assert result['ma5'].is_nan().sum() == 0

    def test_unstable_dtype_goes_full_path(self):
        df = _make_ohlcv().with_columns(
            pl.Series('up_count', np.arange(80), dtype=pl.Int64)
        )
        assert preprocess_data_polars(df) is not df


# ---------------------------------------------------------------------------
# P3: 遗留 pandas 插件路径列裁剪
# ---------------------------------------------------------------------------

class _LegacyPlugin(IndicatorPlugin):
    """声明所需列的遗留 pandas 插件（supports_polars 默认为 False）"""

    def __init__(self, name='legacy_test'):
        super().__init__()
        self._name = name
        self.seen_columns = None

    def get_name(self):
        return self._name

    def get_version(self):
        return '1.0.0'

    def get_author(self):
        return 'test'

    def get_description(self):
        return 'test'

    def get_required_columns(self):
        return ['close']

    def get_output_columns(self):
        return [f'{self._name}_out']

    def calculate(self, data, **kwargs):
        self.seen_columns = list(data.columns)
        data = data.copy()
        data[f'{self._name}_out'] = data['close'] * 2
        return data


class _UndeclaredPlugin(_LegacyPlugin):
    """未声明所需列的插件（应触发全量回退）"""

    def get_required_columns(self):
        return []

    def get_output_columns(self):
        return []


class _StubPluginManager:
    def __init__(self, plugins):
        self._plugins = plugins

    def get_available_indicator_plugins(self):
        return self._plugins


class TestLegacyPluginTrimming:
    """遗留插件路径按声明列裁剪输入/输出，避免全量列往返转换"""

    def test_calculate_polars_shim_trims_input_and_output(self):
        plugin = _LegacyPlugin('shim_test')
        df = _make_ohlcv(extra_cols=True)

        result = plugin.calculate_polars(df)

        # 输入裁剪：插件只见基列 + 声明列，看不到额外的 amount 列
        assert plugin.seen_columns is not None
        assert 'close' in plugin.seen_columns
        assert 'amount' not in plugin.seen_columns
        # 输出裁剪：仅回转 date + 输出列，不把输入列重复转回 Polars
        assert set(result.columns) == {'date', 'shim_test_out'}

    def test_legacy_path_trims_input(self):
        plugin = _LegacyPlugin('legacy_trim')
        analyzer = TechnicalAnalyzer(
            _make_ohlcv(extra_cols=True),
            plugin_manager=_StubPluginManager({'legacy_trim': plugin}),
        )

        analyzer.calculate_plugin_indicator('legacy_trim')

        assert plugin.seen_columns is not None
        assert 'close' in plugin.seen_columns
        assert 'amount' not in plugin.seen_columns
        assert 'legacy_trim_out' in analyzer.pl_df.columns

    def test_plugin_required_columns_fallback_on_undeclared(self):
        plugin = _UndeclaredPlugin('legacy_full')
        analyzer = TechnicalAnalyzer(
            _make_ohlcv(extra_cols=True),
            plugin_manager=_StubPluginManager({'legacy_full': plugin}),
        )

        analyzer.calculate_plugin_indicator('legacy_full')

        # 未声明所需列时回退全量，保证旧插件兼容
        assert 'amount' in plugin.seen_columns

    def test_build_shared_legacy_source(self):
        declared = _LegacyPlugin('shared_a')
        undeclared = _UndeclaredPlugin('shared_b')
        analyzer = TechnicalAnalyzer(
            _make_ohlcv(extra_cols=True),
            plugin_manager=_StubPluginManager(
                {'shared_a': declared, 'shared_b': undeclared}),
        )

        # 全部遗留插件都声明所需列 → 返回裁剪后的共享源帧
        shared = analyzer._build_shared_legacy_source(['shared_a'])
        assert shared is not None
        assert 'close' in shared.columns
        assert 'amount' not in shared.columns

        # 批次中存在未声明插件 → 放弃共享（回退逐插件裁剪）
        assert analyzer._build_shared_legacy_source(['shared_a', 'shared_b']) is None
