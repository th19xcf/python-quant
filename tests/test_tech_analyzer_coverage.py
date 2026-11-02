#!/usr/bin/env python3

"""TechnicalAnalyzer 及其 analyzer mixin 的覆盖率补齐用例。

覆盖三块此前长期低覆盖的热点：
1. technical_analyzer 的输入类型分派、数据哈希、窗口/简单指标幂等与采样；
2. cache_ops 的计算状态重置与缓存清理；
3. parallel_ops / plugin_ops 的缓存命中、插件（Polars/遗留 pandas）、
   增量与 GPU 降级等分支。

全部使用构造数据与假插件，不依赖数据库、网络与真实插件包。
"""

from types import SimpleNamespace

import numpy as np
import pandas as pd
import polars as pl
import pytest

from src.tech_analysis import indicator_calculator
from src.tech_analysis import technical_analyzer as ta_module
from src.tech_analysis.analyzer import parallel_ops, plugin_ops
from src.tech_analysis.indicator_cache import global_indicator_cache
from src.tech_analysis.technical_analyzer import TechnicalAnalyzer

# ----------------------------------------------------------------------
# 构造数据与假插件
# ----------------------------------------------------------------------


def _ohlcv(n: int = 120, extra: bool = True) -> pl.DataFrame:
    """构造稳定的 OHLCV 数据（date 为 pl.Date）。"""
    rng = np.random.default_rng(7)
    close = 100.0 + np.cumsum(rng.standard_normal(n))
    dates = pl.date_range(
        pl.date(2024, 1, 1), pl.date(2024, 12, 31), interval='1d', eager=True
    ).head(n)
    data = {
        'date': dates,
        'open': close + rng.standard_normal(n) * 0.2,
        'high': close + np.abs(rng.standard_normal(n)),
        'low': close - np.abs(rng.standard_normal(n)),
        'close': close,
        'volume': rng.integers(100_000, 1_000_000, n).astype(float),
    }
    if extra:
        data['amount'] = data['volume'] * 2.0
    return pl.DataFrame(data)


class _FakePluginManager:
    """按注册字典返回可用指标插件的替身。"""

    def __init__(self, plugins=None):
        self._plugins = dict(plugins or {})

    def get_available_indicator_plugins(self):
        return self._plugins

    def get_available_datasource_plugins(self):
        return {}


class _PolarsPlugin:
    """支持 Polars 的插件替身，输出一列派生指标。"""

    def __init__(self, output='plugin_polars'):
        self.output = output
        self.calls = 0

    def supports_polars(self):
        return True

    def get_required_columns(self):
        return ['close']

    def get_output_columns(self):
        return [self.output]

    def calculate_polars(self, data, **kwargs):
        self.calls += 1
        return data.select(['date', 'close']).with_columns(
            (pl.col('close') * 2).alias(self.output)
        )


class _LegacyPlugin:
    """遗留 pandas 插件替身：pandas 入、pandas 出。"""

    def __init__(self, output='plugin_legacy', required=('close',)):
        self.output = output
        self.required = list(required)
        self.calls = 0

    def supports_polars(self):
        return False

    def get_required_columns(self):
        return self.required

    def get_output_columns(self):
        return [self.output]

    def calculate(self, data, **kwargs):
        self.calls += 1
        result = data.copy()
        result[self.output] = result['close'] * 3
        return result


class _BrokenLegacyPlugin(_LegacyPlugin):
    def calculate(self, data, **kwargs):
        raise ValueError('插件计算失败')


class _SyncFuture:
    """同步 Future 替身，供线程池替换后确定性收集结果。"""

    def __init__(self, func, *args, **kwargs):
        self._func = func
        self._args = args
        self._kwargs = kwargs

    def result(self):
        return self._func(*self._args, **self._kwargs)


class _SyncExecutor:
    """把 ThreadPoolExecutor 替换为同线程执行，避免测试并发竞态。"""

    def __init__(self, max_workers=None):
        self.max_workers = max_workers

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def submit(self, func, *args, **kwargs):
        return _SyncFuture(func, *args, **kwargs)


@pytest.fixture(autouse=True)
def _clean_indicator_cache():
    global_indicator_cache.clear()
    yield
    global_indicator_cache.clear()


@pytest.fixture
def frame():
    return _ohlcv()


# ----------------------------------------------------------------------
# technical_analyzer：输入分派、哈希、幂等、采样
# ----------------------------------------------------------------------


@pytest.mark.parametrize('kind', ['polars', 'pandas', 'records'])
def test_init_accepts_multiple_input_kinds(kind, frame):
    """Polars/pandas/records 三种输入都能落成非惰性主帧并生成哈希。"""
    if kind == 'polars':
        data = frame
    elif kind == 'pandas':
        data = frame.to_pandas()
    else:
        data = frame.select(['open', 'high', 'low', 'close', 'volume']).to_dicts()

    analyzer = TechnicalAnalyzer(data)

    assert isinstance(analyzer.pl_df, pl.DataFrame)
    assert analyzer.is_lazy is False
    assert analyzer.pl_df.height == frame.height
    assert isinstance(analyzer._calculate_polars_data_hash(), int)
    assert analyzer.get_data().height == frame.height


def test_init_lazyframe_keeps_lazy_and_get_data_collects(frame):
    """LazyFrame 输入保持惰性，get_data 触发 collect 并切换回非惰性。"""
    analyzer = TechnicalAnalyzer(frame.lazy())

    assert analyzer.is_lazy is True
    assert isinstance(analyzer._calculate_polars_data_hash(), int)
    assert analyzer.get_data().height == frame.height
    assert analyzer.is_lazy is False


def test_data_hash_is_zero_for_empty_frame():
    """空表哈希返回 0，避免空数据被当成有效缓存键。"""
    empty = _ohlcv().head(0)
    analyzer = TechnicalAnalyzer(empty)

    assert analyzer._calculate_polars_data_hash() == 0


def test_window_indicators_return_expected_columns_and_are_idempotent(frame):
    """ma/rsi/kdj/vol_ma/wr 的计算列正确，重复调用不新增重复列。"""
    analyzer = TechnicalAnalyzer(frame)

    first = analyzer.calculate_ma()
    expected = {'ma5', 'ma10', 'ma20', 'ma60', 'ma'}
    assert expected.issubset(set(first.columns))
    second = analyzer.calculate_ma()
    assert second.columns == first.columns
    assert analyzer.is_indicator_calculated('ma', 5) is True
    assert analyzer.is_indicator_calculated('ma', 7) is False

    rsi = analyzer.calculate_rsi(windows=6)
    assert {'rsi6', 'rsi'}.issubset(set(rsi.columns))
    assert analyzer.calculate_rsi(windows=[6]).columns == rsi.columns

    kdj = analyzer.calculate_kdj(windows=[9])
    assert {'k9', 'd9', 'j9'}.issubset(set(kdj.columns))

    vol_ma = analyzer.calculate_vol_ma(windows=5)
    assert {'vol_ma5', 'vol_ma'}.issubset(set(vol_ma.columns))
    assert analyzer.calculate_vol_ma(windows=[5]).columns == vol_ma.columns

    wr = analyzer.calculate_wr()
    assert {'wr10', 'wr6', 'wr2', 'wr'}.issubset(set(wr.columns))
    assert analyzer.calculate_wr(windows=[10, 6]).columns == wr.columns


def test_boll_and_wr_handle_scalar_window(frame):
    """boll/wr 的默认窗口与标量窗口都被规范化成列表处理。"""
    analyzer = TechnicalAnalyzer(frame)

    boll = analyzer.calculate_boll(windows=20, std_dev=2.5)
    assert {'mb20', 'up20', 'dn20', 'mb', 'up', 'dn'}.issubset(set(boll.columns))

    wr = analyzer.calculate_wr(windows=12)
    assert {'wr12', 'wr'}.issubset(set(wr.columns))
    assert analyzer.calculate_wr(windows=[12]).columns == wr.columns


def test_simple_indicators_macd_and_obv(frame):
    """macd 走简单指标路径；obv 走并行路径，重复调用幂等。"""
    analyzer = TechnicalAnalyzer(frame)

    macd = analyzer.calculate_macd(fast_period=6, slow_period=13, signal_period=5)
    assert {'macd', 'macd_signal', 'macd_hist'}.issubset(set(macd.columns))
    assert analyzer.calculate_macd().columns == macd.columns
    assert analyzer.is_indicator_calculated('macd') is True

    obv = analyzer.calculate_indicator_parallel('obv')
    assert 'obv' in obv.columns
    assert analyzer.calculate_indicator_parallel('obv').columns == obv.columns


class _CachedFrameStub:
    """缓存结果替身：只暴露 columns/select/lazy，join 合法性由下方 stub 兜住。"""

    def __init__(self, columns):
        self.columns = list(columns)
        self.selected = []

    def select(self, cols):
        self.selected = list(cols)
        return self

    def lazy(self):
        return self


class _JoinFrameStub:
    """主帧替身：记录 join 调用次数，避免依赖真实 join 键对齐。"""

    def __init__(self, frame):
        self._frame = frame
        self.joins = []
        self.collect_calls = 0

    @property
    def columns(self):
        return self._frame.columns

    def join(self, _other, on, how):
        self.joins.append((on, how))
        return self

    def collect(self):
        self.collect_calls += 1
        return self._frame


@pytest.mark.parametrize('lazy', [False, True])
def test_cache_hit_join_path_updates_state_without_recompute(monkeypatch, frame, lazy):
    """缓存命中走 join 合列分支：DataFrame/LazyFrame 两种主帧都只合并不重算。"""
    analyzer = TechnicalAnalyzer(frame)
    stub = _JoinFrameStub(frame)
    analyzer.pl_df = stub
    analyzer.is_lazy = lazy
    cached = _CachedFrameStub(list(frame.columns) + ['cached_ind'])
    monkeypatch.setattr(
        ta_module.global_indicator_cache, 'get', lambda *a, **kw: cached
    )

    analyzer.calculate_ma(windows=[5])

    assert stub.joins == [(['date'], 'left')]
    assert analyzer.calculated_indicators['ma'] == {5}
    assert 'cached_ind' in cached.selected


def test_sample_data_and_get_data_options(frame):
    """采样支持 uniform/adaptive 及 pandas 输出，get_data 可带采样参数。"""
    analyzer = TechnicalAnalyzer(frame)

    uniform = analyzer.sample_data(target_points=200, strategy='uniform')
    assert uniform.height == frame.height

    adaptive = analyzer.sample_data(target_points=10, strategy='adaptive')
    assert adaptive.height <= 10

    as_pandas = analyzer.sample_data(
        target_points=10, strategy='adaptive', return_polars=False
    )
    assert isinstance(as_pandas, pd.DataFrame)
    assert len(as_pandas) <= 10

    sampled = analyzer.get_data(
        sample=True, sample_params={'target_points': 5, 'strategy': 'adaptive'}
    )
    assert sampled.height <= 5

    assert isinstance(analyzer.get_data(return_polars=False), pd.DataFrame)


def test_supported_indicators_include_plugins(frame):
    """内置指标与插件指标合并；查询接口对两者都返回 True。"""
    plugin = _PolarsPlugin()
    manager = _FakePluginManager({'plugin_polars': plugin})
    analyzer = TechnicalAnalyzer(frame, plugin_manager=manager)

    supported = analyzer.get_supported_indicators()

    assert 'ma' in supported
    assert 'plugin_polars' in supported
    assert analyzer.is_indicator_supported('macd') is True
    assert analyzer.is_indicator_supported('nope') is False


# ----------------------------------------------------------------------
# cache_ops：状态查询、重置与缓存清理
# ----------------------------------------------------------------------


def test_cache_key_and_status_helpers(frame):
    """缓存键对参数顺序不敏感；状态查询区分窗口/布尔/未知指标。"""
    analyzer = TechnicalAnalyzer(frame)
    analyzer.calculate_ma(windows=[5])

    assert analyzer._generate_cache_key('ma', windows=5, std_dev=2.0) == (
        analyzer._generate_cache_key('ma', std_dev=2.0, windows=5)
    )
    assert analyzer.is_indicator_calculated('not_exist') is False
    assert analyzer.is_indicator_calculated('macd') is False
    assert analyzer.is_indicator_calculated('ma', None) is False
    assert isinstance(analyzer.get_calculated_indicators(), dict)
    assert 'stats' in analyzer.get_cache_stats()


def test_reset_window_indicators_drops_columns(frame):
    """按窗口/按类型重置窗口指标都会删除对应派生列。"""
    analyzer = TechnicalAnalyzer(frame)
    analyzer.calculate_ma(windows=[5, 10])
    analyzer.calculate_boll(windows=[20])
    analyzer.calculate_wr(windows=[10, 6])
    analyzer.calculate_vol_ma(windows=[5])

    analyzer.reset_calculation('ma', window=5)
    assert 'ma5' not in analyzer.pl_df.columns
    assert 'ma10' in analyzer.pl_df.columns

    analyzer.reset_calculation('ma')
    assert not {'ma5', 'ma10'} & set(analyzer.pl_df.columns)

    analyzer.reset_calculation('boll')
    assert not {'mb20', 'up20', 'dn20', 'mb', 'up', 'dn'} & set(analyzer.pl_df.columns)

    analyzer.reset_calculation('wr')
    assert 'wr10' not in analyzer.pl_df.columns

    analyzer.reset_calculation('vol_ma', window=5)
    assert 'vol_ma5' not in analyzer.pl_df.columns


def test_reset_simple_indicator_and_all(frame):
    """macd/obv 重置恢复布尔状态并删除列；重置全部只保留原始列。"""
    analyzer = TechnicalAnalyzer(frame)
    analyzer.calculate_macd()
    analyzer.calculate_indicator_parallel('obv')

    analyzer.reset_calculation('macd')
    assert analyzer.calculated_indicators['macd'] is False
    assert 'macd' not in analyzer.pl_df.columns

    analyzer.reset_calculation('obv')
    assert 'obv' not in analyzer.pl_df.columns

    analyzer.reset_calculation()
    assert set(analyzer.pl_df.columns) <= {'open', 'high', 'low', 'close', 'volume'}
    assert analyzer.calculated_indicators['ma'] == set()


def test_reset_plugin_state(frame):
    """插件指标状态可按插件名或整体清空。"""
    plugin = _PolarsPlugin()
    manager = _FakePluginManager({'plugin_polars': plugin})
    analyzer = TechnicalAnalyzer(frame, plugin_manager=manager)

    analyzer.calculate_plugin_indicator('plugin_polars')
    assert 'plugin_polars' in analyzer.calculated_indicators['plugin']

    analyzer.reset_calculation('plugin_polars')
    assert 'plugin_polars' not in analyzer.calculated_indicators['plugin']

    analyzer.calculate_plugin_indicator('plugin_polars')
    analyzer._calculate_cache['plugin_legacy'] = object()
    analyzer.reset_calculation('plugin')
    assert analyzer.calculated_indicators['plugin'] == {}
    assert 'plugin_legacy' not in analyzer._calculate_cache


def test_clear_calculation_cache_both_scopes(frame):
    """按指标清缓存调用一次 reset；全量清缓存清空内部缓存。"""
    analyzer = TechnicalAnalyzer(frame)
    analyzer.calculate_ma(windows=[5])
    analyzer._calculate_cache['ma'] = object()

    assert analyzer.clear_calculation_cache('ma') is True
    assert analyzer.calculated_indicators['ma'] == set()

    analyzer._calculate_cache['x'] = object()
    assert analyzer.clear_calculation_cache() is True
    assert analyzer._calculate_cache == {}


def test_clear_calculation_cache_returns_false_on_error(monkeypatch, frame):
    """reset 抛 OSError 时返回 False，不向上抛出。"""
    analyzer = TechnicalAnalyzer(frame)

    def _boom(*_args, **_kwargs):
        raise OSError('缓存清理失败')

    monkeypatch.setattr(analyzer, 'reset_calculation', _boom)

    assert analyzer.clear_calculation_cache('ma') is False


# ----------------------------------------------------------------------
# parallel_ops：批量/并行计算与缓存、GPU、增量分支
# ----------------------------------------------------------------------


def test_calculate_indicator_parallel_valid_windows(frame):
    """合法窗口指标走批量惰性路径，非法指标抛 ValueError。"""
    analyzer = TechnicalAnalyzer(frame)

    result = analyzer.calculate_indicator_parallel('ma', windows=[5])
    assert 'ma5' in result.columns
    assert analyzer.calculate_indicator_parallel('ma', windows=[5]).columns == result.columns

    dmi = analyzer.calculate_indicator_parallel('dmi', windows=14)
    assert {'pdi_14', 'adx_14', 'pdi', 'adx'}.issubset(set(dmi.columns))

    with pytest.raises(ValueError, match='不支持的指标类型'):
        analyzer.calculate_indicator_parallel('definitely_unknown')


def test_calculate_indicators_parallel_mixes_builtin_and_plugin(monkeypatch, frame):
    """内置指标与插件指标混合：缓存命中只合列，未命中才计算。"""
    plugin = _LegacyPlugin('plugin_legacy')
    manager = _FakePluginManager({'plugin_legacy': plugin})
    analyzer = TechnicalAnalyzer(frame, plugin_manager=manager)

    def _fake_get(_data, indicator_type, **_params):
        # 命中时返回与主帧同列的结果：无新增列，走"命中即跳过计算"分支
        return frame if indicator_type == 'ma' else None

    monkeypatch.setattr(global_indicator_cache, 'get', _fake_get)
    monkeypatch.setattr('concurrent.futures.ThreadPoolExecutor', _SyncExecutor)
    monkeypatch.setattr(
        'concurrent.futures.as_completed', lambda futures: list(futures)
    )

    result = analyzer.calculate_indicators_parallel(['ma', 'rsi', 'plugin_legacy'])

    assert 'rsi14' in result.columns
    assert 'plugin_legacy' in result.columns
    assert analyzer.calculated_indicators['ma'] == {14}
    assert analyzer.calculated_indicators['macd'] is False


def test_calculate_plugins_parallel_skips_calculated_and_counts_failures(monkeypatch, frame):
    """插件并行：已登记参数被跳过；单个插件失败只记录不抛出。"""
    ok_plugin = _LegacyPlugin('ok_plugin')
    bad_plugin = _BrokenLegacyPlugin('bad_plugin')
    manager = _FakePluginManager({'ok_plugin': ok_plugin, 'bad_plugin': bad_plugin})
    analyzer = TechnicalAnalyzer(frame, plugin_manager=manager)
    monkeypatch.setattr('concurrent.futures.ThreadPoolExecutor', _SyncExecutor)
    monkeypatch.setattr('concurrent.futures.as_completed', lambda futures: list(futures))

    analyzer.calculate_plugin_indicators_parallel(['ok_plugin', 'bad_plugin'])

    assert 'ok_plugin' in analyzer.pl_df.columns
    assert 'bad_plugin' not in analyzer.pl_df.columns
    assert analyzer.calculated_indicators['plugin']['ok_plugin']

    # 第二次同样的参数组合全部被跳过，不再触发插件调用
    calls_before = ok_plugin.calls
    analyzer.calculate_plugin_indicators_parallel(['ok_plugin'])
    assert ok_plugin.calls == calls_before


def test_calculate_plugin_indicators_parallel_without_plugins(frame):
    """没有可用插件时直接返回主帧（惰性帧会被 collect）。"""
    analyzer = TechnicalAnalyzer(frame.lazy())

    result = analyzer.calculate_plugin_indicators_parallel([])

    assert result.height == frame.height
    assert analyzer.is_lazy is False


def test_calculate_all_indicators_and_builtin_batch_error(monkeypatch, frame):
    """批量入口覆盖默认指标、显式指标与计算失败事件分支。"""
    analyzer = TechnicalAnalyzer(frame)

    result = analyzer.calculate_all_indicators(indicator_types=['ma'], windows=[5])
    assert 'ma5' in result.columns
    assert analyzer.calculated_indicators['ma'] == {5}

    empty = analyzer.calculate_all_indicators(indicator_types=[])
    assert empty.height == frame.height

    def _boom(*_args, **_kwargs):
        raise ValueError('批量计算失败')

    monkeypatch.setattr(parallel_ops, 'calculate_multiple_indicators_polars', _boom)
    success, error_message = analyzer._calculate_builtin_batch(['ma'], {'windows': [5]})
    assert success is False
    assert '批量计算失败' in error_message


def test_load_indicator_input_handles_external_frames(frame):
    """显式传入 pandas/records 数据会替换主帧并保留复权列。"""
    analyzer = TechnicalAnalyzer(frame)
    with_adj = frame.with_columns(pl.lit(1.0).alias('qfq_factor'))

    pandas_result = analyzer.calculate_all_indicators(with_adj.to_pandas(), ['ma'])
    assert 'ma5' in pandas_result.columns

    records = with_adj.to_dicts()
    records_result = analyzer.calculate_all_indicators(records, ['rsi'])
    assert 'rsi14' in records_result.columns


def test_try_gpu_indicator_falls_back_or_merges(monkeypatch, frame):
    """GPU 不可用返回 False；可用且计算成功时把结果并回主帧。"""
    analyzer = TechnicalAnalyzer(frame)

    monkeypatch.setattr(parallel_ops, 'is_gpu_available', lambda: False)
    assert analyzer._try_gpu_indicator('ma', {'windows': [5]}) is False

    monkeypatch.setattr(parallel_ops, 'is_gpu_available', lambda: True)
    monkeypatch.setattr(
        parallel_ops, 'calculate_with_gpu',
        lambda _ind, _data, **_kw: {'gpu_ma5': [1.0] * frame.height},
    )
    assert analyzer._try_gpu_indicator('ma', {'windows': [5]}) is True
    assert 'gpu_ma5' in analyzer.pl_df.columns


class _FakeIncrementalCalculator:
    def __init__(self, supported=True, result=None, raise_error=False):
        self.supported = supported
        self.result = result
        self.raise_error = raise_error

    def is_supported(self, _indicator):
        return self.supported

    def incremental_calculate(self, _indicator, _df, _incremental, **_kw):
        if self.raise_error:
            raise ValueError('增量计算失败')
        return self.result


def test_try_incremental_indicator_success_and_failure(monkeypatch, frame):
    """增量成功时 vstack 并登记状态；列序不一致时补齐并按主帧列序重排。"""
    analyzer = TechnicalAnalyzer(frame)
    reordered = frame.select(list(reversed(frame.columns)))
    monkeypatch.setattr(
        parallel_ops, 'global_incremental_calculator',
        _FakeIncrementalCalculator(result=reordered),
    )

    assert analyzer._try_incremental_indicator(
        'ma', SimpleNamespace(), {'windows': [5]}
    ) is True
    assert analyzer.pl_df.height == frame.height * 2

    monkeypatch.setattr(
        parallel_ops, 'global_incremental_calculator',
        _FakeIncrementalCalculator(raise_error=True),
    )
    assert analyzer._try_incremental_indicator(
        'rsi', SimpleNamespace(), {'windows': [6]}
    ) is False


def test_calculate_indicator_via_manager_and_store_failure(monkeypatch, frame):
    """管理器计算成功；缓存写入失败只降级记录日志。"""
    analyzer = TechnicalAnalyzer(frame)

    result = analyzer._calculate_indicator_via_manager('ma', {'windows': [5]})
    assert 'ma5' in result.columns

    def _boom(*_args, **_kwargs):
        raise ValueError('缓存写入失败')

    columns_before = list(analyzer.pl_df.columns)
    monkeypatch.setattr(parallel_ops.global_indicator_cache, 'set', _boom)
    analyzer._store_indicator_cache('rsi', {'windows': [6]})
    assert analyzer.pl_df.columns == columns_before


def test_update_indicator_state_handles_unknown_and_bool(frame):
    """未知指标直接返回；窗口集合与布尔状态分别更新。"""
    analyzer = TechnicalAnalyzer(frame)

    analyzer._update_indicator_state('unknown', {'windows': 5})
    assert 'unknown' not in analyzer.calculated_indicators

    analyzer._update_indicator_state('ma', {'windows': 6})
    assert 6 in analyzer.calculated_indicators['ma']

    analyzer._update_indicator_state('macd', {})
    assert analyzer.calculated_indicators['macd'] is True


# ----------------------------------------------------------------------
# plugin_ops：插件命中、全量、增量与并行
# ----------------------------------------------------------------------


def test_plugin_manager_absent_guards(frame):
    """无插件管理器时插件列表为空，计算插件指标抛 ValueError。"""
    analyzer = TechnicalAnalyzer(frame)

    assert analyzer.get_available_plugin_indicators() == []
    with pytest.raises(ValueError, match='插件管理器未初始化'):
        analyzer.calculate_plugin_indicator('anything')


def test_plugin_requires_registered_name(frame):
    """插件名未注册时抛 ValueError。"""
    analyzer = TechnicalAnalyzer(frame, plugin_manager=_FakePluginManager({}))

    with pytest.raises(ValueError, match='不存在或未启用'):
        analyzer.calculate_plugin_indicator('missing')


def test_plugin_polars_full_calculation_and_state_hit(frame):
    """Polars 插件全量计算并入新列；同参数二次调用直接命中状态缓存。"""
    plugin = _PolarsPlugin('plugin_polars')
    manager = _FakePluginManager({'plugin_polars': plugin})
    analyzer = TechnicalAnalyzer(frame, plugin_manager=manager)

    first = analyzer.calculate_plugin_indicator('plugin_polars')
    assert 'plugin_polars' in first.columns
    calls = plugin.calls

    second = analyzer.calculate_plugin_indicator('plugin_polars')
    assert second.columns == first.columns
    assert plugin.calls == calls

    # 插件返回 None 时不得新增列
    monkeypatch_plugin = _PolarsPlugin('plugin_none')
    monkeypatch_plugin.calculate_polars = lambda data, **kw: None
    manager._plugins['plugin_none'] = monkeypatch_plugin
    analyzer.calculate_plugin_indicator('plugin_none')
    assert 'plugin_none' not in analyzer.pl_df.columns


def test_plugin_global_cache_hit_merges_columns(monkeypatch, frame):
    """全局缓存命中的插件结果只合并新列并登记状态。"""
    plugin = _PolarsPlugin('plugin_cached')
    analyzer = TechnicalAnalyzer(
        frame, plugin_manager=_FakePluginManager({'plugin_cached': plugin})
    )
    cached = frame.select(['date']).with_columns(pl.lit(2.0).alias('plugin_cached'))
    monkeypatch.setattr(global_indicator_cache, 'get', lambda *a, **kw: cached)

    result = analyzer.calculate_plugin_indicator('plugin_cached')

    assert 'plugin_cached' in result.columns
    assert 'plugin_cached' in analyzer.calculated_indicators['plugin']


def test_plugin_incremental_calculation(monkeypatch, frame):
    """支持增量的插件走 incremental_calculate，结果 vstack 到主帧。"""

    class _IncrementalPlugin:
        def supports_polars(self):
            return False

        def supports_incremental(self):
            return True

        def get_required_columns(self):
            return ['close']

        def get_output_columns(self):
            return ['plugin_inc']

        def calculate(self, data, **kwargs):
            return data

        def incremental_calculate(self, data, incremental, **kwargs):
            return data

    manager = _FakePluginManager({'plugin_inc': _IncrementalPlugin()})
    analyzer = TechnicalAnalyzer(frame, plugin_manager=manager)

    result = analyzer.calculate_plugin_indicator(
        'plugin_inc', incremental_data='new_rows'
    )

    assert result.height == frame.height * 2
    assert analyzer.calculated_indicators['plugin']['plugin_inc']


def test_plugin_calculation_error_wrapped(frame):
    """插件内部 ValueError 被包装为 RuntimeError，便于调用方统一处理。"""
    analyzer = TechnicalAnalyzer(
        frame, plugin_manager=_FakePluginManager({'broken': _BrokenLegacyPlugin('broken')})
    )

    with pytest.raises(RuntimeError, match='计算插件指标broken失败'):
        analyzer.calculate_plugin_indicator('broken')


def test_legacy_plugin_uses_shared_source_when_available(monkeypatch, frame):
    """两个遗留插件并行时，共享裁剪源帧并各自产出列。"""
    first = _LegacyPlugin('legacy_a')
    second = _LegacyPlugin('legacy_b')
    manager = _FakePluginManager({'legacy_a': first, 'legacy_b': second})
    analyzer = TechnicalAnalyzer(frame, plugin_manager=manager)
    monkeypatch.setattr('concurrent.futures.ThreadPoolExecutor', _SyncExecutor)
    monkeypatch.setattr('concurrent.futures.as_completed', lambda futures: list(futures))

    shared = analyzer._build_shared_legacy_source(['legacy_a', 'legacy_b'])
    assert shared is not None
    assert 'amount' not in shared.columns

    analyzer.calculate_plugin_indicators_parallel(['legacy_a', 'legacy_b'])
    assert {'legacy_a', 'legacy_b'} <= set(analyzer.pl_df.columns)


def test_shared_source_returns_none_for_incomplete_declarations(frame):
    """插件未注册、纯 Polars、未声明所需列时共享源帧回退为 None。"""
    manager = _FakePluginManager({'polars_only': _PolarsPlugin('polars_only')})
    analyzer = TechnicalAnalyzer(frame, plugin_manager=manager)

    assert analyzer._build_shared_legacy_source(['missing']) is None
    assert analyzer._build_shared_legacy_source(['polars_only']) is None

    class _NoRequired(_LegacyPlugin):
        def get_required_columns(self):
            return []

    manager._plugins['no_required'] = _NoRequired('no_required')
    assert analyzer._build_shared_legacy_source(['no_required']) is None

    analyzer_without_manager = TechnicalAnalyzer(frame)
    assert analyzer_without_manager._build_shared_legacy_source(['x']) is None


def test_plugin_hash_and_selection_helpers(frame):
    """参数哈希支持 list/dict/set；已计算插件被过滤掉。"""
    analyzer = TechnicalAnalyzer(frame, plugin_manager=_FakePluginManager({}))

    assert analyzer._plugin_params_hash({'a': [1, 2], 'b': {'x': 1}, 'c': {2, 3}}) == (
        analyzer._plugin_params_hash({'a': [1, 2], 'b': {'x': 1}, 'c': {2, 3}})
    )
    analyzer._mark_plugin_calculated('p', 123)
    analyzer._mark_plugin_calculated('p', hash(()))
    assert analyzer._plugin_cache_hit('p', 123) is True
    assert analyzer._plugin_cache_hit('p', 456) is False
    assert analyzer._select_plugins_to_calculate(['p'], {}) == []
    assert analyzer._select_plugins_to_calculate(['p'], {'windows': 5}) == ['p']


def test_invalidate_plugin_cache_and_stats(monkeypatch, frame):
    """指定/全部插件缓存失效都调用全局缓存 clear；统计接口返回字典。"""
    plugin = _PolarsPlugin('plugin_x')
    manager = _FakePluginManager({'plugin_x': plugin})
    analyzer = TechnicalAnalyzer(frame, plugin_manager=manager)

    cleared = []
    monkeypatch.setattr(
        plugin_ops.global_indicator_cache, 'clear', lambda name=None: cleared.append(name)
    )

    analyzer.invalidate_plugin_cache('plugin_x')
    assert cleared == ['plugin_x']

    analyzer.invalidate_plugin_cache()
    assert cleared[-1] == 'plugin_x'

    stats = analyzer.get_plugin_cache_stats()
    assert set(stats) == {'total_entries', 'total_hits', 'total_size', 'entries'}


def test_plugin_output_column_helpers(frame):
    """插件列声明异常时回退全部列；正常声明裁剪到基列 + 声明列。"""
    analyzer = TechnicalAnalyzer(frame)

    assert analyzer._plugin_required_columns(_PolarsPlugin()) == [
        'date', 'open', 'high', 'low', 'close', 'volume'
    ]

    class _Exploding(_LegacyPlugin):
        def get_required_columns(self):
            raise RuntimeError('声明失败')

    assert analyzer._plugin_required_columns(_Exploding()) == analyzer.pl_df.columns


# ----------------------------------------------------------------------
# indicator_calculator：批量指标分支与错误输入
# ----------------------------------------------------------------------


def test_calculate_multiple_indicators_default_and_lazy(frame):
    """默认全量指标与 LazyFrame 输入分别返回 DataFrame / LazyFrame。"""
    eager = indicator_calculator.calculate_multiple_indicators_polars(frame)

    assert isinstance(eager, pl.DataFrame)
    assert {'ma5', 'macd', 'k14', 'rsi14', 'wr10', 'up20'} <= set(eager.columns)

    lazy = indicator_calculator.calculate_multiple_indicators_polars(frame.lazy(), ['ma'])
    assert isinstance(lazy, pl.LazyFrame)
    assert 'ma5' in lazy.collect().columns


@pytest.mark.parametrize(
    'indicator',
    [
        'ma', 'rsi', 'kdj', 'vol_ma', 'wr', 'boll', 'macd', 'dmi', 'cci',
        'roc', 'mtm', 'obv', 'vr', 'psy', 'trix', 'brar', 'asi', 'emv',
        'mcst', 'dma', 'fsl', 'sar', 'cr', 'expma', 'bbi',
    ],
)
def test_calculate_multiple_indicators_each_indicator(indicator, frame):
    """逐指标批量计算至少产出一列，且不破坏主帧行数。"""
    result = indicator_calculator.calculate_multiple_indicators_polars(
        frame, [indicator]
    )

    assert result.height == frame.height
    assert len(result.columns) > len(frame.columns)


@pytest.mark.parametrize('indicator', ['abi', 'adl', 'adr', 'obos'])
def test_calculate_multiple_indicators_breadth_with_injected_data(indicator, frame):
    """大势型指标需注入涨跌家数数据，注入后正常产出一列。"""
    breadth = pl.DataFrame(
        {
            'trade_date': frame['date'],
            'up_count': np.arange(frame.height, dtype=np.int64) + 10,
            'down_count': np.arange(frame.height, dtype=np.int64) + 5,
            'flat_count': np.zeros(frame.height, dtype=np.int64),
            'total_count': np.arange(frame.height, dtype=np.int64) + 15,
        }
    )
    result = indicator_calculator.calculate_multiple_indicators_polars(
        frame, [indicator], market_breadth=breadth
    )

    assert result.height == frame.height
    assert len(result.columns) > len(frame.columns)


def test_calculate_multiple_indicators_empty_windows_and_missing_column(frame):
    """空窗口不新增列；缺 close 列时抛 Polars 列不存在错误。"""
    empty_windows = indicator_calculator.calculate_multiple_indicators_polars(
        frame, ['ma'], windows=[]
    )
    assert empty_windows.columns == frame.columns

    broken = pl.DataFrame({'date': [1, 2], 'open': [1.0, 2.0]})
    with pytest.raises(pl.exceptions.ColumnNotFoundError):
        indicator_calculator.calculate_multiple_indicators_polars(broken, ['ma'])
