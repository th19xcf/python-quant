"""缓存语义隔离与频率转换测试。"""

from datetime import date
from types import SimpleNamespace

import polars as pl
import pytest

from src.data.data_cache import DataCache
from src.data.managers.data_fetcher import DataFetcher
from src.tech_analysis import indicator_cache as ic


def _stock_df():
    return pl.DataFrame({
        'date': ['2024-01-02', '2024-01-03'],
        'close': [1.0, 2.0],
    })


@pytest.fixture
def cache():
    return DataCache()


def test_cross_adjustment_type_is_not_shared(cache):
    cache.set(_stock_df(), 'stock', '600000.SH', '2024-01-01', '2024-12-31',
              frequency='1d', adjustment_type='hfq')

    assert cache.get('stock', '600000.SH', '2024-01-02', '2024-01-03',
                     frequency='1d', adjustment_type='qfq') is None
    assert cache.get('stock', '600000.SH', '2024-01-02', '2024-01-03',
                     frequency='1d', adjustment_type='hfq') is not None


def test_as_of_is_part_of_semantic_key(cache):
    cache.set(_stock_df(), 'stock', '600000.SH', '2024-01-01', '2024-12-31',
              frequency='1d', adjustment_type='qfq', as_of='2024-06-30')

    assert cache.get('stock', '600000.SH', '2024-01-02', '2024-01-03',
                     frequency='1d', adjustment_type='qfq', as_of='2024-06-30') is not None
    assert cache.get('stock', '600000.SH', '2024-01-02', '2024-01-03',
                     frequency='1d', adjustment_type='qfq', as_of=None) is None


def test_frequency_upgrade_no_longer_returns_daily_data(cache):
    cache.set(_stock_df(), 'stock', '600000.SH', '2024-01-01', '2024-12-31',
              frequency='1d', adjustment_type='hfq')

    # 周线请求不能命中日线缓存（旧实现会直接返回日线数据）
    assert cache.get('stock', '600000.SH', '2024-01-02', '2024-01-03',
                     frequency='1w', adjustment_type='hfq') is None


def test_partial_hit_filters_date_typed_column(cache):
    df = pl.DataFrame({
        'date': [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)],
        'close': [1.0, 2.0, 3.0],
    })
    cache.set(df, 'stock', '600000.SH', '2024-01-01', '2024-12-31', frequency='1d')

    out = cache.get('stock', '600000.SH', '2024-01-03', '2024-01-04', frequency='1d')

    assert out is not None
    assert out['date'].to_list() == [date(2024, 1, 3), date(2024, 1, 4)]
    assert cache.get_stats()['partial_hits'] == 1


def test_week_frequency_is_aggregated_not_silently_daily(monkeypatch):
    fetcher = DataFetcher(SimpleNamespace(), None)
    daily = pl.DataFrame({
        'trade_date': [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 8)],
        'open': [1.0, 2.0, 3.0], 'high': [1.0, 2.0, 3.0],
        'low': [1.0, 2.0, 3.0], 'close': [1.0, 2.0, 3.0],
        'volume': [10.0, 20.0, 30.0], 'amount': [100.0, 200.0, 300.0],
    })
    monkeypatch.setattr(fetcher, '_get_data_from_database', lambda *a, **k: daily)

    from src.data.data_cache import global_data_cache
    global_data_cache.clear()

    weekly = fetcher.get_stock_data('600000.SH', '2024-01-01', '2024-01-10',
                                    '1w', 'hfq')

    assert weekly.height == 2
    assert weekly['date'].to_list() == [date(2024, 1, 2), date(2024, 1, 8)]
    assert weekly['close'].to_list() == [2.0, 3.0]

    global_data_cache.clear()


def test_formula_version_invalidates_indicator_cache():
    cache = ic.IndicatorCache()
    data = pl.DataFrame({
        'date': ['2024-01-02', '2024-01-03'],
        'open': [1.0, 2.0], 'high': [1.0, 2.0], 'low': [1.0, 2.0],
        'close': [1.0, 2.0], 'volume': [10.0, 20.0],
    })
    result = pl.DataFrame({'ma5': [9.0, 9.0]})

    try:
        ic.set_formula_version('v1')
        cache.set(data, result, 'ma', period=5)
        assert cache.get(data, 'ma', period=5) is not None

        # 公式版本变化后，相同数据与参数不得命中旧结果
        ic.set_formula_version('v2')
        assert cache.get(data, 'ma', period=5) is None
    finally:
        ic.set_formula_version(None)


def test_formula_version_can_be_passed_explicitly():
    cache = ic.IndicatorCache()
    data = pl.DataFrame({'date': ['2024-01-02'], 'close': [1.0],
                         'open': [1.0], 'high': [1.0], 'low': [1.0],
                         'volume': [10.0]})
    result = pl.DataFrame({'ma5': [1.0]})

    cache.set(data, result, 'ma', period=5, formula_version='plugin-1.0')
    assert cache.get(data, 'ma', period=5, formula_version='plugin-1.0') is not None
    assert cache.get(data, 'ma', period=5, formula_version='plugin-1.1') is None


def test_default_formula_version_is_stable_fingerprint():
    ic.set_formula_version(None)
    first = ic.get_formula_version()
    second = ic.get_formula_version()

    assert first == second
    assert len(first) == 16
