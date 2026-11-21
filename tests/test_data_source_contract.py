"""数据源契约测试：所有具体数据源必须实现标准接口。"""

import importlib
import inspect
from datetime import date

import polars as pl
import pytest

from src.data.sources.base import (
    KLINE_STANDARD_COLUMNS,
    REQUIRED_SOURCE_METHODS,
    DataSourceBase,
)

# 需要纳入契约检查的数据源模块
_SOURCE_MODULES = [
    'src.data.baostock_handler',
    'src.data.tdx_handler',
    'src.data.akshare_handler',
    'src.data.tencent.client',
]


def _iter_concrete_subclasses(cls):
    for sub in cls.__subclasses__():
        if not inspect.isabstract(sub):
            yield sub
        yield from _iter_concrete_subclasses(sub)


def test_all_standard_columns_documented():
    """标准列清单必须包含复权因子与昨收，避免归一化漏列。"""
    assert 'pre_close' in KLINE_STANDARD_COLUMNS
    assert 'qfq_factor' in KLINE_STANDARD_COLUMNS
    assert 'hfq_factor' in KLINE_STANDARD_COLUMNS


def _import_source(module_name: str):
    """导入数据源模块；可选依赖（如 baostock）未安装时跳过而非失败。"""
    try:
        return importlib.import_module(module_name)
    except ImportError as e:
        pytest.skip(f'{module_name} 依赖未安装: {e}')


def test_handler_modules_load_without_error():
    for module_name in _SOURCE_MODULES:
        _import_source(module_name)


@pytest.mark.parametrize('module_name', _SOURCE_MODULES)
def test_concrete_source_implements_contract(module_name):
    """每个具体数据源都必须覆盖 get_stock_data / get_index_data。"""
    _import_source(module_name)

    checked = 0
    for cls in _iter_concrete_subclasses(DataSourceBase):
        if cls.__module__ != module_name:
            continue
        checked += 1
        assert cls.missing_contract_methods() == [], (
            f"{cls.__name__} 未实现: {cls.missing_contract_methods()}")

    assert checked > 0, f"{module_name} 中未找到 DataSourceBase 子类"


def test_required_methods_not_inherited_silently():
    """未覆盖契约方法的子类必须被 missing_contract_methods 识别。"""
    class IncompleteSource(DataSourceBase):
        def get_stock_data(self, *args, **kwargs):
            return None

    assert 'get_index_data' in IncompleteSource.missing_contract_methods()
    assert all(m in REQUIRED_SOURCE_METHODS
               for m in IncompleteSource.missing_contract_methods())


def test_tencent_capabilities_match_behavior(monkeypatch):
    """腾讯客户端声明具备的能力必须真能调用，未声明的能力必须显式拒绝。"""
    from src.data.tencent.client import TencentClient

    client = TencentClient()
    assert isinstance(client, DataSourceBase)
    assert client.is_available() is True
    assert TencentClient.missing_contract_methods() == []
    assert TencentClient.CAPABILITIES == frozenset({'daily'})

    # 不具备的能力沿用基类默认桩，不伪造实现
    for stub in ('update_stock_daily', 'update_stock_basic',
                 'get_stock_dividend', 'get_fund_data'):
        assert getattr(TencentClient, stub) is getattr(DataSourceBase, stub)

    captured: dict = {}

    def fake_fetch_daily(ts_code: str, count: int = 800) -> pl.DataFrame:
        captured.update(ts_code=ts_code, count=count)
        return pl.DataFrame({
            'date': [date(2024, 1, 2), date(2024, 1, 5), date(2024, 1, 12)],
            'open': [1.0, 1.0, 1.0], 'high': [1.0, 1.0, 1.0],
            'low': [1.0, 1.0, 1.0], 'close': [1.0, 1.0, 1.0],
            'volume': [1.0, 1.0, 1.0], 'amount': [1.0, 1.0, 1.0],
        })

    monkeypatch.setattr(client, 'fetch_daily', fake_fetch_daily)

    stock = client.get_stock_data('600000.SH', '2024-01-01', '2024-01-10',
                                  adjustment_type='none')
    assert captured['ts_code'] == '600000.SH'
    assert stock['date'].to_list() == [date(2024, 1, 2), date(2024, 1, 5)]

    index = client.get_index_data('000001.SH', '20240101', '20240110')
    assert index['date'].to_list() == [date(2024, 1, 2), date(2024, 1, 5)]

    # 未声明的复权口径与频率必须显式拒绝，而不是静默返回原始价
    with pytest.raises(ValueError, match='不复权'):
        client.get_stock_data('600000.SH', '2024-01-01', '2024-01-10')
    with pytest.raises(ValueError, match='daily'):
        client.get_index_data('000001.SH', '2024-01-01', '2024-01-10', freq='m5')
