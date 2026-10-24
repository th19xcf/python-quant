"""数据源契约测试：所有具体数据源必须实现标准接口。"""

import importlib
import inspect

import pytest

from src.data.sources.base import (DataSourceBase, KLINE_STANDARD_COLUMNS,
                                   REQUIRED_SOURCE_METHODS)

# 需要纳入契约检查的数据源模块
_SOURCE_MODULES = [
    'src.data.baostock_handler',
    'src.data.tdx_handler',
    'src.data.akshare_handler',
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


def test_handler_modules_load_without_error():
    for module_name in _SOURCE_MODULES:
        importlib.import_module(module_name)


@pytest.mark.parametrize('module_name', _SOURCE_MODULES)
def test_concrete_source_implements_contract(module_name):
    """每个具体数据源都必须覆盖 get_stock_data / get_index_data。"""
    importlib.import_module(module_name)

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
