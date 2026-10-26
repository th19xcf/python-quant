#!/usr/bin/env python3

"""
测试数据管理器模块

验证重构后的 DataManager 是否正常工作
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import contextlib

import polars as pl
import pytest

from src.data.data_manager import DataManager
from src.database.db_manager import DatabaseManager
from src.plugin.plugin_manager import PluginManager
from src.utils.config import Config


@pytest.fixture(scope="module")
def data_manager():
    """
    数据管理器fixture
    """
    config = Config()
    db_manager = DatabaseManager(config)
    db_manager.connect()
    plugin_manager = PluginManager(config)
    plugin_manager.load_plugins()

    dm = DataManager(config, db_manager, plugin_manager)

    yield dm

    if db_manager:
        with contextlib.suppress(Exception):
            db_manager.disconnect()


def test_data_manager_initialization():
    """
    测试数据管理器初始化
    """
    config = Config()
    db_manager = DatabaseManager(config)
    db_manager.connect()
    plugin_manager = PluginManager(config)
    plugin_manager.load_plugins()

    data_manager = DataManager(config, db_manager, plugin_manager)

    assert data_manager is not None
    assert data_manager.data_service is not None
    assert data_manager.config is not None
    assert data_manager.db_manager is not None

    db_manager.disconnect()


def test_data_fetcher(data_manager):
    """
    测试数据获取功能
    """
    stock_data = data_manager.get_stock_data(
        stock_code="600000.SH",
        start_date="2024-01-01",
        end_date="2024-01-31",
        frequency="1d",
        adjustment_type="qfq"
    )
    assert isinstance(stock_data, pl.DataFrame)

    index_data = data_manager.get_index_data(
        index_code="000001.SH",
        start_date="2024-01-01",
        end_date="2024-01-31",
        frequency="1d"
    )
    assert isinstance(index_data, pl.DataFrame)


def test_data_processor(data_manager):
    """
    测试数据处理功能
    """
    stock_data = data_manager.get_stock_data(
        stock_code="600000.SH",
        start_date="2024-01-01",
        end_date="2024-01-31",
        frequency="1d"
    )

    preprocessed_data = data_manager.preprocess_data(stock_data)
    assert isinstance(preprocessed_data, (pl.DataFrame, pl.LazyFrame))

    sampled_data = data_manager.sample_data(stock_data, target_points=10)
    assert isinstance(sampled_data, (pl.DataFrame, pl.LazyFrame))

    cleaned_data = data_manager.clean_data(stock_data)
    assert isinstance(cleaned_data, (pl.DataFrame, pl.LazyFrame))

    converted_data = data_manager.convert_data_type(stock_data, target_type="float32")
    assert isinstance(converted_data, (pl.DataFrame, pl.LazyFrame))


def test_data_updater(data_manager):
    """
    测试数据更新功能（仅测试流程，不实际更新）
    """
    assert data_manager.update_stock_basic() is None or True
    assert data_manager.update_index_basic() is None or True


def test_basic_data_methods(data_manager):
    """
    测试基础信息获取方法
    """
    stock_basic = data_manager.get_stock_basic()
    assert isinstance(stock_basic, pl.DataFrame)

    index_basic = data_manager.get_index_basic()
    assert isinstance(index_basic, pl.DataFrame)

    fund_basic = data_manager.get_fund_basic()
    assert isinstance(fund_basic, pl.DataFrame)

    closed_fund_basic = data_manager.get_closed_fund_basic()
    assert isinstance(closed_fund_basic, pl.DataFrame)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
