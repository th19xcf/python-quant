#!/usr/bin/env python3

"""数据同步聚合入口的导出契约。"""

import src.data.services.sync_service as sync_service
from src.data.services.sync_fund import sync_stock_fund_to_fund_basic
from src.data.services.sync_index import sync_tdx_index_to_database
from src.data.services.sync_stock import repair_stock_basic, sync_tdx_stock_to_database

EXPECTED_EXPORTS = {
    'sync_tdx_stock_to_database',
    'sync_tdx_index_to_database',
    'sync_stock_fund_to_fund_basic',
    'repair_stock_basic',
}


def test_all_exports_are_declared():
    assert set(sync_service.__all__) == EXPECTED_EXPORTS


def test_exports_point_to_real_implementations():
    assert sync_service.sync_tdx_stock_to_database is sync_tdx_stock_to_database
    assert sync_service.sync_tdx_index_to_database is sync_tdx_index_to_database
    assert sync_service.sync_stock_fund_to_fund_basic is sync_stock_fund_to_fund_basic
    assert sync_service.repair_stock_basic is repair_stock_basic


def test_public_entries_are_callable():
    for name in EXPECTED_EXPORTS:
        assert callable(getattr(sync_service, name))
