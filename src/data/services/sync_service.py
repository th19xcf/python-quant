#!/usr/bin/env python3

"""
数据同步服务模块（公开聚合入口）

从 main.py 抽离的股票/指数/基金同步业务逻辑，按业务线拆分到：
    - sync_common.py  同步流程骨架、代码判定、字段归一化、通达信日线定位
    - sync_stock.py   股票基本信息同步
    - sync_index.py   指数基本信息同步
    - sync_fund.py    基金基本信息同步

本模块只对外暴露三条公开同步入口，内部实现细节（`_` 前缀函数）由各子模块
自行持有，不再经此聚合导出，避免对外暴露私有实现。
"""

from src.data.services.sync_fund import (
    backfill_fund_basic_from_local_tdx,
    sync_stock_fund_to_fund_basic,
)
from src.data.services.sync_index import sync_tdx_index_to_database
from src.data.services.sync_stock import repair_stock_basic, sync_tdx_stock_to_database

__all__ = [
    "sync_tdx_stock_to_database",
    "sync_tdx_index_to_database",
    "sync_stock_fund_to_fund_basic",
    "backfill_fund_basic_from_local_tdx",
    "repair_stock_basic",
]
