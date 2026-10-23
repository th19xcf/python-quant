#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
update_data —— 数据更新用例。

封装 DataUpdater 的常用路径：批量更新日线、批量算复权因子。
"""

from __future__ import annotations

from typing import Optional
from loguru import logger


def update_stock_data(
    codes: Optional[list] = None,
    update_adj: bool = True,
) -> dict:
    """
    更新股票数据（日线 + 可选复权因子）。

    Args:
        codes:     要更新的股票代码列表。None = 全市场。
        update_adj: 同时重算复权因子（默认 True）。

    Returns:
        dict —— {total, success, failed, details: [...]}
    """
    from src.utils.config import Config
    from src.database.db_manager import DatabaseManager
    from src.data.managers.data_updater import DataUpdater

    config = Config()
    db = DatabaseManager(config)
    db.connect()

    try:
        updater = DataUpdater(db_manager=db)
        result = updater.update_all_stocks(codes=codes)
        logger.info(f"日线更新完成: {result.get('success', 0)}/{result.get('total', 0)}")

        if update_adj:
            from src.data.adj_factor_calculator import AdjFactorCalculator
            calc = AdjFactorCalculator(db_manager=db)
            target_codes = codes or result.get('success_codes', [])
            if target_codes:
                calc.batch_calculate_and_save(target_codes, update_daily=True)
                logger.info(f"复权因子更新完成: {len(target_codes)} 只")

        return result
    finally:
        db.disconnect()
