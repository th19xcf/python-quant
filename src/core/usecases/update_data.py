#!/usr/bin/env python3

"""
update_data —— 数据更新用例。

封装 DataService 的常用路径：批量更新日线、批量算复权因子。
"""

from __future__ import annotations

from loguru import logger


def update_stock_data(
    codes: list[str] | None = None,
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
    from src.data.services.data_service import DataService
    from src.database.db_manager import DatabaseManager
    from src.utils.config import Config

    config = Config()
    db = DatabaseManager(config)
    db.connect()

    try:
        service = DataService(config, db_manager=db)
        success = service.update_stock_daily(codes)
        logger.info(f"日线更新完成: success={success}, codes={len(codes) if codes else '全市场'}")

        if update_adj:
            from src.data.adj_factor_calculator import AdjFactorCalculator
            calc = AdjFactorCalculator(db_manager=db)
            if codes:
                calc.batch_calculate_and_save(codes, update_daily=True)
                logger.info(f"复权因子更新完成: {len(codes)} 只")
            else:
                logger.warning("未指定 codes，跳过复权因子重算（全市场请显式传入代码列表）")

        return {
            'total': len(codes) if codes else 0,
            'success': 1 if success else 0,
            'failed': 0 if success else 1,
            'details': [],
        }
    finally:
        db.disconnect()
