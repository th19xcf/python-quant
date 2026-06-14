#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
批量重算并落库股票复权因子。
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data.adj_factor_calculator import AdjFactorCalculator
from src.data.tdx_handler import TdxHandler
from src.database.db_manager import DatabaseManager
from src.database.models.stock import StockBasic
from src.utils.config import get_config


def _parse_date(value: str | None):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d")


def _parse_stocks(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    stocks = [item.strip().upper() for item in raw.split(",") if item.strip()]
    return stocks or None


def _load_stocks(db_manager: DatabaseManager, stocks: list[str] | None, limit: int) -> list[str]:
    if stocks:
        return stocks[:limit] if limit > 0 else stocks

    session = db_manager.get_session()
    query = session.query(StockBasic.ts_code).order_by(StockBasic.ts_code)
    if limit > 0:
        query = query.limit(limit)
    return [row.ts_code for row in query.all()]


def main() -> None:
    parser = argparse.ArgumentParser(description="批量重算并落库复权因子")
    parser.add_argument("--stocks", default="", help="指定股票代码，逗号分隔，例如 600519.SH,000001.SZ")
    parser.add_argument("--start", default="", help="开始日期 YYYY-MM-DD，可选")
    parser.add_argument("--end", default="", help="结束日期 YYYY-MM-DD，可选")
    parser.add_argument("--limit", type=int, default=0, help="限制处理股票数量，0 表示不限制")
    parser.add_argument("--no-update-daily", action="store_true", help="仅更新 stock_adj_factor，不回写 stock_daily")
    args = parser.parse_args()

    stocks_arg = _parse_stocks(args.stocks)
    start_date = _parse_date(args.start)
    end_date = _parse_date(args.end)

    config = get_config()
    db_manager = DatabaseManager(config)
    db_manager.connect()

    try:
        ts_codes = _load_stocks(db_manager, stocks_arg, args.limit)
        if not ts_codes:
            raise RuntimeError("没有可处理的股票代码")

        logger.info(
            f"开始批量重算复权因子: total={len(ts_codes)}, start={args.start or 'ALL'}, end={args.end or 'ALL'}, update_daily={not args.no_update_daily}"
        )

        tdx_handler = TdxHandler(config, db_manager)
        calculator = AdjFactorCalculator(db_manager=db_manager, tdx_handler=tdx_handler)
        calculator.batch_calculate_and_save(
            ts_codes=ts_codes,
            start_date=start_date,
            end_date=end_date,
            update_daily=not args.no_update_daily,
        )

        logger.info("批量重算复权因子完成")
    finally:
        db_manager.disconnect()


if __name__ == "__main__":
    main()
