#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
重算并落库股票复权因子。
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
from src.utils.config import get_config


def _parse_date(value: str | None):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d")


def main() -> None:
    parser = argparse.ArgumentParser(description="重算并落库复权因子")
    parser.add_argument("--stock", required=True, help="股票代码，例如 600519.SH")
    parser.add_argument("--start", default="", help="开始日期 YYYY-MM-DD，可选")
    parser.add_argument("--end", default="", help="结束日期 YYYY-MM-DD，可选")
    parser.add_argument("--no-update-daily", action="store_true", help="仅更新 stock_adj_factor，不回写 stock_daily")
    args = parser.parse_args()

    config = get_config()
    db_manager = DatabaseManager(config)
    db_manager.connect()

    try:
        tdx_handler = TdxHandler(config, db_manager)
        calculator = AdjFactorCalculator(db_manager=db_manager, tdx_handler=tdx_handler)

        start_date = _parse_date(args.start)
        end_date = _parse_date(args.end)

        logger.info(f"开始重算复权因子: {args.stock}, start={args.start or 'ALL'}, end={args.end or 'ALL'}")
        result = calculator.calculate_adj_factors(
            ts_code=args.stock,
            start_date=start_date,
            end_date=end_date,
            use_tdx=True,
        )

        if result is None or result.is_empty():
            raise RuntimeError("未计算到任何复权数据")

        calculator.save_adj_factors(result)
        if not args.no_update_daily:
            calculator.update_stock_daily_adj(result)

        logger.info(f"重算完成: {args.stock}, records={result.height}")
    finally:
        db_manager.disconnect()


if __name__ == "__main__":
    main()
