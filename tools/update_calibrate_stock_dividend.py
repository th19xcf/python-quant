#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
更新并校准 stock_dividend 数据。
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import akshare as ak
from loguru import logger
from sqlalchemy import and_, func, or_

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data.akshare_handler import AkShareHandler
from src.database.db_manager import DatabaseManager
from src.database.models.stock import StockDividend
from src.utils.config import get_config


def _parse_stocks(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    stocks = [item.strip().upper() for item in raw.split(',') if item.strip()]
    return stocks or None


def _clean_negative_values(session) -> int:
    updated = 0
    rows = session.query(StockDividend).filter(
        or_(
            StockDividend.cash_div < 0,
            StockDividend.share_div < 0,
            StockDividend.rights_issue_price < 0,
            StockDividend.rights_issue_ratio < 0,
            StockDividend.total_div < 0,
        )
    ).all()

    for row in rows:
        changed = False
        if row.cash_div is not None and row.cash_div < 0:
            row.cash_div = 0.0
            changed = True
        if row.share_div is not None and row.share_div < 0:
            row.share_div = 0.0
            changed = True
        if row.rights_issue_price is not None and row.rights_issue_price < 0:
            row.rights_issue_price = None
            changed = True
        if row.rights_issue_ratio is not None and row.rights_issue_ratio < 0:
            row.rights_issue_ratio = None
            changed = True
        if row.total_div is not None and row.total_div < 0:
            row.total_div = 0.0
            changed = True
        if changed:
            updated += 1

    return updated


def _dedupe_by_ex_date(session) -> int:
    deleted = 0
    duplicate_groups = (
        session.query(
            StockDividend.ts_code,
            StockDividend.ex_date,
            func.count(StockDividend.id).label("cnt"),
        )
        .filter(StockDividend.ex_date.isnot(None))
        .group_by(StockDividend.ts_code, StockDividend.ex_date)
        .having(func.count(StockDividend.id) > 1)
        .all()
    )

    for ts_code, ex_date, _ in duplicate_groups:
        rows = (
            session.query(StockDividend)
            .filter_by(ts_code=ts_code, ex_date=ex_date)
            .order_by(StockDividend.updated_at.desc(), StockDividend.id.desc())
            .all()
        )
        for row in rows[1:]:
            session.delete(row)
            deleted += 1

    return deleted


def _dedupe_without_ex_date(session) -> int:
    deleted = 0
    duplicate_groups = (
        session.query(
            StockDividend.ts_code,
            StockDividend.dividend_year,
            StockDividend.report_date,
            StockDividend.record_date,
            StockDividend.cash_div,
            StockDividend.share_div,
            StockDividend.total_div,
            StockDividend.rights_issue_price,
            StockDividend.rights_issue_ratio,
            func.count(StockDividend.id).label("cnt"),
        )
        .filter(StockDividend.ex_date.is_(None))
        .group_by(
            StockDividend.ts_code,
            StockDividend.dividend_year,
            StockDividend.report_date,
            StockDividend.record_date,
            StockDividend.cash_div,
            StockDividend.share_div,
            StockDividend.total_div,
            StockDividend.rights_issue_price,
            StockDividend.rights_issue_ratio,
        )
        .having(func.count(StockDividend.id) > 1)
        .all()
    )

    for ts_code, dividend_year, report_date, record_date, cash_div, share_div, total_div, rights_issue_price, rights_issue_ratio, _ in duplicate_groups:
        rows = (
            session.query(StockDividend)
            .filter(
                and_(
                    StockDividend.ts_code == ts_code,
                    StockDividend.dividend_year == dividend_year,
                    StockDividend.report_date == report_date,
                    StockDividend.record_date == record_date,
                    StockDividend.cash_div == cash_div,
                    StockDividend.share_div == share_div,
                    StockDividend.total_div == total_div,
                    StockDividend.rights_issue_price == rights_issue_price,
                    StockDividend.rights_issue_ratio == rights_issue_ratio,
                    StockDividend.ex_date.is_(None),
                )
            )
            .order_by(StockDividend.updated_at.desc(), StockDividend.id.desc())
            .all()
        )
        for row in rows[1:]:
            session.delete(row)
            deleted += 1

    return deleted


def _fill_dividend_year(session) -> int:
    updated = 0
    rows = (
        session.query(StockDividend)
        .filter(StockDividend.dividend_year.is_(None))
        .all()
    )
    for row in rows:
        source_date = row.report_date or row.ex_date or row.record_date
        if source_date:
            row.dividend_year = str(source_date.year)
            updated += 1
    return updated


def _parse_optional_date(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text or text in {"None", "nan", "NaT", "--"}:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return __import__("datetime").datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _parse_optional_float(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        if numeric != numeric:
            return None
        return numeric

    text = str(value).strip().replace(",", "")
    if not text or text in {"None", "nan", "NaN", "--"}:
        return None

    for suffix in ("元", "股"):
        text = text.replace(suffix, "")

    try:
        numeric = float(text)
    except ValueError:
        return None
    if numeric != numeric:
        return None
    return numeric


def _parse_rights_ratio(value):
    if value is None:
        return None

    if isinstance(value, (int, float)):
        numeric = _parse_optional_float(value)
        if numeric is None:
            return None
        # AkShare配股比例通常是“每10股配X股”的X
        return round(numeric / 10.0, 6)

    text = str(value).strip()
    if not text or text in {"None", "nan", "NaN", "--"}:
        return None

    nums = [float(item) for item in re.findall(r"\d+(?:\.\d+)?", text)]
    if not nums:
        return None
    if len(nums) >= 2 and nums[0] > 0:
        return round(nums[1] / nums[0], 6)
    return round(nums[0] / 10.0, 6)


def _build_ts_code(symbol: str) -> str:
    if symbol.startswith("6"):
        return f"{symbol}.SH"
    if symbol.startswith("8") or symbol.startswith("92"):
        return f"{symbol}.BJ"
    return f"{symbol}.SZ"


def _pick_value(row: dict, keys: tuple[str, ...]):
    for key in keys:
        if key in row and row.get(key) not in (None, "", "NaT"):
            return row.get(key)
    return None


def _backfill_rights_issue_from_ak_pg(session, stocks: list[str] | None = None) -> dict:
    try:
        pg_df = ak.stock_pg_em()
    except Exception as exc:
        logger.warning(f"获取AkShare配股总表失败，跳过配股回填: {exc}")
        return {"matched": 0, "updated": 0, "unmatched": 0, "skipped_invalid": 0}

    if pg_df is None or pg_df.empty:
        logger.warning("AkShare配股总表为空，跳过配股回填")
        return {"matched": 0, "updated": 0, "unmatched": 0, "skipped_invalid": 0}

    rows = pg_df.to_dict("records")
    stock_set = set(stocks) if stocks else None

    matched = 0
    updated = 0
    unmatched = 0
    skipped_invalid = 0

    for row in rows:
        symbol_raw = _pick_value(row, ("股票代码", "证券代码"))
        if symbol_raw is None:
            skipped_invalid += 1
            continue

        symbol = str(symbol_raw).strip().zfill(6)
        ts_code = _build_ts_code(symbol)
        if stock_set and ts_code not in stock_set:
            continue

        record_date = _parse_optional_date(_pick_value(row, ("股权登记日",)))
        rights_ratio = _parse_rights_ratio(_pick_value(row, ("配股比例",)))
        rights_price = _parse_optional_float(_pick_value(row, ("配股价", "配股价格")))

        if record_date is None or rights_ratio is None or rights_ratio <= 0:
            skipped_invalid += 1
            continue

        candidate_rows = (
            session.query(StockDividend)
            .filter(StockDividend.ts_code == ts_code)
            .filter(
                or_(
                    StockDividend.record_date == record_date,
                    StockDividend.ex_date == record_date,
                )
            )
            .order_by(StockDividend.updated_at.desc(), StockDividend.id.desc())
            .all()
        )

        if not candidate_rows:
            unmatched += 1
            continue

        target = candidate_rows[0]
        matched += 1

        changed = False
        if (target.rights_issue_ratio or 0.0) <= 0:
            target.rights_issue_ratio = rights_ratio
            changed = True
        if rights_price is not None and rights_price > 0 and (target.rights_issue_price or 0.0) <= 0:
            target.rights_issue_price = rights_price
            changed = True

        if changed:
            updated += 1

    return {
        "matched": matched,
        "updated": updated,
        "unmatched": unmatched,
        "skipped_invalid": skipped_invalid,
    }


def run_calibration(db_manager: DatabaseManager, stocks: list[str] | None = None) -> dict:
    session = db_manager.get_session()

    before_total = session.query(StockDividend).count()
    before_null_ex_date = session.query(StockDividend).filter(StockDividend.ex_date.is_(None)).count()

    fill_year_count = _fill_dividend_year(session)
    rights_backfill_report = _backfill_rights_issue_from_ak_pg(session, stocks)
    dedupe_ex_date_count = _dedupe_by_ex_date(session)
    dedupe_no_ex_date_count = _dedupe_without_ex_date(session)
    clean_negative_count = _clean_negative_values(session)

    session.commit()

    after_total = session.query(StockDividend).count()
    after_null_ex_date = session.query(StockDividend).filter(StockDividend.ex_date.is_(None)).count()

    return {
        "before_total": before_total,
        "after_total": after_total,
        "before_null_ex_date": before_null_ex_date,
        "after_null_ex_date": after_null_ex_date,
        "filled_dividend_year": fill_year_count,
        "rights_backfill_matched": rights_backfill_report["matched"],
        "rights_backfill_updated": rights_backfill_report["updated"],
        "rights_backfill_unmatched": rights_backfill_report["unmatched"],
        "rights_backfill_skipped_invalid": rights_backfill_report["skipped_invalid"],
        "dedupe_by_ex_date": dedupe_ex_date_count,
        "dedupe_without_ex_date": dedupe_no_ex_date_count,
        "clean_negative_rows": clean_negative_count,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="更新并校准 stock_dividend 数据")
    parser.add_argument("--stocks", default="", help="指定股票代码，逗号分隔，例如 600519.SH,000001.SZ")
    parser.add_argument("--skip-update", action="store_true", help="跳过从数据源更新，仅执行数据库校准")
    args = parser.parse_args()

    stocks = _parse_stocks(args.stocks)
    config = get_config()

    db_manager = DatabaseManager(config)
    db_manager.connect()
    try:
        if not args.skip_update:
            logger.info(f"开始更新 stock_dividend, stocks={stocks or 'ALL'}")
            ak_handler = AkShareHandler(config, db_manager)
            ak_handler.update_stock_dividend(stocks)

        report = run_calibration(db_manager, stocks=stocks)
    finally:
        db_manager.disconnect()

    logger.info("stock_dividend 校准完成")
    for key, value in report.items():
        logger.info(f"{key}: {value}")


if __name__ == "__main__":
    main()
