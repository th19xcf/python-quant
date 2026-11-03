"""TDX → stock_daily 补库（离线全量/增量，幂等可重复执行）。

选型理由：放在 ``services`` 而非 ``tools``。补库需要「本地 .day 解析 +
库内水位查询 + 批量 upsert」三件套，并且未来要接定时调度；``tools/`` 现有
脚本都是一次性、写死标的的修补脚本，不适合承载可重复执行的批量入口。
CLI 入口在 ``src/main.py``（``--sync-stock-daily``），可直接调用本模块。

口径：
- 只读本地通达信 ``.day`` 文件，不依赖任何远程数据源，离线可完整运行；
- 唯一键 ``(ts_code, trade_date)`` 由 ``upsert_rows`` 命中，重复执行结果幂等；
- 全量：按 TDX 全历史建行（已存在行被相同值覆盖）；增量：以库内每只股票
  最新交易日为水位，只补水位之后（保留紧邻水位的一根做 pre_close 种子）；
- 价格/量纲沿用 TDX 解析契约：vol=手（个股股→手）、amount=元，
  与 DataFetcher / upsert 的单位契约一致，本模块不做任何二次换算。
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from pathlib import Path
from typing import Any

from loguru import logger
from sqlalchemy import func

from src.data.index_daily_fill import build_missing_rows
from src.data.tdx_day_parser import read_recent_records
from src.database.models.stock import StockDaily
from src.database.upsert import upsert_rows
from src.utils.code_converter import is_fund_code, is_index_code, to_tdx_file_stem
from src.utils.exceptions import DataSourceException

# 每批处理的股票数与写库行数上限（避免单事务过大）
_BATCH_STOCKS = 50
_VALUE_FIELDS = (
    'open', 'high', 'low', 'close', 'pre_close',
    'change', 'pct_chg', 'vol', 'amount',
)


def default_stock_universe(config: Any) -> list[str]:
    """从本地通达信目录枚举股票代码（排除指数与基金），按 ts_code 升序。"""
    from src.data.tdx_handler import TdxHandler

    handler = TdxHandler(config, None)
    if not handler.available:
        raise DataSourceException(
            'stock_daily_fill',
            f"通达信数据路径不存在: {handler.tdx_data_path}，无法离线补库",
        )
    codes = [
        code for code in handler.get_stock_list()
        if not is_index_code(code) and not is_fund_code(code)
    ]
    return sorted(set(codes))


def _day_file_path(tdx_data_path: Path, ts_code: str) -> Path:
    """定位股票对应的通达信日线文件（如 sh600000.day）。"""
    stem = to_tdx_file_stem(ts_code)
    return tdx_data_path / stem[:2] / 'lday' / f"{stem}.day"


def _latest_trade_dates(db_manager: Any) -> dict[str, date]:
    """一次分组查询拿到每只股票库内最新交易日（增量水位）。"""
    session = db_manager.get_session()
    rows = (
        session.query(StockDaily.ts_code, func.max(StockDaily.trade_date))
        .group_by(StockDaily.ts_code)
        .all()
    )
    return {ts_code: latest for ts_code, latest in rows if latest is not None}


def _incremental_window(records: Sequence[dict[str, Any]],
                        water_mark: date | None) -> tuple[list[dict[str, Any]], list[date]]:
    """按水位裁剪记录，并返回「视为已存在」的日期。

    保留紧邻水位的那一根记录作为 pre_close 种子，保证增量首行的
    pre_close/change/pct_chg 与全量口径完全一致。
    """
    ordered = sorted(records, key=lambda item: item['date'])
    if water_mark is None:
        return ordered, []

    first_new = next(
        (index for index, record in enumerate(ordered) if record['date'] > water_mark),
        len(ordered),
    )
    if first_new == len(ordered):
        return [], []

    window = ordered[max(0, first_new - 1):]
    existing = [record['date'] for record in window if record['date'] <= water_mark]
    return window, existing


def _flush_batch(session: Any, pending: list[dict[str, Any]],
                 summary: dict[str, Any], dry_run: bool) -> None:
    """写入一批待补行；dry-run 时只累计行数。"""
    if not pending:
        return
    if dry_run:
        summary['rows'] += len(pending)
        logger.info(f"[dry-run] 批次行数 {len(pending)}，累计 {summary['rows']}")
        return

    try:
        inserted, updated = upsert_rows(
            session, StockDaily, pending,
            key_fields=('ts_code', 'trade_date'),
            value_fields=_VALUE_FIELDS,
        )
        session.commit()
        summary['rows'] += len(pending)
        summary['inserted'] += inserted
        summary['updated'] += updated
        logger.info(
            f"批次写入 行 {len(pending)}（新增 {inserted} / 更新 {updated}），"
            f"累计行 {summary['rows']}"
        )
    except Exception as e:  # noqa: BLE001 - 单批失败不拖垮整轮补库
        session.rollback()
        summary['failed_batches'] += 1
        summary['skipped_reasons']['write_failed'] += len(pending)
        logger.exception(f"批次写入失败（已回滚本批 {len(pending)} 行）: {e}")


def fill_stock_daily(
    config: Any,
    db_manager: Any,
    *,
    ts_codes: Sequence[str] | None = None,
    full: bool = False,
    dry_run: bool = False,
    since: date | None = None,
    max_date: date | None = None,
    batch_stocks: int = _BATCH_STOCKS,
) -> dict[str, Any]:
    """把本地 TDX 日线补入 ``stock_daily``。

    Args:
        config: 配置对象（读取 ``data.tdx_data_path``）
        db_manager: 数据库管理器
        ts_codes: 显式股票池；None 时枚举本地全部股票（排除指数/基金）
        full: True 为全量（全历史），False 为增量（库内最新交易日之后）
        dry_run: 只统计不写库
        since: 只写入不早于该日期的记录（用于库内水位缺失时的有界回填）
        max_date: 只写入不晚于该日期的记录（用于避开盘中未收盘 K 线）
        batch_stocks: 每批股票数

    Returns:
        统计字典：模式、股票数、行数、新增/更新数、跳过原因分布。
    """
    from src.data.tdx_handler import TdxHandler

    handler = TdxHandler(config, None)
    if not handler.available:
        raise DataSourceException(
            'stock_daily_fill',
            f"通达信数据路径不存在: {handler.tdx_data_path}，无法离线补库",
        )

    codes = sorted(set(ts_codes)) if ts_codes else default_stock_universe(config)

    summary: dict[str, Any] = {
        'mode': 'full' if full else 'incremental',
        'dry_run': dry_run,
        'stocks_total': len(codes),
        'stocks_processed': 0,
        'rows': 0,
        'inserted': 0,
        'updated': 0,
        'failed_batches': 0,
        'skipped_reasons': {
            'no_file': 0,
            'empty_file': 0,
            'no_new_rows': 0,
            'read_failed': 0,
            'write_failed': 0,
        },
    }
    if not codes:
        logger.warning("stock_daily 补库：股票池为空，未执行任何操作")
        return summary

    watermarks: dict[str, date] = {} if full else _latest_trade_dates(db_manager)

    session = None if dry_run else db_manager.get_session()
    pending: list[dict[str, Any]] = []
    pending_stocks = 0

    try:
        for index, ts_code in enumerate(codes, start=1):
            file_path = _day_file_path(handler.tdx_data_path, ts_code)
            if not file_path.exists():
                summary['skipped_reasons']['no_file'] += 1
                logger.debug(f"{ts_code} 无本地 .day 文件，跳过")
                continue

            try:
                records = read_recent_records(file_path)
            except (OSError, ValueError) as e:
                summary['skipped_reasons']['read_failed'] += 1
                logger.warning(f"{ts_code} 解析 {file_path} 失败，跳过: {e}")
                continue

            if max_date is not None:
                records = [r for r in records if r['date'] <= max_date]
            if not records:
                summary['skipped_reasons']['empty_file'] += 1
                continue

            window, existing = _incremental_window(records, watermarks.get(ts_code))
            if since is not None:
                # 有界回填：保留 since 前一根作为 pre_close 种子并标记为既有，
                # 保证首行的 pre_close/change/pct_chg 与全量口径一致。
                seed = [r for r in window if r['date'] < since][-1:]
                window = seed + [r for r in window if r['date'] >= since]
                existing = list(existing) + [r['date'] for r in seed]
                if len(window) == len(seed):
                    summary['skipped_reasons']['no_new_rows'] += 1
                    continue
            rows = build_missing_rows(ts_code, window, existing)
            if not rows:
                summary['skipped_reasons']['no_new_rows'] += 1
                continue

            pending.extend(rows)
            pending_stocks += 1
            summary['stocks_processed'] += 1

            if pending_stocks >= batch_stocks:
                _flush_batch(session, pending, summary, dry_run)
                pending = []
                pending_stocks = 0

        _flush_batch(session, pending, summary, dry_run)

        logger.info(
            f"stock_daily 补库完成（{summary['mode']}"
            f"{'，dry-run' if dry_run else ''}）："
            f"股票 {summary['stocks_total']} 只，覆盖 {summary['stocks_processed']} 只，"
            f"行 {summary['rows']}，新增 {summary['inserted']}，更新 {summary['updated']}，"
            f"跳过 {summary['skipped_reasons']}"
        )
        return summary
    finally:
        if session is not None:
            session.close()
