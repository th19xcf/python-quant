"""本地列存 → MySQL 日线入库（基金 / 指数）。

与 ``stock_daily_fill``（TDX 直读 → stock_daily）不同，本模块的源数据是
已构建好的本地 Parquet 列存（``data/local_store/quotes/{fund,index}/``），
不重新解析 ``.day``，也不修改列存构建逻辑。

口径与边界：
- 基金：``volume`` 已是手、``amount`` 已是元、价格已按 3 位小数修正，
  本模块**不做任何缩放**，原样写库；
- 指数：``volume`` 为手、``amount`` 为元，原样写库；
- 派生字段统一按 ``index_daily_fill.build_missing_rows`` 的口径生成：
  ``pre_close`` 取本地序列上一交易日 close，序列首行
  ``pre_close/change/pct_chg`` 置 NULL，不沿用列存里被填 0 的首行值；
- 冲突策略：**本地为准（local-wins）**。``(ts_code, trade_date)`` 已存在时
  用本地列存值覆盖 OHLC/量额/派生字段；只覆盖共同日期，不删除仅远程存在的
  日期行。该策略在日志与返回值中显式标注，不做静默双写。
- 基金按代码段分流：沪 500/505 → ``closed_fund_daily``，其余 → ``fund_daily``
  （口径复用 ``sync_fund._classify_local_fund_symbol``，不另建一套映射）；
- 写入走 ``upsert_rows``，重复执行幂等；进度文件记录已完成标的，支持断点续跑。

CLI 入口在 ``src/main.py``：``--sync-fund-daily`` / ``--sync-index-daily``。
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

from loguru import logger

from src.data.atomic_write import write_text_atomic
from src.data.index_daily_fill import build_missing_rows
from src.data.local_store.store import LocalQuoteStore, get_local_quote_store
from src.data.services.sync_fund import _classify_local_fund_symbol
from src.database.models.fund import ClosedFundDaily, FundDaily
from src.database.models.index import IndexDaily
from src.database.upsert import upsert_rows

# 单次写库的行数阈值（跨标的累积，控制单事务体积）
_BATCH_ROWS = 20_000
_VALUE_FIELDS = (
    'open', 'high', 'low', 'close', 'pre_close',
    'change', 'pct_chg', 'vol', 'amount',
)
_RECORD_COLUMNS = ('date', 'open', 'high', 'low', 'close', 'volume', 'amount')
# 冲突策略显式常量：供日志、返回值与报告引用
CONFLICT_POLICY_LOCAL_WINS = 'local-wins'

_TABLE_CONF = {
    'fund_daily': (FundDaily, 'fund'),
    'closed_fund_daily': (ClosedFundDaily, 'fund'),
    'index_daily': (IndexDaily, 'index'),
}


def _default_progress_path(asset: str) -> Path:
    return Path('logs') / 'sync_progress' / f'sync_{asset}_daily.json'


def _load_progress(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        logger.warning(f'进度文件无法解析，按无进度处理: {path}')
        return {}


def _write_progress(path: Path, payload: dict[str, Any]) -> None:
    """原子写进度文件（tmp + replace），避免中断时留下半个 JSON。"""
    write_text_atomic(json.dumps(payload, ensure_ascii=False, indent=2), path)


def _fund_table_for(ts_code: str, unclassified: list[str]) -> str:
    """基金代码 → 目标日线表；无法判定类型的一律落在 fund_daily。"""
    symbol = ts_code.split('.')[0]
    classified = _classify_local_fund_symbol(symbol)
    if classified is None:
        unclassified.append(ts_code)
        return 'fund_daily'
    _fund_type, basic_table = classified
    return 'closed_fund_daily' if basic_table == 'closed_fund_basic' else 'fund_daily'


def _index_basic_codes(db_manager: Any) -> set[str]:
    """指数默认标的池：仅取 ``index_basic`` 中有元数据的代码。

    本地列存另有约 1,200 个 880/881/884 板块指数（通达信自有分类），
    没有 ``index_basic`` 元数据，默认不入库；可用 ``--codes`` 显式指定。
    """
    from src.database.models.index import IndexBasic  # noqa: PLC0415

    session = db_manager.get_session()
    try:
        return {
            ts_code for (ts_code,) in session.query(IndexBasic.ts_code).all()
            if ts_code
        }
    except Exception as exc:  # noqa: BLE001 - 元数据不可读时显式报错
        raise RuntimeError(f'读取 index_basic 代码失败: {exc}') from exc


def _refresh_anomaly_lists_step(store_root: Path) -> dict[str, Any]:
    """末步：刷新异常台账清单；失败只记 warning，不抛给主流程。"""
    from src.data.quality.anomaly_lists import (  # noqa: PLC0415
        refresh_anomaly_lists,
    )

    try:
        result = refresh_anomaly_lists(store_root)
    except Exception as exc:  # noqa: BLE001 - 兜底，确保补数主流程非致命
        logger.warning(f'异常台账清单刷新异常（不影响本次入库）: {exc}')
        return {'status': 'failed', 'error': f'{type(exc).__name__}: {exc}',
                'elapsed_sec': 0.0}
    if result['status'] != 'ok':
        logger.warning(
            f'异常台账清单刷新失败（不影响本次入库）: {result["error"]}')
    return result


def _records_from_frame(frame: Any) -> list[dict[str, Any]]:
    """列存帧 → ``build_missing_rows`` 需要的原始记录（只做列名映射）。"""
    available = [col for col in _RECORD_COLUMNS if col in frame.columns]
    missing = set(_RECORD_COLUMNS) - set(available)
    if missing:
        raise ValueError(f'列存缺少必要列: {sorted(missing)}')
    return frame.select(available).to_dicts()


def _flush(
    session: Any,
    pending: dict[str, list[dict[str, Any]]],
    summary: dict[str, Any],
    dry_run: bool,
) -> bool:
    """把各表待写行一次性提交；返回是否成功（供进度标记）。"""
    total = sum(len(rows) for rows in pending.values())
    if total == 0:
        return True
    if dry_run:
        return True

    try:
        for table, rows in pending.items():
            if not rows:
                continue
            model = _TABLE_CONF[table][0]
            inserted, updated = upsert_rows(
                session, model, rows,
                key_fields=('ts_code', 'trade_date'),
                value_fields=_VALUE_FIELDS,
            )
            stats = summary['tables'][table]
            stats['inserted'] += inserted
            stats['updated'] += updated
            stats['rows_committed'] += len(rows)
        session.commit()
        summary['rows_committed'] += total
        logger.info(f'批次写入完成，共 {total} 行（已提交）')
        return True
    except Exception as e:  # noqa: BLE001 - 单批失败回滚，不拖垮整轮
        session.rollback()
        summary['failed_batches'] += 1
        logger.exception(f'批次写入失败（已回滚 {total} 行）: {e}')
        return False


def _build_code_rows(store: LocalQuoteStore, asset: str, ts_code: str,
                     since: date | None, unclassified: list[str]
                     ) -> tuple[tuple[str, list[dict[str, Any]]] | None, str]:
    """构造单标的待写行，返回 ((表名, 行), 跳过原因)。"""
    frame = store.read_file(asset, ts_code)
    if frame is None or frame.is_empty():
        return None, 'no_file'
    try:
        rows = build_missing_rows(ts_code, _records_from_frame(frame), [])
    except (OSError, ValueError) as e:
        logger.warning(f'{ts_code} 读取/构造失败，跳过: {e}')
        return None, 'read_failed'
    if since is not None:
        rows = [row for row in rows if row['trade_date'] >= since]
    if not rows:
        return None, 'empty_after_since'
    table = ('index_daily' if asset == 'index'
             else _fund_table_for(ts_code, unclassified))
    return (table, rows), ''


def _accumulate(summary: dict[str, Any], table: str, ts_code: str,
                rows: list[dict[str, Any]],
                pending: dict[str, list[dict[str, Any]]],
                pending_codes: list[str]) -> None:
    pending[table].extend(rows)
    pending_codes.append(ts_code)
    summary['codes_processed'] += 1
    summary['rows'] += len(rows)
    stats = summary['tables'][table]
    stats['codes'] += 1
    stats['rows'] += len(rows)
    start, end = rows[0]['trade_date'], rows[-1]['trade_date']
    stats['start'] = start if stats['start'] is None else min(stats['start'], start)
    stats['end'] = end if stats['end'] is None else max(stats['end'], end)


def _finish_batch(
    success: bool,
    summary: dict[str, Any],
    dry_run: bool,
    pending: dict[str, list[dict[str, Any]]],
    pending_codes: list[str],
    done: set[str],
    progress: dict[str, Any],
    progress_file: Path,
    asset: str,
    since: date | None,
) -> None:
    """清空待写缓冲；成功且非 dry-run 时推进进度文件。"""
    if success and not dry_run and pending_codes:
        done.update(pending_codes)
        progress.update({
            'asset': asset,
            'since': since.isoformat() if since else None,
            'conflict_policy': CONFLICT_POLICY_LOCAL_WINS,
            'done_codes': sorted(done),
            'rows': summary['rows'],
            'updated_at': datetime.now().isoformat(timespec='seconds'),
        })
        _write_progress(progress_file, progress)
    for rows in pending.values():
        rows.clear()
    pending_codes.clear()


def _resolve_universe(db_manager: Any, store: LocalQuoteStore, asset: str,
                      ts_codes: Sequence[str] | None) -> tuple[list[str], list[str]]:
    """返回 (待处理代码, 被排除的本地代码)。"""
    if ts_codes:
        return sorted(set(ts_codes)), []
    if asset == 'fund':
        return store.codes('fund'), []
    local = set(store.codes('index'))
    basic = _index_basic_codes(db_manager)
    return sorted(local & basic), sorted(local - basic)


def sync_local_store_daily(
    config: Any,
    db_manager: Any,
    *,
    asset: str,
    ts_codes: Sequence[str] | None = None,
    since: date | None = None,
    dry_run: bool = False,
    batch_rows: int = _BATCH_ROWS,
    progress_path: Path | str | None = None,
    resume: bool = True,
    store: LocalQuoteStore | None = None,
    refresh_lists: bool = False,
) -> dict[str, Any]:
    """把本地列存中的基金 / 指数日线写入 MySQL。

    Args:
        config: 配置对象（解析列存根目录）
        db_manager: 数据库管理器（仅非 dry-run 时使用）
        asset: ``fund`` 或 ``index``
        ts_codes: 显式标的池；None 时用默认池（基金=列存全部基金，
            指数=``index_basic`` 中且有本地列存的代码）
        since: 只写入不早于该日期的行（首行 pre_close 仍取序列上一交易日）
        dry_run: 只统计行数与覆盖区间，不写库、不写进度
        batch_rows: 单批累积行数阈值
        progress_path: 断点续跑进度文件；None 用 ``logs/sync_progress/``
        resume: 是否读取已有进度跳过已完成标的（upsert 幂等，跳过只省时间）
        store: 注入列存（测试用）；None 时按配置解析
        refresh_lists: 写入完成后是否刷新异常台账清单（默认关；日常 CLI 默认开）

    Returns:
        统计字典，含每张表的标的数 / 行数 / 新增 / 更新 / 日期区间、
        跳过原因、冲突策略与耗时。
    """
    if asset not in ('fund', 'index'):
        raise ValueError(f'不支持的 asset: {asset}')

    store = store or get_local_quote_store(config)
    if not store.enabled:
        raise RuntimeError(
            '本地列存未启用（data.local_store_enabled=false），无法执行入库同步')

    started = datetime.now()
    unclassified: list[str] = []
    codes, excluded = _resolve_universe(db_manager, store, asset, ts_codes)

    summary: dict[str, Any] = {
        'asset': asset,
        'dry_run': dry_run,
        'conflict_policy': CONFLICT_POLICY_LOCAL_WINS,
        'codes_total': len(codes),
        'codes_processed': 0,
        'codes_resumed': 0,
        'excluded_local_codes': len(excluded) if asset == 'index' else 0,
        'excluded_sample': excluded[:10],
        'unclassified_funds': 0,
        'unclassified_sample': [],
        'rows': 0,
        'rows_committed': 0,
        'failed_batches': 0,
        'skipped_reasons': {
            'no_file': 0,
            'empty_after_since': 0,
            'read_failed': 0,
        },
        'tables': {
            table: {'codes': 0, 'rows': 0, 'rows_committed': 0,
                    'inserted': 0, 'updated': 0, 'start': None, 'end': None}
            for table in _TABLE_CONF
        },
    }
    if not codes:
        logger.warning(f'{asset} 入库：标的池为空，未执行任何操作')
        summary['anomaly_lists'] = {'status': 'skipped',
                                    'reason': 'empty_universe'}
        return summary

    if asset == 'index':
        logger.info(
            'index_daily 冲突策略=local-wins（按 ts_code+trade_date 用本地列存覆盖，'
            f'不删除仅远程存在的日期）；默认池=index_basic ∩ 本地列存 {len(codes)} 只，'
            f'排除本地无元数据板块指数 {len(excluded)} 只'
        )
    else:
        logger.info(
            f'基金入库：标的 {len(codes)} 只，冲突策略=local-wins；'
            'vol=手/amount=元/价格按 3 位小数原样写入，不做任何缩放'
        )

    progress_file = Path(progress_path) if progress_path else _default_progress_path(asset)
    progress: dict[str, Any] = {}
    done: set[str] = set()
    if resume and not dry_run:
        progress = _load_progress(progress_file)
        same_scope = (
            progress.get('asset') == asset
            and progress.get('since') == (since.isoformat() if since else None)
        )
        if progress and not same_scope:
            logger.warning(
                f'进度文件 {progress_file} 的 asset/since 与本次不一致，忽略旧进度')
        elif progress:
            done = set(progress.get('done_codes') or [])
            logger.info(f'读取进度 {progress_file}：已完成 {len(done)} 只，续跑')

    session = None if dry_run else db_manager.get_session()
    pending: dict[str, list[dict[str, Any]]] = {table: [] for table in _TABLE_CONF}
    pending_rows = 0
    pending_codes: list[str] = []

    try:
        for ts_code in codes:
            if ts_code in done:
                summary['codes_resumed'] += 1
                continue

            built, reason = _build_code_rows(
                store, asset, ts_code, since, unclassified)
            if built is None:
                summary['skipped_reasons'][reason] += 1
                continue

            table, rows = built
            _accumulate(summary, table, ts_code, rows, pending, pending_codes)
            pending_rows += len(rows)

            if pending_rows >= batch_rows:
                ok = _flush(session, pending, summary, dry_run)
                _finish_batch(ok, summary, dry_run, pending, pending_codes,
                              done, progress, progress_file, asset, since)
                pending_rows = 0

        ok = _flush(session, pending, summary, dry_run)
        _finish_batch(ok, summary, dry_run, pending, pending_codes,
                      done, progress, progress_file, asset, since)
    finally:
        if session is not None:
            session.close()

    summary['unclassified_funds'] = len(unclassified)
    summary['unclassified_sample'] = unclassified[:10]
    if refresh_lists and not dry_run:
        summary['anomaly_lists'] = _refresh_anomaly_lists_step(store.root)
    else:
        summary['anomaly_lists'] = {
            'status': 'skipped',
            'reason': 'dry-run' if dry_run else 'disabled',
        }
    summary['elapsed_sec'] = round((datetime.now() - started).total_seconds(), 2)
    summary['progress_path'] = str(progress_file)
    logger.info(
        f'{asset} 入库完成{"（dry-run）" if dry_run else ""}：'
        f'标的 {summary["codes_processed"]}/{summary["codes_total"]}，'
        f'行 {summary["rows"]}，表 {summary["tables"]}，'
        f'跳过 {summary["skipped_reasons"]}，'
        f'清单刷新 {summary["anomaly_lists"]["status"]} '
        f'({summary["anomaly_lists"].get("elapsed_sec", 0.0)}s)，'
        f'总耗时 {summary["elapsed_sec"]}s'
    )
    return summary


__all__ = (
    'CONFLICT_POLICY_LOCAL_WINS',
    'sync_local_store_daily',
)
