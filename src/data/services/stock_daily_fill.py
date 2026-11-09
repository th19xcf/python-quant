"""TDX → stock_daily 补库（离线全量/增量，幂等可重复执行）。

选型理由：放在 ``services`` 而非 ``tools``。补库需要「本地 .day 解析 +
库内水位查询 + 批量 upsert」三件套，并且未来要接定时调度；``tools/`` 现有
脚本都是一次性、写死标的的修补脚本，不适合承载可重复执行的批量入口。
CLI 入口在 ``src/main.py``（``--sync-stock-daily``），可直接调用本模块。

口径：
- 取数源可切换（``source``）：``auto``（默认，网络源优先、失败回退本地
  ``.day``）、``local``（只读本地）、``mootdx``（只走网络，便于对拍）；
  网络源与本地 ``.day`` 的 5 日 OHLCV 对拍偏差 0/1458 字段，口径一致；
- 唯一键 ``(ts_code, trade_date)`` 由 ``upsert_rows`` 命中，重复执行结果幂等；
- 全量：按 TDX 全历史建行（已存在行被相同值覆盖）；增量：以库内每只股票
  最新交易日为水位，只补水位之后（保留紧邻水位的一根做 pre_close 种子）；
  全量模式固定走本地 ``.day``（网络源按页取全历史代价过高，且股票池本身
  由本地目录枚举）；
- 价格/量纲沿用 TDX 解析契约：vol=手（个股股→手）、amount=元，
  与 DataFetcher / upsert 的单位契约一致，本模块不做任何二次换算。
"""

from __future__ import annotations

import json
import os
from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

from loguru import logger
from sqlalchemy import func

from src.data.index_daily_fill import build_missing_rows
from src.data.services.tdx_online_source import TdxOnlineClient, TdxOnlineError
from src.data.tdx_day_parser import read_first_last_trade_date, read_recent_records
from src.database.models.stock import StockDaily
from src.database.upsert import upsert_rows
from src.utils.code_converter import is_fund_code, is_index_code, to_tdx_file_stem
from src.utils.exceptions import DataSourceException

# 每批处理的股票数与写库行数上限（避免单事务过大）
_BATCH_STOCKS = 50
# 全历史回补是单次 1,700 万行级的大批量任务：调大 upsert 分块，
# 把网络往返次数压到默认值的 1/5（实测吞吐 736 → 1,970 行/秒）。
_UPSERT_WRITE_CHUNK = 2000
_UPSERT_PRELOAD_CHUNK = 1000
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


def _coverage_ranges(db_manager: Any) -> dict[str, tuple[date, date]]:
    """一次分组查询拿到每只股票库内 (最早, 最晚) 交易日。

    全量模式用它判断「该股票前缀已完整」：库内最早 <= 源端最早且
    库内最晚 >= 源端最晚时无需重扫；否则（如仅有 2026 年有界回填）
    仍需全历史重建。
    """
    session = db_manager.get_session()
    rows = (
        session.query(
            StockDaily.ts_code,
            func.min(StockDaily.trade_date),
            func.max(StockDaily.trade_date),
        )
        .group_by(StockDaily.ts_code)
        .all()
    )
    result: dict[str, tuple[date, date]] = {}
    for ts_code, first, last in rows:
        first_date = _as_date(first)
        last_date = _as_date(last)
        if first_date is not None and last_date is not None:
            result[ts_code] = (first_date, last_date)
    return result


def _as_date(value: Any) -> date | None:
    """兼容 MySQL DATE / SQLite 文本的日期归一。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)[:10]
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _default_progress_path(mode: str) -> Path:
    return Path('logs') / 'sync_progress' / f'stock_daily_{mode}.json'


def _load_progress(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        logger.warning(f'进度文件无法解析，按无进度处理: {path}')
        return {}


def _write_progress(path: Path, payload: dict[str, Any]) -> None:
    """原子写进度文件（tmp + replace）。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.json.tmp')
    tmp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    os.replace(tmp, path)


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
                 summary: dict[str, Any], dry_run: bool) -> bool:
    """写入一批待补行；返回是否成功（用于推进断点进度）。"""
    if not pending:
        return True
    if dry_run:
        summary['rows'] += len(pending)
        logger.info(f"[dry-run] 批次行数 {len(pending)}，累计 {summary['rows']}")
        return True

    try:
        inserted, updated = upsert_rows(
            session, StockDaily, pending,
            key_fields=('ts_code', 'trade_date'),
            value_fields=_VALUE_FIELDS,
            write_chunk=_UPSERT_WRITE_CHUNK,
            preload_chunk=_UPSERT_PRELOAD_CHUNK,
        )
        session.commit()
        summary['rows'] += len(pending)
        summary['inserted'] += inserted
        summary['updated'] += updated
        logger.info(
            f"批次写入 行 {len(pending)}（新增 {inserted} / 更新 {updated}），"
            f"累计行 {summary['rows']}"
        )
        return True
    except Exception as e:  # noqa: BLE001 - 单批失败不拖垮整轮补库
        session.rollback()
        summary['failed_batches'] += 1
        summary['skipped_reasons']['write_failed'] += len(pending)
        logger.exception(f"批次写入失败（已回滚本批 {len(pending)} 行）: {e}")
        return False


def _mark_progress(
    success: bool,
    done: set[str],
    progress_codes: list[str],
    summary: dict[str, Any],
    progress: dict[str, Any],
    progress_file: Path | None,
    dry_run: bool,
    since: date | None,
    max_date: date | None,
) -> None:
    """把本批成功处理的股票写入进度文件；失败批次不标记，便于续跑重试。"""
    if success and not dry_run and progress_codes and progress_file is not None:
        done.update(progress_codes)
        progress.update({
            'mode': summary['mode'],
            'since': since.isoformat() if since else None,
            'max_date': max_date.isoformat() if max_date else None,
            'done_codes': sorted(done),
            'rows': summary['rows'],
            'updated_at': datetime.now().isoformat(timespec='seconds'),
        })
        _write_progress(progress_file, progress)
    progress_codes.clear()


def _build_rows(
    ts_code: str,
    records: Sequence[dict[str, Any]],
    *,
    watermarks: dict[str, date],
    since: date | None,
    max_date: date | None,
) -> tuple[list[dict[str, Any]] | None, str]:
    """把原始日线记录裁剪成待补行；返回 (行, 跳过原因)。"""
    if max_date is not None:
        records = [r for r in records if r['date'] <= max_date]
    if not records:
        return None, 'empty_file'

    window, existing = _incremental_window(records, watermarks.get(ts_code))
    if since is not None:
        # 有界回填：保留 since 前一根作为 pre_close 种子并标记为既有，
        # 保证首行的 pre_close/change/pct_chg 与全量口径一致。
        seed = [r for r in window if r['date'] < since][-1:]
        window = seed + [r for r in window if r['date'] >= since]
        existing = list(existing) + [r['date'] for r in seed]
        if len(window) == len(seed):
            return None, 'no_new_rows'
    rows = build_missing_rows(ts_code, window, existing)
    if not rows:
        return None, 'no_new_rows'
    return rows, ''


def _collect_rows(
    tdx_data_path: Path,
    ts_code: str,
    *,
    full: bool,
    coverage: dict[str, tuple[date, date]],
    watermarks: dict[str, date],
    since: date | None,
    max_date: date | None,
) -> tuple[list[dict[str, Any]] | None, str]:
    """构造单只股票待补行；返回 (行, 跳过原因)。

    原因取值：no_file / already_complete / empty_file / read_failed /
    no_new_rows；成功时原因为空串。``already_complete`` 由调用方记入进度。
    """
    file_path = _day_file_path(tdx_data_path, ts_code)
    if not file_path.exists():
        return None, 'no_file'

    if full and ts_code in coverage:
        src_first, src_last = read_first_last_trade_date(file_path)
        if max_date is not None and src_last is not None:
            src_last = min(src_last, max_date)
        db_first, db_last = coverage[ts_code]
        if (src_first is not None and src_last is not None
                and db_first <= src_first and db_last >= src_last):
            return None, 'already_complete'

    try:
        records = read_recent_records(file_path)
    except (OSError, ValueError) as e:
        logger.warning(f"{ts_code} 解析 {file_path} 失败，跳过: {e}")
        return None, 'read_failed'

    return _build_rows(
        ts_code, records, watermarks=watermarks, since=since, max_date=max_date)


def _online_records(
    client: TdxOnlineClient,
    ts_code: str,
    *,
    watermarks: dict[str, date],
    since: date | None,
    max_date: date | None = None,
) -> list[dict[str, Any]]:
    """取网络源原始日线；库内无水位且无 since 时无法保证历史覆盖，交给本地源。"""
    anchor = watermarks.get(ts_code)
    if since is not None and (anchor is None or since < anchor):
        anchor = since
    if anchor is None:
        raise TdxOnlineError(
            'no_watermark', '库内无水位，网络源无法保证全历史覆盖，改用本地 .day')
    # 日历日数 >= 交易日数，留 5 根余量；单页最多 800 根，超出则交给本地全历史
    end = min(date.today(), max_date) if max_date is not None else date.today()
    needed = (end - anchor).days + 5
    if needed > 800:
        raise TdxOnlineError(
            'range_too_large', f'需回补 {needed} 个自然日，超过网络源单页上限')
    return [quote.as_record()
            for quote in client.fetch_daily(ts_code, count=max(20, needed))]


def _collect_rows_with_source(
    tdx_data_path: Path,
    ts_code: str,
    *,
    source: str,
    client: TdxOnlineClient | None,
    full: bool,
    coverage: dict[str, tuple[date, date]],
    watermarks: dict[str, date],
    since: date | None,
    max_date: date | None,
) -> tuple[list[dict[str, Any]] | None, str, str, str]:
    """按取数源构造待补行。

    Returns:
        (行, 跳过原因, 数据来源标记, 回退原因)
        来源标记：``network`` / ``local`` / ``local_fallback`` / ``failed``
        （网络单独失败或网络与本地均失败）。
    """
    if source == 'local' or full or client is None:
        rows, reason = _collect_rows(
            tdx_data_path, ts_code, full=full, coverage=coverage,
            watermarks=watermarks, since=since, max_date=max_date)
        return rows, reason, 'local', ''

    try:
        records = _online_records(
            client, ts_code, watermarks=watermarks, since=since, max_date=max_date)
    except TdxOnlineError as exc:
        if source == 'mootdx':
            return None, 'network_failed', 'failed', exc.reason
        rows, reason = _collect_rows(
            tdx_data_path, ts_code, full=full, coverage=coverage,
            watermarks=watermarks, since=since, max_date=max_date)
        if rows is None and reason in ('no_file', 'empty_file', 'read_failed'):
            return None, 'source_failed', 'failed', exc.reason
        return rows, reason, 'local_fallback', exc.reason

    rows, reason = _build_rows(
        ts_code, records, watermarks=watermarks, since=since, max_date=max_date)
    return rows, reason, 'network', ''


def _source_manifest_path(
    progress_file: Path | None, mode: str, override: Path | str | None
) -> Path:
    if override is not None:
        return Path(override)
    parent = progress_file.parent if progress_file else Path('logs') / 'sync_progress'
    return parent / f'stock_daily_sources_{mode}.json'


def _account_source(
    provenance_map: dict[str, str],
    stats: dict[str, Any],
    ts_code: str,
    provenance: str,
    fallback_reason: str,
) -> None:
    """按来源标记累计统计；回退原因按类型计数。"""
    provenance_map[ts_code] = provenance
    if provenance == 'network':
        stats['network'] += 1
    elif provenance == 'local_fallback':
        stats['local_fallback'] += 1
        reasons = stats['fallback_reasons']
        reasons[fallback_reason] = reasons.get(fallback_reason, 0) + 1
    elif provenance == 'failed':
        stats['failed'] += 1
    else:
        stats['local'] += 1


def _log_skip(ts_code: str, reason: str, provenance: str,
              fallback_reason: str) -> None:
    if provenance == 'local_fallback':
        logger.warning(f'{ts_code} 网络源失败（{fallback_reason}），已回退本地 .day')
    elif provenance == 'failed':
        logger.error(
            f'{ts_code} 网络源（{fallback_reason}）与本地源（{reason}）均失败')


def _write_source_manifest(
    progress_file: Path | None,
    override: Path | str | None,
    summary: dict[str, Any],
    provenance_map: dict[str, str],
    source: str,
) -> str:
    manifest = _source_manifest_path(progress_file, summary['mode'], override)
    _write_progress(manifest, {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'mode': summary['mode'],
        'source_requested': source,
        'source_effective': summary['source_effective'],
        'source_stats': summary['source_stats'],
        'per_code': provenance_map,
    })
    logger.info(
        f"取数来源：网络 {summary['source_stats']['network']} 只，"
        f"本地回退 {summary['source_stats']['local_fallback']} 只"
        f"（{summary['source_stats']['fallback_reasons']}），"
        f"本地 {summary['source_stats']['local']} 只，"
        f"失败 {summary['source_stats']['failed']} 只；清单 {manifest}"
    )
    return str(manifest)


def _load_resume_state(
    progress_file: Path | None,
    mode: str,
    since: date | None,
    max_date: date | None,
) -> tuple[dict[str, Any], set[str]]:
    """读取续跑进度；mode/since/max_date 不一致时忽略旧进度。"""
    if progress_file is None:
        return {}, set()
    progress = _load_progress(progress_file)
    if not progress:
        return {}, set()
    same_scope = (
        progress.get('mode') == mode
        and progress.get('since') == (since.isoformat() if since else None)
        and progress.get('max_date') == (max_date.isoformat() if max_date else None)
    )
    if not same_scope:
        logger.warning(
            f'进度文件 {progress_file} 的 mode/since/max_date 与本次不一致，忽略旧进度')
        return {}, set()
    done = set(progress.get('done_codes') or [])
    logger.info(f'读取进度 {progress_file}：已完成 {len(done)} 只，续跑')
    return progress, done


def _prepare_client(
    source_effective: str, injected: TdxOnlineClient | None
) -> tuple[TdxOnlineClient | None, bool]:
    """返回 (客户端, 是否由本函数创建并负责关闭)。"""
    if source_effective == 'local':
        return injected, False
    if injected is not None:
        return injected, False
    return TdxOnlineClient(), True


def _log_periodic(index: int, total: int, summary: dict[str, Any]) -> None:
    if index % 200 == 0:
        logger.info(
            f"进度 {index}/{total}（已处理 "
            f"{summary['stocks_processed']} 只，行 {summary['rows']}）")


def _raise_if_all_failed(codes: Sequence[str], summary: dict[str, Any]) -> None:
    stats = summary['source_stats']
    if codes and stats['failed'] == len(codes):
        raise DataSourceException(
            'stock_daily_fill',
            f"网络源与本地 .day 均失败（{len(codes)} 只全部失败），"
            f"回退原因：{stats['fallback_reasons']}",
        )


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
    progress_path: Path | str | None = None,
    resume: bool = True,
    source: str = 'auto',
    online_client: TdxOnlineClient | None = None,
    source_manifest_path: Path | str | None = None,
) -> dict[str, Any]:
    """把 TDX 日线补入 ``stock_daily``。

    Args:
        config: 配置对象（读取 ``data.tdx_data_path``）
        db_manager: 数据库管理器
        ts_codes: 显式股票池；None 时枚举本地全部股票（排除指数/基金）
        full: True 为全量（全历史），False 为增量（库内最新交易日之后）。
            全量模式下若库内已覆盖源端首末日期，则该股票视为已完成并跳过，
            因此可断点续跑、重复执行幂等。全量固定走本地 ``.day``。
        dry_run: 只统计不写库
        since: 只写入不早于该日期的记录（用于库内水位缺失时的有界回填）
        max_date: 只写入不晚于该日期的记录（用于避开盘中未收盘 K 线）
        batch_stocks: 每批股票数
        progress_path: 断点续跑进度文件；None 用 ``logs/sync_progress/``
        resume: 是否读取已有进度跳过已完成股票（upsert 幂等，跳过只省时间）
        source: ``auto``（默认，网络优先→本地回退）/ ``local`` / ``mootdx``
        online_client: 可注入的网络源客户端（测试用）；None 时按需创建
        source_manifest_path: 来源清单落盘路径；None 用进度目录

    Returns:
        统计字典：模式、股票数、行数、新增/更新数、跳过原因分布、取数来源统计。
    """
    from src.data.tdx_handler import TdxHandler

    if source not in ('auto', 'local', 'mootdx'):
        raise ValueError(f"source 只能是 auto/local/mootdx，收到 {source!r}")

    handler = TdxHandler(config, None)
    if not handler.available:
        raise DataSourceException(
            'stock_daily_fill',
            f"通达信数据路径不存在: {handler.tdx_data_path}，无法离线补库",
        )

    codes = sorted(set(ts_codes)) if ts_codes else default_stock_universe(config)

    source_effective = 'local' if (full or source == 'local') else source
    summary: dict[str, Any] = {
        'mode': 'full' if full else 'incremental',
        'dry_run': dry_run,
        'source': source,
        'source_effective': source_effective,
        'stocks_total': len(codes),
        'stocks_processed': 0,
        'stocks_resumed': 0,
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
            'already_complete': 0,
            'network_failed': 0,
            'source_failed': 0,
        },
        'source_stats': {
            'network': 0,
            'local': 0,
            'local_fallback': 0,
            'failed': 0,
            'fallback_reasons': {},
        },
    }
    if not codes:
        logger.warning("stock_daily 补库：股票池为空，未执行任何操作")
        return summary

    if full and source != 'local':
        logger.warning('全量模式固定使用本地 .day（网络源仅用于增量），source=%s', source)

    client, owns_client = _prepare_client(source_effective, online_client)
    provenance_map: dict[str, str] = {}

    watermarks: dict[str, date] = {} if full else _latest_trade_dates(db_manager)
    coverage: dict[str, tuple[date, date]] = (
        _coverage_ranges(db_manager) if full else {})

    progress_file: Path | None = Path(progress_path) if progress_path else None
    progress: dict[str, Any] = {}
    done: set[str] = set()
    if resume and not dry_run:
        progress, done = _load_resume_state(
            progress_file, summary['mode'], since, max_date)

    session = None if dry_run else db_manager.get_session()
    pending: list[dict[str, Any]] = []
    pending_stocks = 0
    progress_codes: list[str] = []

    try:
        for index, ts_code in enumerate(codes, start=1):
            if ts_code in done:
                summary['stocks_resumed'] += 1
                continue

            rows, reason, provenance, fallback_reason = _collect_rows_with_source(
                handler.tdx_data_path, ts_code,
                source=source_effective, client=client, full=full,
                coverage=coverage, watermarks=watermarks,
                since=since, max_date=max_date)

            _account_source(provenance_map, summary['source_stats'],
                            ts_code, provenance, fallback_reason)

            if rows is None:
                summary['skipped_reasons'][reason] += 1
                if reason == 'already_complete':
                    # 已完整股票也记进度，续跑时连首尾日期探测都省掉
                    progress_codes.append(ts_code)
                    if len(progress_codes) >= batch_stocks:
                        _mark_progress(True, done, progress_codes, summary,
                                       progress, progress_file, dry_run,
                                       since, max_date)
                else:
                    _log_skip(ts_code, reason, provenance, fallback_reason)
                continue

            pending.extend(rows)
            pending_stocks += 1
            progress_codes.append(ts_code)
            summary['stocks_processed'] += 1

            if pending_stocks >= batch_stocks:
                ok = _flush_batch(session, pending, summary, dry_run)
                _mark_progress(ok, done, progress_codes, summary, progress,
                               progress_file, dry_run, since, max_date)
                pending = []
                pending_stocks = 0

            _log_periodic(index, len(codes), summary)

        ok = _flush_batch(session, pending, summary, dry_run)
        _mark_progress(ok, done, progress_codes, summary, progress,
                       progress_file, dry_run, since, max_date)

        if source_effective != 'local':
            summary['source_manifest_path'] = _write_source_manifest(
                progress_file, source_manifest_path, summary, provenance_map, source)

        logger.info(
            f"stock_daily 补库完成（{summary['mode']}"
            f"{'，dry-run' if dry_run else ''}，source={source_effective}）："
            f"股票 {summary['stocks_total']} 只，覆盖 {summary['stocks_processed']} 只，"
            f"行 {summary['rows']}，新增 {summary['inserted']}，更新 {summary['updated']}，"
            f"跳过 {summary['skipped_reasons']}"
        )
        summary['progress_path'] = str(progress_file) if progress_file else ''

        _raise_if_all_failed(codes, summary)
        return summary
    finally:
        if owns_client and client is not None:
            client.close()
        if session is not None:
            session.close()
