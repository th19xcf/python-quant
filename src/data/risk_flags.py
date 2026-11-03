#!/usr/bin/env python3

"""ST / 风险警示三态数据 API（本地优先，不联网，不回溯历史）。

数据来源（按优先级）：

1. ``{store_root}/meta/names.json``——本地通达信证券名称快照；
2. 远程库 ``stock_basic`` 的 ``name`` / ``status``（可选，仅补本地无名称的标的）。

三态语义：

- ``st``：名称前缀命中 ``ST`` / ``*ST`` / ``S*ST`` / ``SST``；
- ``non_st``：有名称且前缀不命中；
- ``unknown``：无名称、已退市或查不到快照——**调用方不得当成 non_st**。

历史能力（重要红线）：通达信本地只保存**当前**名称，没有戴帽/摘帽日期，
因此历史 ST 状态**不可回溯**。本模块只按日期累积每日快照
（``meta/st_status/st_status_YYYYMMDD.parquet``），查询时取不晚于
``as_of`` 的最新一份；早于首份快照的日期一律返回 ``unknown``，
绝不用今天的名单回填历史。
"""

import argparse
import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

ST = 'st'
NON_ST = 'non_st'
UNKNOWN = 'unknown'

SOURCE_TDX_NAMES = 'tdx_names'
SOURCE_STOCK_BASIC = 'stock_basic'
SOURCE_MISSING = 'missing'

_ST_PREFIX = re.compile(r'^(S\*ST|SST|\*ST|ST)')
_SNAPSHOT_CACHE: dict[str, tuple[int, pl.DataFrame]] = {}


def _as_date(value: Any) -> date | None:
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(text[:10] if '-' in text else text, fmt).date()
        except ValueError:
            continue
    return None


def status_from_name(name: Any) -> str:
    """按名称前缀判定 ST 三态；空名称返回 ``unknown``。"""
    text = str(name).strip() if name is not None else ''
    if not text:
        return UNKNOWN
    return ST if _ST_PREFIX.match(text) else NON_ST


def snapshot_path(store_root: Path | str, as_of: date) -> Path:
    return Path(store_root) / 'meta' / 'st_status' \
        / f'st_status_{as_of:%Y%m%d}.parquet'


def _load_names(path: Path) -> dict[str, str]:
    if not path.is_file():
        logger.warning(f'[st] 名称快照缺失: {path}')
        return {}
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        logger.warning(f'[st] 名称快照解析失败: {path}: {exc}')
        return {}
    return {str(key).upper(): str(value) for key, value in payload.items() if value}


def build_snapshot(
    store_root: Path | str, as_of: Any = None, *,
    names_path: Path | str | None = None,
    db_basic: dict[str, tuple[str | None, str | None]] | None = None,
    codes: list[str] | None = None,
) -> pl.DataFrame:
    """构建（并落盘）某日的 ST 快照，只覆盖个股。

    Args:
        db_basic: ``{ts_code: (name, status)}``，来自远程库 ``stock_basic``；
            仅用于本地无名称的标的，``status='D'`` 一律判 ``unknown``。
        codes: 显式标的列表（测试用）；缺省读取本地列存 stock 全量。
    """
    snapshot_date = _as_date(as_of) or date.today()
    root = Path(store_root)
    names = _load_names(Path(names_path) if names_path else root / 'meta' / 'names.json')
    if codes is None:
        from src.data.local_store.store import LocalQuoteStore  # noqa: PLC0415

        codes = LocalQuoteStore(root).codes('stock')

    rows: list[dict[str, Any]] = []
    for code in codes:
        local_name = names.get(code)
        if local_name:
            rows.append({
                'code': code, 'name': local_name,
                'status': status_from_name(local_name), 'source': SOURCE_TDX_NAMES,
                'reason': '',
            })
            continue
        basic_name, basic_status = (db_basic or {}).get(code, (None, None))
        if basic_status == 'D':
            rows.append({
                'code': code, 'name': str(basic_name or ''),
                'status': UNKNOWN, 'source': SOURCE_STOCK_BASIC,
                'reason': 'delisted',
            })
        elif basic_name:
            rows.append({
                'code': code, 'name': str(basic_name),
                'status': status_from_name(basic_name),
                'source': SOURCE_STOCK_BASIC, 'reason': '',
            })
        else:
            rows.append({
                'code': code, 'name': '', 'status': UNKNOWN,
                'source': SOURCE_MISSING, 'reason': 'name_missing',
            })

    frame = pl.DataFrame(rows).with_columns(
        [pl.lit(snapshot_date).alias('as_of'),
         pl.col('status').eq(ST).alias('is_st')])
    frame = frame.with_columns(
        pl.when(pl.col('status') == UNKNOWN).then(None)
        .otherwise(pl.col('is_st')).alias('is_st'))
    path = snapshot_path(root, snapshot_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(path)
    logger.info(f'[st] ST 快照已落盘: {path}（{frame.height} 个标的）')
    return frame


def _snapshot_files(store_root: Path | str) -> list[tuple[date, Path]]:
    dir_path = Path(store_root) / 'meta' / 'st_status'
    if not dir_path.is_dir():
        return []
    files: list[tuple[date, Path]] = []
    for path in dir_path.glob('st_status_*.parquet'):
        day = _as_date(path.stem.split('_')[-1])
        if day is not None:
            files.append((day, path))
    return sorted(files)


def _read_snapshot(path: Path) -> pl.DataFrame:
    stat = path.stat()
    cached = _SNAPSHOT_CACHE.get(str(path))
    if cached is not None and cached[0] == stat.st_mtime_ns:
        return cached[1]
    frame = pl.read_parquet(path)
    _SNAPSHOT_CACHE[str(path)] = (stat.st_mtime_ns, frame)
    return frame


def load_snapshot(
    store_root: Path | str, as_of: Any = None,
) -> tuple[pl.DataFrame | None, date | None]:
    """取不晚于 ``as_of`` 的最新 ST 快照；早于首份快照返回 ``(None, None)``。"""
    target = _as_date(as_of) or date.today()
    files = [item for item in _snapshot_files(store_root) if item[0] <= target]
    if not files:
        return None, None
    day, path = files[-1]
    return _read_snapshot(path), day


def get_st_status_detail(
    code: str, as_of: Any = None, *, store_root: Path | str | None = None,
) -> dict[str, Any]:
    """返回 ST 三态详情（含来源、快照日与原因）。"""
    from src.data.services.st_status_service import normalize_ts_code  # noqa: PLC0415

    result: dict[str, Any] = {
        'code': normalize_ts_code(code) or str(code).upper(),
        'status': UNKNOWN, 'name': '', 'source': SOURCE_MISSING,
        'as_of': None, 'reason': 'no_snapshot',
    }
    if store_root is None:
        from src.data.local_store.store import resolve_store_root  # noqa: PLC0415
        from src.utils.config import get_config  # noqa: PLC0415

        store_root = resolve_store_root(get_config())
    frame, day = load_snapshot(store_root, as_of)
    if frame is None or day is None:
        return result
    rows = frame.filter(pl.col('code') == result['code']).to_dicts()
    if not rows:
        result['as_of'] = day.isoformat()
        result['reason'] = 'code_not_covered'
        return result
    row = rows[0]
    result.update({
        'status': row['status'], 'name': row['name'], 'source': row['source'],
        'as_of': day.isoformat(), 'reason': row['reason'] or '',
    })
    return result


def get_st_status(
    code: str, as_of: Any = None, *, store_root: Path | str | None = None,
) -> str:
    """ST 三态查询：``st`` / ``non_st`` / ``unknown``。"""
    return str(get_st_status_detail(code, as_of, store_root=store_root)['status'])


def get_st_intervals(
    code: str, start: Any, end: Any, *, store_root: Path | str | None = None,
) -> list[dict[str, Any]]:
    """按已累积快照返回 [start, end] 内的 ST 状态区间（不外推、不补洞）。"""
    from src.data.services.st_status_service import normalize_ts_code  # noqa: PLC0415

    start_day = _as_date(start)
    end_day = _as_date(end)
    if store_root is None:
        from src.data.local_store.store import resolve_store_root  # noqa: PLC0415
        from src.utils.config import get_config  # noqa: PLC0415

        store_root = resolve_store_root(get_config())
    target = normalize_ts_code(code) or str(code).upper()
    series: list[tuple[date, str]] = []
    for day, path in _snapshot_files(store_root):
        if (start_day is not None and day < start_day) \
                or (end_day is not None and day > end_day):
            continue
        frame = _read_snapshot(path)
        rows = frame.filter(pl.col('code') == target).to_dicts()
        if rows:
            series.append((day, rows[0]['status']))
    intervals: list[dict[str, Any]] = []
    for day, status in series:
        if intervals and intervals[-1]['status'] == status:
            intervals[-1]['end'] = day
            continue
        intervals.append({'status': status, 'start': day, 'end': day})
    return [
        {**item, 'start': item['start'].isoformat(), 'end': item['end'].isoformat()}
        for item in intervals
    ]


def history_coverage(store_root: Path | str | None = None) -> dict[str, Any]:
    """历史可回溯性：本地方案只能从首份快照起累积，早期日期不可回溯。"""
    if store_root is None:
        from src.data.local_store.store import resolve_store_root  # noqa: PLC0415
        from src.utils.config import get_config  # noqa: PLC0415

        store_root = resolve_store_root(get_config())
    files = _snapshot_files(store_root)
    return {
        'backfillable': False,
        'snapshots': len(files),
        'first_snapshot': files[0][0].isoformat() if files else None,
        'last_snapshot': files[-1][0].isoformat() if files else None,
        'note': '本地方案无戴帽/摘帽日期，早于首份快照的日期只能返回 unknown',
    }


def _db_basic() -> dict[str, tuple[str | None, str | None]]:
    """从远程库读取 stock_basic 的 name/status（失败时返回空，不阻断）。"""
    try:
        from sqlalchemy import text  # noqa: PLC0415

        from src.database.db_manager import DatabaseManager  # noqa: PLC0415
        from src.utils.config import get_config  # noqa: PLC0415

        db_manager = DatabaseManager(get_config())
        db_manager.connect()
        with db_manager.engine.connect() as conn:
            rows = list(conn.execute(text('SELECT ts_code, name, status FROM stock_basic')))
        db_manager.disconnect()
        return {str(row[0]): (row[1], row[2]) for row in rows}
    # 兜底理由：离线/库不可用时只用本地名称快照，不影响 API 与快照生成
    except Exception as exc:  # noqa: BLE001
        logger.warning(f'[st] stock_basic 不可用，仅用本地名称快照: {exc}')
        return {}


def _main(args: argparse.Namespace) -> int:
    from src.data.local_store.store import resolve_store_root  # noqa: PLC0415
    from src.utils.config import get_config  # noqa: PLC0415

    store_root = Path(args.store_root) if args.store_root else \
        resolve_store_root(get_config())
    if args.command == 'refresh':
        frame = build_snapshot(store_root, args.as_of, db_basic=_db_basic())
        counts = dict(frame.group_by('status').len().iter_rows())
        logger.info(f'[st] 快照分布: {counts}')
        logger.info(f'[st] 历史可回溯: {history_coverage(store_root)}')
        return 0
    snapshot, day = load_snapshot(store_root, args.as_of)
    if snapshot is None:
        logger.warning('[st] 尚无 ST 快照，请先执行 refresh')
        return 1
    counts = dict(snapshot.group_by('status').len().iter_rows())
    logger.info(f'[st] 快照日期 {day}，标的 {snapshot.height}，分布 {counts}')
    logger.info(f'[st] 历史可回溯: {history_coverage(store_root)}')
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description='ST / 风险警示三态快照')
    parser.add_argument('command', choices=['refresh', 'stats'])
    parser.add_argument('--as-of', default=None, help='快照日期 YYYY-MM-DD')
    parser.add_argument('--store-root', default=None, help='本地列存根目录')
    return _main(parser.parse_args())


if __name__ == '__main__':
    raise SystemExit(main())
