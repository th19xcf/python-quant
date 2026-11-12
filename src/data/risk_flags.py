#!/usr/bin/env python3

"""ST / 风险警示三态数据 API（本地优先，不联网）。

数据来源（按优先级）：

1. ``{store_root}/meta/st/st_intervals.parquet``——历史 ST 区间表（由
   :mod:`src.data.st_history` 从深交所简称变更 + 巨潮公告重建），覆盖
   区间内的历史日期可精确判定；
2. ``{store_root}/meta/names.json``——本地通达信证券名称快照（当日真值）；
3. 远程库 ``stock_basic`` 的 ``name`` / ``status``（可选，仅补本地无名称的标的）。

三态语义：

- ``st``：区间命中，或名称前缀命中 ``ST`` / ``*ST`` / ``S*ST`` / ``SST``；
- ``non_st``：区间未命中且 ``as_of`` 落在该标的**可信覆盖窗口**内，或有名称且
  前缀不命中；
- ``unknown``：无区间、无名称、已退市、或查询日期早于该标的可信窗口——
  **调用方不得当成 non_st**。

历史能力（重要红线）：通达信本地只保存**当前**名称，快照按日累积后只能覆盖
快照日之后；早于首份快照的历史日期由区间表兜底。区间表对「没有任何记录」的
标的返回 ``unknown``，绝不用区间外推回填历史。
"""

import argparse
import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, TypedDict

import polars as pl
from loguru import logger

from src.data.atomic_write import write_parquet_atomic
from src.data.snapshot_manifest import write_snapshot_manifest

ST = 'st'
NON_ST = 'non_st'
UNKNOWN = 'unknown'

SOURCE_TDX_NAMES = 'tdx_names'
SOURCE_STOCK_BASIC = 'stock_basic'
SOURCE_ST_HISTORY = 'st_history'
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


#: 正式路径的文件名：单表按 ``as_of`` 纵向累积，路径不带日期（DG-4）
HISTORY_NAME = 'st_status_history.parquet'


def snapshot_path(store_root: Path | str) -> Path:
    """ST 快照正式路径（唯一一份，取哪一天由 ``as_of`` 列决定）。"""
    return Path(store_root) / 'meta' / 'st_status' / HISTORY_NAME


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
    if not names and not db_basic:
        # fail-closed：全 UNKNOWN 的快照看起来"生成成功"，但会让下游把 ST 股
        # 当非 ST 放行。宁可不落盘，让 freshness 巡检直接报 critical。
        logger.error(
            '[st] 名称码表为空且无 stock_basic 兜底，拒绝落盘全 UNKNOWN 的 ST '
            f'快照（as_of={snapshot_date}）；请恢复 meta/names.json 后重试'
        )
        return pl.DataFrame()
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
    path = snapshot_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    _upsert_history(frame, path)
    logger.info(f'[st] ST 快照已并入 {path}（as_of={snapshot_date}，{frame.height} 个标的）')
    write_manifest(root)
    return frame


#: 目录级 manifest 的登记内容（``files`` / 汇总由 :func:`write_manifest` 实测填充）。
class _ManifestPayload(TypedDict):
    """``write_snapshot_manifest`` 的关键字参数集。

    用 TypedDict 而不是 ``dict[str, ...]``：后者 ``**`` 展开会把每个键都宽成
    ``str | list[str]``，既丢了逐键类型，也掩盖 consumers 的真实元素类型。
    """

    prefix: str
    dataset: str
    semantics: str
    source: str
    authority: str
    path_pattern: str
    consumers: list[str]
    produced_by: str


_MANIFEST_PAYLOAD: _ManifestPayload = {
    'prefix': 'st_status_',
    'dataset': 'st_status_history',
    'semantics': (
        '单表日期快照序列：每行是「某标的某日的 ST 三态截面」，'
        '按 as_of 纵向累积、只增不重写；正式路径不带日期，取哪一天一律由 '
        'as_of 列决定。历史 ST 判定须走 meta/st/st_intervals.parquet 区间表，'
        '本表只作当日横截面核对'
    ),
    'source': (
        '本地名称快照 meta/names.json 派生；本地无名称时用远程 stock_basic 兜底'
        '（已退市判 unknown）'
    ),
    'authority': (
        'docs/数据权威路径清单.md 登记的 meta/st_status/st_status_history.parquet；'
        '选择函数 src/data/risk_flags.py::load_snapshot（取 as_of 不晚于目标日的'
        '最大 as_of 那一层）'
    ),
    'path_pattern': 'meta/st_status/st_status_history.parquet',
    'consumers': [
        'src/data/risk_flags.py::load_snapshot / snapshot_days（as_of 不晚于目标日）',
        'src/data/st_history.py::_snapshot_series 按 as_of 升序逐层读（防未来函数）',
        'src/advice/risk_filters.py load_st_snapshot',
        'src/data/quality/clean_set.py 取 as_of == 2026-11-03 那一层（已登记陈旧截面缺陷）',
        'src/data/quality/freshness_config.json 的 latest_as_of_distinct / parquet_max',
        'tools/build_universe_layers.py::_snapshot_comparison 取最大 as_of 做 L3 对拍',
    ],
    'produced_by': 'src/data/risk_flags.py::build_snapshot（按 as_of 覆盖同日后合并落盘）',
}


def write_manifest(store_root: Path | str) -> Path:
    """重算 ``meta/st_status/manifest.json``（只写 manifest，不重建任何快照）。"""
    root = Path(store_root)
    return write_snapshot_manifest(root, root / 'meta' / 'st_status', **_MANIFEST_PAYLOAD)


def _read_history(store_root: Path | str) -> pl.DataFrame | None:
    """读整张 history 表（按文件 mtime 缓存）；表不存在返回 ``None``。"""
    path = snapshot_path(store_root)
    try:
        stat = path.stat()
    except OSError:
        return None
    cached = _SNAPSHOT_CACHE.get(str(path))
    if cached is not None and cached[0] == stat.st_mtime_ns:
        return cached[1]
    frame = pl.read_parquet(path)
    _SNAPSHOT_CACHE[str(path)] = (stat.st_mtime_ns, frame)
    return frame


def snapshot_days(store_root: Path | str) -> list[date]:
    """表内已有的快照日（升序）；无表或全空返回空列表。"""
    frame = _read_history(store_root)
    if frame is None or frame.height == 0:
        return []
    return sorted(day for day in frame['as_of'].unique().to_list() if day is not None)


def _upsert_history(frame: pl.DataFrame, path: Path) -> None:
    """把当日快照按 ``as_of`` 覆盖式并入单表（同一 as_of 重跑不重复累积）。"""
    as_of = frame['as_of'][0]
    if path.is_file():
        existing = pl.read_parquet(path).filter(pl.col('as_of') != as_of)
        frame = pl.concat([existing, frame], how='vertical')
    write_parquet_atomic(frame.sort(['as_of', 'code']), path)


def load_snapshot(
    store_root: Path | str, as_of: Any = None,
) -> tuple[pl.DataFrame | None, date | None]:
    """取 ``as_of`` 不晚于目标日的最大一层快照；早于首层返回 ``(None, None)``。"""
    target = _as_date(as_of) or date.today()
    frame = _read_history(store_root)
    if frame is None or frame.height == 0:
        return None, None
    days = [day for day in frame.filter(pl.col('as_of') <= target)['as_of']
            .unique().to_list() if day is not None]
    if not days:
        return None, None
    day = max(days)
    return frame.filter(pl.col('as_of') == day), day


def get_st_status_detail(
    code: str, as_of: Any = None, *, store_root: Path | str | None = None,
) -> dict[str, Any]:
    """返回 ST 三态详情（含来源、快照日与原因）。

    优先用不晚于 ``as_of`` 的每日快照（当日真值）；该日期早于首份快照时
    回退到历史区间表，二者都无法给出结论才返回 ``unknown``。
    """
    from src.data.services.st_status_service import normalize_ts_code  # noqa: PLC0415

    result: dict[str, Any] = {
        'code': normalize_ts_code(code) or str(code).upper(),
        'status': UNKNOWN, 'name': '', 'source': SOURCE_MISSING,
        'as_of': None, 'reason': 'no_snapshot',
        'coverage_start': None, 'coverage_end': None,
        'interval_start': None, 'interval_end': None,
    }
    if store_root is None:
        from src.data.local_store.store import resolve_store_root  # noqa: PLC0415
        from src.utils.config import get_config  # noqa: PLC0415

        store_root = resolve_store_root(get_config())
    frame, day = load_snapshot(store_root, as_of)
    if frame is not None and day is not None:
        rows = frame.filter(pl.col('code') == result['code']).to_dicts()
        if rows:
            row = rows[0]
            result.update({
                'status': row['status'], 'name': row['name'], 'source': row['source'],
                'as_of': day.isoformat(), 'reason': row['reason'] or '',
            })
            if row['status'] != UNKNOWN:
                return result
            result['reason'] = row['reason'] or 'snapshot_unknown'
    history = _history_detail(result['code'], as_of, store_root)
    if history is not None:
        return history
    if frame is not None and day is not None:
        result['as_of'] = day.isoformat()
        result['reason'] = 'code_not_covered'
    return result


def _history_detail(code: str, as_of: Any, store_root: Path | str) -> dict[str, Any] | None:
    """历史区间表判定；无可信记录返回 ``None`` 交给快照兜底。"""
    from src.data import st_history  # noqa: PLC0415

    if not st_history.intervals_path(store_root).is_file():
        return None
    hit = st_history.status_at(store_root, code, as_of)
    if hit.status == UNKNOWN:
        return None
    return {
        'code': code, 'status': hit.status, 'name': '',
        'source': SOURCE_ST_HISTORY, 'as_of': str(as_of or ''),
        'reason': hit.reason, 'coverage_start': hit.coverage_start,
        'coverage_end': hit.coverage_end, 'interval_start': hit.start,
        'interval_end': hit.end, 'flag': hit.flag,
    }


def get_st_status(
    code: str, as_of: Any = None, *, store_root: Path | str | None = None,
) -> str:
    """ST 三态查询：``st`` / ``non_st`` / ``unknown``。"""
    return str(get_st_status_detail(code, as_of, store_root=store_root)['status'])


def get_st_intervals(
    code: str, start: Any, end: Any, *, store_root: Path | str | None = None,
) -> list[dict[str, Any]]:
    """返回 [start, end] 内的 ST 状态区间。

    有历史区间表时优先使用（区间外但落在可信窗口内的空档显式标 ``non_st``）；
    否则按已累积快照拼接（不外推、不补洞）。
    """
    from src.data.services.st_status_service import normalize_ts_code  # noqa: PLC0415

    start_day = _as_date(start)
    end_day = _as_date(end)
    if store_root is None:
        from src.data.local_store.store import resolve_store_root  # noqa: PLC0415
        from src.utils.config import get_config  # noqa: PLC0415

        store_root = resolve_store_root(get_config())
    target = normalize_ts_code(code) or str(code).upper()
    from src.data import st_history  # noqa: PLC0415

    if st_history.intervals_path(store_root).is_file():
        historical = st_history.intervals_for_backtest(
            store_root, [target], start_day, end_day).get(target)
        if historical:
            return [{'status': ST if item['is_st'] else NON_ST,
                      'start': item['start_date'], 'end': item['end_date']}
                    for item in historical]
    series: list[tuple[date, str]] = []
    history_frame = _read_history(store_root)
    if history_frame is not None:
        for day in snapshot_days(store_root):
            if (start_day is not None and day < start_day) \
                    or (end_day is not None and day > end_day):
                continue
            rows = history_frame.filter(
                (pl.col('as_of') == day) & (pl.col('code') == target)).to_dicts()
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
    """历史可回溯性：区间表覆盖范围 + 每日快照累积情况。"""
    if store_root is None:
        from src.data.local_store.store import resolve_store_root  # noqa: PLC0415
        from src.utils.config import get_config  # noqa: PLC0415

        store_root = resolve_store_root(get_config())
    days = snapshot_days(store_root)
    from src.data import st_history  # noqa: PLC0415

    hist = st_history.coverage_summary(store_root)
    return {
        'backfillable': bool(hist.get('available')),
        'snapshots': len(days),
        'first_snapshot': days[0].isoformat() if days else None,
        'last_snapshot': days[-1].isoformat() if days else None,
        'history': hist,
        'note': '历史日期由区间表判定；无记录的标的仍返回 unknown，不做非 ST 默认',
    }


def _basic_from_snapshot(
    store_root: Path,
) -> dict[str, tuple[str | None, str | None]]:
    """从本地 ``meta/stock_basic.parquet`` 读取 name/status；缺失返回空。"""
    target = Path(store_root) / 'meta' / 'stock_basic.parquet'
    if not target.is_file():
        return {}
    try:
        import polars as pl  # noqa: PLC0415

        frame = pl.read_parquet(target, columns=['ts_code', 'name', 'status'])
        return {
            str(code): (name, status)
            for code, name, status in frame.iter_rows()
        }
    # 兜底理由：快照损坏时回退远程库，不阻断 ST 快照生成
    except Exception as exc:  # noqa: BLE001
        logger.warning(f'[st] stock_basic 快照读取失败，回退远程: {exc}')
        return {}


def _db_basic(
    store_root: Path | None = None,
) -> dict[str, tuple[str | None, str | None]]:
    """读取 stock_basic 的 name/status：优先本地快照，其次远程库。"""
    if store_root is not None:
        local = _basic_from_snapshot(store_root)
        if local:
            return local
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
        frame = build_snapshot(store_root, args.as_of, db_basic=_db_basic(store_root))
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
