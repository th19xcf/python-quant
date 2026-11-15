#!/usr/bin/env python3

"""数据版本台账：基础表 → 派生表 → 跑批产物的「基于哪版数据」标记。

两份台账（均已落正式层 ``data/local_store/meta/``）::

    data_version.json      # 基础表版本（current + versions 历史，只增不覆盖）
    derived_versions.json  # 每张派生表建它时看到的是哪版（侧车登记，只记最新一次）

版本口径
========
- 标识 ``dv-YYYYMMDD-<12hex>``：``YYYYMMDD`` 取行情末日
  （``meta/daily_index.parquet`` 的 ``end`` 最大值）；``hex`` 是七类基础表
  登记统计的汇总哈希。
- 行情类用 ``meta/daily_index.parquet`` 自身的 ``rows`` / ``sha256`` / ``bytes``
  作摘要（随 ``flush_meta`` 自动刷新）；名录 / 财报+估值 / 因子 / 日历 / 事件
  用各自 ``manifest.json`` 里已声明的 parquet 统计（``rows`` + ``sha256``）作摘要。
- ``versions`` 保留最近 ``HISTORY_KEEP`` 个不同版本（最新在前）；同一版本重复
  计算只更新 ``last_seen`` / ``seen_count``，不新增条目、不覆盖旧记录。
- 派生表侧车只记「建它那一刻的数据版本」，**不回填、不猜**：没有标记即
  ``unstamped``，判不了就如实显示，不拿当前版本补造。

判定
====
``audit_derived`` 逐张比对「侧车登记的 digest」与「当前数据版本 digest」：
``ok`` / ``stale``（基础表已变、派生表未重建）/ ``unstamped``（从未标记）/
``missing``（派生表文件不存在）。只报状态，**不触发任何自动重建或重跑**。

用法::

    python tools/data_version.py record            # 日更收尾记录一版
    python tools/data_version.py show              # 当前版本与历史
    python tools/data_version.py stamp --table meta/multi_asset/etf_pool.parquet
    python tools/data_version.py audit             # 判派生表是否落后于基础表
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

from src.data.atomic_write import write_text_atomic

SCHEMA = 'data_version/v1'
DERIVED_SCHEMA = 'derived_version/v1'
VERSION_FILE = Path('meta') / 'data_version.json'
DERIVED_FILE = Path('meta') / 'derived_versions.json'
#: 版本历史保留条数（不同 digest 才占一条）
HISTORY_KEEP = 50

#: 七类基础表的版本来源：类名 → 说明 + 组件（manifest 或摘要 parquet）
BASE_CLASSES: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ('quotes', '行情', ('meta/daily_index.parquet',)),
    ('basic', '名录', ('meta/stock_basic.manifest.json',
                       'meta/fund_basic.manifest.json',
                       'meta/index_basic.manifest.json')),
    ('financials', '财报+估值', ('meta/financials/manifest.json',)),
    ('factors', '因子', ('factors/manifest.json',)),
    ('calendar', '日历', ('meta/trading_calendar/manifest.json',)),
    ('events', '事件', ('meta/events/manifest.json',)),
)

#: 需要跟踪版本落差的派生表：相对路径 → 名称 + 生产者
DERIVED_TABLES: tuple[tuple[str, str, str], ...] = (
    ('meta/multi_asset/etf_pool.parquet', 'ETF 候选池',
     'tools/build_multi_asset_etf_pool.py'),
    ('meta/breadth/breadth_daily.parquet', '市场宽度',
     'src/data/services/breadth_service.py'),
    ('meta/breadth/breadth_ma_daily.parquet', '宽度均线',
     'tools/build_breadth_ma.py'),
    ('factors/stock_turnover.parquet', '个股换手率',
     'tools/fetch_stock_shares.py'),
    ('meta/st/st_intervals.parquet', 'ST 区间', 'src/data/st_history.py'),
)


def _now() -> str:
    return datetime.now().isoformat(timespec='seconds')


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for block in iter(lambda: handle.read(4 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def _canonical(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def _iter_stat_nodes(node: Any, fallback: str | None = None):
    """递归收集 manifest 里带行数与 sha256 的登记条目。

    兼容两种登记形态：``files: {名: {rows, sha256}}`` 与 schema v2 的
    平铺形态（``rows`` + ``source_parquet_sha256`` + ``source_parquet``）。
    """
    if isinstance(node, dict):
        sha = node.get('sha256') or node.get('source_parquet_sha256')
        rows = node.get('rows')
        declared = (node.get('path') or node.get('name')
                    or node.get('source_parquet') or fallback)
        if isinstance(sha, str) and sha and isinstance(rows, int) and declared:
            yield str(declared).replace('\\', '/'), rows, sha.lower()
        for key, value in node.items():
            if isinstance(value, (dict, list)):
                yield from _iter_stat_nodes(value, key)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_stat_nodes(item, fallback)


def _manifest_digest(path: Path) -> str:
    """manifest 内已声明文件的 ``rows:sha256`` 汇总哈希；不可读记 ``missing``。"""
    if not path.is_file():
        return 'missing'
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return 'unreadable'
    entries: dict[str, str] = {}
    for key, rows, sha in _iter_stat_nodes(payload):
        entries[key] = f'{rows}:{sha}'
    if not entries:
        return 'empty'
    return hashlib.sha256(_canonical(entries).encode('utf-8')).hexdigest()


def _quotes_component(store_root: Path) -> dict[str, Any]:
    """行情摘要：``meta/daily_index.parquet`` 的文件事实 + 末日。"""
    path = store_root / 'meta' / 'daily_index.parquet'
    if not path.is_file():
        return {'missing': True}
    component: dict[str, Any] = {
        'bytes': path.stat().st_size,
        'sha256': _sha256_file(path),
    }
    try:
        import polars as pl

        end = pl.scan_parquet(path).select(pl.col('end').max()).collect().item()
        component['end'] = end.isoformat() if isinstance(end, date) else str(end)
    except Exception:  # noqa: BLE001 - 摘要读取失败不阻断版本记录
        component['end'] = None
    return component


def compute_version(store_root: Path | str) -> dict[str, Any]:
    """按七类基础表登记统计算当前数据版本（纯计算，不写盘）。"""
    root = Path(store_root)
    components: dict[str, Any] = {}
    for name, _label, rels in BASE_CLASSES:
        if name == 'quotes':
            components[name] = _quotes_component(root)
        else:
            components[name] = {
                rel: _manifest_digest(root / rel) for rel in rels}
    digest = hashlib.sha256(_canonical(components).encode('utf-8')).hexdigest()
    as_of = components.get('quotes', {}).get('end')
    stamp = (as_of or date.today().isoformat()).replace('-', '')
    return {
        'version': f'dv-{stamp}-{digest[:12]}',
        'digest': digest,
        'as_of': as_of,
        'computed_at': _now(),
        'classes': components,
    }


def _load(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.is_file():
        return default
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return default
    return payload if isinstance(payload, dict) else default


def load_current(store_root: Path | str) -> dict[str, Any] | None:
    payload = _load(Path(store_root) / VERSION_FILE, {})
    current = payload.get('current')
    return current if isinstance(current, dict) and current.get('digest') else None


def record_version(store_root: Path | str, *, keep: int = HISTORY_KEEP,
                   note: str = '') -> dict[str, Any]:
    """记录当前数据版本；同 digest 只刷新时间，不同 digest 追加一条历史。"""
    root = Path(store_root)
    snapshot = compute_version(root)
    path = root / VERSION_FILE
    payload = _load(path, {'schema': SCHEMA, 'current': None, 'versions': []})
    versions = [item for item in payload.get('versions') or [] if isinstance(item, dict)]
    previous = payload.get('current') if isinstance(payload.get('current'), dict) else None
    changed = not previous or previous.get('digest') != snapshot['digest']
    now = _now()
    if not changed:
        previous['last_seen'] = now
        previous['seen_count'] = int(previous.get('seen_count') or 1) + 1
        if note:
            previous['note'] = note
        if versions and versions[0].get('digest') == previous.get('digest'):
            versions[0] = previous
        current = previous
    else:
        current = {**snapshot, 'first_seen': now, 'last_seen': now, 'seen_count': 1}
        if note:
            current['note'] = note
        versions.insert(0, current)
        del versions[keep:]
    payload = {'schema': SCHEMA, 'current': current, 'versions': versions}
    write_text_atomic(json.dumps(payload, ensure_ascii=False, indent=2), path)
    return {'version': current['version'], 'digest': current['digest'],
            'as_of': current.get('as_of'), 'changed': changed,
            'history': len(versions)}


def ensure_current(store_root: Path | str, *, note: str = '') -> dict[str, Any]:
    """台账缺失或与磁盘算出的版本不一致时补记一版，返回当前版本。"""
    current = load_current(store_root)
    snapshot = compute_version(store_root)
    if current and current.get('digest') == snapshot['digest']:
        return current
    record_version(store_root, note=note)
    return load_current(store_root) or snapshot


def stamp_derived(store_root: Path | str, table: str, *,
                  files: Iterable[str] | None = None,
                  note: str = '') -> dict[str, Any]:
    """给一张派生表登记「基于哪版数据」（侧车 ``derived_versions.json``）。"""
    root = Path(store_root)
    current = ensure_current(root, note='派生表标记前自动补记当前版本')
    path = root / DERIVED_FILE
    payload = _load(path, {'schema': DERIVED_SCHEMA, 'tables': {}})
    tables = payload.get('tables')
    if not isinstance(tables, dict):
        tables = {}
    entry = {
        'based_on_digest': current['digest'],
        'based_on_version': current['version'],
        'data_as_of': current.get('as_of'),
        'stamped_at': _now(),
        'files': sorted(str(item).replace('\\', '/') for item in (files or [table])),
        'note': note,
    }
    tables[table.replace('\\', '/')] = entry
    payload = {'schema': DERIVED_SCHEMA, 'tables': tables}
    write_text_atomic(json.dumps(payload, ensure_ascii=False, indent=2), path)
    return entry


def stamp_rebuilt(store_root: Path | str, changed: Iterable[str]) -> list[str]:
    """给本次日更里被重建的派生表打版本标记，返回标记过的表。"""
    rebuilt = {str(item).replace('\\', '/') for item in changed}
    stamped: list[str] = []
    for table, _label, producer in DERIVED_TABLES:
        if table in rebuilt and (Path(store_root) / table).is_file():
            stamp_derived(store_root, table, note=f'日更重建（{producer}）')
            stamped.append(table)
    return stamped


def audit_derived(store_root: Path | str) -> dict[str, Any]:
    """判每张派生表是否落后于当前基础表版本（只报状态，不重建）。"""
    root = Path(store_root)
    current = load_current(root) or compute_version(root)
    payload = _load(root / DERIVED_FILE, {})
    tables = payload.get('tables') if isinstance(payload.get('tables'), dict) else {}
    rows: list[dict[str, Any]] = []
    counts = {'ok': 0, 'stale': 0, 'unstamped': 0, 'missing': 0}
    for table, label, producer in DERIVED_TABLES:
        if not (root / table).is_file():
            status = 'missing'
        elif table not in tables:
            status = 'unstamped'
        elif tables[table].get('based_on_digest') != current['digest']:
            status = 'stale'
        else:
            status = 'ok'
        counts[status] += 1
        rows.append({
            'table': table, 'label': label, 'producer': producer, 'status': status,
            'based_on_version': (tables.get(table) or {}).get('based_on_version'),
            'stamped_at': (tables.get(table) or {}).get('stamped_at'),
        })
    return {'current': current, 'rows': rows, 'counts': counts}


def format_audit_line(result: dict[str, Any]) -> str:
    counts = result['counts']
    current = result['current']
    return (f"[版本] {current['version']}（行情末日 {current.get('as_of')}）| "
            f"派生 ok {counts['ok']} / 落后 {counts['stale']} / "
            f"未标记 {counts['unstamped']} / 缺失 {counts['missing']}")
