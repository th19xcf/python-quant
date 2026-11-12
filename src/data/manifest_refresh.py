#!/usr/bin/env python3

"""写盘后按磁盘实测回写 manifest 统计字段（通用实现）。

背景：多个产者在写正式表后不刷新同目录 manifest 的登记值，``rows`` / ``bytes`` /
``sha256`` 停在旧值，``tools/manifest_tool.py --check`` 读到的是过期登记，人工
核对也会被带偏。``src/data/st_history.py::refresh_manifest_files`` 已解决
``meta/st`` 一处，但只认「files 是 list、条目自带相对 store 根的 path」这一种
布局；本模块把它推广到现有的四种布局：

1. ``files`` 是 list，条目自带相对 store 根的 ``path``（meta/factors、meta/events）；
2. ``files`` 是 list，条目 ``path`` 只有文件名、目录由顶层 ``dir`` 给出
   （meta/anomalies）；
3. 顶层自述（``path`` + 统计字段），同目录另用 ``sibling_tables.files`` 记账
   另一张表（meta/breadth）；
4. 顶层自述但路径写在 ``files.parquet`` 里（meta/industry_pit）。

判定方式统一：**递归找出所有「声明了该文件且带统计字段」的节点**，命中即按磁盘
重算。只改统计字段（``rows`` / ``bytes`` / ``sha256`` / 已有的 ``columns`` /
``cols`` / ``date_range``），其余人工字段（``level`` / ``semantics`` /
``produced_by`` / ``authority`` / ``registered_reason`` / ``local_adjustments``
等）原样保留——本模块不写登记内容，也不新增条目：manifest 里没有该文件时保持
沉默（``required=True`` 时才报错），避免把「漏登记」伪装成「已登记」。

写盘走 :func:`src.data.atomic_write.write_text_atomic`（DG-9）；内容与原文逐字节
相同时不落盘，因此重复调用幂等，不会在 ``logs/data_changelog.csv`` 里留下空记录。

用法（一次性回写，产物与产者解耦时用）::

    python -m src.data.manifest_refresh \\
        --manifest meta/events/manifest.json --file meta/events/events.parquet
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable

import polars as pl
from loguru import logger

from src.data.atomic_write import write_text_atomic

#: 视为「统计字段」的键：节点至少要带一个，才算声明了该文件的登记条目。
STAT_KEYS = ('rows', 'bytes', 'sha256', 'columns', 'cols', 'date_range')
#: 回写时无条件重算的键。
ALWAYS_KEYS = ('rows', 'bytes', 'sha256')
#: ``date_range`` 未登记 ``date_column`` 时按此顺序找日期列（与 manifest_tool 同序）。
DATE_COLUMNS = (
    'trade_date', 'date', 'end_date', 'ex_date', 'start_date', 'effective_date',
    'in_date', 'out_date', 'coverage_start', 'record_date', 'period', 'pay_date',
    'publish_date',
)
STORE_PREFIX = 'data/local_store'


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def file_facts(path: Path) -> dict[str, Any]:
    """按磁盘实测取 rows / bytes / sha256 / columns（非 parquet 按文本表处理）。"""
    facts: dict[str, Any] = {
        'bytes': path.stat().st_size,
        'sha256': sha256_file(path),
        'rows': None,
        'columns': None,
    }
    suffix = path.suffix.lower()
    if suffix == '.parquet':
        lazy = pl.scan_parquet(str(path))
        facts['columns'] = list(lazy.collect_schema().names())
        facts['rows'] = int(lazy.select(pl.len()).collect().item())
    elif suffix in ('.csv', '.tsv'):
        with path.open('r', encoding='utf-8', newline='') as handle:
            lines = handle.read().splitlines()
        facts['columns'] = [c.strip() for c in lines[0].split(',')] if lines else []
        facts['rows'] = max(len(lines) - 1, 0)
    return facts


def _resolve(raw: str, store_root: Path, base: Path | None) -> Path:
    text = str(raw).strip().replace('\\', '/')
    if text.startswith(STORE_PREFIX + '/'):
        return store_root.joinpath(*text[len(STORE_PREFIX) + 1:].split('/'))
    candidate = Path(text)
    if candidate.is_absolute() or (len(text) > 1 and text[1] == ':'):
        return candidate
    if base is not None and ('/' not in text or (base / text).exists()):
        return base / text
    return store_root.joinpath(*[part for part in text.split('/') if part])


def _same(left: Path, right: Path) -> bool:
    try:
        return left.resolve() == right.resolve()
    except OSError:
        return str(left.absolute()).lower() == str(right.absolute()).lower()


def _declared_path(node: dict[str, Any]) -> str | None:
    for key in ('path', 'source_parquet'):
        value = node.get(key)
        if isinstance(value, str) and value.lower().endswith(
                ('.parquet', '.csv', '.tsv', '.json', '.txt')):
            return value
    files = node.get('files')
    if isinstance(files, dict):
        value = files.get('parquet')
        if isinstance(value, str):
            return value
    return None


def _candidates(data: dict[str, Any], store_root: Path) -> list[tuple[dict[str, Any], str | None, Path | None]]:
    """递归收集 (节点, 同名键, 该层的目录基准)，含顶层自身。"""
    out: list[tuple[dict[str, Any], str | None, Path | None]] = [(data, None, store_root)]

    def walk(node: dict[str, Any], base: Path | None) -> None:
        declared_dir = node.get('dir')
        if isinstance(declared_dir, str):
            base = _resolve(declared_dir, store_root, base)
        for key, value in node.items():
            if isinstance(value, dict):
                out.append((value, key, base))
                walk(value, base)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        out.append((item, None, base))
                        walk(item, base)

    walk(data, store_root)
    return out


def _declares(node: dict[str, Any], fallback: str | None, target: Path,
              store_root: Path, base: Path | None) -> bool:
    if not any(key in node for key in STAT_KEYS):
        return False
    declared = _declared_path(node)
    if declared is not None and _same(_resolve(declared, store_root, base), target):
        return True
    # ``files`` 以文件名为键的同目录写法：键名即声明（自带 path 的条目按 path 判，不走这里）
    return 'path' not in node and 'source_parquet' not in node \
        and bool(fallback) and fallback == target.name


def _date_bounds(path: Path, column: str) -> list[str] | None:
    try:
        series = pl.read_parquet(path, columns=[column])[column]
        low, high = series.min(), series.max()
    except (OSError, pl.exceptions.PolarsError):
        return None
    if low is None or high is None:
        return None
    return [str(low)[:10], str(high)[:10]]


def _apply(node: dict[str, Any], path: Path, facts: dict[str, Any]) -> list[str]:
    """按磁盘实测更新一个登记节点的统计字段，返回实际改动的键。"""
    changed: list[str] = []
    for key in ALWAYS_KEYS:
        value = facts.get(key)
        if value is None:
            continue
        if node.get(key) != value:
            node[key] = value
            changed.append(key)
    columns = facts.get('columns')
    if columns is None:
        return changed
    if 'columns' in node and list(node['columns']) != columns:
        node['columns'] = columns
        changed.append('columns')
    if 'cols' in node and node['cols'] != len(columns):
        node['cols'] = len(columns)
        changed.append('cols')
    if 'date_range' in node:
        column = node.get('date_column')
        if not isinstance(column, str) or column not in columns:
            column = next((name for name in DATE_COLUMNS if name in columns), None)
        if column is not None:
            bounds = _date_bounds(path, column)
            if bounds is not None and list(node['date_range']) != bounds:
                node['date_range'] = bounds
                changed.append('date_range')
    return changed


def _dump_indent(text: str) -> int:
    for line in text.splitlines()[1:]:
        if line.strip():
            return len(line) - len(line.lstrip(' '))
    return 2


def refresh_manifest_files(manifest_path: Path | str, store_root: Path | str,
                           written: Iterable[Path | str], *,
                           required: bool = False) -> list[str]:
    """把本次写盘的正式文件按磁盘实测回写进指定 manifest。

    ``manifest_path`` 不存在时直接返回空列表（测试用的临时 store 不凭空造登记）；
    ``required=True`` 时，manifest 存在但找不到某文件的登记条目即抛错——那属于
    漏登记，不是「无需回写」。返回实际命中并重算的路径（相对 ``store_root``）。
    """
    target = Path(manifest_path)
    if not target.is_file():
        return []
    root = Path(store_root)
    original = target.read_text(encoding='utf-8')
    payload = json.loads(original)
    if not isinstance(payload, dict):
        raise TypeError(f'{target} 顶层不是 JSON 对象，无法回写统计字段')
    nodes = _candidates(payload, root)
    refreshed: list[str] = []
    for item in written:
        path = Path(item)
        rel = path.relative_to(root).as_posix() if path.is_relative_to(root) else str(path)
        facts = file_facts(path)
        hits = [node for node, fallback, base in nodes
                if _declares(node, fallback, path, root, base)]
        if not hits:
            if required:
                raise RuntimeError(
                    f'{target} 没有 {rel} 的登记条目，本次写盘无法回写；'
                    '请先补登记再写盘（DG-3）')
            continue
        for node in hits:
            _apply(node, path, facts)
        refreshed.append(rel)
    if not refreshed:
        return []
    text = json.dumps(payload, ensure_ascii=False, indent=_dump_indent(original))
    if original.endswith('\n') and not text.endswith('\n'):
        text += '\n'
    if text != original:
        write_text_atomic(text, target)
    return refreshed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='按磁盘实测回写 manifest 统计字段')
    parser.add_argument('--store-root', default=None, help='local_store 根（默认按项目定位）')
    parser.add_argument('--manifest', required=True, help='manifest 路径，相对 store 根或绝对路径')
    parser.add_argument('files', nargs='+', help='要回写的文件，相对 store 根或绝对路径')
    args = parser.parse_args(argv)

    if args.store_root:
        root = Path(args.store_root)
    else:
        from src.data.local_store.paths import default_store_root

        root = default_store_root()
    manifest = Path(args.manifest)
    if not manifest.is_absolute():
        manifest = root / manifest
    written = [Path(item) if Path(item).is_absolute() else root / item for item in args.files]
    missing = [str(path) for path in written if not path.is_file()]
    if missing:
        logger.error('文件不存在：{}', missing)
        return 2
    before = manifest.read_bytes() if manifest.is_file() else b''
    refreshed = refresh_manifest_files(manifest, root, written, required=True)
    after = manifest.read_bytes() if manifest.is_file() else b''
    logger.info('已回写 {} 条 -> {}', len(refreshed), manifest)
    for rel in refreshed:
        logger.info('  - {}', rel)
    logger.info('manifest 内容有变化' if before != after
                else 'manifest 内容无变化（登记值与磁盘一致）')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
