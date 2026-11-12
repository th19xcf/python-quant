#!/usr/bin/env python3

"""快照目录的目录级 manifest 生成（单表 history 与按标的/分区拆分的族都适用）。

语义类「一个目录 + 目录级 manifest」——合法语义，不是占位值：

- 正式路径有一条固定文件名（如 ``meta/industry/industry_history.parquet``），
  manifest 挂在**目录**上，用 ``files`` 数组登记该目录下的正式文件；
- 历史上的「日期快照族」形态（``industry_<YYYYMMDD>.parquet``）已于 2026-11-13
  合并为单表，旧口径逐条留在 ``docs/数据权威路径清单.md`` 的「已废弃路径」表；
- 单份文件按 ``as_of`` 纵向累积、只增不重写（历史日期不可补）；
- 因此不给每个日期文件各写一份单文件 manifest——那会造出多个「单文件正式路径」，
  与固定文件名并存，违反 DG-1（每类语义只允许一个正式路径）。

调用方是各快照的生产者（``src/data/industry.py``、``src/data/risk_flags.py``）：
每次落盘后调用本模块重算一遍，保证重跑不会丢登记。

除 ``files`` 与 ``total_rows`` / ``total_bytes`` 外不写入任何时间戳：manifest
内容完全由磁盘上那批文件决定，同一批文件重复运行结果逐字节相同。
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import polars as pl

from src.data.atomic_write import write_text_atomic


def _sha256(path: Path, chunk: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for block in iter(lambda: handle.read(chunk), b''):
            digest.update(block)
    return digest.hexdigest()


def _parquet_rows_columns(path: Path) -> tuple[int | None, list[str] | None]:
    """读 parquet 行数与列名（只读文件元数据，不读数据）。"""
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return None, None
    meta = pq.ParquetFile(str(path))
    return meta.metadata.num_rows, list(meta.schema_arrow.names)


def _date_span(path: Path, column: str) -> list[str] | None:
    """取某日期列的最小/最大值；列不存在或全空返回 ``None``。"""
    try:
        series = pl.read_parquet(path, columns=[column])[column]
    except Exception:  # noqa: BLE001 - 缺列只让该份快照不登记日期区间
        return None
    low, high = series.min(), series.max()
    if low is None or high is None:
        return None
    return [str(low)[:10], str(high)[:10]]


def write_snapshot_manifest(
    store_root: Path | str,
    directory: Path | str,
    *,
    prefix: str,
    dataset: str,
    semantics: str,
    source: str,
    authority: str,
    path_pattern: str,
    consumers: list[str],
    produced_by: str,
    as_of_column: str = 'as_of',
) -> Path:
    """重算并落盘 ``directory/manifest.json``，返回该路径。

    Args:
        store_root: 本地列存根目录，用于把文件路径写成相对 ``data/local_store``
            的形式（``files[].path`` 的登记基准）。
        directory: 快照所在目录（如 ``{store_root}/meta/industry``）。
        prefix: 文件名前缀（如 ``industry_``），只登记 ``<prefix>*.parquet``；
            用于排除同目录下的非快照文件（历史候选版本等）。
        path_pattern: 通配形式的正式路径，写进 manifest 供人读。
        consumers: 消费方清单，逐条写清「谁读、怎么读」。
        produced_by: 生产者标识，便于下次改动时找到写入口。
        as_of_column: 快照内表示快照日的列名，用于登记 ``date_range``。
    """
    base = Path(directory)
    root = Path(store_root)
    entries: list[dict[str, Any]] = []
    for path in sorted(base.glob(f'{prefix}*.parquet')):
        rows, columns = _parquet_rows_columns(path)
        entry: dict[str, Any] = {
            'path': path.relative_to(root).as_posix(),
            'rows': rows,
            'bytes': path.stat().st_size,
            'sha256': _sha256(path),
            'columns': columns,
        }
        span = _date_span(path, as_of_column)
        if span is not None:
            entry['date_column'] = as_of_column
            entry['date_range'] = span
        entries.append(entry)

    payload = {
        'dataset': dataset,
        'semantics': semantics,
        'source': source,
        'authority': authority,
        'path_pattern': path_pattern,
        'consumers': consumers,
        'produced_by': produced_by,
        'files': entries,
        'total_rows': sum(int(entry['rows'] or 0) for entry in entries),
        'total_bytes': sum(int(entry['bytes']) for entry in entries),
    }
    manifest_path = base / 'manifest.json'
    text = json.dumps(payload, ensure_ascii=False, indent=2) + '\n'
    write_text_atomic(text, manifest_path)
    return manifest_path
