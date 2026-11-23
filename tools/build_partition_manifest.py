#!/usr/bin/env python3

r"""为分区数据集生成目录级 ``manifest.json``（分区清单 + 总行数 + max_date + 生成时间）。

格式与 ``meta/st/daily/manifest.json`` 一致：``range`` / ``max_date`` / ``rows`` /
``files[]``（path / rows / start / end / bytes），并补 ``trading_days``。
幂等：内容（文件清单、行数、日期边界）未变时不重写，保留原 ``built_at``。

用法::

    .venv\\Scripts\\python.exe tools\\build_partition_manifest.py --only limit_daily
    .venv\\Scripts\\python.exe tools\\build_partition_manifest.py --only limit_daily,margin_detail
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data.atomic_write import write_text_atomic  # noqa: E402
from src.data.dataset_contracts import (  # noqa: E402
    contract_keys,
    iter_contracts,
    resolve_files,
)

DEFAULT_ROOT = ROOT / 'data' / 'local_store'


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)[:10]


def build_payload(contract: Any, root: Path) -> dict[str, Any]:
    files = sorted(resolve_files(contract, root))
    if not files:
        raise FileNotFoundError(f'{contract.key} 无分区文件：{contract.pattern}')
    entries: list[dict[str, Any]] = []
    total = 0
    start: str | None = None
    end: str | None = None
    for path in files:
        stats = pl.scan_parquet(str(path)).select(
            pl.len().alias('rows'),
            pl.col(contract.date_column).min().alias('start'),
            pl.col(contract.date_column).max().alias('end'),
        ).collect().row(0, named=True)
        rows = int(stats['rows'])
        part_start, part_end = _iso(stats['start']), _iso(stats['end'])
        entries.append({
            'path': path.name,
            'rows': rows,
            'start': part_start,
            'end': part_end,
            'bytes': path.stat().st_size,
        })
        total += rows
        if part_start and (start is None or part_start < start):
            start = part_start
        if part_end and (end is None or part_end > end):
            end = part_end
    trading_days = int(pl.scan_parquet([str(p) for p in files]).select(
        pl.col(contract.date_column).n_unique()).collect().item())
    return {
        'mode': 'full',
        'range': {'start': start, 'end': end},
        'max_date': end,
        'trading_days': trading_days,
        'rows': total,
        'files': entries,
    }


def write_manifest(contract: Any, root: Path, *, dry_run: bool = False) -> str:
    target = root / str(contract.manifest)
    payload = build_payload(contract, root)
    old: dict[str, Any] = {}
    if target.is_file():
        try:
            old = json.loads(target.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            old = {}
    # 幂等：去掉生成时间后完全一致即视为未变，保留原 built_at、不重写文件
    if {k: v for k, v in old.items() if k != 'built_at'} == payload:
        return f'{contract.key} 未变（保留 built_at={old.get("built_at")}）'
    payload['built_at'] = datetime.now().isoformat(timespec='seconds')
    payload = {'mode': payload.pop('mode'), 'built_at': payload.pop('built_at'), **payload}
    if dry_run:
        return f'{contract.key} 将重写（rows={payload["rows"]:,}，max_date={payload["max_date"]}）'
    write_text_atomic(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', target)
    return f'{contract.key} 已写 {contract.manifest}（{len(payload["files"])} 分区，{payload["rows"]:,} 行，max_date={payload["max_date"]}）'


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='生成分区数据集目录级 manifest')
    parser.add_argument('--root', type=Path, default=DEFAULT_ROOT)
    parser.add_argument('--only', required=True, help='逗号分隔的数据集键')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args(argv)

    keys = [k.strip() for k in args.only.split(',') if k.strip()]
    unknown = [k for k in keys if k not in contract_keys()]
    if unknown:
        print(f'未注册的数据集：{unknown}', file=sys.stderr)
        return 2
    for contract in iter_contracts(keys):
        if not contract.manifest:
            print(f'[跳过] {contract.key} 契约未声明目录级 manifest', file=sys.stderr)
            continue
        print(f'[manifest] {write_manifest(contract, args.root, dry_run=args.dry_run)}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
