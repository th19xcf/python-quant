#!/usr/bin/env python3

r"""``data/local_store/factors/`` 目录级 manifest 生成。

背景：``factors/`` 是复权因子与分红、换手的正式目录，但长期没有目录级
manifest，覆盖信息只散落在 ``meta.json`` / ``fund_meta.json`` 与各文件
自身里。按 DG-3 / DG-4，正式目录需要一份可机读的记账：逐文件 SHA256、
字节数、行数、列数、标的数、日期区间，以及上游（行情分片、股本表）的
最新 mtime，全部如实取自磁盘，不做任何推算。

口径边界：

- 只统计 ``factors/`` **顶层**文件；``rebuild/`` 是任务进度目录（DG-5 的
  staging 语义），其 ``manifest.json`` 单列在 ``sub_manifests``，不当作数据文件。
  ``turnover_parts/`` 已于 2026-11-12 按 DG-32 迁到仓库根 ``_staging/turnover_parts/``，
  不再属于本目录，故不再登记。
- 写盘一律走 :func:`src.data.atomic_write.write_text_atomic`（DG-9）。
- 本模块只读数据文件，不修改任何 parquet / csv 内容。
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from src.data.atomic_write import write_text_atomic
from src.data.local_store.paths import default_store_root

MANIFEST_NAME = 'manifest.json'
#: 逐文件记账时用来取日期区间的候选列（按顺序取第一个命中的）。
_DATE_COLUMNS = ('trade_date', 'date', 'end_date', 'in_date', 'ex_date')
#: 已知文件的语义与产出方；未列出的文件按 ``data`` 记账。
_ROLES: dict[str, tuple[str, str]] = {
    'stock_adj_factor.parquet': (
        '个股复权因子快照（唯一来源；消费方按 LocalQuoteStore 读取）',
        'python -m src.data.local_store rebuild-factors'),
    'stock_dividend.parquet': (
        '个股分红事件表（因子重建的上游事件源）',
        'python tools/collect_stock_dividend.py'),
    'stock_turnover.parquet': ('个股换手率派生表', 'python -m src.data.local_store sync-turnover'),
    'fund_adj_factor.parquet': ('基金复权因子快照', 'tools/build_fund_corporate_actions.py'),
    'fund_dividend.parquet': ('场内基金分红事件表', 'tools/build_fund_corporate_actions.py'),
    'fund_share_change.parquet': ('场内基金份额折算 / 分拆事件表', 'tools/build_fund_corporate_actions.py'),
    'covered_codes.txt': ('个股因子快照已覆盖标的清单（sidecar）', 'rebuild-factors 收尾'),
    'fund_covered_codes.txt': ('基金因子快照已覆盖标的清单（sidecar）', 'tools/build_fund_corporate_actions.py'),
    'meta.json': ('个股因子构建状态（最近一次 rebuild 的统计与哈希）', 'rebuild-factors 收尾'),
    'fund_meta.json': ('基金因子构建状态', 'tools/build_fund_corporate_actions.py'),
}
_SUB_MANIFESTS = (Path('rebuild') / MANIFEST_NAME,)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _mtime(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec='seconds')


def _table_entry(path: Path, relative: str) -> dict[str, Any]:
    frame = pl.scan_parquet(str(path))
    schema = frame.collect_schema()
    names = list(schema.names())
    entry: dict[str, Any] = {
        'path': relative,
        'kind': 'parquet',
        'bytes': path.stat().st_size,
        'sha256': _sha256(path),
        'mtime': _mtime(path),
        'rows': int(frame.select(pl.len()).collect().item()),
        'cols': len(names),
        'columns': names,
    }
    if 'ts_code' in names:
        entry['codes'] = int(frame.select(pl.col('ts_code').n_unique()).collect().item())
    for column in _DATE_COLUMNS:
        if column not in names:
            continue
        low, high = frame.select(
            pl.col(column).min().alias('low'), pl.col(column).max().alias('high')
        ).collect().row(0)
        entry['date_column'] = column
        entry['date_range'] = [
            None if low is None else str(low), None if high is None else str(high)]
        break
    return entry


def _text_entry(path: Path, relative: str, kind: str) -> dict[str, Any]:
    return {
        'path': relative,
        'kind': kind,
        'bytes': path.stat().st_size,
        'sha256': _sha256(path),
        'mtime': _mtime(path),
    }


def _upstream(store_root: Path) -> dict[str, Any]:
    """上游输入的最新 mtime（只 stat，不读内容）。"""
    stock_dir = store_root / 'quotes' / 'stock'
    shards = sorted(stock_dir.rglob('*.parquet')) if stock_dir.is_dir() else []
    latest = max((p.stat().st_mtime for p in shards), default=None)
    payload: dict[str, Any] = {
        'quotes_stock': {
            'path': 'quotes/stock',
            'files': len(shards),
            'latest_mtime': None if latest is None else
            datetime.fromtimestamp(latest).isoformat(timespec='seconds'),
        },
    }
    shares = store_root / 'meta' / 'factors' / 'stock_shares.parquet'
    if shares.is_file():
        payload['stock_shares'] = {
            'path': 'meta/factors/stock_shares.parquet',
            'bytes': shares.stat().st_size,
            'sha256': _sha256(shares),
            'mtime': _mtime(shares),
            'rows': int(pl.scan_parquet(str(shares)).select(pl.len()).collect().item()),
        }
    return payload


def collect_factors_manifest(
    store_root: str | Path | None = None,
) -> dict[str, Any]:
    """按磁盘现状生成 manifest 载荷（不写盘）。

    Args:
        store_root: 列存根目录；``None`` 时取项目默认 ``data/local_store``。
    """
    root = Path(store_root) if store_root is not None else default_store_root()
    factors_dir = root / 'factors'
    files: dict[str, Any] = {}
    table_rows = 0
    table_bytes = 0
    if factors_dir.is_dir():
        for path in sorted(factors_dir.iterdir()):
            if not path.is_file() or path.name == MANIFEST_NAME:
                continue
            relative = str(path.relative_to(root)).replace('\\', '/')
            if path.suffix == '.parquet':
                files[path.name] = _table_entry(path, relative)
                table_rows += files[path.name]['rows']
                table_bytes += files[path.name]['bytes']
            else:
                kind = 'state' if path.name.endswith('.json') else 'sidecar'
                files[path.name] = _text_entry(path, relative, kind)
            role, producer = _ROLES.get(path.name, ('未登记语义', '未登记产出方'))
            files[path.name]['semantics'] = role
            files[path.name]['produced_by'] = producer

    sub_manifests = []
    for relative in _SUB_MANIFESTS:
        path = factors_dir / relative
        if path.is_file():
            sub_manifests.append({
                'path': str(Path('factors') / relative).replace('\\', '/'),
                'bytes': path.stat().st_size,
                'sha256': _sha256(path),
                'mtime': _mtime(path),
            })

    return {
        'dataset': 'factors',
        'version': 1,
        'status': 'completed',
        'path': 'data/local_store/factors',
        'semantics': (
            '个股/基金复权因子快照与分红、换手事件表的正式目录；'
            '一行 = 一个 (标的, 交易日) 或一个 (标的, 除权事件)，口径见各文件 semantics'
        ),
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'produced_by': 'src/data/factors_manifest.py（rebuild-factors 收尾自动刷新）',
        'files': files,
        'totals': {
            'parquet_files': sum(1 for v in files.values() if v['kind'] == 'parquet'),
            'other_files': sum(1 for v in files.values() if v['kind'] != 'parquet'),
            'rows': table_rows,
            'bytes': table_bytes,
        },
        'upstream': _upstream(root),
        'sub_manifests': sub_manifests,
        'counting': {
            'sha256': '逐文件流式计算（1 MiB 分块），不含本 manifest 自身',
            'rows': '全部顶层 *.parquet 的 num_rows 之和，跨表相加仅表示规模',
            'bytes': '全部顶层 *.parquet 的实际字节数之和（os.stat）',
            'codes': '各表 ts_code 的唯一值个数，逐表独立统计，不可跨表相加',
        },
        'method': {
            'write': 'write_text_atomic（临时文件 → fsync → os.replace），单文件原子',
            'scope': '仅 factors/ 顶层；rebuild/ 为任务进度目录，见 sub_manifests',
        },
    }


def write_factors_manifest(store_root: str | Path | None = None) -> Path:
    """生成并原子写入 ``factors/manifest.json``，返回落盘路径。"""
    root = Path(store_root) if store_root is not None else default_store_root()
    target = root / 'factors' / MANIFEST_NAME
    payload = collect_factors_manifest(root)
    write_text_atomic(
        json.dumps(payload, ensure_ascii=False, indent=2) + '\n', target)
    return target
