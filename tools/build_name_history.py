#!/usr/bin/env python3

"""从 tushare ``namechange`` 重建 ``meta/name_history.parquet``（PIT 历史简称）。

产物::

    data/local_store/meta/name_history.parquet          # 8 列，与 2026-11-09 版一致
    data/local_store/meta/name_history_coverage.json    # 覆盖度 + 验收数字
    reports/name_history_build_<YYYYMMDD>/report.json   # 验收 + 三方差异分类计数

口径
====
``name_history`` 只回答「某只票在某个历史时点叫什么」，是历史简称的唯一权威；
**当前简称仍以 ``meta/names.json`` 为准**（通达信当日快照，覆盖全域名），
上市日仍以 ``stock_basic.list_date`` 为准。三者只做对账，不互相覆盖——
详见 ``src/data/name_consistency.py``。

数据链路
========
复用 ``tools/build_st_history_from_tushare.py`` 的分页拉取与
``tmp/st_namechange_raw/`` 断点缓存（缺列会自动重拉该页），这里只做：
``原始分片 → 去重 → 日期规整 → 8 列落盘``。写盘前 ``copy2`` 到
``meta/_backup/``，再用 ``write_parquet_atomic`` 原子替换。

用法::

    .venv\\Scripts\\python.exe tools\\build_name_history.py            # 只比对，不写盘
    .venv\\Scripts\\python.exe tools\\build_name_history.py --offline  # 只用缓存
    .venv\\Scripts\\python.exe tools\\build_name_history.py --apply    # 备份后覆盖
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
for _path in (str(ROOT), str(ROOT / 'tools')):
    if _path not in sys.path:
        sys.path.insert(0, _path)

# tools/ 不是包，按文件名导入同目录脚本以共用分页拉取与备份逻辑
from build_st_history_from_tushare import (  # noqa: E402
    RAW_DIR,
    _parse_day,
    _sha256,
    backup_existing,
    fetch_raw,
)

from src.data.atomic_write import write_parquet_atomic, write_text_atomic  # noqa: E402
from src.data.name_consistency import build_tables  # noqa: E402

TARGET = ROOT / 'data' / 'local_store' / 'meta' / 'name_history.parquet'
COVERAGE = ROOT / 'data' / 'local_store' / 'meta' / 'name_history_coverage.json'
MANIFEST = ROOT / 'data' / 'local_store' / 'meta' / 'name_history.manifest.json'
BACKUP_DIR = ROOT / '_backup' / 'meta'
REPORT_ROOT = ROOT / 'reports'

SOURCE = 'tushare_namechange'
#: tushare ``namechange`` 全字段；比 ST 重建多取 ``ann_date``（现有落盘文件有这一列）
FIELDS = ('ts_code', 'name', 'start_date', 'end_date', 'ann_date', 'change_reason')
SCHEMA: dict[str, Any] = {
    'ts_code': pl.Utf8,
    'name': pl.Utf8,
    'start_date': pl.Date,
    'end_date': pl.Date,
    'ann_date': pl.Date,
    'change_reason': pl.Utf8,
    'source': pl.Utf8,
    'fetched_at': pl.Datetime,
}
_NULL_TEXT = {'', 'NAN', 'NAT', 'NONE'}


def _clean_text(value: Any) -> str:
    text = str(value if value is not None else '').strip()
    return '' if text.upper() in _NULL_TEXT else text


def normalize(raw: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, int]]:
    """原始分片 → 8 列区间表；返回 (表, 丢弃计数)。"""
    dropped = {'bad_start': 0, 'empty_name': 0, 'duplicate': 0}
    rows: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    now = datetime.now()
    for item in raw.to_dicts():
        name = _clean_text(item.get('name'))
        if not name:
            dropped['empty_name'] += 1
            continue
        code = _clean_text(item.get('ts_code'))
        start = _parse_day(_clean_text(item.get('start_date')) or None)
        if not code or start is None:
            dropped['bad_start'] += 1
            continue
        end = _parse_day(_clean_text(item.get('end_date')) or None)
        ann = _parse_day(_clean_text(item.get('ann_date')) or None)
        key = (code, name, start, end, ann)
        if key in seen:
            dropped['duplicate'] += 1
            continue
        seen.add(key)
        rows.append({
            'ts_code': code, 'name': name, 'start_date': start,
            'end_date': end, 'ann_date': ann,
            'change_reason': _clean_text(item.get('change_reason')),
            'source': SOURCE, 'fetched_at': now,
        })
    if not rows:
        return pl.DataFrame(schema=SCHEMA), dropped
    frame = pl.DataFrame(rows, schema=SCHEMA, strict=False).sort(
        ['ts_code', 'start_date', 'end_date'], nulls_last=True)
    return frame, dropped


def _stats(frame: pl.DataFrame) -> dict[str, Any]:
    """验收三项 + 覆盖度：行数 / 只数 / 开区间只数。"""
    if frame.height == 0:
        return {'rows': 0, 'codes': 0, 'open_rows': 0, 'open_codes': 0}
    open_frame = frame.filter(pl.col('end_date').is_null())
    return {
        'rows': frame.height,
        'codes': frame['ts_code'].n_unique(),
        'open_rows': open_frame.height,
        'open_codes': open_frame['ts_code'].n_unique(),
        'min_start': str(frame['start_date'].min()),
        'max_start': str(frame['start_date'].max()),
        'ann_date_null': int(frame['ann_date'].is_null().sum()),
        'name_change_codes': frame['ts_code'].n_unique()
        - frame.group_by('ts_code').len().filter(pl.col('len') == 1).height,
    }


def compare_with_existing(frame: pl.DataFrame, target: Path) -> dict[str, Any]:
    """与现有落盘文件比对：行数 / 只数 / 开区间 / 逐行四元组差异。"""
    if not target.is_file():
        return {'target_exists': False}
    existing = pl.read_parquet(target)
    mine_key = {(r['ts_code'], r['name'], r['start_date'], r['end_date'])
                for r in frame.select(
                    'ts_code', 'name', 'start_date', 'end_date').to_dicts()}
    ref_key = {(r['ts_code'], r['name'], r['start_date'], r['end_date'])
               for r in existing.select(
                   'ts_code', 'name', 'start_date', 'end_date').to_dicts()}
    only_new = sorted(mine_key - ref_key)
    only_old = sorted(ref_key - mine_key)
    return {
        'target_exists': True,
        'existing': _stats(existing),
        'only_in_new': len(only_new),
        'only_in_existing': len(only_old),
        'diff_rows': len(only_new) + len(only_old),
        'only_in_new_sample': [list(row) for row in only_new[:5]],
        'only_in_existing_sample': [list(row) for row in only_old[:5]],
        'schema_same': existing.columns == frame.columns,
    }


def write_build_report(payload: dict[str, Any], day: date) -> Path:
    out_dir = REPORT_ROOT / f'name_history_build_{day:%Y%m%d}'
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / 'report.json'
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str),
                    encoding='utf-8')
    return path


def _parse_args(argv: Iterable[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='从 tushare namechange 重建 meta/name_history.parquet')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--check', action='store_true', help='只比对不写盘（默认）')
    mode.add_argument('--apply', action='store_true', help='备份后原子覆盖目标文件')
    parser.add_argument('--offline', action='store_true', help='只用本地缓存，不联网')
    parser.add_argument('--refresh', action='store_true', help='忽略分片缓存重新拉取')
    parser.add_argument('--cache-dir', default=str(RAW_DIR))
    parser.add_argument('--target', default=str(TARGET))
    parser.add_argument('--backup-dir', default=str(BACKUP_DIR))
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    target = Path(args.target)
    raw = fetch_raw(Path(args.cache_dir), offline=args.offline,
                    refresh=args.refresh, fields=FIELDS)
    frame, dropped = normalize(raw)
    report: dict[str, Any] = {
        'raw_rows': raw.height,
        'dropped': dropped,
        'built': _stats(frame),
    }
    if frame.height == 0:
        report['error'] = '规整结果为空，拒绝写盘'
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
        return 2

    report['diff_vs_existing'] = compare_with_existing(frame, target)
    store = target.parents[1]  # data/local_store
    report['reconcile'] = {
        name: table.group_by('category').len().sort('category').to_dicts()
        for name, table in build_tables(store).items()
    }

    if args.apply:
        backup = backup_existing(target, Path(args.backup_dir))
        write_parquet_atomic(frame, target)
        coverage = {
            'dataset': 'name_history',
            'source': SOURCE,
            'authority_for': '历史简称（PIT）；当前简称以 meta/names.json 为准',
            'generated_at': datetime.now().isoformat(timespec='seconds'),
            'cache_dir': str(args.cache_dir),
            'raw_rows': raw.height,
            'dedup_dropped': dropped,
            **_stats(frame),
            'target_sha256': _sha256(target),
            'backup': str(backup) if backup else None,
            'diff_vs_previous': {
                'only_in_new': report['diff_vs_existing'].get('only_in_new'),
                'only_in_existing': report['diff_vs_existing'].get('only_in_existing'),
            },
        }
        COVERAGE.write_text(
            json.dumps(coverage, ensure_ascii=False, indent=2, default=str),
            encoding='utf-8')
        manifest = {
            'dataset': 'name_history',
            'semantics': 'A 股历史简称区间表，每行一个 [start_date, end_date) 简称段',
            'source': SOURCE,
            'authority': '历史简称以本表为准；当前简称以 meta/names.json 为准',
            'path': 'meta/name_history.parquet',
            'produced_by': 'tools/build_name_history.py',
            'generated_at': datetime.now().isoformat(timespec='seconds'),
            'rows': frame.height,
            'bytes': target.stat().st_size,
            'sha256': _sha256(target),
            'columns': list(frame.columns),
            'date_range_note': '区间表含 start_date/end_date 两列，无单一日期列，故不登记 date_range',
        }
        write_text_atomic(
            json.dumps(manifest, ensure_ascii=False, indent=2, default=str) + '\n',
            MANIFEST)
        report['written'] = {
            'target': str(target), 'rows': frame.height,
            'sha256': _sha256(target), 'coverage': str(COVERAGE),
            'manifest': str(MANIFEST),
            'backup': str(backup) if backup else None,
        }
    report['report_path'] = str(write_build_report(report, date.today()))
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == '__main__':
    sys.exit(main())
