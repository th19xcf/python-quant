#!/usr/bin/env python3

"""从 tushare ``namechange`` 重建 ST 历史区间表（与现有落盘文件同 schema）。

产物（默认只比对，``--apply`` 才写）::

    data/local_store/meta/st_history.parquet

schema（6 列，与 2026-11-09 落盘文件一致）::

    ts_code(str) / st_flag(bool) / start_date(date) / end_date(date|null)
    / st_type(str) / source(str)

推导规则
========
1. 证券简称先剥离股改标记 ``G``，再按前缀判定风险警示类型：
   含 ``*ST`` → ``*ST``；含 ``ST`` → ``ST``；以 ``PT`` 开头或名称以 ``*``
   开头（如 ``*金亚``）→ ``其他风险警示``；
2. 只保留判定为风险警示的简称记录，按 ``(ts_code, name, start_date,
   end_date)`` 去重（tushare 偶有完全重复行）；
3. 同一标的按 ``start_date`` 升序，把「``st_type`` 相同且紧邻（下一段起点
   <= 上一段终点 + 1 天）」的区间合并；``end_date`` 为空表示仍未摘帽的
   开区间，合并后保持为空。

可复现性
========
- 原始响应按页缓存在 ``tmp/st_namechange_raw/part-<offset>.parquet``，
  ``manifest.json`` 记录已完成页；重复执行命中缓存不再联网（``--refresh``
  强制重拉，``--offline`` 只用缓存）；
- 推导是纯函数，同一份原始数据在相同 polars 版本下重写字节一致：
  ``--check`` 会把结果写两份到临时目录并比对 SHA256 自证。

用法::

    python tools/build_st_history_from_tushare.py            # 重建 + 比对，不写盘
    python tools/build_st_history_from_tushare.py --check    # 同上（显式模式）
    python tools/build_st_history_from_tushare.py --apply    # 备份后原子覆盖
    python tools/build_st_history_from_tushare.py --offline  # 只用本地缓存
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import tempfile
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data.atomic_write import write_parquet_atomic  # noqa: E402

TARGET = ROOT / 'data' / 'local_store' / 'meta' / 'st_history.parquet'
BACKUP_DIR = ROOT / 'data' / 'local_store' / '_backup'
RAW_DIR = ROOT / 'tmp' / 'st_namechange_raw'
TOKEN_FILE = ROOT / '.secrets' / 'tushare_token.txt'

SOURCE = 'tushare_namechange'
PAGE_LIMIT = 5000
REQUEST_INTERVAL = 0.4
RAW_FIELDS = ('ts_code', 'name', 'start_date', 'end_date', 'change_reason')

SCHEMA: dict[str, Any] = {
    'ts_code': pl.Utf8,
    'st_flag': pl.Boolean,
    'start_date': pl.Date,
    'end_date': pl.Date,
    'st_type': pl.Utf8,
    'source': pl.Utf8,
}
RAW_SCHEMA: dict[str, Any] = {
    'ts_code': pl.Utf8,
    'name': pl.Utf8,
    'start_date': pl.Utf8,
    'end_date': pl.Utf8,
    'change_reason': pl.Utf8,
}


# ----------------------------------------------------------------------
# 纯函数：名称分类 → 区间合并
# ----------------------------------------------------------------------
def classify_name(name: Any) -> str | None:
    """按证券简称判定风险警示类型；非风险警示返回 ``None``。"""
    if name is None:
        return None
    text = str(name).strip().upper()
    if not text:
        return None
    # G 是股改标记（G*ST / GST），不属于风险警示本身，先剥离再判
    body = text[1:] if text.startswith('G') else text
    if 'ST' in body:
        return '*ST' if '*ST' in body else 'ST'
    if body.startswith('PT'):
        return '其他风险警示'
    if text.startswith('*'):
        # 名称以 * 开头但不含 ST（如 *金亚 / *欣泰）：交易所口径的高风险警示
        return '其他风险警示'
    return None


def _parse_day(value: Any) -> date | None:
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    for fmt, size in (('%Y%m%d', 8), ('%Y-%m-%d', 10)):
        try:
            return datetime.strptime(text[:size], fmt).date()
        except ValueError:
            continue
    return None


def _candidates(raw: pl.DataFrame) -> list[dict[str, Any]]:
    """原始名称变更 → 去重后的候选段（只保留风险警示期）。"""
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for item in raw.to_dicts():
        st_type = classify_name(item.get('name'))
        if st_type is None:
            continue
        code = str(item.get('ts_code') or '').strip()
        start = _parse_day(item.get('start_date'))
        if not code or start is None:
            continue
        end = _parse_day(item.get('end_date'))
        name = str(item.get('name') or '')
        key = (code, name, start.isoformat(), end.isoformat() if end else '')
        if key in seen:
            continue
        seen.add(key)
        out.append({'ts_code': code, 'name': name, 'st_type': st_type,
                    'start': start, 'end': end})
    # name 参与排序：同日多条同类型记录按名称稳定合并，保证可复现
    out.sort(key=lambda row: (row['ts_code'], row['start'], row['name']))
    return out


def derive_intervals(raw: pl.DataFrame) -> pl.DataFrame:
    """候选段 → 合并相邻后的区间表（纯函数，不触碰文件系统）。"""
    if raw.height == 0:
        return pl.DataFrame(schema=SCHEMA)
    merged: list[dict[str, Any]] = []
    for row in _candidates(raw):
        last = merged[-1] if merged else None
        if (
            last is not None
            and last['ts_code'] == row['ts_code']
            and last['st_type'] == row['st_type']
            and last['end_date'] is not None
            and row['start'] <= last['end_date'] + timedelta(days=1)
        ):
            # 开区间优先：任一段未摘帽，合并后保持开区间
            if row['end'] is None:
                last['end_date'] = None
            else:
                last['end_date'] = max(last['end_date'], row['end'])
            continue
        merged.append({'ts_code': row['ts_code'], 'st_flag': True,
                       'start_date': row['start'], 'end_date': row['end'],
                       'st_type': row['st_type'], 'source': SOURCE})
    if not merged:
        return pl.DataFrame(schema=SCHEMA)
    frame = pl.DataFrame(merged, schema=SCHEMA)
    return frame.sort(['ts_code', 'start_date', 'end_date'], nulls_last=True)


# ----------------------------------------------------------------------
# 原始数据：分页抓取 + 分片缓存 + 断点续跑
# ----------------------------------------------------------------------
def _pro_api() -> Any:
    import tushare as ts  # noqa: PLC0415

    if not TOKEN_FILE.is_file():
        raise RuntimeError(f'tushare token 不存在: {TOKEN_FILE}')
    token = TOKEN_FILE.read_text(encoding='utf-8').strip()
    if not token:
        raise RuntimeError('tushare token 为空')
    return ts.pro_api(token)


def _fetch_page(pro: Any, offset: int) -> list[dict[str, Any]]:
    last_exc: Exception | None = None
    for attempt in range(4):
        try:
            frame = pro.namechange(fields=','.join(RAW_FIELDS),
                                   limit=PAGE_LIMIT, offset=offset)
            if frame is None:
                return []
            return list(frame.to_dict('records'))
        except Exception as exc:  # noqa: BLE001  网络/接口异常统一退避重试
            last_exc = exc
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f'namechange 拉取失败（offset={offset}）: {last_exc}')


def _write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


def fetch_raw(cache_dir: Path, *, offline: bool = False,
              refresh: bool = False) -> pl.DataFrame:
    """抓取（或读缓存）全量名称变更史；返回原始 5 列。"""
    cache_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = cache_dir / 'manifest.json'
    manifest: dict[str, Any] = {}
    if manifest_path.is_file():
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    pages: set[int] = set(manifest.get('pages') or [])
    pro: Any = None
    offset = 0
    while True:
        part = cache_dir / f'part-{offset:07d}.parquet'
        if not refresh and part.is_file():
            rows = pl.read_parquet(part).height
            pages.add(offset)
        else:
            if offline:
                raise RuntimeError(f'离线模式缺少缓存分片: {part}')
            if pro is None:
                pro = _pro_api()
            records = _fetch_page(pro, offset)
            rows = len(records)
            if rows:
                write_parquet_atomic(
                    pl.DataFrame(records, schema=RAW_SCHEMA, strict=False), part)
                time.sleep(REQUEST_INTERVAL)
            pages.add(offset)
            _write_manifest(manifest_path, {
                'pages': sorted(pages), 'page_size': PAGE_LIMIT,
                'fetched_at': datetime.now().isoformat(timespec='seconds')})
        if rows < PAGE_LIMIT:
            break
        offset += PAGE_LIMIT
    frames = [pl.read_parquet(path) for path in sorted(cache_dir.glob('part-*.parquet'))]
    raw = pl.concat(frames, how='vertical_relaxed') if frames \
        else pl.DataFrame(schema=RAW_SCHEMA)
    _write_manifest(manifest_path, {
        'pages': sorted(pages), 'page_size': PAGE_LIMIT, 'rows': raw.height,
        'complete': True,
        'fetched_at': datetime.now().isoformat(timespec='seconds')})
    return raw


# ----------------------------------------------------------------------
# 比对 / 写盘
# ----------------------------------------------------------------------
def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _key_set(frame: pl.DataFrame) -> set[tuple[str, str, str, str]]:
    return {
        (str(row['ts_code']),
         row['start_date'].isoformat() if row.get('start_date') else '',
         row['end_date'].isoformat() if row.get('end_date') else '',
         str(row['st_type']))
        for row in frame.to_dicts()
    }


def compare_frames(mine: pl.DataFrame, ref: pl.DataFrame) -> dict[str, Any]:
    """按 (ts_code, start_date, end_date, st_type) 四元组比对两份区间表。"""
    mine_keys = _key_set(mine)
    ref_keys = _key_set(ref)
    only_mine = sorted(mine_keys - ref_keys)
    only_ref = sorted(ref_keys - mine_keys)
    return {
        'mine_rows': mine.height,
        'ref_rows': ref.height,
        'only_in_new': len(only_mine),
        'only_in_existing': len(only_ref),
        'diff_rows': len(only_mine) + len(only_ref),
        'only_in_new_sample': only_mine[:10],
        'only_in_existing_sample': only_ref[:10],
    }


def backup_existing(target: Path, backup_dir: Path) -> Path | None:
    """写盘前把现有文件复制到 ``_backup``（带时间戳）；不存在返回 ``None``。"""
    if not target.is_file():
        return None
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    dest = backup_dir / f'{target.stem}_{stamp}.parquet'
    shutil.copy2(target, dest)
    return dest


def apply_output(frame: pl.DataFrame, target: Path,
                 backup_dir: Path) -> dict[str, Any]:
    """备份 + 原子写；空结果拒绝落盘（避免把覆盖表清成空文件）。"""
    if frame.height == 0:
        raise RuntimeError('推导结果为空，拒绝写盘')
    backup = backup_existing(target, backup_dir)
    write_parquet_atomic(frame, target)
    return {'written': str(target), 'rows': frame.height,
            'backup': str(backup) if backup else None,
            'sha256': _sha256(target)}


def check_output(frame: pl.DataFrame, target: Path) -> dict[str, Any]:
    """重建到临时目录并比对：内容差异 + 两次落盘是否字节一致。"""
    with tempfile.TemporaryDirectory() as tmp:
        first = Path(tmp) / 'a.parquet'
        second = Path(tmp) / 'b.parquet'
        write_parquet_atomic(frame, first)
        write_parquet_atomic(frame, second)
        report: dict[str, Any] = {
            'rebuilt_rows': frame.height,
            'rebuilt_sha256': _sha256(first),
            'byte_reproducible': _sha256(first) == _sha256(second),
            'target_exists': target.is_file(),
        }
        if target.is_file():
            report['existing_sha256'] = _sha256(target)
            report['diff'] = compare_frames(frame, pl.read_parquet(target))
    return report


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------
def _parse_args(argv: Iterable[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='从 tushare namechange 重建 ST 历史区间表')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--check', action='store_true',
                      help='重建到临时目录并与现有文件比对（默认行为）')
    mode.add_argument('--apply', action='store_true',
                      help='备份后原子覆盖目标文件')
    parser.add_argument('--offline', action='store_true', help='只用本地缓存，不联网')
    parser.add_argument('--refresh', action='store_true', help='忽略分片缓存重新拉取')
    parser.add_argument('--cache-dir', default=str(RAW_DIR))
    parser.add_argument('--target', default=str(TARGET))
    parser.add_argument('--backup-dir', default=str(BACKUP_DIR))
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    raw = fetch_raw(Path(args.cache_dir), offline=args.offline, refresh=args.refresh)
    frame = derive_intervals(raw)
    report: dict[str, Any] = {
        'raw_rows': raw.height,
        'interval_rows': frame.height,
        'code_count': frame['ts_code'].n_unique() if frame.height else 0,
        'open_intervals': frame['end_date'].is_null().sum() if frame.height else 0,
    }
    if frame.height == 0:
        report['error'] = '推导结果为空，未写盘'
    elif args.apply:
        report.update(apply_output(frame, Path(args.target), Path(args.backup_dir)))
    else:
        report.update(check_output(frame, Path(args.target)))
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0 if frame.height else 2


if __name__ == '__main__':
    sys.exit(main())
