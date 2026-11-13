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

补充源
======
tushare ``namechange`` 对个别代码迁移旧码一条记录都没有（G-134：``000043.SZ``、
``300114.SZ``），这类代码用深交所简称变更原始件 ``meta/st/raw/sz_change_name.parquet``
补段，``source='sz_change_name'``。**只对 tushare 名下 0 行的代码生效**，已有代码
一行不动；首段回溯到 ``list_date``，末段收口到 ``delist_date``；上市首日的行情端
标记（``N`` / ``C`` / ``XD`` / ``XR`` / ``DR``）不写成简称；事件链不自洽即跳过不猜。

代码变更沿用（G-146）
====================
tushare 与深交所简称变更表都只把 2006-10-09 那次改名登记在旧码 ``000043.SZ``
名下，新码 ``001914.SZ`` 因此缺 2006-10-09~2007-11-29 的「深南光A」段。本脚本按
:data:`CARRIED_CODE_MAP` 里**已登记的一对**代码把旧码事件段沿用给新码，
``source='carried_from:<旧码>'``（字段级来源，不整行打笼统标签）。

只沿用「落在新码既有段空洞内、且与既有段无重叠」的段，既有行一行不动；**不做
「旧码事件自动沿用」的通用逻辑**，新加一对必须先取证并登记（见
``docs/数据权威路径清单.md``「名称历史的精度分层」与 ``meta/code_migration.parquet``）。

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
import unicodedata
from datetime import date, datetime, timedelta
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
#: 深交所简称变更原始件（相对 ``data/local_store``）。**只补 tushare 名下 0 行的代码**
#: （G-134：``000043.SZ`` / ``300114.SZ`` 在 tushare ``namechange`` 里一条都没有）。
SZ_RAW = 'meta/st/raw/sz_change_name.parquet'
SOURCE_SZ = 'sz_change_name'
SZ_REASON = '深交所简称变更表'
#: **已登记的「同一主体代码变更」映射（新码 → 旧码），只允许这里列出的对**。
#: 取证与登记见 ``docs/数据权威路径清单.md`` 的「名称历史的精度分层」；映射本身
#: 以 ``meta/code_migration.parquet`` 为唯一正式来源（``000043.SZ → 001914.SZ``，
#: ``confidence=high``）。新增一对必须先取证并登记，禁止按代码前缀或尾号推定。
CARRIED_CODE_MAP: dict[str, str] = {'001914.SZ': '000043.SZ'}
#: 沿用段的 ``source`` 前缀；完整取值 ``carried_from:<旧码>``，逐字段可追溯
CARRIED_SOURCE_PREFIX = 'carried_from:'
CARRIED_REASON = '旧码事件按登记映射沿用'
#: 行情端当日临时标记：出现在 ``name_prev`` 时不是简称，不写入区间表
#: （与 ``src/data/name_consistency.py`` 的 ``_MARKET_PREFIXES`` 同义）
_MARKET_MARKERS = ('XD', 'XR', 'DR', 'N', 'C')
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


def _nfkc(value: Any) -> str:
    """全角转半角 + 去空白。

    深交所件写「深南光Ａ」「G 深南光」，tushare 写「深南光A」「G深南光」；
    不归一就等于给同一只票的同一天造出两个不同简称（G-134）。
    """
    text = unicodedata.normalize('NFKC', str(value if value is not None else ''))
    return ''.join(text.split())


def _split_market_marker(name: str) -> tuple[str, bool]:
    for prefix in _MARKET_MARKERS:
        if name.startswith(prefix) and len(name) > len(prefix):
            return name[len(prefix):], True
    return name, False


def sz_segments(events: list[dict[str, Any]], list_date: date,
                delist_date: date | None) -> tuple[list[tuple[str, date, date | None]] | None, str | None]:
    """深交所变更事件 → ``[(name, start, end)]``；链不自洽时返回 ``(None, 原因)``，不猜。

    口径：
    ① 首段名取首个事件的 ``name_prev``（上市时简称）并回溯到 ``list_date``；
    ② 若 ``name_prev`` 带行情端标记（``N`` / ``C`` / ``XD`` / ``XR`` / ``DR``），
       它不是简称，首段改用同事件的 ``name_new``，该事件不单独成段——否则会写出
       「N中航电」并与新码 ``302132.SZ`` 同名日期的「中航电测」打架；
    ③ 每个事件以 ``change_date`` 起新段，段尾 = 后继段起点 - 1 天；
    ④ 末段收口到 ``stock_basic.delist_date``（退市股不延伸到今天）。
    """
    rows = sorted(events, key=lambda item: item['change_date'])
    for prev, nxt in zip(rows, rows[1:], strict=False):
        if _nfkc(prev['name_new']) != _nfkc(nxt['name_prev']):
            return None, 'inconsistent_chain'
    head, marked = _split_market_marker(_nfkc(rows[0]['name_prev']))
    if marked:
        head = _nfkc(rows[0]['name_new'])
        rows = rows[1:]
    segments: list[list[Any]] = [[head, list_date]]
    for item in rows:
        day = item['change_date']
        if day <= segments[-1][1]:
            # 同日改名：不产生零长段，同名日以新名覆盖
            segments[-1][0] = _nfkc(item['name_new'])
            continue
        segments.append([_nfkc(item['name_new']), day])
    closed: list[tuple[str, date, date | None]] = []
    for current, following in zip(segments, segments[1:], strict=False):
        closed.append((current[0], current[1], following[1] - timedelta(days=1)))
    last_name, last_start = segments[-1]
    if delist_date is not None and delist_date < last_start:
        return None, 'delist_before_last_segment'
    closed.append((last_name, last_start, delist_date))
    return closed, None


def sz_supplement(store: Path, existing: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, Any]]:
    """深交所简称变更表补段：**只补 tushare 一条都没有的代码**。

    ``existing`` 是 tushare 归一化结果；已在其中的代码一律不动（避免覆盖公告直证），
    空 ``code`` / ``stock_basic`` 缺该码 / 事件链不自洽的一律跳过并计数，不猜。
    """
    stats: dict[str, Any] = {
        'source': SOURCE_SZ, 'raw_path': str(store / SZ_RAW),
        'codes': 0, 'segments': 0, 'skipped_empty_code': 0,
        'skipped_no_stock_basic': 0, 'skipped_no_list_date': 0,
        'skipped_inconsistent_chain': 0, 'skipped_delist_before_last_segment': 0,
        'added': [],
    }
    raw_path = store / SZ_RAW
    basic_path = store / 'meta' / 'stock_basic.parquet'
    if not raw_path.is_file() or not basic_path.is_file():
        stats['missing_input'] = True
        return pl.DataFrame(schema=SCHEMA), stats
    raw = pl.read_parquet(raw_path)
    basic = pl.read_parquet(basic_path, columns=['ts_code', 'list_date', 'delist_date'])
    window = {item['ts_code']: (item['list_date'], item['delist_date'])
              for item in basic.to_dicts()}
    known = set(existing['ts_code'].to_list())
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in raw.to_dicts():
        code = _clean_text(item.get('code'))
        if not code:
            stats['skipped_empty_code'] += 1
            continue
        if code in known:
            continue
        grouped.setdefault(code, []).append(item)
    now = datetime.now()
    rows: list[dict[str, Any]] = []
    for code in sorted(grouped):
        if code not in window:
            stats['skipped_no_stock_basic'] += 1
            continue
        list_date, delist_date = window[code]
        if list_date is None:
            stats['skipped_no_list_date'] += 1
            continue
        segments, reason = sz_segments(grouped[code], list_date, delist_date)
        if segments is None:
            stats[f'skipped_{reason}'] = stats.get(f'skipped_{reason}', 0) + 1
            continue
        stats['codes'] += 1
        for name, start, end in segments:
            rows.append({
                'ts_code': code, 'name': name, 'start_date': start, 'end_date': end,
                'ann_date': None, 'change_reason': SZ_REASON,
                'source': SOURCE_SZ, 'fetched_at': now,
            })
            stats['added'].append([code, name, str(start), str(end)])
    stats['segments'] = len(rows)
    return pl.DataFrame(rows, schema=SCHEMA, strict=False), stats


def carried_code_segments(
        frame: pl.DataFrame,
        mapping: dict[str, str] | None = None) -> tuple[pl.DataFrame, dict[str, Any]]:
    """按**已登记**的代码变更映射，把旧码事件段沿用给新码（G-146）。

    口径（DG-18 / DG-31）：
    ① 只处理 :data:`CARRIED_CODE_MAP` 里登记的对，不做「旧码事件自动沿用」；
    ② 只补空洞——旧码段必须整段落在新码两条既有段之间的空档内，且与新码既有段
       零重叠、零乱序；落在空档外（与新码已有段重叠）一律跳过并计数；
    ③ 未命中任何分支时**跳过并计数**，不写 null、不猜；
    ④ 沿用段 ``source='carried_from:<旧码>'``，逐行可追溯到具体旧码。
    """
    pairs = CARRIED_CODE_MAP if mapping is None else mapping
    stats: dict[str, Any] = {
        'pairs': len(pairs), 'segments': 0, 'added': [],
        'skipped_no_new_code': 0, 'skipped_no_old_code': 0,
        'skipped_open_gap': 0, 'skipped_outside_gap': 0, 'skipped_duplicate': 0,
    }
    now = datetime.now()
    rows: list[dict[str, Any]] = []
    existing = set(zip(frame['ts_code'].to_list(), frame['start_date'].to_list(),
                       frame['end_date'].to_list(), strict=False))
    for new_code, old_code in pairs.items():
        new_rows = frame.filter(pl.col('ts_code') == new_code).sort(
            ['start_date', 'end_date'], nulls_last=True)
        old_rows = frame.filter(pl.col('ts_code') == old_code).sort(
            ['start_date', 'end_date'], nulls_last=True)
        if new_rows.height == 0:
            stats['skipped_no_new_code'] += 1
            continue
        if old_rows.height == 0:
            stats['skipped_no_old_code'] += 1
            continue
        gaps: list[tuple[date, date]] = []
        items = new_rows.to_dicts()
        for prev, nxt in zip(items, items[1:], strict=False):
            if prev['end_date'] is None:
                # 开区间之后不存在可补空洞，后续旧码段一律不能沿用
                stats['skipped_open_gap'] += 1
                break
            gaps.append((prev['end_date'] + timedelta(days=1),
                         nxt['start_date'] - timedelta(days=1)))
        for item in old_rows.to_dicts():
            start, end = item['start_date'], item['end_date']
            if start is None or end is None:
                stats['skipped_outside_gap'] += 1
                continue
            if (new_code, start, end) in existing:
                stats['skipped_duplicate'] += 1
                continue
            if not any(gap_start <= start and end <= gap_end
                       for gap_start, gap_end in gaps):
                stats['skipped_outside_gap'] += 1
                continue
            existing.add((new_code, start, end))
            rows.append({
                'ts_code': new_code, 'name': item['name'],
                'start_date': start, 'end_date': end,
                'ann_date': item['ann_date'],
                'change_reason': f'{CARRIED_REASON}（{old_code}）',
                'source': f'{CARRIED_SOURCE_PREFIX}{old_code}',
                'fetched_at': now,
            })
            stats['added'].append([new_code, item['name'], str(start), str(end),
                                   f'{CARRIED_SOURCE_PREFIX}{old_code}'])
    stats['segments'] = len(rows)
    return pl.DataFrame(rows, schema=SCHEMA, strict=False), stats


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
    store = target.parents[1]  # data/local_store
    sz_frame, sz_stats = sz_supplement(store, frame)
    if sz_frame.height:
        frame = pl.concat([frame, sz_frame], how='vertical_relaxed').sort(
            ['ts_code', 'start_date', 'end_date'], nulls_last=True)
    carry_frame, carry_stats = carried_code_segments(frame)
    if carry_frame.height:
        frame = pl.concat([frame, carry_frame], how='vertical_relaxed').sort(
            ['ts_code', 'start_date', 'end_date'], nulls_last=True)
    report: dict[str, Any] = {
        'raw_rows': raw.height,
        'dropped': dropped,
        'sz_supplement': sz_stats,
        'code_carry': carry_stats,
        'built': _stats(frame),
    }
    if frame.height == 0:
        report['error'] = '规整结果为空，拒绝写盘'
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
        return 2

    report['diff_vs_existing'] = compare_with_existing(frame, target)
    report['reconcile'] = {
        name: table.group_by('category').len().sort('category').to_dicts()
        for name, table in build_tables(store).items()
    }

    if args.apply:
        backup = backup_existing(target, Path(args.backup_dir))
        write_parquet_atomic(frame, target)
        coverage = {
            'dataset': 'name_history',
            'source': f'{SOURCE}+{SOURCE_SZ}+{CARRIED_SOURCE_PREFIX}<旧码>',
            'source_note': (
                f'{SOURCE} 为主源；{SOURCE_SZ} 只补 tushare 名下 0 行的代码，'
                '首段回溯到 stock_basic.list_date，末段收口到 delist_date；'
                f'{CARRIED_SOURCE_PREFIX}<旧码> 只按已登记的一对代码沿用空洞段'),
            'authority_for': '历史简称（PIT）；当前简称以 meta/names.json 为准',
            'generated_at': datetime.now().isoformat(timespec='seconds'),
            'cache_dir': str(args.cache_dir),
            'raw_rows': raw.height,
            'dedup_dropped': dropped,
            'sz_supplement': {k: v for k, v in sz_stats.items() if k != 'added'},
            'sz_supplement_rows': sz_stats['added'],
            'code_carry': {k: v for k, v in carry_stats.items() if k != 'added'},
            'code_carry_rows': carry_stats['added'],
            'code_carry_map': dict(CARRIED_CODE_MAP),
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
            'source': f'{SOURCE}+{SOURCE_SZ}+{CARRIED_SOURCE_PREFIX}<旧码>',
            'source_note': (
                f'{SOURCE} 为主源；{SOURCE_SZ} 只补 tushare 名下 0 行的代码；'
                f'{CARRIED_SOURCE_PREFIX}<旧码> 为按已登记代码变更映射沿用的旧码事件段'),
            'authority': '历史简称以本表为准；当前简称以 meta/names.json 为准',
            'path': 'meta/name_history.parquet',
            'produced_by': 'tools/build_name_history.py',
            'generated_at': datetime.now().isoformat(timespec='seconds'),
            'rows': frame.height,
            'bytes': target.stat().st_size,
            'sha256': _sha256(target),
            'columns': list(frame.columns),
            'code_carry_map': dict(CARRIED_CODE_MAP),
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
