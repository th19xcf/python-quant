#!/usr/bin/env python3

"""给 ``meta/name_history.parquet`` 里缺收尾的历史段补 ``end_date``。

规则（唯一依据：同标的后继段）
==============================
同一 ``ts_code`` 下按 ``(start_date, end_date)`` 升序排列后，若某段存在起点
更晚的后继段，则::

    end_date = 后继段.start_date - 1 天

只有确实没有后继段时才保持开区间。**不允许**用 ``stock_basic.name`` 或
``names.json`` 反推收尾日期——它们是当日快照，不是历史段边界。

会被修的两种形态
----------------
1. 开区间但存在后继段（``end_date`` 为空）；
2. 收尾过晚（``end_date >= 后继段.start_date``，同标的两段区间重叠）。

不会被修（列进 unresolved，只报告）
----------------------------------
- 无后继段的开区间：按规则必须保持开区间，硬改就是臆造；
- ``end_date < 后继段.start_date - 1`` 的「空洞」：数据源缺记录，补洞同样
  是臆造；
- ``后继段.start_date <= 本段.start_date`` 的顺序倒挂：无可用边界。

幂等性
======
已经正确的段（``end_date == 后继段.start_date - 1``）不会被改动，重复执行
第二次改动 0 段。

与重建脚本的关系
================
``tools/build_name_history.py --apply`` 从 tushare 重新生成整表，新表里这些
段的 ``end_date`` 仍然为空/过晚。**重建后必须再跑一次本脚本 ``--apply``**，
否则收尾修复会被覆盖掉。

产物
====
::

    reports/name_history_build_<YYYYMMDD>/name_history_end_date_fixes.csv
    reports/name_history_build_<YYYYMMDD>/name_history_end_date_unresolved.csv
    reports/name_history_build_<YYYYMMDD>/name_at_g_check.csv
    reports/name_history_build_<YYYYMMDD>/repair_report.json

用法::

    .venv\\Scripts\\python.exe tools/fix_name_history_end_dates.py           # 只报告
    .venv\\Scripts\\python.exe tools/fix_name_history_end_dates.py --apply   # 备份后原子覆盖
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data.atomic_write import write_parquet_atomic  # noqa: E402
from src.data.name_consistency import name_at_frame  # noqa: E402

TARGET = ROOT / 'data' / 'local_store' / 'meta' / 'name_history.parquet'
COVERAGE = ROOT / 'data' / 'local_store' / 'meta' / 'name_history_coverage.json'
BACKUP_ROOT = ROOT / 'data' / 'local_store' / 'meta' / '_backup'
REPORT_ROOT = ROOT / 'reports'

SAMPLE_SIZE = 20
TODAY = date(2026, 11, 10)


# ----------------------------------------------------------------------
# 区间审计 / 修复计划
# ----------------------------------------------------------------------
def _ordered(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.sort(['ts_code', 'start_date', 'end_date'], nulls_last=True)


def audit(frame: pl.DataFrame) -> dict[str, Any]:
    """逐标的相邻段审计：重叠 / 倒挂 / 空洞 / 同日边界。"""
    records = _ordered(frame).select(
        'ts_code', 'name', 'start_date', 'end_date').to_dicts()
    overlap: list[tuple[str, str, str]] = []
    inversion: list[tuple[str, str, str]] = []
    gaps: list[tuple[str, str, str]] = []
    same_day: list[tuple[str, str, str]] = []
    for cur, nxt in zip(records, records[1:], strict=False):
        if cur['ts_code'] != nxt['ts_code']:
            continue
        code = cur['ts_code']
        if nxt['start_date'] <= cur['start_date']:
            inversion.append((code, str(cur['start_date']), str(nxt['start_date'])))
            continue
        if cur['end_date'] is None:
            continue
        if nxt['start_date'] <= cur['end_date']:
            overlap.append((code, str(cur['end_date']), str(nxt['start_date'])))
            if nxt['start_date'] == cur['end_date']:
                same_day.append((code, str(cur['end_date']), str(nxt['start_date'])))
        elif cur['end_date'] < nxt['start_date'] - timedelta(days=1):
            gaps.append((code, str(cur['end_date']), str(nxt['start_date'])))
    open_frame = frame.filter(pl.col('end_date').is_null())
    return {
        'rows': frame.height,
        'codes': frame['ts_code'].n_unique(),
        'open_rows': open_frame.height,
        'open_codes': open_frame['ts_code'].n_unique(),
        'overlap_pairs': len(overlap),
        'inversion_pairs': len(inversion),
        'gap_pairs': len(gaps),
        'same_day_pairs': len(same_day),
        'overlap_sample': [list(row) for row in overlap[:10]],
        'inversion_sample': [list(row) for row in inversion[:10]],
        'same_day_sample': [list(row) for row in same_day[:10]],
    }


def plan_fixes(frame: pl.DataFrame) -> tuple[list[dict[str, Any]],
                                             list[dict[str, Any]]]:
    """按「后继段 - 1 天」生成改动清单与顺序倒挂清单。"""
    records = _ordered(frame).select(
        'ts_code', 'name', 'start_date', 'end_date').to_dicts()
    fixes: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    for cur, nxt in zip(records, records[1:], strict=False):
        if cur['ts_code'] != nxt['ts_code']:
            continue
        needs = cur['end_date'] is None or cur['end_date'] >= nxt['start_date']
        if not needs:
            continue
        new_end = nxt['start_date'] - timedelta(days=1)
        row = {
            'ts_code': cur['ts_code'], 'name': cur['name'],
            'start_date': cur['start_date'], 'old_end_date': cur['end_date'],
            'new_end_date': new_end, 'next_start': nxt['start_date'],
            'next_name': nxt['name'],
            'reason': '开区间补收尾' if cur['end_date'] is None else '收尾过晚重叠',
        }
        if nxt['start_date'] <= cur['start_date'] or new_end < cur['start_date']:
            row['reason'] = '顺序倒挂，无法确定边界'
            blocked.append(row)
            continue
        fixes.append(row)
    return fixes, blocked


def list_unresolved(frame: pl.DataFrame, store_root: Path,
                    blocked: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """本规则修不动的两类：G 股改残留、其他类仍开区间的标的。

    分类口径直接复用 C 的对账模块（``name_consistency``），只读不改；
    该模块不可用时退化为「末段 G 名」单一口径，不阻断主流程。
    """
    last = _ordered(frame).group_by('ts_code', maintain_order=True).last()
    last_map = {item['ts_code']: item for item in last.to_dicts()}
    try:
        from src.data.name_consistency import name_history_vs_stock_basic

        table = name_history_vs_stock_basic(store_root)
        category = {item['ts_code']: item['category']
                    for item in table.to_dicts()}
    except Exception:  # noqa: BLE001  对账失败不阻断修复主流程
        category = {}
    rows = list(blocked)
    for code, item in last_map.items():
        cat = category.get(code)
        if item['end_date'] is None and cat in ('G股改残留', '其他'):
            rows.append({
                'ts_code': code, 'name': item['name'],
                'start_date': item['start_date'], 'old_end_date': None,
                'new_end_date': None, 'next_start': None, 'next_name': None,
                'reason': f'{cat}｜无后继段，保持开区间',
            })
        elif cat == 'G股改残留' and item['end_date'] is not None:
            rows.append({
                'ts_code': code, 'name': item['name'],
                'start_date': item['start_date'],
                'old_end_date': item['end_date'],
                'new_end_date': None, 'next_start': None, 'next_name': None,
                'reason': (f'G股改残留｜末段已收尾于 {item["end_date"]}，'
                           '缺 post-G 改名记录，无后继段可依'),
            })
    return rows


def apply_fixes(frame: pl.DataFrame,
                fixes: list[dict[str, Any]]) -> pl.DataFrame:
    """按行号精确替换 ``end_date``，保持原始行序与列序。"""
    if not fixes:
        return frame
    wanted = {(item['ts_code'], item['name'], item['start_date'],
               item['old_end_date']): item['new_end_date'] for item in fixes}
    work = frame.with_row_index('_rid')
    ordered = _ordered(work).to_dicts()
    mapping: dict[int, date] = {}
    for item in ordered:
        key = (item['ts_code'], item['name'], item['start_date'],
               item['end_date'])
        if key in wanted:
            mapping[item['_rid']] = wanted[key]
    if len(mapping) != len(fixes):
        raise RuntimeError(
            f'修复定位失败：计划 {len(fixes)} 段，命中 {len(mapping)} 段')
    patch = pl.DataFrame(
        {'_rid': list(mapping), 'end_date_new': list(mapping.values())},
        schema={'_rid': pl.UInt32, 'end_date_new': pl.Date})
    return (
        work.join(patch, on='_rid', how='left')
        .with_columns(pl.coalesce('end_date_new', 'end_date').alias('end_date'))
        .drop('end_date_new')
        .sort('_rid')
        .drop('_rid')
    )


# ----------------------------------------------------------------------
# PIT 取简称
# ----------------------------------------------------------------------
# 不再自带 ``name_at`` 副本：按日期取简称统一走
# :func:`src.data.name_consistency.name_at_frame`，返回值含 ``source`` /
# ``precision``，避免推断名被当成公告直证用。


def g_codes(history: pl.DataFrame) -> list[str]:
    """末段名称仍以 ``G`` 开头的标的（即 C 报告里的 G 股改残留）。"""
    last = _ordered(history).group_by('ts_code', maintain_order=True).last()
    marked = last.filter(pl.col('name').str.starts_with('G'))
    return sorted(marked['ts_code'].to_list())


def g_with_successor(history: pl.DataFrame) -> list[str]:
    """G 名段后已有非 G 后继段的标的（摘 G 后还能查到名字）。"""
    ordered = _ordered(history).with_columns(
        pl.col('name').shift(-1).over('ts_code').alias('_next_name'),
        pl.col('start_date').shift(-1).over('ts_code').alias('_next_start'))
    marked = ordered.filter(
        pl.col('name').str.starts_with('G')
        & pl.col('_next_start').is_not_null()
        & ~pl.col('_next_name').str.starts_with('G'))
    return sorted(set(marked['ts_code'].to_list()))


def check_g_samples(history: pl.DataFrame, samples: list[tuple[str, str]],
                    limit: int = SAMPLE_SIZE
                    ) -> tuple[list[dict[str, Any]], int]:
    """抽 G 股改标的，在「G 名期间 / 摘 G 后一年 / 今天」三时点断言。"""
    rows: list[dict[str, Any]] = []
    failures = 0
    for code, group in samples[:limit]:
        seg = history.filter(
            (pl.col('ts_code') == code)
            & pl.col('name').str.starts_with('G')
            & pl.col('end_date').is_not_null()
        ).sort('start_date')
        if seg.height == 0:
            rows.append({'ts_code': code, 'group': group, 'pass': False,
                         'reason': '无已收尾的 G 名段'})
            failures += 1
            continue
        item = seg.to_dicts()[-1]
        start, end = item['start_date'], item['end_date']
        during = start + (end - start) // 2
        after = end + timedelta(days=365)
        during_hit = name_at_frame(history, code, during)
        after_hit = name_at_frame(history, code, after)
        today_hit = name_at_frame(history, code, TODAY)
        name_during = during_hit['name']
        name_after = after_hit['name']
        name_today = today_hit['name']
        ok = (
            name_during is not None and name_during.startswith('G')
            and not (name_after or '').startswith('G')
            and not (name_today or '').startswith('G')
        )
        failures += 0 if ok else 1
        rows.append({
            'ts_code': code, 'group': group, 'g_name': item['name'],
            'g_start': start, 'g_end': end,
            'during_day': during, 'name_during': name_during,
            'source_during': during_hit['source'],
            'after_day': after, 'name_after': name_after,
            'source_after': after_hit['source'],
            'today': TODAY, 'name_today': name_today,
            'source_today': today_hit['source'],
            'pass': ok,
        })
    return rows, failures


# ----------------------------------------------------------------------
# 落盘
# ----------------------------------------------------------------------
def _sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def backup_with_relpath(target: Path, store_root: Path,
                        backup_root: Path) -> Path:
    """``copy2`` 到 ``<backup_root>/<时间戳>/<相对 store_root 的路径>``。"""
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    dest = backup_root / stamp / target.relative_to(store_root)
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(target, dest)
    return dest


def _write_csv(rows: list[dict[str, Any]], path: Path,
               columns: Iterable[str]) -> Path:
    """按 ``columns`` 顺序逐列落 CSV；空值写空串，其余转字符串。"""
    cols = list(columns)
    frame = pl.DataFrame({
        col: ['' if row.get(col) is None else str(row.get(col))
              for row in rows]
        for col in cols
    }, schema=dict.fromkeys(cols, pl.Utf8))
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_csv(path)
    return path


def update_coverage(frame: pl.DataFrame, target: Path,
                    repair: dict[str, Any]) -> dict[str, Any]:
    """把 coverage.json 的行数 / 开区间 / sha256 重新对齐到当前文件。"""
    payload: dict[str, Any] = {}
    if COVERAGE.is_file():
        payload = json.loads(COVERAGE.read_text(encoding='utf-8'))
    open_frame = frame.filter(pl.col('end_date').is_null())
    payload.update({
        'rows': frame.height,
        'codes': frame['ts_code'].n_unique(),
        'open_rows': open_frame.height,
        'open_codes': open_frame['ts_code'].n_unique(),
        'target_sha256': _sha256(target),
        'end_date_repair': repair,
    })
    COVERAGE.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding='utf-8')
    return payload


def _row_dump(fixes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{key: ('' if value is None else str(value))
             for key, value in item.items()} for item in fixes]


def _parse_args(argv: Iterable[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='按后继段补 name_history 缺失/过晚的 end_date')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--check', action='store_true', help='只报告不写盘（默认）')
    mode.add_argument('--apply', action='store_true', help='备份后原子覆盖')
    parser.add_argument('--target', default=str(TARGET))
    parser.add_argument('--store-root', default=None,
                        help='本地列存根目录（默认从 target 推导）')
    parser.add_argument('--report-dir', default=None)
    parser.add_argument('--sample-size', type=int, default=SAMPLE_SIZE)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    target = Path(args.target)
    if not target.is_file():
        print(f'目标不存在: {target}')
        return 2
    frame = pl.read_parquet(target)
    store_root = Path(args.store_root) if args.store_root else target.parents[1]
    report_dir = Path(args.report_dir) if args.report_dir \
        else REPORT_ROOT / f'name_history_build_{TODAY:%Y%m%d}'

    before = audit(frame)
    fixes, blocked = plan_fixes(frame)
    unresolved = list_unresolved(frame, store_root, blocked)
    patched = apply_fixes(frame, fixes)
    after = audit(patched)

    residual_g = g_codes(patched)
    resolved_g = g_with_successor(patched)
    half = max(1, args.sample_size // 2)
    samples = ([(code, 'G股改残留-末段G名') for code in residual_g[:half]]
               + [(code, 'G名后有后继段')
                  for code in resolved_g[:args.sample_size - half]])
    checks, failures = check_g_samples(patched, samples, args.sample_size)

    now = datetime.now()
    csv_fixes = _write_csv(
        _row_dump(fixes), report_dir / 'name_history_end_date_fixes.csv',
        ('ts_code', 'name', 'start_date', 'old_end_date', 'new_end_date',
         'next_start', 'next_name', 'reason'))
    csv_unresolved = _write_csv(
        _row_dump(unresolved),
        report_dir / 'name_history_end_date_unresolved.csv',
        ('ts_code', 'name', 'start_date', 'old_end_date', 'new_end_date',
         'next_start', 'next_name', 'reason'))
    csv_check = _write_csv(
        _row_dump(checks), report_dir / 'name_at_g_check.csv',
        ('ts_code', 'group', 'g_name', 'g_start', 'g_end', 'during_day',
         'name_during', 'source_during', 'after_day', 'name_after',
         'source_after', 'today', 'name_today', 'source_today', 'pass'))

    report: dict[str, Any] = {
        'generated_at': now.isoformat(timespec='seconds'),
        'target': str(target),
        'before': before,
        'after': after,
        'changed_segments': len(fixes),
        'changed_by_reason': {
            key: sum(1 for item in fixes if item['reason'] == key)
            for key in ('开区间补收尾', '收尾过晚重叠')
        },
        'open_rows_before': before['open_rows'],
        'open_rows_after': after['open_rows'],
        'open_codes_before': before['open_codes'],
        'open_codes_after': after['open_codes'],
        'blocked_rows': len(blocked),
        'open_no_successor_rows': patched.filter(
            pl.col('end_date').is_null()).height,
        'unresolved_rows': len(unresolved),
        'unresolved_by_reason': {
            key: sum(1 for item in unresolved if item['reason'] == key)
            for key in sorted({item['reason'] for item in unresolved})
        },
        'g_residual_codes': len(residual_g),
        'g_with_successor_codes': len(resolved_g),
        'g_check_sample': len(checks),
        'g_check_failures': failures,
        'g_check_null_after': sum(1 for item in checks
                                  if item.get('name_after') is None),
        'g_check_null_today': sum(1 for item in checks
                                  if item.get('name_today') is None),
        'csv': {
            'fixes': str(csv_fixes), 'unresolved': str(csv_unresolved),
            'g_check': str(csv_check),
        },
    }

    if args.apply:
        backup = backup_with_relpath(target, store_root, BACKUP_ROOT)
        write_parquet_atomic(patched, target)
        disk = pl.read_parquet(target)
        report['written'] = {
            'rows': disk.height,
            'sha256': _sha256(target),
            'backup': str(backup),
            'coverage': str(COVERAGE),
        }
        coverage = update_coverage(disk, target, {
            'repaired_at': now.isoformat(timespec='seconds'),
            'script': 'tools/fix_name_history_end_dates.py',
            'rule': 'end_date = 后继段.start_date - 1 天（无后继段保持开区间）',
            'changed_segments': len(fixes),
            'open_rows_before': before['open_rows'],
            'open_rows_after': after['open_rows'],
            'open_codes_before': before['open_codes'],
            'open_codes_after': after['open_codes'],
            'backup': str(backup),
            'report': str(report_dir / 'repair_report.json'),
        })
        report['coverage_sha256'] = coverage['target_sha256']
    elif COVERAGE.is_file():
        applied = json.loads(COVERAGE.read_text(encoding='utf-8')) \
            .get('end_date_repair')
        if applied:
            report['applied_record'] = applied

    report_path = report_dir / 'repair_report.json'
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str),
        encoding='utf-8')
    print(json.dumps({
        'changed_segments': report['changed_segments'],
        'changed_by_reason': report['changed_by_reason'],
        'open_rows_before': before['open_rows'],
        'open_rows_after': after['open_rows'],
        'open_codes_before': before['open_codes'],
        'open_codes_after': after['open_codes'],
        'overlap_before': before['overlap_pairs'],
        'overlap_after': after['overlap_pairs'],
        'inversion_before': before['inversion_pairs'],
        'inversion_after': after['inversion_pairs'],
        'same_day_before': before['same_day_pairs'],
        'same_day_after': after['same_day_pairs'],
        'blocked_rows': len(blocked),
        'open_no_successor_rows': report['open_no_successor_rows'],
        'unresolved_rows': len(unresolved),
        'unresolved_by_reason': report['unresolved_by_reason'],
        'g_residual_codes': len(residual_g),
        'g_with_successor_codes': len(resolved_g),
        'g_check_failures': failures,
        'g_check_null_after': report['g_check_null_after'],
        'g_check_null_today': report['g_check_null_today'],
        'written': report.get('written'),
        'report': str(report_path),
    }, ensure_ascii=False, indent=2, default=str))
    return 0 if failures == 0 else 3


if __name__ == '__main__':
    sys.exit(main())
