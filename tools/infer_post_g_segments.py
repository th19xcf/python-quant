#!/usr/bin/env python3

"""为 G 股改残留标的补一条 post-G 名称段（推断，非公告直证）。

背景
====
tushare ``namechange`` 里 88 只标的的末段名称仍停在 ``G万科A`` 这类股改标记
名上，收尾于 ``2006-10-08``，但没有后续改名记录。结果是这些标的在
``2006-10-09`` 之后按 ``status_at`` 的两端闭区间语义查不到任何名称。

生成规则（固定，不再推断其他口径）
==================================
1. 名称 = G 段名去掉前缀 ``G``（``G万科A`` → ``万科A``）。**不使用**
   ``names.json`` 当前名，避免把今天的名字倒灌进历史；
2. 区间 = ``2006-10-09`` 起；若该标的已有更晚的段，则到「后继段
   ``start_date`` - 1 天」，否则保持开区间；
3. ``source`` 写 ``inferred:strip_G``，``change_reason`` 写「推断：去掉 G 前缀
   （非公告直证）」——公告直证的段 ``source`` 一律是 ``tushare_namechange``，
   两者可一眼区分；
4. 幂等：某标的已存在起点更晚的段时直接跳过，重复执行第二次新增 0 段。

已知局限
========
被推断的标的若已退市或 2006 年后又改过名，这条开区间会一直延伸到今天，
返回的是 2006 年去 G 后的名字。运行报告里会统计生成名与 ``names.json``
当前名不一致的只数，供人工复核，但不参与取值。

产物
====
::

    reports/name_history_build_<YYYYMMDD>/post_g_report.json

只落一份机器可读的推断记录（规则 / source / 条数 / 备份 / sha256），
不另出供人肉核对的 CSV。

用法::

    .venv\\Scripts\\python.exe tools/infer_post_g_segments.py           # 只报告
    .venv\\Scripts\\python.exe tools/infer_post_g_segments.py --apply   # 备份后原子覆盖
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data.atomic_write import write_parquet_atomic, write_text_atomic  # noqa: E402
from src.data.name_consistency import name_at_frame  # noqa: E402

TARGET = ROOT / 'data' / 'local_store' / 'meta' / 'name_history.parquet'
COVERAGE = ROOT / 'data' / 'local_store' / 'meta' / 'name_history_coverage.json'
MANIFEST = ROOT / 'data' / 'local_store' / 'meta' / 'name_history.manifest.json'
BACKUP_ROOT = ROOT / '_backup' / 'meta'
REPORT_ROOT = ROOT / 'reports'

POST_G_START = date(2006, 10, 9)
SOURCE = 'inferred:strip_G'
REASON = '推断：去掉 G 前缀（非公告直证）'


def _ordered(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.sort(['ts_code', 'start_date', 'end_date'], nulls_last=True)


def _pit_name(history: pl.DataFrame, code: str, day: date) -> str | None:
    """取 ``code`` 在 ``day`` 当天的历史简称；委托共享读取 API。

    按日期取简称统一走 :func:`src.data.name_consistency.name_at_frame`
    （返回值含 ``source`` / ``precision``），本函数只取其中的 ``name``
    供推断前后对账，不是业务 API。
    """
    return name_at_frame(history, code, day)['name']


def _load_names_json(store_root: Path) -> dict[str, str]:
    path = store_root / 'meta' / 'names.json'
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return {}
    return {str(key): str(value) for key, value in payload.items()} \
        if isinstance(payload, dict) else {}


def plan(history: pl.DataFrame) -> tuple[list[dict[str, Any]],
                                         list[dict[str, Any]]]:
    """末段仍是 G 名且无后继段 → 生成 post-G 段；其余列入跳过清单。"""
    last = _ordered(history).group_by('ts_code', maintain_order=True).last()
    candidates = last.filter(
        pl.col('name').str.starts_with('G') & pl.col('end_date').is_not_null())
    created: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for item in candidates.sort('ts_code').to_dicts():
        g_end = item['end_date']
        stripped = item['name'][1:]
        row = {
            'ts_code': item['ts_code'], 'g_name': item['name'],
            'inferred_name': stripped, 'start_date': POST_G_START,
            'end_date': None,
        }
        if not stripped:
            row['skip_reason'] = '去 G 后为空'
            skipped.append(row)
            continue
        if POST_G_START <= g_end:
            row['skip_reason'] = f'G 段收尾 {g_end} 不早于 {POST_G_START}'
            skipped.append(row)
            continue
        created.append(row)
    return created, skipped


def apply(history: pl.DataFrame,
          created: list[dict[str, Any]]) -> pl.DataFrame:
    """追加推断段并按 (ts_code, start_date, end_date) 规范排序。"""
    if not created:
        return history
    stamp = datetime.now()
    rows = [{
        'ts_code': item['ts_code'], 'name': item['inferred_name'],
        'start_date': item['start_date'], 'end_date': item['end_date'],
        'ann_date': None, 'change_reason': REASON, 'source': SOURCE,
        'fetched_at': stamp,
    } for item in created]
    extra = pl.DataFrame(rows, schema=history.schema, strict=False)
    return _ordered(pl.concat([history, extra], how='vertical_relaxed'))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def backup_with_relpath(target: Path, store_root: Path,
                        backup_root: Path) -> Path:
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    dest = backup_root / stamp / target.relative_to(store_root)
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(target, dest)
    return dest


def update_coverage(frame: pl.DataFrame, target: Path,
                    inferred: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if COVERAGE.is_file():
        payload = json.loads(COVERAGE.read_text(encoding='utf-8'))
    open_frame = frame.filter(pl.col('end_date').is_null())
    payload.update({
        'rows': frame.height,
        'codes': frame['ts_code'].n_unique(),
        'open_rows': open_frame.height,
        'open_codes': open_frame['ts_code'].n_unique(),
        'ann_date_null': frame.filter(pl.col('ann_date').is_null()).height,
        'target_sha256': _sha256(target),
        'inferred_segments': inferred,
    })
    COVERAGE.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding='utf-8')
    return payload


def update_manifest(frame: pl.DataFrame, target: Path, step: str) -> dict[str, Any]:
    """把 manifest 的 rows / bytes / sha256 对齐到当前文件（DG-4）。

    ``build_name_history.py`` 只登记自己那一步的产物，本步骤改写同一个 parquet，
    不回写就会留下与磁盘不符的 manifest。
    """
    payload: dict[str, Any] = {}
    if MANIFEST.is_file():
        payload = json.loads(MANIFEST.read_text(encoding='utf-8'))
    payload.update({
        'rows': frame.height,
        'bytes': target.stat().st_size,
        'sha256': _sha256(target),
        'generated_at': datetime.now().isoformat(timespec='seconds'),
    })
    steps = [item for item in payload.get('post_steps', []) if item != step]
    payload['post_steps'] = [*steps, step]
    write_text_atomic(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str) + '\n',
        MANIFEST)
    return payload


def _parse_args(argv: Iterable[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='为 G 股改残留标的新增 post-G 名称段（推断）')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--check', action='store_true', help='只报告不写盘（默认）')
    mode.add_argument('--apply', action='store_true', help='备份后原子覆盖')
    parser.add_argument('--target', default=str(TARGET))
    parser.add_argument('--store-root', default=None)
    parser.add_argument('--report-dir', default=None)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    target = Path(args.target)
    if not target.is_file():
        print(f'目标不存在: {target}')
        return 2
    history = pl.read_parquet(target)
    store_root = Path(args.store_root) if args.store_root else target.parents[1]
    report_dir = Path(args.report_dir) if args.report_dir \
        else REPORT_ROOT / f'name_history_build_{date.today():%Y%m%d}'

    created, skipped = plan(history)
    patched = apply(history, created)
    current_names = _load_names_json(store_root)

    rows: list[dict[str, Any]] = []
    before_ok = 0
    after_ok = 0
    for item in created:
        code = item['ts_code']
        before = _pit_name(history, code, POST_G_START)
        after = _pit_name(patched, code, POST_G_START)
        before_ok += 1 if before is not None else 0
        after_ok += 1 if after is not None else 0
        now = current_names.get(code)
        rows.append({
            'ts_code': code, 'g_name': item['g_name'],
            'inferred_name': item['inferred_name'],
            'names_json_current': now,
            'name_differs': now is not None and now != item['inferred_name'],
        })

    differs = sum(1 for item in rows if item['name_differs'])
    report: dict[str, Any] = {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'target': str(target),
        'rule': ('名称 = G 段名去前缀 G；区间 = 2006-10-09 起，有更晚段则到'
                 '其后继段 start - 1 天，否则开区间'),
        'source': SOURCE, 'change_reason': REASON,
        'rows_before': history.height,
        'rows_after': patched.height,
        'created_segments': len(created),
        'skipped_rows': len(skipped),
        'skipped': skipped,
        'check_day': str(POST_G_START),
        'pit_name_before_non_null': before_ok,
        'pit_name_after_non_null': after_ok,
        'pit_name_still_null': [
            item['ts_code'] for item in created
            if _pit_name(patched, item['ts_code'], POST_G_START) is None],
        'names_json_differs': differs,
        'names_json_differs_sample': [
            [item['ts_code'], item['inferred_name'], item['names_json_current']]
            for item in rows if item['name_differs']],
    }

    if args.apply and created:
        backup = backup_with_relpath(target, store_root, BACKUP_ROOT)
        write_parquet_atomic(patched, target)
        disk = pl.read_parquet(target)
        report['written'] = {
            'rows': disk.height, 'sha256': _sha256(target),
            'backup': str(backup), 'coverage': str(COVERAGE),
        }
        coverage = update_coverage(disk, target, {
            'inferred_at': datetime.now().isoformat(timespec='seconds'),
            'script': 'tools/infer_post_g_segments.py',
            'rule': report['rule'],
            'source': SOURCE,
            'precision': 'lowest（推断，非公告直证）',
            'created_segments': len(created),
            'names_json_differs': differs,
            'backup': str(backup),
            'report': str(report_dir / 'post_g_report.json'),
        })
        report['coverage_sha256'] = coverage['target_sha256']
        report['manifest_sha256'] = update_manifest(
            disk, target, 'tools/infer_post_g_segments.py')['sha256']
    elif COVERAGE.is_file():
        applied = json.loads(COVERAGE.read_text(encoding='utf-8')) \
            .get('inferred_segments')
        if applied:
            report['applied_record'] = applied

    report_path = report_dir / 'post_g_report.json'
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str),
        encoding='utf-8')
    print(json.dumps({
        'rows_before': report['rows_before'],
        'rows_after': report['rows_after'],
        'created_segments': len(created),
        'skipped_rows': len(skipped),
        'pit_name_before_non_null': before_ok,
        'pit_name_after_non_null': after_ok,
        'pit_name_still_null': report['pit_name_still_null'],
        'names_json_differs': differs,
        'written': report.get('written'),
        'report': str(report_path),
    }, ensure_ascii=False, indent=2, default=str))
    return 0 if after_ok == len(created) else 3


if __name__ == '__main__':
    sys.exit(main())
