#!/usr/bin/env python3
"""修复 2026-11-28 周批把 income.net_profit 覆盖回归母口径的破损（只修这一列）。

背景
----
2026-11-28 09:10 周批 ``tools/collect_financials.py`` 合并 2026-11-14 的旧分片
（akshare 原始「净利润」＝归母口径），经 ``store.merge_parts`` 的「逐列最后非空」
把 2026-11-11 已完成的圆锥合并口径修正（``net_profit``＝新浪 ``NETPROFIT``）覆盖回去，
2016–2026 每年约 71~77% 的 ``net_profit`` 退回母值，``net==parent`` 行占比由约 26%
升到约 98%；2010–2015 未受影响（采集起点 2016）。

修复口径
--------
不做整表回滚（会丢今天新增的 81 行），只在现表上重做 2026-11-11 那次列级修正：
- 以 11-11 定版快照 ``backups/critical_assets/.../income.parquet`` 为权威，
  对同时在现表与快照里的 ``(ts_code, end_date, ann_date)`` 行，
  ``net_profit`` 与 ``source``（含 ``|net_profit=...`` 标记）逐值回填为快照值；
- 今天新增、快照里没有的 81 行原样保留，``source`` 追加 ``|net_profit=keep_local``
  声明其 ``net_profit`` 未经新浪合并口径修正；
- 其余列（revenue 等周批合法更新、fetched_at）保持现状，不回滚。

证据
----
``_staging/income_netprofit_fix_20261128/`` 下留：
- ``before_after.json``：行数、``net==parent`` 逐年占比、``net_profit`` 改动格数、
  与快照逐值对拍差异数、SHA256；
- 修复前的 DG-22 快照目录路径与 SHA256。

用法::

    .venv\\Scripts\\python.exe tools\\_oneshot\\fix_income_netprofit_regression_20261128.py --apply
    .venv\\Scripts\\python.exe tools\\_oneshot\\fix_income_netprofit_regression_20261128.py --dry-run
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data.dataset_contracts import write_dataset  # noqa: E402
from src.data.financials.store import SCHEMAS, table_path  # noqa: E402

BACKUP = (PROJECT_ROOT / 'backups' / 'critical_assets' / 'data' / 'local_store'
          / 'meta' / 'financials' / 'income.parquet')
STAGING = PROJECT_ROOT / '_staging' / 'income_netprofit_fix_20261128'
KEY = ['ts_code', 'end_date', 'ann_date']


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def net_eq_parent_ratio(df: pl.DataFrame) -> dict[str, float]:
    d = df.filter(pl.col('net_profit').is_not_null()
                  & pl.col('parent_net_profit').is_not_null())
    if d.is_empty():
        return {}
    eq = ((d['net_profit'] - d['parent_net_profit']).abs() <= 0.5)
    d = d.with_columns(eq.alias('_eq'), pl.col('end_date').dt.year().alias('_y'))
    rows = d.group_by('_y').agg(pl.col('_eq').mean().alias('r')).sort('_y')
    return {str(int(r['_y'])): round(float(r['r']), 4) for r in rows.iter_rows(named=True)}


def build_fixed(cur: pl.DataFrame, bak: pl.DataFrame) -> tuple[pl.DataFrame, dict]:
    """以 11-11 快照为权威回填 net_profit 与 source；新增行保留并打 keep_local。"""
    bak_np = bak.select([*KEY, pl.col('net_profit').alias('_np_bak'),
                         pl.col('source').alias('_src_bak')])
    merged = cur.join(bak_np, on=KEY, how='left')
    # 快照里 net_profit 可能是 null（missing 分支），故以「该键是否在快照中出现」为准：
    # 用 `_src_bak` 非空作为命中标记（快照 source 每行都有值），net_profit 原样取快照值。
    hit = pl.col('_src_bak').is_not_null()
    fixed = merged.with_columns(
        pl.when(hit).then(pl.col('_np_bak')).otherwise(pl.col('net_profit')).alias('net_profit'),
        pl.when(hit).then(pl.col('_src_bak'))
        .otherwise(pl.col('source') + '|net_profit=keep_local').alias('source'),
    ).select(list(SCHEMAS['income']))
    new_keys = cur.select(KEY).join(bak.select(KEY), on=KEY, how='anti')
    stats = {'rows_cur': cur.height, 'rows_bak': bak.height,
             'overlap': cur.height - new_keys.height, 'new_rows': new_keys.height}
    return fixed, {'stats': stats}


def main() -> int:
    ap = argparse.ArgumentParser(description='修复 income.net_profit 周批回退（只改这一列）')
    ap.add_argument('--apply', action='store_true', help='写正式表')
    ap.add_argument('--dry-run', action='store_true', help='只验证不写盘')
    args = ap.parse_args()
    apply = args.apply and not args.dry_run

    src = table_path('income')
    cur = pl.read_parquet(src)
    bak = pl.read_parquet(BACKUP)
    cur_sha, bak_sha = sha256(src), sha256(BACKUP)
    if not bak_sha.startswith('d50d515a'):
        raise SystemExit(f'11-11 快照 SHA 与 manifest 登记态不符：{bak_sha}')

    fixed, meta = build_fixed(cur, bak)
    stats = meta['stats']

    # ---- DG-20 / DG-31 断言 ----
    if fixed.height != cur.height:
        raise AssertionError(f'行数不守恒：{cur.height} → {fixed.height}')
    became_null = int((cur['net_profit'].is_not_null()
                       & fixed['net_profit'].is_null()).sum())
    if became_null:
        raise AssertionError(f'{became_null} 行 net_profit 非空被写成 null')

    # ---- 与 11-11 快照逐值对拍（重叠行）----
    cmp = fixed.select([*KEY, pl.col('net_profit').alias('np_new'),
                        pl.col('source').alias('src_new')]).join(
        bak.select([*KEY, pl.col('net_profit').alias('np_bak'),
                    pl.col('source').alias('src_bak')]), on=KEY, how='inner')
    np_diff = int(((cmp['np_new'] - cmp['np_bak']).abs() > 0.5).sum())
    src_diff = int((cmp['src_new'] != cmp['src_bak']).sum())
    np_null_new, np_null_bak = int(cmp['np_new'].is_null().sum()), int(cmp['np_bak'].is_null().sum())

    # ---- 快照：周批回退态 vs 修复态，net==parent 逐年占比 ----
    ratio_before = net_eq_parent_ratio(cur)
    ratio_after = net_eq_parent_ratio(fixed)
    changed_cells = int(((fixed['net_profit'] - cur['net_profit']).abs() > 0.5).sum())

    STAGING.mkdir(parents=True, exist_ok=True)
    backup_dir = PROJECT_ROOT / '_backup' / f'financials_income_{datetime.now():%Y%m%d_%H%M%S}'
    if apply:
        backup_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, backup_dir / 'income.parquet')
        if sha256(backup_dir / 'income.parquet') != cur_sha:
            raise AssertionError('DG-22 快照 SHA 与改前不符')
        write_dataset('financials_income',
                      fixed.sort(['ts_code', 'end_date']),
                      writer='tools/_oneshot/fix_income_netprofit_regression_20261128.py')
    new_sha = sha256(src) if apply else cur_sha

    evidence = {
        'at': datetime.now().isoformat(timespec='seconds'),
        'apply': apply,
        'rows': {'before': cur.height, 'after': fixed.height,
                 'overlap': stats['overlap'], 'new_rows_preserved': stats['new_rows'],
                 'snapshot_rows': bak.height},
        'net_profit': {
            'changed_cells_vs_regressed': changed_cells,
            'diff_vs_snapshot_overlap': np_diff,
            'source_diff_vs_snapshot_overlap': src_diff,
            'null_new': np_null_new, 'null_snapshot': np_null_bak,
        },
        'net_eq_parent_ratio_before': ratio_before,
        'net_eq_parent_ratio_after': ratio_after,
        'sha256': {'before_regressed': cur_sha, 'snapshot_11_11': bak_sha,
                   'after': new_sha},
        'dg22_snapshot': str(backup_dir.relative_to(PROJECT_ROOT)) if apply else None,
    }
    (STAGING / ('after.json' if apply else 'dryrun.json')).write_text(
        json.dumps(evidence, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    print(json.dumps(evidence, ensure_ascii=False, indent=2, default=str))

    if np_diff or src_diff:
        raise SystemExit(f'与 11-11 快照对拍未通过：net_profit 差异 {np_diff}，source 差异 {src_diff}')
    if not apply:
        print('[DRY-RUN] 未写盘')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
