#!/usr/bin/env python3
"""分红派现总额派生列（只读股本表，不回填源端 total_div）。

现金口径 = 有效层
==================
每股派现一律取**有效层** ``{base}_fix.fill_null(原始列)``，并剔除
``no_ex_rights=True`` 的事件；取值实现收口在 ``src/data/dividend_effective.py``，
本文件不再写第二份 ``fill_null``。理由：复权因子层自 2026-11-10 起读有效层，
派生列若只读原始层，同一张表就会有两个口径 —— 601258.SH 2019-12-30
（原始 ``cash_div=0.0`` / ``cash_div_fix=0.04``）的派生总分红停在 0，
与按 0.04 生效的复权因子对不上。

派生口径
========
* ``total_div_est_all`` = 有效 ``cash_div`` × ``shares_as_of(ts_code, ex_date)``
  的 **总股本**；
* ``total_div_est_float`` = 有效 ``cash_div`` × 同一时点的 **流通股本**；
* 同时写入 ``est_total_share`` / ``est_float_share`` / ``est_effective_date``，
  记录实际采用的股本快照与生效日，供审计复核。

点时语义
========
股本一律取 ``effective_date <= ex_date`` 的最后一行（forward-fill），
**不使用当前股本回填历史**；``ex_date`` 为空、有效 ``cash_div`` 为空、股本无覆盖
的行一律置 null，并在报告中按原因计数。``no_ex_rights=True`` 的事件不向原股东
分配，现金派生列显式写 **0.0**（不是 null —— null 表示「算不出」，此处是「已知为 0」）。

写入
====
只替换「有效现金 ≠ 原始现金」的行；未命中任何分支的行**原样写回原值**（DG-31）。
先 ``copy2`` 备份原 parquet 到 ``data/local_store/_backup/``，再走
``src/data/atomic_write.write_parquet_atomic`` 原子替换同一文件；
不写 MySQL、不修改 ``total_div``、不重算复权因子。

用法::

    python tools/derive_total_dividend.py --dry-run
    python tools/derive_total_dividend.py
"""

# ruff: noqa: T201

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data.atomic_write import write_parquet_atomic  # noqa: E402
from src.data.dividend_effective import (  # noqa: E402
    apply_effective_columns,
    drop_non_ex_rights,
    effective_value,
)

DEFAULT_DIV = PROJECT_ROOT / 'data/local_store/factors/stock_dividend.parquet'
DEFAULT_SHARES = PROJECT_ROOT / 'data/local_store/meta/factors/stock_shares.parquet'
DEFAULT_BACKUP = PROJECT_ROOT / '_backup'
DEFAULT_REPORT = (
    PROJECT_ROOT / 'reports/dividend_derived_20261110/total_div_effective.json')

DERIVED_SCHEMA: dict[str, Any] = {
    'total_div_est_all': pl.Float64,
    'total_div_est_float': pl.Float64,
    'est_total_share': pl.Int64,
    'est_float_share': pl.Int64,
    'est_effective_date': pl.Date,
    'total_div_est_rec_all': pl.Float64,
    'total_div_est_rec_float': pl.Float64,
    'est_rec_total_share': pl.Int64,
    'est_rec_float_share': pl.Int64,
    'est_rec_effective_date': pl.Date,
}
#: 四个金额派生列（本次口径变更的目标列）
CASH_COLUMNS: tuple[str, ...] = (
    'total_div_est_all', 'total_div_est_float',
    'total_div_est_rec_all', 'total_div_est_rec_float')
CONTEXT = 'derive_total_dividend'


def load_shares(path: Path) -> pl.DataFrame:
    """读取股本区间事实表，只保留派生所需列并按 (票, 生效日) 升序。"""
    frame = pl.read_parquet(
        path, columns=['ts_code', 'effective_date', 'total_share', 'float_share'])
    return frame.sort(['ts_code', 'effective_date'])


def effective_cash(frame: pl.DataFrame) -> pl.Series:
    """有效层每股派现（``cash_div_fix.fill_null(cash_div)``），供影响面判定与报告。"""
    overlay = apply_effective_columns(frame, context=f'{CONTEXT}.effective_cash')
    return overlay['cash_div'].cast(pl.Float64)


def affected_rows(frame: pl.DataFrame) -> pl.Series:
    """有效现金与原始现金不同的行：只有这些行的派生列会被重算覆盖。"""
    raw = frame['cash_div'].cast(pl.Float64)
    eff = effective_cash(frame)
    return ((raw - eff).abs().fill_null(0.0) > 1e-12) | (raw.is_null() != eff.is_null())


def _estimate(numbered: pl.DataFrame, shares: pl.DataFrame) -> pl.DataFrame:
    """按行号计算两版派生列（``_row_id`` 为键，避免同日多笔分红 join 后错行）。"""
    events = numbered.filter(pl.col('ex_date').is_not_null())
    joined = events.sort(['ts_code', 'ex_date']).join_asof(
        shares,
        left_on='ex_date',
        right_on='effective_date',
        by='ts_code',
        strategy='backward',
    )
    estimates = joined.select([
        '_row_id',
        (pl.col('cash_div') * pl.col('total_share').cast(pl.Float64))
        .alias('total_div_est_all'),
        (pl.col('cash_div') * pl.col('float_share').cast(pl.Float64))
        .alias('total_div_est_float'),
        pl.col('total_share').cast(pl.Int64, strict=False).alias('est_total_share'),
        pl.col('float_share').cast(pl.Int64, strict=False).alias('est_float_share'),
        pl.col('effective_date').alias('est_effective_date'),
    ])

    rec_events = numbered.filter(pl.col('record_date').is_not_null())
    joined_rec = rec_events.sort(['ts_code', 'record_date']).join_asof(
        shares,
        left_on='record_date',
        right_on='effective_date',
        by='ts_code',
        strategy='backward',
    )
    rec_estimates = joined_rec.select([
        '_row_id',
        (pl.col('cash_div') * pl.col('total_share').cast(pl.Float64))
        .alias('total_div_est_rec_all'),
        (pl.col('cash_div') * pl.col('float_share').cast(pl.Float64))
        .alias('total_div_est_rec_float'),
        pl.col('total_share').cast(pl.Int64, strict=False).alias('est_rec_total_share'),
        pl.col('float_share').cast(pl.Int64, strict=False).alias('est_rec_float_share'),
        pl.col('effective_date').alias('est_rec_effective_date'),
    ])
    return estimates.join(rec_estimates, on='_row_id', how='full', coalesce=True)


def derive(frame: pl.DataFrame, shares: pl.DataFrame) -> pl.DataFrame:
    """在原表上追加十列派生值；原列与行序保持不变。

    现金口径 = 有效层（``apply_effective_columns``），``no_ex_rights=True`` 的
    事件由 ``drop_non_ex_rights`` 判定剔除后，四个金额列显式写 0.0。

    两版股本口径：
    * ``*_est_all/float``：股本取 ``ex_date`` 当日（原口径，送转事件会含新增股份）；
    * ``*_est_rec_all/float``：股本取 ``record_date`` 当日（派现基数口径）；
      ``record_date`` 为空时不回退到 ``ex_date``，该行 rec 列置 null。
    """
    base = frame.drop([c for c in DERIVED_SCHEMA if c in frame.columns])
    effective = apply_effective_columns(base, context=f'{CONTEXT}.derive')
    numbered = effective.with_row_index('_row_id')
    kept = drop_non_ex_rights(numbered, context=f'{CONTEXT}.derive')
    # 被剔除的事件：现金已知则金额列写 0（已知不分配），现金缺失仍为 null（算不出）
    excluded = numbered.join(
        kept.select('_row_id'), on='_row_id', how='anti').select(
            '_row_id', pl.col('cash_div').is_not_null().alias('_cash_known'))

    estimates = _estimate(numbered, shares)
    out = (base.with_row_index('_row_id')
           .join(estimates, on='_row_id', how='left')
           .join(excluded, on='_row_id', how='left'))
    no_distribution = pl.col('_cash_known').fill_null(False)
    out = out.with_columns([
        pl.when(no_distribution).then(pl.lit(0.0)).otherwise(pl.col(col)).alias(col)
        for col in CASH_COLUMNS])
    out = out.drop(['_row_id', '_cash_known'])
    return out.select([*base.columns, *DERIVED_SCHEMA])


def apply_changes(frame: pl.DataFrame, new: pl.DataFrame,
                  affected: pl.Series) -> pl.DataFrame:
    """只把受影响行的派生列替换为新值；未命中分支的行写回原值（DG-31）。

    显式 ``when(affected).then(新值).otherwise(原值)``，没有 ``coalesce`` 兜底：
    未命中行拿到的就是它自己的存量值，不可能被写成 null。
    """
    staged = frame.hstack(
        new.select([pl.col(c).alias(f'{c}__new') for c in DERIVED_SCHEMA]))
    staged = staged.with_columns(affected.alias('__affected'))
    for col in DERIVED_SCHEMA:
        if col not in frame.columns:  # 新表首次派生：没有存量值可写回
            staged = staged.with_columns(pl.col(f'{col}__new').alias(col))
            continue
        staged = staged.with_columns(
            pl.when(pl.col('__affected'))
            .then(pl.col(f'{col}__new'))
            .otherwise(pl.col(col))
            .alias(col))
    columns = [*frame.columns, *[c for c in DERIVED_SCHEMA if c not in frame.columns]]
    return staged.select(columns)


def null_counts(frame: pl.DataFrame) -> dict[str, int]:
    return {col: frame[col].null_count() for col in frame.columns}


def assert_no_new_nulls(before: pl.DataFrame, after: pl.DataFrame) -> None:
    """DG-31：逐列空值计数必须与改动前完全相等（能对「0 → N」这一情形失败）。"""
    if after.height != before.height:
        raise RuntimeError(f'行数不守恒：{before.height} → {after.height}')
    old = null_counts(before)
    new = null_counts(after)
    regressed = {c: (old[c], new[c]) for c in before.columns if old[c] != new[c]}
    if regressed:
        raise RuntimeError(f'空值计数变化（旧 → 新）：{regressed}')
    for col in before.columns:
        old_known = before[col].is_not_null()
        newly_null = after[col].is_null() & old_known
        if newly_null.any():
            raise RuntimeError(
                f'{col}：{int(newly_null.sum())} 行既有非空值被写成 null')


def coverage(frame: pl.DataFrame) -> dict[str, Any]:
    """统计两版可算/不可算数量与缺失原因（必须来自实际派生结果）。"""
    total = frame.height
    cash = effective_cash(frame)
    ex_null = frame.filter(pl.col('ex_date').is_null()).height
    rec_null = frame.filter(pl.col('record_date').is_null()).height
    has_ex = frame['ex_date'].is_not_null()
    has_rec = frame['record_date'].is_not_null()
    ex_cash_null = int((has_ex & cash.is_null()).sum())
    rec_cash_null = int((has_rec & cash.is_null()).sum())
    ex_base = has_ex & cash.is_not_null()
    rec_base = has_rec & cash.is_not_null()

    all_ok = frame.filter(ex_base, pl.col('est_total_share').is_not_null()).height
    all_missing_share = frame.filter(ex_base, pl.col('est_total_share').is_null()).height
    float_ok = frame.filter(ex_base, pl.col('est_float_share').is_not_null(),
                            pl.col('est_float_share') > 0).height
    float_missing = frame.filter(ex_base, pl.col('est_float_share').is_null()
                                 | (pl.col('est_float_share') <= 0)).height
    rec_all_ok = frame.filter(rec_base, pl.col('est_rec_total_share').is_not_null()).height
    rec_all_missing = frame.filter(rec_base, pl.col('est_rec_total_share').is_null()).height
    rec_float_ok = frame.filter(rec_base, pl.col('est_rec_float_share').is_not_null(),
                                pl.col('est_rec_float_share') > 0).height
    rec_float_missing = frame.filter(rec_base, pl.col('est_rec_float_share').is_null()
                                     | (pl.col('est_rec_float_share') <= 0)).height

    effective = frame.filter(pl.col('est_effective_date').is_not_null())
    rec_effective = frame.filter(pl.col('est_rec_effective_date').is_not_null())
    return {
        'rows': total,
        'ex_date_null': ex_null,
        'record_date_null': rec_null,
        'cash_div_null_with_ex': ex_cash_null,
        'cash_div_null_with_rec': rec_cash_null,
        'est_all': {
            'computable': all_ok,
            'missing': ex_null + ex_cash_null + all_missing_share,
            'coverage_rate': round(all_ok / total, 6) if total else 0.0,
            'missing_reasons': {
                'ex_date_null': ex_null,
                'cash_div_null': ex_cash_null,
                'share_asof_missing': all_missing_share,
            },
        },
        'est_float': {
            'computable': float_ok,
            'missing': ex_null + ex_cash_null + float_missing,
            'coverage_rate': round(float_ok / total, 6) if total else 0.0,
            'missing_reasons': {
                'ex_date_null': ex_null,
                'cash_div_null': ex_cash_null,
                'float_share_missing': float_missing,
            },
        },
        'est_rec_all': {
            'computable': rec_all_ok,
            'missing': rec_null + rec_cash_null + rec_all_missing,
            'coverage_rate': round(rec_all_ok / total, 6) if total else 0.0,
            'missing_reasons': {
                'record_date_null': rec_null,
                'cash_div_null': rec_cash_null,
                'share_asof_missing': rec_all_missing,
            },
        },
        'est_rec_float': {
            'computable': rec_float_ok,
            'missing': rec_null + rec_cash_null + rec_float_missing,
            'coverage_rate': round(rec_float_ok / total, 6) if total else 0.0,
            'missing_reasons': {
                'record_date_null': rec_null,
                'cash_div_null': rec_cash_null,
                'float_share_missing': rec_float_missing,
            },
        },
        'est_effective_date': {
            'non_null': effective.height,
            'earliest': str(effective['est_effective_date'].min())
            if effective.height else None,
            'latest': str(effective['est_effective_date'].max())
            if effective.height else None,
        },
        'est_rec_effective_date': {
            'non_null': rec_effective.height,
            'earliest': str(rec_effective['est_rec_effective_date'].min())
            if rec_effective.height else None,
            'latest': str(rec_effective['est_rec_effective_date'].max())
            if rec_effective.height else None,
        },
        'sum_est_all': round(float(frame['total_div_est_all'].sum() or 0.0), 2),
        'sum_est_float': round(float(frame['total_div_est_float'].sum() or 0.0), 2),
        'sum_est_rec_all': round(float(frame['total_div_est_rec_all'].sum() or 0.0), 2),
        'sum_est_rec_float': round(
            float(frame['total_div_est_rec_float'].sum() or 0.0), 2),
    }


def change_details(before: pl.DataFrame, after: pl.DataFrame,
                   affected: pl.Series) -> list[dict[str, Any]]:
    """逐行列出受影响行的前后值与原因（按行序，全部取自实际结果）。"""
    rows = []
    old_rows = before.filter(pl.Series('aff', affected)).iter_rows(named=True)
    new_rows = after.filter(pl.Series('aff', affected)).iter_rows(named=True)
    for old, new in zip(old_rows, new_rows, strict=True):
        eff_cash = effective_value(old, 'cash_div')
        changes = {}
        for col in DERIVED_SCHEMA:
            if old[col] != new[col] or (
                    old[col] is None) != (new[col] is None):
                changes[col] = {'old': old[col], 'new': new[col]}
        if old.get('no_ex_rights'):
            reason = ('no_ex_rights=True：事件不向原股东分配，现金派生列固定 0.0')
        elif old.get('cash_div_fix') is not None:
            reason = (f'有效层 cash_div_fix={old["cash_div_fix"]} 覆盖原始 '
                      f'cash_div={old["cash_div"]}，按有效值重算')
        else:
            reason = '有效层与原始层不同，按有效值重算'
        rows.append({
            'ts_code': old['ts_code'],
            'report_date': old.get('report_date'),
            'report_period': str(old['report_period']) if old.get('report_period') else None,
            'record_date': str(old['record_date']) if old.get('record_date') else None,
            'ex_date': str(old['ex_date']) if old.get('ex_date') else None,
            'cash_div_raw': old.get('cash_div'),
            'cash_div_fix': old.get('cash_div_fix'),
            'cash_div_effective': eff_cash,
            'no_ex_rights': bool(old.get('no_ex_rights') or False),
            'changes': changes,
            'reason': reason,
        })
    return rows


def compare_derived(before: pl.DataFrame, recomputed: pl.DataFrame,
                    affected: pl.Series) -> dict[str, Any]:
    """对拍：纯重算结果与存量逐列比，按「受影响 / 未受影响」分组统计差异行数。

    ``recomputed`` 必须是 ``derive()`` 的原始输出（未经过 ``apply_changes``），
    否则未受影响行的比对会退化成「存量与自己比」，永远是 0。
    """
    mask = pl.Series('__affected', affected)
    groups = {
        'affected': (before.filter(mask), recomputed.filter(mask)),
        'unaffected': (before.filter(~mask), recomputed.filter(~mask)),
    }
    drift: dict[str, dict[str, int]] = {}
    for name, (old, new) in groups.items():
        drift[name] = {}
        for col in DERIVED_SCHEMA:
            a, b = old[col], new[col]
            differs = (a != b) & ~(a.is_null() & b.is_null())
            drift[name][col] = int(differs.sum())
    return {
        'affected_rows': int(mask.sum()),
        'recomputed_drift': drift,
        'unaffected_cash_drift': {c: drift['unaffected'][c] for c in CASH_COLUMNS},
    }


def backup_file(path: Path, backup_dir: Path) -> Path:
    """备份原文件，文件名带时间戳，不覆盖既有备份。"""
    backup_dir.mkdir(parents=True, exist_ok=True)
    target = backup_dir / f'{path.stem}.{datetime.now():%Y%m%d_%H%M%S}{path.suffix}'
    shutil.copy2(path, target)
    return target


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(4 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def write_report(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str),
                   encoding='utf-8', newline='\n')
    os.replace(tmp, path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='派生分红派现总额（有效层口径，两版股本）')
    parser.add_argument('--div', type=Path, default=DEFAULT_DIV)
    parser.add_argument('--shares', type=Path, default=DEFAULT_SHARES)
    parser.add_argument('--backup-dir', type=Path, default=DEFAULT_BACKUP)
    parser.add_argument('--report', type=Path, default=DEFAULT_REPORT)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args(argv)

    sha_before = file_sha256(args.div)
    frame = pl.read_parquet(args.div)
    shares = load_shares(args.shares)
    affected = affected_rows(frame)
    new = derive(frame, shares)
    out = apply_changes(frame, new, affected)
    assert_no_new_nulls(frame, out)
    comparison = compare_derived(frame, new, affected)
    details = change_details(frame, out, affected)
    stats = coverage(out)
    nulls_before = null_counts(frame)
    nulls_after = null_counts(out)

    changed_cells = sum(len(item['changes']) for item in details)
    existing_nonnull_changed = sum(
        1 for item in details for change in item['changes'].values()
        if change['old'] is not None)
    if comparison['unaffected_cash_drift'] and any(
            comparison['unaffected_cash_drift'].values()):
        raise RuntimeError(
            f'未受影响行的金额派生列出现漂移：{comparison["unaffected_cash_drift"]}')

    report = {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'dividend_path': str(args.div),
        'shares_path': str(args.shares),
        'cash_basis': {
            'layer': 'effective',
            'module': 'src/data/dividend_effective.py',
            'functions': ['apply_effective_columns', 'drop_non_ex_rights',
                          'effective_value'],
            'rule': '有效值 = {base}_fix.fill_null(原始列)；no_ex_rights=True 的事件'
                    '现金派生列写 0.0',
        },
        'shares_rows': shares.height,
        'shares_codes': shares['ts_code'].n_unique(),
        'columns': {
            'total_div_est_all': '有效 cash_div × ex_date 当日总股本（送转事件含新增股份）',
            'total_div_est_float': '有效 cash_div × ex_date 当日流通股本',
            'est_total_share': 'ex_date 口径实际采用的总股本（股）',
            'est_float_share': 'ex_date 口径实际采用的流通股本（股）',
            'est_effective_date': 'ex_date 口径采用的股本生效日（effective_date <= ex_date）',
            'total_div_est_rec_all': '有效 cash_div × record_date 当日总股本（派现基数口径）',
            'total_div_est_rec_float': '有效 cash_div × record_date 当日流通股本（派现基数口径）',
            'est_rec_total_share': 'record_date 口径实际采用的总股本（股）',
            'est_rec_float_share': 'record_date 口径实际采用的流通股本（股）',
            'est_rec_effective_date': (
                'record_date 口径采用的股本生效日（effective_date <= record_date）；'
                'record_date 为空时不回退 ex_date'),
        },
        'coverage': stats,
        'rows_before': frame.height,
        'rows_after': out.height,
        'rows_conserved': frame.height == out.height,
        'sha256': {'before': sha_before, 'after': None, 'backup': None},
        'changed_rows': len(details),
        'changed_cells': changed_cells,
        'existing_nonnull_changed': existing_nonnull_changed,
        'changed_rows_detail': details,
        'comparison': comparison,
        'null_counts': {
            col: {'before': nulls_before[col], 'after': nulls_after[col]}
            for col in DERIVED_SCHEMA
        },
        'before_columns': frame.columns,
        'after_columns': out.columns,
    }
    write_report(args.report, report)
    if args.dry_run:
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
        return 0

    backup = backup_file(args.div, args.backup_dir)
    write_parquet_atomic(out, args.div, compression='zstd', statistics=True)
    sha_after = file_sha256(args.div)
    report['sha256'].update({'after': sha_after, 'backup': backup.name,
                             'backup_sha256': file_sha256(backup)})
    write_report(args.report, report)

    print(f'已写入 {args.div}：{out.height:,} 行，派生列 {len(DERIVED_SCHEMA)} 个')
    print(f'改动 {len(details)} 行 / {changed_cells} 个单元格'
          f'（既有非空值改动 {existing_nonnull_changed}）')
    for item in details:
        print(f'  {item["ts_code"]} ex_date={item["ex_date"]}：'
              f'{item["reason"]}')
        for col, change in item['changes'].items():
            print(f'    {col}: {change["old"]} → {change["new"]}')
    print(f'备份：{backup}（SHA256 {report["sha256"]["backup_sha256"][:8]}…）')
    print(f'SHA256 {sha_before[:8]}… → {sha_after[:8]}…')
    print(f'总股本口径可算 {stats["est_all"]["computable"]:,} / '
          f'{stats["rows"]:,}（{stats["est_all"]["coverage_rate"]:.2%}），'
          f'流通股本口径可算 {stats["est_float"]["computable"]:,} '
          f'（{stats["est_float"]["coverage_rate"]:.2%}）')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
