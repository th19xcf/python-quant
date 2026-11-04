#!/usr/bin/env python3
"""分红派现总额派生列（只读股本表，不回填源端 total_div）。

派生口径
========
* ``total_div_est_all`` = ``cash_div`` × ``shares_as_of(ts_code, ex_date)`` 的
  **总股本**；
* ``total_div_est_float`` = ``cash_div`` × 同一时点的 **流通股本**；
* 同时写入 ``est_total_share`` / ``est_float_share`` / ``est_effective_date``，
  记录实际采用的股本快照与生效日，供审计复核。

点时语义
========
股本一律取 ``effective_date <= ex_date`` 的最后一行（forward-fill），
**不使用当前股本回填历史**；``ex_date`` 为空、``cash_div`` 为空、股本无覆盖
的行一律置 null，并在报告中按原因计数。

写入
====
先备份原 parquet 到 ``data/local_store/_backup/``，再原子替换同一文件；
不写 MySQL、不修改 ``total_div``、不重算复权因子。

用法::

    python tools/derive_total_dividend.py --dry-run
    python tools/derive_total_dividend.py
"""

# ruff: noqa: T201

from __future__ import annotations

import argparse
import json
import os
import shutil
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DIV = PROJECT_ROOT / 'data/local_store/factors/stock_dividend.parquet'
DEFAULT_SHARES = PROJECT_ROOT / 'data/local_store/meta/factors/stock_shares.parquet'
DEFAULT_BACKUP = PROJECT_ROOT / 'data/local_store/_backup'
DEFAULT_REPORT = PROJECT_ROOT / 'reports/stock_dividend_20261105/total_div_est.json'

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


def load_shares(path: Path) -> pl.DataFrame:
    """读取股本区间事实表，只保留派生所需列并按 (票, 生效日) 升序。"""
    frame = pl.read_parquet(
        path, columns=['ts_code', 'effective_date', 'total_share', 'float_share'])
    return frame.sort(['ts_code', 'effective_date'])


def derive(frame: pl.DataFrame, shares: pl.DataFrame) -> pl.DataFrame:
    """在原表上追加两版派生列；原列与行序保持不变。

    两版口径：
    * ``*_est_all/float``：股本取 ``ex_date`` 当日（原口径，送转事件会含新增股份）；
    * ``*_est_rec_all/float``：股本取 ``record_date`` 当日（派现基数口径）；
      ``record_date`` 为空时不回退到 ``ex_date``，该行 rec 列置 null。

    同一 ``(ts_code, 日期)`` 可能有多笔分红，按行号回填，避免 join 后错行。
    """
    base = frame.drop([c for c in DERIVED_SCHEMA if c in frame.columns])
    numbered = base.with_row_index('_row_id')

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

    out = (numbered.join(estimates, on='_row_id', how='left')
           .join(rec_estimates, on='_row_id', how='left')
           .drop('_row_id'))
    return out.select([*base.columns, *DERIVED_SCHEMA])


def coverage(frame: pl.DataFrame) -> dict[str, Any]:
    """统计两版可算/不可算数量与缺失原因（必须来自实际派生结果）。"""
    total = frame.height
    ex_null = frame.filter(pl.col('ex_date').is_null()).height
    rec_null = frame.filter(pl.col('record_date').is_null()).height
    ex_cash_null = frame.filter(pl.col('ex_date').is_not_null(),
                                 pl.col('cash_div').is_null()).height
    rec_cash_null = frame.filter(pl.col('record_date').is_not_null(),
                                 pl.col('cash_div').is_null()).height
    ex_base = pl.col('ex_date').is_not_null() & pl.col('cash_div').is_not_null()
    rec_base = pl.col('record_date').is_not_null() & pl.col('cash_div').is_not_null()

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


def backup_file(path: Path, backup_dir: Path) -> Path:
    """备份原文件，文件名带时间戳，不覆盖既有备份。"""
    backup_dir.mkdir(parents=True, exist_ok=True)
    target = backup_dir / f'{path.stem}.{datetime.now():%Y%m%d_%H%M%S}{path.suffix}'
    shutil.copy2(path, target)
    return target


def write_report(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str),
                   encoding='utf-8', newline='\n')
    os.replace(tmp, path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='派生分红派现总额（总股本/流通股本两口径）')
    parser.add_argument('--div', type=Path, default=DEFAULT_DIV)
    parser.add_argument('--shares', type=Path, default=DEFAULT_SHARES)
    parser.add_argument('--backup-dir', type=Path, default=DEFAULT_BACKUP)
    parser.add_argument('--report', type=Path, default=DEFAULT_REPORT)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args(argv)

    frame = pl.read_parquet(args.div)
    shares = load_shares(args.shares)
    out = derive(frame, shares)
    stats = coverage(out)

    report = {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'dividend_path': str(args.div),
        'shares_path': str(args.shares),
        'shares_rows': shares.height,
        'shares_codes': shares['ts_code'].n_unique(),
        'columns': {
            'total_div_est_all': 'cash_div × ex_date 当日总股本（原口径，送转事件含新增股份）',
            'total_div_est_float': 'cash_div × ex_date 当日流通股本（原口径）',
            'est_total_share': 'ex_date 口径实际采用的总股本（股）',
            'est_float_share': 'ex_date 口径实际采用的流通股本（股）',
            'est_effective_date': 'ex_date 口径采用的股本生效日（effective_date <= ex_date）',
            'total_div_est_rec_all': 'cash_div × record_date 当日总股本（派现基数口径）',
            'total_div_est_rec_float': 'cash_div × record_date 当日流通股本（派现基数口径）',
            'est_rec_total_share': 'record_date 口径实际采用的总股本（股）',
            'est_rec_float_share': 'record_date 口径实际采用的流通股本（股）',
            'est_rec_effective_date': (
                'record_date 口径采用的股本生效日（effective_date <= record_date）；'
                'record_date 为空时不回退 ex_date'),
        },
        'coverage': stats,
        'before_rows': frame.height,
        'after_rows': out.height,
        'before_columns': frame.columns,
        'after_columns': out.columns,
    }
    write_report(args.report, report)
    if args.dry_run:
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
        return 0

    backup = backup_file(args.div, args.backup_dir)
    tmp = args.div.with_suffix('.parquet.tmp')
    out.write_parquet(tmp, compression='zstd', statistics=True)
    os.replace(tmp, args.div)
    print(f'已写入 {args.div}：{out.height:,} 行，新增 {len(DERIVED_SCHEMA)} 列')
    print(f'备份：{backup}')
    print(f'总股本口径可算 {stats["est_all"]["computable"]:,} / '
          f'{stats["rows"]:,}（{stats["est_all"]["coverage_rate"]:.2%}），'
          f'流通股本口径可算 {stats["est_float"]["computable"]:,} '
          f'（{stats["est_float"]["coverage_rate"]:.2%}）')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
