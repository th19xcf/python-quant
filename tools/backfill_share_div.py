#!/usr/bin/env python3
"""分红事件表送转字段缺失扫描与回补（只读股本表，不回写 MySQL）。

判定逻辑
========
对 ``share_div`` 为空或 <= 0 的事件行，取其 ``ex_date``（缺失退回
``record_date``）当天与前一天（``day - 1``）的股本快照：

* 股本增加，且股本表变更原因仅含「送股/转增/送转」→ 候选；
* 隐含比例 ``cur/prev - 1`` 能落到 0.01 档位（容差 0.003）→ **确认缺失**；
* 比例偏离档位、同日多个股本事件、原因缺失 → **无法解释**；
* 变更原因含增发/回购/注销/配售/上市等 → **误报**，不回补。

回补原则
========
只回补「确认缺失」且原值为空/0 的行，用 ``round(隐含比例, 2)`` 写入
``share_div``，并新增 ``share_div_source``（``orig`` / ``inferred_from_shares``
/ ``announcement``）。不覆盖任何非空非零原值，不联网猜数。

回补后重算两版派现总额列，并输出回补前后的送转/无送转校验对照。

用法::

    python tools/backfill_share_div.py --dry-run
    python tools/backfill_share_div.py
"""

# ruff: noqa: T201

from __future__ import annotations

import argparse
import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DIV = PROJECT_ROOT / 'data/local_store/factors/stock_dividend.parquet'
DEFAULT_SHARES = PROJECT_ROOT / 'data/local_store/meta/factors/stock_shares.parquet'
DEFAULT_BACKUP = PROJECT_ROOT / 'data/local_store/_backup'
DEFAULT_REPORT = PROJECT_ROOT / 'reports/stock_dividend_20261105/share_div_backfill.json'

SPLIT_WORDS = ('送股', '转增', '送转')
#: 不含过于宽泛的「上市」——「送股上市」「转增股上市」本身是送转，不是干扰事件。
OTHER_WORDS = (
    '增发', '回购', '注销', '配售', '债转股', '股权激励', '限售', '流通',
    '高管', '首发', '超额配售', '非公开发行', '可转债', '合并', '分立',
    '行权', '期权', '上市前股本',
)
#: 常见送转档位步长：10 送 1 / 10 送 1.5 / 10 转 4.5 / 10 送 10 等均落在 0.05。
#: 反推比例偏离最近档位超过 ``RATIO_TOL`` 即视为无法解释，不回补。
GRID_STEP = 0.05
RATIO_TOL = 0.003
MIN_RATIO = 0.05
MAX_RATIO = 2.0
SOURCE_ORIG = 'orig'
SOURCE_INFERRED = 'inferred_from_shares'


def classify(
    prev_share: int | None,
    cur_share: int | None,
    reason: str | None,
    *,
    multiple: bool = False,
) -> dict[str, Any] | None:
    """对单个股本变化判定类别；无变化返回 None。"""
    if prev_share is None or cur_share is None or cur_share <= prev_share:
        return None
    implied = cur_share / prev_share - 1
    if multiple:
        return {'category': 'unexplained', 'reason': 'multiple_share_events',
                'implied': implied, 'inferred': None}
    text = reason or ''
    has_split = any(w in text for w in SPLIT_WORDS)
    has_other = any(w in text for w in OTHER_WORDS)
    if has_other and not has_split:
        return {'category': 'false_positive', 'reason': text,
                'implied': implied, 'inferred': None}
    if not has_split or has_other:
        return {'category': 'unexplained',
                'reason': text or 'missing_change_reason',
                'implied': implied, 'inferred': None}
    nearest = round(round(implied / GRID_STEP) * GRID_STEP, 2)
    if not (MIN_RATIO <= implied <= MAX_RATIO) or abs(implied - nearest) > RATIO_TOL:
        return {'category': 'unexplained', 'reason': f'ratio_off_grid:{implied:.4f}',
                'implied': implied, 'inferred': None}
    return {'category': 'confirmed', 'reason': text, 'implied': implied,
            'inferred': nearest}


def _asof_snapshot(cand: pl.DataFrame, shares: pl.DataFrame, key: str,
                   suffix: str) -> pl.DataFrame:
    table = shares.select(['ts_code', 'effective_date', 'total_share',
                           'change_reason']).sort(['ts_code', 'effective_date'])
    table = table.rename({
        'effective_date': f'effective_date{suffix}',
        'total_share': f'total_share{suffix}',
        'change_reason': f'change_reason{suffix}',
    })
    return cand.sort(['ts_code', key]).join_asof(
        table, left_on=key, right_on=f'effective_date{suffix}', by='ts_code',
        strategy='backward')


def scan(frame: pl.DataFrame, shares: pl.DataFrame) -> list[dict[str, Any]]:
    """扫描 share_div 空缺且股本当日有增加的事件行，返回逐行判定。"""
    dup = (shares.group_by(['ts_code', 'effective_date']).len()
           .filter(pl.col('len') > 1)
           .select(['ts_code', 'effective_date'])
           .with_columns(pl.lit(True).alias('_multiple'))
           .rename({'effective_date': 'effective_date_cur'}))
    candidates = (frame.with_row_index('_row_id')
                  .filter(pl.col('share_div').is_null() | (pl.col('share_div') <= 0),
                          pl.col('ex_date').is_not_null()
                          | pl.col('record_date').is_not_null())
                  .with_columns(pl.coalesce(['ex_date', 'record_date']).alias('_day'),
                                (pl.coalesce(['ex_date', 'record_date'])
                                 - pl.duration(days=1)).alias('_prev_day')))
    cur = _asof_snapshot(candidates, shares, '_day', '_cur')
    prev = _asof_snapshot(candidates, shares, '_prev_day', '_prev')
    merged = cur.join(prev.select(['_row_id', 'total_share_prev',
                                   'effective_date_prev', 'change_reason_prev']),
                      on='_row_id', how='left')
    merged = merged.filter(pl.col('total_share_prev').is_not_null(),
                           pl.col('total_share_cur').is_not_null(),
                           pl.col('total_share_cur') > pl.col('total_share_prev'))
    merged = merged.join(dup, on=['ts_code', 'effective_date_cur'], how='left')
    merged = merged.with_columns(pl.col('_multiple').fill_null(False))

    decisions: list[dict[str, Any]] = []
    for row in merged.to_dicts():
        verdict = classify(row['total_share_prev'], row['total_share_cur'],
                           row['change_reason_cur'], multiple=row['_multiple'])
        if verdict is None:
            continue
        decisions.append({
            '_row_id': row['_row_id'],
            'ts_code': row['ts_code'],
            'day': str(row['_day']),
            'prev_share': row['total_share_prev'],
            'cur_share': row['total_share_cur'],
            'implied_ratio': round(verdict['implied'], 6),
            'shift_10': round(verdict['implied'] * 10, 2),
            'share_div_before': row['share_div'],
            'change_reason': verdict['reason'],
            'category': verdict['category'],
            'inferred_share_div': verdict['inferred'],
        })
    return decisions


def apply_backfill(frame: pl.DataFrame,
                   decisions: list[dict[str, Any]]) -> tuple[pl.DataFrame, int]:
    """只把确认缺失写入空/零值行，返回 (新表, 实际回补行数)。"""
    fixes = [d for d in decisions if d['category'] == 'confirmed'
             and d['inferred_share_div'] is not None]
    if 'share_div_source' in frame.columns:
        out = frame.with_columns(pl.col('share_div_source').fill_null(SOURCE_ORIG))
    else:
        out = frame.with_columns(pl.lit(SOURCE_ORIG).alias('share_div_source'))
    if not fixes:
        return out, 0
    patch = pl.DataFrame({
        '_row_id': [d['_row_id'] for d in fixes],
        '_share_div_fix': [float(d['inferred_share_div']) for d in fixes],
    })
    out = out.with_row_index('_row_id').join(patch, on='_row_id', how='left')
    fill = (pl.col('_share_div_fix').is_not_null()
            & (pl.col('share_div').is_null() | (pl.col('share_div') <= 0)))
    filled_n = out.filter(fill).height
    out = out.with_columns(
        pl.when(fill).then(pl.col('_share_div_fix')).otherwise(pl.col('share_div'))
        .alias('share_div'),
        pl.when(fill).then(pl.lit(SOURCE_INFERRED))
        .otherwise(pl.col('share_div_source')).alias('share_div_source'),
    ).drop('_row_id', '_share_div_fix')
    return out, filled_n


def load_shares_all(path: Path) -> pl.DataFrame:
    """读取股本表全字段（含变更原因），按 (票, 生效日) 升序。"""
    frame = pl.read_parquet(path)
    need = {'ts_code', 'effective_date', 'total_share', 'float_share', 'change_reason'}
    missing = need - set(frame.columns)
    if missing:
        raise ValueError(f'股本表缺少字段: {sorted(missing)}')
    return frame.select(['ts_code', 'effective_date', 'total_share',
                         'float_share', 'change_reason']
                        ).sort(['ts_code', 'effective_date'])


def _num(value: Any, ndigits: int) -> float | None:
    return round(float(value), ndigits) if value is not None else None


def validate(frame: pl.DataFrame) -> dict[str, Any]:
    """送转组两版比值 vs 1+送转比例；无送转组两版偏差。"""
    usable = frame.filter(pl.col('total_div_est_all').is_not_null(),
                          pl.col('total_div_est_rec_all').is_not_null(),
                          pl.col('total_div_est_rec_all') > 0)
    split = usable.filter(pl.col('share_div').fill_null(0) > 1e-9)
    no_split = usable.filter(pl.col('share_div').fill_null(0) <= 1e-9)
    split = split.with_columns((pl.col('total_div_est_all')
                                / pl.col('total_div_est_rec_all')).alias('ratio'),
                               (1 + pl.col('share_div')).alias('expected'))
    within = split.filter(((pl.col('ratio') / pl.col('expected')) - 1).abs()
                          <= 0.05).height
    no_split = no_split.with_columns((pl.col('total_div_est_all')
                                      / pl.col('total_div_est_rec_all')).alias('ratio'))
    over = no_split.filter((pl.col('ratio') - 1).abs() > 0.01).height
    return {
        'split': {
            'rows': split.height,
            'median_ratio': _num(split['ratio'].median(), 6) if split.height else None,
            'median_expected': _num(split['expected'].median(), 6)
            if split.height else None,
            'within_5pct': within,
            'within_5pct_rate': round(within / split.height, 6) if split.height else None,
        },
        'no_split': {
            'rows': no_split.height,
            'median_abs_dev': _num(
                no_split.select((pl.col('ratio') - 1).abs().median()).item(), 8)
            if no_split.height else None,
            'over_1pct': over,
            'over_1pct_rate': round(over / no_split.height, 6)
            if no_split.height else None,
        },
    }


def _samples(decisions: list[dict[str, Any]], category: str,
             limit: int = 5) -> list[dict[str, Any]]:
    keys = ('ts_code', 'day', 'prev_share', 'cur_share', 'implied_ratio',
            'shift_10', 'share_div_before', 'change_reason', 'inferred_share_div')
    rows = [d for d in decisions if d['category'] == category][:limit]
    return [{k: r[k] for k in keys} for r in rows]


def _counts(decisions: list[dict[str, Any]]) -> dict[str, int]:
    result = {'total_candidates': len(decisions),
              'confirmed': 0, 'unexplained': 0, 'false_positive': 0}
    for d in decisions:
        result[d['category']] += 1
    return result


def backup_file(path: Path, backup_dir: Path) -> Path:
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
    parser = argparse.ArgumentParser(description='扫描并回补分红表送转字段缺失')
    parser.add_argument('--div', type=Path, default=DEFAULT_DIV)
    parser.add_argument('--shares', type=Path, default=DEFAULT_SHARES)
    parser.add_argument('--backup-dir', type=Path, default=DEFAULT_BACKUP)
    parser.add_argument('--report', type=Path, default=DEFAULT_REPORT)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args(argv)

    from derive_total_dividend import derive  # noqa: PLC0415

    frame = pl.read_parquet(args.div)
    shares = load_shares_all(args.shares)
    decisions = scan(frame, shares)
    before = validate(frame)
    filled, filled_count = apply_backfill(frame, decisions)
    after_frame = derive(filled, shares)
    est_changed = not (after_frame.select(
        ['total_div_est_all', 'total_div_est_rec_all'])
        .equals(frame.select(['total_div_est_all', 'total_div_est_rec_all'])))
    report = {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'dividend_path': str(args.div),
        'shares_path': str(args.shares),
        'counts': _counts(decisions),
        'backfilled': filled_count,
        'samples': {c: _samples(decisions, c)
                    for c in ('confirmed', 'unexplained', 'false_positive')},
        'validation': {'before': before, 'after': validate(after_frame)},
        'est_values_changed_by_backfill': est_changed,
        'note': ('派现总额只依赖 cash_div 与股本快照，回补 share_div 不改变其数值；'
                 '回补改变的是送转/无送转分组与校验结果'),
        'rows': frame.height,
        'after_rows': after_frame.height,
    }
    write_report(args.report, report)
    print(json.dumps(report['counts'], ensure_ascii=False))
    print(f'回补 {filled_count} 行；校验前 {before}；校验后 {report["validation"]["after"]}')
    if args.dry_run:
        return 0

    out = after_frame
    backup = backup_file(args.div, args.backup_dir)
    tmp = args.div.with_suffix('.parquet.tmp')
    out.write_parquet(tmp, compression='zstd', statistics=True)
    os.replace(tmp, args.div)
    print(f'已写入 {args.div}：{out.height:,} 行；备份 {backup}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
