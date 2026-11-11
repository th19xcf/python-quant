#!/usr/bin/env python3
"""分红事件表送转字段缺失扫描与回补（只读股本表，不回写 MySQL）。

判定逻辑（2026-11-10 修订，旧档位判据已废弃）
============================================
对 ``share_div`` 为空或 <= 0 的事件行，取其 ``ex_date``（缺失退回
``record_date``）当天与前一天（``day - 1``）的股本快照，先做三条前置排除：

* 该行 ``no_ex_rights=True`` → ``not_split/no_ex_rights``：重整转增不向原股东
  分配、不除权，股东持股数与送转比例无关（603555/603603/600225 实测）；
* 该行 ``rights_issue_ratio`` 非空且 > 0 → ``not_split/rights_issue``：股本增加
  来自配股认购，``share_div`` 应保持 0（实测 822 行「配售A股上市」属此类）；
* 同一 ``(ts_code, ex_date)`` 已存在非空非零 ``share_div`` 行 →
  ``duplicate_event``：本行是重复记录，事件已由另一行承载
  （000002.SZ 1991-06-08、600317.SH 2014-06-16 实测）。

余下按变动原因分词：

* 含增发/回购/注销/配售/债转股/限制性股票/… 且不含送转词 → ``not_split``；
* 原因缺失，或既含送转词又含干扰词 → ``unexplained``；
* 明确含「送股/转增/送转」且无干扰词 → ``confirmed``。

**档位（0.05）只作附注，不再是判据**：重整转增常见 10 转 13.18 / 26.47 这类
精确测算比例，不落 0.05 档，旧判据把它们误判成「无法解释」。

回补原则
========
只回补 ``confirmed`` 且原值为空/0 的行，写入 ``round(隐含比例, 6)``（6 位小数，
不再四舍五入到 2 位——0.591031 若写成 0.59 会污染复权因子），并把
``share_div_source`` 置为 ``inferred_from_shares``。不覆盖任何非空非零原值，
不联网猜数，不顺手改其它行的 ``share_div_source``。

写盘只改回补行的 ``share_div`` 与 ``share_div_source``；派生列（``total_div_est_*``）
一律不动，``derive()`` 只用于校验对照。

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
import sys
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
#: 「限制性股票」为 2026-11-10 补入：东财股本结构用它标注股权激励授予，比例
#: 可能恰好落在送转档位上（实测 601677.SH 2017-03-21 为 0.0049，×10 后正好
#: 0.05），旧判据会把这类行丢进「无法解释」，若改按档位判则会回补出 0.0。
OTHER_WORDS = (
    '增发', '回购', '注销', '配售', '债转股', '股权激励', '限售', '流通',
    '高管', '首发', '超额配售', '非公开发行', '可转债', '合并', '分立',
    '行权', '期权', '上市前股本', '限制性股票',
)
#: 档位只作**附注**（``grid_gap``），不再作为判据：重整转增常见 10 转 13.18 /
#: 26.47 这类精确测算比例，不落 0.05 档（实测 603603.SH 0.3182、600225.SH
#: 2.6466）。判据改为「变动原因明确含送转、且无干扰事件」即确认，比例取股本
#: 精确比值（6 位小数），不再四舍五入到 2 位。
GRID_STEP = 0.05
RATIO_TOL = 0.003
MAX_RATIO = 2.0
SOURCE_ORIG = 'orig'
SOURCE_INFERRED = 'inferred_from_shares'
CONFIRMED = 'confirmed'
NOT_SPLIT = 'not_split'
DUPLICATE = 'duplicate_event'
UNEXPLAINED = 'unexplained'


def classify(
    prev_share: int | None,
    cur_share: int | None,
    reason: str | None,
    *,
    multiple: bool = False,
    no_ex_rights: bool = False,
    rights_issue: bool = False,
) -> dict[str, Any] | None:
    """对单个股本变化判定类别；无变化返回 None。

    判定顺序（后两条是 2026-11-10 新增的前置排除）：

    1. 股本同日多行 → ``unexplained/multiple_share_events``；
    2. ``no_ex_rights=True`` → ``not_split/no_ex_rights``：重整转增不向原股东
       分配、不除权，股东持股数与送转比例无关（603555/603603/600225 实测）；
    3. 该行是配股事件（``rights_issue_ratio`` 非空且 > 0）→
       ``not_split/rights_issue``：股本增加来自配股认购，`share_div` 应保持 0；
    4. 原因含干扰词且不含送转词 → ``not_split``；
    5. 原因缺失或含送转又含干扰词 → ``unexplained``；
    6. 原因明确含送转且无干扰 → ``confirmed``，比例取股本精确比值（6 位）。
    """
    if prev_share is None or cur_share is None or cur_share <= prev_share:
        return None
    implied = cur_share / prev_share - 1
    text = reason or ''
    if multiple:
        return {'category': UNEXPLAINED, 'code': 'multiple_share_events',
                'reason': text, 'implied': implied, 'inferred': None}
    if no_ex_rights:
        return {'category': NOT_SPLIT, 'code': 'no_ex_rights',
                'reason': text, 'implied': implied, 'inferred': None}
    if rights_issue:
        return {'category': NOT_SPLIT, 'code': 'rights_issue',
                'reason': text, 'implied': implied, 'inferred': None}
    has_split = any(w in text for w in SPLIT_WORDS)
    has_other = any(w in text for w in OTHER_WORDS)
    if has_other and not has_split:
        return {'category': NOT_SPLIT, 'code': 'non_split_change',
                'reason': text, 'implied': implied, 'inferred': None}
    if not has_split:
        return {'category': UNEXPLAINED, 'code': 'missing_change_reason',
                'reason': text, 'implied': implied, 'inferred': None}
    if has_other:
        return {'category': UNEXPLAINED, 'code': 'mixed_reason',
                'reason': text, 'implied': implied, 'inferred': None}
    if implied > MAX_RATIO:
        return {'category': UNEXPLAINED, 'code': 'ratio_out_of_range',
                'reason': text, 'implied': implied, 'inferred': None}
    return {'category': CONFIRMED, 'code': 'split_reason', 'reason': text,
            'implied': implied, 'inferred': round(implied, 6)}


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
    # 同一 (ts_code, ex_date) 已存在非空非零 share_div 的行：该事件已由另一行承载，
    # 本行是重复记录，不得再按股本反推（实测 000002.SZ 1991-06-08、600317.SH
    # 2014-06-16 均属此情形，两行现金分红不同、股本变动只有一个）。
    has_value = (frame.filter(pl.col('share_div').is_not_null()
                              & (pl.col('share_div') > 0),
                              pl.col('ex_date').is_not_null())
                 .select(['ts_code', 'ex_date'])
                 .unique()
                 .with_columns(pl.lit(True).alias('_dup_event')))
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
    merged = merged.join(has_value, left_on=['ts_code', 'ex_date'],
                         right_on=['ts_code', 'ex_date'], how='left')
    merged = merged.with_columns(pl.col('_multiple').fill_null(False),
                                 pl.col('_dup_event').fill_null(False))

    decisions: list[dict[str, Any]] = []
    for row in merged.to_dicts():
        if row['_dup_event']:
            decisions.append({
                '_row_id': row['_row_id'],
                'ts_code': row['ts_code'],
                'day': str(row['_day']),
                'prev_share': row['total_share_prev'],
                'cur_share': row['total_share_cur'],
                'implied_ratio': round(row['total_share_cur']
                                       / row['total_share_prev'] - 1, 6),
                'shift_10': round((row['total_share_cur']
                                   / row['total_share_prev'] - 1) * 10, 2),
                'share_div_before': row['share_div'],
                'change_reason': row['change_reason_cur'] or '',
                'code': 'duplicate_event',
                'category': DUPLICATE,
                'inferred_share_div': None,
                'shares_prev_date': str(row['effective_date_prev']),
                'shares_cur_date': str(row['effective_date_cur']),
            })
            continue
        rights = row.get('rights_issue_ratio')
        verdict = classify(
            row['total_share_prev'], row['total_share_cur'],
            row['change_reason_cur'],
            multiple=row['_multiple'],
            no_ex_rights=bool(row.get('no_ex_rights') or False),
            rights_issue=bool(rights is not None and float(rights) > 0),
        )
        if verdict is None:
            continue
        implied = verdict['implied']
        decisions.append({
            '_row_id': row['_row_id'],
            'ts_code': row['ts_code'],
            'day': str(row['_day']),
            'prev_share': row['total_share_prev'],
            'cur_share': row['total_share_cur'],
            'implied_ratio': round(implied, 6),
            'shift_10': round(implied * 10, 2),
            'share_div_before': row['share_div'],
            'change_reason': verdict['reason'],
            'code': verdict['code'],
            'category': verdict['category'],
            'inferred_share_div': verdict['inferred'],
            'shares_prev_date': str(row['effective_date_prev']),
            'shares_cur_date': str(row['effective_date_cur']),
        })
    return decisions


def apply_backfill(frame: pl.DataFrame,
                   decisions: list[dict[str, Any]]) -> tuple[pl.DataFrame, int]:
    """只把确认缺失写入空/零值行，返回 (新表, 实际回补行数)。

    只改回补行的 ``share_div`` 与 ``share_div_source`` 两个单元格，其余行
    （含 ``share_div_source`` 为空的 36 行）保持原样，避免顺带改写。
    """
    fixes = [d for d in decisions if d['category'] == CONFIRMED
             and d['inferred_share_div'] is not None]
    if 'share_div_source' not in frame.columns:
        frame = frame.with_columns(
            pl.lit(None, dtype=pl.String).alias('share_div_source'))
    if not fixes:
        return frame, 0
    patch = pl.DataFrame({
        '_row_id': [d['_row_id'] for d in fixes],
        '_share_div_fix': [float(d['inferred_share_div']) for d in fixes],
    })
    out = frame.with_row_index('_row_id').join(patch, on='_row_id', how='left')
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


def _diff_count(old: pl.DataFrame, new: pl.DataFrame, column: str,
                null_sentinel: Any, *, dtype: Any) -> int:
    """逐值比对单列改动行数；空值用哨兵替换，保证「null → 值」能被计入（DG-31）。"""
    left = old[column].cast(dtype).fill_null(null_sentinel)
    right = new[column].cast(dtype).fill_null(null_sentinel)
    return int((left != right).sum())


def _samples(decisions: list[dict[str, Any]], category: str,
             limit: int = 5) -> list[dict[str, Any]]:
    keys = ('ts_code', 'day', 'prev_share', 'cur_share', 'implied_ratio',
            'shift_10', 'share_div_before', 'change_reason', 'code',
            'shares_prev_date', 'shares_cur_date', 'inferred_share_div')
    rows = [d for d in decisions if d['category'] == category][:limit]
    return [{k: r[k] for k in keys} for r in rows]


def _counts(decisions: list[dict[str, Any]]) -> dict[str, int]:
    result = {'total_candidates': len(decisions),
              CONFIRMED: 0, UNEXPLAINED: 0, NOT_SPLIT: 0, DUPLICATE: 0}
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

    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    from src.data.atomic_write import write_parquet_atomic  # noqa: PLC0415

    frame = pl.read_parquet(args.div)
    shares = load_shares_all(args.shares)
    decisions = scan(frame, shares)
    before = validate(frame)
    filled, filled_count = apply_backfill(frame, decisions)
    after_frame = derive(filled, shares)
    est_changed = not (after_frame.select(
        ['total_div_est_all', 'total_div_est_rec_all'])
        .equals(frame.select(['total_div_est_all', 'total_div_est_rec_all'])))
    div_changed = _diff_count(frame, filled, 'share_div', -1e18, dtype=pl.Float64)
    src_changed = _diff_count(frame, filled, 'share_div_source', '\x00', dtype=pl.String)
    assert div_changed == filled_count, (div_changed, filled_count)
    assert src_changed == filled_count, (src_changed, filled_count)
    report = {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'dividend_path': str(args.div),
        'shares_path': str(args.shares),
        'counts': _counts(decisions),
        'backfilled': filled_count,
        'changed_cells': {'share_div': div_changed,
                          'share_div_source': src_changed},
        'confirmed_rows': [d for d in decisions if d['category'] == CONFIRMED],
        'samples': {c: _samples(decisions, c)
                    for c in (CONFIRMED, UNEXPLAINED, NOT_SPLIT, DUPLICATE)},
        'validation': {'before': before, 'after': validate(after_frame)},
        'est_values_changed_by_backfill': est_changed,
        'note': ('派现总额只依赖 cash_div 与股本快照，回补 share_div 不改变其数值；'
                 '回补改变的是送转/无送转分组与校验结果。'
                 '写盘只改回补行的 share_div 与 share_div_source，'
                 '派生列一律不动（避免顺带改写 601258.SH / 600306.SH 两行既有派生值）'),
        'rows': frame.height,
        'after_rows': after_frame.height,
    }
    write_report(args.report, report)
    print(json.dumps(report['counts'], ensure_ascii=False))
    print(f'回补 {filled_count} 行；改动单元格 {report["changed_cells"]}；'
          f'校验前 {before}；校验后 {report["validation"]["after"]}')
    if args.dry_run:
        return 0

    backup = backup_file(args.div, args.backup_dir)
    write_parquet_atomic(filled, args.div)
    print(f'已写入 {args.div}：{filled.height:,} 行；备份 {backup}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
