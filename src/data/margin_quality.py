#!/usr/bin/env python3

"""个股两融明细的逐日完整性标记（数据侧，不靠人工记忆）。

问题：``margin_detail`` 按自然月分区，个别交易日源端只回部分交易所
（2026-11-20 仅沪市 2,019 行，缺深市 2,119 / 北交所 353），下游若按日
取截面做分位或排序会系统性失真。

本模块扫描 ``meta/margin_detail/margin_detail_YYYYMM.parquet``，按
交易所后缀（.SH/.SZ/.BJ）统计每日行数，与前一交易日对比：

- 某交易所前一日有、当日为 0 → 记 ``missing_exchanges``；
- 当日行数 < 前一日 80% → 记 ``row_drop``；
- 其余为完整。

产物：

- ``meta/margin_detail/completeness.parquet``
  ``trade_date / rows / sh_rows / sz_rows / bj_rows / prev_rows /
  row_ratio / is_complete / missing_exchanges / note / checked_at``
- ``meta/margin_detail/completeness.json``（摘要 + 用法说明）

用法::

    python -m src.data.margin_quality            # 重建标记
    from src.data.margin_quality import incomplete_days, filter_complete
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import polars as pl

from src.data.atomic_write import write_parquet_atomic, write_text_atomic

REL_DIR = Path('meta') / 'margin_detail'
COMPLETENESS_SCHEMA: dict[str, Any] = {
    'trade_date': pl.Date, 'rows': pl.Int64,
    'sh_rows': pl.Int64, 'sz_rows': pl.Int64, 'bj_rows': pl.Int64,
    'prev_rows': pl.Int64, 'row_ratio': pl.Float64,
    'is_complete': pl.Boolean, 'missing_exchanges': pl.Utf8,
    'note': pl.Utf8, 'checked_at': pl.Datetime,
}
MIN_ROW_RATIO = 0.8


def _dir(store_root: Path | str) -> Path:
    return Path(store_root) / REL_DIR


def completeness_path(store_root: Path | str) -> Path:
    return _dir(store_root) / 'completeness.parquet'


def _exchange(ts_code: str) -> str:
    suffix = str(ts_code).upper().rsplit('.', 1)[-1]
    return {'SH': 'sh', 'SZ': 'sz', 'BJ': 'bj'}.get(suffix, 'other')


def build_completeness(store_root: Path | str, *, write: bool = True) -> pl.DataFrame:
    """扫描全部月度分区，重建逐日完整性标记（幂等，可重跑）。"""
    root = _dir(store_root)
    files = sorted(root.glob('margin_detail_[0-9][0-9][0-9][0-9][0-9][0-9].parquet'))
    if not files:
        return pl.DataFrame(schema=COMPLETENESS_SCHEMA)
    frame = pl.concat([pl.read_parquet(path, columns=['trade_date', 'ts_code'])
                       for path in files], how='vertical_relaxed')
    frame = frame.with_columns(
        pl.col('ts_code').map_elements(_exchange, return_dtype=pl.Utf8)
        .alias('exchange'))
    daily = (frame.group_by('trade_date')
             .agg(pl.len().alias('rows'),
                  (pl.col('exchange') == 'sh').sum().alias('sh_rows'),
                  (pl.col('exchange') == 'sz').sum().alias('sz_rows'),
                  (pl.col('exchange') == 'bj').sum().alias('bj_rows'))
             .sort('trade_date'))
    daily = daily.with_columns(
        pl.col('rows').shift(1).alias('prev_rows'),
        pl.col('sh_rows').shift(1).alias('prev_sh'),
        pl.col('sz_rows').shift(1).alias('prev_sz'),
        pl.col('bj_rows').shift(1).alias('prev_bj'),
    ).with_columns(
        (pl.col('rows') / pl.col('prev_rows')).alias('row_ratio'))

    stamped = datetime.now().replace(microsecond=0)
    results: list[dict[str, Any]] = []
    for row in daily.iter_rows(named=True):
        prev = row['prev_rows']
        missing = []
        if prev is not None:
            # 只有「前一交易日该交易所有票、当日归零」才算缺失，
            # 北交所 2021 年前本就没有两融标的，不能误判。
            for key, prev_key, label in (('sh_rows', 'prev_sh', 'SH'),
                                         ('sz_rows', 'prev_sz', 'SZ'),
                                         ('bj_rows', 'prev_bj', 'BJ')):
                if row[key] == 0 and (row[prev_key] or 0) > 0:
                    missing.append(label)
        ratio = row['row_ratio']
        complete = not missing and (ratio is None or ratio >= MIN_ROW_RATIO)
        note = ''
        if missing:
            note = f'当日缺 {"/".join(missing)}（源端仅返回部分交易所）'
        elif ratio is not None and ratio < MIN_ROW_RATIO:
            note = f'行数较前一交易日下降 {(1 - ratio):.0%}'
        results.append({
            'trade_date': row['trade_date'], 'rows': row['rows'],
            'sh_rows': row['sh_rows'], 'sz_rows': row['sz_rows'],
            'bj_rows': row['bj_rows'], 'prev_rows': prev,
            'row_ratio': ratio, 'is_complete': complete,
            'missing_exchanges': ','.join(missing), 'note': note,
            'checked_at': stamped,
        })
    out = pl.DataFrame(results, schema=COMPLETENESS_SCHEMA, strict=False)
    if write:
        root.mkdir(parents=True, exist_ok=True)
        write_parquet_atomic(out, completeness_path(store_root))
        bad = out.filter(~pl.col('is_complete'))
        summary = {
            'built_at': stamped.isoformat(timespec='seconds'),
            'days': out.height,
            'incomplete_days': bad.height,
            'incomplete': bad.select([
                'trade_date', 'rows', 'sh_rows', 'sz_rows', 'bj_rows',
                'missing_exchanges', 'note']).to_dicts(),
            'rule': ('任一交易所前一日有、当日为 0，或当日行数 < 前一日 '
                     f'{MIN_ROW_RATIO:.0%}，即判不完整'),
            'usage': ('下游按日取截面前先查 completeness.parquet 的 '
                      'is_complete 列，或调用 src.data.margin_quality.'
                      'incomplete_days() 剔除；只标记、不删除已落盘的原始行'),
        }
        write_text_atomic(json.dumps(summary, ensure_ascii=False, indent=2,
                                     default=str),
                          root / 'completeness.json')
    return out


def load_completeness(store_root: Path | str) -> pl.DataFrame | None:
    path = completeness_path(store_root)
    if not path.is_file():
        return None
    return pl.read_parquet(path)


def incomplete_days(store_root: Path | str) -> set[date]:
    frame = load_completeness(store_root)
    if frame is None or frame.height == 0:
        return set()
    return set(frame.filter(~pl.col('is_complete'))['trade_date'].to_list())


def is_complete(store_root: Path | str, day: date) -> bool:
    """未知日期（无记录）按不完整处理，避免把没校验过的日子当成好数据。"""
    frame = load_completeness(store_root)
    if frame is None:
        return False
    hit = frame.filter(pl.col('trade_date') == day)
    return bool(hit.height and hit['is_complete'][0])


def filter_complete(frame: pl.DataFrame, store_root: Path | str,
                    column: str = 'trade_date') -> pl.DataFrame:
    """按日截面取数后调用：剔除完整性标记为 False 的交易日。"""
    bad = incomplete_days(store_root)
    if not bad:
        return frame
    return frame.filter(~pl.col(column).is_in(list(bad)))


def load_day(store_root: Path | str, day: date,
             columns: Iterable[str] | None = None) -> pl.DataFrame:
    """取单日截面；该日被标记不完整时返回空表（下游应据此跳过）。"""
    month = f'{day:%Y%m}'
    path = _dir(store_root) / f'margin_detail_{month}.parquet'
    if not path.is_file():
        return pl.DataFrame()
    frame = pl.read_parquet(path, columns=list(columns) if columns else None)
    if not is_complete(store_root, day):
        return frame.clear()
    return frame.filter(pl.col('trade_date') == day)


if __name__ == '__main__':
    import sys

    root = Path(sys.argv[1]) if len(sys.argv) > 1 else \
        Path(__file__).resolve().parents[2] / 'data' / 'local_store'
    result = build_completeness(root)
    bad = result.filter(~pl.col('is_complete'))
    print(f'已重建 {result.height} 个交易日的完整性标记，'
          f'不完整 {bad.height} 天：{bad["trade_date"].to_list()}')
