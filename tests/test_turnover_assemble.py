#!/usr/bin/env python3

"""换手率事实表组装（upsert）回归测试。

背景：``_assemble`` 早期实现是按 ``ts_code`` 整段替换目标表，与
``_build_tasks`` 的增量窗口（``last_date + 1`` 起）组合后，会把该标的的全部
历史行删掉——2026-11-06 增量重建把 1,780 万行削成 121 万行即是此因。
下列用例锁住「历史必须保留、同交易日以新数据为准」这两条语义。
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import polars as pl

from src.data.local_store.turnover import _assemble

SCHEMA = {
    'ts_code': pl.String,
    'trade_date': pl.Date,
    'turn': pl.Float64,
    'source': pl.String,
    'fetched_at': pl.Datetime('us'),
}


def _frame(rows: list[tuple[str, date, float]]) -> pl.DataFrame:
    stamp = datetime(2026, 11, 6, 10, 0, 0)
    return pl.DataFrame(
        [{'ts_code': code, 'trade_date': day, 'turn': turn,
          'source': 'test', 'fetched_at': stamp}
         for code, day, turn in rows],
        schema=SCHEMA,
    )


def test_incremental_part_keeps_history(tmp_path: Path) -> None:
    target = tmp_path / 'stock_turnover.parquet'
    _frame([
        ('600000.SH', date(2026, 11, 2), 1.0),
        ('600000.SH', date(2026, 11, 3), 1.1),
        ('000001.SZ', date(2026, 11, 3), 2.0),
    ]).write_parquet(target)
    part = tmp_path / 'part_00000.parquet'
    _frame([('600000.SH', date(2026, 11, 4), 1.2)]).write_parquet(part)

    rows = _assemble([part], target, {'600000.SH'})

    frame = pl.read_parquet(target)
    assert rows == 4
    assert frame.height == 4
    # 增量分块只带 1 天，但该标的既有 2 天历史必须还在
    kept = frame.filter(pl.col('ts_code') == '600000.SH').sort('trade_date')
    assert kept['trade_date'].to_list() == [
        date(2026, 11, 2), date(2026, 11, 3), date(2026, 11, 4)]
    assert frame.filter(pl.col('ts_code') == '000001.SZ').height == 1


def test_same_trade_date_prefers_new_value(tmp_path: Path) -> None:
    target = tmp_path / 'stock_turnover.parquet'
    _frame([('600000.SH', date(2026, 11, 3), 1.0)]).write_parquet(target)
    part = tmp_path / 'part_00000.parquet'
    _frame([('600000.SH', date(2026, 11, 3), 9.9)]).write_parquet(part)

    assert _assemble([part], target, {'600000.SH'}) == 1
    frame = pl.read_parquet(target)
    assert frame.height == 1
    assert frame['turn'].to_list() == [9.9]


def test_first_build_without_target(tmp_path: Path) -> None:
    target = tmp_path / 'stock_turnover.parquet'
    part = tmp_path / 'part_00000.parquet'
    _frame([('600000.SH', date(2026, 11, 3), 1.0),
            ('600000.SH', date(2026, 11, 4), 1.1)]).write_parquet(part)

    assert _assemble([part], target, set()) == 2
    assert pl.read_parquet(target).height == 2


def test_empty_parts_is_noop(tmp_path: Path) -> None:
    assert _assemble([], tmp_path / 'stock_turnover.parquet', set()) is None
