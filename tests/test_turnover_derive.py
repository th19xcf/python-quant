#!/usr/bin/env python3

"""换手率本地派生（免上游）回归测试。

背景：``turn = volume(手) × 100 ÷ float_share(股) × 100``。2026-11-06 在
99.57 万行样本上与 baostock 上游值口径对齐（中位相对误差 0、p99 = 9e-6），
故上游断供时改走本地派生。以下用例锁住「公式正确、只补缺口、不改既有行、
无股本不瞎算」四条语义。
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import polars as pl

from src.data.local_store.turnover import (
    TURN_DERIVED_SOURCE,
    derive_turnover_local,
    shares_path,
    turnover_path,
)

QUOTE_SCHEMA = {
    'date': pl.Date,
    'ts_code': pl.String,
    'volume': pl.Float64,
}


def _write_quotes(root: Path, rows: list[tuple[str, date, float | None]]) -> None:
    by_code: dict[str, list[dict[str, object]]] = {}
    for code, day, volume in rows:
        by_code.setdefault(code, []).append(
            {'date': day, 'ts_code': code, 'volume': volume})
    for code, items in by_code.items():
        market = code.split('.')[1].lower()
        sub = root / 'quotes' / 'stock' / market / f'{code}.parquet'
        sub.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(items, schema=QUOTE_SCHEMA).write_parquet(sub)


def _write_shares(root: Path, rows: list[tuple[str, date, int]]) -> None:
    path = shares_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [{'ts_code': c, 'effective_date': d, 'float_share': f} for c, d, f in rows],
        schema={'ts_code': pl.String, 'effective_date': pl.Date,
                'float_share': pl.Int64},
    ).write_parquet(path)


def _write_turnover(root: Path, rows: list[tuple[str, date, float]]) -> None:
    path = turnover_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime(2026, 11, 5, 10, 0, 0)
    pl.DataFrame(
        [{'ts_code': c, 'trade_date': d, 'turn': t, 'source': 'baostock',
          'fetched_at': stamp} for c, d, t in rows],
        schema={'ts_code': pl.String, 'trade_date': pl.Date, 'turn': pl.Float64,
                'source': pl.String, 'fetched_at': pl.Datetime('us')},
    ).write_parquet(path)


def test_formula_matches_baostock_definition(tmp_path: Path) -> None:
    """1e6 手 ÷ 100 亿股 → 1.0%（volume 单位是手）。"""
    _write_quotes(tmp_path, [('600000.SH', date(2026, 11, 4), 1_000_000.0)])
    _write_shares(tmp_path, [('600000.SH', date(2020, 1, 1), 10_000_000_000)])

    stats = derive_turnover_local(tmp_path, dry_run=False)

    assert stats['gap_rows'] == 1
    assert stats['rows_added'] == 1
    frame = pl.read_parquet(turnover_path(tmp_path))
    assert frame.height == 1
    assert abs(frame['turn'][0] - 1.0) < 1e-12
    assert frame['source'][0] == TURN_DERIVED_SOURCE


def test_existing_rows_are_never_overwritten(tmp_path: Path) -> None:
    """已有交易日必须原样保留，只用派生值填缺口。"""
    _write_quotes(tmp_path, [
        ('600000.SH', date(2026, 11, 3), 1_000_000.0),
        ('600000.SH', date(2026, 11, 4), 2_000_000.0),
    ])
    _write_shares(tmp_path, [('600000.SH', date(2020, 1, 1), 10_000_000_000)])
    _write_turnover(tmp_path, [('600000.SH', date(2026, 11, 3), 9.9)])

    stats = derive_turnover_local(tmp_path, dry_run=False)

    assert stats['gap_rows'] == 1
    frame = pl.read_parquet(turnover_path(tmp_path)).sort('trade_date')
    assert frame.height == 2
    assert frame['turn'].to_list()[0] == 9.9  # 既有行未被本地值 1.0 覆盖
    assert abs(frame['turn'].to_list()[1] - 2.0) < 1e-12


def test_skip_rows_without_share_snapshot(tmp_path: Path) -> None:
    """股本快照首条晚于行情日 → 不得瞎算，计入 no_share_rows。"""
    _write_quotes(tmp_path, [
        ('600000.SH', date(2019, 6, 3), 1_000_000.0),
        ('600000.SH', date(2026, 11, 4), 1_000_000.0),
    ])
    _write_shares(tmp_path, [('600000.SH', date(2020, 1, 1), 10_000_000_000)])

    stats = derive_turnover_local(tmp_path, dry_run=False)

    assert stats['no_share_rows'] == 1
    assert stats['derived_rows'] == 1
    frame = pl.read_parquet(turnover_path(tmp_path))
    assert frame['trade_date'].to_list() == [date(2026, 11, 4)]


def test_zero_or_null_volume_is_not_a_gap(tmp_path: Path) -> None:
    """停牌日（volume 0 / null）本就无换手率，不应被当成缺口。"""
    _write_quotes(tmp_path, [
        ('600000.SH', date(2026, 11, 3), 0.0),
        ('600000.SH', date(2026, 11, 4), None),
        ('600000.SH', date(2026, 11, 5), 1_000_000.0),
    ])
    _write_shares(tmp_path, [('600000.SH', date(2020, 1, 1), 10_000_000_000)])

    stats = derive_turnover_local(tmp_path, dry_run=False)

    assert stats['gap_rows'] == 1
    assert pl.read_parquet(turnover_path(tmp_path)).height == 1


def test_dry_run_does_not_write(tmp_path: Path) -> None:
    _write_quotes(tmp_path, [('600000.SH', date(2026, 11, 4), 1_000_000.0)])
    _write_shares(tmp_path, [('600000.SH', date(2020, 1, 1), 10_000_000_000)])

    stats = derive_turnover_local(tmp_path, dry_run=True)

    assert stats['gap_rows'] == 1
    assert stats['written'] is False
    assert not turnover_path(tmp_path).exists()


def test_date_range_and_codes_filter(tmp_path: Path) -> None:
    _write_quotes(tmp_path, [
        ('600000.SH', date(2026, 11, 3), 1_000_000.0),
        ('600000.SH', date(2026, 11, 4), 1_000_000.0),
        ('000001.SZ', date(2026, 11, 4), 1_000_000.0),
    ])
    _write_shares(tmp_path, [
        ('600000.SH', date(2020, 1, 1), 10_000_000_000),
        ('000001.SZ', date(2020, 1, 1), 10_000_000_000),
    ])

    stats = derive_turnover_local(
        tmp_path, start=date(2026, 11, 4), codes=['600000.SH'], dry_run=False)

    assert stats['gap_rows'] == 1
    frame = pl.read_parquet(turnover_path(tmp_path))
    assert frame['ts_code'].to_list() == ['600000.SH']
    assert frame['trade_date'].to_list() == [date(2026, 11, 4)]


def test_missing_shares_file_raises(tmp_path: Path) -> None:
    _write_quotes(tmp_path, [('600000.SH', date(2026, 11, 4), 1_000_000.0)])
    try:
        derive_turnover_local(tmp_path, dry_run=True)
    except FileNotFoundError as exc:
        assert 'stock_shares.parquet' in str(exc)
    else:  # pragma: no cover - 缺少股本表必须报错，不能静默跳过
        raise AssertionError('缺少股本快照时应抛 FileNotFoundError')
