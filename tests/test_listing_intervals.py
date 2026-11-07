#!/usr/bin/env python3

"""按日在市股票池（PIT universe）的回归测试。"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import polars as pl

from src.data.quality import listing_intervals as li


def _fixture(root: Path) -> None:
    (root / 'meta' / 'universe').mkdir(parents=True)
    pl.DataFrame({
        'ts_code': ['000001.SZ', '000005.SZ', '600519.SH'],
        'data_type': ['stock', 'stock', 'stock'],
        'market': ['sz', 'sz', 'sh'],
        'rows': [100, 50, 80],
        'start': [dt.date(1991, 4, 3), dt.date(1991, 1, 2), dt.date(2001, 8, 27)],
        'end': [dt.date(2026, 11, 5), dt.date(2024, 3, 5), dt.date(2026, 11, 5)],
    }).write_parquet(root / 'meta' / 'daily_index.parquet')
    pl.DataFrame({
        'ts_code': ['000001.SZ', '000005.SZ', '600519.SH'],
        'is_stale': [False, True, False],
        'action': ['keep', 'exclude', 'keep'],
        'last_trade_date': [dt.date(2026, 11, 5), dt.date(2024, 3, 5), dt.date(2026, 11, 5)],
    }).write_parquet(root / 'meta' / 'universe' / 'stale_status.parquet')


def test_build_joins_stale_status(tmp_path: Path) -> None:
    _fixture(tmp_path)
    frame = li.build_listing_intervals(tmp_path)
    assert frame.height == 3
    row = frame.filter(pl.col('ts_code') == '000005.SZ').row(0, named=True)
    assert row['listing_status'] == 'delisted_or_stale'
    assert row['last_quote_date'] == dt.date(2024, 3, 5)


def test_listed_codes_is_point_in_time(tmp_path: Path) -> None:
    _fixture(tmp_path)
    assert li.listed_codes('2020-01-02', tmp_path) == {'000001.SZ', '000005.SZ', '600519.SH'}
    assert li.listed_codes('2025-01-02', tmp_path) == {'000001.SZ', '600519.SH'}
    assert li.listed_codes('1995-01-02', tmp_path) == {'000001.SZ', '000005.SZ'}


def test_asset_write_then_read(tmp_path: Path) -> None:
    _fixture(tmp_path)
    path = li.write_listing_intervals(tmp_path)
    assert path.is_file()
    assert pl.read_parquet(path).height == 3
    assert li.listed_codes('2025-01-02', tmp_path) == {'000001.SZ', '600519.SH'}


def test_query_works_without_generated_asset(tmp_path: Path) -> None:
    _fixture(tmp_path)
    assert not (tmp_path / li.OUTPUT_RELATIVE).exists()
    assert li.listed_codes('2020-01-02', tmp_path) == {
        '000001.SZ', '000005.SZ', '600519.SH'}
