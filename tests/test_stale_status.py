#!/usr/bin/env python3

"""停更/退市标的状态表：生成、判定与股票池过滤。"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import polars as pl
import pytest

from src.advice.universe import list_local_stock_codes
from src.data.quality.stale_status import (
    TDX_FREEZE_DATE,
    build_stale_status,
    is_stale_code,
    load_stale_status,
    stale_codes,
    status_path,
)

AS_OF = date(2026, 11, 5)


def _make_store(tmp_path, ends: dict[str, date]) -> tuple:
    meta = tmp_path / 'meta'
    meta.mkdir(parents=True)
    index = pl.DataFrame({
        'ts_code': list(ends),
        'data_type': ['stock'] * len(ends),
        'market': ['sh'] * len(ends),
        'rows': [100] * len(ends),
        'end': list(ends.values()),
    })
    index.write_parquet(meta / 'daily_index.parquet')
    quotes = tmp_path / 'quotes' / 'stock' / 'sh'
    quotes.mkdir(parents=True)
    for code in ends:
        (quotes / f'{code}.parquet').write_bytes(b'placeholder')
    config = SimpleNamespace(
        data=SimpleNamespace(local_store_path=str(tmp_path), local_store_enabled=True)
    )
    return tmp_path, config


def test_build_and_classify(tmp_path) -> None:
    root, _ = _make_store(tmp_path, {
        '600000.SH': AS_OF,
        '600068.SH': date(2021, 9, 13),
        '600069.SH': TDX_FREEZE_DATE,
    })
    frame = build_stale_status(root, AS_OF)
    assert frame.height == 3
    assert int(frame['is_stale'].sum()) == 2
    by_code = {row['ts_code']: row for row in frame.iter_rows(named=True)}
    assert by_code['600000.SH']['action'] == 'keep'
    assert by_code['600068.SH']['action'] == 'exclude'
    assert by_code['600069.SH']['stale_batch'] == 'tdx_2019_09_17'
    assert by_code['600068.SH']['lag_calendar_days'] == (
        AS_OF - date(2021, 9, 13)
    ).days
    assert status_path(root).is_file()
    assert load_stale_status(root) is not None


def test_stale_codes_and_query(tmp_path) -> None:
    root, _ = _make_store(tmp_path, {
        '600000.SH': AS_OF,
        '600068.SH': date(2021, 9, 13),
    })
    build_stale_status(root, AS_OF)
    assert stale_codes(root) == {'600068.SH'}
    assert is_stale_code('600068.sh', root)
    assert not is_stale_code('600000.SH', root)
    assert load_stale_status(root) is load_stale_status(root), '同 mtime 应命中缓存'


def test_missing_status_file_warns_and_fails_open(tmp_path, caplog) -> None:
    assert stale_codes(tmp_path) == set()
    assert not is_stale_code('600000.SH', tmp_path)


def test_missing_index_raises(tmp_path) -> None:
    with pytest.raises(FileNotFoundError, match='覆盖度索引'):
        build_stale_status(tmp_path, AS_OF)


def test_universe_filters_stale_by_default(tmp_path) -> None:
    root, config = _make_store(tmp_path, {
        '600000.SH': AS_OF,
        '600068.SH': date(2021, 9, 13),
    })
    build_stale_status(root, AS_OF)
    assert list_local_stock_codes(config) == ['600000.SH']
    assert list_local_stock_codes(config, exclude_stale=False) == [
        '600000.SH', '600068.SH'
    ]
