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


def test_single_leading_code_does_not_mark_everyone_stale(tmp_path) -> None:
    """回归：基准日取众数，不能用 max(end)。

    实测曾因一只标的补到 11-06，把其余 9018 只 end=11-05 的标的全部
    判成停更（743 → 9760），导致组合回测股票池几乎被清空。
    """
    root, _ = _make_store(tmp_path, {
        '600000.SH': AS_OF,
        '600001.SH': AS_OF,
        '600002.SH': AS_OF,
        '600003.SH': date(2026, 11, 6),   # 唯一领先一天的标的
        '600068.SH': date(2021, 9, 13),   # 真正停更
    })
    frame = build_stale_status(root)
    assert frame['detected_at'][0] == AS_OF
    assert stale_codes(root) == {'600068.SH'}
    assert int(frame['is_stale'].sum()) == 1


def test_negative_lag_is_signed_not_clamped_to_zero(tmp_path) -> None:
    """反例自测：末行晚于基准日必须记 -1，不能记 0。

    旧实现 ``max(0, (snapshot - end).days)`` 会把「领先 1 天」写成 0，
    与真正「刚好同步」的标的不可区分（2026-11-13 实测 300237.SZ end=11-06、
    基准 11-05，lag 被记 0）。本用例在旧实现下必然失败，用来守住 DG-31。
    """
    root, _ = _make_store(tmp_path, {
        '600000.SH': AS_OF,
        '600001.SH': AS_OF,
        '600002.SH': AS_OF,
        '600003.SH': date(2026, 11, 6),   # 领先基准 1 天
        '600004.SH': date(2026, 11, 8),   # 领先基准 3 天
    })
    frame = build_stale_status(root, AS_OF)
    by_code = {row['ts_code']: row for row in frame.iter_rows(named=True)}
    assert by_code['600003.SH']['lag_calendar_days'] == -1
    assert by_code['600004.SH']['lag_calendar_days'] == -3
    assert by_code['600000.SH']['lag_calendar_days'] == 0
    # 领先标的不得被判成停更
    assert not by_code['600003.SH']['is_stale']
    assert stale_codes(root) == set()
    # 负 lag 若被钳成 0，上述三条会同时失效；这里再加一条分布断言防回归
    assert frame.filter(pl.col('lag_calendar_days') < 0).height == 2
