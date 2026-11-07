#!/usr/bin/env python3

"""退市/停更股尾部补采的涨跌幅口径回归测试。

源端 AkShare 的「涨跌幅」只保留 2 位小数，直接落盘会让 ``quality`` 的
``pct_chg_mismatch``（容差 1e-6）报 warning；本测试锁定「派生全精度 +
已落盘行可复跑归一」这两条修复。
"""

from __future__ import annotations

import datetime as dt
import importlib
import json
from pathlib import Path

import polars as pl

bf = importlib.import_module('tools.backfill_delisted_quotes')


def _local_frame() -> pl.DataFrame:
    return pl.DataFrame({
        'date': [dt.date(2026, 6, 30)],
        'ts_code': ['000001.SZ'],
        'open': [2.10], 'high': [2.12], 'low': [2.08], 'close': [2.10],
        'pre_close': [2.08], 'volume': [1000.0], 'amount': [210000.0],
        'pct_chg': [0.9615384615384616], 'change': [0.02],
        'qfq_factor': [1.0], 'hfq_factor': [2.0],
    }, schema_overrides={'date': pl.Date})


def _remote_frame() -> pl.DataFrame:
    return pl.DataFrame({
        'date': [dt.date(2026, 7, 1)],
        'open': [2.12], 'high': [2.31], 'low': [2.11], 'close': [2.30],
        'change': [0.20], 'pct_chg': [9.52],
        'volume': [1200.0], 'amount': [276000.0],
    }, schema_overrides={'date': pl.Date})


def test_new_rows_derives_full_precision_pct_chg() -> None:
    rows, first_gap = bf._new_rows('000001.SZ', _local_frame(), _remote_frame())
    expected = (2.30 / 2.10 - 1) * 100
    assert rows.height == 1
    assert abs(rows['pct_chg'][0] - expected) < 1e-12
    assert rows['source_pct_chg'][0] == 9.52
    assert abs(rows['pct_chg'][0] - 9.52) > 1e-6  # 确认不是源端舍入值
    assert abs(first_gap) < 1e-12
    assert bf._validate(rows, first_gap) == []


def test_validate_flags_gross_source_mismatch() -> None:
    rows, first_gap = bf._new_rows('000001.SZ', _local_frame(), _remote_frame())
    rows = rows.with_columns(pl.lit(50.0).alias('source_pct_chg'))
    problems = bf._validate(rows, first_gap)
    assert any('源端涨跌幅' in item for item in problems)


def test_validate_flags_broken_derived_value() -> None:
    rows, first_gap = bf._new_rows('000001.SZ', _local_frame(), _remote_frame())
    rows = rows.with_columns(pl.lit(0.0).alias('pct_chg'))
    problems = bf._validate(rows, first_gap)
    assert any('派生涨跌幅' in item for item in problems)


def _make_store(tmp_path: Path) -> tuple[Path, Path, pl.DataFrame]:
    root = tmp_path / 'store'
    staging = tmp_path / 'staging'
    path = bf._quote_path(root, '000001.SZ')
    path.parent.mkdir(parents=True, exist_ok=True)
    staging.mkdir(parents=True, exist_ok=True)
    historical = _local_frame()
    added = pl.DataFrame({
        'date': [dt.date(2026, 7, 1)],
        'ts_code': ['000001.SZ'],
        'open': [2.12], 'high': [2.31], 'low': [2.11], 'close': [2.30],
        'pre_close': [2.10], 'volume': [1200.0], 'amount': [276000.0],
        'pct_chg': [9.52], 'change': [0.20],
        'qfq_factor': [1.0], 'hfq_factor': [2.0],
    }, schema_overrides={'date': pl.Date})
    frame = pl.concat([historical, added], how='vertical_relaxed')
    frame.write_parquet(path)
    added.select('date').write_parquet(staging / '000001.SZ.parquet')
    return root, staging, frame


def test_normalize_precision_dry_run_does_not_write(tmp_path: Path) -> None:
    root, staging, original = _make_store(tmp_path)
    result = bf.normalize_committed_precision(root, staging, commit=False)
    assert result['commit'] is False
    assert result['codes'] == 1
    assert result['rows'] == 1
    assert pl.read_parquet(bf._quote_path(root, '000001.SZ')).equals(original)


def test_normalize_precision_commit_only_touches_added_row(tmp_path: Path) -> None:
    root, staging, original = _make_store(tmp_path)
    result = bf.normalize_committed_precision(root, staging, commit=True)
    assert result['rows'] == 1
    frame = pl.read_parquet(bf._quote_path(root, '000001.SZ'))
    expected = (2.30 / 2.10 - 1) * 100
    assert abs(frame.filter(pl.col('date') == dt.date(2026, 7, 1))[
        'pct_chg'][0] - expected) < 1e-12
    assert frame.filter(pl.col('date') == dt.date(2026, 6, 30))[
        'pct_chg'][0] == original['pct_chg'][0]
    # 重复执行应幂等
    again = bf.normalize_committed_precision(root, staging, commit=True)
    assert again['codes'] == 0 and again['rows'] == 0


def test_cli_normalize_precision_dry_run(tmp_path: Path, capsys) -> None:
    root, staging, _ = _make_store(tmp_path)
    exit_code = bf.main([
        '--normalize-precision', '--store-root', str(root),
        '--staging', str(staging),
    ])
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload['mode'] == 'normalize-precision'
    assert payload['commit'] is False
    assert payload['rows'] == 1
