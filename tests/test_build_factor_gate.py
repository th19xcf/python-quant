#!/usr/bin/env python3

"""``build`` 前置因子快照门禁与收尾一致性门禁的回归测试。

对应 P0-2：权威因子快照缺失时不得写入行情（否则 ``_factor_arrays`` 会把
qfq/hfq 填成 1.0，把正确复权价静默改成未复权价）。
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import polars as pl

from src.data.local_store.build import _factor_snapshot_gate, _parse_args
from src.data.local_store.store import LocalFactorStore


def _snapshot_frame(day: dt.date) -> pl.DataFrame:
    return pl.DataFrame({
        'ts_code': ['000001.SZ'],
        'trade_date': [day],
        'qfq_factor': [1.0],
        'hfq_factor': [1.0],
    })


def _write_all_snapshots(root: Path, fs: LocalFactorStore, day: dt.date) -> None:
    (root / 'factors').mkdir(parents=True, exist_ok=True)
    _snapshot_frame(day).write_parquet(fs.factor_path)
    _snapshot_frame(day).write_parquet(fs.fund_factor_path)
    pl.DataFrame({'ts_code': ['000001.SZ'], 'ex_date': [day]}).write_parquet(
        fs.dividend_path)


def _write_daily_index(root: Path, end: dt.date) -> None:
    (root / 'meta').mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        'data_type': ['stock'],
        'end': [end],
    }).write_parquet(root / 'meta' / 'daily_index.parquet')


def test_parse_args_new_flags_default_off() -> None:
    args = _parse_args(['build'])
    assert args.allow_missing_factors is False
    assert args.skip_factor_verify is False
    assert _parse_args(['build', '--allow-missing-factors']).allow_missing_factors
    assert _parse_args(['build', '--skip-factor-verify']).skip_factor_verify


def test_gate_rejects_when_snapshots_missing(tmp_path: Path) -> None:
    fs = LocalFactorStore(tmp_path)
    assert _factor_snapshot_gate(fs, tmp_path) == 1


def test_gate_allows_explicit_escape_hatch(tmp_path: Path) -> None:
    fs = LocalFactorStore(tmp_path)
    assert _factor_snapshot_gate(fs, tmp_path, allow_missing=True) == 0


def test_gate_accepts_complete_snapshots(tmp_path: Path) -> None:
    fs = LocalFactorStore(tmp_path)
    day = dt.date(2026, 11, 5)
    _write_all_snapshots(tmp_path, fs, day)
    _write_daily_index(tmp_path, day)
    assert _factor_snapshot_gate(fs, tmp_path) == 0


def test_gate_warns_but_passes_when_snapshot_lags(tmp_path: Path) -> None:
    fs = LocalFactorStore(tmp_path)
    _write_all_snapshots(tmp_path, fs, dt.date(2026, 11, 1))
    _write_daily_index(tmp_path, dt.date(2026, 11, 5))
    assert _factor_snapshot_gate(fs, tmp_path) == 0


def test_gate_requires_dividend_snapshot_too(tmp_path: Path) -> None:
    fs = LocalFactorStore(tmp_path)
    _write_all_snapshots(tmp_path, fs, dt.date(2026, 11, 5))
    fs.dividend_path.unlink()
    assert _factor_snapshot_gate(fs, tmp_path) == 1
