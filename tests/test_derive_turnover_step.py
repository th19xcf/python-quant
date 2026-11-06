#!/usr/bin/env python3

"""每日补数收尾的换手率派生步骤（``_derive_turnover_step``）回归测试。

该步骤挂在 ``build`` 的收尾链上（异常台账刷新 / 新鲜度巡检之前），语义要求：

1. 默认真的写盘，补齐当日缺口；
2. ``--skip-derive-turnover`` 与 dry-run 都不得落盘；
3. 无缺口时不做无谓重写；
4. 任何异常（如股本快照缺失）都只告警，不得让每日补数失败。
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import polars as pl

from src.data.local_store.build import _derive_turnover_step, _parse_args
from src.data.local_store.turnover import shares_path, turnover_path

QUOTE_SCHEMA = {
    'date': pl.Date,
    'ts_code': pl.String,
    'volume': pl.Float64,
}


def _write_quotes(root: Path, rows: list[tuple[str, date, float]]) -> None:
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


def test_step_writes_missing_rows_by_default(tmp_path: Path) -> None:
    _write_quotes(tmp_path, [
        ('600000.SH', date(2026, 11, 3), 1_000_000.0),
        ('600000.SH', date(2026, 11, 4), 2_000_000.0),
    ])
    _write_shares(tmp_path, [('600000.SH', date(2020, 1, 1), 10_000_000_000)])
    _write_turnover(tmp_path, [('600000.SH', date(2026, 11, 3), 9.9)])

    result = _derive_turnover_step(tmp_path)

    assert result is not None
    assert result['written'] is True
    assert result['rows_added'] == 1
    frame = pl.read_parquet(turnover_path(tmp_path)).sort('trade_date')
    assert frame.height == 2
    assert frame['turn'].to_list()[0] == 9.9  # 既有行原样保留


def test_step_skip_flag_does_not_touch_table(tmp_path: Path) -> None:
    _write_quotes(tmp_path, [('600000.SH', date(2026, 11, 4), 1_000_000.0)])
    _write_shares(tmp_path, [('600000.SH', date(2020, 1, 1), 10_000_000_000)])

    assert _derive_turnover_step(tmp_path, skip=True) is None
    assert not turnover_path(tmp_path).exists()


def test_step_dry_run_reports_without_writing(tmp_path: Path) -> None:
    _write_quotes(tmp_path, [('600000.SH', date(2026, 11, 4), 1_000_000.0)])
    _write_shares(tmp_path, [('600000.SH', date(2020, 1, 1), 10_000_000_000)])

    result = _derive_turnover_step(tmp_path, dry_run=True)

    assert result is not None
    assert result['gap_rows'] == 1
    assert result['written'] is False
    assert not turnover_path(tmp_path).exists()


def test_step_no_gap_leaves_existing_file_alone(tmp_path: Path) -> None:
    _write_quotes(tmp_path, [('600000.SH', date(2026, 11, 4), 1_000_000.0)])
    _write_shares(tmp_path, [('600000.SH', date(2020, 1, 1), 10_000_000_000)])
    _write_turnover(tmp_path, [('600000.SH', date(2026, 11, 4), 1.0)])
    before = turnover_path(tmp_path).read_bytes()

    result = _derive_turnover_step(tmp_path)

    assert result is not None
    assert result['gap_rows'] == 0
    assert turnover_path(tmp_path).read_bytes() == before


def test_step_is_non_fatal_when_shares_snapshot_missing(tmp_path: Path) -> None:
    """股本快照缺失属可预期故障：只告警，不得中断每日补数。"""
    _write_quotes(tmp_path, [('600000.SH', date(2026, 11, 4), 1_000_000.0)])

    assert _derive_turnover_step(tmp_path) is None


def test_cli_exposes_skip_flag() -> None:
    assert _parse_args(['build']).skip_derive_turnover is False
    assert _parse_args(['build', '--skip-derive-turnover']).skip_derive_turnover is True
