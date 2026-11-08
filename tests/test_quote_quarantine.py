#!/usr/bin/env python3

"""行情行级隔离台账的读侧过滤回归测试。"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import polars as pl

from src.data.quality import quarantine


def _frame() -> pl.DataFrame:
    return pl.DataFrame({
        'date': [dt.date(2024, 1, 2), dt.date(2024, 1, 3), dt.date(2024, 1, 4)],
        'close': [10.0, 10.5, 11.0],
    })


def test_drop_quarantined_removes_only_listed_dates(tmp_path: Path) -> None:
    (tmp_path / 'meta' / 'anomalies').mkdir(parents=True)
    pl.DataFrame({
        'ts_code': ['000001.SZ', '000001.SZ', '600000.SH'],
        'trade_date': ['2024-01-02', '2024-01-04', '2024-01-03'],
        'rule_ids': ['volume_invalid'] * 3,
    }).write_parquet(tmp_path / 'meta' / 'anomalies' / 'quarantine_rows.parquet')

    result = quarantine.drop_quarantined(_frame(), '000001.SZ', root=tmp_path)
    assert result['date'].to_list() == [dt.date(2024, 1, 3)]
    assert quarantine.drop_quarantined(
        _frame(), '600000.SH', root=tmp_path)['date'].to_list() == [
            dt.date(2024, 1, 2), dt.date(2024, 1, 4)]
    assert quarantine.quarantine_stats(root=tmp_path) == {
        'available': True, 'rows': 3, 'codes': 2,
        'path': str(tmp_path / 'meta' / 'anomalies' / 'quarantine_rows.parquet'),
    }


def test_missing_ledger_fails_open(tmp_path: Path) -> None:
    frame = _frame()
    assert quarantine.drop_quarantined(frame, '000001.SZ', root=tmp_path) is frame
    assert quarantine.quarantine_stats(root=tmp_path)['available'] is False
    assert quarantine.placeholder_codes(root=tmp_path) == frozenset()


def test_placeholder_codes_reads_anomaly_list(tmp_path: Path) -> None:
    (tmp_path / 'meta' / 'anomalies').mkdir(parents=True)
    (tmp_path / 'meta' / 'anomalies' / 'placeholder_quote_codes.csv').write_text(
        'ts_code,start,end\n160136.SZ,2020-01-01,2020-12-31\n'
        '160137.SZ,2021-01-01,2021-12-31\n', encoding='utf-8')
    assert quarantine.placeholder_codes(root=tmp_path) == frozenset(
        {'160136.SZ', '160137.SZ'})
