#!/usr/bin/env python3

"""新鲜度巡检的覆盖声明校验：补几条近期记录不能消除区间断更。"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import polars as pl

from src.data.quality.freshness import (
    Cadence,
    DatasetSpec,
    check_dataset,
    parse_config,
)

TODAY = date(2026, 11, 6)
DECLARATION = {
    'path': 'meta/events/coverage.json',
    'end_field': 'end',
    'complete_field': 'complete',
    'max_lag_days': 30,
}


def _make_store(tmp_path: Path) -> Path:
    events = tmp_path / 'meta' / 'events'
    events.mkdir(parents=True)
    pl.DataFrame({
        'publish_time': [datetime(2026, 11, 1, 10, 30)],
        'ts_code': ['600000.SH'],
    }).write_parquet(events / 'events.parquet')
    return tmp_path


def _write_coverage(tmp_path: Path, payload: dict) -> None:
    (tmp_path / 'meta' / 'events' / 'coverage.json').write_text(
        json.dumps(payload, ensure_ascii=False), encoding='utf-8'
    )


def _spec(declaration: dict) -> DatasetSpec:
    return DatasetSpec(
        key='events',
        label='公告事件',
        category='事件',
        freshness_field='publish_time',
        source={
            'kind': 'parquet_max',
            'path': 'meta/events/events.parquet',
            'column': 'publish_time',
        },
        cadence=Cadence('calendar_day', 90.0, 1.0, 3.0, '不定期'),
        coverage={},
        note='',
        declaration=declaration,
    )


def test_config_declaration_is_parsed() -> None:
    specs = parse_config({
        'defaults': {'holidays': []},
        'datasets': [{
            'key': 'events', 'label': 'events', 'category': '事件',
            'freshness_field': 'publish_time',
            'source': {'kind': 'file_mtime', 'path': 'x'},
            'cadence': {'unit': 'calendar_day', 'expected_interval': 90,
                        'warn_multiple': 1, 'critical_multiple': 3},
            'declaration': DECLARATION,
        }],
    })
    assert specs[0].declaration == DECLARATION


def test_without_declaration_recent_records_are_fresh(tmp_path) -> None:
    root = _make_store(tmp_path)
    result = check_dataset(root, _spec({}), TODAY, set())
    assert result.severity == 'ok'
    assert result.state == 'fresh'


def test_lapsed_coverage_declaration_is_critical(tmp_path) -> None:
    root = _make_store(tmp_path)
    _write_coverage(tmp_path, {
        'source': 'cninfo_2025', 'start': '2025-01-01',
        'end': '2025-12-31', 'rows': 22101, 'complete': True,
    })
    result = check_dataset(root, _spec(DECLARATION), TODAY, set())
    assert result.severity == 'critical'
    assert result.state == 'coverage_lapsed'
    assert any('覆盖声明止于 2025-12-31' in reason for reason in result.reasons)


def test_complete_false_is_critical(tmp_path) -> None:
    root = _make_store(tmp_path)
    _write_coverage(tmp_path, {
        'start': '2025-01-01', 'end': '2026-11-05', 'complete': False,
    })
    result = check_dataset(root, _spec(DECLARATION), TODAY, set())
    assert result.severity == 'critical'
    assert any('complete=false' in reason for reason in result.reasons)


def test_missing_declaration_file_is_critical(tmp_path) -> None:
    root = _make_store(tmp_path)
    result = check_dataset(root, _spec(DECLARATION), TODAY, set())
    assert result.severity == 'critical'
    assert any('覆盖声明文件缺失' in reason for reason in result.reasons)


def test_current_declaration_passes(tmp_path) -> None:
    root = _make_store(tmp_path)
    _write_coverage(tmp_path, {
        'start': '2025-01-01', 'end': '2026-11-05', 'complete': True,
    })
    result = check_dataset(root, _spec(DECLARATION), TODAY, set())
    assert result.severity == 'ok'
    assert result.state == 'fresh'
