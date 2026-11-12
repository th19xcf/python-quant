#!/usr/bin/env python3

"""数据新鲜度检查器测试：三档判定、缺失、覆盖占比与输出格式。"""

from __future__ import annotations

import json
import os
from datetime import date, datetime, time, timedelta
from pathlib import Path

import polars as pl
import pytest

from src.data.quality.freshness import (
    check_all,
    check_dataset,
    parse_config,
    summarize,
    write_outputs,
)

TODAY = date(2026, 11, 5)  # 周四


def _write_daily(root: Path, name: str, rows: list[dict[str, object]]) -> None:
    pl.DataFrame(rows).write_parquet(root / name)


def _entry(
    key: str,
    path: str,
    *,
    column: str = 'trade_date',
    unit: str = 'trading_day',
    interval: float = 1,
    warn: float = 1,
    critical: float = 3,
    coverage: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        'key': key,
        'label': key,
        'category': '测试',
        'freshness_field': column,
        'source': {'kind': 'parquet_max', 'path': path, 'column': column},
        'cadence': {
            'unit': unit,
            'expected_interval': interval,
            'warn_multiple': warn,
            'critical_multiple': critical,
            'label': '测试频率',
        },
        'coverage': coverage or {},
        'note': '',
    }


def _run(
    root: Path,
    entries: list[dict[str, object]],
    today: date = TODAY,
) -> dict[str, object]:
    config = {'defaults': {'holidays': []}, 'datasets': entries}
    return {item.key: item for item in check_all(root, config, today)}


def _coverage(covered: int, expected: int, min_ratio: float, critical_ratio: float) -> dict[str, object]:
    return {
        'count': {'kind': 'parquet_distinct', 'path': 'quotes.parquet', 'column': 'ts_code'},
        'expected': {'kind': 'fixed', 'value': expected},
        'min_ratio': min_ratio,
        'critical_ratio': critical_ratio,
    }


def test_normal_is_ok(tmp_path: Path) -> None:
    _write_daily(tmp_path, 'quotes.parquet', [
        {'ts_code': '000001.SZ', 'trade_date': TODAY},
    ])
    results = _run(tmp_path, [_entry('quotes', 'quotes.parquet')])
    assert results['quotes'].severity == 'ok'
    assert results['quotes'].state == 'fresh'
    assert results['quotes'].lag_units == 0


def test_just_overdue_warns(tmp_path: Path) -> None:
    _write_daily(tmp_path, 'quotes.parquet', [
        {'ts_code': '000001.SZ', 'trade_date': date(2026, 11, 3)},
    ])
    results = _run(tmp_path, [_entry('quotes', 'quotes.parquet')])
    assert results['quotes'].lag_units == 2
    assert results['quotes'].severity == 'warn'
    assert results['quotes'].state == 'stale'


def test_weekend_does_not_inflate_trading_days(tmp_path: Path) -> None:
    _write_daily(tmp_path, 'quotes.parquet', [
        {'ts_code': '000001.SZ', 'trade_date': date(2026, 10, 30)},  # 周五
    ])
    results = _run(
        tmp_path,
        [_entry('quotes', 'quotes.parquet')],
        today=date(2026, 11, 2),  # 周一
    )
    assert results['quotes'].lag_units == 1
    assert results['quotes'].lag_calendar_days == 3
    assert results['quotes'].severity == 'warn'


def test_severely_overdue_critical(tmp_path: Path) -> None:
    _write_daily(tmp_path, 'quotes.parquet', [
        {'ts_code': '000001.SZ', 'trade_date': date(2026, 10, 28)},
    ])
    results = _run(tmp_path, [_entry('quotes', 'quotes.parquet')])
    assert results['quotes'].overdue_ratio == 6
    assert results['quotes'].severity == 'critical'


def test_quarterly_uses_calendar_days(tmp_path: Path) -> None:
    ann_date = TODAY - timedelta(days=120)
    _write_daily(tmp_path, 'income.parquet', [
        {'ts_code': '000001.SZ', 'ann_date': ann_date},
    ])
    results = _run(tmp_path, [
        _entry(
            'income',
            'income.parquet',
            column='ann_date',
            unit='calendar_day',
            interval=90,
            warn=1,
            critical=3,
        ),
    ])
    assert results['income'].lag_units == 120
    assert results['income'].severity == 'warn'


def test_missing_data_is_critical(tmp_path: Path) -> None:
    results = _run(tmp_path, [_entry('missing', 'not_there.parquet')])
    assert results['missing'].severity == 'critical'
    assert results['missing'].state == 'missing'
    assert results['missing'].latest_date is None
    assert results['missing'].reasons


def test_coverage_insufficient_warns(tmp_path: Path) -> None:
    rows = [
        {'ts_code': f'00000{index}.SZ', 'trade_date': TODAY} for index in range(4)
    ]
    _write_daily(tmp_path, 'quotes.parquet', rows)
    results = _run(tmp_path, [
        _entry('quotes', 'quotes.parquet', coverage=_coverage(4, 10, 0.6, 0.3)),
    ])
    assert results['quotes'].severity == 'warn'
    assert results['quotes'].coverage_severity == 'warn'
    assert results['quotes'].coverage_ratio == pytest.approx(0.4)


def test_coverage_insufficient_critical(tmp_path: Path) -> None:
    rows = [
        {'ts_code': f'00000{index}.SZ', 'trade_date': TODAY} for index in range(2)
    ]
    _write_daily(tmp_path, 'quotes.parquet', rows)
    results = _run(tmp_path, [
        _entry('quotes', 'quotes.parquet', coverage=_coverage(2, 10, 0.6, 0.3)),
    ])
    assert results['quotes'].severity == 'critical'
    assert results['quotes'].coverage_severity == 'critical'


def _write_covered_codes(root: Path, covered: int) -> None:
    """写 ``covered`` 只互不相同的标的（parquet_distinct 按 ts_code 去重计数）。"""
    rows = [
        {'ts_code': f'{index:06d}.SZ', 'trade_date': TODAY} for index in range(covered)
    ]
    _write_daily(root, 'quotes.parquet', rows)


@pytest.mark.parametrize('covered, expected_severity', [
    (9499, 'warn'),   # 0.9499 < min_ratio 0.95 → warn
    (9500, 'ok'),     # 恰好 0.95：判据是 ``ratio < min_ratio``，等于即放行
    (9501, 'ok'),     # 0.9501 > 0.95
])
def test_coverage_ratio_boundary_at_min_ratio(
    tmp_path: Path, covered: int, expected_severity: str,
) -> None:
    """数值锚：min_ratio=0.95 的三点边界（90 万分之一侧差）。

    锚点来源：``covered/10000`` 手算占比，对照 ``_coverage_severity`` 的
    ``ratio < min_ratio → warn``；把 ``<`` 改成 ``<=`` 会让 0.95 变 warn，
    把 ``min_ratio`` 0.95 调成 0.90 会让 0.9499 变 ok，两种改法本用例均必红。
    """
    _write_covered_codes(tmp_path, covered)
    results = _run(tmp_path, [
        _entry('quotes', 'quotes.parquet', coverage=_coverage(covered, 10000, 0.95, 0.90)),
    ])
    result = results['quotes']
    assert result.coverage_ratio == pytest.approx(covered / 10000)
    assert result.coverage_severity == expected_severity
    assert result.severity == expected_severity


@pytest.mark.parametrize('covered, expected_severity', [
    (8999, 'critical'),   # 0.8999 < critical_ratio 0.90 → critical
    (9000, 'warn'),       # 恰好 0.90：临界档判据同样是严格小于
    (9001, 'warn'),       # 0.9001 高于 critical_ratio，但低于 min_ratio 0.95
])
def test_coverage_ratio_boundary_at_critical_ratio(
    tmp_path: Path, covered: int, expected_severity: str,
) -> None:
    """数值锚：critical_ratio=0.90 的三点边界。

    锚点来源同上一用例，另加一层「critical 优先于 min_ratio」的顺序验证：
    0.90 时不该落 critical，0.8999 时必须落 critical。
    """
    _write_covered_codes(tmp_path, covered)
    results = _run(tmp_path, [
        _entry('quotes', 'quotes.parquet', coverage=_coverage(covered, 10000, 0.95, 0.90)),
    ])
    result = results['quotes']
    assert result.coverage_ratio == pytest.approx(covered / 10000)
    assert result.coverage_severity == expected_severity
    assert result.severity == expected_severity


def test_file_mtime_detects_stale_names(tmp_path: Path) -> None:
    names = tmp_path / 'names.json'
    names.write_text(json.dumps({'000001.SZ': '平安银行'}), encoding='utf-8')
    stale = datetime.combine(TODAY - timedelta(days=2), time(12, 0)).timestamp()
    os.utime(names, (stale, stale))
    entry = _entry('names', 'names.json', unit='calendar_day', interval=1, warn=2, critical=7)
    entry['source'] = {'kind': 'file_mtime', 'path': 'names.json'}
    entry['coverage'] = {'count': {'kind': 'json_keys', 'path': 'names.json'}}
    results = _run(tmp_path, [entry])
    assert results['names'].lag_units == 2
    assert results['names'].severity == 'warn'
    assert results['names'].covered_count == 1


def test_single_dataset_api(tmp_path: Path) -> None:
    _write_daily(tmp_path, 'quotes.parquet', [
        {'ts_code': '000001.SZ', 'trade_date': TODAY},
    ])
    spec = parse_config({'datasets': [_entry('quotes', 'quotes.parquet')]})[0]
    result = check_dataset(tmp_path, spec, TODAY)
    assert result.key == 'quotes'
    assert result.severity == 'ok'


def test_outputs_are_machine_readable(tmp_path: Path) -> None:
    _write_daily(tmp_path, 'quotes.parquet', [
        {'ts_code': '000001.SZ', 'trade_date': TODAY},
    ])
    _write_daily(tmp_path, 'stale.parquet', [
        {'ts_code': '000001.SZ', 'trade_date': date(2026, 10, 28)},
    ])
    results = list(_run(tmp_path, [
        _entry('quotes', 'quotes.parquet'),
        _entry('stale', 'stale.parquet'),
    ]).values())
    json_path, csv_path = write_outputs(tmp_path / 'artifacts', results, TODAY)
    payload = json.loads(json_path.read_text(encoding='utf-8'))
    assert payload['readonly'] is True
    assert payload['summary'] == {'total': 2, 'ok': 1, 'warn': 0, 'critical': 1}
    assert payload['datasets'][0]['key'] == 'quotes'
    assert summarize(results)['critical'] == 1
    csv_bytes = csv_path.read_bytes()
    assert not csv_bytes.startswith(b'\xef\xbb\xbf')
    assert b'overdue_ratio' in csv_bytes.split(b'\n')[0]


def test_invalid_config_rejected() -> None:
    with pytest.raises(ValueError):
        parse_config({'datasets': [_entry('bad', 'x.parquet', warn=3, critical=3)]})
    with pytest.raises(ValueError):
        parse_config({'datasets': [_entry('bad', 'x.parquet', unit='unknown')]})


def test_latest_as_of_distinct_counts_only_the_latest_layer(tmp_path: Path) -> None:
    """最新层内的去重标的数，不是全历史并集。

    构造：000003.SZ 只存在于旧层（模拟已退市，历史表里留痕、最新截面里没有）。
    正确口径给 2；退回 parquet_distinct（全历史并集）会数成 3，本用例即变红。
    """
    pl.DataFrame([
        {'code': '000001.SZ', 'as_of': date(2026, 11, 3)},
        {'code': '000002.SZ', 'as_of': date(2026, 11, 3)},
        {'code': '000003.SZ', 'as_of': date(2026, 11, 3)},
        {'code': '000001.SZ', 'as_of': date(2026, 11, 4)},
        {'code': '000002.SZ', 'as_of': date(2026, 11, 4)},
    ]).write_parquet(tmp_path / 'history.parquet')
    coverage = {
        'count': {'kind': 'latest_as_of_distinct', 'path': 'history.parquet',
                  'as_of_column': 'as_of', 'column': 'code'},
        'expected': {'kind': 'fixed', 'value': 2},
        'min_ratio': 0.9,
        'critical_ratio': 0.5,
    }
    latest = _run(tmp_path, [_entry('history', 'history.parquet', column='as_of',
                                    coverage=coverage)])['history']
    assert latest.covered_count == 2
    assert latest.coverage_ratio == pytest.approx(1.0)

    coverage['count'] = {'kind': 'parquet_distinct', 'path': 'history.parquet',
                         'column': 'code'}
    union = _run(tmp_path, [_entry('history', 'history.parquet', column='as_of',
                                   coverage=coverage)])['history']
    assert union.covered_count == 3
