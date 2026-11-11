#!/usr/bin/env python3

"""每日只读巡检门禁的回归测试（对应 P0-4）。"""

from __future__ import annotations

import datetime as dt
import importlib
import json
from pathlib import Path

import polars as pl
import pytest

gate = importlib.import_module('tools.daily_data_gate')

GOOD_FACTORS = {'passed': True, 'mismatch_codes': {'stock': 0, 'index': 0, 'fund': 0}}
GOOD_FRESHNESS = {'passed': True, 'ok': 10, 'warn': 9, 'critical': 0}
GOOD_BACKUP = {'passed': True, 'dests': {'backups': {'passed': True}}}
TODAY = dt.date(2026, 11, 7)
EXPECTED_CODES = 10000


@pytest.fixture
def mocked_checks(monkeypatch):
    state = {'factors': GOOD_FACTORS, 'freshness': GOOD_FRESHNESS,
             'backup': GOOD_BACKUP}

    monkeypatch.setattr(gate, '_check_factors', lambda root: state['factors'])
    monkeypatch.setattr(
        gate, '_check_freshness',
        lambda root, cfg, today: state['freshness'])
    monkeypatch.setattr(gate, '_check_backups', lambda dests: state['backup'])
    return state


def test_all_green_passes(mocked_checks) -> None:
    report = gate.run_gate(today=dt.date(2026, 11, 7))
    assert report['passed'] is True
    assert set(report['checks']) == {'factor_consistency', 'freshness', 'backup'}


def test_factor_mismatch_fails(mocked_checks) -> None:
    mocked_checks['factors'] = {
        'passed': False, 'mismatch_codes': {'stock': 3, 'index': 0, 'fund': 0},
    }
    assert gate.run_gate(today=dt.date(2026, 11, 7))['passed'] is False


def test_freshness_critical_fails(mocked_checks) -> None:
    mocked_checks['freshness'] = {
        'passed': False, 'ok': 7, 'warn': 7, 'critical': 5,
    }
    assert gate.run_gate(today=dt.date(2026, 11, 7))['passed'] is False


def test_missing_backup_fails(mocked_checks) -> None:
    mocked_checks['backup'] = {
        'passed': False, 'dests': {'backups': {'passed': False, 'error': 'manifest 缺失'}},
    }
    assert gate.run_gate(today=dt.date(2026, 11, 7))['passed'] is False


def test_skip_backup_check(mocked_checks) -> None:
    report = gate.run_gate(
        today=dt.date(2026, 11, 7), skip_backup_check=True)
    assert report['checks']['backup']['skipped'] is True
    assert report['passed'] is True


def test_cli_writes_report_and_exit_code(mocked_checks, tmp_path: Path) -> None:
    out_dir = tmp_path / 'gate'
    code = gate.main([
        '--today', '2026-11-07',
        '--out-dir', str(out_dir),
        '--skip-backup-check',
    ])
    assert code == 0
    report = json.loads((out_dir / 'gate_20261107.json').read_text(encoding='utf-8'))
    assert report['passed'] is True
    assert report['as_of'] == '2026-11-07'


def test_cli_returns_one_on_failure(mocked_checks, tmp_path: Path) -> None:
    mocked_checks['freshness'] = {
        'passed': False, 'ok': 7, 'warn': 7, 'critical': 5,
    }
    code = gate.main([
        '--today', '2026-11-07',
        '--out-dir', str(tmp_path / 'gate'),
        '--skip-backup-check',
    ])
    assert code == 1


# ----------------------------------------------------------------------
# 新鲜度检查「真跑」：不 mock ``_check_freshness``，只 mock 其余两项
# （因子一致性依赖真实列存、备份校验依赖外部目录，与本用例无关）。
# ----------------------------------------------------------------------

@pytest.fixture
def real_freshness(monkeypatch):
    monkeypatch.setattr(gate, '_check_factors', lambda root: GOOD_FACTORS)
    monkeypatch.setattr(gate, '_check_backups', lambda dests: GOOD_BACKUP)


def _write_store(root: Path, covered: int) -> None:
    """在 ``root`` 下写 ``covered`` 只互不相同的标的，最新交易日 = TODAY。"""
    rows = [
        {'ts_code': f'{index:06d}.SZ', 'trade_date': TODAY} for index in range(covered)
    ]
    pl.DataFrame(rows).write_parquet(root / 'quotes.parquet')


def _write_freshness_config(path: Path) -> None:
    """覆盖门槛 95%：低于 95% 直接 critical（min 与 critical 同为 0.95）。"""
    payload = {
        'defaults': {'holidays': []},
        'datasets': [{
            'key': 'quotes',
            'label': 'quotes',
            'category': '测试',
            'freshness_field': 'trade_date',
            'source': {
                'kind': 'parquet_max', 'path': 'quotes.parquet', 'column': 'trade_date',
            },
            'cadence': {
                'unit': 'trading_day', 'expected_interval': 1,
                'warn_multiple': 1, 'critical_multiple': 3, 'label': '日频',
            },
            'coverage': {
                'count': {
                    'kind': 'parquet_distinct', 'path': 'quotes.parquet',
                    'column': 'ts_code',
                },
                'expected': {'kind': 'fixed', 'value': EXPECTED_CODES},
                'min_ratio': 0.95,
                'critical_ratio': 0.95,
            },
        }],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding='utf-8')


def test_real_freshness_at_95_percent_passes(tmp_path: Path, real_freshness) -> None:
    """覆盖率恰好 95%（9500/10000）放行：门禁结论从「恒真」变成可判假。"""
    _write_store(tmp_path, 9500)
    config = tmp_path / 'freshness.json'
    _write_freshness_config(config)
    report = gate.run_gate(
        store_root=tmp_path, config_path=config, today=TODAY, skip_backup_check=True)
    freshness = report['checks']['freshness']
    assert freshness['ok'] == 1
    assert freshness['critical'] == 0
    assert freshness['passed'] is True
    assert report['passed'] is True


def test_real_freshness_below_95_percent_fails(tmp_path: Path, real_freshness) -> None:
    """覆盖率 94.99%（9499/10000）必须 critical：门禁的整体退出码随之失败。"""
    _write_store(tmp_path, 9499)
    config = tmp_path / 'freshness.json'
    _write_freshness_config(config)
    report = gate.run_gate(
        store_root=tmp_path, config_path=config, today=TODAY, skip_backup_check=True)
    freshness = report['checks']['freshness']
    assert freshness['critical'] == 1
    assert freshness['critical_labels'] == ['quotes']
    assert freshness['passed'] is False
    assert report['passed'] is False
