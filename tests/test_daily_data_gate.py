#!/usr/bin/env python3

"""每日只读巡检门禁的回归测试（对应 P0-4）。"""

from __future__ import annotations

import datetime as dt
import importlib
import json
from pathlib import Path

import pytest

gate = importlib.import_module('tools.daily_data_gate')

GOOD_FACTORS = {'passed': True, 'mismatch_codes': {'stock': 0, 'index': 0, 'fund': 0}}
GOOD_FRESHNESS = {'passed': True, 'ok': 10, 'warn': 9, 'critical': 0}
GOOD_BACKUP = {'passed': True, 'dests': {'backups': {'passed': True}}}


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
