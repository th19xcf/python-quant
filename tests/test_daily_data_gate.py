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
GOOD_SUMMARY = {'passed': True, 'tables': 13, 'errors': [], 'warnings': []}
GOOD_AUDIT = {'passed': True, 'version': 'dv-20261113-000000000000',
              'counts': {'ok': 4, 'stale': 0, 'unstamped': 0, 'missing': 0},
              'missing': [], 'stale': []}
TODAY = dt.date(2026, 11, 7)
EXPECTED_CODES = 10000


@pytest.fixture
def mocked_checks(monkeypatch):
    state = {'factors': GOOD_FACTORS, 'freshness': GOOD_FRESHNESS,
             'backup': GOOD_BACKUP, 'summary': GOOD_SUMMARY,
             'audit': GOOD_AUDIT}

    monkeypatch.setattr(gate, '_check_factors', lambda root: state['factors'])
    monkeypatch.setattr(
        gate, '_check_freshness',
        lambda root, cfg, today: state['freshness'])
    monkeypatch.setattr(gate, '_check_freshness_summary', lambda root: state['summary'])
    monkeypatch.setattr(gate, '_check_version_audit',
                        lambda root, **kwargs: state['audit'])
    monkeypatch.setattr(gate, '_check_backups', lambda dests: state['backup'])
    return state


def test_all_green_passes(mocked_checks) -> None:
    report = gate.run_gate(today=dt.date(2026, 11, 7))
    assert report['passed'] is True
    assert set(report['checks']) == {
        'factor_consistency', 'freshness', 'freshness_summary',
        'version_audit', 'backup'}


def test_freshness_summary_error_fails(mocked_checks) -> None:
    """13 行体检出 error 即失败（与 ``freshness_check --summary`` 同判据）。"""
    mocked_checks['summary'] = {
        'passed': False, 'tables': 13, 'errors': ['meta/financials/valuation_daily.parquet'],
        'warnings': [],
    }
    assert gate.run_gate(today=dt.date(2026, 11, 7))['passed'] is False


def test_version_audit_missing_table_fails(mocked_checks) -> None:
    """派生表文件缺失即失败；``stale`` 只记录、不阻断。"""
    mocked_checks['audit'] = {
        'passed': False, 'version': 'dv-20261113-000000000000',
        'counts': {'ok': 3, 'stale': 0, 'unstamped': 0, 'missing': 1},
        'missing': ['meta/state/state_vector_daily.parquet'], 'stale': [],
    }
    assert gate.run_gate(today=dt.date(2026, 11, 7))['passed'] is False

    mocked_checks['audit'] = {
        'passed': True, 'version': 'dv-20261113-000000000000',
        'counts': {'ok': 3, 'stale': 1, 'unstamped': 0, 'missing': 0},
        'missing': [], 'stale': ['meta/state/state_vector_daily.parquet'],
    }
    assert gate.run_gate(today=dt.date(2026, 11, 7))['passed'] is True


def test_version_audit_reads_real_module(monkeypatch) -> None:
    """真跑一次封装：把 ``audit_derived`` 换成三态，核验分档映射。"""
    from src.data import data_version as dv

    monkeypatch.setattr(dv, 'audit_derived', lambda root: {
        'current': {'version': 'dv-20261113-abcdefabcdef'},
        'rows': [
            {'table': 'a.parquet', 'status': 'ok'},
            {'table': 'b.parquet', 'status': 'stale'},
            {'table': 'c.parquet', 'status': 'missing'},
        ],
        'counts': {'ok': 1, 'stale': 1, 'unstamped': 0, 'missing': 1},
    })
    result = gate._check_version_audit(Path('.'), record=False, today=TODAY)
    assert result['passed'] is False
    assert result['recorded_version'] is None
    assert result['missing'] == ['c.parquet']
    assert result['stale'] == ['b.parquet']
    assert result['counts']['missing'] == 1


def _write_gate_report(out_dir: Path, day: str, stale: list[str]) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f'gate_{day}.json'
    path.write_text(json.dumps({'checks': {'version_audit': {'stale': stale}}}),
                    encoding='utf-8')
    return path


def test_persistent_stale_hits_two_consecutive_runs(tmp_path: Path) -> None:
    """同一派生表在上一份报告里也 stale → 报「连续两日未消」。"""
    _write_gate_report(tmp_path, '20261113', ['a.parquet'])
    _write_gate_report(tmp_path, '20261114', ['a.parquet', 'b.parquet'])
    result = gate._persistent_stale(['b.parquet', 'c.parquet'],
                                    dt.date(2026, 11, 15), tmp_path)
    assert result['compared_with'] == 'gate_20261114.json'
    assert result['tables'] == ['b.parquet']


def test_persistent_stale_ignores_same_day_report(tmp_path: Path) -> None:
    """当天的报告不算「上一日」，缺上一日时只报空。"""
    _write_gate_report(tmp_path, '20261107', ['a.parquet'])
    result = gate._persistent_stale(['a.parquet'], TODAY, tmp_path)
    assert result['compared_with'] is None
    assert result['tables'] == []


def test_persistent_stale_no_previous_dir(tmp_path: Path) -> None:
    result = gate._persistent_stale(['a.parquet'], TODAY, tmp_path / 'missing')
    assert result == {'compared_with': None, 'tables': []}


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
    # 体检 13 行与版本审计都依赖真实 ``data/local_store``，合成仓库下按好结果短路
    monkeypatch.setattr(gate, '_check_freshness_summary', lambda root: GOOD_SUMMARY)
    monkeypatch.setattr(gate, '_check_version_audit',
                        lambda root, **kwargs: GOOD_AUDIT)


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
