#!/usr/bin/env python3

"""P2：外部依赖数据契约占位的只读巡检回归测试。"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl

from src.data.quality.extended_contracts import (
    CONTRACTS,
    check_contracts,
    main,
    summarize,
)

AS_OF = date(2026, 11, 6)


def _suspension_path(root: Path) -> Path:
    return root / 'meta' / 'suspension' / 'suspension_daily.parquet'


def test_empty_root_reports_all_missing(tmp_path: Path) -> None:
    results = check_contracts(tmp_path, as_of=AS_OF)
    assert len(results) == len(CONTRACTS)
    assert {item['status'] for item in results} == {'missing'}
    assert summarize(results)['missing'] == len(CONTRACTS)
    for item in results:
        assert item['blockers']
        assert item['impact_if_missing']
        assert item['exists'] is False


def test_file_without_required_columns_is_partial(tmp_path: Path) -> None:
    path = _suspension_path(tmp_path)
    path.parent.mkdir(parents=True)
    pl.DataFrame({'ts_code': ['600000.SH'],
                  'trade_date': [date(2026, 11, 5)]}).write_parquet(path)

    results = {item['key']: item for item in check_contracts(tmp_path, as_of=AS_OF)}
    assert results['suspension']['status'] == 'partial'
    assert any('缺少必需列' in b for b in results['suspension']['blockers'])


def test_ready_requires_complete_declaration(tmp_path: Path) -> None:
    path = _suspension_path(tmp_path)
    path.parent.mkdir(parents=True)
    pl.DataFrame({
        'ts_code': ['600000.SH'],
        'trade_date': [date(2026, 11, 5)],
        'is_suspended': [False],
    }).write_parquet(path)

    before = {item['key']: item for item in check_contracts(tmp_path, as_of=AS_OF)}
    assert before['suspension']['status'] == 'partial'
    assert any('覆盖声明' in b for b in before['suspension']['blockers'])

    coverage = path.parent / 'coverage.json'
    coverage.write_text(json.dumps({'start': '2026-01-01',
                                    'end': '2026-11-05',
                                    'complete': False}), encoding='utf-8')
    assert {item['key']: item for item in check_contracts(tmp_path, as_of=AS_OF)}[
        'suspension']['status'] == 'partial'

    coverage.write_text(json.dumps({'start': '2026-01-01',
                                    'end': '2026-11-05',
                                    'complete': True}), encoding='utf-8')
    after = {item['key']: item for item in check_contracts(tmp_path, as_of=AS_OF)}
    assert after['suspension']['status'] == 'ready'
    assert after['suspension']['rows'] == 1
    assert after['suspension']['blockers'] == []


def test_fail_on_gap_exit_code(tmp_path: Path) -> None:
    assert main(['--root', str(tmp_path), '--fail-on-gap', '--json']) == 1
    # 不加门禁时是只读巡检，不抛错也不写 data/**
    assert main(['--root', str(tmp_path), '--json']) == 0
    assert not (tmp_path / 'meta').exists()
