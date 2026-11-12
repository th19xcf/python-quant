"""两处工具写盘接入 ``write_text_atomic`` 后必须真正落到台账。

目的不是测工具业务逻辑，而是测「绕开原子写 → 埋点失效」这个盲区已经封住：
``tools/reconcile_p2_metadata.py`` 的 ``fund_meta.json`` 与
``tools/build_trading_calendar.py`` 的 ``coverage.json`` 都在
``data/local_store/**`` 正式层下，改前用 ``Path.write_text`` 直接落盘、台账无记录，
改后走 ``write_text_atomic`` 才会自动记 ``create`` / ``update``。
"""

from __future__ import annotations

import json
from datetime import date

import polars as pl
import pytest

from src.data import changelog
from tools import build_trading_calendar as calendar_tool
from tools import reconcile_p2_metadata as reconcile_tool


@pytest.fixture
def store_env(tmp_path, monkeypatch):
    """仓库根指向 tmp_path，正式库落在 ``<tmp>/data/local_store``。"""
    monkeypatch.setattr(changelog, 'repo_root', lambda: tmp_path)
    monkeypatch.setattr(
        changelog, 'changelog_path',
        lambda: tmp_path / 'logs' / 'data_changelog.csv')
    monkeypatch.setenv('QUANT_CHANGELOG_TASK', 'test_task')
    store = tmp_path / 'data' / 'local_store'
    store.mkdir(parents=True)
    return tmp_path, store, tmp_path / 'logs' / 'data_changelog.csv'


def _rows_for(ledger, rel_path: str):
    return [r for r in changelog.read_events(ledger) if r['path'] == rel_path]


def test_reconcile_commit_records_fund_meta_update(store_env):
    root, store, ledger = store_env
    factors = store / 'factors'
    factors.mkdir(parents=True)
    pl.DataFrame({'ts_code': ['000001.SZ', '000002.SZ']}).write_parquet(
        factors / 'fund_share_change.parquet')
    pl.DataFrame({'ts_code': ['000001.SZ']}).write_parquet(
        factors / 'fund_adj_factor.parquet')
    pl.DataFrame({'ts_code': ['000001.SZ']}).write_parquet(
        factors / 'fund_dividend.parquet')
    (factors / 'fund_meta.json').write_text(
        json.dumps({'share_change_rows': 0, 'share_change_codes': 0,
                    'factor_rows': 0, 'dividend_rows': 0}),
        encoding='utf-8')

    code = reconcile_tool.main([
        '--store-root', str(store),
        '--report', str(root / 'artifacts' / 'p2_reconcile' / 'report.json'),
        '--commit',
    ])
    assert code == 0

    rows = _rows_for(ledger, 'data/local_store/factors/fund_meta.json')
    assert len(rows) == 1, 'fund_meta.json 必须留下恰好一条台账记录（不多不少）'
    assert rows[0]['action'] == 'update'
    assert rows[0]['bytes'] == str((factors / 'fund_meta.json').stat().st_size)
    assert rows[0]['sha256'] == changelog.file_sha256(factors / 'fund_meta.json')

    # coverage_gaps.parquet 已按 DG-32 退役（零读侧、内容已在 report.json 的
    # coverage_gaps 段），--commit 不得再把它写回正式层，台账也不得出现该路径。
    gaps = _rows_for(ledger, 'data/local_store/meta/anomalies/coverage_gaps.parquet')
    assert gaps == [], '退役表 coverage_gaps.parquet 不得被 --commit 重新写出'


def test_trading_calendar_records_coverage_json(store_env):
    _, store, ledger = store_env
    output_dir = store / 'meta' / 'trading_calendar'
    frame = pl.DataFrame(
        {'trade_date': [date(2026, 11, 13)]}, schema={'trade_date': pl.Date})
    coverage = {'dataset': 'official_future_trading_calendar',
                'official_future_days': 1}

    wrote = calendar_tool.write_outputs(frame, coverage, output_dir, store)
    assert wrote is True

    rows = _rows_for(ledger, 'data/local_store/meta/trading_calendar/coverage.json')
    assert len(rows) == 1, 'coverage.json 必须留下恰好一条台账记录（无重复记账）'
    assert rows[0]['action'] == 'create'
    assert rows[0]['bytes'] == str((output_dir / 'coverage.json').stat().st_size)

    parquet_rows = _rows_for(
        ledger, 'data/local_store/meta/trading_calendar/official_future.parquet')
    assert len(parquet_rows) == 1 and parquet_rows[0]['action'] == 'create'
