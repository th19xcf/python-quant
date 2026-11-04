#!/usr/bin/env python3

"""财务事实表点时查询与合并的单测。"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest

from src.data.financials import store


def _income_frame() -> pl.DataFrame:
    return pl.DataFrame({
        'ts_code': ['600519.SH', '600519.SH', '600519.SH'],
        'end_date': [date(2025, 9, 30), date(2025, 12, 31), date(2026, 3, 31)],
        'ann_date': [date(2025, 10, 30), date(2026, 3, 28), None],
        'ann_date_source': ['em_notice', 'em_notice', 'statutory_deadline'],
        'source': ['test'] * 3,
        'fetched_at': [datetime(2026, 11, 4, 10, 0)] * 3,
        'revenue': [100.0, 200.0, 300.0],
        'net_profit': [10.0, 20.0, 30.0],
    })


@pytest.fixture()
def store_root(tmp_path: Path) -> Path:
    root = tmp_path
    store.write_table(_income_frame(), 'income', root)
    return root


def test_schema_contains_required_columns() -> None:
    """四张表的必备列必须在 schema 中（采集/查询契约）。"""
    required = {
        'income': {'ts_code', 'end_date', 'ann_date', 'source', 'fetched_at'},
        'balance': {'ts_code', 'end_date', 'ann_date', 'source', 'fetched_at'},
        'cashflow': {'ts_code', 'end_date', 'ann_date', 'source', 'fetched_at'},
        'valuation': {'ts_code', 'trade_date', 'pe', 'pb', 'ps', 'dv_ratio',
                      'total_mv', 'circ_mv', 'source', 'fetched_at'},
    }
    for table, columns in required.items():
        assert columns <= set(store.SCHEMAS[table]), table


def test_pit_visibility_boundary(store_root: Path) -> None:
    """ann_date 之前不可见；当天及以后可见（end_date 不能替代 ann_date）。"""
    # end_date 已过，但公告日未到 → 不可见
    assert store.latest_announced_as_of(
        '600519.SH', '2025-09-29', store_root=store_root) is None
    assert store.latest_announced_as_of(
        '600519.SH', date(2025, 10, 29), store_root=store_root) is None

    # 公告日当天可见（Q3）
    row = store.latest_announced_as_of('600519.SH', '2025-10-30', store_root=store_root)
    assert row is not None
    assert row['end_date'] == date(2025, 9, 30)
    assert row['ann_date'] == date(2025, 10, 30)
    assert row['revenue'] == 100.0

    # 年报公告前仍是 Q3；公告当天切到年报
    before = store.latest_announced_as_of(
        '600519.SH', '2026-03-27', store_root=store_root)
    assert before is not None and before['end_date'] == date(2025, 9, 30)
    on_day = store.latest_announced_as_of(
        '600519.SH', '2026-03-28', store_root=store_root)
    assert on_day is not None
    assert on_day['end_date'] == date(2025, 12, 31)
    assert on_day['revenue'] == 200.0


def test_null_ann_date_is_invisible(store_root: Path) -> None:
    """ann_date 为空的行永远不可见（2026Q1 行不能被当成已公告）。"""
    row = store.latest_announced_as_of('600519.SH', '2027-01-01', store_root=store_root)
    assert row is not None
    assert row['end_date'] == date(2025, 12, 31)


def test_panel_latest_per_code(tmp_path: Path) -> None:
    frame = pl.DataFrame({
        'ts_code': ['600519.SH', '600519.SH', '000001.SZ'],
        'end_date': [date(2025, 9, 30), date(2025, 12, 31), date(2025, 9, 30)],
        'ann_date': [date(2025, 10, 30), date(2026, 3, 28), date(2026, 1, 20)],
        'ann_date_source': ['em_notice'] * 3,
        'source': ['test'] * 3,
        'fetched_at': [datetime(2026, 11, 4)] * 3,
        'revenue': [1.0, 2.0, 3.0],
    })
    store.write_table(frame, 'income', tmp_path)
    panel = store.latest_announced_panel(
        ['600519.SH', '000001.SZ'], '2026-02-01', store_root=tmp_path)
    assert panel.height == 2
    rows = {r['ts_code']: r for r in panel.to_dicts()}
    assert rows['600519.SH']['end_date'] == date(2025, 9, 30)
    assert rows['000001.SZ']['end_date'] == date(2025, 9, 30)


def test_merge_parts_dedupes_by_key(tmp_path: Path) -> None:
    """同报告期后采的更正公告覆盖旧版本。"""
    base = {
        'ts_code': ['600519.SH'], 'end_date': [date(2025, 12, 31)],
        'ann_date': [date(2026, 3, 28)], 'ann_date_source': ['em_notice'],
        'source': ['test'], 'fetched_at': [datetime(2026, 11, 4)], 'revenue': [200.0],
    }
    store.write_table(pl.DataFrame(base), 'income', tmp_path)
    corrected = {**base, 'ann_date': [date(2026, 8, 20)], 'revenue': [205.0]}
    part_dir = store.parts_dir('income', tmp_path)
    part_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(corrected).write_parquet(part_dir / '20251231.parquet')
    rows = store.merge_parts('income', tmp_path)
    assert rows == 2
    frame = store.read_table('income', tmp_path)
    assert frame['revenue'].sum() == 405.0
    latest = store.latest_announced_as_of('600519.SH', '2026-09-01', store_root=tmp_path)
    assert latest is not None and latest['revenue'] == 205.0


def test_latest_valuation_as_of(tmp_path: Path) -> None:
    frame = pl.DataFrame({
        'ts_code': ['600519.SH', '600519.SH', '000001.SZ'],
        'trade_date': [date(2026, 11, 3), date(2026, 11, 4), date(2026, 11, 4)],
        'pe': [20.0, 21.0, 5.0], 'pb': [8.0, 8.1, 0.6], 'ps': [7.0, 7.1, 1.0],
        'dv_ratio': [3.0, 3.0, 5.0], 'total_mv': [1e8, 1.1e8, 2e7],
        'circ_mv': [1e8, 1.1e8, 2e7], 'source': ['test'] * 3,
        'fetched_at': [datetime(2026, 11, 4)] * 3,
    })
    store.write_table(frame, 'valuation', tmp_path)
    latest = store.latest_valuation('2026-11-03', store_root=tmp_path)
    assert latest.height == 1
    assert latest.to_dicts()[0]['ts_code'] == '600519.SH'
    assert latest.to_dicts()[0]['pe'] == 20.0
    latest_all = store.latest_valuation('2026-11-04', store_root=tmp_path)
    assert latest_all.height == 2
    rows = {r['ts_code']: r for r in latest_all.to_dicts()}
    assert rows['600519.SH']['trade_date'] == date(2026, 11, 4)
    assert rows['600519.SH']['pe'] == 21.0
    assert rows['000001.SZ']['trade_date'] == date(2026, 11, 4)


def test_coverage_reports_counts(store_root: Path) -> None:
    info = store.coverage('income', store_root)
    assert info['rows'] == 3
    assert info['codes'] == 1
    assert info['ann_date_null'] == 1
