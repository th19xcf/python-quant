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


def test_merge_parts_keeps_non_null_when_new_row_is_null(tmp_path: Path) -> None:
    """G-080：新分片该列为空时不得把历史非空值降级为 null。"""
    base = {
        'ts_code': ['600519.SH'], 'end_date': [date(2025, 12, 31)],
        'ann_date': [date(2026, 3, 28)], 'ann_date_source': ['em_notice'],
        'source': ['em'], 'fetched_at': [datetime(2026, 11, 4)],
        'revenue': [200.0], 'net_profit': [20.0],
    }
    store.write_table(pl.DataFrame(base), 'income', tmp_path)
    # 后采分片只有 net_profit，revenue 为空
    later = {**base, 'source': ['sina'], 'revenue': [None], 'net_profit': [25.0]}
    part_dir = store.parts_dir('income', tmp_path)
    part_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(later).write_parquet(part_dir / '20251231_late.parquet')

    # 反例自测：旧实现（整行 last-wins）确实会把 revenue 抹成 null，
    # 说明下面的断言不是空过。
    legacy = pl.concat([
        pl.read_parquet(store.table_path('income', tmp_path)),
        pl.read_parquet(part_dir / '20251231_late.parquet'),
    ], how='diagonal_relaxed').unique(subset=['ts_code', 'end_date'], keep='last')
    assert legacy['revenue'][0] is None
    assert legacy['net_profit'][0] == 25.0

    rows = store.merge_parts('income', tmp_path)
    assert rows == 1
    frame = store.read_table('income', tmp_path)
    assert frame.height == 1
    assert frame['revenue'][0] == 200.0       # 旧非空值保留
    assert frame['net_profit'][0] == 25.0     # 新非空值覆盖
    assert frame['source'][0] == 'sina'


def test_merge_parts_field_level_source_follows_value(tmp_path: Path) -> None:
    """G-080：逐字段来源串随该字段的来源行走，缺段不猜、值的掉档不发生。"""
    base = pl.DataFrame({
        'ts_code': ['600519.SH'], 'trade_date': [date(2026, 11, 4)],
        'close': [1500.0], 'pe': [20.0], 'pb': [8.0], 'dv_ratio': [3.0],
        'source': ['pe=em|pb=em|dv=daily_basic'],
        'fetched_at': [datetime(2026, 11, 4)],
    })
    store.write_table(base, 'valuation', tmp_path)
    part_dir = store.parts_dir('valuation', tmp_path)
    part_dir.mkdir(parents=True, exist_ok=True)
    # 新分片：只有 pb 更新且其 source 只带 pb 段；pe 为空
    pl.DataFrame({
        'ts_code': ['600519.SH'], 'trade_date': [date(2026, 11, 4)],
        'pb': [8.6], 'source': ['pb=em_v2'],
        'fetched_at': [datetime(2026, 11, 5)],
    }).write_parquet(part_dir / 'batch_late.parquet')

    # 反例自测：旧实现会把 pe/dv_ratio 抹成 null、source 整串换成 pb=em_v2
    legacy = pl.concat([
        pl.read_parquet(store.table_path('valuation', tmp_path)),
        pl.read_parquet(part_dir / 'batch_late.parquet'),
    ], how='diagonal_relaxed').unique(
        subset=['ts_code', 'trade_date'], keep='last')
    assert legacy['pe'][0] is None and legacy['dv_ratio'][0] is None
    assert legacy['source'][0] == 'pb=em_v2'

    assert store.merge_parts('valuation', tmp_path) == 1
    frame = store.read_table('valuation', tmp_path)
    row = frame.to_dicts()[0]
    assert row['pe'] == 20.0 and row['dv_ratio'] == 3.0 and row['pb'] == 8.6
    # 段级跟随：pe/dv 保持原来源，pb 换成新来源，缺段的 close 不凭空造段
    assert row['source'] == 'pe=em|pb=em_v2|dv=daily_basic'


def test_merge_parts_source_without_segments_falls_back(tmp_path: Path) -> None:
    """G-080：整串无 ``=`` 段（三表 ``local:…`` 形态）退化为「最后非空整串」。"""
    base = {
        'ts_code': ['600519.SH'], 'end_date': [date(2025, 12, 31)],
        'ann_date': [date(2026, 3, 28)], 'ann_date_source': ['em_notice'],
        'source': ['local:em_income'], 'fetched_at': [datetime(2026, 11, 4)],
        'revenue': [200.0],
    }
    store.write_table(pl.DataFrame(base), 'income', tmp_path)
    part_dir = store.parts_dir('income', tmp_path)
    part_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({**base, 'source': ['local:em_income_v2'],
                  'revenue': [210.0]}).write_parquet(part_dir / '20251231.parquet')

    assert store.merge_parts('income', tmp_path) == 1
    row = store.read_table('income', tmp_path).to_dicts()[0]
    assert row['revenue'] == 210.0
    assert row['source'] == 'local:em_income_v2'


def test_coverage_reports_counts(store_root: Path) -> None:
    info = store.coverage('income', store_root)
    assert info['rows'] == 3
    assert info['codes'] == 1
    assert info['ann_date_null'] == 1
