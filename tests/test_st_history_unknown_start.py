"""上市日缺失不得用 1990 兜底：覆盖统计口径单测（离线、只写 tmp_path）。"""

from datetime import date

import polars as pl

from src.data import st_history
from src.data.local_store.store import LocalQuoteStore


def _seed_sz_raw(root):
    raw = root / 'meta' / 'st' / 'raw'
    raw.mkdir(parents=True)
    pl.DataFrame(
        [{'code': '000001.SZ', 'change_date': date(2005, 6, 1),
          'name_prev': 'ST甲', 'name_new': '甲股份'}],
        schema={'code': pl.Utf8, 'change_date': pl.Date,
                'name_prev': pl.Utf8, 'name_new': pl.Utf8},
    ).write_parquet(raw / 'sz_change_name.parquet')
    (root / 'meta' / 'st').mkdir(parents=True, exist_ok=True)


def _write_quotes(root, code, first):
    store = LocalQuoteStore(root)
    store.write_daily('stock', code, pl.DataFrame({
        'date': [first, date(2026, 11, 9)], 'close': [1.0, 2.0]}))
    store.flush_meta()


def test_missing_listing_uses_earliest_evidence_not_1990(tmp_path):
    """无行情/上市日 → 以最早名称变更日为窗口起点，绝不用 1990 兜底。"""
    root = tmp_path / 'store'
    _seed_sz_raw(root)

    summary = st_history.build_store(root)

    assert summary['unknown_start_count'] == 0
    coverage = st_history.load_coverage(root)
    assert coverage is not None and coverage.height == 1
    row = coverage.to_dicts()[0]
    assert row['coverage_start'] == date(2005, 6, 1)
    assert row['coverage_start'] != date(1990, 1, 1)


def test_known_listing_starts_at_listing_not_1990(tmp_path):
    """有上市日 → 覆盖起点等于上市日（此前是 1990 兜底）。"""
    root = tmp_path / 'store'
    _seed_sz_raw(root)
    _write_quotes(root, '000001.SZ', date(2005, 1, 4))

    summary = st_history.build_store(root)

    assert summary['unknown_start_count'] == 0
    assert summary['coverage_rows'] == 1
    coverage = st_history.load_coverage(root)
    row = coverage.to_dicts()[0]
    assert row['coverage_start'] == date(2005, 1, 4)
    assert row['coverage_start'] != date(1990, 1, 1)
    # 上市时简称为 ST 甲 → 上市日至改名日为一段 ST 区间
    intervals = st_history.load_intervals(root)
    assert intervals is not None and intervals.height == 1
    assert intervals.to_dicts()[0]['start_date'] == date(2005, 1, 4)
