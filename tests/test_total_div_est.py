"""total_div_est 两版派生单测：ex_date 与 record_date 口径、缺失与不回填。"""

import sys
from datetime import date
from pathlib import Path

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / 'tools'))

from derive_total_dividend import coverage, derive  # noqa: E402


def _dividends() -> pl.DataFrame:
    return pl.DataFrame({
        'ts_code': ['600000.SH', '600000.SH', '600000.SH', '600000.SH',
                    '600000.SH', '600001.SH', '600002.SH', '600003.SH'],
        'ex_date': [date(2024, 5, 21), date(2025, 5, 21), None,
                    date(2024, 5, 21), date(2024, 7, 1),
                    date(2024, 6, 1), date(2024, 6, 1), date(2024, 6, 10)],
        'record_date': [date(2024, 5, 20), date(2025, 5, 20), None,
                        date(2024, 5, 20), None,
                        date(2024, 5, 30), date(2024, 5, 30), date(2024, 6, 5)],
        'cash_div': [0.5, 0.3, 0.2, None, 0.1, 1.0, 1.0, 0.5],
        'share_div': [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5],
    })


def _shares() -> pl.DataFrame:
    return pl.DataFrame({
        'ts_code': ['600000.SH', '600000.SH', '600002.SH', '600003.SH',
                    '600003.SH'],
        'effective_date': [date(2020, 1, 1), date(2024, 10, 1), date(2020, 1, 1),
                           date(2020, 1, 1), date(2024, 6, 10)],
        'total_share': [1_000_000_000, 2_000_000_000, 100_000_000,
                        1_000_000_000, 1_500_000_000],
        'float_share': [800_000_000, 1_600_000_000, None,
                        1_000_000_000, 1_500_000_000],
    })


def test_uses_as_of_share_not_future():
    out = derive(_dividends(), _shares())
    rows = {r['cash_div']: r for r in out.to_dicts() if r['ts_code'] == '600000.SH'}
    first, second = rows[0.5], rows[0.3]
    # ex 口径：2024-05 事件不得使用 2024-10 才生效的新股本
    assert first['est_total_share'] == 1_000_000_000
    assert first['est_effective_date'] == date(2020, 1, 1)
    assert first['total_div_est_all'] == 0.5 * 1_000_000_000
    # record 口径同理
    assert first['est_rec_total_share'] == 1_000_000_000
    assert first['est_rec_effective_date'] == date(2020, 1, 1)
    assert first['total_div_est_rec_all'] == 0.5 * 1_000_000_000
    # 2025 年事件已可使用 2024-10-01 生效的新股本
    assert second['est_total_share'] == 2_000_000_000
    assert second['total_div_est_all'] == 0.3 * 2_000_000_000
    assert second['total_div_est_rec_all'] == 0.3 * 2_000_000_000


def test_split_event_record_vs_ex_differ():
    out = derive(_dividends(), _shares())
    row = out.filter(pl.col('ts_code') == '600003.SH').row(0, named=True)
    # 10 送 5：除权日股本 15 亿，登记日股本 10 亿
    assert row['total_div_est_all'] == 0.5 * 1_500_000_000
    assert row['total_div_est_rec_all'] == 0.5 * 1_000_000_000
    assert row['total_div_est_all'] / row['total_div_est_rec_all'] == 1.5
    assert row['est_rec_effective_date'] == date(2020, 1, 1)


def test_record_date_null_does_not_fallback():
    out = derive(_dividends(), _shares())
    row = out.filter(pl.col('ts_code') == '600000.SH',
                     pl.col('cash_div') == 0.1).row(0, named=True)
    assert row['total_div_est_all'] is not None
    assert row['total_div_est_rec_all'] is None
    assert row['est_rec_total_share'] is None


def test_ex_date_null_and_cash_null_are_null():
    out = derive(_dividends(), _shares())
    plan = out.filter(pl.col('ts_code') == '600000.SH', pl.col('cash_div') == 0.2).row(
        0, named=True)
    assert plan['total_div_est_all'] is None
    assert plan['total_div_est_rec_all'] is None
    no_cash = out.filter(pl.col('ts_code') == '600000.SH',
                         pl.col('ex_date') == date(2024, 5, 21),
                         pl.col('cash_div').is_null()).row(0, named=True)
    assert no_cash['total_div_est_all'] is None
    assert no_cash['total_div_est_rec_all'] is None


def test_share_missing_is_null_and_counted():
    out = derive(_dividends(), _shares())
    row = out.filter(pl.col('ts_code') == '600001.SH').row(0, named=True)
    assert row['total_div_est_all'] is None
    assert row['total_div_est_rec_all'] is None
    stats = coverage(out)
    assert stats['est_all']['missing_reasons'] == {
        'ex_date_null': 1, 'cash_div_null': 1, 'share_asof_missing': 1}
    assert stats['est_all']['computable'] == 5
    assert stats['est_rec_all']['missing_reasons'] == {
        'record_date_null': 2, 'cash_div_null': 1, 'share_asof_missing': 1}
    assert stats['est_rec_all']['computable'] == 4


def test_float_missing_only_affects_float_column():
    out = derive(_dividends(), _shares())
    row = out.filter(pl.col('ts_code') == '600002.SH').row(0, named=True)
    assert row['total_div_est_all'] == 100_000_000
    assert row['total_div_est_float'] is None
    assert row['est_float_share'] is None


def test_rerun_is_idempotent():
    once = derive(_dividends(), _shares())
    twice = derive(once, _shares())
    assert twice.equals(once)
