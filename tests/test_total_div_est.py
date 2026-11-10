"""total_div_est 两版派生单测：ex_date 与 record_date 口径、缺失与不回填、
有效层（``*_fix`` / ``no_ex_rights``）口径与 DG-31 写回兜底。"""

import sys
from datetime import date
from pathlib import Path

import polars as pl
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / 'tools'))

import derive_total_dividend as mod  # noqa: E402
from derive_total_dividend import (  # noqa: E402
    DERIVED_SCHEMA,
    affected_rows,
    apply_changes,
    assert_no_new_nulls,
    coverage,
    derive,
)

from src.data.dividend_effective import DividendSchemaError  # noqa: E402

FIX_CODE = '601258.SH'


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
        'cash_div_fix': [None] * 8,
        'share_div_fix': [None] * 8,
        'no_ex_rights': [False] * 8,
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


def _fix_frame() -> tuple[pl.DataFrame, pl.DataFrame]:
    """601258.SH 2019-12-30 的等价夹具：原始 cash_div=0，修正 cash_div_fix=0.04。"""
    frame = pl.DataFrame({
        'ts_code': [FIX_CODE, FIX_CODE],
        'ex_date': [date(2019, 12, 30), date(2022, 3, 1)],
        'record_date': [date(2019, 12, 27), date(2022, 2, 25)],
        'cash_div': [0.0, 0.1],
        'share_div': [0.56416, 0.0],
        'cash_div_fix': [0.04, None],
        'share_div_fix': [0.0, None],
        'no_ex_rights': [False, False],
    })
    shares = pl.DataFrame({
        'ts_code': [FIX_CODE, FIX_CODE],
        'effective_date': [date(2019, 10, 17), date(2019, 12, 30)],
        'total_share': [6_538_478_402, 10_227_225_070],
        'float_share': [6_538_478_402, 10_227_225_070],
    })
    return frame, shares


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


# --- 有效层口径 -------------------------------------------------------------


def test_effective_layer_overrides_raw_cash_div():
    frame, shares = _fix_frame()
    out = derive(frame, shares)
    row = out.filter(pl.col('ex_date') == date(2019, 12, 30)).row(0, named=True)
    assert row['total_div_est_all'] == pytest.approx(0.04 * 10_227_225_070)
    assert row['total_div_est_float'] == pytest.approx(0.04 * 10_227_225_070)
    assert row['total_div_est_rec_all'] == pytest.approx(0.04 * 6_538_478_402)
    assert row['total_div_est_rec_float'] == pytest.approx(0.04 * 6_538_478_402)


def test_identity_effective_columns_must_fail():
    """把有效层换成恒等函数（= 旧实现只读原始层），断言必须失败。"""
    frame, shares = _fix_frame()
    expected = 0.04 * 10_227_225_070

    def check() -> None:
        row = derive(frame, shares).filter(
            pl.col('ex_date') == date(2019, 12, 30)).row(0, named=True)
        assert row['total_div_est_all'] == pytest.approx(expected), (
            f'601258.SH 2019-12-30 必须按 cash_div_fix=0.04 派生，实得 '
            f'{row["total_div_est_all"]}')

    check()
    original = mod.apply_effective_columns
    try:
        mod.apply_effective_columns = lambda frame, **kwargs: frame
        with pytest.raises(AssertionError):
            check()
    finally:
        mod.apply_effective_columns = original


def test_missing_fix_columns_raise():
    frame = _dividends().drop(['cash_div_fix', 'share_div_fix', 'no_ex_rights'])
    with pytest.raises(DividendSchemaError):
        derive(frame, _shares())


def test_no_ex_rights_cash_estimates_are_zero_not_null():
    frame = _dividends().with_columns(
        pl.when(pl.col('ts_code') == '600003.SH')
        .then(pl.lit(True)).otherwise(pl.col('no_ex_rights')).alias('no_ex_rights'))
    out = derive(frame, _shares())
    row = out.filter(pl.col('ts_code') == '600003.SH').row(0, named=True)
    # 事件不向原股东分配 → 金额 0（不是 null，也不是按 cash_div 算出的 0.5×股本）
    assert row['total_div_est_all'] == 0.0
    assert row['total_div_est_rec_all'] == 0.0


def test_affected_rows_only_cash_div_fix():
    frame, _ = _fix_frame()
    mask = affected_rows(frame)
    assert mask.to_list() == [True, False]


def test_apply_changes_writes_back_unaffected_rows():
    frame, shares = _fix_frame()
    # 存量表：先按「无修正」派生一次，再补上 cash_div_fix=0.04 重算
    stored = derive(frame.with_columns(pl.lit(None, dtype=pl.Float64)
                                       .alias('cash_div_fix')), shares)
    updated = frame
    affected = affected_rows(updated)
    assert affected.to_list() == [True, False]
    out = apply_changes(stored, derive(updated, shares), affected)
    assert out.height == stored.height
    # 未命中分支的行原样写回（含既有派生值），不得出现新增空值
    assert out.filter(~pl.Series('aff', affected)).equals(
        stored.filter(~pl.Series('aff', affected)))
    for col in DERIVED_SCHEMA:
        assert out[col].null_count() == stored[col].null_count()
    changed = out.filter(pl.Series('aff', affected)).row(0, named=True)
    assert changed['total_div_est_all'] == pytest.approx(0.04 * 10_227_225_070)
    assert stored.filter(pl.Series('aff', affected))['total_div_est_all'][0] == 0.0


def test_assert_no_new_nulls_detects_null_regression():
    """DG-31：断言必须能对「空值从 0 变成 N」失败（自测反例）。"""
    frame, shares = _fix_frame()
    before = derive(frame, shares)
    assert_no_new_nulls(before, apply_changes(frame, before, affected_rows(frame)))
    broken = before.with_columns(
        pl.when(pl.int_range(pl.len()) == 1).then(None)
        .otherwise(pl.col('total_div_est_all')).alias('total_div_est_all'))
    assert broken['total_div_est_all'].null_count() == 1
    with pytest.raises(RuntimeError):
        assert_no_new_nulls(before, broken)
    short = before.head(1)
    with pytest.raises(RuntimeError):
        assert_no_new_nulls(before, short)
