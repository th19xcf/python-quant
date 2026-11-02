"""index_daily 缺口补行逻辑测试（纯函数 + sqlite 幂等验证）。"""

from datetime import date

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from src.data.index_daily_fill import (
    build_missing_rows,
    contiguous_segments,
    missing_trade_dates,
)
from src.database.models.index import IndexDaily


def _rec(day, close, volume=1000, amount=12345.0):
    return {
        'date': day, 'open': close, 'high': close, 'low': close,
        'close': close, 'volume': volume, 'amount': amount,
    }


def test_missing_trade_dates_is_set_difference():
    tdx = [date(2023, 1, 3), date(2023, 1, 4), date(2023, 1, 5)]
    existing = [date(2023, 1, 4)]
    assert missing_trade_dates(tdx, existing) == [
        date(2023, 1, 3), date(2023, 1, 5)]


def test_gap_first_row_uses_tdx_prev_close():
    """缺口首行的昨收取 TDX 上一交易日 close，而不是表内上一行。"""
    records = [_rec(date(2022, 12, 30), 3000.0), _rec(date(2023, 1, 3), 3100.0)]
    rows = build_missing_rows('000001.SH', records, [date(2022, 12, 30)])

    assert len(rows) == 1
    row = rows[0]
    assert row['pre_close'] == 3000.0
    assert row['change'] == 100.0
    assert row['pct_chg'] == pytest.approx(100.0 / 3000.0 * 100)


def test_no_prev_close_leaves_derived_fields_empty():
    """TDX 序列首行没有上一交易日，派生字段置空而非造数。"""
    rows = build_missing_rows(
        '399006.SZ', [_rec(date(2010, 6, 1), 1000.0)], [])

    row = rows[0]
    assert row['pre_close'] is None
    assert row['change'] is None
    assert row['pct_chg'] is None


def test_existing_rows_are_skipped_but_still_advance_prev_close():
    records = [
        _rec(date(2023, 1, 3), 3000.0),
        _rec(date(2023, 1, 4), 3050.0),
        _rec(date(2023, 1, 5), 3100.0),
    ]
    rows = build_missing_rows(
        '000001.SH', records, [date(2023, 1, 4)])

    assert [row['trade_date'] for row in rows] == [
        date(2023, 1, 3), date(2023, 1, 5)]
    assert rows[0]['pre_close'] is None
    # 1-5 的昨收必须是 TDX 的 1-4（既有行），不能跳过它
    assert rows[1]['pre_close'] == 3050.0
    assert rows[1]['change'] == 50.0


def test_rerun_with_all_dates_existing_inserts_nothing():
    records = [_rec(date(2023, 1, 3), 3000.0), _rec(date(2023, 1, 4), 3050.0)]
    all_dates = [record['date'] for record in records]

    assert build_missing_rows('000001.SH', records, all_dates) == []


def test_contiguous_segments_splits_on_natural_day_gap():
    days = [date(2023, 1, 3), date(2023, 1, 4), date(2023, 2, 1)]
    assert contiguous_segments(days) == [
        (date(2023, 1, 3), date(2023, 1, 4)), (date(2023, 2, 1), date(2023, 2, 1))]


def test_sqlite_rerun_does_not_add_rows():
    engine = create_engine('sqlite:///:memory:')
    IndexDaily.__table__.create(engine)
    session = sessionmaker(bind=engine)()

    records = [_rec(date(2023, 1, 3), 3000.0), _rec(date(2023, 1, 4), 3050.0)]
    rows = build_missing_rows('000001.SH', records, [])
    session.add_all([IndexDaily(**row) for row in rows])
    session.commit()
    first_count = len(session.scalars(select(IndexDaily)).all())

    existing = list(session.scalars(select(IndexDaily.trade_date)))
    rerun_rows = build_missing_rows('000001.SH', records, existing)
    session.add_all([IndexDaily(**row) for row in rerun_rows])
    session.commit()

    second_count = len(session.scalars(select(IndexDaily)).all())
    assert rerun_rows == []
    assert second_count == first_count == 2

    session.close()
    engine.dispose()
