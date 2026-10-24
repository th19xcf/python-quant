# -*- coding: utf-8 -*-

"""akshare 分红数据双键去重逻辑单元测试（sqlite 内存库）"""

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.data.akshare_handler import AkShareHandler
from src.database.models.stock import StockDividend


@pytest.fixture
def handler(tmp_path):
    # 用文件库而非 :memory:，避免与其他测试创建的 sqlite 内存库相互干扰
    engine = create_engine(f'sqlite:///{tmp_path / "dividend.db"}')
    StockDividend.__table__.create(engine)
    db_session = sessionmaker(bind=engine)()

    instance = AkShareHandler.__new__(AkShareHandler)
    instance.session = db_session

    yield instance

    db_session.close()


def _row(ex_date=None, year='2023', cash_div=0.5, share_div=0.0):
    return {
        'symbol': '600000',
        'name': '浦发银行',
        'dividend_year': year,
        'report_date': date(2023, 3, 1),
        'record_date': None,
        'ex_date': ex_date,
        'pay_date': None,
        'cash_div': cash_div,
        'share_div': share_div,
        'total_div': 1.0,
        'rights_issue_price': None,
        'rights_issue_ratio': None,
    }


def test_insert_new_rows(handler):
    inserted, updated = handler._save_dividend_rows(
        '600000.SH', [_row(ex_date=date(2023, 6, 1)), _row(year='2022')])
    handler.session.commit()

    assert (inserted, updated) == (2, 0)
    assert handler.session.query(StockDividend).count() == 2


def test_rerun_is_idempotent(handler):
    rows = [_row(ex_date=date(2023, 6, 1)), _row(year='2022')]
    handler._save_dividend_rows('600000.SH', rows)
    handler.session.commit()

    inserted, updated = handler._save_dividend_rows('600000.SH', rows)
    handler.session.commit()

    assert (inserted, updated) == (0, 2)
    assert handler.session.query(StockDividend).count() == 2


def test_placeholder_is_backfilled_by_ex_date(handler):
    handler._save_dividend_rows('600000.SH', [_row(year='2023', cash_div=0.3)])
    handler.session.commit()

    inserted, updated = handler._save_dividend_rows(
        '600000.SH', [_row(ex_date=date(2023, 6, 1), cash_div=0.5)])
    handler.session.commit()

    assert (inserted, updated) == (0, 1)
    record = handler.session.query(StockDividend).one()
    assert record.ex_date == date(2023, 6, 1)
    assert record.cash_div == 0.5


def test_missing_ex_date_does_not_erase_existing(handler):
    handler._save_dividend_rows(
        '600000.SH', [_row(ex_date=date(2023, 6, 1), cash_div=0.5)])
    handler.session.commit()

    inserted, updated = handler._save_dividend_rows(
        '600000.SH', [_row(ex_date=None, cash_div=0.4)])
    handler.session.commit()

    assert (inserted, updated) == (0, 1)
    record = handler.session.query(StockDividend).one()
    assert record.ex_date == date(2023, 6, 1)
    assert record.cash_div == 0.4


def test_two_events_same_year_are_kept_separately(handler):
    handler._save_dividend_rows(
        '600000.SH', [_row(ex_date=date(2023, 1, 10), cash_div=0.2)])
    handler.session.commit()

    inserted, updated = handler._save_dividend_rows(
        '600000.SH', [_row(ex_date=date(2023, 9, 10), cash_div=0.8)])
    handler.session.commit()

    assert (inserted, updated) == (1, 0)
    assert handler.session.query(StockDividend).count() == 2
