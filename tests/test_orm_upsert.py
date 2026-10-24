# -*- coding: utf-8 -*-

"""ORM 批量 upsert 工具单元测试（sqlite 内存库，模型无关）"""

from datetime import date

import pytest
from sqlalchemy import Column, Date, Float, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from src.database.upsert import upsert_rows

Base = declarative_base()


class _FakeDaily(Base):
    __tablename__ = 'fake_daily'
    id = Column(Integer, primary_key=True)
    ts_code = Column(String(12))
    trade_date = Column(Date)
    close = Column(Float)
    vol = Column(Float)
    frequency = Column(String(4))


@pytest.fixture
def session():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    db_session = sessionmaker(bind=engine)()
    yield db_session
    db_session.close()


def _row(day, close, vol=100.0):
    return {
        'ts_code': '600000.SH',
        'trade_date': date(2024, 1, day),
        'close': close,
        'vol': vol,
    }


def test_insert_new_rows(session):
    inserted, updated = upsert_rows(
        session, _FakeDaily, [_row(2, 10.0), _row(3, 11.0)],
        key_fields=('ts_code', 'trade_date'),
        value_fields=('close', 'vol'),
    )
    session.commit()

    assert (inserted, updated) == (2, 0)
    assert session.query(_FakeDaily).count() == 2


def test_update_existing_rows_without_duplicates(session):
    upsert_rows(session, _FakeDaily, [_row(2, 10.0)], ('ts_code', 'trade_date'),
                ('close', 'vol'))
    session.commit()

    inserted, updated = upsert_rows(
        session, _FakeDaily, [_row(2, 20.0)],
        key_fields=('ts_code', 'trade_date'),
        value_fields=('close', 'vol'),
    )
    session.commit()

    assert (inserted, updated) == (0, 1)
    assert session.query(_FakeDaily).count() == 1
    assert session.query(_FakeDaily).one().close == 20.0


def test_create_defaults_only_applied_on_insert(session):
    upsert_rows(session, _FakeDaily, [_row(2, 10.0)], ('ts_code', 'trade_date'),
                ('close', 'vol'), create_defaults={'frequency': 'd'})
    session.commit()

    upsert_rows(session, _FakeDaily, [_row(2, 12.0)], ('ts_code', 'trade_date'),
                ('close', 'vol'), create_defaults={'frequency': '5m'})
    session.commit()

    row = session.query(_FakeDaily).one()
    assert row.close == 12.0
    assert row.frequency == 'd'


def test_duplicate_keys_in_single_batch_written_once(session):
    inserted, updated = upsert_rows(
        session, _FakeDaily, [_row(2, 10.0), _row(2, 15.0)],
        key_fields=('ts_code', 'trade_date'),
        value_fields=('close', 'vol'),
    )
    session.commit()

    assert (inserted, updated) == (1, 1)
    assert session.query(_FakeDaily).count() == 1
    assert session.query(_FakeDaily).one().close == 15.0


def test_empty_rows_noop(session):
    assert upsert_rows(session, _FakeDaily, [], ('ts_code', 'trade_date')) == (0, 0)
