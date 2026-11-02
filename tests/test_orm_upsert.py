
"""ORM 批量 upsert 工具单元测试（sqlite 内存库，模型无关）

覆盖原子写法的关键语义：
- 同批重复键只落一行、末行覆盖前行；
- 跨批重复键（含 > _WRITE_CHUNK 行）不报重复键错误，按更新处理；
- 方言选择：MySQL 走 ON DUPLICATE KEY UPDATE，SQLite 走 ON CONFLICT DO UPDATE；
- 冲突更新保留 created_at、刷新 updated_at，create_defaults 只在插入生效。
"""

from datetime import date, datetime

import pytest
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    String,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.dialects import mysql, sqlite
from sqlalchemy.orm import declarative_base, sessionmaker

from src.database.upsert import _build_upsert_statement, upsert_rows

Base = declarative_base()


class _FakeDaily(Base):
    __tablename__ = 'fake_daily'
    id = Column(Integer, primary_key=True)
    ts_code = Column(String(12), nullable=False)
    trade_date = Column(Date, nullable=False)
    close = Column(Float)
    vol = Column(Float)
    frequency = Column(String(4))
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)

    # SQLite 的 ON CONFLICT DO UPDATE 需要匹配的唯一索引，否则无法命中冲突分支
    __table_args__ = (
        UniqueConstraint('ts_code', 'trade_date', name='uq_fake_daily'),
    )


@pytest.fixture
def session():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    db_session = sessionmaker(bind=engine)()
    yield db_session
    db_session.close()
    # 释放连接池持有的 sqlite 连接，避免 ResourceWarning: unclosed database
    engine.dispose()


def _row(day, close, vol=100.0, ts_code='600000.SH'):
    return {
        'ts_code': ts_code,
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


def test_duplicate_keys_across_write_chunks(session):
    """跨批（> _WRITE_CHUNK 行）命中已存在键时按更新处理，不报重复键错误。"""
    # 用一个不落在下面 6000xx 命名空间里的键，避免新行与已存在键意外重合
    existing = _row(1, 10.0, ts_code='610000.SH')
    upsert_rows(session, _FakeDaily, [existing], ('ts_code', 'trade_date'),
                ('close', 'vol'))
    session.commit()

    # 400 个新键占满第一批，第二批首行是已存在的键（同一 (ts_code, trade_date)）
    rows = [
        {'ts_code': f'6000{i:02d}.SH', 'trade_date': date(2024, 1, 1),
         'close': 1.0, 'vol': 1.0}
        for i in range(400)
    ]
    rows.append({**existing, 'close': 99.0})

    inserted, updated = upsert_rows(session, _FakeDaily, rows,
                                    ('ts_code', 'trade_date'), ('close', 'vol'))
    session.commit()

    assert (inserted, updated) == (400, 1)
    assert session.query(_FakeDaily).count() == 401
    stored = (session.query(_FakeDaily)
              .filter_by(ts_code=existing['ts_code'],
                         trade_date=existing['trade_date']).one())
    assert stored.close == 99.0


def test_conflict_keeps_created_at_and_refreshes_updated_at(session):
    """冲突时只更新业务列：created_at 保留原值，updated_at 刷新。"""
    created = datetime(2024, 1, 1, 9, 0, 0)
    session.add(_FakeDaily(
        ts_code='600000.SH', trade_date=date(2024, 1, 2),
        close=10.0, vol=100.0, frequency='d',
        created_at=created, updated_at=created,
    ))
    session.commit()

    upsert_rows(session, _FakeDaily, [_row(2, 20.0)], ('ts_code', 'trade_date'),
                ('close', 'vol'))
    session.commit()

    row = session.query(_FakeDaily).one()
    assert row.created_at == created
    assert row.updated_at is not None
    assert row.updated_at != created


@pytest.mark.parametrize('dialect_name', ['mysql', 'sqlite'])
def test_statement_selected_by_dialect(dialect_name):
    """按方言生成原子 upsert 语句，冲突分支不应依赖调用方再查一次。"""
    stmt = _build_upsert_statement(
        _FakeDaily, ('ts_code', 'trade_date'), ('close', 'vol'), dialect_name)
    dialect = mysql.dialect() if dialect_name == 'mysql' else sqlite.dialect()
    compiled = str(stmt.compile(dialect=dialect)).lower()

    if dialect_name == 'mysql':
        assert 'on duplicate key update' in compiled
        assert 'close' in compiled and 'updated_at' in compiled
    else:
        assert 'on conflict' in compiled
        assert 'do update' in compiled


def test_insert_sets_both_timestamps(session):
    """插入路径由模型默认值写入 created_at / updated_at。"""
    upsert_rows(session, _FakeDaily, [_row(2, 10.0)], ('ts_code', 'trade_date'),
                ('close', 'vol'))
    session.commit()

    row = session.query(_FakeDaily).one()
    assert row.created_at is not None
    assert row.updated_at is not None


def test_empty_rows_noop(session):
    assert upsert_rows(session, _FakeDaily, [], ('ts_code', 'trade_date')) == (0, 0)
