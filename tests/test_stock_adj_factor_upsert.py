"""stock_adj_factor 原子 upsert 写入路径测试（sqlite 内存库）。

覆盖用户要求的四个语义：
- 同批重复键只落一行、取末值；
- 跨批重复键（按 batch_size 分批）同样只落一行、取末值，不报 1062；
- 冲突更新保留 created_at；
- 冲突更新刷新 updated_at。
"""

from datetime import date, datetime

import polars as pl
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.data.adj_factor_calculator import AdjFactorCalculator
from src.database.models.stock import StockAdjFactor


class _StubDbManager:
    """只提供 get_session 的最小 db_manager 桩。"""

    def __init__(self, session):
        self._session = session

    def get_session(self):
        return self._session


@pytest.fixture
def session():
    engine = create_engine('sqlite:///:memory:')
    StockAdjFactor.__table__.create(engine)
    db_session = sessionmaker(bind=engine)()
    yield db_session
    db_session.close()
    engine.dispose()


def _frame(rows):
    return pl.DataFrame({
        'ts_code': [r[0] for r in rows],
        'trade_date': [r[1] for r in rows],
        'qfq_factor': [r[2] for r in rows],
        'hfq_factor': [r[3] for r in rows],
    })


def _calculator(session):
    return AdjFactorCalculator(_StubDbManager(session), tdx_handler=None)


def test_duplicate_keys_in_single_batch_keep_last_value(session):
    """同批重复键只留 1 行，且取最后一行因子值。"""
    calc = _calculator(session)
    calc.save_adj_factors(_frame([
        ('600000.SH', date(2024, 1, 2), 1.0, 1.0),
        ('600000.SH', date(2024, 1, 2), 0.9, 1.1),
    ]), batch_size=1000)

    rows = session.query(StockAdjFactor).all()
    assert len(rows) == 1
    assert rows[0].qfq_factor == 0.9
    assert rows[0].hfq_factor == 1.1


def test_duplicate_keys_across_batches_keep_last_value(session):
    """跨批（batch_size=1）重复键走 ON CONFLICT 更新，不抛 IntegrityError。"""
    calc = _calculator(session)
    calc.save_adj_factors(_frame([
        ('600000.SH', date(2024, 1, 2), 1.0, 1.0),
        ('600000.SH', date(2024, 1, 2), 0.8, 1.2),
    ]), batch_size=1)

    rows = session.query(StockAdjFactor).all()
    assert len(rows) == 1
    assert rows[0].qfq_factor == 0.8
    assert rows[0].hfq_factor == 1.2


def test_conflict_keeps_created_at_and_refreshes_updated_at(session):
    """冲突只更新业务列：created_at 保留，updated_at 刷新。"""
    created = datetime(2024, 1, 1, 9, 0, 0)
    session.add(StockAdjFactor(
        ts_code='600000.SH', trade_date=date(2024, 1, 2),
        qfq_factor=1.0, hfq_factor=1.0,
        created_at=created, updated_at=created,
    ))
    session.commit()

    calc = _calculator(session)
    calc.save_adj_factors(_frame([
        ('600000.SH', date(2024, 1, 2), 0.95, 1.05),
    ]))

    row = session.query(StockAdjFactor).one()
    assert row.qfq_factor == 0.95
    assert row.hfq_factor == 1.05
    assert row.created_at == created
    assert row.updated_at is not None
    assert row.updated_at != created


def test_insert_writes_both_timestamps(session):
    calc = _calculator(session)
    calc.save_adj_factors(_frame([('600000.SH', date(2024, 1, 2), 1.0, 1.0)]))

    row = session.query(StockAdjFactor).one()
    assert row.created_at is not None
    assert row.updated_at is not None


def test_vectorized_column_names_unchanged(session):
    """写入列只有 ts_code/trade_date/qfq_factor/hfq_factor 四个业务列。"""
    calc = _calculator(session)
    calc.save_adj_factors(_frame([
        ('000001.SZ', date(2024, 1, 2), 0.75, 1.25),
    ]))

    row = session.query(StockAdjFactor).one()
    assert row.ts_code == '000001.SZ'
    assert row.trade_date == date(2024, 1, 2)
    assert row.qfq_factor == 0.75
    assert row.hfq_factor == 1.25
