"""本地列存 → fund_daily / closed_fund_daily / index_daily 入库测试。"""

from datetime import date
from types import SimpleNamespace

import polars as pl
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.data.local_store.store import LocalQuoteStore
from src.data.services.local_store_daily_sync import sync_local_store_daily
from src.database.models.fund import ClosedFundDaily, FundDaily
from src.database.models.index import IndexBasic, IndexDaily


class _DB:
    """最小 db_manager 替身：sync 只用 get_session()。"""

    def __init__(self, session):
        self._session = session

    def get_session(self):
        return self._session


def _frame(rows):
    return pl.DataFrame(
        {
            'date': [r[0] for r in rows],
            'open': [r[1] for r in rows],
            'high': [r[2] for r in rows],
            'low': [r[3] for r in rows],
            'close': [r[4] for r in rows],
            'volume': [r[5] for r in rows],
            'amount': [r[6] for r in rows],
        },
        schema_overrides={'date': pl.Date},
    )


_FUND_ROWS = [
    (date(2024, 1, 2), 10.0, 10.5, 9.8, 10.0, 100.0, 1000.0),
    (date(2024, 1, 3), 10.0, 10.8, 9.9, 10.5, 120.0, 1260.0),
    (date(2024, 1, 4), 10.5, 11.0, 10.4, 10.8, 130.0, 1404.0),
]
_CLOSED_ROWS = [
    (date(2024, 1, 2), 1.0, 1.05, 0.98, 1.02, 80.0, 800.0),
    (date(2024, 1, 3), 1.02, 1.06, 1.0, 1.05, 90.0, 900.0),
]


def _store(tmp_path):
    store = LocalQuoteStore(tmp_path / 'local_store', enabled=True)
    store.write_daily('fund', '510050.SH', _frame(_FUND_ROWS), full=True)
    store.write_daily('fund', '505888.SH', _frame(_CLOSED_ROWS), full=True)
    store.write_daily('index', '000001.SH', _frame(_FUND_ROWS), full=True)
    store.flush_meta()
    return store


def _open_db(tmp_path):
    engine = create_engine(f'sqlite:///{tmp_path / "daily.db"}')
    for model in (FundDaily, ClosedFundDaily, IndexDaily, IndexBasic):
        model.__table__.create(engine)
    session = sessionmaker(bind=engine)()
    return engine, session


def _config():
    return SimpleNamespace(data=SimpleNamespace())


def test_fund_split_units_and_pre_close(tmp_path):
    """基金按代码段分流；vol=手/amount=元/价格不缩放；首行 pre_close 为空。"""
    store = _store(tmp_path)
    engine, session = _open_db(tmp_path)
    try:
        summary = sync_local_store_daily(
            _config(), _DB(session), asset='fund', store=store,
            ts_codes=['510050.SH', '505888.SH'],
            progress_path=tmp_path / 'progress.json')
        assert summary['rows'] == 5
        assert summary['tables']['fund_daily']['codes'] == 1
        assert summary['tables']['closed_fund_daily']['codes'] == 1
        assert summary['tables']['fund_daily']['start'] == date(2024, 1, 2)
        assert summary['tables']['fund_daily']['end'] == date(2024, 1, 4)

        fund_rows = session.query(FundDaily).order_by(FundDaily.trade_date).all()
        assert [row.trade_date for row in fund_rows] == [
            date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]
        assert fund_rows[0].pre_close is None
        assert fund_rows[0].change is None
        assert fund_rows[1].pre_close == 10.0
        assert fund_rows[1].change == 0.5
        assert fund_rows[0].open == 10.0          # 不做任何缩放
        assert fund_rows[0].vol == 100.0          # 手
        assert fund_rows[0].amount == 1000.0      # 元
        assert session.query(ClosedFundDaily).count() == 2
    finally:
        session.close()
        engine.dispose()


def test_repeat_run_is_idempotent(tmp_path):
    """关闭续跑后重跑：行数不变，既有行被本地值覆盖（updated 计数）。"""
    store = _store(tmp_path)
    engine, session = _open_db(tmp_path)
    try:
        first = sync_local_store_daily(
            _config(), _DB(session), asset='fund', store=store,
            ts_codes=['510050.SH'], progress_path=tmp_path / 'progress.json')
        assert first['tables']['fund_daily']['inserted'] == 3

        second = sync_local_store_daily(
            _config(), _DB(session), asset='fund', store=store,
            ts_codes=['510050.SH'], progress_path=tmp_path / 'p2.json')
        assert second['tables']['fund_daily']['inserted'] == 0
        assert second['tables']['fund_daily']['updated'] == 3
        assert session.query(FundDaily).count() == 3
    finally:
        session.close()
        engine.dispose()


def test_dry_run_counts_without_writing(tmp_path):
    """dry-run 只统计行数与区间，不写库、不产生进度文件。"""
    store = _store(tmp_path)
    engine, session = _open_db(tmp_path)
    progress = tmp_path / 'progress.json'
    try:
        summary = sync_local_store_daily(
            _config(), _DB(session), asset='fund', store=store,
            ts_codes=['510050.SH', '505888.SH'], dry_run=True,
            progress_path=progress)
        assert summary['rows'] == 5
        assert summary['rows_committed'] == 0
        assert session.query(FundDaily).count() == 0
        assert session.query(ClosedFundDaily).count() == 0
        assert not progress.exists()
    finally:
        session.close()
        engine.dispose()


def test_since_keeps_pre_close_seed(tmp_path):
    """since 过滤后首行 pre_close 仍取本地序列的上一交易日。"""
    store = _store(tmp_path)
    engine, session = _open_db(tmp_path)
    try:
        summary = sync_local_store_daily(
            _config(), _DB(session), asset='fund', store=store,
            ts_codes=['510050.SH'], since=date(2024, 1, 3),
            progress_path=tmp_path / 'progress.json')
        rows = session.query(FundDaily).order_by(FundDaily.trade_date).all()
        assert summary['rows'] == 2
        assert [row.trade_date for row in rows] == [
            date(2024, 1, 3), date(2024, 1, 4)]
        assert rows[0].pre_close == 10.0
    finally:
        session.close()
        engine.dispose()


def test_index_local_wins_overwrites_and_keeps_remote_only_dates(tmp_path):
    """index_daily 冲突策略本地为准：共同日期被覆盖，仅远程存在的日期保留。"""
    store = _store(tmp_path)
    engine, session = _open_db(tmp_path)
    try:
        session.add_all([
            IndexDaily(ts_code='000001.SH', trade_date=date(2024, 1, 1), close=900.0),
            IndexDaily(ts_code='000001.SH', trade_date=date(2024, 1, 2), close=999.0),
        ])
        session.commit()

        summary = sync_local_store_daily(
            _config(), _DB(session), asset='index', store=store,
            ts_codes=['000001.SH'], progress_path=tmp_path / 'progress.json')
        assert summary['conflict_policy'] == 'local-wins'
        assert summary['tables']['index_daily']['updated'] == 1
        assert summary['tables']['index_daily']['inserted'] == 2

        rows = session.query(IndexDaily).order_by(IndexDaily.trade_date).all()
        assert len(rows) == 4                  # 远程独有的 1/1 未被删除
        assert rows[0].trade_date == date(2024, 1, 1)
        assert rows[1].close == 10.0            # 共同日期被本地值覆盖
    finally:
        session.close()
        engine.dispose()


def test_resume_skips_done_codes(tmp_path):
    """进度文件记录已完成标的：再跑直接跳过，不重复读列存。"""
    store = _store(tmp_path)
    engine, session = _open_db(tmp_path)
    progress = tmp_path / 'progress.json'
    try:
        sync_local_store_daily(
            _config(), _DB(session), asset='fund', store=store,
            ts_codes=['510050.SH'], progress_path=progress)

        summary = sync_local_store_daily(
            _config(), _DB(session), asset='fund', store=store,
            ts_codes=['510050.SH'], progress_path=progress)
        assert summary['codes_resumed'] == 1
        assert summary['rows'] == 0
    finally:
        session.close()
        engine.dispose()
