"""TDX → stock_daily 离线补库测试：全量/增量/dry-run/幂等。"""

import struct
from datetime import date
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.data.services.stock_daily_fill import fill_stock_daily
from src.database.models.stock import StockDaily

_RECORDS_V1 = [
    ('20240102', 10.0, 10.5, 9.8, 10.0, 100, 1000),
    ('20240103', 10.0, 10.8, 9.9, 10.5, 120, 1260),
]
_RECORD_V2 = ('20240104', 10.5, 11.0, 10.4, 10.8, 130, 1404)


def _write_day_file(tmp_path, records):
    path = tmp_path / 'sh' / 'lday' / 'sh600000.day'
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'wb') as f:
        for day, open_, high, low, close, volume, amount in records:
            f.write(struct.pack(
                '<IIIIIfII',
                int(day), int(open_ * 100), int(high * 100), int(low * 100),
                int(close * 100), float(amount), int(volume * 100), 0,
            ))
    return path


def _config(tmp_path):
    return SimpleNamespace(data=SimpleNamespace(tdx_data_path=str(tmp_path)))


class _DB:
    """最小 db_manager 替身：fill_stock_daily 只用 get_session()。"""

    def __init__(self, session):
        self._session = session

    def get_session(self):
        return self._session


def _open(tmp_path):
    engine = create_engine(f'sqlite:///{tmp_path / "stock_daily.db"}')
    StockDaily.__table__.create(engine)
    session = sessionmaker(bind=engine)()
    return engine, session


def _rows(session):
    return session.query(StockDaily).order_by(StockDaily.trade_date).all()


def test_dry_run_counts_rows_without_writing(tmp_path):
    """dry-run 只统计行数，不写库。"""
    _write_day_file(tmp_path, _RECORDS_V1)
    engine, session = _open(tmp_path)
    try:
        summary = fill_stock_daily(
            _config(tmp_path), _DB(session), full=True, dry_run=True)

        assert summary['rows'] == 2
        assert summary['inserted'] == 0
        assert summary['stocks_processed'] == 1
        assert _rows(session) == []
    finally:
        session.close()
        engine.dispose()


def test_incremental_inserts_rows_with_units_and_pre_close(tmp_path):
    """空库首次增量 = 全量：写入手/元口径，pre_close 按 TDX 上一交易日。"""
    _write_day_file(tmp_path, _RECORDS_V1)
    engine, session = _open(tmp_path)
    try:
        summary = fill_stock_daily(_config(tmp_path), _DB(session))
    finally:
        session.close()

    assert summary['mode'] == 'incremental'
    assert summary['rows'] == 2
    assert summary['inserted'] == 2

    check = sessionmaker(bind=engine)()
    try:
        rows = _rows(check)
        assert [row.trade_date for row in rows] == [
            date(2024, 1, 2), date(2024, 1, 3)]
        assert rows[0].pre_close is None
        assert rows[1].pre_close == 10.0
        assert rows[1].change == 0.5
        assert rows[0].vol == 100.0      # 手
        assert rows[0].amount == 1000.0  # 元
    finally:
        check.close()
        engine.dispose()


def test_repeat_run_is_idempotent(tmp_path):
    """重复执行不新增行：库内水位已到最新交易日，整只股票被跳过。"""
    _write_day_file(tmp_path, _RECORDS_V1)
    engine, session = _open(tmp_path)
    try:
        fill_stock_daily(_config(tmp_path), _DB(session))
    finally:
        session.close()

    second_session = sessionmaker(bind=engine)()
    try:
        summary = fill_stock_daily(_config(tmp_path), _DB(second_session))
        count = second_session.query(StockDaily).count()
    finally:
        second_session.close()
        engine.dispose()

    assert summary['rows'] == 0
    assert summary['skipped_reasons']['no_new_rows'] == 1
    assert count == 2


def test_incremental_only_fills_after_watermark(tmp_path):
    """新增一根记录后，增量只补这一根，且 pre_close 接续水位前的收盘。"""
    _write_day_file(tmp_path, _RECORDS_V1)
    engine, session = _open(tmp_path)
    try:
        fill_stock_daily(_config(tmp_path), _DB(session))
    finally:
        session.close()

    _write_day_file(tmp_path, _RECORDS_V1 + [_RECORD_V2])
    second_session = sessionmaker(bind=engine)()
    try:
        summary = fill_stock_daily(_config(tmp_path), _DB(second_session))
    finally:
        second_session.close()

    check = sessionmaker(bind=engine)()
    try:
        rows = _rows(check)
        assert summary['rows'] == 1
        assert summary['inserted'] == 1
        assert len(rows) == 3
        assert rows[-1].trade_date == date(2024, 1, 4)
        assert rows[-1].pre_close == 10.5
    finally:
        check.close()
        engine.dispose()


def test_missing_file_is_counted_as_skipped(tmp_path):
    """本地缺 .day 文件时按 no_file 计数，不抛异常、不写库。"""
    (tmp_path / 'sh' / 'lday').mkdir(parents=True)
    engine, session = _open(tmp_path)
    try:
        summary = fill_stock_daily(
            _config(tmp_path), _DB(session), ts_codes=['600001.SH'])
        assert summary['skipped_reasons']['no_file'] == 1
        assert summary['rows'] == 0
    finally:
        session.close()
        engine.dispose()


def test_max_date_excludes_newer_record(tmp_path):
    """max_date 只写入不晚于目标日的记录，盘中行不入库。"""
    _write_day_file(tmp_path, _RECORDS_V1 + [_RECORD_V2])
    engine, session = _open(tmp_path)
    try:
        summary = fill_stock_daily(
            _config(tmp_path), _DB(session), max_date=date(2024, 1, 3))
        dates = [row.trade_date for row in _rows(session)]
    finally:
        session.close()
        engine.dispose()

    assert summary['rows'] == 2
    assert dates == [date(2024, 1, 2), date(2024, 1, 3)]


def test_since_bounded_backfill_keeps_pre_close(tmp_path):
    """since 有界回填：首行的 pre_close 仍取自 since 前一根，不置空。"""
    _write_day_file(tmp_path, _RECORDS_V1 + [_RECORD_V2])
    engine, session = _open(tmp_path)
    try:
        summary = fill_stock_daily(
            _config(tmp_path), _DB(session), since=date(2024, 1, 3))
        data = [(row.trade_date, row.pre_close) for row in _rows(session)]
    finally:
        session.close()
        engine.dispose()

    assert summary['rows'] == 2
    assert [d for d, _ in data] == [date(2024, 1, 3), date(2024, 1, 4)]
    assert data[0][1] == 10.0
    assert data[1][1] == 10.5

