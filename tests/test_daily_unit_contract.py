#!/usr/bin/env python3

"""K 线单位契约：vol = 手（1 手 = 100 股），amount = 元。

覆盖三个收口点，防止再次出现「一处写手、一处写股」：
1. TDX .day 解析：原始 uint32 为「股」，解析输出必须折成「手」；
2. upsert 写入：原样落库，不做任何换算；
3. DataFetcher 读库：vol 直映射为 volume，不做二次换算。
"""

import struct
from datetime import date
from types import SimpleNamespace

import polars as pl
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from src.data.managers.data_fetcher import DataFetcher
from src.data.sources.base import DataSourceBase
from src.data.tdx_day_parser import parse_day_file
from src.data.tdx_handler import TdxHandler
from src.database.models.stock import StockDaily
from src.database.upsert import upsert_rows


def _write_day_file(path, records):
    """按真实 .day 布局写记录：volume 参数以「手」给出，落盘 uint32 = 手 * 100 股。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'wb') as f:
        for day, open_, high, low, close, volume, amount in records:
            f.write(struct.pack(
                "<IIIIIfII",
                int(day), int(open_ * 100), int(high * 100), int(low * 100),
                int(close * 100), float(amount), int(volume * 100), 0,
            ))


def _handler(tmp_path, db_manager=None):
    config = SimpleNamespace(data=SimpleNamespace(
        tdx_data_path=str(tmp_path), max_workers=1))
    return TdxHandler(config, db_manager)


@pytest.fixture
def day_file(tmp_path):
    # 120 手 = 12,000 股，均价 10 元 → 成交额 120,000 元
    records = [
        ("20240102", 10.0, 10.5, 9.8, 10.0, 120, 120_000.0),
        ("20240103", 10.0, 10.8, 9.9, 10.5, 150, 157_500.0),
    ]
    path = tmp_path / "sh" / "lday" / "sh600000.day"
    _write_day_file(path, records)
    return path


def test_parse_day_file_outputs_hand_and_yuan(day_file):
    """解析层：uint32 股的 12,000 必须输出 120 手，成交额保持元。"""
    df = parse_day_file(day_file)

    first = df.sort('date').row(0, named=True)
    assert first['volume'] == pytest.approx(120.0)      # 手
    assert first['amount'] == pytest.approx(120_000.0)  # 元


def test_tdx_handler_kline_outputs_hand_and_yuan(tmp_path, day_file):
    """TDX handler 归一化后仍是手/元，不叠加第二次换算。"""
    handler = _handler(tmp_path)
    try:
        df = handler.get_kline_data(
            "600000.SH", "2024-01-02", "2024-01-03", adjust="none").collect()
    finally:
        handler.close()

    first = df.sort('date').row(0, named=True)
    assert first['volume'] == pytest.approx(120.0)
    assert first['amount'] == pytest.approx(120_000.0)


@pytest.mark.parametrize(('volume_unit', 'raw_volume', 'expected'), [
    ('share', 12_000.0, 120.0),   # 股 → 手，只除一次
    ('hand', 120.0, 120.0),       # 已是手，不再换算
])
def test_normalize_kline_converts_volume_exactly_once(volume_unit, raw_volume, expected):
    """归一化收口点：按 volume_unit 决定是否 /100，且只执行一次。"""
    raw = pl.DataFrame({
        'date': [date(2024, 1, 2)],
        'open': [10.0], 'high': [10.5], 'low': [9.8], 'close': [10.0],
        'volume': [raw_volume], 'amount': [120_000.0],
    })
    normalized = DataSourceBase._normalize_kline(
        raw, column_map={}, volume_unit=volume_unit)

    assert normalized['volume'][0] == pytest.approx(expected)
    assert normalized['amount'][0] == pytest.approx(120_000.0)


def test_upsert_writes_hand_and_yuan_without_conversion(tmp_path, day_file):
    """写入层：解析结果原样落库，vol=手、amount=元。"""
    parsed = parse_day_file(day_file).sort('date')
    rows = [
        {
            'ts_code': '600000.SH',
            'trade_date': row['date'],
            'open': row['open'], 'high': row['high'],
            'low': row['low'], 'close': row['close'],
            'vol': row['volume'], 'amount': row['amount'],
        }
        for row in parsed.iter_rows(named=True)
    ]

    engine = create_engine(f'sqlite:///{tmp_path / "units.db"}')
    StockDaily.__table__.create(engine)
    session = sessionmaker(bind=engine)()
    try:
        upsert_rows(
            session, StockDaily, rows,
            key_fields=('ts_code', 'trade_date'),
            value_fields=('open', 'high', 'low', 'close', 'vol', 'amount'),
        )
        session.commit()

        stored = session.execute(
            select(StockDaily).where(StockDaily.trade_date == date(2024, 1, 2))
        ).scalar_one()
        assert stored.vol == pytest.approx(120.0)
        assert stored.amount == pytest.approx(120_000.0)
    finally:
        session.close()
        engine.dispose()


def test_database_read_does_not_rescale_volume(tmp_path):
    """读路径：DB 里的 120 手读出仍是 120，不乘 100 也不除 100。"""
    engine = create_engine(f'sqlite:///{tmp_path / "read.db"}')
    StockDaily.__table__.create(engine)
    session = sessionmaker(bind=engine)()
    try:
        session.add(StockDaily(
            ts_code='600000.SH', trade_date=date(2024, 1, 2),
            open=10.0, high=10.5, low=9.8, close=10.0, pre_close=10.0,
            change=0.0, pct_chg=0.0, vol=120.0, amount=120_000.0,
            qfq_factor=1.0, hfq_factor=1.0,
        ))
        session.commit()

        db_manager = SimpleNamespace(
            is_connected=lambda: True, engine=engine)
        fetcher = DataFetcher(SimpleNamespace(), db_manager)
        df = fetcher._get_data_from_database(
            'stock', '600000.SH', '2024-01-02', '2024-01-02', '1d')

        assert df.height == 1
        assert df['volume'][0] == pytest.approx(120.0)
        assert df['amount'][0] == pytest.approx(120_000.0)
    finally:
        session.close()
        engine.dispose()


def _baostock_daily_frame(volume_shares: str) -> pl.DataFrame:
    """构造 Baostock 原始日线行（上游字段值均为字符串，volume 单位为股）。"""
    return pl.DataFrame({
        'date': ['2024-01-02'],
        'open': ['10.0'], 'high': ['10.5'], 'low': ['9.8'], 'close': ['10.0'],
        'preclose': ['9.9'], 'volume': [volume_shares],
        'amount': ['120000.0'], 'pctChg': ['1.0'],
    })


def test_baostock_daily_record_is_hand_before_write():
    """Baostock 上游 12,000 股必须折为 120 手再进入 stock_daily。"""
    from src.data.sources.baostock.stock import BaostockStockMixin

    rows, _ = BaostockStockMixin._build_stock_daily_rows(
        _baostock_daily_frame('12000'), '600000.SH')

    assert rows[0]['vol'] == pytest.approx(120.0)          # 手
    assert rows[0]['amount'] == pytest.approx(120_000.0)   # 元


def test_baostock_daily_upsert_does_not_divide_twice(tmp_path):
    """归一化只在记录构造处执行一次，upsert 写库后仍是 120 手。"""
    from src.data.sources.baostock.stock import BaostockStockMixin

    rows, _ = BaostockStockMixin._build_stock_daily_rows(
        _baostock_daily_frame('12000'), '600000.SH')

    engine = create_engine(f'sqlite:///{tmp_path / "baostock.db"}')
    StockDaily.__table__.create(engine)
    session = sessionmaker(bind=engine)()
    try:
        upsert_rows(
            session, StockDaily, rows,
            key_fields=('ts_code', 'trade_date'),
            value_fields=('open', 'high', 'low', 'close', 'pre_close',
                          'change', 'pct_chg', 'vol', 'amount'),
        )
        session.commit()

        stored = session.execute(select(StockDaily)).scalar_one()
        assert stored.vol == pytest.approx(120.0)
        assert stored.amount == pytest.approx(120_000.0)
    finally:
        session.close()
        engine.dispose()


def test_baostock_minute_record_is_hand():
    """Baostock 分钟线同样只做一次 股→手 换算。"""
    from src.data.sources import baostock_schema

    record = baostock_schema.minute_row_to_record({
        'time': '20240102093500000',
        'open': '10.0', 'high': '10.2', 'low': '9.9', 'close': '10.1',
        'volume': '12000', 'amount': '120000.0',
    }, '600000.SH')

    assert record['vol'] == pytest.approx(120.0)
    assert record['amount'] == pytest.approx(120_000.0)


def test_index_daily_amount_column_is_double():
    """指数成交额量级可达 1e12，模型必须用 Double（8 字节）而非 Float。"""
    from sqlalchemy import Double

    from src.database.models.index import IndexDaily

    assert isinstance(IndexDaily.__table__.c.amount.type, Double)


def test_index_daily_price_and_vol_columns_are_double():
    """指数点位与成交量同样用 Double，避免 float32 尾差进入比对与回测。"""
    from sqlalchemy import Double

    from src.database.models.index import IndexDaily

    for name in ('open', 'high', 'low', 'close', 'pre_close',
                 'change', 'pct_chg', 'vol'):
        assert isinstance(IndexDaily.__table__.c[name].type, Double), name


def test_akshare_index_daily_missing_amount_is_null(tmp_path):
    """AkShare 指数日线不提供成交额：新插入行必须为 NULL，不得写 0 冒充。"""
    from src.data.sources.akshare.index import AkShareIndexMixin
    from src.database.models.index import IndexDaily

    engine = create_engine(f'sqlite:///{tmp_path / "index_amount.db"}')
    IndexDaily.__table__.create(engine)
    session = sessionmaker(bind=engine)()
    try:
        mixin = SimpleNamespace(session=session)
        AkShareIndexMixin._persist_akshare_index_daily(mixin, '000001.SH', [{
            'ts_code': '000001.SH', 'trade_date': date(2024, 1, 2),
            'open': 3000.0, 'high': 3010.0, 'low': 2990.0,
            'close': 3005.0, 'vol': 120.0,
        }])

        stored = session.execute(select(IndexDaily)).scalar_one()
        assert stored.amount is None
        assert stored.vol == pytest.approx(120.0)
    finally:
        session.close()
        engine.dispose()
