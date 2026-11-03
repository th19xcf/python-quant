"""通达信 get_stock_data 的昨收/涨跌衍生列测试。"""

import struct
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.data.tdx_handler import TdxHandler


def _write_day_file(path, records):
    """
    按 32 字节定长格式写入记录：(date, open, high, low, close, volume, amount)。

    字段布局与真实 .day 一致：@20 为 float32 成交额（元），
    @24 为 uint32 成交量（股），volume 参数以「手」给出。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
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
    records = [
        ("20240102", 10.0, 10.5, 9.8, 10.0, 100, 1000),
        ("20240103", 10.0, 10.8, 9.9, 10.5, 120, 1260),
        ("20240104", 10.5, 11.0, 10.4, 10.8, 130, 1404),
    ]
    path = tmp_path / "sh" / "lday" / "sh600000.day"
    _write_day_file(path, records)
    return path


def test_first_row_pre_close_uses_lookback(tmp_path, day_file):
    """区间首根K线的昨收来自多取的前置交易日，不再为 0。"""
    handler = _handler(tmp_path)
    try:
        df = handler.get_stock_data("600000.SH", "2024-01-03", "2024-01-04")
    finally:
        handler.close()

    assert df.height == 2
    first = df.row(0, named=True)
    assert first["date"] == date(2024, 1, 3)
    assert first["pre_close"] == pytest.approx(10.0)
    assert first["change"] == pytest.approx(0.5)
    assert first["pct_chg"] == pytest.approx(5.0)


def test_no_extra_row_leaks_into_result(tmp_path, day_file):
    """为补齐昨收而多取的 2024-01-02 不得出现在返回结果中。"""
    handler = _handler(tmp_path)
    try:
        df = handler.get_stock_data("600000.SH", "2024-01-03", "2024-01-04")
    finally:
        handler.close()

    assert df["date"].to_list() == [date(2024, 1, 3), date(2024, 1, 4)]


def test_ex_dividend_pre_close_uses_factor_ratio(tmp_path, day_file, monkeypatch):
    """
    除权日昨收按 qfq 因子比值还原：
    pre_close = prev_close * f_{t-1} / f_t。
    除权日 f 从 1.0 降到 0.9，昨收应为 10.0 * 1.0 / 0.9。
    """
    handler = _handler(tmp_path)

    def fake_factors(ts_code, start_date=None, end_date=None, price_records=None):
        return pl.DataFrame({
            "trade_date": [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)],
            "qfq_factor": [1.0, 0.9, 0.9],
            "hfq_factor": [1.0, 1.0 / 0.9, 1.0 / 0.9],
        })

    monkeypatch.setattr(handler, "_get_adj_factors", fake_factors)
    try:
        df = handler.get_stock_data("600000.SH", "2024-01-03", "2024-01-03")
    finally:
        handler.close()

    row = df.row(0, named=True)
    assert row["pre_close"] == pytest.approx(10.0 * 1.0 / 0.9)
    assert row["change"] == pytest.approx(10.5 - 10.0 / 0.9)


def test_kline_path_uses_hand_and_yuan(tmp_path, day_file):
    """get_kline_data 输出成交量单位为手、成交额单位为元。"""
    handler = _handler(tmp_path)
    try:
        df = handler.get_kline_data(
            "600000.SH", "2024-01-02", "2024-01-04", adjust="none").collect()
    finally:
        handler.close()

    first = df.sort("date").row(0, named=True)
    assert first["volume"] == pytest.approx(100.0)   # 手
    assert first["amount"] == pytest.approx(1000.0)  # 元


def test_missing_file_returns_empty(tmp_path):
    handler = _handler(tmp_path)
    try:
        df = handler.get_stock_data("600001.SH", "2024-01-03", "2024-01-04")
    finally:
        handler.close()

    assert df.is_empty()


def _dividend(ex_date=None, share_div=0.0):
    """构造分红记录替身（只含复权因子计算用到的字段）。"""
    return SimpleNamespace(
        ts_code='600000.SH', ex_date=ex_date, cash_div=None,
        share_div=share_div, rights_issue_ratio=None, rights_issue_price=None,
    )


def test_dividend_frame_uses_explicit_float64(tmp_path, monkeypatch):
    """分红列显式 Float64：0.0 与 0.4 混合时不会被推断成 Int64"""
    from src.data.adj_factor_calculator import AdjFactorCalculator

    handler = _handler(tmp_path)
    captured = {}

    def fake_calculate(prices_df, dividends_df):
        captured['dividends'] = dividends_df
        return pl.DataFrame({
            'trade_date': prices_df['trade_date'],
            'qfq_factor': [1.0] * prices_df.height,
            'hfq_factor': [1.0] * prices_df.height,
        })

    monkeypatch.setattr(AdjFactorCalculator, 'calculate_factors', fake_calculate)
    try:
        handler._compute_adj_factor_frame(
            '600000.SH',
            [{'trade_date': date(2024, 1, 2), 'close': 10.0}],
            [_dividend(date(2024, 1, 2), share_div=0.0),
             _dividend(date(2024, 1, 3), share_div=0.4)],
        )
    finally:
        handler.close()

    dividends_df = captured['dividends']
    for column in ('cash_div', 'share_div', 'rights_issue_ratio', 'rights_issue_price'):
        assert dividends_df.schema[column] == pl.Float64
    assert dividends_df['share_div'].to_list() == [0.0, 0.4]


def test_adj_factor_frame_handles_mixed_float_values(tmp_path):
    """真实计算器：0.0/0.4 混合送转不再触发 Int64 构建异常"""
    handler = _handler(tmp_path)
    try:
        prices = [
            {'trade_date': date(2024, 1, 2), 'close': 10.0},
            {'trade_date': date(2024, 1, 3), 'close': 10.5},
            {'trade_date': date(2024, 1, 4), 'close': 11.0},
        ]
        result = handler._compute_adj_factor_frame(
            '600000.SH', prices,
            [_dividend(date(2024, 1, 3), share_div=0.0),
             _dividend(date(2024, 1, 4), share_div=0.4)],
        )
    finally:
        handler.close()

    assert result is not None
    assert result.height == len(prices)


def test_load_dividend_records_filters_null_ex_date_and_index(tmp_path):
    """分红查询只返回有除权日的个股记录，指数代码直接跳过"""
    from src.database.models.stock import StockDividend

    engine = create_engine(f'sqlite:///{tmp_path / "dividend.db"}')
    StockDividend.__table__.create(engine)
    session = sessionmaker(bind=engine)()
    session.add_all([
        StockDividend(ts_code='600000.SH', symbol='600000', name='浦发银行',
                      ex_date=date(2024, 1, 11), cash_div=0.5, share_div=0.0),
        StockDividend(ts_code='600000.SH', symbol='600000', name='浦发银行',
                      ex_date=None, cash_div=0.3, share_div=0.4),
        StockDividend(ts_code='000001.SH', symbol='000001', name='上证指数',
                      ex_date=date(2024, 2, 1), cash_div=0.5, share_div=0.0),
    ])
    session.commit()

    class _DB:
        def get_session(self):
            return session

    handler = _handler(tmp_path, _DB())
    try:
        records = handler._load_dividend_records('600000.SH')
        assert records is not None
        assert [record.ex_date for record in records] == [date(2024, 1, 11)]
        assert handler._load_dividend_records('000001.SH') is None
    finally:
        handler.close()
        session.close()
        engine.dispose()


def test_stock_and_index_lists_are_separated(tmp_path):
    """通达信扫描按资产类型分流：sh000xxx 指数不进入个股列表"""
    for market, stem in [
        ('sh', 'sh600000'), ('sh', 'sh000001'), ('sh', 'sh880001'),
        ('sz', 'sz000001'), ('sz', 'sz399001'),
    ]:
        path = tmp_path / market / 'lday' / f'{stem}.day'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b'')

    handler = _handler(tmp_path)
    try:
        assert sorted(handler.get_stock_list()) == ['000001.SZ', '600000.SH']
        assert sorted(handler.get_index_list()) == [
            '000001.SH', '399001.SZ', '880001.SH']
    finally:
        handler.close()


def test_stock_list_excludes_bse_convertible_bonds(tmp_path):
    """北交所定向可转债号段 810xxx 不得进入个股列表（810011 优机定转）"""
    for stem in ('bj810011', 'bj920001'):
        path = tmp_path / 'bj' / 'lday' / f'{stem}.day'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b'')

    handler = _handler(tmp_path)
    try:
        stocks = set(handler.get_stock_list())
        assert '810011.BJ' not in stocks
        assert '920001.BJ' in stocks
    finally:
        handler.close()


def test_stock_list_covers_chinext_301_and_fund_prefixes(tmp_path):
    """301 创业板新段与扩展基金前缀（50/52/56/58/15/16/18）进入个股列表"""
    for market, stem in [
        ('sz', 'sz301001'),
        ('sh', 'sh501001'), ('sh', 'sh520001'), ('sh', 'sh560001'),
        ('sh', 'sh588001'), ('sz', 'sz158000'), ('sz', 'sz180101'),
        ('sz', 'sz160105'),
    ]:
        path = tmp_path / market / 'lday' / f'{stem}.day'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b'')

    handler = _handler(tmp_path)
    try:
        stocks = set(handler.get_stock_list())
        assert '301001.SZ' in stocks
        for code in ['501001.SH', '520001.SH', '560001.SH', '588001.SH',
                     '158000.SZ', '180101.SZ', '160105.SZ']:
            assert code in stocks
    finally:
        handler.close()


def test_single_parse_shared_between_kline_and_adj_factors(tmp_path, day_file, monkeypatch):
    """K 线与复权因子复用同一份解析结果：单次取数只解析 .day 一次，且数值等价。"""
    from src.data import tdx_handler as tdx_module
    from src.database.models.stock import StockAdjFactor, StockDividend

    engine = create_engine(f'sqlite:///{tmp_path / "single_parse.db"}')
    StockDividend.__table__.create(engine)
    StockAdjFactor.__table__.create(engine)
    session = sessionmaker(bind=engine)()
    session.add(StockDividend(
        ts_code='600000.SH', symbol='600000', name='浦发银行',
        ex_date=date(2024, 1, 3), cash_div=1.0, share_div=0.0,
    ))
    session.commit()

    class _DB:
        def get_session(self):
            return session

    handler = _handler(tmp_path, _DB())
    calls = []
    original = tdx_module.read_recent_records

    def _counting(path, count=None, volume_in_hand=None):
        calls.append(Path(path).name)
        return original(path, count, volume_in_hand)

    monkeypatch.setattr(tdx_module, 'read_recent_records', _counting)
    try:
        df = handler.get_stock_data('600000.SH', '2024-01-03', '2024-01-03')
        assert calls == ['sh600000.day'], f'单次取数应只解析一次，实际 {calls}'

        records = original(day_file)
        factors_shared = handler._get_adj_factors(
            '600000.SH', None, None, price_records=records)
        assert calls == ['sh600000.day'], '传入 records 后不应再次解析文件'

        factors_reparsed = handler._get_adj_factors('600000.SH', None, None)
        assert calls == ['sh600000.day', 'sh600000.day']
    finally:
        handler.close()
        session.close()
        engine.dispose()

    # 数值等价：复用 records 与独立重解析得到完全相同的因子表
    assert factors_shared is not None
    assert factors_reparsed is not None
    assert factors_shared.equals(factors_reparsed)
    assert df.height == 1
