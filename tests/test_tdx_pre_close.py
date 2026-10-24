"""通达信 get_stock_data 的昨收/涨跌衍生列测试。"""

import struct
from datetime import date
from types import SimpleNamespace

import polars as pl
import pytest

from src.data.tdx_handler import TdxHandler


def _write_day_file(path, records):
    """按 32 字节定长格式写入记录：(date, open, high, low, close, volume, amount)。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        for day, open_, high, low, close, volume, amount in records:
            f.write(struct.pack(
                "<IIIIIIII",
                int(day), int(open_ * 100), int(high * 100), int(low * 100),
                int(close * 100), volume, int(amount * 100), 0,
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

    def fake_factors(ts_code, start_date=None, end_date=None):
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


def test_amount_keeps_raw_convention_in_kline_path(tmp_path, day_file):
    """get_kline_data 的成交额保留原始字段值（amount_scale=1.0）。"""
    handler = _handler(tmp_path)
    try:
        df = handler.get_kline_data(
            "600000.SH", "2024-01-02", "2024-01-04", adjust="none").collect()
    finally:
        handler.close()

    # 文件写入时 amount*100，原始字段为 1000*100=100000
    assert df.sort("date")["amount"][0] == pytest.approx(100000.0)


def test_missing_file_returns_empty(tmp_path):
    handler = _handler(tmp_path)
    try:
        df = handler.get_stock_data("600001.SH", "2024-01-03", "2024-01-04")
    finally:
        handler.close()

    assert df.is_empty()
