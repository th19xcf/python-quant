"""通达信 .day 解析器单元测试。"""

import struct
from datetime import date

import pytest

from src.data.tdx_day_parser import (build_day_file_path, parse_day_file,
                                     read_latest_quote, resolve_market)


def _write_day_file(path, records):
    """
    按 32 字节定长格式写入测试记录。

    字段布局与真实 .day 一致：@20 为 float32 成交额（元），
    @24 为 uint32 成交量（股）。records 中的 volume 以「手」给出，
    amount 以「元」给出。
    """
    with open(path, "wb") as f:
        for day, open_, high, low, close, volume, amount in records:
            f.write(struct.pack(
                "<IIIIIfII",
                int(day), int(open_ * 100), int(high * 100), int(low * 100),
                int(close * 100), float(amount), int(volume * 100), 0,
            ))


def test_read_latest_quote_with_previous_close(tmp_path):
    file_path = tmp_path / "sh000001.day"
    _write_day_file(file_path, [
        ("20240102", 10.0, 11.0, 9.0, 10.5, 100, 1000),
        ("20240103", 10.5, 12.0, 10.0, 11.0, 200, 2200),
    ])

    quote = read_latest_quote(file_path)

    assert quote is not None
    assert quote.date == "2024-01-03"
    assert quote.open == 10.5
    assert quote.high == 12.0
    assert quote.low == 10.0
    assert quote.close == 11.0
    assert quote.preclose == 10.5
    assert quote.volume == 200
    assert quote.amount == pytest.approx(2200.0)
    assert quote.change == pytest.approx(0.5)
    assert quote.pct_chg == pytest.approx(0.5 / 10.5 * 100)
    assert quote.amplitude == pytest.approx((12.0 - 10.0) / 10.5 * 100)


def test_read_latest_quote_single_record_has_zero_change(tmp_path):
    file_path = tmp_path / "sh600000.day"
    _write_day_file(file_path, [("20240103", 10.0, 10.5, 9.5, 10.25, 300, 3000)])

    quote = read_latest_quote(file_path)

    assert quote is not None
    assert quote.preclose == 10.25
    assert quote.change == 0.0
    assert quote.pct_chg == 0.0


def test_read_latest_quote_missing_file_returns_none(tmp_path):
    assert read_latest_quote(tmp_path / "not_exists.day") is None


def test_parse_day_file_respects_max_days(tmp_path):
    file_path = tmp_path / "sh000001.day"
    _write_day_file(file_path, [
        ("20240102", 10.0, 11.0, 9.0, 10.5, 100, 1000),
        ("20240103", 10.5, 12.0, 10.0, 11.0, 200, 2200),
    ])

    df = parse_day_file(file_path, max_days=1)

    assert df.height == 1
    assert df["date"][0] == date(2024, 1, 3)
    assert df["close"][0] == 11.0


def test_volume_amount_layout_matches_real_files(tmp_path):
    """
    锁定真实 .day 字段布局：@20 为 float32 成交额（元），
    @24 为 uint32 成交量（股）。两者比值应约等于当日均价。
    """
    price = 9.66
    shares = 46_256_248
    amount = price * shares
    path = tmp_path / "sh600000.day"
    with open(path, "wb") as f:
        f.write(struct.pack("<IIIIIfII", 20261021, 972, 976, 961, 966,
                            amount, shares, 0))

    df = parse_day_file(path)

    assert df["volume"][0] == pytest.approx(shares / 100)   # 股 → 手
    assert df["amount"][0] == pytest.approx(amount, rel=1e-6)
    assert df["amount"][0] / (df["volume"][0] * 100) == pytest.approx(price, rel=1e-6)


def test_parse_day_file_empty_file(tmp_path):
    file_path = tmp_path / "sh000001.day"
    file_path.write_bytes(b"")

    assert parse_day_file(file_path).height == 0


def test_resolve_market_by_ts_code_suffix():
    assert resolve_market("510300", "510300.SH") == "sh"
    assert resolve_market("510300", "510300.SZ") == "sz"
    assert resolve_market("899050", "899050.BJ") == "bj"


def test_resolve_market_by_code_prefix():
    assert resolve_market("510300") == "sh"
    assert resolve_market("159915") == "sz"
    assert resolve_market("899050") == "bj"


def test_build_day_file_path(tmp_path):
    path = build_day_file_path(tmp_path, "sh", "000001")
    assert path == tmp_path / "sh" / "lday" / "sh000001.day"
