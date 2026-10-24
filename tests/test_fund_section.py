"""FundSection 基金板块单元测试。"""

import struct

from src.ui.fund_section import (FundSection, _default_fund_data,
                                 is_closed_fund_ts_code, is_etf_fund_ts_code)


def test_is_etf_fund_ts_code():
    assert is_etf_fund_ts_code("510300.SH")
    assert is_etf_fund_ts_code("520500.SH")
    assert is_etf_fund_ts_code("560010.SH")
    assert is_etf_fund_ts_code("588000.SH")
    assert is_etf_fund_ts_code("159915.SZ")
    assert not is_etf_fund_ts_code("500001.SH")   # 封闭式基金段
    assert not is_etf_fund_ts_code("600519.SH")   # 股票
    assert not is_etf_fund_ts_code("000001.SZ")
    assert not is_etf_fund_ts_code("")
    assert not is_etf_fund_ts_code("510300")


def test_is_closed_fund_ts_code():
    assert is_closed_fund_ts_code("500001.SH")
    assert is_closed_fund_ts_code("500038.SH")
    assert not is_closed_fund_ts_code("510300.SH")  # ETF 段排除
    assert not is_closed_fund_ts_code("159915.SZ")  # 深市
    assert not is_closed_fund_ts_code("600519.SH")
    assert not is_closed_fund_ts_code("")
    assert not is_closed_fund_ts_code("500001")


def test_default_fund_data_fields():
    data = _default_fund_data("510300", "沪深300ETF")
    assert data["code"] == "510300"
    assert data["name"] == "沪深300ETF"
    assert data["price"] == "0.00"
    assert data["amount"] == "0.00亿"
    assert len(data) == 13


def _write_day_file(path, close, preclose):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        for day, price in (("20240102", preclose), ("20240103", close)):
            f.write(struct.pack(
                "<IIIIIIII",
                int(day), int(price * 100), int(price * 100), int(price * 100),
                int(price * 100), 100, 100000000, 0,
            ))


def test_get_real_fund_data_from_tdx(tmp_path):
    from types import SimpleNamespace

    _write_day_file(tmp_path / "sh" / "lday" / "sh510300.day", 4.2, 4.0)
    window = SimpleNamespace(
        data_manager=SimpleNamespace(
            config=SimpleNamespace(data=SimpleNamespace(tdx_data_path=str(tmp_path)))
        )
    )
    section = FundSection(window)

    data = section.get_real_fund_data("510300", "沪深300ETF", "510300.SH")

    assert data is not None
    assert data["code"] == "510300"
    assert data["price"] == "4.20"
    assert data["preclose"] == "4.00"
    assert data["change"] == "+0.20"
    assert data["change_pct"] == "+5.00%"
    assert data["volume"] == "100"
    assert data["amount"] == "0.01亿"


def test_get_real_fund_data_missing_file(tmp_path):
    from types import SimpleNamespace

    window = SimpleNamespace(
        data_manager=SimpleNamespace(
            config=SimpleNamespace(data=SimpleNamespace(tdx_data_path=str(tmp_path)))
        )
    )
    section = FundSection(window)

    assert section.get_real_fund_data("510300", "沪深300ETF") is None


def test_get_real_fund_data_without_data_manager():
    from types import SimpleNamespace

    section = FundSection(SimpleNamespace(data_manager=None))
    assert section.get_real_fund_data("510300", "沪深300ETF") is None
