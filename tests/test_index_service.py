"""IndexService 单元测试。"""

import struct

import pytest

from src.data.services.index_service import IndexService


def _write_day_file(path, close, preclose):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        for day, price in (("20240102", preclose), ("20240103", close)):
            f.write(struct.pack(
                "<IIIIIIII",
                int(day), int(price * 100), int(price * 100), int(price * 100),
                int(price * 100), 100, 100000, 0,
            ))


@pytest.fixture()
def tdx_tree(tmp_path):
    _write_day_file(tmp_path / "sh" / "lday" / "sh000001.day", 3100.0, 3000.0)
    _write_day_file(tmp_path / "sz" / "lday" / "sz399001.day", 9800.0, 9700.0)
    _write_day_file(tmp_path / "bj" / "lday" / "bj899050.day", 1200.0, 1180.0)
    return tmp_path


def test_list_index_quotes_all_markets(tdx_tree):
    service = IndexService(str(tdx_tree), db_manager=None)

    quotes = service.list_index_quotes()

    assert [quote["ts_code"] for quote in quotes] == ["000001.SH", "399001.SZ", "899050.BJ"]
    assert quotes[0]["name"] == "000001"
    assert quotes[0]["close"] == 3100.0
    assert quotes[0]["preclose"] == 3000.0
    assert quotes[0]["pct_chg"] == pytest.approx(100 / 3000 * 100)


def test_list_index_quotes_market_filter(tdx_tree):
    service = IndexService(str(tdx_tree), db_manager=None)

    quotes = service.list_index_quotes(market_filter="sh")

    assert len(quotes) == 1
    assert quotes[0]["ts_code"] == "000001.SH"


def test_list_index_quotes_category_filter_without_db(tdx_tree):
    service = IndexService(str(tdx_tree), db_manager=None)

    assert service.list_index_quotes(category_filter="宽基指数") == []


def test_resolve_ts_code_numeric_fallback():
    service = IndexService(None, db_manager=None)

    assert service.resolve_ts_code("000001") == "000001.SH"
    assert service.resolve_ts_code("399006") == "399006.SZ"
    assert service.resolve_ts_code("899050") == "899050.BJ"
    assert service.resolve_ts_code("399006.SZ") == "399006.SZ"


def test_normalize_index_name():
    assert IndexService.normalize_index_name("沪深300指数") == "沪深300"
    assert IndexService.normalize_index_name("上证 50（成份指数）") == "上证50"
    assert IndexService.normalize_index_name("") == ""


def test_list_index_quotes_without_tdx_path():
    service = IndexService(None, db_manager=None)

    assert service.list_index_quotes() == []
