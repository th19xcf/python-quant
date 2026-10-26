"""DividendService 除权数据服务单元测试。"""

import datetime

import polars as pl

from src.data.services.dividend_service import (
    DividendService,
    filter_dividends_by_dates,
    format_stock_code,
)


def test_format_stock_code():
    assert format_stock_code("600519.SH") == "600519.SH"
    assert format_stock_code("sh600519") == "600519.SH"
    assert format_stock_code("sz000001") == "000001.SZ"
    assert format_stock_code("600519") == "600519.SH"
    assert format_stock_code("000001") == "000001.SZ"
    assert format_stock_code("300750") == "300750.SZ"


def test_filter_dividends_by_dates():
    dividends = [
        {"ex_date": datetime.date(2024, 1, 3), "cash_div": 0.5},
        {"ex_date": datetime.date(2024, 6, 1), "cash_div": 0.8},
        {"ex_date": None, "cash_div": 0.1},
    ]
    kline_dates = [datetime.datetime(2024, 1, 3), datetime.date(2024, 1, 4)]

    filtered = filter_dividends_by_dates(dividends, kline_dates)
    assert len(filtered) == 1
    assert filtered[0]["cash_div"] == 0.5


def test_filter_dividends_empty_dates():
    assert filter_dividends_by_dates([{"ex_date": datetime.date(2024, 1, 3)}], []) == []


def test_filter_dividends_skips_unrecognized_dates():
    dividends = [{"ex_date": datetime.date(2024, 1, 3)}]
    # 纯字符串无 date/strftime/astype 属性会被丢弃 → 过滤结果为空
    assert filter_dividends_by_dates(dividends, ["2024-01-03"]) == []
    # datetime 字符串经由 ex_date 匹配不到也不返回
    assert filter_dividends_by_dates(dividends, [datetime.date(2024, 1, 3)])


class _FakeDataManager:
    def __init__(self, df):
        self._df = df
        self.queried_with = None

    def get_stock_dividend(self, ts_code):
        self.queried_with = ts_code
        return self._df


def test_service_formats_code_and_filters():
    df = pl.DataFrame({
        "ex_date": [datetime.date(2024, 1, 3), datetime.date(2024, 6, 1)],
        "record_date": [datetime.date(2024, 1, 2), datetime.date(2024, 5, 30)],
        "cash_div": [0.5, 0.8],
    })
    dm = _FakeDataManager(df)
    service = DividendService(dm)

    result = service.get_dividends_in_range("600519", [datetime.date(2024, 1, 3)])

    assert dm.queried_with == "600519.SH"
    assert len(result) == 1
    assert result[0]["cash_div"] == 0.5
    # 输出列按可用列裁剪：无 pay_date/share_div 列时自动忽略
    assert "ex_date" in result[0] and "record_date" in result[0]


def test_service_returns_empty_without_data_manager():
    assert DividendService(None).get_dividends_in_range("600519", [1]) == []


def test_service_returns_empty_on_empty_frame():
    df = pl.DataFrame({"ex_date": [], "cash_div": []})
    assert DividendService(_FakeDataManager(df)).get_dividends_in_range("600519", [1]) == []
