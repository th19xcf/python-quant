"""sync_service 纯函数回归基线。

sync_service 承担股票/指数/基金三类同步编排，此前无任何测试覆盖。
本文件只锁定其中不依赖数据库、网络与外部数据源的纯函数行为，
供后续拆分模块时作为行为不变的回归网。
"""

from datetime import date, datetime
from types import SimpleNamespace

import polars as pl
import pytest

from src.data.services import sync_service as ss

# ----------------------------------------------------------------------
# 代码判定
# ----------------------------------------------------------------------

@pytest.mark.parametrize("ts_code, expected", [
    ("000001.SH", True),
    ("880001.SH", True),
    ("399001.SZ", True),
    ("884001.BJ", True),
    ("899050.BJ", True),
    ("600000.SH", False),
    ("000001.SZ", False),
    ("300001.SZ", False),
    ("920001.BJ", False),
    ("600000", False),
    ("", False),
])
def test_is_index_ts_code(ts_code, expected):
    assert ss._is_index_ts_code(ts_code) is expected


@pytest.mark.parametrize("ts_code, expected", [
    ("510300.SH", True),
    ("520000.SH", True),
    ("588000.SH", True),
    ("159915.SZ", True),
    ("161725.SZ", True),
    ("600000.SH", False),
    ("000001.SZ", False),
    ("300001.SZ", False),
    ("920001.BJ", False),
])
def test_is_fund_ts_code_by_symbol(ts_code, expected):
    assert ss._is_fund_ts_code(ts_code) is expected


def test_is_fund_ts_code_by_market_and_name():
    # 数据源可能返回中文或英文市场字段，需归一化后判定
    assert ss._is_fund_ts_code("000001.SZ", market="基金") is True
    assert ss._is_fund_ts_code("000001.SZ", market="fund") is True
    assert ss._is_fund_ts_code("000001.SZ", name="沪深300ETF") is True
    assert ss._is_fund_ts_code("000001.SZ", name="某某LOF") is True
    assert ss._is_fund_ts_code("000001.SZ", name="平安银行") is False


@pytest.mark.parametrize("raw, expected", [
    ("600000", "600000.SH"),
    ("sh.600000", "600000.SH"),
    ("000001.SZ", "000001.SZ"),
    ("510300", "510300.SH"),
    ("920181", "920181.BJ"),
    ("abc", None),
    ("600000.XX", None),
    (None, None),
    ("", None),
])
def test_to_ts_code_or_none(raw, expected):
    assert ss._to_ts_code_or_none(raw) == expected


# ----------------------------------------------------------------------
# 字段归一化
# ----------------------------------------------------------------------

@pytest.mark.parametrize("raw, expected", [
    ("L", "L"), ("1", "L"), ("listed", "L"),
    ("D", "D"), ("0", "D"), ("delisted", "D"),
    ("P", "P"), ("SUSPEND", "P"), ("pause", "P"),
    ("未知状态", "L"), (None, "L"), ("", "L"),
])
def test_normalize_stock_status(raw, expected):
    assert ss._normalize_stock_status(raw) == expected


@pytest.mark.parametrize("ts_code, expected", [
    ("688001.SH", "科创板"),
    ("600000.SH", "主板"),
    ("300001.SZ", "创业板"),
    ("002001.SZ", "中小板"),
    ("000001.SZ", "主板"),
    ("920001.BJ", "北交所"),
    ("600000", ""),
    (None, ""),
])
def test_infer_stock_market_from_ts_code(ts_code, expected):
    assert ss._infer_stock_market_from_ts_code(ts_code) == expected


def test_normalize_text_value():
    assert ss._normalize_text_value(None) == ""
    assert ss._normalize_text_value("  ") == ""
    assert ss._normalize_text_value("  平安银行 ") == "平安银行"
    assert ss._normalize_text_value("abcdef", max_len=3) == "abc"
    assert ss._normalize_text_value(123) == "123"


def test_normalize_date_value():
    assert ss._normalize_date_value(None) is None
    assert ss._normalize_date_value(datetime(2024, 1, 2, 9, 30)) == date(2024, 1, 2)
    assert ss._normalize_date_value(date(2024, 1, 2)) == date(2024, 1, 2)
    assert ss._normalize_date_value("2024-01-02") == date(2024, 1, 2)
    assert ss._normalize_date_value("2024/01/02") == date(2024, 1, 2)
    assert ss._normalize_date_value("20240102") == date(2024, 1, 2)
    assert ss._normalize_date_value(20240102) == date(2024, 1, 2)
    for blank in ("0000-00-00", "None", "nan", "NaT", "", "not-a-date"):
        assert ss._normalize_date_value(blank) is None


def test_update_stock_text_field_counts_fill_and_refresh():
    row = SimpleNamespace(name=None)
    result = {"filled_blank_fields": 0, "refreshed_fields": 0}

    assert ss._update_stock_text_field(row, "name", "平安银行", result) is True
    assert row.name == "平安银行"
    assert result == {"filled_blank_fields": 1, "refreshed_fields": 0}

    # 同值不改
    assert ss._update_stock_text_field(row, "name", "平安银行", result) is False
    assert result == {"filled_blank_fields": 1, "refreshed_fields": 0}

    # 已有值变化计为刷新
    assert ss._update_stock_text_field(row, "name", "浦发银行", result) is True
    assert row.name == "浦发银行"
    assert result == {"filled_blank_fields": 1, "refreshed_fields": 1}

    # 空值不覆盖
    assert ss._update_stock_text_field(row, "name", "", result) is False
    assert row.name == "浦发银行"


def test_update_stock_date_field_counts_fill_and_refresh():
    row = SimpleNamespace(list_date=None)
    result = {"filled_blank_fields": 0, "refreshed_fields": 0}

    assert ss._update_stock_date_field(row, "list_date", "1999-11-10", result) is True
    assert row.list_date == date(1999, 11, 10)
    assert result == {"filled_blank_fields": 1, "refreshed_fields": 0}

    assert ss._update_stock_date_field(row, "list_date", None, result) is False
    assert ss._update_stock_date_field(row, "list_date", "1999-11-10", result) is False

    assert ss._update_stock_date_field(row, "list_date", "2000-01-01", result) is True
    assert row.list_date == date(2000, 1, 1)
    assert result == {"filled_blank_fields": 1, "refreshed_fields": 1}


# ----------------------------------------------------------------------
# 基本信息合并
# ----------------------------------------------------------------------

def test_build_stock_info_accepts_source_field_aliases():
    info = ss._build_stock_info(
        "600000.SH", "sh.600000",
        {
            "code_name": "浦发银行",
            "ipoDate": "1999-11-10",
            "outDate": "",
            "industry": "银行",
            "area": "上海",
            "status": "1",
        },
        "baostock",
    )

    assert info == {
        "symbol": "sh.600000",
        "name": "浦发银行",
        "area": "上海",
        "industry": "银行",
        "market": "",
        "list_date": "1999-11-10",
        "delist_date": None,
        "status": "1",
        "source": "baostock",
    }


def test_merge_stock_basic_baostock_only():
    baostock_df = pl.DataFrame({
        "code": ["sh.600000", "sz.000001"],
        "code_name": ["浦发银行", "平安银行"],
        "ipoDate": ["1999-11-10", "1991-04-03"],
        "outDate": ["", ""],
        "industry": ["银行", "银行"],
    })

    merged = ss.merge_stock_basic(baostock_df, None)

    assert set(merged) == {"600000.SH", "000001.SZ"}
    assert merged["600000.SH"]["name"] == "浦发银行"
    assert merged["600000.SH"]["source"] == "baostock"


def test_merge_stock_basic_akshare_fills_missing_fields():
    baostock_df = pl.DataFrame({
        "code": ["sh.600000"],
        "code_name": ["浦发银行"],
        "ipoDate": ["1999-11-10"],
        "industry": [""],
    })
    akshare_df = pl.DataFrame({
        "code": ["600000"],
        "name": ["浦发银行"],
        "area": ["上海"],
        "industry": ["银行"],
        "上市日期": ["1999-11-10"],
    })

    merged = ss.merge_stock_basic(baostock_df, akshare_df)

    assert merged["600000.SH"]["area"] == "上海"
    assert merged["600000.SH"]["industry"] == "银行"
    # Baostock 优先，来源标记保留
    assert merged["600000.SH"]["source"] == "baostock"


def test_merge_stock_basic_akshare_only_uses_raw_symbol():
    akshare_df = pl.DataFrame({
        "code": ["600001"],
        "name": ["新股票"],
        "area": [""],
        "industry": ["行业A"],
    })

    merged = ss.merge_stock_basic(None, akshare_df)

    assert merged["600001.SH"]["symbol"] == "600001"
    assert merged["600001.SH"]["source"] == "akshare"
    assert merged["600001.SH"]["status"] == "L"


def test_merge_stock_basic_filters_existing_codes():
    baostock_df = pl.DataFrame({
        "code": ["sh.600000", "sz.000001"],
        "code_name": ["浦发银行", "平安银行"],
    })

    merged = ss.merge_stock_basic(baostock_df, None, existing_codes={"600000.SH"})

    assert list(merged) == ["000001.SZ"]


def test_get_stock_info_from_apis_converts_string_date():
    merged_basic = {
        "600000.SH": {"symbol": "600000", "name": "浦发银行", "list_date": "1999-11-10"},
    }

    info = ss.get_stock_info_from_apis("600000.SH", merged_basic, None, None)

    assert info["ts_code"] == "600000.SH"
    assert info["list_date"] == date(1999, 11, 10)
    # 返回副本，不污染合并结果
    assert merged_basic["600000.SH"]["list_date"] == "1999-11-10"


def test_get_stock_info_from_apis_falls_back_to_symbol_match():
    merged_basic = {
        "000001.SH": {"symbol": "000001", "name": "上证指数"},
    }

    info = ss.get_stock_info_from_apis("000001.SZ", merged_basic, None, None)

    assert info["ts_code"] == "000001.SZ"
    assert info["name"] == "上证指数"


def test_get_stock_info_from_apis_returns_none_when_absent():
    assert ss.get_stock_info_from_apis("600000.SH", {}, None, None) is None


def test_create_stock_basic_from_info_truncates_and_defaults():
    stock_info = {"name": "长" * 50, "symbol": "s" * 12}
    stock = ss._create_stock_basic_from_info("600000.SH", stock_info)

    assert stock.ts_code == "600000.SH"
    assert stock.name == "长" * 45
    assert stock.symbol == "s" * 9
    assert stock.status == "L"


# ----------------------------------------------------------------------
# 指数与基金
# ----------------------------------------------------------------------

@pytest.mark.parametrize("raw, expected", [
    ("sh.000001", "000001.SH"),
    ("000001.SH", "000001.SH"),
    ("000001", "000001.SH"),
    ("399001", "399001.SZ"),
    ("884001", "884001.BJ"),
    ("899050", "899050.BJ"),
    ("600000", None),
    ("0000010", None),
    ("", None),
    (None, None),
])
def test_normalize_index_ts_code(raw, expected):
    assert ss._normalize_index_ts_code(raw) == expected


def test_parse_index_date():
    assert ss._parse_index_date("2024-01-02") == date(2024, 1, 2)
    assert ss._parse_index_date("2024/01/02") == date(2024, 1, 2)
    assert ss._parse_index_date("20240102") == date(2024, 1, 2)
    for blank in (None, "", "0000-00-00", "NaT", "not-a-date"):
        assert ss._parse_index_date(blank) is None


@pytest.mark.parametrize("ts_code, expected_publisher", [
    ("000001.SH", "上交所"),
    ("399001.SZ", "深交所"),
    ("884001.BJ", "北交所"),
    ("000001", "交易所"),
])
def test_infer_index_meta_publisher(ts_code, expected_publisher):
    meta = ss._infer_index_meta(ts_code, "")
    assert meta["publisher"] == expected_publisher


@pytest.mark.parametrize("name, expected", [
    ("证监会行业指数", ("行业指数", "行业")),
    ("主题指数", ("主题指数", "主题")),
    ("中证红利", ("策略指数", "红利")),
    ("创业板指", ("板块指数", "创业板")),
    ("科创50", ("板块指数", "科创板")),
    ("北证50", ("板块指数", "北交所")),
    ("上证50", ("综合指数", "宽基")),
    ("自定义指数", ("综合指数", "通用")),
    (None, ("综合指数", "通用")),
])
def test_infer_index_meta_type_and_category(name, expected):
    meta = ss._infer_index_meta("000001.SH", name)
    assert (meta["index_type"], meta["category"]) == expected
    assert meta["weight_rule"] == "市值加权"


@pytest.mark.parametrize("symbol, name, expected", [
    ("510300", "沪深300ETF", "ETF"),
    ("161725", "招商中证白酒", "LOF"),
    ("000001", "某基金", "基金"),
])
def test_infer_fund_type(symbol, name, expected):
    assert ss._infer_fund_type(symbol, name) == expected


# ----------------------------------------------------------------------
# 通达信日线文件（委托统一解析入口）
# ----------------------------------------------------------------------

def _write_day_records(path, day_values):
    import struct

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        for day in day_values:
            f.write(struct.pack("<IIIIIfII", int(day), 1000, 1100, 900, 1050,
                                1000.0, 10000, 0))


def test_get_tdx_day_file_path_layout(tmp_path):
    config = SimpleNamespace(data=SimpleNamespace(tdx_data_path=str(tmp_path)))

    assert ss._get_tdx_day_file_path(config, "600000.SH") == tmp_path / "sh" / "lday" / "sh600000.day"
    assert ss._get_tdx_day_file_path(config, "000001.SZ") == tmp_path / "sz" / "lday" / "sz000001.day"
    assert ss._get_tdx_day_file_path(config, "920001.BJ") == tmp_path / "bj" / "lday" / "bj920001.day"
    assert ss._get_tdx_day_file_path(config, "600000") is None
    assert ss._get_tdx_day_file_path(config, "600000.XX") is None
    assert ss._get_tdx_day_file_path(config, None) is None


def test_extract_tdx_first_last_trade_date(tmp_path):
    config = SimpleNamespace(data=SimpleNamespace(tdx_data_path=str(tmp_path)))
    _write_day_records(
        tmp_path / "sh" / "lday" / "sh600000.day",
        ["20240102", "20240103", "20240104"],
    )

    assert ss._extract_tdx_first_last_trade_date(config, "600000.SH") == (
        date(2024, 1, 2), date(2024, 1, 4),
    )


def test_extract_tdx_first_last_trade_date_missing_file(tmp_path):
    config = SimpleNamespace(data=SimpleNamespace(tdx_data_path=str(tmp_path)))

    assert ss._extract_tdx_first_last_trade_date(config, "600000.SH") == (None, None)
