"""证券代码格式统一转换工具测试。"""

import pytest

from src.utils.code_converter import (
    infer_market,
    is_fund_code,
    parse_code,
    to_baostock_code,
    to_index_ts_code,
    to_symbol,
    to_tdx_file_stem,
    to_tdx_market,
    to_ts_code,
)


class TestParseCode:
    """parse_code 全格式解析"""

    @pytest.mark.parametrize("code,expected", [
        ("600000.SH", ("600000", "SH")),
        ("600000.sh", ("600000", "SH")),
        ("000001.SZ", ("000001", "SZ")),
        ("920181.BJ", ("920181", "BJ")),
    ])
    def test_standard_ts_code(self, code, expected):
        assert parse_code(code) == expected

    @pytest.mark.parametrize("code,expected", [
        ("sh.600000", ("600000", "SH")),
        ("SH.600000", ("600000", "SH")),
        ("sz.000001", ("000001", "SZ")),
        ("bj.920181", ("920181", "BJ")),
    ])
    def test_baostock_format(self, code, expected):
        assert parse_code(code) == expected

    @pytest.mark.parametrize("code,expected", [
        ("sh600000", ("600000", "SH")),
        ("SZ000001", ("000001", "SZ")),
        ("bj920181", ("920181", "BJ")),
    ])
    def test_tdx_file_stem_format(self, code, expected):
        assert parse_code(code) == expected

    @pytest.mark.parametrize("code,expected", [
        ("600000", ("600000", "SH")),
        ("900901", ("900901", "SH")),   # 沪市 B 股
        ("510880", ("510880", "SH")),   # 沪市 ETF
        ("159915", ("159915", "SZ")),   # 深市 ETF
        ("000001", ("000001", "SZ")),
        ("300001", ("300001", "SZ")),
        ("830799", ("830799", "BJ")),   # 北交所（原新三板）
        ("920001", ("920001", "BJ")),   # 北交所新股
        ("430047", ("430047", "BJ")),   # 北交所老股
    ])
    def test_bare_symbol_infer(self, code, expected):
        assert parse_code(code) == expected

    def test_920_with_sz_suffix_treated_as_bj(self):
        """920 开头北交所新股即使标注 .SZ 也判为 BJ（历史兼容）"""
        assert parse_code("920001.SZ") == ("920001", "BJ")

    @pytest.mark.parametrize("bad", [
        "", "abc", "60000", "6000000", "600000.XX", "xx.600000",
        "600000.SH.X", "sh.60000", None,
    ])
    def test_invalid_raises(self, bad):
        with pytest.raises((ValueError, TypeError)):
            parse_code(bad)


class TestInferMarket:
    @pytest.mark.parametrize("symbol,market", [
        ("600000", "SH"), ("688981", "SH"), ("900901", "SH"),
        ("510880", "SH"), ("588000", "SH"),
        ("000001", "SZ"), ("300750", "SZ"), ("159915", "SZ"),
        ("430047", "BJ"), ("830799", "BJ"), ("920001", "BJ"),
    ])
    def test_infer(self, symbol, market):
        assert infer_market(symbol) == market

    def test_invalid(self):
        with pytest.raises(ValueError):
            infer_market("60000")


class TestConverters:
    def test_to_ts_code(self):
        assert to_ts_code("sh.600000") == "600000.SH"
        assert to_ts_code("bj.920181") == "920181.BJ"
        assert to_ts_code("600000") == "600000.SH"

    def test_to_baostock_code(self):
        assert to_baostock_code("600000.SH") == "sh.600000"
        assert to_baostock_code("920181.BJ") == "bj.920181"
        assert to_baostock_code("000001") == "sz.000001"

    def test_to_symbol(self):
        assert to_symbol("600000.SH") == "600000"
        assert to_symbol("sh.600000") == "600000"

    def test_to_tdx(self):
        assert to_tdx_market("600000.SH") == "sh"
        assert to_tdx_file_stem("920181.BJ") == "bj920181"
        assert to_tdx_file_stem("sh.600000") == "sh600000"

    def test_round_trip(self):
        """任意格式进、标准格式出，再转回去应保持不变"""
        for code in ["600000.SH", "sh.600000", "sh600000", "600000"]:
            assert to_ts_code(to_baostock_code(code)) == "600000.SH"


class TestIndexAndFundCode:
    """指数归一化与基金前缀判定（统一单点）"""

    @pytest.mark.parametrize("code,expected", [
        ("000001.SH", "000001.SH"),
        ("sh.000001", "000001.SH"),
        ("000001", "000001.SH"),
        ("399001", "399001.SZ"),
        ("sh880001", "880001.SH"),
        ("881001", "881001.SH"),
        ("899050", "899050.BJ"),
    ])
    def test_to_index_ts_code(self, code, expected):
        assert to_index_ts_code(code) == expected

    @pytest.mark.parametrize("code", ["123456", "510300"])
    def test_to_index_ts_code_rejects_stock_prefix(self, code):
        with pytest.raises(ValueError):
            to_index_ts_code(code)

    @pytest.mark.parametrize("code", [
        "501001.SH", "508001.SH", "520001.SH", "526001.SH",
        "560001.SH", "562001.SH", "588001.SH", "500001.SH",
        "158000.SZ", "160105.SZ", "180101.SZ",
    ])
    def test_is_fund_code_true(self, code):
        assert is_fund_code(code) is True

    @pytest.mark.parametrize("code", [
        "600000.SH", "000001.SZ", "301001.SZ", "688001.SH", "920680.BJ",
    ])
    def test_is_fund_code_false(self, code):
        assert is_fund_code(code) is False

    def test_is_fund_code_honors_explicit_market(self):
        """同名代码在不同市场口径下以显式市场为准"""
        assert is_fund_code("500001", "SH") is True
        assert is_fund_code("500001", "SZ") is False
