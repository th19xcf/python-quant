"""FundSection 基金板块单元测试。"""

import struct
from types import SimpleNamespace

import polars as pl

from src.ui.fund_section import (
    FUND_TYPE_MENUS,
    FundSection,
    _default_fund_data,
    is_closed_fund_ts_code,
    is_etf_fund_ts_code,
    load_local_fund_basic,
    split_by_fund_type,
)


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
            # 真实布局：@20 为 float32 成交额（元），@24 为 uint32 成交量（股）；
            # 基金价格字段按 3 位小数存储（×1000），不是个股的 ×100
            f.write(struct.pack(
                "<IIIIIfII",
                int(day), int(price * 1000), int(price * 1000), int(price * 1000),
                int(price * 1000), 1000000.0, 100 * 100, 0,
            ))


def _fake_data_manager(tdx_data_path):
    """构造仅暴露本地行情读取能力的 DataManager 替身。"""
    from src.data.data_manager import DataManager
    from src.data.tdx_day_parser import build_day_file_path, resolve_market

    manager = SimpleNamespace(config=SimpleNamespace(data=SimpleNamespace(tdx_data_path=tdx_data_path)))
    # get_latest_local_quote 内部会调 get_local_day_file_path，替身必须一并提供；
    # 这里直接复用 tdx_day_parser 的 canonical 映射，不依赖 DataManager 实例状态
    manager.get_local_day_file_path = lambda code, ts_code=None: build_day_file_path(
        tdx_data_path, resolve_market(code, ts_code), code
    )
    manager.get_latest_local_quote = lambda code, ts_code=None: DataManager.get_latest_local_quote(
        manager, code, ts_code
    )
    return manager


def test_get_real_fund_data_from_tdx(tmp_path):
    _write_day_file(tmp_path / "sh" / "lday" / "sh510300.day", 4.2, 4.0)
    window = SimpleNamespace(data_manager=_fake_data_manager(str(tmp_path)))
    section = FundSection(window)

    data = section.get_real_fund_data("510300", "沪深300ETF", "510300.SH")

    assert data is not None
    assert data["code"] == "510300.SH"
    assert data["price"] == "4.20"
    assert data["preclose"] == "4.00"
    assert data["change"] == "+0.20"
    assert data["change_pct"] == "+5.00%"
    assert data["volume"] == "100"
    assert data["amount"] == "0.01亿"


def test_get_real_fund_data_missing_file(tmp_path):
    window = SimpleNamespace(data_manager=_fake_data_manager(str(tmp_path)))
    section = FundSection(window)

    assert section.get_real_fund_data("510300", "沪深300ETF") is None


def test_get_real_fund_data_without_data_manager():
    from types import SimpleNamespace

    section = FundSection(SimpleNamespace(data_manager=None))
    assert section.get_real_fund_data("510300", "沪深300ETF") is None


# ---------------------------------------------------------------------------
# 菜单分类：按 fund_type 读本地名录取数（不依赖数据库）
# ---------------------------------------------------------------------------


def test_split_by_fund_type_keeps_unknown_out_of_menus():
    frame = pl.DataFrame({
        'ts_code': ['510300.SH', '160105.SZ', '150001.SZ', '999999.SH', '888888.SH'],
        'fund_type': ['ETF', 'LOF', '分级', None, '其它'],
    })

    menus, unknown = split_by_fund_type(frame)

    assert list(menus) == list(FUND_TYPE_MENUS)
    assert menus['ETF']['ts_code'].to_list() == ['510300.SH']
    assert unknown == {'空值': 1, '其它': 1}


def test_fund_source_is_local_and_works_without_database():
    """无 data_manager（离线/无库）时名录仍非空，且三类条数与名录一致。"""
    frame = load_local_fund_basic(None)
    assert not frame.is_empty()

    section = FundSection(SimpleNamespace(data_manager=None))
    for kind in FUND_TYPE_MENUS:
        source = section._get_fund_source(kind)
        expected = frame.filter(pl.col('fund_type') == kind).height
        assert source.height == expected
        assert source.height > 0
        assert source.columns == ['ts_code', 'name']

    etf = section._get_fund_source('ETF')
    assert '510300.SH' in etf['ts_code'].to_list()


def test_load_funds_task_keeps_market_suffix_in_code():
    """代码列必须是带市场后缀的标准代码，不能截成纯数字。"""
    section = FundSection(SimpleNamespace(data_manager=None))
    section._get_fund_source = lambda kind: pl.DataFrame(
        {'ts_code': ['510300.SH', '159915.SZ'], 'name': ['沪深300ETF', '创业板ETF']})
    section.get_real_fund_data = lambda code, name, ts_code=None: None

    result = section._load_funds_task('ETF', 'ETF基金', 'ETF_funds')

    assert [row['code'] for row in result['ETF_funds']] == ['510300.SH', '159915.SZ']


def test_fill_fund_table_measures_all_column_widths(qapp, monkeypatch):
    """根因回归：基金表必须按内容实测列宽，不能沿用上一视图的占位宽度。"""
    from PySide6.QtWidgets import QTableWidget

    import src.ui.fund_section as fund_section

    captured = {}
    monkeypatch.setattr(
        fund_section, 'apply_estimated_column_widths',
        lambda table, rows: captured.update(rows=rows))

    table = QTableWidget(0, 13)
    table.setHorizontalHeaderLabels([''] * 13)
    section = FundSection(SimpleNamespace(stock_table=table, data_manager=None))
    fund = dict(zip(fund_section._FUND_COLUMNS, [
        '2026-11-16', '510300.SH', '沪深300ETF华泰柏瑞', '-0.14%', '4.05', '-0.01',
        '123,456', '0.52亿', '4.06', '4.07', '4.04', '4.06', '0.74%',
    ], strict=True))

    section._fill_fund_table([fund])

    assert captured['rows'] == [[fund[key] for key in fund_section._FUND_COLUMNS]]
    assert table.item(0, 1).text() == '510300.SH'
