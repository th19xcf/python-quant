"""基金基础表补齐的纯逻辑测试（不连数据库）。"""

from datetime import date
from types import SimpleNamespace

from src.data.services.sync_fund import (
    _classify_local_fund_symbol,
    _fill_fund_backfill_fields,
)


def test_classify_local_fund_symbol():
    assert _classify_local_fund_symbol('510300') == ('ETF', 'fund_basic')
    assert _classify_local_fund_symbol('588000') == ('ETF', 'fund_basic')
    assert _classify_local_fund_symbol('159915') == ('ETF', 'fund_basic')
    assert _classify_local_fund_symbol('158036') == ('ETF', 'fund_basic')
    # 530=上证系列 ETF、551=科创债 ETF：tushare fund_basic(market=E) 各 14/12 只，均为 ETF
    assert _classify_local_fund_symbol('530060') == ('ETF', 'fund_basic')
    assert _classify_local_fund_symbol('551060') == ('ETF', 'fund_basic')
    assert _classify_local_fund_symbol('501000') == ('LOF', 'fund_basic')
    assert _classify_local_fund_symbol('508000') == ('LOF', 'fund_basic')
    assert _classify_local_fund_symbol('160130') == ('LOF', 'fund_basic')
    assert _classify_local_fund_symbol('184801') == ('LOF', 'fund_basic')
    assert _classify_local_fund_symbol('150001') == ('分级', 'fund_basic')
    # 502 段：既有分级母/子份额（无名称时按段兜底为分级）
    assert _classify_local_fund_symbol('502001') == ('分级', 'fund_basic')
    # 封闭式：无本地类型来源，只补基础表，不猜 fund_type
    assert _classify_local_fund_symbol('505888') == (None, 'closed_fund_basic')
    # 非基金代码段不猜测
    assert _classify_local_fund_symbol('600000') is None
    assert _classify_local_fund_symbol('399001') is None


def test_classify_prefers_name_over_symbol_segment():
    """502 段混有分级与整改后 LOF：有名称时名称优先。"""
    assert _classify_local_fund_symbol('502001', '西部利得中证500等权重指数分级-A') == (
        '分级', 'fund_basic')
    assert _classify_local_fund_symbol('502036', '大成中证互联网金融指数分级') == (
        '分级', 'fund_basic')
    # 转型后的普通 LOF 与 ETF 联接基金必须保持 LOF，不能因为 502 段或名字带 ETF 改成 ETF
    assert _classify_local_fund_symbol('502003', '易方达中证军工指数(LOF)-A') == (
        'LOF', 'fund_basic')
    assert _classify_local_fund_symbol('502056', '广发中证医疗ETF联接(LOF)-A') == (
        'LOF', 'fund_basic')
    assert _classify_local_fund_symbol('160119', '南方中证500ETF联接(LOF)-A') == (
        'LOF', 'fund_basic')


def test_fill_only_empty_fields_never_overwrites():
    fund = SimpleNamespace(symbol='', market='', list_date=None, fund_type='')
    changed = _fill_fund_backfill_fields(fund, '510300', 'sh', date(2005, 2, 23))
    assert changed is True
    assert fund.symbol == '510300'
    assert fund.market == 'SH'
    assert fund.list_date == date(2005, 2, 23)
    assert fund.fund_type == 'ETF'

    # 已有值（名称/状态等）不在这里被覆盖
    existing = SimpleNamespace(symbol='510300', market='SH',
                               list_date=date(2005, 2, 23), fund_type='ETF')
    assert _fill_fund_backfill_fields(existing, '510300', 'sh', date(2005, 2, 23)) is False
