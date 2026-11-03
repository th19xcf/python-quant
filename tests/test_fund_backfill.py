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
    assert _classify_local_fund_symbol('501000') == ('LOF', 'fund_basic')
    assert _classify_local_fund_symbol('508000') == ('LOF', 'fund_basic')
    assert _classify_local_fund_symbol('160130') == ('LOF', 'fund_basic')
    assert _classify_local_fund_symbol('184801') == ('LOF', 'fund_basic')
    assert _classify_local_fund_symbol('150001') == ('分级', 'fund_basic')
    # 封闭式：无本地类型来源，只补基础表，不猜 fund_type
    assert _classify_local_fund_symbol('505888') == (None, 'closed_fund_basic')
    # 非基金代码段不猜测
    assert _classify_local_fund_symbol('600000') is None
    assert _classify_local_fund_symbol('399001') is None


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
