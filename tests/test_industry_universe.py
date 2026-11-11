"""行业选池（申万 PIT → ts_codes）的用例。

前半部分是合成表上的纯逻辑，后半部分在真实列存上核对黄金的逐年只数与边界日。
真实数据缺失时自动跳过，不影响纯逻辑用例。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from src.data.industry_universe import (
    REASON_BEFORE_TREE,
    REASON_NO_MEMBER,
    REASON_UNKNOWN_INDUSTRY,
    SW_TREE_EFFECTIVE_DATE,
    select_industry_universe,
)

REAL_PIT = (
    Path(__file__).resolve().parents[1]
    / 'data/local_store/meta/industry_pit/industry_pit_sw.parquet'
)


def _pit(rows: list[dict]) -> pl.DataFrame:
    """构造最小申万区间表；未给出的层级别名用空串占位。"""
    base = {'industry_l1': '', 'industry_l1_name': '',
            'industry_l2': '', 'industry_l2_name': '',
            'industry_l3': '', 'industry_l3_name': ''}
    filled = [{**base, **row} for row in rows]
    return pl.DataFrame(filled).with_columns(
        pl.col('in_date').cast(pl.Date),
        pl.col('out_date').cast(pl.Date),
    )


def _basic(rows: list[tuple[str, date | None]]) -> pl.DataFrame:
    return pl.DataFrame({
        'ts_code': [code for code, _ in rows],
        'delist_date': [d for _, d in rows],
    }).with_columns(pl.col('delist_date').cast(pl.Date))


GOLD_PIT = _pit([
    {'ts_code': '600489.SH', 'industry_l1': '240000', 'industry_l1_name': '有色金属',
     'industry_l2': '240400', 'industry_l2_name': '贵金属',
     'industry_l3': '240401', 'industry_l3_name': '黄金',
     'in_date': date(2014, 2, 21), 'out_date': None},
    {'ts_code': '600547.SH', 'industry_l1': '240000', 'industry_l1_name': '有色金属',
     'industry_l2': '240400', 'industry_l2_name': '贵金属',
     'industry_l3': '240401', 'industry_l3_name': '黄金',
     'in_date': date(2014, 2, 21), 'out_date': date(2020, 12, 31)},
    {'ts_code': '000001.SZ', 'industry_l1': '480000', 'industry_l1_name': '银行',
     'industry_l2': '480100', 'industry_l2_name': '银行',
     'industry_l3': '480101', 'industry_l3_name': '国有大型银行',
     'in_date': date(2014, 2, 21), 'out_date': None},
    # 已退出该行业的标的，用来制造「行业存在但当日无成分」
    {'ts_code': '600547.SH', 'industry_l1': '240000', 'industry_l1_name': '有色金属',
     'industry_l2': '240400', 'industry_l2_name': '贵金属',
     'industry_l3': '240402', 'industry_l3_name': '白银',
     'in_date': date(2014, 2, 21), 'out_date': date(2015, 12, 31)},
])
GOLD_BASIC = _basic([('600489.SH', None), ('600547.SH', date(2020, 12, 31)),
                     ('000001.SZ', None)])


# ---------------------------------------------------------------- 纯逻辑用例

def test_selects_by_name_and_by_code() -> None:
    by_name = select_industry_universe('L3', '黄金', '2016-06-30', pit=GOLD_PIT,
                                       stock_basic=GOLD_BASIC)
    by_code = select_industry_universe('l3', '240401', '2016-06-30', pit=GOLD_PIT,
                                       stock_basic=GOLD_BASIC)
    assert by_name.reason is None
    assert by_name.codes == ['600489.SH', '600547.SH']
    assert by_code.codes == by_name.codes
    assert by_name.matched_industry_codes == ['240401']


def test_interval_is_closed_on_both_ends() -> None:
    """in_date 当日应入选，out_date 当日仍应入选，次日排除。"""
    on_in = select_industry_universe('L3', '黄金', date(2014, 2, 21), pit=GOLD_PIT,
                                     stock_basic=GOLD_BASIC, include_delisted=True)
    on_out = select_industry_universe('L3', '黄金', date(2020, 12, 31), pit=GOLD_PIT,
                                      stock_basic=GOLD_BASIC, include_delisted=True)
    after_out = select_industry_universe('L3', '黄金', date(2021, 1, 1), pit=GOLD_PIT,
                                         stock_basic=GOLD_BASIC, include_delisted=True)
    assert '600547.SH' in on_in.codes
    assert '600547.SH' in on_out.codes
    assert '600547.SH' not in after_out.codes


def test_before_tree_returns_empty_with_reason() -> None:
    result = select_industry_universe('L3', '黄金', '2014-02-20', pit=GOLD_PIT,
                                      stock_basic=GOLD_BASIC)
    assert result.codes == []
    assert result.reason == REASON_BEFORE_TREE
    assert str(SW_TREE_EFFECTIVE_DATE) in result.message
    assert '返回空名单' in result.message


def test_unknown_industry_name_has_reason() -> None:
    result = select_industry_universe('L3', '不存在的行业', '2016-06-30', pit=GOLD_PIT,
                                      stock_basic=GOLD_BASIC)
    assert result.codes == []
    assert result.reason == REASON_UNKNOWN_INDUSTRY
    assert '不存在' in result.message


def test_industry_without_member_at_date_has_reason() -> None:
    result = select_industry_universe('L3', '白银', '2021-06-30', pit=GOLD_PIT,
                                      stock_basic=GOLD_BASIC)
    assert result.codes == []
    assert result.reason == REASON_NO_MEMBER
    assert '没有区间内成分' in result.message


def test_delisted_excluded_by_default_and_kept_on_demand() -> None:
    default = select_industry_universe('L3', '黄金', '2020-12-31', pit=GOLD_PIT,
                                       stock_basic=GOLD_BASIC)
    kept = select_industry_universe('L3', '黄金', '2020-12-31', pit=GOLD_PIT,
                                    stock_basic=GOLD_BASIC, include_delisted=True)
    assert default.excluded_delisted == ['600547.SH']
    assert default.codes == ['600489.SH']
    assert kept.codes == ['600489.SH', '600547.SH']


def test_missing_delist_date_skips_filter_without_silence() -> None:
    basic = pl.DataFrame({'ts_code': ['600489.SH', '600547.SH'],
                          'name': ['中金黄金', '山东黄金']})
    result = select_industry_universe('L3', '黄金', '2020-12-31', pit=GOLD_PIT,
                                      stock_basic=basic)
    assert result.codes == ['600489.SH', '600547.SH']
    assert '未做退市剔除' in result.message


@pytest.mark.parametrize('level', ['L4', '', 'l3x'])
def test_invalid_level_raises(level: str) -> None:
    with pytest.raises(ValueError):
        select_industry_universe(level, '黄金', '2016-06-30', pit=GOLD_PIT)


def test_empty_name_and_bad_date_raise() -> None:
    with pytest.raises(ValueError):
        select_industry_universe('L3', '  ', '2016-06-30', pit=GOLD_PIT)
    with pytest.raises(ValueError):
        select_industry_universe('L3', '黄金', 'not-a-date', pit=GOLD_PIT)


def test_accepts_compact_date_and_datetime() -> None:
    from datetime import datetime

    assert select_industry_universe('L3', '黄金', '20160630', pit=GOLD_PIT,
                                    stock_basic=GOLD_BASIC).codes == ['600489.SH', '600547.SH']
    assert select_industry_universe('L3', '黄金', datetime(2016, 6, 30, 15, 0),
                                    pit=GOLD_PIT, stock_basic=GOLD_BASIC).codes == [
        '600489.SH', '600547.SH']


# ---------------------------------------------------------------- 真实数据用例

requires_real_store = pytest.mark.skipif(
    not REAL_PIT.exists(), reason=f'缺少真实申万区间表 {REAL_PIT}')


@requires_real_store
@pytest.mark.parametrize(('as_of', 'expected'), [
    ('2015-01-01', 10),
    ('2016-12-31', 12),
    ('2021-12-31', 10),   # 原始 11 只，含 2021-03-03 已退市的 600687.SH
    ('2026-11-09', 10),   # 原始 13 只，含 3 只已退市
])
def test_real_gold_member_counts(as_of: str, expected: int) -> None:
    result = select_industry_universe('L3', '黄金', as_of)
    assert result.reason is None, result.message
    assert len(result.codes) == expected
    assert result.codes == sorted(result.codes)


@requires_real_store
def test_real_gold_tree_boundary() -> None:
    before = select_industry_universe('L3', '黄金', '2014-02-20')
    on_day = select_industry_universe('L3', '黄金', '2014-02-21')
    assert before.codes == []
    assert before.reason == REASON_BEFORE_TREE
    assert before.message
    assert len(on_day.codes) == 10


@requires_real_store
def test_real_delisted_member_excluded_after_delist_date() -> None:
    """600687.SH（退市刚泰）2021-03-03 退市：退市前应入选，退市后应排除。"""
    before = select_industry_universe('L3', '黄金', '2021-03-01')
    after = select_industry_universe('L3', '黄金', '2021-03-05')
    kept = select_industry_universe('L3', '黄金', '2021-05-01', include_delisted=True)
    assert '600687.SH' in before.codes
    assert '600687.SH' not in after.codes
    assert '600687.SH' in after.excluded_delisted
    assert '600687.SH' in kept.codes


@requires_real_store
def test_real_unknown_industry_and_concept_gap() -> None:
    assert select_industry_universe('L3', '黄金概念', '2026-11-09').reason == \
        REASON_UNKNOWN_INDUSTRY
    assert select_industry_universe('L2', '贵金属', '2026-11-09').codes
