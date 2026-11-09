"""申万行业 PIT 区间表的纯函数测试。"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from src.data.industry_pit import (
    _finalize_intervals,
    _industry_codes,
    build_tree_lookup,
    normalize_ts_code,
)


@pytest.mark.parametrize(('raw', 'expect'), [
    ('000001', '000001.SZ'),
    ('1', '000001.SZ'),
    ('600519', '600519.SH'),
    ('688981', '688981.SH'),
    ('430047', '430047.BJ'),
    ('920305', '920305.BJ'),
    ('300750.SZ', '300750.SZ'),
])
def test_normalize_ts_code(raw: str, expect: str) -> None:
    assert normalize_ts_code(raw) == expect


def test_industry_codes() -> None:
    assert _industry_codes('480301') == ('480000', '480300', '480301')


def test_finalize_intervals_builds_half_open_ranges() -> None:
    frame = pl.DataFrame({
        'ts_code': ['000001.SZ', '000001.SZ', '000002.SZ'],
        'industry_code': ['440101', '480101', '430101'],
        'in_date': [date(1991, 4, 3), date(2014, 2, 21), date(1991, 1, 29)],
        'record_updated_at': [date(2015, 10, 27)] * 3,
    })
    out = _finalize_intervals(frame).sort(['ts_code', 'in_date'])
    rows = out.to_dicts()
    assert rows[0]['out_date'] == date(2014, 2, 20)
    assert rows[1]['out_date'] is None
    assert rows[2]['out_date'] is None


def test_finalize_intervals_dedups_same_effective_date() -> None:
    frame = pl.DataFrame({
        'ts_code': ['600519.SH', '600519.SH'],
        'industry_code': ['340301', '340302'],
        'in_date': [date(2001, 7, 31), date(2001, 7, 31)],
        'record_updated_at': [date(2015, 10, 27), date(2021, 7, 30)],
    })
    out = _finalize_intervals(frame)
    assert out.height == 1
    assert out['industry_code'].to_list() == ['340302']


def test_build_tree_lookup_prefers_2021_and_fills_parent_names() -> None:
    records = {
        'SW2014': [
            {'SORTCODE': 'S48', 'SORTNAME': '银行旧'},
            {'SORTCODE': 'S4801', 'SORTNAME': '银行二级旧'},
            {'SORTCODE': 'S480101', 'SORTNAME': '银行旧'},
        ],
        'SW2021': [
            {'SORTCODE': 'S48', 'SORTNAME': '银行'},
            {'SORTCODE': 'S4803', 'SORTNAME': '股份制银行'},
            {'SORTCODE': 'S480301', 'SORTNAME': '股份制银行Ⅲ'},
        ],
    }
    lookup = build_tree_lookup(records)
    assert lookup['480301'] == {
        'version': 'SW2021',
        'industry_l1_name': '银行',
        'industry_l2_name': '股份制银行',
        'industry_l3_name': '股份制银行Ⅲ',
    }
    assert lookup['480101']['version'] == 'SW2014'


def test_build_tree_lookup_accepts_akshare_chinese_columns() -> None:
    records = {
        'SW2021': [
            {'类目编码': 'S11', '类目名称': '农林牧渔'},
            {'类目编码': 'S1101', '类目名称': '种植业'},
            {'类目编码': 'S110101', '类目名称': '粮食种植'},
        ],
    }
    lookup = build_tree_lookup(records)
    assert lookup['110101']['industry_l1_name'] == '农林牧渔'
    assert lookup['110101']['industry_l2_name'] == '种植业'
