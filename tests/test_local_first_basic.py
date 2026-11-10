#!/usr/bin/env python3

"""``local_first_basic`` 合并式导出的核心行为测试。"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from src.data.services.local_first_basic import (
    assert_no_null_increase,
    merge_fund_basic,
    merge_index_basic,
    null_counts,
)

SCHEMA = {
    'id': pl.Int64, 'ts_code': pl.String, 'symbol': pl.String, 'name': pl.String,
    'fund_type': pl.String, 'market': pl.String, 'list_date': pl.Date,
    'delist_date': pl.Date, 'status': pl.String,
    'created_at': pl.Datetime('us'), 'updated_at': pl.Datetime('us'),
    'is_tradable': pl.Boolean, 'tradable_reason': pl.String, 'source': pl.String,
}


def _frame(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=SCHEMA)


def test_conflict_keeps_local_verified_value():
    local = _frame([{
        'id': 1, 'ts_code': '510300.SH', 'symbol': '510300',
        'name': '沪深300ETF', 'fund_type': 'ETF', 'market': 'SH',
        'list_date': date(2012, 5, 28), 'delist_date': None, 'status': 'L',
        'created_at': None, 'updated_at': None,
        'is_tradable': True, 'tradable_reason': 'ok', 'source': 'local_verified',
    }])
    db = pl.DataFrame([{
        'id': 1, 'ts_code': '510300.SH', 'symbol': '510300',
        'name': '旧名', 'fund_type': '基金', 'market': 'SH',
        'list_date': date(2012, 5, 1), 'delist_date': None, 'status': '1',
    }])

    merged, diffs, counters = merge_fund_basic(local, db)

    row = merged.to_dicts()[0]
    assert row['name'] == '沪深300ETF'
    assert row['list_date'] == date(2012, 5, 28)
    assert row['fund_type'] == 'ETF'
    assert row['status'] == 'L'
    assert row['is_tradable'] is True
    assert counters['conflict_keep_local'] >= 1
    actions = {(d['field'], d['action']) for d in diffs}
    assert ('name', 'conflict_keep_local') in actions
    assert ('list_date', 'conflict_keep_local') in actions


def test_fill_only_when_local_null():
    local = _frame([{
        'id': 2, 'ts_code': '159915.SZ', 'symbol': '159915',
        'name': None, 'fund_type': 'ETF', 'market': 'SZ',
        'list_date': None, 'delist_date': None, 'status': 'L',
        'created_at': None, 'updated_at': None,
        'is_tradable': False, 'tradable_reason': 'pending', 'source': 'local',
    }])
    db = pl.DataFrame([{
        'id': 2, 'ts_code': '159915.SZ', 'symbol': '159915',
        'name': '创业板ETF', 'fund_type': 'ETF', 'market': 'SZ',
        'list_date': date(2011, 9, 20), 'delist_date': None, 'status': '1',
    }])

    merged, diffs, counters = merge_fund_basic(local, db)
    row = merged.to_dicts()[0]
    assert row['name'] == '创业板ETF'
    assert row['list_date'] == date(2011, 9, 20)
    assert row['is_tradable'] is False
    assert row['tradable_reason'] == 'pending'
    assert counters['fill_from_db'] == 2
    assert {d['action'] for d in diffs if d['field'] in {'name', 'list_date'}} == {'fill_from_db'}


def test_db_does_not_erase_local_only_delist_date():
    local = _frame([{
        'id': 3, 'ts_code': '150018.SZ', 'symbol': '150018', 'name': '旧分级',
        'fund_type': '基金', 'market': 'SZ', 'list_date': date(2010, 1, 1),
        'delist_date': date(2020, 12, 31), 'status': 'D',
        'created_at': None, 'updated_at': None,
        'is_tradable': False, 'tradable_reason': 'delisted', 'source': 'local',
    }])
    db = pl.DataFrame([{
        'id': 3, 'ts_code': '150018.SZ', 'symbol': '150018', 'name': '旧分级',
        'fund_type': '基金', 'market': 'SZ', 'list_date': date(2010, 1, 1),
        'delist_date': None, 'status': '1',
    }])

    merged, diffs, _ = merge_fund_basic(local, db)
    row = merged.to_dicts()[0]
    assert row['delist_date'] == date(2020, 12, 31)
    assert row['status'] == 'D'
    assert {'field': 'delist_date', 'action': 'local_only_keep'} in [
        {'field': d['field'], 'action': d['action']} for d in diffs
    ]


def test_append_missing_db_row_preserves_row_count_rule():
    local = _frame([{
        'id': 4, 'ts_code': '510500.SH', 'symbol': '510500', 'name': '中证500ETF',
        'fund_type': 'ETF', 'market': 'SH', 'list_date': date(2013, 2, 6),
        'delist_date': None, 'status': 'L', 'created_at': None, 'updated_at': None,
        'is_tradable': True, 'tradable_reason': 'ok', 'source': 'local',
    }])
    db = pl.DataFrame([
        {
            'id': 4, 'ts_code': '510500.SH', 'symbol': '510500', 'name': '中证500ETF',
            'fund_type': 'ETF', 'market': 'SH', 'list_date': date(2013, 2, 6),
            'delist_date': None, 'status': '1',
        },
        {
            'id': 5, 'ts_code': '588000.SH', 'symbol': '588000', 'name': '科创50ETF',
            'fund_type': 'ETF', 'market': 'SH', 'list_date': date(2020, 9, 22),
            'delist_date': None, 'status': '1',
        },
    ])

    merged, diffs, counters = merge_fund_basic(local, db)
    assert counters['append_row'] == 1
    assert merged.height == local.height + 1
    appended = merged.filter(pl.col('ts_code') == '588000.SH').to_dicts()[0]
    assert appended['name'] == '科创50ETF'
    assert appended['is_tradable'] is None
    assert any(d['action'] == 'append_row' for d in diffs)


def test_null_increase_assertion_fails_on_n_inflation():
    before = {'name': 0, 'list_date': 2}
    after_ok = {'name': 0, 'list_date': 0}
    assert_no_null_increase(before, after_ok)

    after_bad = {'name': 5, 'list_date': 2}
    with pytest.raises(AssertionError, match='name 空值增加: 0 -> 5'):
        assert_no_null_increase(before, after_bad)


def test_null_counts_matches_frame():
    local = _frame([{
        'id': 1, 'ts_code': 'A.SH', 'symbol': 'A', 'name': None,
        'fund_type': None, 'market': 'SH', 'list_date': None, 'delist_date': None,
        'status': 'L', 'created_at': None, 'updated_at': None,
        'is_tradable': None, 'tradable_reason': None, 'source': None,
    }])
    counts = null_counts(local)
    assert counts['name'] == 1
    assert counts['status'] == 0


INDEX_SCHEMA = {
    'id': pl.Int64, 'ts_code': pl.String, 'name': pl.String, 'market': pl.String,
    'publisher': pl.String, 'index_type': pl.String, 'category': pl.String,
    'base_date': pl.Date, 'base_point': pl.Null, 'list_date': pl.Date,
    'weight_rule': pl.String, 'desc': pl.String,
    'created_at': pl.Datetime('us'), 'updated_at': pl.Datetime('us'),
}


def test_index_merge_keeps_local_rows_missing_in_db():
    """库里没有的两行（新增中证指数）必须整行保留，且不得被整表覆盖丢掉。"""
    local = pl.DataFrame([
        {
            'id': 1, 'ts_code': '000001.SH', 'name': '上证指数', 'market': 'SH',
            'publisher': '上交所', 'index_type': '综合指数', 'category': '宽基',
            'base_date': date(1990, 12, 19), 'base_point': None,
            'list_date': date(1991, 7, 15), 'weight_rule': '市值加权',
            'desc': '上证综合指数', 'created_at': None, 'updated_at': None,
        },
        {
            'id': 2, 'ts_code': '000985.SH', 'name': '中证全指', 'market': 'SH',
            'publisher': '上交所', 'index_type': '综合指数', 'category': '宽基',
            'base_date': None, 'base_point': None, 'list_date': None,
            'weight_rule': '市值加权', 'desc': '中证全指', 'created_at': None,
            'updated_at': None,
        },
    ], schema=INDEX_SCHEMA)
    db = pl.DataFrame([{
        'id': 1, 'ts_code': '000001.SH', 'name': '上证指数', 'market': 'SH',
        'publisher': '上交所', 'index_type': '综合指数', 'category': '宽基',
        'base_date': date(1991, 1, 1), 'base_point': None,
        'list_date': date(1991, 7, 15), 'weight_rule': '市值加权',
        'desc': '上证综合指数(000001.SH)，通达信基准',
    }])

    merged, diffs, counters = merge_index_basic(local, db)

    assert merged.height == local.height
    assert counters['missing_in_db'] == 1
    assert counters['append_row'] == 0
    codes = merged['ts_code'].to_list()
    assert codes == ['000001.SH', '000985.SH']
    row = merged.filter(pl.col('ts_code') == '000985.SH').to_dicts()[0]
    assert row['name'] == '中证全指'
    assert merged['base_point'].null_count() == merged.height
    assert any(d['field'] == 'base_date' and d['action'] == 'conflict_keep_local'
               for d in diffs)


def test_index_null_typed_column_refuses_silent_type_change():
    """本地全空列（dtype=Null）遇到库里有值时，必须报错而不是悄悄改列类型。"""
    local = pl.DataFrame([{
        'id': 1, 'ts_code': '000001.SH', 'name': '上证指数', 'market': 'SH',
        'publisher': '上交所', 'index_type': '综合指数', 'category': '宽基',
        'base_date': None, 'base_point': None, 'list_date': None,
        'weight_rule': '市值加权', 'desc': '上证综合指数',
        'created_at': None, 'updated_at': None,
    }], schema=INDEX_SCHEMA)
    db = pl.DataFrame([{
        'id': 1, 'ts_code': '000001.SH', 'name': '上证指数', 'market': 'SH',
        'publisher': '上交所', 'index_type': '综合指数', 'category': '宽基',
        'base_date': None, 'base_point': 100.0, 'list_date': None,
        'weight_rule': '市值加权', 'desc': '上证综合指数',
    }])

    with pytest.raises(ValueError, match='base_point 在本地为全空列'):
        merge_index_basic(local, db)
