"""stock_basic 重建守卫单测：截断名、当日标记、官方摘牌日。

运行：``.venv\\Scripts\\python.exe -m pytest tests/test_stock_basic_guards.py -q``

其中 ``test_truncated_name_would_override_without_guard`` 是「断言能失败」的
反证用例：它关闭守卫并断言截断名确实覆盖成功。一旦守卫一被删除或失效，
``test_truncated_name_keeps_existing`` 立刻变红，而本条会因参数消失而报错。
"""

from __future__ import annotations

from datetime import date
from typing import Any

import polars as pl

from tools.build_stock_basic import (
    COLUMNS,
    SCHEMA,
    clean_name,
    merge_with_existing,
    name_guard_reason,
    pins_official_delist,
)


def _frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    base = {
        'ts_code': '', 'name': None, 'name_basis': 'names_json',
        'list_date': date(2000, 1, 1), 'delist_date': None, 'status': 'L',
        'exchange': 'SSE', 'market': 'sh', 'board': '主板', 'delist_reason': None,
        'list_date_basis': 'quote_first_bar', 'delist_date_basis': None,
        'date_precision': 'bar',
    }
    return pl.DataFrame([{**base, **row} for row in rows],
                        schema={c: SCHEMA[c] for c in COLUMNS})


def test_clean_name_strips_xd_dr_n() -> None:
    """守卫二前提：XD / DR / N 三类当日标记都能被清洗掉。"""
    assert clean_name('XD皖新传') == '皖新传'
    assert clean_name('DR某某') == '某某'
    assert clean_name('N钶锐') == '钶锐'
    # 只有标记本身时保持原样，交给 name_guard_reason 判异常
    assert clean_name('XD') == 'XD'
    assert name_guard_reason('XD', '皖新传媒') is not None


def test_truncated_name_keeps_existing() -> None:
    """守卫一：候选是现有名的更短前缀 → 保留现有值并记跳过原因。"""
    candidate = _frame([{'ts_code': '601801.SH', 'name': '皖新传'}])
    existing = _frame([{'ts_code': '601801.SH', 'name': '皖新传媒'}])

    merged, diffs, counters = merge_with_existing(candidate, existing)

    assert merged['name'][0] == '皖新传媒'
    assert counters['guard_keep_existing'] == 1
    assert counters['conflict_keep_candidate'] == 0
    guard = [d for d in diffs if d['action'] == 'guard_keep_existing']
    assert len(guard) == 1
    assert guard[0]['candidate_value'] == '皖新传'
    assert '守卫一' in guard[0]['reason']


def test_truncated_name_would_override_without_guard() -> None:
    """反证：关闭守卫后截断名确实会覆盖，证明上一条断言不是恒真。"""
    candidate = _frame([{'ts_code': '601801.SH', 'name': '皖新传'}])
    existing = _frame([{'ts_code': '601801.SH', 'name': '皖新传媒'}])

    merged, _, counters = merge_with_existing(candidate, existing, guards=False)

    assert merged['name'][0] == '皖新传'
    assert counters['guard_keep_existing'] == 0


def test_longer_name_overrides() -> None:
    """守卫一不拦真修正：现有名是候选前缀时应允许覆盖。"""
    candidate = _frame([{'ts_code': '001381.SZ', 'name': '皇冠新材'}])
    existing = _frame([{'ts_code': '001381.SZ', 'name': '皇冠'}])

    merged, _, counters = merge_with_existing(candidate, existing)

    assert merged['name'][0] == '皇冠新材'
    assert counters['conflict_keep_candidate'] == 1


def test_same_length_rename_overrides() -> None:
    """同名长度真改名不受守卫一影响。"""
    candidate = _frame([{'ts_code': '600251.SH', 'name': '新疆冠农'}])
    existing = _frame([{'ts_code': '600251.SH', 'name': '冠农股份'}])

    merged, _, counters = merge_with_existing(candidate, existing)

    assert merged['name'][0] == '新疆冠农'
    assert counters['guard_keep_existing'] == 0


def test_official_delist_not_moved_earlier() -> None:
    """守卫三：官方摘牌日晚于末根 bar 时，日期链 4 列全部保持原值。"""
    candidate = _frame([{
        'ts_code': '600680.SH', 'name': '上海普天', 'status': 'D',
        'delist_date': date(2018, 4, 27), 'delist_reason': '退市',
        'list_date_basis': 'quote_first_bar', 'delist_date_basis': 'quote_last_bar',
        'date_precision': 'bar',
    }])
    existing = _frame([{
        'ts_code': '600680.SH', 'name': '上海普天', 'status': 'D',
        'delist_date': date(2019, 5, 23), 'delist_reason': '退市',
        'list_date_basis': 'official_list', 'delist_date_basis': 'official_delist',
        'date_precision': 'official',
    }])

    assert pins_official_delist(candidate.to_dicts()[0], existing.to_dicts()[0])
    merged, diffs, counters = merge_with_existing(candidate, existing)

    row = merged.row(0, named=True)
    assert row['delist_date'] == date(2019, 5, 23)
    assert row['delist_date_basis'] == 'official_delist'
    assert row['date_precision'] == 'official'
    assert row['list_date_basis'] == 'official_list'
    assert counters['guard_keep_existing'] == 1
    guard = [d for d in diffs if d['action'] == 'guard_keep_existing']
    assert '守卫三' in guard[0]['reason']
    assert guard[0]['candidate_value'] == '2018-04-27'


def test_guard_does_not_pin_quoted_delist() -> None:
    """守卫三只针对「官方摘牌日」，表内已是 bar 口径时不拦。"""
    candidate = _frame([{
        'ts_code': '000001.SZ', 'name': '平安银行', 'status': 'D',
        'delist_date': date(2020, 1, 2), 'delist_reason': '退市',
        'delist_date_basis': 'quote_last_bar', 'date_precision': 'bar',
    }])
    existing = _frame([{
        'ts_code': '000001.SZ', 'name': '平安银行', 'status': 'D',
        'delist_date': date(2020, 1, 3), 'delist_reason': '退市',
        'delist_date_basis': 'quote_last_bar', 'date_precision': 'bar',
    }])

    merged, _, counters = merge_with_existing(candidate, existing)

    assert merged['delist_date'][0] == date(2020, 1, 2)
    assert counters['guard_keep_existing'] == 0
