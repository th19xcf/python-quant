"""index_daily 派生字段重算测试（纯函数，无数据库）。

断言口径：
    change  = close - pre_close
    pct_chg = change / pre_close * 100（pre_close 为 0 时 0.0）
容差：价格量级 ~1e4，change 与 pct_chg 均用绝对容差 1e-9（远小于 float64
在 1e4 量级的 1 ULP≈2e-12，也远小于业务分辨率 0.01）。
"""

from datetime import date

import pytest

from src.data.index_daily_derived import recalc_derived_rows

TOL = 1e-9


def _provider(pairs):
    mapping = {tuple(key): value for key, value in pairs}
    return lambda code, day: mapping.get((code, day))


def test_change_and_pct_chg_match_close_within_tolerance():
    rows = [
        {'id': 1, 'ts_code': '000001.SH', 'trade_date': date(2024, 1, 30),
         'close': 2830.53},
        {'id': 2, 'ts_code': '000001.SH', 'trade_date': date(2024, 1, 31),
         'close': 2788.55},
        {'id': 3, 'ts_code': '000001.SH', 'trade_date': date(2024, 2, 1),
         'close': 2770.74},
    ]
    prev = {
        ('000001.SH', date(2024, 1, 30)): 2883.36,
        ('000001.SH', date(2024, 1, 31)): 2830.53,
        ('000001.SH', date(2024, 2, 1)): 2788.55,
    }
    prev_close_of = _provider(prev.items())

    updates = {row['id']: row
               for row in recalc_derived_rows(rows, prev_close_of)}

    for source in rows:
        result = updates[source['id']]
        expected_pre = prev[(source['ts_code'], source['trade_date'])]
        expected_change = source['close'] - expected_pre
        assert result['pre_close'] == expected_pre
        assert abs(result['change'] - expected_change) <= TOL
        assert abs(result['change']
                   - (source['close'] - result['pre_close'])) <= TOL
        assert abs(result['pct_chg']
                   - (expected_change / expected_pre) * 100) <= TOL


def test_prev_close_comes_from_provider_not_previous_table_row():
    """表内存在日期缺口时，昨收必须取权威日历，而不是表内上一行。

    2024-02-29 的真实上一交易日是 2024-02-28（close=2957.85），
    表内上一行是 2024-01-31（close=2788.55）。用表内上一行会跨日错位。
    """
    rows = [
        {'id': 1, 'ts_code': '000001.SH', 'trade_date': date(2024, 1, 31),
         'close': 2788.55},
        {'id': 2, 'ts_code': '000001.SH', 'trade_date': date(2024, 2, 29),
         'close': 3015.17},
    ]
    prev_close_of = _provider([
        (('000001.SH', date(2024, 1, 31)), 2830.53),
        (('000001.SH', date(2024, 2, 29)), 2957.85),
    ])

    updates = {row['id']: row
               for row in recalc_derived_rows(rows, prev_close_of)}
    assert updates[2]['pre_close'] == 2957.85
    assert updates[2]['change'] == pytest.approx(3015.17 - 2957.85, abs=TOL)


def test_rows_without_authoritative_prev_close_are_skipped():
    rows = [
        {'id': 1, 'ts_code': '000001.SH', 'trade_date': date(2020, 1, 2),
         'close': 3089.26},
        {'id': 2, 'ts_code': '000001.SH', 'trade_date': date(2020, 1, 3),
         'close': 3080.0},
        {'id': 3, 'ts_code': '000001.SH', 'trade_date': date(2020, 1, 6),
         'close': None},
    ]
    prev_close_of = _provider([(('000001.SH', date(2020, 1, 3)), 3089.26)])

    updates = recalc_derived_rows(rows, prev_close_of)

    assert [row['id'] for row in updates] == [2]


def test_zero_prev_close_gives_zero_pct_chg():
    rows = [{'id': 1, 'ts_code': '399001.SZ', 'trade_date': date(2024, 3, 1),
             'close': 100.0}]
    updates = recalc_derived_rows(rows, lambda code, day: 0.0)

    assert updates[0]['pct_chg'] == 0.0
    assert updates[0]['change'] == pytest.approx(100.0, abs=TOL)


def test_different_codes_use_their_own_prev_close():
    rows = [
        {'id': 1, 'ts_code': '000001.SH', 'trade_date': date(2024, 3, 1),
         'close': 3027.02},
        {'id': 2, 'ts_code': '399001.SZ', 'trade_date': date(2024, 3, 1),
         'close': 9000.0},
    ]
    prev_close_of = _provider([
        (('000001.SH', date(2024, 3, 1)), 3015.17),
        (('399001.SZ', date(2024, 3, 1)), 8800.0),
    ])

    updates = {row['id']: row
               for row in recalc_derived_rows(rows, prev_close_of)}

    assert updates[1]['pre_close'] == 3015.17
    assert updates[2]['pre_close'] == 8800.0
