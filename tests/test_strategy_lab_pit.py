"""策略试算 PIT 口径：财报只能按公告日 as-of，不能提前看到未来一期。"""

from __future__ import annotations

from datetime import date

import polars as pl

from src.quant.strategy_lab.panels import asof_lookup, build_asof_frame


def _frame() -> pl.DataFrame:
    return pl.DataFrame({
        'ts_code': ['000001.SZ'] * 3,
        'end_date': [date(2019, 12, 31), date(2020, 12, 31), date(2021, 12, 31)],
        'ann_date': [date(2020, 4, 30), date(2021, 4, 30), date(2022, 4, 30)],
        'parent_net_profit': [100.0, 200.0, 300.0],
    })


def test_financial_not_visible_before_announcement():
    series = build_asof_frame(_frame(), 'parent_net_profit', date_col='ann_date')
    pair = series['000001.SZ']
    # 2020-12-31 是第二期的报告期，但第二期 2021-04-30 才公告：
    # 信号日 2020-12-31 只能看到第一期（100），不能看到 200。
    assert asof_lookup(pair, date(2020, 12, 31)) == 100.0
    assert asof_lookup(pair, date(2021, 4, 29)) == 100.0
    assert asof_lookup(pair, date(2021, 4, 30)) == 200.0


def test_no_record_before_first_announcement():
    pair = build_asof_frame(_frame(), 'parent_net_profit', date_col='ann_date')['000001.SZ']
    assert asof_lookup(pair, date(2020, 4, 29)) is None
    assert asof_lookup(pair, date(2019, 1, 1)) is None


def test_same_announcement_keeps_latest_period():
    df = pl.DataFrame({
        'ts_code': ['000001.SZ', '000001.SZ'],
        'end_date': [date(2020, 6, 30), date(2020, 12, 31)],
        'ann_date': [date(2021, 3, 31), date(2021, 3, 31)],
        'value': [1.0, 2.0],
    })
    pair = build_asof_frame(df, 'value', date_col='ann_date')['000001.SZ']
    assert asof_lookup(pair, date(2021, 12, 31)) == 2.0


def test_missing_code_returns_none():
    series = build_asof_frame(_frame(), 'parent_net_profit', date_col='ann_date')
    assert asof_lookup(series.get('999999.SZ'), date(2021, 12, 31)) is None
