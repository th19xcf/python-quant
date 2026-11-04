#!/usr/bin/env python3

"""换手率来源分级测试（股本反算优先）。

覆盖：
* 一级：逐日流通股本 → ``source='float_share'``，数值 = 成交量(手)×100/股本×100
* 二级：仅一条当前股本 → ``source='float_share_static'``
* 三级：无股本记录 → 已落库 baostock ``turn``，``source='baostock'``
* 优先级：股本与 baostock 同时存在时股本反算胜出
* 逐行回退：部分日期早于首个股本生效日时，逐行落到 baostock
"""

from __future__ import annotations

from datetime import date, datetime

import polars as pl

from src.data.local_store import share_capital as sc
from src.data.local_store.turnover import TURN_SCHEMA, attach_turn, turnover_path

CODE = '600000.SH'


def _daily(dates: list[date], volume: int = 100_000) -> pl.DataFrame:
    n = len(dates)
    return pl.DataFrame({
        'date': dates,
        'open': [10.0] * n,
        'high': [10.5] * n,
        'low': [9.5] * n,
        'close': [10.0] * n,
        'volume': [float(volume)] * n,
        'amount': [volume * 10.0] * n,
    })


def _write_shares(root, rows: list[tuple[date, int]]) -> None:
    frame = pl.DataFrame(
        {
            'ts_code': [CODE] * len(rows),
            'effective_date': [r[0] for r in rows],
            'total_share': [r[1] * 2 for r in rows],
            'float_share': [r[1] for r in rows],
            'free_float_share': [None] * len(rows),
            'change_reason': ['test'] * len(rows),
            'source': ['test'] * len(rows),
            'fetched_at': [datetime(2024, 1, 1)] * len(rows),
        },
        schema=sc.SHARE_SCHEMA,
    )
    sc.write_shares(frame, root=root)


def _write_baostock(root, rows: list[tuple[date, float]]) -> None:
    path = turnover_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            'ts_code': [CODE] * len(rows),
            'trade_date': [r[0] for r in rows],
            'turn': [r[1] for r in rows],
            'source': ['baostock'] * len(rows),
            'fetched_at': [datetime(2024, 1, 1)] * len(rows),
        },
        schema=TURN_SCHEMA,
    ).write_parquet(path)


def test_float_share_daily_is_first_priority(tmp_path):
    """一级：逐日股本按 effective_date 向后填充，数值与量纲正确。"""
    root = tmp_path / 'store'
    dates = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5)]
    # 3 亿股 → 10 万股 → turn = 100000×10000/3e9 = 0.3333%
    _write_shares(root, [(date(2024, 1, 2), 3_000_000_000),
                         (date(2024, 1, 4), 6_000_000_000)])

    out = attach_turn(_daily(dates), CODE, root).collect().sort('date')

    assert out['turn_source'].unique().to_list() == ['float_share']
    # 10 万股 = 1e7 股；前两日 1e7/3e9×100=0.3333%，后两日 1e7/6e9×100=0.1667%
    expected = [0.33333, 0.33333, 0.16667, 0.16667]
    for got, want in zip(out['turn'].to_list(), expected, strict=True):
        assert abs(got - want) < 1e-5
    assert out['date'].to_list() == dates


def test_single_share_row_is_static_approximation(tmp_path):
    """二级：仅一条当前股本时，用其近似全部历史并标注 float_share_static。"""
    root = tmp_path / 'store'
    dates = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]
    _write_shares(root, [(date(2024, 6, 1), 2_000_000_000)])

    out = attach_turn(_daily(dates), CODE, root).collect()

    assert out['turn_source'].unique().to_list() == ['float_share_static']
    # 10 万股 = 1e7 股；1e7 / 2e9 × 100 = 0.5%
    assert all(abs(v - 0.5) < 1e-6 for v in out['turn'].to_list())


def test_baostock_turn_is_third_priority(tmp_path):
    """三级：无股本记录时使用已落库 baostock turn。"""
    root = tmp_path / 'store'
    dates = [date(2024, 1, 2), date(2024, 1, 3)]
    _write_baostock(root, [(dates[0], 0.7), (dates[1], 0.9)])

    out = attach_turn(_daily(dates), CODE, root).collect()

    assert out['turn_source'].unique().to_list() == ['baostock']
    assert out['turn'].to_list() == [0.7, 0.9]


def test_float_share_beats_baostock_when_both_exist(tmp_path):
    """优先级：股本反算必须覆盖 baostock 值。"""
    root = tmp_path / 'store'
    dates = [date(2024, 1, 2), date(2024, 1, 3)]
    _write_shares(root, [(date(2024, 1, 2), 2_000_000_000)])
    _write_baostock(root, [(dates[0], 9.9), (dates[1], 9.9)])

    out = attach_turn(_daily(dates), CODE, root).collect()

    assert out['turn_source'].unique().to_list() == ['float_share_static']
    assert all(abs(v - 0.5) < 1e-6 for v in out['turn'].to_list())


def test_rows_before_first_effective_date_fall_back_per_row(tmp_path):
    """逐行回退：早于首个股本生效日的日期用 baostock，其余用股本反算。"""
    root = tmp_path / 'store'
    dates = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]
    _write_shares(root, [
        (date(2024, 1, 3), 2_000_000_000),
        (date(2024, 1, 4), 1_000_000_000),
    ])
    _write_baostock(root, [(dates[0], 1.1)])

    out = attach_turn(_daily(dates), CODE, root).collect().sort('date')

    assert out['turn_source'].to_list() == ['baostock', 'float_share', 'float_share']
    assert out['turn'].to_list()[0] == 1.1
