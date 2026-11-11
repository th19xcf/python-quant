#!/usr/bin/env python3
"""涨跌停档位契约：风控查表（``PriceLimitTradable.DEFAULT_LIMITS``）与撮合推导
（``ExecutionRules.resolve_limit_pct / resolve_sector``）必须一一对应。

背景：两处各存一份涨跌停档位，撮合侧是唯一真源（按交易日/ST/代码段细分），
风控侧只能作查表快照。任一侧单独调整（例如只把风控的创业板改成 10%，
或在撮合侧新增一个号段却忘了同步档位表）都会让同一笔委托在「撮合判定」与
「风控拦截」两条路径上得到相反结论。本文件是这两张表的守门用例。
"""

from __future__ import annotations

import datetime as dt

import pytest

from src.risk import PriceLimitTradable
from src.risk.execution_rules import ExecutionRules

CHINEXT_REFORM = dt.date(2020, 8, 24)
AFTER_REFORM = dt.date(2026, 1, 5)
BEFORE_REFORM = dt.date(2020, 8, 21)

#: (ts_code, is_st, trade_date, 期望 sector, 期望幅度)
GRID = [
    ('600000.SH', None, None, '主板', 0.10),
    ('000001.SZ', None, None, '主板', 0.10),
    ('600000.SH', True, None, 'ST', 0.05),
    ('300750.SZ', None, AFTER_REFORM, '创业板', 0.20),
    ('300750.SZ', None, CHINEXT_REFORM, '创业板', 0.20),
    ('300750.SZ', None, BEFORE_REFORM, '主板', 0.10),
    ('300750.SZ', True, AFTER_REFORM, '创业板', 0.20),
    ('300750.SZ', True, BEFORE_REFORM, 'ST', 0.05),
    ('688981.SH', None, None, '科创板', 0.20),
    ('689009.SH', None, None, '科创板', 0.20),
    ('688981.SH', True, None, '科创板', 0.20),
    ('920001.BJ', None, None, '北交所', 0.30),
    ('830799.BJ', None, None, '北交所', 0.30),
    ('920001.BJ', True, None, '北交所', 0.30),
    ('900901.SH', None, None, '主板', 0.10),
    ('200002.SZ', None, None, '主板', 0.10),
]


@pytest.mark.parametrize('ts_code, is_st, trade_date, sector, pct', GRID)
def test_sector_and_limit_pct_agree(ts_code, is_st, trade_date, sector, pct):
    """撮合侧推导出的 sector 与幅度，必须能在风控档位表里查到同一个数。"""
    resolved_sector = ExecutionRules.resolve_sector(
        ts_code, is_st=is_st, trade_date=trade_date)
    resolved_pct = ExecutionRules.resolve_limit_pct(
        ts_code, is_st=is_st, trade_date=trade_date)
    assert resolved_sector == sector
    assert resolved_pct == pct
    assert PriceLimitTradable.DEFAULT_LIMITS[resolved_sector] == resolved_pct


def test_risk_table_covers_exactly_reachable_sectors():
    """档位表的键集 == 撮合侧实际会产出的板块标签集合。

    多一个键（表里有、撮合永远不产出）是死配置；少一个键（撮合产出、
    风控查不到）会让 ``PriceLimitTradable`` fail-closed 拒掉全部委托。
    """
    reachable = {
        ExecutionRules.resolve_sector(ts_code, is_st=is_st, trade_date=trade_date)
        for ts_code, is_st, trade_date, _sector, _pct in GRID
    }
    assert reachable == set(PriceLimitTradable.DEFAULT_LIMITS)


def test_unknown_code_falls_back_to_main_board_limit():
    """无法识别代码时两侧兜底一致（均按主板 10%）。"""
    assert ExecutionRules.resolve_sector('') == '主板'
    assert (ExecutionRules.resolve_limit_pct('')
            == PriceLimitTradable.DEFAULT_LIMITS['主板'])
