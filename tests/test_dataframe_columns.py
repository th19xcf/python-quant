#!/usr/bin/env python3

"""日期列判定的唯一实现：``date`` / ``trade_date`` 双轨收敛后的边界用例。"""

from __future__ import annotations

import pytest

from src.utils.dataframe_columns import resolve_date_column


def test_prefers_date_when_both_present():
    assert resolve_date_column(['ts_code', 'date', 'trade_date']) == 'date'


def test_falls_back_to_trade_date():
    assert resolve_date_column(['ts_code', 'trade_date', 'close']) == 'trade_date'


def test_prefer_overrides_when_both_present():
    assert resolve_date_column(
        ['date', 'trade_date'], prefer='trade_date') == 'trade_date'


def test_returns_none_without_date_columns():
    assert resolve_date_column(['ts_code', 'close']) is None


def test_accepts_any_iterable():
    assert resolve_date_column(iter(['trade_date', 'close'])) == 'trade_date'


def test_rejects_unknown_prefer():
    with pytest.raises(ValueError, match='prefer'):
        resolve_date_column(['date'], prefer='trade_day')
