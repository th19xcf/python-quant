"""分红日频增量的窗口与内容比对单测（不触网、不写正式数据）。"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.refresh_dividend_daily import (  # noqa: E402
    _content_changed,
    _quarter_periods,
    _window_start,
)


@pytest.mark.parametrize('target, expected', [
    (date(2026, 11, 18), ['20260930', '20260630']),
    (date(2026, 1, 5), ['20251231', '20250930']),
    (date(2026, 3, 31), ['20260331', '20251231']),
])
def test_quarter_periods(target: date, expected: list[str]) -> None:
    assert _quarter_periods(target) == expected


def test_window_start_prefers_last_success_and_clamps() -> None:
    target = date(2026, 11, 18)
    state = {'fund': {'last_success': '2026-11-14'}}
    assert _window_start(target, state, 'fund', 30) == date(2026, 11, 15)
    same_day = {'fund': {'last_success': '2026-11-18'}}
    assert _window_start(target, same_day, 'fund', 30) == target
    assert _window_start(target, {}, 'fund', 7) == date(2026, 11, 12)


def test_content_changed_ignores_row_order_but_sees_value_change() -> None:
    frame = pl.DataFrame({
        'ts_code': ['000001.SZ', '000002.SZ'],
        'cash_div': [1.0, 2.0],
        'ex_date': [date(2026, 11, 10), date(2026, 11, 11)],
    })
    assert not _content_changed(frame, frame.reverse())
    changed = frame.with_columns(pl.when(pl.col('ts_code') == '000002.SZ')
                                 .then(3.0).otherwise(pl.col('cash_div'))
                                 .alias('cash_div'))
    assert _content_changed(frame, changed)
