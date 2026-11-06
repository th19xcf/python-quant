#!/usr/bin/env python3

"""P2：统一交易日历的口径、退化与集成回归测试。"""

from __future__ import annotations

import os
import time
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from src.data.quality.freshness import _count_trading_days
from src.data.trading_calendar import (
    get_calendar,
    is_trading_day,
    next_trading_day,
    previous_trading_day,
    reset_calendar_cache,
    trading_dates,
)

# 2026 年国庆：10-01~10-07 非交易日，10-08（周四）恢复。
HOLIDAY_WEEKDAYS = (date(2026, 10, 1), date(2026, 10, 2),
                    date(2026, 10, 5), date(2026, 10, 6), date(2026, 10, 7))
INDEX_DATES = (date(2026, 9, 29), date(2026, 9, 30),
               date(2026, 10, 8), date(2026, 10, 9), date(2026, 11, 5))


@pytest.fixture(autouse=True)
def _clear_cache() -> None:
    reset_calendar_cache()
    yield
    reset_calendar_cache()


def _write_index(root: Path, days: tuple[date, ...]) -> Path:
    path = root / 'quotes' / 'index' / 'sh' / '000001.SH.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'date': list(days)}, schema={'date': pl.Date}).write_parquet(path)
    return path


def test_local_index_is_authoritative_and_excludes_holidays(tmp_path: Path) -> None:
    _write_index(tmp_path, INDEX_DATES)
    cal = get_calendar(tmp_path, as_of=date(2026, 11, 6))

    assert cal.source == 'local_index:000001.SH'
    assert cal.is_authoritative
    assert cal.authoritative_end == date(2026, 11, 5)
    assert cal.authoritative_dates() == INDEX_DATES
    assert cal.approx_from == date(2026, 11, 6)

    for day in HOLIDAY_WEEKDAYS:
        assert not cal.is_trading_day(day), f'节假日 {day} 不应是交易日'
    assert cal.is_trading_day(date(2026, 10, 8))
    # 权威段末端之后用工作日近似补齐，否则「今天比数据晚一天」会被算成 0
    assert cal.is_trading_day(date(2026, 11, 6))


def test_count_semantics_is_half_open(tmp_path: Path) -> None:
    cal = get_calendar(tmp_path, as_of=date(2026, 11, 6))
    # 无基准文件 → 工作日近似；(周五, 周一] 只有周一 1 个交易日
    assert cal.count(date(2026, 10, 30), date(2026, 11, 2)) == 1
    assert cal.count(date(2026, 11, 2), date(2026, 11, 2)) == 0
    assert cal.count(date(2026, 11, 3), date(2026, 11, 2)) == 0
    assert trading_dates(date(2026, 10, 30), date(2026, 11, 2), tmp_path) == (
        date(2026, 10, 30), date(2026, 11, 2))


def test_fallback_flags_degradation(tmp_path: Path) -> None:
    cal = get_calendar(tmp_path, as_of=date(2026, 11, 6))
    assert cal.source == 'weekday_fallback'
    assert not cal.is_authoritative
    assert cal.authoritative_dates() == ()
    assert cal.approx_from is not None
    assert not is_trading_day(date(2026, 11, 1), tmp_path)  # 周日


def test_holidays_override_applies_to_fallback(tmp_path: Path) -> None:
    holiday = {date(2026, 11, 2)}  # 周一，人为声明为休市
    cal = get_calendar(tmp_path, as_of=date(2026, 11, 6), holidays=holiday)
    assert not cal.is_trading_day(date(2026, 11, 2))
    assert cal.count(date(2026, 10, 30), date(2026, 11, 2)) == 0


def test_previous_and_next(tmp_path: Path) -> None:
    _write_index(tmp_path, INDEX_DATES)
    assert previous_trading_day(date(2026, 10, 8), tmp_path) == date(2026, 9, 30)
    assert previous_trading_day(date(2026, 10, 8), tmp_path,
                                inclusive=True) == date(2026, 10, 8)
    assert next_trading_day(date(2026, 9, 30), tmp_path) == date(2026, 10, 8)


def test_cache_invalidates_when_index_file_changes(tmp_path: Path) -> None:
    _write_index(tmp_path, INDEX_DATES[:2])
    first = get_calendar(tmp_path, as_of=date(2026, 11, 6))
    assert first.authoritative_end == date(2026, 9, 30)

    path = _write_index(tmp_path, INDEX_DATES)
    # 显式推进 mtime，避免同一时间片写入导致缓存键不变的偶发失败
    bumped = time.time() + 2
    os.utime(path, (bumped, bumped))
    second = get_calendar(tmp_path, as_of=date(2026, 11, 6))
    assert second.authoritative_end == date(2026, 11, 5)


def test_freshness_uses_real_calendar_not_weekday_approximation(tmp_path: Path) -> None:
    """节假日密集区间：真实日历应比「周一至周五」少算 5 个交易日。"""
    _write_index(tmp_path, INDEX_DATES)
    latest, today = date(2026, 9, 30), date(2026, 10, 9)

    weekday_only = _count_trading_days(latest, today, set())
    with_calendar = _count_trading_days(latest, today, set(), store_root=tmp_path)

    assert weekday_only == 7
    assert with_calendar == 2
    assert with_calendar < weekday_only
