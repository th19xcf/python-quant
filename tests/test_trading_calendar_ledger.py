"""交易日历方案 B：并集 + 台账（日期级例外）回归测试。"""
from __future__ import annotations

import os
import time
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from src.data.trading_calendar import (
    SESSION_LEDGER_REL,
    get_calendar,
    reset_calendar_cache,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _clear_cache() -> None:
    reset_calendar_cache()
    yield
    reset_calendar_cache()


def _write_index(root: Path, code: str, days: tuple[date, ...]) -> Path:
    market = code.rsplit('.', 1)[-1].lower()
    path = root / 'quotes' / 'index' / market / f'{code}.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'date': list(days)}, schema={'date': pl.Date}).write_parquet(path)
    return path


def _write_ledger(root: Path, sessions: tuple[date, ...] = (),
                  closed: tuple[date, ...] = ()) -> Path:
    path = root / SESSION_LEDGER_REL
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = ([{'date': d, 'status': 'session'} for d in sessions]
            + [{'date': d, 'status': 'closed'} for d in closed])
    pl.DataFrame(rows, schema={'date': pl.Date, 'status': pl.String}).write_parquet(path)
    return path


def test_union_of_benchmark_members(tmp_path: Path) -> None:
    """并集：两个成员各自的独有日期都必须进日历（不再「第一个非空就停」）。"""
    _write_index(tmp_path, '000001.SH', (date(1991, 9, 27), date(1991, 9, 30)))
    _write_index(tmp_path, '399001.SZ', (date(1991, 9, 30), date(1991, 10, 5)))
    _write_ledger(tmp_path, closed=(date(1991, 10, 12),))

    cal = get_calendar(tmp_path, as_of=date(1991, 10, 20))
    assert cal.authoritative_dates() == (
        date(1991, 9, 27), date(1991, 9, 30), date(1991, 10, 5))
    assert cal.source == 'local_index:union(2)+ledger1'


def test_ledger_session_fills_calendar_gap(tmp_path: Path) -> None:
    """台账 session 补齐指数并集给不出的日期（1991 深市周六 / 1992 漏输出）。"""
    _write_index(tmp_path, '000001.SH', (date(1991, 9, 27), date(1991, 9, 30)))
    _write_ledger(tmp_path, sessions=(date(1991, 9, 28),))

    cal = get_calendar(tmp_path, as_of=date(1991, 10, 5))
    assert date(1991, 9, 28) in cal.dates
    assert cal.authoritative_end == date(1991, 9, 30)
    assert cal.source == 'local_index:union(1)+ledger1'


def test_ledger_closed_beats_index_bar(tmp_path: Path) -> None:
    """台账 closed 优先级最高：指数当日有 bar 也必须剔除（排除表唯一特权）。"""
    _write_index(tmp_path, '000001.SH',
                 (date(1991, 9, 27), date(1991, 9, 30), date(1991, 10, 5)))
    _write_ledger(tmp_path, closed=(date(1991, 9, 30),))

    cal = get_calendar(tmp_path, as_of=date(1991, 10, 10))
    assert date(1991, 9, 30) not in cal.dates
    assert cal.authoritative_dates() == (date(1991, 9, 27), date(1991, 10, 5))


def test_missing_ledger_raises_instead_of_falling_back(tmp_path: Path) -> None:
    """台账缺失不得回落成「只读 000001.SH」（DG-2）。"""
    _write_index(tmp_path, '000001.SH', (date(1991, 9, 27),))
    with pytest.raises(RuntimeError) as excinfo:
        get_calendar(tmp_path, as_of=date(1991, 10, 5))
    assert '台账缺失或为空' in str(excinfo.value)


def test_empty_ledger_raises(tmp_path: Path) -> None:
    _write_index(tmp_path, '000001.SH', (date(1991, 9, 27),))
    path = tmp_path / SESSION_LEDGER_REL
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(schema={'date': pl.Date, 'status': pl.String}).write_parquet(path)
    with pytest.raises(RuntimeError) as excinfo:
        get_calendar(tmp_path, as_of=date(1991, 10, 5))
    assert '台账缺失或为空' in str(excinfo.value)


def test_cache_invalidates_when_ledger_changes(tmp_path: Path) -> None:
    """缓存 key 必须含台账 mtime：台账比指数晚写入时旧日历不得命中。"""
    _write_index(tmp_path, '000001.SH', (date(2026, 9, 29), date(2026, 9, 30)))
    path = _write_ledger(tmp_path, closed=(date(2026, 10, 3),))
    first = get_calendar(tmp_path, as_of=date(2026, 11, 6))
    assert first.authoritative_end == date(2026, 9, 30)

    path = _write_ledger(tmp_path, closed=(date(2026, 10, 3),),
                         sessions=(date(2026, 10, 8),))
    bumped = time.time() + 2
    os.utime(path, (bumped, bumped))
    second = get_calendar(tmp_path, as_of=date(2026, 11, 6))
    assert second.authoritative_end == date(2026, 10, 8)


def test_weekday_fallback_needs_no_ledger(tmp_path: Path) -> None:
    """全无指数文件时仍走工作日近似（退化的是「所有源都不可用」）。"""
    cal = get_calendar(tmp_path, as_of=date(2026, 11, 6))
    assert cal.source == 'weekday_fallback'
    assert cal.authoritative_end is None


def test_registry_has_no_runtime_consumer() -> None:
    """DG-1：registry 退役后运行期零引用，台账是唯一一份日期级裁定。"""
    offenders = [
        path.relative_to(PROJECT_ROOT).as_posix()
        for path in (PROJECT_ROOT / 'src').rglob('*.py')
        if 'weekend_calendar_registry' in path.read_text(encoding='utf-8')
    ]
    assert offenders == [], offenders


def test_session_ledger_read_only_by_calendar_module() -> None:
    """台账只有 src/data/trading_calendar.py 一个运行期读取方。"""
    consumers = [
        path.relative_to(PROJECT_ROOT).as_posix()
        for path in (PROJECT_ROOT / 'src').rglob('*.py')
        if 'session_ledger' in path.read_text(encoding='utf-8')
    ]
    assert consumers == ['src/data/trading_calendar.py'], consumers
