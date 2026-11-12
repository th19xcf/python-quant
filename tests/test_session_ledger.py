"""session 台账（唯一日期级例外裁定）schema 约束的反例自测。"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.build_session_ledger import COLUMNS, _check_consistency  # noqa: E402


def _row(day: date = date(1991, 1, 5), status: str = 'session',
         kind: str = 'weekend_session_1991', unresolved: bool = False,
         note: str | None = None) -> dict:
    return {
        'date': day,
        'status': status,
        'kind': kind,
        'evidence': '夹具',
        'source': 'index:399001.SZ',
        'evidence_path': 'quotes/index/sz/399001.SZ.parquet',
        'effective_from': date(1990, 1, 1),
        'effective_to': date(2099, 12, 31),
        'unresolved': unresolved,
        'decided_at': date(2026, 11, 13),
        'note': note,
    }


def _frame(*rows: dict) -> pl.DataFrame:
    return pl.DataFrame(list(rows), schema=COLUMNS)


def test_valid_rows_pass() -> None:
    assert _check_consistency(_frame(_row())) == []


def test_null_status_fails() -> None:
    """空值从 0 变成 1 必须被断言捕获（DG-31）。"""
    problems = _check_consistency(_frame(_row(status=None)))
    assert any('status 有 1 个空值' in p for p in problems), problems


def test_duplicate_date_fails() -> None:
    problems = _check_consistency(_frame(_row(), _row()))
    assert any('date 不唯一' in p for p in problems), problems


def test_forbidden_1993_05_01_fails() -> None:
    problems = _check_consistency(_frame(_row(day=date(1993, 5, 1))))
    assert any('禁止日期' in p for p in problems), problems


def test_unresolved_requires_note_and_session() -> None:
    problems = _check_consistency(_frame(_row(kind='unresolved_session', unresolved=True)))
    assert any('unresolved 行缺 note' in p for p in problems), problems
    problems = _check_consistency(_frame(_row(status='closed', kind='unresolved_session',
                                              unresolved=True, note='x')))
    assert any('unresolved 行必须 status=session' in p for p in problems), problems


def test_nav_placeholder_must_be_closed() -> None:
    problems = _check_consistency(_frame(_row(kind='nav_placeholder_weekend')))
    assert any('nav_placeholder 必须 closed' in p for p in problems), problems
