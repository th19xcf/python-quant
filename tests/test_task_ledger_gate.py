"""单会话闸门与收口自动记账的用例。

阈值同时用字面量（60 / 100）断言：把 ``HARD_STOP_TOOL_CALLS`` 改成极大值时，
本文件必须变红，不允许写成恒真。
"""

from __future__ import annotations

import csv
from pathlib import Path

from tools.task_ledger import (
    ABSOLUTE_MAX_TOOL_CALLS,
    HARD_STOP_TOOL_CALLS,
    append_finalize_row,
    session_gate,
)


def test_thresholds_are_60_and_100():
    assert HARD_STOP_TOOL_CALLS == 60
    assert ABSOLUTE_MAX_TOOL_CALLS == 100


def test_gate_continues_below_hard_stop():
    assert session_gate(0) == 'continue'
    assert session_gate(59) == 'continue'


def test_gate_refuses_new_work_at_60():
    """达到 60 次即拒绝继续（写交接后回报），82 次实测样本必须落在停手区。"""
    assert session_gate(60) == 'handoff'
    assert session_gate(82) == 'handoff'
    assert session_gate(99) == 'handoff'


def test_gate_absolute_stop_at_100():
    assert session_gate(100) == 'stop'
    assert session_gate(106) == 'stop'


def _rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding='utf-8', newline='') as handle:
        return list(csv.DictReader(handle))


def test_append_finalize_row_creates_file_with_header(tmp_path):
    ledger = tmp_path / 'nested' / 'task_ledger.csv'
    state = append_finalize_row(
        ledger, commit='abcdef1234567890', task_id='sess-1', tool_calls=82, duration_min=31.5
    )
    assert state == 'written'
    lines = ledger.read_text(encoding='utf-8').splitlines()
    assert lines[0] == 'dispatched_at,session_id,title,phase,turns,tool_calls,duration_min,note'
    rows = _rows(ledger)
    assert len(rows) == 1
    row = rows[0]
    assert row['session_id'] == 'sess-1'
    assert row['title'] == 'sess-1'
    assert row['phase'] == 'done'
    assert row['tool_calls'] == '82'
    assert row['duration_min'] == '31.5'
    assert 'commit abcdef1' in row['note']


def test_append_finalize_row_is_idempotent_per_commit(tmp_path):
    """同一提交重复收口只写一行；第二次返回 duplicate。"""
    ledger = tmp_path / 'task_ledger.csv'
    first = append_finalize_row(ledger, commit='deadbee1', task_id='sess-1', tool_calls=3)
    second = append_finalize_row(ledger, commit='deadbee1', task_id='sess-1', tool_calls=4)
    assert (first, second) == ('written', 'duplicate')
    assert len(_rows(ledger)) == 1


def test_append_finalize_row_marks_unknown_calls(tmp_path):
    """调用数与耗时取不到时写 unknown / 留空，任务标识退化为提交短哈希。"""
    ledger = tmp_path / 'task_ledger.csv'
    append_finalize_row(ledger, commit='cafe1234', tool_calls=None, duration_min=None)
    row = _rows(ledger)[0]
    assert row['tool_calls'] == 'unknown'
    assert row['duration_min'] == ''
    assert row['session_id'] == 'cafe123'
