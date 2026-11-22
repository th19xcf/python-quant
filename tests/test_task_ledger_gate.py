"""单会话闸门与收口自动记账的用例。

阈值同时用字面量（60 / 100）断言：把 ``HARD_STOP_TOOL_CALLS`` 改成极大值时，
本文件必须变红，不允许写成恒真。
"""

from __future__ import annotations

import csv
from pathlib import Path

from tools.task_ledger import (
    ABSOLUTE_MAX_TOOL_CALLS,
    FIELDS,
    HARD_STOP_TOOL_CALLS,
    HEADER_VARIANTS,
    TURN_SWITCH_ROUNDS,
    _read_rows,
    append_finalize_row,
    main,
    session_gate,
)


def test_thresholds_are_60_and_100_and_15_rounds():
    assert HARD_STOP_TOOL_CALLS == 60
    assert ABSOLUTE_MAX_TOOL_CALLS == 100
    assert TURN_SWITCH_ROUNDS == 15


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
    assert lines[0] == ','.join(FIELDS)
    rows = _rows(ledger)
    assert len(rows) == 1
    row = rows[0]
    assert row['session_id'] == 'sess-1'
    assert row['title'] == 'sess-1'
    assert row['phase'] == 'done'
    assert row['rounds'] == 'unknown'
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


def test_add_derives_rounds_from_prior_rows(tmp_path):
    """未显式给 --rounds 时按该任务既有行数 +1 累积。"""
    ledger = tmp_path / 'task_ledger.csv'
    base = ['--ledger', str(ledger), 'add', '--session-id', 's1', '--title', 'T1']
    assert main(base + ['--phase', 'dispatch', '--apply']) == 0
    assert main(base + ['--phase', 'done', '--tool-calls', '40',
                        '--duration-min', '12.5', '--apply']) == 0
    rows = _read_rows(ledger)
    assert [row['rounds'] for row in rows] == ['1', '2']
    assert rows[1]['tool_calls'] == '40'


def test_add_accepts_explicit_rounds(tmp_path):
    ledger = tmp_path / 'task_ledger.csv'
    main(['--ledger', str(ledger), 'add', '--session-id', 's1', '--title', 'T1',
          '--phase', 'done', '--rounds', '17', '--apply'])
    assert _read_rows(ledger)[0]['rounds'] == '17'


def test_legacy_header_reads_and_upgrades_on_append(tmp_path):
    """旧 8 列账本可读；追加时表头升级为当前列，旧行内容不改、rounds 留空。"""
    ledger = tmp_path / 'task_ledger.csv'
    header = ','.join(HEADER_VARIANTS[-1])
    ledger.write_text(
        header + '\n2026-11-17T09:00:00,s1,T1,dispatch,3,20,5.0,\n', encoding='utf-8')

    rows = _read_rows(ledger)
    assert rows[0]['rounds'] == ''
    assert rows[0]['turns'] == '3'

    main(['--ledger', str(ledger), 'add', '--session-id', 's1', '--title', 'T1',
          '--phase', 'done', '--tool-calls', '30', '--apply'])
    lines = ledger.read_text(encoding='utf-8').splitlines()
    assert lines[0] == ','.join(FIELDS)
    rows = _read_rows(ledger)
    assert len(rows) == 2
    assert rows[0]['rounds'] == '' and rows[0]['turns'] == '3'
    assert rows[1]['rounds'] == '2'


def _write_ledger(path: Path, rows: list[list[str]]) -> None:
    body = '\n'.join(','.join(row) for row in rows)
    path.write_text(','.join(FIELDS) + '\n' + body + '\n', encoding='utf-8')


def test_status_sorted_desc_with_all_fields(tmp_path, capsys):
    ledger = tmp_path / 'task_ledger.csv'
    _write_ledger(ledger, [
        ['2026-11-17T09:00:00', 'sA', '任务A', 'dispatch', '1', '2', '', '', ''],
        ['2026-11-17T09:00:00', 'sA', '任务A', 'done', '', '3', '80', '20.5',
         'commit abc1234'],
        ['2026-11-18T08:00:00', 'sB', '任务B', 'dispatch', '', '2', '10', '5', ''],
    ])
    assert main(['--ledger', str(ledger), 'status']) == 0
    out = capsys.readouterr().out.splitlines()
    assert len(out) == 2
    assert out[0] == '任务B | sB | rounds=2 | calls=10 | min=5 | commit=-'
    assert out[1] == '任务A | sA | rounds=3 | calls=80 | min=20.5 | commit=abc1234'


def test_status_marks_switch_at_15_rounds(tmp_path, capsys):
    ledger = tmp_path / 'task_ledger.csv'
    _write_ledger(ledger, [
        ['2026-11-18T08:00:00', 'sC', '任务C', 'dispatch', '', '15', '5', '3', ''],
        ['2026-11-18T08:00:00', 'sD', '任务D', 'dispatch', '', '14', '5', '3', ''],
    ])
    assert main(['--ledger', str(ledger), 'status']) == 0
    out = capsys.readouterr().out.splitlines()
    assert out[0].endswith('switch=yes')
    assert 'switch=yes' not in out[1]


def test_finalize_without_dispatch_warns_unknown_rounds(tmp_path, capsys):
    """找不到派单行时轮次写 unknown、stderr 告警，但不抛错、不阻断收口。"""
    ledger = tmp_path / 'task_ledger.csv'
    state = append_finalize_row(
        ledger, commit='bee1234abcd', task_id='无派单任务', tool_calls=7)
    assert state == 'written'
    assert _rows(ledger)[0]['rounds'] == 'unknown'
    assert 'unknown' in capsys.readouterr().err


def test_finalize_with_dispatch_counts_rounds(tmp_path):
    ledger = tmp_path / 'task_ledger.csv'
    main(['--ledger', str(ledger), 'add', '--session-id', 'T9', '--title', 'T9',
          '--phase', 'dispatch', '--apply'])
    append_finalize_row(ledger, commit='feed1234567', task_id='T9', tool_calls=5)
    assert _rows(ledger)[-1]['rounds'] == '2'
