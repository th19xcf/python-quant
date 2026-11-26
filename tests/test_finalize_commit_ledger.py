"""finalize_commit 记账标题与失败可见性（2026-11-26 回归）。

以前缺省把提交短哈希当 title 传给 task_ledger，被「title 不得为哈希」校验拒绝后
只打一行 warning、进程仍返回 0，导致「提交即记账」静默失效（commit 0263a78）。
"""
from __future__ import annotations

import argparse
import re

import pytest

from tools import finalize_commit as fc

_HASH_LIKE = re.compile(r'^[0-9a-f]{7,40}$')


def _args(**kwargs) -> argparse.Namespace:
    base = {'title': '', 'task_id': '', 'tool_calls': None, 'duration_min': None}
    base.update(kwargs)
    return argparse.Namespace(**base)


@pytest.fixture()
def captured(monkeypatch):
    calls: list[dict] = []

    def fake_append(ledger, *, commit, task_id=None, tool_calls=None,
                    duration_min=None, **kwargs):
        calls.append({'commit': commit, 'task_id': task_id,
                      'tool_calls': tool_calls})
        return 'appended'

    monkeypatch.setattr(fc, 'append_finalize_row', fake_append)
    return calls


def test_default_title_is_commit_subject_not_hash(captured, monkeypatch):
    monkeypatch.delenv('QUANT_TASK_ID', raising=False)
    message = 'chore(rules): 新增 R18 新路径失败分支必验\n\n正文行不参与标题'

    ok = fc.record_ledger('0263a78abcdef0123456789', _args(), message)

    assert ok is True
    title = captured[0]['task_id']
    assert title == 'chore(rules): 新增 R18 新路径失败分支必验'
    assert not _HASH_LIKE.match(title)


def test_explicit_title_is_used_verbatim(captured):
    ok = fc.record_ledger('0263a78abcdef0123456789',
                          _args(title='规则更新R18与派单模板约束'),
                          'chore(rules): 新增 R18')

    assert ok is True
    assert captured[0]['task_id'] == '规则更新R18与派单模板约束'


def test_task_id_beats_message_subject(captured, monkeypatch):
    monkeypatch.setenv('QUANT_TASK_ID', '')
    ok = fc.record_ledger('0263a78abcdef0123456789',
                          _args(task_id='finalize记账修复'), 'chore(x): y')

    assert ok is True
    assert captured[0]['task_id'] == 'finalize记账修复'


def test_ledger_failure_returns_false_and_prints(captured, monkeypatch, capsys):
    def boom(*args, **kwargs):
        raise RuntimeError('拒绝写入：title 疑似 commit 哈希')

    monkeypatch.setattr(fc, 'append_finalize_row', boom)

    ok = fc.record_ledger('0263a78abcdef0123456789', _args(), 'chore(x): y')

    assert ok is False
    err = capsys.readouterr().err
    assert '记账失败' in err
