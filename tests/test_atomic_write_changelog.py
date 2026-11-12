"""`atomic_write` 台账自动埋点用例：只记正式层、递归防护、失败不反噬。"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime

import polars as pl
import pytest

from src.data import changelog
from src.data.atomic_write import (
    suspend_changelog_recording,
    write_parquet_atomic,
    write_text_atomic,
)
from tools.check_changelog import check_ledger


@pytest.fixture
def store_env(tmp_path, monkeypatch):
    """把仓库根指到 tmp_path，并准备一个正式层目录。"""
    monkeypatch.setattr(changelog, 'repo_root', lambda: tmp_path)
    monkeypatch.setattr(changelog, 'changelog_path', lambda: tmp_path / 'logs' / 'data_changelog.csv')
    monkeypatch.setenv('QUANT_CHANGELOG_TASK', 'test_task')
    store = tmp_path / 'data' / 'local_store' / 'quotes'
    store.mkdir(parents=True)
    return tmp_path, store, tmp_path / 'logs' / 'data_changelog.csv'


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def test_formal_write_records_create_then_update(store_env):
    _, store, ledger = store_env
    target = store / 'x.txt'

    write_text_atomic('one', target)
    rows = changelog.read_events(ledger)
    assert [(r['action'], r['bytes'], r['prev_sha256']) for r in rows] == [
        ('create', '3', ''),
    ]
    assert rows[0]['path'] == 'data/local_store/quotes/x.txt'
    assert rows[0]['sha256'] == _sha(b'one')
    assert rows[0]['task'] == 'test_task'

    write_text_atomic('two', target)
    rows = changelog.read_events(ledger)
    assert len(rows) == 2
    assert rows[1]['action'] == 'update'
    assert rows[1]['prev_sha256'] == rows[0]['sha256']
    assert rows[1]['sha256'] == _sha(b'two')


def test_parquet_formal_write_records(store_env):
    _, store, ledger = store_env
    target = store / 'p.parquet'

    write_parquet_atomic(pl.DataFrame({'a': [1, 2, 3]}), target)

    rows = changelog.read_events(ledger)
    assert len(rows) == 1
    assert rows[0]['action'] == 'create'
    assert rows[0]['bytes'] == str(target.stat().st_size)
    assert rows[0]['sha256'] == _sha(target.read_bytes())


def test_non_formal_paths_are_never_recorded(store_env):
    tmp, store, ledger = store_env
    write_text_atomic('x', tmp / 'reports' / 'a.txt')
    write_text_atomic('x', tmp / '_staging' / 'task_20261112' / 'b.txt')
    write_text_atomic('x', tmp / 'logs' / 'c.txt')
    write_text_atomic('x', store / '_backup' / 'd.txt')
    write_text_atomic('x', tmp / 'research' / 'events' / 'data' / 'e.txt')

    assert not ledger.exists()


def test_suspend_disables_recording(store_env):
    _, store, ledger = store_env

    with suspend_changelog_recording():
        write_text_atomic('x', store / 'z.txt')

    assert (store / 'z.txt').read_text() == 'x'
    assert not ledger.exists()


def test_ledger_write_itself_is_not_recorded(store_env):
    """把台账放进正式层这种最坏情形下，表头升级也不能触发自我记账。"""
    _, store, _ = store_env
    ledger_inside_store = store / 'ledger.csv'
    old_header = ','.join(changelog.FIELDS[:-1])
    old_row = 'r0,2026-11-05T00:00:00,t,create,research/a,1,' + 'a' * 64 + ',system,r'
    ledger_inside_store.write_text(f'{old_header}\n{old_row}\n', encoding='utf-8', newline='')

    changelog.append_event(
        action='update', path='data/local_store/quotes/x.txt', task='t',
        reason='r', sha256='b' * 64, size_bytes=1, csv_file=ledger_inside_store,
    )

    rows = changelog.read_events(ledger_inside_store)
    assert len(rows) == 2
    assert not any(r['path'].endswith('ledger.csv') for r in rows)


def test_manifest_sha_used_when_bytes_and_mtime_match(store_env, monkeypatch):
    _, store, ledger = store_env
    target = store / 'y.txt'
    write_text_atomic('old', target)
    old_sha = _sha(b'old')

    stat = target.stat()
    mtime = datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%dT%H:%M:%S')
    (store / 'manifest.json').write_text(json.dumps({'files': [
        {'path': 'y.txt', 'bytes': stat.st_size, 'mtime': mtime, 'sha256': old_sha},
    ]}), encoding='utf-8')

    def _must_not_hash(_path):
        raise AssertionError('manifest 命中时不应再流式重算旧文件')

    monkeypatch.setattr('src.data.atomic_write._hash_file', _must_not_hash)
    write_text_atomic('new', target)

    rows = changelog.read_events(ledger)
    assert rows[-1]['prev_sha256'] == old_sha


def test_manifest_stale_bytes_falls_back_to_streaming(store_env):
    _, store, ledger = store_env
    target = store / 'm.txt'
    write_text_atomic('old', target)
    old_sha = _sha(b'old')
    stat = target.stat()
    mtime = datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%dT%H:%M:%S')
    (store / 'manifest.json').write_text(json.dumps({'files': [
        {'path': 'm.txt', 'bytes': stat.st_size + 1, 'mtime': mtime, 'sha256': 'f' * 64},
    ]}), encoding='utf-8')

    write_text_atomic('new', target)

    rows = changelog.read_events(ledger)
    assert rows[-1]['prev_sha256'] == old_sha


def test_recording_failure_keeps_data_and_writes_failure_log(store_env, monkeypatch):
    tmp, store, ledger = store_env
    write_text_atomic('first', store / 'ok.txt')  # 先造出合法台账
    assert ledger.exists()

    def _boom(**_kwargs):
        raise RuntimeError('台账坏掉了')

    monkeypatch.setattr(changelog, 'append_event', _boom)
    target = store / 'keep.txt'
    write_text_atomic('keep', target)

    assert target.read_text() == 'keep'
    assert not (store / '.keep.txt.tmp').exists()
    failures = tmp / 'logs' / 'data_changelog_failures.csv'
    assert failures.exists()
    lines = [line for line in failures.read_text(encoding='utf-8').splitlines() if line.strip()]
    assert len(lines) == 2  # 表头 + 1 条失败
    assert 'RuntimeError' in lines[1]

    result = check_ledger(ledger, None, failures)
    assert not result.ok
    assert any('记账失败' in item for item in result.failures)


def test_failure_log_absent_means_pass(store_env):
    _, store, ledger = store_env
    write_text_atomic('ok', store / 'ok.txt')
    result = check_ledger(ledger, None, store.parent.parent / 'logs' / 'data_changelog_failures.csv')
    assert result.ok, result.failures
