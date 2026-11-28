"""数据维护统一入口（tools/maintenance_entry.py）的回归测试。

覆盖：
1. 路由：``--force`` 优先；未强制时按交易日历（此处 monkeypatch）判 daily/weekly；
2. 周度清单：五组步骤的顺序与关键命令（含 ``--end`` 取当天、外部采集串行）；
3. 锁：活跃锁挡下返回 None 并写「已有实例在跑」；陈旧锁归档后接管；
4. 周度失败语义：单步失败继续跑完并汇总，退出码 1；
5. 入口退出码：撞锁为 3；dry-run 不取锁、不执行。
"""

import json
import os
import sys
import types
from datetime import date

import pytest

from tools import maintenance_entry as me


@pytest.fixture()
def isolated(tmp_path, monkeypatch):
    """把锁与日志重定向到 tmp，避免碰仓库真实文件。"""
    lock = tmp_path / 'maintenance_entry.lock'
    daily_lock = tmp_path / 'daily_update.lock'
    log = tmp_path / 'maintenance_entry.log'
    monkeypatch.setattr(me, 'LOCK_PATH', lock)
    monkeypatch.setattr(me, 'DAILY_LOCK_PATH', daily_lock)
    monkeypatch.setattr(me, 'LOG_PATH', log)
    return types.SimpleNamespace(lock=lock, daily_lock=daily_lock, log=log)


def test_force_overrides_calendar(monkeypatch):
    def boom(_day):
        raise AssertionError('强制模式下不应查日历')

    monkeypatch.setattr(me, '_is_trading_day', boom)
    assert me.decide_mode(date(2026, 11, 28), 'daily') == 'daily'
    assert me.decide_mode(date(2026, 11, 27), 'weekly') == 'weekly'


def test_auto_routes_by_trading_day(monkeypatch):
    monkeypatch.setattr(me, '_is_trading_day', lambda day: day == date(2026, 11, 27))
    assert me.decide_mode(date(2026, 11, 27), None) == 'daily'
    assert me.decide_mode(date(2026, 11, 28), None) == 'weekly'


def test_weekly_steps_follow_prompt_order(monkeypatch):
    day = date(2026, 11, 28)
    monkeypatch.setattr(me, '_last_trading_day', lambda d: date(2026, 11, 27))
    steps = me.weekly_steps(day)
    labels = [label for label, _ in steps]
    assert labels[0].startswith('①')
    assert labels[1].startswith('②') and labels[2].startswith('②')
    assert labels[3].startswith('③') and labels[4].startswith('③')
    assert labels[5].startswith('④')
    assert all(label.startswith('⑤') for label in labels[6:])
    assert labels[-1] == '⑤ build_new_factors'

    cmds = dict(steps)
    assert 'tools/research/prepare_monthly_samples.py' in cmds['⑤ prepare_monthly_samples']
    assert cmds['⑤ prepare_monthly_samples'][-2:] == ['--end', '2026-11-27']
    assert '--workers' in cmds['② fetch_stock_shares']
    assert '--allow-remote' in cmds['③ backfill_events_fetch']
    assert cmds['③ backfill_events_import'][-2:] == ['--import-only', '--commit']


def test_active_lock_blocks_and_logs(isolated, monkeypatch):
    isolated.lock.write_text(json.dumps({'pid': os.getppid(), 'started_at': me._now().isoformat()}),
                             encoding='utf-8')
    assert me.acquire_lock('weekly', date(2026, 11, 28)) is None
    assert '已有实例在跑' in isolated.log.read_text(encoding='utf-8')


def test_daily_lock_blocks_weekly(isolated):
    isolated.daily_lock.write_text(
        json.dumps({'pid': os.getppid(), 'started_at': me._now().isoformat()}), encoding='utf-8')
    assert me.acquire_lock('weekly', date(2026, 11, 28)) is None
    assert '日更入口锁' in isolated.log.read_text(encoding='utf-8')


def test_stale_lock_is_archived_and_taken(isolated):
    isolated.lock.write_text(
        json.dumps({'pid': 999999, 'started_at': '2020-01-01T00:00:00'}), encoding='utf-8')
    path = me.acquire_lock('weekly', date(2026, 11, 28))
    assert path == isolated.lock
    assert path.is_file()
    archived = list(isolated.lock.parent.glob('maintenance_entry.lock.stale.*'))
    assert len(archived) == 1


def test_weekly_continues_after_failure():
    fail = [sys.executable, '-c', 'raise SystemExit(5)']
    ok = [sys.executable, '-c', 'raise SystemExit(0)']
    steps = (('bad', fail), ('good', ok))
    rc, failures = me.run_steps(steps, continue_on_error=True)
    assert rc == 1
    assert failures == ['bad（rc=5）']


def test_daily_stops_at_first_failure():
    fail = [sys.executable, '-c', 'raise SystemExit(4)']
    ok = [sys.executable, '-c', 'raise SystemExit(0)']
    rc, failures = me.run_steps((('bad', fail), ('good', ok)), continue_on_error=False)
    assert rc == 4
    assert failures == ['bad（rc=4）']


def test_main_exits_3_on_lock_conflict(isolated, monkeypatch):
    monkeypatch.setattr(me, '_is_trading_day', lambda day: True)
    isolated.lock.write_text(json.dumps({'pid': os.getppid(), 'started_at': me._now().isoformat()}),
                             encoding='utf-8')
    assert me.main(['--force', 'daily']) == me.EXIT_LOCK_BUSY


def test_main_dry_run_takes_no_lock(isolated, monkeypatch):
    monkeypatch.setattr(me, '_is_trading_day', lambda day: False)
    assert me.main(['--date', '2026-11-28', '--dry-run']) == 0
    assert not isolated.lock.exists()
    assert not isolated.log.exists()
