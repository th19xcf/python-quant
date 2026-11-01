#!/usr/bin/env python3
"""
P1 加固项回归测试

覆盖：
- R5 连接池显式上限（max_overflow / pool_timeout 配置与校验）
- R7 插件加载前的名称与路径校验
- R8 任务超时真正返回（不再被 ThreadPoolExecutor.shutdown(wait=True) 阻塞）+ 协作式取消
"""

import time

import pytest

from src.plugin.plugin_manager import PluginManager, is_safe_module_name
from src.ui.task_manager import TaskManager, TaskRunner
from src.utils.config import DatabaseSettings


class TestPoolLimits:
    """R5：连接池必须有显式的溢出上限与等待超时"""

    def test_defaults(self):
        db = DatabaseSettings()
        assert db.max_overflow == 10
        assert db.pool_timeout == 30

    def test_zero_overflow_allowed(self):
        db = DatabaseSettings(max_overflow=0)
        assert db.max_overflow == 0

    def test_negative_overflow_rejected(self):
        with pytest.raises(ValueError):
            DatabaseSettings(max_overflow=-1)

    def test_zero_pool_timeout_rejected(self):
        with pytest.raises(ValueError):
            DatabaseSettings(pool_timeout=0)


class TestPluginNameGuard:
    """R7：插件名不得遮蔽标准库/内建模块"""

    def test_stdlib_name_rejected(self):
        assert not is_safe_module_name('json')
        assert not is_safe_module_name('sys')

    def test_normal_name_accepted(self):
        assert is_safe_module_name('kline_visualization')

    def test_illegal_identifier_rejected(self):
        assert not is_safe_module_name('bad-name')


class TestPluginPathGuard:
    """R7：插件路径必须位于已配置的插件目录内"""

    def _manager(self, plugin_dirs):
        manager = PluginManager.__new__(PluginManager)
        manager.plugin_dirs = list(plugin_dirs)
        return manager

    def test_inside_dir_accepted(self, tmp_path):
        plugin_file = tmp_path / 'demo.py'
        plugin_file.write_text('# demo', encoding='utf-8')
        assert self._manager([str(tmp_path)])._is_inside_plugin_dirs(str(plugin_file))

    def test_outside_dir_rejected(self, tmp_path):
        outside = tmp_path / 'outside'
        outside.mkdir()
        plugin_file = outside / 'demo.py'
        plugin_file.write_text('# demo', encoding='utf-8')
        assert not self._manager([str(tmp_path / 'plugins')])._is_inside_plugin_dirs(str(plugin_file))


class TestIndicatorLazyRegistration:
    """P1-5：指标注册改为惰性 + 后台预热"""

    def test_manager_construction_does_not_register(self):
        import src.tech_analysis.indicator_manager as im

        if im._indicators_registered:
            pytest.skip('其他用例已触发指标注册')
        im.IndicatorManager()
        assert not im._indicators_registered

    def test_ensure_registered_is_idempotent(self):
        from src.tech_analysis.indicator_manager import (
            ensure_indicators_registered,
            global_indicator_registry,
        )

        ensure_indicators_registered()
        first = set(global_indicator_registry.get_supported_indicators())
        ensure_indicators_registered()
        assert first == set(global_indicator_registry.get_supported_indicators())
        assert 'ma' in first


class TestSharedExecutor:
    """P1-7：任务执行复用同一个线程池"""

    def test_executor_is_reused(self):
        from src.ui import task_manager

        assert task_manager._get_executor() is task_manager._get_executor()


def _sleepy_task(task_id=None, signals=None):
    time.sleep(1.0)
    return 'done'


def _cooperative_task(cancel_event=None, task_id=None, signals=None):
    deadline = time.time() + 1.0
    while time.time() < deadline and not cancel_event.is_set():
        time.sleep(0.02)
    return 'cancelled-early'


class TestTaskTimeout:
    """R8：任务超时必须立刻返回，而不是等任务线程自然结束"""

    def test_timeout_returns_promptly(self):
        runner = TaskRunner('t-1', 'sleepy', _sleepy_task, timeout_seconds=0.15)
        start = time.time()
        with pytest.raises(TimeoutError):
            runner._run_with_timeout()
        elapsed = time.time() - start
        assert elapsed < 0.5, f'超时后仍阻塞 {elapsed:.2f}s'

    def test_cancel_event_injected_when_declared(self):
        runner = TaskRunner('t-2', 'cooperative', _cooperative_task, timeout_seconds=0.5)
        runner.cancel()
        result = runner._run_with_timeout()
        assert result == 'cancelled-early'

    def test_cancel_event_not_injected_for_plain_func(self):
        runner = TaskRunner('t-3', 'sleepy', _sleepy_task, timeout_seconds=5)
        assert not runner._accepts_cancel_event()
        assert runner._run_with_timeout() == 'done'


class TestTaskRunnerSignalDispatch:
    """任务信号分发：成功/异常/超时都必须回传终止信号，异常不穿透 run()"""

    def test_run_emits_started_completed_finished(self, qapp):
        seen = {}
        runner = TaskRunner('ok-1', 'ok-task', lambda **_: 42, timeout_seconds=5)
        runner.signals.started.connect(
            lambda tid, name: seen.update(started=(tid, name))
        )
        runner.signals.completed.connect(
            lambda tid, result: seen.update(completed=(tid, result))
        )
        runner.signals.error.connect(
            lambda tid, msg: seen.update(error=(tid, msg))
        )
        runner.signals.finished.connect(lambda tid: seen.update(finished=tid))

        runner.run()

        assert seen['started'] == ('ok-1', 'ok-task')
        assert seen['completed'] == ('ok-1', 42)
        assert 'error' not in seen
        assert seen['finished'] == 'ok-1'

    def test_run_converts_exception_to_error_signal(self, qapp):
        def _boom(**_kwargs):
            raise RuntimeError('boom')

        seen = {}
        runner = TaskRunner('err-1', 'bad-task', _boom, timeout_seconds=5)
        runner.signals.error.connect(
            lambda tid, msg: seen.update(error=(tid, msg))
        )
        runner.signals.finished.connect(lambda tid: seen.update(finished=tid))

        runner.run()

        assert seen['error'][0] == 'err-1'
        assert seen['error'][1].startswith('任务执行失败')
        assert 'boom' in seen['error'][1]
        assert seen['finished'] == 'err-1'

    def test_run_timeout_dispatches_error_and_marks_cancelled(self, qapp):
        seen = {}
        runner = TaskRunner('to-1', 'slow-task', _sleepy_task, timeout_seconds=0.1)
        runner.signals.error.connect(
            lambda tid, msg: seen.update(error=(tid, msg))
        )
        runner.signals.finished.connect(lambda tid: seen.update(finished=tid))

        runner.run()

        assert runner.is_cancelled is True
        assert runner.cancel_event.is_set()
        assert '超时' in seen['error'][1]
        assert seen['finished'] == 'to-1'


class TestTaskManagerLifecycle:
    """任务管理器：取消、状态查询、统计回调与共享池调度"""

    def test_cancel_task_and_status_queries(self, qapp):
        manager = TaskManager()
        runner = TaskRunner('t-1', 'cancel-me', _cooperative_task, timeout_seconds=5)
        manager.tasks['t-1'] = runner
        manager.task_stats['total'] = 1
        manager.task_stats['running'] = 1

        assert manager.get_task_status('t-1') == {
            'task_id': 't-1', 'task_name': 'cancel-me', 'is_running': True,
        }
        assert manager.get_all_tasks() == {
            't-1': {'task_name': 'cancel-me', 'is_running': True},
        }
        assert manager.get_task_status('missing') is None

        manager.cancel_task('t-1')
        assert runner.is_cancelled is True
        assert runner.cancel_event.is_set()

        manager.clear_completed_tasks()
        assert 't-1' not in manager.tasks

        manager.reset_task_stats()
        assert manager.get_task_stats()['total'] == 0

        # 未知任务取消是空操作，不应抛出
        manager.cancel_task('missing')

    def test_callbacks_forward_signals_and_update_stats(self, qapp):
        manager = TaskManager()
        received = []
        manager.task_started.connect(
            lambda tid, name: received.append(('started', tid, name))
        )
        manager.task_progress.connect(
            lambda tid, cur, total: received.append(('progress', cur, total))
        )
        manager.task_completed.connect(
            lambda tid, result: received.append(('completed', tid, result))
        )
        manager.task_error.connect(
            lambda tid, msg: received.append(('error', tid, msg))
        )
        manager.task_finished.connect(
            lambda tid: received.append(('finished', tid))
        )

        runner = TaskRunner('t-2', 'named', lambda **_: None)
        runner.is_cancelled = True
        manager.tasks['t-2'] = runner
        manager.task_stats['running'] = 1

        manager._on_task_started('t-2', 'named')
        manager._on_task_progress('t-2', 1, 3)
        manager._on_task_completed('t-2', 'ok')
        manager._on_task_error('t-2', 'bad')
        manager._on_task_finished('t-2')

        assert manager.task_stats['completed'] == 1
        assert manager.task_stats['failed'] == 1
        assert manager.task_stats['cancelled'] == 1
        assert manager.task_stats['running'] == 0
        assert 't-2' not in manager.tasks
        assert received == [
            ('started', 't-2', 'named'),
            ('progress', 1, 3),
            ('completed', 't-2', 'ok'),
            ('error', 't-2', 'bad'),
            ('finished', 't-2'),
        ]

    def test_create_task_wires_signals_and_finishes(self, qapp):
        manager = TaskManager()
        task_id = manager.create_task(
            'quick', lambda **_: 'ok', timeout_seconds=5
        )

        assert task_id in manager.tasks
        assert manager.task_stats['total'] == 1
        assert manager.task_stats['running'] == 1

        manager.thread_pool.waitForDone()
        deadline = time.time() + 3
        while manager.task_stats['running'] > 0 and time.time() < deadline:
            qapp.processEvents()
            time.sleep(0.01)

        assert manager.task_stats['completed'] == 1
        assert manager.task_stats['running'] == 0
        assert task_id not in manager.tasks
