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
from src.ui.task_manager import TaskRunner
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
