#!/usr/bin/env python3
"""配置模块副作用与热加载语义的守护用例。

- 构造 LogSettings 不得写盘（Config 在模块导入期构造）。
- 热加载替换内部实例时必须提示模块级 config 快照不会刷新。
"""

from __future__ import annotations

from loguru import logger

from src.utils import config as config_module
from src.utils.config import ConfigManager, LogSettings


def test_log_settings_construction_does_not_create_directory(tmp_path, monkeypatch) -> None:
    """构造配置只做校验，不创建日志目录。"""
    monkeypatch.chdir(tmp_path)

    LogSettings(file_path='logs_should_not_exist/app.log')

    assert not (tmp_path / 'logs_should_not_exist').exists()


def test_watch_config_warns_module_snapshot_is_stale(monkeypatch) -> None:
    """热加载成功后必须 warning 说明模块级 config 快照不刷新。"""

    class _FakeStopEvent:
        """第一次 wait 返回 False 进入一轮检查，第二次返回 True 退出。"""

        def __init__(self) -> None:
            self.calls = 0

        def wait(self, timeout: float) -> bool:
            self.calls += 1
            return self.calls > 1

    manager = ConfigManager()
    manager._running = True
    manager._stop_event = _FakeStopEvent()  # noqa: SLF001
    manager._last_modified = 0.0
    monkeypatch.setattr(manager, '_get_env_file_mtime', lambda: 1.0)
    monkeypatch.setattr(config_module, 'Config', lambda: object())

    messages: list[str] = []
    sink_id = logger.add(lambda m: messages.append(m.record['message']), level='WARNING')
    try:
        manager._watch_config(0)
    finally:
        logger.remove(sink_id)

    assert any('模块级' in msg and 'get_config()' in msg for msg in messages)
