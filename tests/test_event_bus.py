#!/usr/bin/env python3

"""
事件总线单元测试

覆盖：订阅者签名适配（blinker 的 sender 位置参数）、filter_func 订阅与取消、
定长历史队列、监控器热路径早退、异步发布有界背压。
"""

import os
import sys
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest

from src.utils import event_bus as event_bus_module
from src.utils.event_bus import EventBus, EventMonitor

# ======================================================================
# Fixtures
# ======================================================================

@pytest.fixture
def bus():
    instance = EventBus()
    yield instance
    instance.shutdown()


# ======================================================================
# 订阅者签名适配
# ======================================================================

class TestSubscriberSignature:

    def test_sender_style_subscriber_receives(self, bus):
        """声明 sender 的订阅者（项目既有风格）按 blinker 语义收到事件"""
        received = []

        def handler(sender, **kwargs):
            received.append(kwargs)

        bus.subscribe('sig_sender_style', handler)
        bus.publish('sig_sender_style', value=1)
        assert received == [{'value': 1}]

    def test_kwargs_only_subscriber_receives(self, bus):
        """只写 **kwargs 的订阅者由总线吞掉 sender，不再抛 TypeError"""
        received = []

        def handler(**kwargs):
            received.append(kwargs)

        bus.subscribe('sig_kwargs_only', handler)
        bus.publish('sig_kwargs_only', value=2)
        assert received == [{'value': 2}]

    def test_bound_method_without_sender_receives(self, bus):
        """插件配置处理器这类不带 sender 的绑定方法不再崩溃"""
        class Handler:
            def __init__(self):
                self.calls = []

            def on_config(self, plugin_name, old_config, new_config):
                self.calls.append(plugin_name)

        handler = Handler()
        bus.subscribe('sig_method_no_sender', handler.on_config)
        bus.publish('sig_method_no_sender', plugin_name='demo',
                    old_config={}, new_config={})
        assert handler.calls == ['demo']


# ======================================================================
# filter_func
# ======================================================================

class TestFilterFunc:

    def test_filter_passes_matching_event(self, bus):
        received = []

        def handler(sender, **kwargs):
            received.append(kwargs)

        bus.subscribe('sig_filter_ok', handler, filter_func=lambda **kw: kw.get('x') == 1)
        bus.publish('sig_filter_ok', x=1)
        assert received == [{'x': 1}]

    def test_filter_blocks_non_matching_event(self, bus):
        received = []

        def handler(sender, **kwargs):
            received.append(kwargs)

        bus.subscribe('sig_filter_block', handler, filter_func=lambda **kw: kw.get('x') == 1)
        bus.publish('sig_filter_block', x=2)
        assert received == []

    def test_unsubscribe_stops_delivery(self, bus):
        """filter_func 分支必须断开实际连接的包装闭包，否则订阅无法移除"""
        received = []

        def handler(sender, **kwargs):
            received.append(kwargs)

        bus.subscribe('sig_filter_unsub', handler, filter_func=lambda **kw: True)
        bus.publish('sig_filter_unsub', x=1)
        assert len(received) == 1

        bus.unsubscribe('sig_filter_unsub', handler)
        bus.publish('sig_filter_unsub', x=2)
        assert len(received) == 1


# ======================================================================
# 事件历史（deque 定长）
# ======================================================================

class TestEventHistory:

    def test_history_is_bounded(self):
        bus = EventBus(history_max_size=3)
        try:
            for i in range(5):
                bus.publish('sig_hist', i=i)
            history = bus.get_event_history(limit=100)
            assert [e['kwargs']['i'] for e in history] == [2, 3, 4]
        finally:
            bus.shutdown()

    def test_set_history_max_size_keeps_latest(self):
        bus = EventBus(history_max_size=10)
        try:
            for i in range(10):
                bus.publish('sig_hist_resize', i=i)
            bus.set_history_max_size(3)
            assert bus.debug_info()['event_history_size'] == 3
            assert [e['kwargs']['i'] for e in bus.get_event_history()] == [7, 8, 9]
        finally:
            bus.shutdown()

    def test_invalid_max_size_rejected(self):
        with pytest.raises(ValueError):
            EventBus(history_max_size=0)


# ======================================================================
# 监控器热路径
# ======================================================================

class TestDebugMonitor:

    def test_debug_monitor_skips_when_debug_disabled(self, bus, monkeypatch):
        """DEBUG 关闭时监控器不得构造 f-string 与脱敏字典"""
        bus.add_monitor(EventMonitor.debug_monitor)
        monkeypatch.setattr(event_bus_module.logger, 'isEnabledFor', lambda level: False)
        called = []
        monkeypatch.setattr(event_bus_module, '_redact_kwargs',
                            lambda kwargs: called.append(kwargs) or kwargs)
        bus.publish('sig_monitor_off', x=1)
        assert called == []


# ======================================================================
# 异步发布背压
# ======================================================================

class TestPublishAsync:

    def test_publish_async_delivers(self):
        bus = EventBus()
        received = []

        def handler(sender=None, **kwargs):
            received.append(kwargs)

        bus.subscribe('sig_async_deliver', handler)
        assert bus.publish_async('sig_async_deliver', x=1) is True
        bus.shutdown()   # shutdown(wait=True) 保证任务已执行
        assert received == [{'x': 1}]

    def test_drop_when_pending_full(self):
        gate = threading.Event()
        bus = EventBus(async_max_pending=1)

        def blocking_handler(sender=None, **kwargs):
            gate.wait(timeout=5)

        bus.subscribe('sig_async_full', blocking_handler)
        try:
            assert bus.publish_async('sig_async_full') is True
            # 名额在提交前已占用，第二次必然被丢弃
            assert bus.publish_async('sig_async_full') is False
            assert bus.debug_info()['async_dropped'] == 1
        finally:
            gate.set()
            bus.shutdown()

    def test_publish_async_after_shutdown_returns_false(self):
        bus = EventBus()
        bus.shutdown()
        assert bus.publish_async('sig_async_closed') is False


# ======================================================================
# Qt 桥接（无 Qt 环境跳过）
# ======================================================================

@pytest.mark.skipif(not event_bus_module._HAS_QT, reason='无 Qt 环境')
class TestQtBridge:

    def test_qobject_binding_is_bridged(self):
        from PySide6.QtCore import QObject

        class Receiver(QObject):
            def __init__(self):
                super().__init__()
                self.calls = []

            def on_event(self, sender=None, **kwargs):
                self.calls.append(kwargs)

        bus = EventBus()
        receiver = Receiver()
        try:
            bus.subscribe('sig_qt_bridge', receiver.on_event)
            bus.publish('sig_qt_bridge', x=1)
            assert receiver.calls == [{'x': 1}]
        finally:
            bus.shutdown()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
