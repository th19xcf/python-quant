#!/usr/bin/env python3

"""
事件总线模块，基于blinker实现事件驱动架构

特性：
1. 简化的事件发布和订阅机制
2. 增强的事件监控和调试功能
3. 线程安全的设计
4. 事件历史记录和统计
5. 支持异步事件发布
"""

import inspect
import logging
import threading
import time
import weakref
from collections import deque
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Deque, Dict, List, Tuple

from blinker import Namespace

# 调试日志中禁止明文输出的字段关键字（命中即脱敏）
_SENSITIVE_LOG_KEYWORDS = (
    'token', 'password', 'passwd', 'secret',
    'api_key', 'apikey', 'authorization', 'cookie',
)


def _redact_kwargs(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """对调试日志中的事件参数做敏感字段脱敏，避免凭据随 DEBUG 日志落盘。"""
    return {
        key: ('***' if any(word in key.lower() for word in _SENSITIVE_LOG_KEYWORDS) else value)
        for key, value in kwargs.items()
    }

# Qt 跨线程事件桥接：blinker 为同步发布，在工作线程 publish 会让接收者运行在
# 工作线程，若接收者触碰 Qt 对象即违反线程亲和性（随机崩溃）。对 QObject 绑定
# 方法的订阅者，统一经以该对象为父的桥接 Signal 转发：AutoConnection 自动保证
# 同线程 Direct（语义与 blinker 直连一致）、跨线程 Queued（投递到接收者线程
# 事件循环），UI 槽函数永远在其所属线程执行。
try:
    from PySide6.QtCore import QObject
    from PySide6.QtCore import Signal as _QtSignal

    class _QtSubscriberBridge(QObject):
        triggered = _QtSignal(dict)

    _HAS_QT = True
except ImportError:  # 无 Qt 环境（如纯数据处理脚本）
    QObject = None  # type: ignore[assignment]
    _QtSubscriberBridge = None  # type: ignore[assignment,misc]
    _HAS_QT = False

logger = logging.getLogger(__name__)

# 事件类型常量
class EventType:
    """事件类型常量定义"""
    # 系统事件
    SYSTEM_INIT = 'system_init'  # 系统初始化完成
    SYSTEM_SHUTDOWN = 'system_shutdown'  # 系统即将关闭

    # 数据相关事件
    DATA_UPDATED = 'data_updated'  # 数据更新完成
    DATA_ERROR = 'data_error'  # 数据获取错误
    DATA_READ_PROGRESS = 'data_read_progress'  # 数据读取进度

    # 技术分析相关事件
    INDICATOR_CALCULATED = 'indicator_calculated'  # 指标计算完成
    INDICATOR_ERROR = 'indicator_error'  # 指标计算错误
    INDICATOR_PROGRESS = 'indicator_progress'  # 指标计算进度

    # 策略相关事件
    STRATEGY_SIGNAL = 'strategy_signal'  # 策略发出信号
    STRATEGY_ERROR = 'strategy_error'  # 策略执行错误
    STRATEGY_BACKTEST_COMPLETED = 'strategy_backtest_completed'  # 策略回测完成

    # UI相关事件
    UI_REFRESH = 'ui_refresh'  # UI需要刷新
    TAB_CHANGED = 'tab_changed'  # 标签页切换
    WINDOW_COUNT_CHANGED = 'window_count_changed'  # 窗口数量变化

    # 插件间通信事件
    PLUGIN_MESSAGE = 'plugin_message'  # 插件间消息传递
    PLUGIN_REQUEST = 'plugin_request'  # 插件间请求-响应
    PLUGIN_RESPONSE = 'plugin_response'  # 插件间响应
    PLUGIN_EVENT = 'plugin_event'  # 插件自定义事件

# 定义全局事件命名空间
quant_events = Namespace()

# 为每个事件类型创建信号对象
EVENT_SIGNALS = {
    EventType.SYSTEM_INIT: quant_events.signal(EventType.SYSTEM_INIT),
    EventType.SYSTEM_SHUTDOWN: quant_events.signal(EventType.SYSTEM_SHUTDOWN),
    EventType.DATA_UPDATED: quant_events.signal(EventType.DATA_UPDATED),
    EventType.DATA_ERROR: quant_events.signal(EventType.DATA_ERROR),
    EventType.DATA_READ_PROGRESS: quant_events.signal(EventType.DATA_READ_PROGRESS),
    EventType.INDICATOR_CALCULATED: quant_events.signal(EventType.INDICATOR_CALCULATED),
    EventType.INDICATOR_ERROR: quant_events.signal(EventType.INDICATOR_ERROR),
    EventType.INDICATOR_PROGRESS: quant_events.signal(EventType.INDICATOR_PROGRESS),
    EventType.STRATEGY_SIGNAL: quant_events.signal(EventType.STRATEGY_SIGNAL),
    EventType.STRATEGY_ERROR: quant_events.signal(EventType.STRATEGY_ERROR),
    EventType.STRATEGY_BACKTEST_COMPLETED: quant_events.signal(EventType.STRATEGY_BACKTEST_COMPLETED),
    EventType.UI_REFRESH: quant_events.signal(EventType.UI_REFRESH),
    EventType.TAB_CHANGED: quant_events.signal(EventType.TAB_CHANGED),
    EventType.WINDOW_COUNT_CHANGED: quant_events.signal(EventType.WINDOW_COUNT_CHANGED),
    EventType.PLUGIN_MESSAGE: quant_events.signal(EventType.PLUGIN_MESSAGE),
    EventType.PLUGIN_REQUEST: quant_events.signal(EventType.PLUGIN_REQUEST),
    EventType.PLUGIN_RESPONSE: quant_events.signal(EventType.PLUGIN_RESPONSE),
    EventType.PLUGIN_EVENT: quant_events.signal(EventType.PLUGIN_EVENT)
}


class EventBus:
    """
    事件总线管理类，提供事件发布和订阅的封装
    """

    def __init__(self, history_max_size: int = 1000, async_max_pending: int = 1000):
        """初始化事件总线

        Args:
            history_max_size: 事件历史记录最大数量
            async_max_pending: 异步发布待处理任务上限，超出后丢弃新任务并计数，
                避免行情突发时无界队列把线程池任务无限堆积
        """
        if history_max_size <= 0:
            raise ValueError("历史记录最大数量必须大于0")
        if async_max_pending <= 0:
            raise ValueError("异步发布待处理上限必须大于0")

        self._events = quant_events
        self._subscribers: Dict[str, List[Tuple[Callable, bool, int, Callable | None]]] = {}
        # 定长双端队列：超出上限自动从头部淘汰，避免 list.pop(0) 的 O(n) 搬移
        self._event_history: Deque[Dict[str, Any]] = deque(maxlen=history_max_size)
        self._history_max_size = history_max_size
        self._event_monitors: List[Callable] = []
        self._lock = threading.RLock()  # 线程锁，确保线程安全

        # 事件统计
        self._event_stats: Dict[str, Dict[str, int]] = {
            'publish_count': {},
            'subscribe_count': {},
            'unsubscribe_count': {}
        }

        # 线程池
        self._executor = ThreadPoolExecutor(max_workers=4)

        # 异步发布背压状态（读写在 self._lock 保护下）
        self._async_max_pending = async_max_pending
        self._async_pending = 0
        self._async_dropped = 0

        # 包装后的订阅记录：(signal_name, 原始订阅者, 实际连接的可调用, Qt 桥接对象或 None)
        # Qt 桥接与 filter_func 都会产生包装闭包，二者统一在此登记，取消订阅时
        # 必须断开 effective（而非原始订阅者），否则 disconnect 会对不上号
        self._wrapped_connections: List[Tuple[str, Callable, Callable, Any]] = []

    def subscribe(self, signal_name: str, subscriber: Callable, weak: bool = True, priority: int = 0, filter_func: Callable | None = None):
        """
        订阅事件

        Args:
            signal_name: 事件名称
            subscriber: 订阅者函数。blinker 以 sender 作为第一位置参数调用
                接收者，因此订阅者应声明 `sender`（可为 None 默认值）；只写
                `**kwargs` 的回调由总线自动适配，无需改动签名
            weak: 是否使用弱引用，默认为True
            priority: 事件优先级，数值越大，优先级越高，默认为0
            filter_func: 事件过滤函数，用于过滤事件，返回True表示接收事件，False表示忽略事件

        Returns:
            blinker.base.SignalConnection: 连接对象
        """
        # 获取信号对象
        signal = self._events.signal(signal_name)

        # QObject 绑定方法跨线程保护：经 Qt 信号桥接，跨线程自动转 QueuedConnection
        original_subscriber = subscriber
        qt_bridge = None
        if _HAS_QT:
            receiver = getattr(subscriber, '__self__', None)
            if isinstance(receiver, QObject):
                subscriber, qt_bridge = self._wrap_qt_subscriber(subscriber)

        # 首个位置参数不是 sender 的订阅者在此吞掉 blinker 注入的 sender，
        # 否则 send() 会抛 "takes 0 positional arguments" /
        # "got multiple values for argument"
        if not self._wants_sender(subscriber):
            inner = subscriber

            def _drop_sender(sender=None, **kwargs):
                return inner(**kwargs)

            subscriber = _drop_sender

        # 创建过滤包装器
        if filter_func:
            def filtered_subscriber(sender=None, **kwargs):
                if filter_func(**kwargs):
                    return subscriber(sender, **kwargs)

            effective = filtered_subscriber
        else:
            effective = subscriber

        # 包装闭包无外部强引用，必须禁用弱引用，改由 _wrapped_connections 持有
        is_wrapped = effective is not original_subscriber
        connection = signal.connect(effective, weak=False if is_wrapped else weak)

        if is_wrapped:
            with self._lock:
                self._wrapped_connections.append(
                    (signal_name, original_subscriber, effective, qt_bridge))

        # 保存订阅信息（weak 记录实际生效值，避免元数据与实际连接方式不一致）
        with self._lock:
            if signal_name not in self._subscribers:
                self._subscribers[signal_name] = []
            self._subscribers[signal_name].append(
                (original_subscriber, False if is_wrapped else weak, priority, filter_func))
            # 按优先级排序
            self._subscribers[signal_name].sort(key=lambda x: x[2], reverse=True)

            # 更新统计
            self._event_stats['subscribe_count'][signal_name] = self._event_stats['subscribe_count'].get(signal_name, 0) + 1

        # 通知监控器
        self._notify_monitors('subscribe', signal_name, original_subscriber)

        return connection

    @staticmethod
    def _wants_sender(receiver: Callable) -> bool:
        """判断接收者的第一个位置参数是否为 sender。

        blinker 的 send() 固定以 sender 作为第一位置参数调用接收者，但本总线
        不做多 sender 语义，需要一个"该不该透传"的判定：首个位置参数名为
        sender 时原样透传（保留既有订阅者语义），否则在适配层吞掉——不能只看
        "能否接受位置参数"，否则 `(plugin_name, ...)` 之类的签名会把 sender
        绑到业务参数上，抛 "got multiple values for argument"。

        签名无法探测时保守返回 True（按 blinker 原生行为透传）。
        """
        try:
            parameters = inspect.signature(receiver).parameters.values()
        except (TypeError, ValueError):
            return True
        for param in parameters:
            if param.kind == inspect.Parameter.VAR_POSITIONAL:
                return True
            if param.kind in (inspect.Parameter.POSITIONAL_ONLY,
                              inspect.Parameter.POSITIONAL_OR_KEYWORD):
                return param.name == 'sender'
        return False

    @staticmethod
    def _wrap_qt_subscriber(subscriber: Callable) -> Tuple[Callable, Any]:
        """将 QObject 绑定方法包装为经 Qt 信号桥接的跨线程安全订阅者

        桥接对象以接收者为父对象，随接收者销毁；同线程派发保持 Direct
        （语义与 blinker 直连一致），跨线程自动转 QueuedConnection。

        Returns:
            (包装后的订阅者, 桥接对象)
        """
        receiver = subscriber.__self__
        func = subscriber.__func__
        accepts_sender = EventBus._wants_sender(subscriber)
        receiver_ref = weakref.ref(receiver)
        bridge = _QtSubscriberBridge(receiver)

        def _deliver(payload: dict):
            obj = receiver_ref()
            if obj is None:
                return
            sender = payload.get('sender')
            kwargs = {key: value for key, value in payload.items() if key != 'sender'}
            if accepts_sender:
                func(obj, sender, **kwargs)
            else:
                func(obj, **kwargs)

        bridge.triggered.connect(_deliver)

        def wrapped(sender=None, **kwargs):
            if receiver_ref() is not None:
                bridge.triggered.emit({'sender': sender, **kwargs})

        return wrapped, bridge

    def unsubscribe(self, signal_name: str, subscriber: Callable):
        """
        取消订阅事件

        Args:
            signal_name: 事件名称
            subscriber: 订阅者函数
        """
        signal = self._events.signal(signal_name)

        # 包装过的订阅（Qt 桥接 / filter_func）必须断开实际连接的包装器，
        # 对原始订阅者 disconnect 会因对象不同而静默失效，订阅永远留在信号上
        with self._lock:
            wrapped_entries = [e for e in self._wrapped_connections
                               if e[0] == signal_name and e[1] == subscriber]
            self._wrapped_connections = [e for e in self._wrapped_connections
                                         if not (e[0] == signal_name and e[1] == subscriber)]
        for _, _, effective, bridge in wrapped_entries:
            signal.disconnect(effective)
            if bridge is not None:
                bridge.setParent(None)
                bridge.deleteLater()
        if not wrapped_entries:
            signal.disconnect(subscriber)

        # 移除订阅信息
        with self._lock:
            if signal_name in self._subscribers:
                self._subscribers[signal_name] = [
                    sub for sub in self._subscribers[signal_name] if sub[0] != subscriber
                ]
                if not self._subscribers[signal_name]:
                    del self._subscribers[signal_name]

            # 更新统计
            self._event_stats['unsubscribe_count'][signal_name] = self._event_stats['unsubscribe_count'].get(signal_name, 0) + 1

        # 通知监控器
        self._notify_monitors('unsubscribe', signal_name, subscriber)

    def publish(self, signal_name: str, **kwargs):
        """
        发布事件

        Args:
            signal_name: 事件名称
            **kwargs: 事件参数
        """
        # 记录事件历史
        event = {
            'signal_name': signal_name,
            'timestamp': time.time(),
            'kwargs': kwargs,
            'thread_id': threading.get_ident()
        }

        with self._lock:
            # deque(maxlen=...) 追加即自动淘汰头部，无 pop(0) 的 O(n) 搬移
            self._event_history.append(event)

            # 更新统计
            self._event_stats['publish_count'][signal_name] = self._event_stats['publish_count'].get(signal_name, 0) + 1

        # 通知监控器
        self._notify_monitors('publish', signal_name, **kwargs)

        # 发布事件
        signal = self._events.signal(signal_name)
        signal.send(**kwargs)

    def publish_async(self, signal_name: str, **kwargs):
        """
        异步发布事件

        ThreadPoolExecutor 的任务队列无界，突发行情下 submit 会把任务无限
        堆积。这里以 async_max_pending 做有界背压：待处理任务达到上限时直接
        丢弃本次发布并计数告警，保证内存与延迟可控（丢事件优于雪崩）。

        Args:
            signal_name: 事件名称
            **kwargs: 事件参数

        Returns:
            bool: 是否成功入队；被丢弃时为 False
        """
        with self._lock:
            if self._async_pending >= self._async_max_pending:
                self._async_dropped += 1
                dropped = True
            else:
                self._async_pending += 1
                dropped = False

        if dropped:
            logger.warning(
                f"异步事件待处理任务已达上限 {self._async_max_pending}，"
                f"丢弃事件: {signal_name}（累计丢弃 {self._async_dropped}）")
            return False

        def _async_publish():
            try:
                self.publish(signal_name, **kwargs)
            finally:
                with self._lock:
                    self._async_pending -= 1

        # 使用线程池执行异步发布
        try:
            self._executor.submit(_async_publish)
        except RuntimeError:
            # 线程池已 shutdown，回退计数并告警，不向调用方抛错
            with self._lock:
                self._async_pending -= 1
            logger.warning(f"事件总线已关闭，丢弃异步事件: {signal_name}")
            return False
        return True

    def get_signal(self, signal_name: str):
        """
        获取事件信号对象

        Args:
            signal_name: 事件名称

        Returns:
            blinker.base.Signal: 信号对象
        """
        return self._events.signal(signal_name)

    def get_subscribers(self, signal_name: str) -> List[Tuple[Callable, bool, int, Callable | None]]:
        """
        获取事件订阅者列表

        Args:
            signal_name: 事件名称

        Returns:
            List[Tuple[Callable, bool, int, Optional[Callable]]]: 订阅者列表
        """
        with self._lock:
            return self._subscribers.get(signal_name, []).copy()

    def get_event_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        获取事件历史记录

        Args:
            limit: 返回的历史记录数量，默认为100

        Returns:
            List[Dict[str, Any]]: 事件历史记录
        """
        with self._lock:
            return list(self._event_history)[-limit:]

    def clear_event_history(self):
        """清除事件历史记录"""
        with self._lock:
            self._event_history.clear()

    def set_history_max_size(self, size: int):
        """
        设置事件历史记录最大数量

        Args:
            size: 最大数量
        """
        if size <= 0:
            raise ValueError("历史记录最大数量必须大于0")

        with self._lock:
            self._history_max_size = size
            # maxlen 不可变，重建队列保留最新 size 条（deque 构造时超限自动留尾部）
            self._event_history = deque(self._event_history, maxlen=size)

    def add_monitor(self, monitor: Callable):
        """
        添加事件监控器

        Args:
            monitor: 监控器函数，接收事件类型、信号名称、订阅者等参数
        """
        with self._lock:
            self._event_monitors.append(monitor)

    def remove_monitor(self, monitor: Callable):
        """
        移除事件监控器

        Args:
            monitor: 监控器函数
        """
        with self._lock:
            if monitor in self._event_monitors:
                self._event_monitors.remove(monitor)

    def _notify_monitors(self, event_type: str, signal_name: str, *args, **kwargs):
        """
        通知事件监控器

        Args:
            event_type: 监控事件类型，可选值：'subscribe'、'unsubscribe'、'publish'
            signal_name: 信号名称
            *args: 位置参数
            **kwargs: 关键字参数
        """
        with self._lock:
            monitors = self._event_monitors.copy()

        for monitor in monitors:
            try:
                monitor(event_type, signal_name, *args, **kwargs)
            except Exception as e:
                # 忽略监控器执行错误
                logger.exception(f"事件监控器执行错误: {e}")

    def get_all_signals(self) -> List[str]:
        """
        获取所有已定义的信号名称

        Returns:
            List[str]: 信号名称列表
        """
        return list(self._events.keys())

    def get_event_stats(self) -> Dict[str, Dict[str, int]]:
        """
        获取事件统计信息

        Returns:
            Dict[str, Dict[str, int]]: 事件统计信息，包含发布、订阅和取消订阅的统计
        """
        with self._lock:
            return {k: v.copy() for k, v in self._event_stats.items()}

    def clear_event_stats(self):
        """
        清除事件统计信息
        """
        with self._lock:
            self._event_stats = {
                'publish_count': {},
                'subscribe_count': {},
                'unsubscribe_count': {}
            }

    def get_event_stat_summary(self) -> Dict[str, int]:
        """
        获取事件统计摘要

        Returns:
            Dict[str, int]: 事件统计摘要，包含总发布数、总订阅数和总取消订阅数
        """
        with self._lock:
            return {
                'total_publish': sum(self._event_stats['publish_count'].values()),
                'total_subscribe': sum(self._event_stats['subscribe_count'].values()),
                'total_unsubscribe': sum(self._event_stats['unsubscribe_count'].values())
            }

    def shutdown(self):
        """
        关闭事件总线，清理资源
        """
        # 关闭线程池
        self._executor.shutdown(wait=True)

        # 清除历史记录和统计
        self.clear_event_history()
        self.clear_event_stats()

    def debug_info(self) -> Dict[str, Any]:
        """
        获取事件总线的调试信息

        Returns:
            Dict[str, Any]: 调试信息
        """
        with self._lock:
            return {
                'signals': self.get_all_signals(),
                'subscribers': {k: len(v) for k, v in self._subscribers.items()},
                'event_history_size': len(self._event_history),
                'history_max_size': self._history_max_size,
                'monitors_count': len(self._event_monitors),
                'async_pending': self._async_pending,
                'async_max_pending': self._async_max_pending,
                'async_dropped': self._async_dropped,
                'stats': self.get_event_stat_summary()
            }


# 创建全局事件总线实例
event_bus = EventBus()


class EventMonitor:
    """事件监控器类"""

    @staticmethod
    def debug_monitor(event_type: str, signal_name: str, *args, **kwargs):
        """
        调试监控器，详细记录事件活动

        Args:
            event_type: 事件类型
            signal_name: 信号名称
            *args: 位置参数
            **kwargs: 关键字参数
        """
        if not logger.isEnabledFor(logging.DEBUG):
            # 热路径：debug 日志被关闭时直接返回，避免逐事件构造 f-string
            # 与 _redact_kwargs() 的字典推导（实测占 publish 总耗时六成以上）
            return

        if event_type == 'publish':
            # 记录事件发布
            logger.debug(f"[EVENT] Published: {signal_name} | Thread: {threading.get_ident()} | Kwargs: {_redact_kwargs(kwargs)}")
        elif event_type == 'subscribe':
            # 记录事件订阅
            subscriber = args[0] if args else None
            subscriber_name = subscriber.__name__ if hasattr(subscriber, '__name__') else str(subscriber)
            logger.debug(f"[EVENT] Subscribed: {signal_name} | Subscriber: {subscriber_name}")
        elif event_type == 'unsubscribe':
            # 记录事件取消订阅
            subscriber = args[0] if args else None
            subscriber_name = subscriber.__name__ if hasattr(subscriber, '__name__') else str(subscriber)
            logger.debug(f"[EVENT] Unsubscribed: {signal_name} | Subscriber: {subscriber_name}")

    @staticmethod
    def info_monitor(event_type: str, signal_name: str, *args, **kwargs):
        """
        信息监控器，记录重要事件活动

        Args:
            event_type: 事件类型
            signal_name: 信号名称
            *args: 位置参数
            **kwargs: 关键字参数
        """
        if event_type == 'publish':
            # 只记录发布事件
            logger.info(f"[EVENT] {signal_name}")

    @staticmethod
    def error_monitor(event_type: str, signal_name: str, *args, **kwargs):
        """
        错误监控器，记录错误事件

        Args:
            event_type: 事件类型
            signal_name: 信号名称
            *args: 位置参数
            **kwargs: 关键字参数
        """
        if event_type == 'publish' and 'error' in signal_name:
            # 记录错误事件
            logger.error(f"[EVENT ERROR] {signal_name} | Details: {kwargs}")


# 添加默认监控器
event_bus.add_monitor(EventMonitor.debug_monitor)

# 事件总线便捷函数
def subscribe(signal_name: str, subscriber: Callable, weak: bool = True, priority: int = 0, filter_func: Callable | None = None):
    """便捷订阅事件函数"""
    return event_bus.subscribe(signal_name, subscriber, weak, priority, filter_func)


def unsubscribe(signal_name: str, subscriber: Callable):
    """便捷取消订阅事件函数"""
    return event_bus.unsubscribe(signal_name, subscriber)


def publish(signal_name: str, **kwargs):
    """便捷发布事件函数"""
    return event_bus.publish(signal_name, **kwargs)


def publish_async(signal_name: str, **kwargs):
    """便捷异步发布事件函数"""
    return event_bus.publish_async(signal_name, **kwargs)


def get_signal(signal_name: str):
    """便捷获取信号函数"""
    return event_bus.get_signal(signal_name)


def get_event_bus() -> EventBus:
    """获取事件总线实例"""
    return event_bus


def shutdown_event_bus():
    """关闭事件总线"""
    event_bus.shutdown()
