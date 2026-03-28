#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
事件总线模块，基于blinker实现事件驱动架构

特性：
1. 简化的事件发布和订阅机制
2. 增强的事件监控和调试功能
3. 线程安全的设计
4. 事件历史记录和统计
5. 支持异步事件发布
"""

from blinker import Namespace
from typing import Any, Callable, Dict, List, Optional, Tuple
import time
import threading
import logging
from concurrent.futures import ThreadPoolExecutor

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
    
    def __init__(self, history_max_size: int = 1000):
        """初始化事件总线
        
        Args:
            history_max_size: 事件历史记录最大数量
        """
        self._events = quant_events
        self._subscribers: Dict[str, List[Tuple[Callable, bool, int, Optional[Callable]]]] = {}
        self._event_history: List[Dict[str, Any]] = []
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
    
    def subscribe(self, signal_name: str, subscriber: Callable, weak: bool = True, priority: int = 0, filter_func: Optional[Callable] = None):
        """
        订阅事件
        
        Args:
            signal_name: 事件名称
            subscriber: 订阅者函数
            weak: 是否使用弱引用，默认为True
            priority: 事件优先级，数值越大，优先级越高，默认为0
            filter_func: 事件过滤函数，用于过滤事件，返回True表示接收事件，False表示忽略事件
        
        Returns:
            blinker.base.SignalConnection: 连接对象
        """
        # 获取信号对象
        signal = self._events.signal(signal_name)
        
        # 创建过滤包装器
        if filter_func:
            def filtered_subscriber(**kwargs):
                if filter_func(**kwargs):
                    return subscriber(**kwargs)
            connection = signal.connect(filtered_subscriber, weak=weak)
        else:
            connection = signal.connect(subscriber, weak=weak)
        
        # 保存订阅信息
        with self._lock:
            if signal_name not in self._subscribers:
                self._subscribers[signal_name] = []
            self._subscribers[signal_name].append((subscriber, weak, priority, filter_func))
            # 按优先级排序
            self._subscribers[signal_name].sort(key=lambda x: x[2], reverse=True)
            
            # 更新统计
            self._event_stats['subscribe_count'][signal_name] = self._event_stats['subscribe_count'].get(signal_name, 0) + 1
        
        # 通知监控器
        self._notify_monitors('subscribe', signal_name, subscriber)
        
        return connection
    
    def unsubscribe(self, signal_name: str, subscriber: Callable):
        """
        取消订阅事件
        
        Args:
            signal_name: 事件名称
            subscriber: 订阅者函数
        """
        signal = self._events.signal(signal_name)
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
            self._event_history.append(event)
            # 限制历史记录数量
            if len(self._event_history) > self._history_max_size:
                self._event_history.pop(0)
            
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
        
        Args:
            signal_name: 事件名称
            **kwargs: 事件参数
        """
        def _async_publish():
            self.publish(signal_name, **kwargs)
        
        # 使用线程池执行异步发布
        self._executor.submit(_async_publish)
    
    def get_signal(self, signal_name: str):
        """
        获取事件信号对象
        
        Args:
            signal_name: 事件名称
            
        Returns:
            blinker.base.Signal: 信号对象
        """
        return self._events.signal(signal_name)
    
    def get_subscribers(self, signal_name: str) -> List[Tuple[Callable, bool, int, Optional[Callable]]]:
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
            return self._event_history[-limit:].copy()
    
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
            # 限制历史记录数量
            if len(self._event_history) > size:
                self._event_history = self._event_history[-size:]
    
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
        return list(self._events._signals.keys())
    
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
        if event_type == 'publish':
            # 记录事件发布
            logger.debug(f"[EVENT] Published: {signal_name} | Thread: {threading.get_ident()} | Kwargs: {kwargs}")
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
def subscribe(signal_name: str, subscriber: Callable, weak: bool = True, priority: int = 0, filter_func: Optional[Callable] = None):
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
