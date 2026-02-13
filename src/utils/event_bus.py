#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
事件总线模块，基于blinker实现事件驱动架构
"""

from blinker import Namespace
import asyncio
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import time
import threading
import logging

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

# 系统事件
system_init = quant_events.signal(EventType.SYSTEM_INIT)
system_shutdown = quant_events.signal(EventType.SYSTEM_SHUTDOWN)

# 数据相关事件
data_updated = quant_events.signal(EventType.DATA_UPDATED)
data_error = quant_events.signal(EventType.DATA_ERROR)
data_read_progress = quant_events.signal(EventType.DATA_READ_PROGRESS)

# 技术分析相关事件
indicator_calculated = quant_events.signal(EventType.INDICATOR_CALCULATED)
indicator_error = quant_events.signal(EventType.INDICATOR_ERROR)
indicator_progress = quant_events.signal(EventType.INDICATOR_PROGRESS)

# 策略相关事件
strategy_signal = quant_events.signal(EventType.STRATEGY_SIGNAL)
strategy_error = quant_events.signal(EventType.STRATEGY_ERROR)
strategy_backtest_completed = quant_events.signal(EventType.STRATEGY_BACKTEST_COMPLETED)

# UI相关事件
ui_refresh = quant_events.signal(EventType.UI_REFRESH)
tab_changed = quant_events.signal(EventType.TAB_CHANGED)
window_count_changed = quant_events.signal(EventType.WINDOW_COUNT_CHANGED)

# 插件间通信事件
plugin_message = quant_events.signal(EventType.PLUGIN_MESSAGE)
plugin_request = quant_events.signal(EventType.PLUGIN_REQUEST)
plugin_response = quant_events.signal(EventType.PLUGIN_RESPONSE)
plugin_event = quant_events.signal(EventType.PLUGIN_EVENT)


class EventBus:
    """
    事件总线管理类，提供事件发布和订阅的封装
    """
    
    def __init__(self):
        """初始化事件总线"""
        self._events = quant_events
        self._subscribers: Dict[str, List[Tuple[Callable, bool, int, Optional[Callable]]]] = {}
        self._event_history: List[Dict[str, Any]] = []
        self._history_max_size = 1000  # 事件历史记录最大数量
        self._event_monitors: List[Callable] = []
        self._lock = threading.RLock()  # 线程锁，确保线程安全
        
        # 添加事件统计功能
        self._event_stats: Dict[str, Dict[str, int]] = {
            'publish_count': {},  # 事件发布次数统计
            'subscribe_count': {},  # 事件订阅次数统计
            'unsubscribe_count': {}  # 事件取消订阅次数统计
        }
    
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
        signal = self._events.signal(signal_name)
        connection = signal.connect(subscriber, weak=weak)
        
        # 保存订阅信息
        with self._lock:
            if signal_name not in self._subscribers:
                self._subscribers[signal_name] = []
            self._subscribers[signal_name].append((subscriber, weak, priority, filter_func))
            # 按优先级排序
            self._subscribers[signal_name].sort(key=lambda x: x[2], reverse=True)
            
            # 更新订阅统计
            if signal_name not in self._event_stats['subscribe_count']:
                self._event_stats['subscribe_count'][signal_name] = 0
            self._event_stats['subscribe_count'][signal_name] += 1
        
        # 通知事件监控器
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
            
            # 更新取消订阅统计
            if signal_name not in self._event_stats['unsubscribe_count']:
                self._event_stats['unsubscribe_count'][signal_name] = 0
            self._event_stats['unsubscribe_count'][signal_name] += 1
        
        # 通知事件监控器
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
            
            # 更新发布统计
            if signal_name not in self._event_stats['publish_count']:
                self._event_stats['publish_count'][signal_name] = 0
            self._event_stats['publish_count'][signal_name] += 1
        
        # 通知事件监控器
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
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(_async_publish)
    
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
            return self._subscribers.get(signal_name, [])
    
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
            return self._event_stats.copy()
    
    def clear_event_stats(self) -> None:
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
    
    def filter_events(self, signal_name: str, filter_func: Callable) -> Callable:
        """
        创建事件过滤装饰器
        
        Args:
            signal_name: 事件名称
            filter_func: 事件过滤函数
        
        Returns:
            Callable: 装饰器函数
        """
        def decorator(func: Callable) -> Callable:
            def wrapper(**kwargs):
                if filter_func(**kwargs):
                    return func(**kwargs)
            return wrapper
        return decorator


# 创建全局事件总线实例
event_bus = EventBus()

# 添加默认的日志监控器，用于记录所有事件活动


def default_event_monitor(event_type: str, signal_name: str, *args, **kwargs):
    """
    默认事件监控器，用于记录所有事件活动
    
    Args:
        event_type: 事件类型，如'subscribe'、'unsubscribe'、'publish'
        signal_name: 信号名称
        *args: 位置参数
        **kwargs: 关键字参数
    """
    if event_type == 'publish':
        # 记录事件发布
        logger.debug(f"Event published: {signal_name}, Thread: {threading.get_ident()}, Args: {args}, Kwargs: {kwargs}")
    elif event_type == 'subscribe':
        # 记录事件订阅
        subscriber = args[0] if args else None
        logger.debug(f"Event subscribed: {signal_name}, Subscriber: {subscriber.__name__ if hasattr(subscriber, '__name__') else str(subscriber)}")
    elif event_type == 'unsubscribe':
        # 记录事件取消订阅
        subscriber = args[0] if args else None
        logger.debug(f"Event unsubscribed: {signal_name}, Subscriber: {subscriber.__name__ if hasattr(subscriber, '__name__') else str(subscriber)}")

# 添加默认监控器
event_bus.add_monitor(default_event_monitor)

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