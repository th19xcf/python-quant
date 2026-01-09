#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
事件总线模块，基于blinker实现事件驱动架构
"""

from blinker import Namespace

# 定义全局事件命名空间
quant_events = Namespace()

# 系统事件
system_init = quant_events.signal('system_init')  # 系统初始化完成
system_shutdown = quant_events.signal('system_shutdown')  # 系统即将关闭

# 数据相关事件
data_updated = quant_events.signal('data_updated')  # 数据更新完成
data_error = quant_events.signal('data_error')  # 数据获取错误

# 技术分析相关事件
indicator_calculated = quant_events.signal('indicator_calculated')  # 指标计算完成
indicator_error = quant_events.signal('indicator_error')  # 指标计算错误

# 策略相关事件
strategy_signal = quant_events.signal('strategy_signal')  # 策略发出信号
strategy_error = quant_events.signal('strategy_error')  # 策略执行错误

# UI相关事件
ui_refresh = quant_events.signal('ui_refresh')  # UI需要刷新
tab_changed = quant_events.signal('tab_changed')  # 标签页切换

# 插件间通信事件
plugin_message = quant_events.signal('plugin_message')  # 插件间消息传递
plugin_request = quant_events.signal('plugin_request')  # 插件间请求-响应
plugin_response = quant_events.signal('plugin_response')  # 插件间响应
plugin_event = quant_events.signal('plugin_event')  # 插件自定义事件


class EventBus:
    """
    事件总线管理类，提供事件发布和订阅的封装
    """
    
    @staticmethod
    def subscribe(signal_name, subscriber, weak=True):
        """
        订阅事件
        
        Args:
            signal_name: 事件名称
            subscriber: 订阅者函数
            weak: 是否使用弱引用，默认为True
        
        Returns:
            blinker.base.SignalConnection: 连接对象
        """
        signal = quant_events.signal(signal_name)
        return signal.connect(subscriber, weak=weak)
    
    @staticmethod
    def unsubscribe(signal_name, subscriber):
        """
        取消订阅事件
        
        Args:
            signal_name: 事件名称
            subscriber: 订阅者函数
        """
        signal = quant_events.signal(signal_name)
        signal.disconnect(subscriber)
    
    @staticmethod
    def publish(signal_name, **kwargs):
        """
        发布事件
        
        Args:
            signal_name: 事件名称
            **kwargs: 事件参数
        """
        signal = quant_events.signal(signal_name)
        signal.send(**kwargs)
    
    @staticmethod
    def get_signal(signal_name):
        """
        获取事件信号对象
        
        Args:
            signal_name: 事件名称
            
        Returns:
            blinker.base.Signal: 信号对象
        """
        return quant_events.signal(signal_name)