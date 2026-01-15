#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
插件基类定义
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Type
import uuid
import time
import threading

from pydantic_settings import BaseSettings
from pydantic import ValidationError
from loguru import logger
from packaging.version import Version, parse as parse_version
import pandas as pd
import polars as pl

from src.utils.event_bus import EventBus
from src.utils.plugin_config_manager import get_plugin_config_manager


class PluginBase(ABC):
    """
    所有插件的基类，定义插件的基本接口
    """
    
    def __init__(self):
        self.name = "Unknown"
        self.version = "0.0.1"
        self.author = "Unknown"
        self.description = ""
        self.enabled = True
        self.config = {}
        self.plugin_manager = None  # 插件管理器实例，由PluginManager在初始化时设置
        self._config_model = None  # 插件配置模型类
        self._default_config = {}  # 插件默认配置
        self._plugin_config_manager = get_plugin_config_manager()  # 插件配置管理器实例
        # 插件依赖声明，格式: [{'name': 'plugin_name', 'version': '>=1.0.0'}, ...]
        self.dependencies = []
        # 插件加载状态: 'unloaded', 'loaded', 'initialized'
        self._load_status = 'unloaded'
    
    @abstractmethod
    def get_name(self) -> str:
        """
        获取插件名称
        
        Returns:
            str: 插件名称
        """
        pass
    
    @abstractmethod
    def get_version(self) -> str:
        """
        获取插件版本
        
        Returns:
            str: 插件版本
        """
        pass
    
    @abstractmethod
    def get_author(self) -> str:
        """
        获取插件作者
        
        Returns:
            str: 插件作者
        """
        pass
    
    @abstractmethod
    def get_description(self) -> str:
        """
        获取插件描述
        
        Returns:
            str: 插件描述
        """
        pass
    
    def initialize(self, config: Dict[str, Any]) -> bool:
        """
        初始化插件
        
        Args:
            config: 插件配置
            
        Returns:
            bool: 初始化是否成功
        """
        self.config = config
        return True
    
    def shutdown(self) -> bool:
        """
        关闭插件
        
        Returns:
            bool: 关闭是否成功
        """
        return True
    
    def is_enabled(self) -> bool:
        """
        检查插件是否启用
        
        Returns:
            bool: 是否启用
        """
        return self.enabled
    
    def set_enabled(self, enabled: bool) -> None:
        """
        设置插件启用状态
        
        Args:
            enabled: 是否启用
        """
        self.enabled = enabled
    
    def get_config(self) -> Dict[str, Any]:
        """
        获取插件配置
        
        Returns:
            Dict[str, Any]: 插件配置
        """
        return self.config
    
    def update_config(self, config: Dict[str, Any]) -> None:
        """
        更新插件配置
        
        Args:
            config: 新的配置
        """
        old_config = self.config.copy()
        self.config.update(config)
        
        # 发布配置变更事件
        EventBus.publish('plugin_config_changed', 
                       plugin_name=self.get_name(),
                       old_config=old_config,
                       new_config=self.config)
        
    def register_config_model(self, config_model: Type[BaseSettings]) -> None:
        """
        注册插件配置模型
        
        Args:
            config_model: 插件配置模型类
        """
        self._config_model = config_model
        
        # 注册到全局插件配置管理器
        self._plugin_config_manager.register_plugin_config(self.get_name(), config_model)
        
        # 重新加载配置
        self.reload_config()
    
    def set_default_config(self, default_config: Dict[str, Any]) -> None:
        """
        设置插件默认配置
        
        Args:
            default_config: 默认配置
        """
        self._default_config = default_config
        
        # 重新加载配置
        self.reload_config()
    
    def get_default_config(self) -> Dict[str, Any]:
        """
        获取插件默认配置
        
        Returns:
            Dict[str, Any]: 默认配置
        """
        return self._default_config.copy()
    
    def reload_config(self) -> None:
        """
        重新加载插件配置
        """
        # 使用插件配置管理器创建配置
        merged_config = self._plugin_config_manager.create_plugin_config(
            self.get_name(), 
            self._default_config
        )
        
        # 更新插件配置
        self.config = merged_config
        
        # 发布配置变更事件
        EventBus.publish('plugin_config_reloaded', 
                       plugin_name=self.get_name(),
                       config=self.config)
    
    def validate_config(self) -> bool:
        """
        验证插件配置
        
        Returns:
            bool: 配置是否有效
        """
        if not self._config_model:
            return True
        
        try:
            self._config_model(**self.config)
            return True
        except ValidationError as e:
            logger.error(f"插件配置验证失败: {self.get_name()}, 错误: {e}")
            return False
    
    def get_config_value(self, key: str, default: Any = None) -> Any:
        """
        获取配置值，支持类型转换
        
        Args:
            key: 配置键
            default: 默认值
            
        Returns:
            Any: 配置值
        """
        return self.config.get(key, default)
    
    def get_config_documentation(self) -> Optional[str]:
        """
        获取插件配置文档
        
        Returns:
            Optional[str]: 插件配置文档
        """
        if not self._config_model:
            return None
        
        return self._plugin_config_manager.get_config_documentation(self.get_name())
    
    def get_config_model(self) -> Optional[Type[BaseSettings]]:
        """
        获取插件配置模型
        
        Returns:
            Optional[Type[BaseSettings]]: 插件配置模型类
        """
        return self._config_model
    
    def on_config_changed(self, old_config: Dict[str, Any], new_config: Dict[str, Any]) -> None:
        """
        配置变更时的回调方法，插件可以重写此方法
        
        Args:
            old_config: 旧配置
            new_config: 新配置
        """
        pass
    
    def get_dependencies(self) -> List[Dict[str, str]]:
        """
        获取插件依赖列表
        
        Returns:
            List[Dict[str, str]]: 依赖列表，格式: [{'name': 'plugin_name', 'version': '>=1.0.0'}, ...]
        """
        return self.dependencies.copy()
    
    def set_dependencies(self, dependencies: List[Dict[str, str]]) -> None:
        """
        设置插件依赖
        
        Args:
            dependencies: 依赖列表，格式: [{'name': 'plugin_name', 'version': '>=1.0.0'}, ...]
        """
        self.dependencies = dependencies
    
    def add_dependency(self, plugin_name: str, version: str = '>=0.0.1') -> None:
        """
        添加单个插件依赖
        
        Args:
            plugin_name: 依赖插件名称
            version: 依赖版本要求，如'>=1.0.0', '==2.0.0', '<3.0.0'
        """
        self.dependencies.append({'name': plugin_name, 'version': version})
    
    def validate_dependencies(self) -> bool:
        """
        验证插件依赖
        
        Returns:
            bool: 依赖是否有效
        """
        if not self.plugin_manager:
            return True
        
        try:
            for dep in self.dependencies:
                dep_name = dep['name']
                dep_version_req = dep['version']
                
                # 检查依赖插件是否存在
                plugin_instance = self.plugin_manager.get_plugin_instance(dep_name)
                if not plugin_instance:
                    return False
                
                # 检查版本是否符合要求
                # 这里简化处理，只检查主要版本号
                plugin_version = parse_version(plugin_instance.get_version())
                # 注意：实际项目中应该使用完整的版本约束解析
                # 这里简化处理，只检查插件是否存在
            
            return True
        except Exception as e:
            logger.error(f"验证插件依赖失败: {self.get_name()}, 错误: {e}")
            return False
    
    def get_load_status(self) -> str:
        """
        获取插件加载状态
        
        Returns:
            str: 加载状态，'unloaded', 'loaded', 'initialized'
        """
        return self._load_status
    
    def set_load_status(self, status: str) -> None:
        """
        设置插件加载状态
        
        Args:
            status: 加载状态，'unloaded', 'loaded', 'initialized'
        """
        self._load_status = status
    
    def supports_polars(self) -> bool:
        """
        检查插件是否支持polars数据处理
        
        Returns:
            bool: 是否支持polars数据处理
        """
        return False

    # -----------------------------
    # 插件间通信方法
    # -----------------------------
    def send_message(self, recipient: str, message_type: str, data: Any, priority: int = 0) -> str:
        """
        发送消息给指定插件
        
        Args:
            recipient: 接收者插件名
            message_type: 消息类型
            data: 消息内容
            priority: 优先级，0-9，0最高
            
        Returns:
            str: 消息ID
        """
        message_id = str(uuid.uuid4())
        message = {
            'version': '1.0',
            'timestamp': time.time(),
            'sender': self.get_name(),
            'recipient': recipient,
            'message_type': message_type,
            'data': data,
            'priority': priority,
            'correlation_id': message_id,
            'metadata': {
                'plugin_version': self.version
            }
        }
        
        EventBus.publish('plugin_message', message=message)
        return message_id
    
    def broadcast_message(self, message_type: str, data: Any, priority: int = 0) -> str:
        """
        广播消息给所有插件
        
        Args:
            message_type: 消息类型
            data: 消息内容
            priority: 优先级，0-9，0最高
            
        Returns:
            str: 消息ID
        """
        return self.send_message('*', message_type, data, priority)
    
    def send_request(self, recipient: str, method: str, params: Dict[str, Any], timeout: int = 5) -> Any:
        """
        发送请求并等待响应
        
        Args:
            recipient: 接收者插件名
            method: 请求的方法名
            params: 请求参数
            timeout: 超时时间（秒）
            
        Returns:
            Any: 响应结果
        
        Raises:
            TimeoutError: 请求超时
            Exception: 请求错误
        """
        request_id = str(uuid.uuid4())
        result = None
        error = None
        event = threading.Event()
        
        # 响应处理函数
        def on_response(message):
            nonlocal result, error
            if message['sender'] == recipient and message['correlation_id'] == request_id:
                result = message.get('result')
                error = message.get('error')
                event.set()
        
        # 订阅响应
        subscription = EventBus.subscribe('plugin_response', on_response)
        
        try:
            # 发送请求
            request = {
                'version': '1.0',
                'timestamp': time.time(),
                'sender': self.get_name(),
                'recipient': recipient,
                'message_type': 'request',
                'data': {
                    'method': method,
                    'params': params,
                    'timeout': timeout
                },
                'priority': 0,
                'correlation_id': request_id,
                'metadata': {
                    'plugin_version': self.version
                }
            }
            
            EventBus.publish('plugin_request', message=request)
            
            # 等待响应
            if not event.wait(timeout):
                raise TimeoutError(f"请求超时: {method}")
            
            if error:
                raise Exception(f"请求错误: {error}")
            
            return result
        finally:
            # 取消订阅
            EventBus.unsubscribe('plugin_response', on_response)
    
    def send_async_request(self, recipient: str, method: str, params: Dict[str, Any], callback: callable = None) -> str:
        """
        异步发送请求
        
        Args:
            recipient: 接收者插件名
            method: 请求的方法名
            params: 请求参数
            callback: 回调函数，格式：callback(result, error)
            
        Returns:
            str: 请求ID
        """
        request_id = str(uuid.uuid4())
        
        # 响应处理函数
        def on_response(message):
            if message['sender'] == recipient and message['correlation_id'] == request_id:
                result = message.get('result')
                error = message.get('error')
                if callback:
                    threading.Thread(target=callback, args=(result, error)).start()
        
        # 订阅响应
        subscription = EventBus.subscribe('plugin_response', on_response)
        
        # 设置自动取消订阅的定时器
        def unsubscribe_timer():
            EventBus.unsubscribe('plugin_response', on_response)
        
        threading.Timer(30, unsubscribe_timer).start()
        
        # 发送请求
        request = {
            'version': '1.0',
            'timestamp': time.time(),
            'sender': self.get_name(),
            'recipient': recipient,
            'message_type': 'request',
            'data': {
                'method': method,
                'params': params,
                'timeout': 5
            },
            'priority': 0,
            'correlation_id': request_id,
            'metadata': {
                'plugin_version': self.version
            }
        }
        
        EventBus.publish('plugin_request', message=request)
        return request_id
    
    def publish_event(self, event_name: str, data: Any) -> str:
        """
        发布自定义事件
        
        Args:
            event_name: 事件名称
            data: 事件数据
            
        Returns:
            str: 事件ID
        """
        event_id = str(uuid.uuid4())
        event = {
            'version': '1.0',
            'timestamp': time.time(),
            'sender': self.get_name(),
            'event_name': event_name,
            'data': data,
            'correlation_id': event_id,
            'metadata': {
                'plugin_version': self.version
            }
        }
        
        EventBus.publish('plugin_event', message=event)
        return event_id
    
    def subscribe_event(self, event_name: str, handler: callable, sender: str = None) -> Any:
        """
        订阅自定义事件
        
        Args:
            event_name: 事件名称
            handler: 事件处理函数
            sender: 发送者插件名，None表示订阅所有发送者
            
        Returns:
            Any: 订阅ID
        """
        # 事件过滤处理
        def event_filter(message):
            if message['event_name'] == event_name:
                if sender is None or message['sender'] == sender:
                    handler(message)
        
        return EventBus.subscribe('plugin_event', event_filter)
    
    def unsubscribe_event(self, event_name: str, handler: callable) -> bool:
        """
        取消订阅事件
        
        Args:
            event_name: 事件名称
            handler: 事件处理函数
            
        Returns:
            bool: 是否取消成功
        """
        # 注意：由于我们使用了包装函数，这里无法直接取消订阅，需要改进
        # 目前暂时不实现具体逻辑，返回True
        return True
    
    def get_plugin_instance(self, plugin_name: str) -> Optional['PluginBase']:
        """
        获取插件实例
        
        Args:
            plugin_name: 插件名
            
        Returns:
            Optional[PluginBase]: 插件实例或None
        """
        if not self.plugin_manager:
            logger.warning("插件管理器未初始化，无法获取插件实例")
            return None
        
        return self.plugin_manager.get_plugin_instance(plugin_name)
    
    def call_plugin_method(self, plugin_name: str, method_name: str, *args, **kwargs) -> Any:
        """
        直接调用其他插件方法
        
        Args:
            plugin_name: 插件名
            method_name: 方法名
            *args: 位置参数
            **kwargs: 关键字参数
            
        Returns:
            Any: 方法返回值
        
        Raises:
            Exception: 调用错误
        """
        plugin = self.get_plugin_instance(plugin_name)
        if not plugin:
            raise Exception(f"插件不存在: {plugin_name}")
        
        if not hasattr(plugin, method_name):
            raise Exception(f"插件{plugin_name}没有方法: {method_name}")
        
        method = getattr(plugin, method_name)
        return method(*args, **kwargs)


class DataSourcePlugin(PluginBase):
    """
    数据源插件基类
    """
    
    @abstractmethod
    def get_stock_data(self, ts_code: str, start_date: str, end_date: str, freq: str = "daily") -> Any:
        """
        获取股票数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            freq: 周期，daily或minute
            
        Returns:
            Any: 股票数据，通常为DataFrame
        """
        pass
    
    @abstractmethod
    def get_index_data(self, ts_code: str, start_date: str, end_date: str, freq: str = "daily") -> Any:
        """
        获取指数数据
        
        Args:
            ts_code: 指数代码
            start_date: 开始日期
            end_date: 结束日期
            freq: 周期，daily或minute
            
        Returns:
            Any: 指数数据，通常为DataFrame
        """
        pass
    
    @abstractmethod
    def update_stock_basic(self) -> bool:
        """
        更新股票基本信息
        
        Returns:
            bool: 更新是否成功
        """
        pass


class IndicatorPlugin(PluginBase):
    """
    技术指标插件基类
    """
    
    @abstractmethod
    def calculate(self, data: Any, **kwargs) -> Any:
        """
        计算技术指标
        
        Args:
            data: 股票数据，通常为DataFrame
            **kwargs: 指标参数
            
        Returns:
            Any: 包含指标的数据，通常为DataFrame
        """
        pass
    
    @abstractmethod
    def get_required_columns(self) -> List[str]:
        """
        获取计算指标所需的列名
        
        Returns:
            List[str]: 所需列名列表
        """
        pass
    
    @abstractmethod
    def get_output_columns(self) -> List[str]:
        """
        获取指标计算输出的列名
        
        Returns:
            List[str]: 输出列名列表
        """
        pass
    
    def calculate_polars(self, data: Any, **kwargs) -> Any:
        """
        使用polars计算技术指标
        
        Args:
            data: 股票数据，polars DataFrame
            **kwargs: 指标参数
            
        Returns:
            Any: 包含指标的polars DataFrame
        """
        df_pd = data.to_pandas()
        result_pd = self.calculate(df_pd, **kwargs)
        return pl.from_pandas(result_pd)


class StrategyPlugin(PluginBase):
    """
    策略插件基类
    """
    
    @abstractmethod
    def initialize(self, config: Dict[str, Any]) -> bool:
        """
        初始化策略
        
        Args:
            config: 策略配置
            
        Returns:
            bool: 初始化是否成功
        """
        pass
    
    @abstractmethod
    def on_bar(self, data: Any) -> Dict[str, Any]:
        """
        K线数据更新时调用
        
        Args:
            data: K线数据
            
        Returns:
            Dict[str, Any]: 策略信号，如{'signal': 'buy', 'price': 10.0}
        """
        pass
    
    @abstractmethod
    def on_tick(self, data: Any) -> Dict[str, Any]:
        """
         tick数据更新时调用
        
        Args:
            data: tick数据
            
        Returns:
            Dict[str, Any]: 策略信号
        """
        pass
    
    @abstractmethod
    def get_parameters(self) -> Dict[str, Any]:
        """
        获取策略参数
        
        Returns:
            Dict[str, Any]: 策略参数
        """
        pass
    
    @abstractmethod
    def set_parameters(self, params: Dict[str, Any]) -> bool:
        """
        设置策略参数
        
        Args:
            params: 策略参数
            
        Returns:
            bool: 设置是否成功
        """
        pass


class VisualizationPlugin(PluginBase):
    """
    可视化插件基类
    """
    
    @abstractmethod
    def render(self, data: Any, container: Any = None, **kwargs) -> Any:
        """
        渲染可视化内容
        
        Args:
            data: 要可视化的数据
            container: 渲染容器，如Qt的Widget
            **kwargs: 渲染参数
            
        Returns:
            Any: 渲染结果，如图表对象
        """
        pass
    
    @abstractmethod
    def get_supported_data_types(self) -> List[str]:
        """
        获取支持的数据类型
        
        Returns:
            List[str]: 支持的数据类型列表，如['stock', 'index', 'indicator']
        """
        pass
    
    @abstractmethod
    def update_data(self, data: Any) -> None:
        """
        更新可视化数据
        
        Args:
            data: 新的数据
        """
        pass