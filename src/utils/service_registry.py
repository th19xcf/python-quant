#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
服务注册中心，集中管理所有服务的注册和发现
"""

from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, Union
from .dependency_injector import DependencyInjector, dependency_injector

T = TypeVar('T')


class ServiceRegistry:
    """服务注册中心，用于管理所有服务的注册和发现"""
    
    def __init__(self):
        """初始化服务注册中心"""
        self._services: Dict[Union[Type, str], Dict[str, Any]] = {}
        self._registrations: List[Callable] = []
        self._initialized = False
    
    def register_service(self, interface: Union[Type, str], implementation: Any, **kwargs):
        """注册服务
        
        Args:
            interface: 接口类型或服务名称
            implementation: 实现类或实例
            **kwargs: 其他参数
                - singleton: 是否为单例模式，默认True
                - factory: 工厂函数，用于创建实例
                - args: 构造函数位置参数
                - kwargs: 构造函数关键字参数
        """
        self._services[interface] = {
            'implementation': implementation,
            'kwargs': kwargs
        }
    
    def register_singleton(self, interface: Union[Type, str], implementation: Any, **kwargs):
        """注册单例服务
        
        Args:
            interface: 接口类型或服务名称
            implementation: 实现类或实例
            **kwargs: 其他参数
        """
        self.register_service(interface, implementation, singleton=True, **kwargs)
    
    def register_transient(self, interface: Union[Type, str], implementation: Any, **kwargs):
        """注册原型服务（每次请求创建新实例）
        
        Args:
            interface: 接口类型或服务名称
            implementation: 实现类或实例
            **kwargs: 其他参数
        """
        self.register_service(interface, implementation, singleton=False, **kwargs)
    
    def register_factory(self, interface: Union[Type, str], factory: Callable, **kwargs):
        """注册工厂服务
        
        Args:
            interface: 接口类型或服务名称
            factory: 工厂函数，用于创建实例
            **kwargs: 其他参数
        """
        self.register_service(interface, None, factory=factory, **kwargs)
    
    def register_decorator(self, interface: Union[Type, str], decorator: Callable):
        """注册服务装饰器
        
        Args:
            interface: 接口类型或服务名称
            decorator: 装饰器函数，用于装饰服务实例
        """
        if interface not in self._services:
            self._services[interface] = {}
        self._services[interface]['decorator'] = decorator
    
    def add_registration_callback(self, callback: Callable):
        """添加服务注册回调
        
        Args:
            callback: 回调函数，在服务初始化时调用
        """
        self._registrations.append(callback)
    
    def initialize_services(self):
        """初始化所有服务
        
        将注册的服务注册到依赖注入容器中
        """
        if self._initialized:
            return
        
        # 执行注册回调
        for callback in self._registrations:
            callback()
        
        # 注册服务到依赖注入容器
        for interface, service_info in self._services.items():
            implementation = service_info['implementation']
            kwargs = service_info.get('kwargs', {})
            decorator = service_info.get('decorator', None)
            
            if decorator and implementation:
                # 如果有装饰器，创建装饰后的实现
                def create_decorated_instance(*args, **kwargs_inner):
                    instance = implementation(*args, **kwargs_inner)
                    return decorator(instance)
                dependency_injector.register(interface, create_decorated_instance, **kwargs)
            else:
                dependency_injector.register(interface, implementation, **kwargs)
        
        self._initialized = True
    
    def get_service(self, interface: Union[Type, str]) -> Any:
        """获取服务实例
        
        Args:
            interface: 接口类型或服务名称
        
        Returns:
            Any: 服务实例
        """
        if not self._initialized:
            self.initialize_services()
        return dependency_injector.resolve(interface)
    
    def has_service(self, interface: Union[Type, str]) -> bool:
        """检查服务是否已注册
        
        Args:
            interface: 接口类型或服务名称
        
        Returns:
            bool: 是否已注册
        """
        return interface in self._services or dependency_injector.has_service(interface)
    
    def get_all_services(self) -> Dict[Union[Type, str], Dict[str, Any]]:
        """获取所有注册的服务
        
        Returns:
            Dict[Union[Type, str], Dict[str, Any]]: 所有注册的服务
        """
        return self._services.copy()
    
    def clear(self):
        """清除所有服务注册
        """
        self._services.clear()
        self._registrations.clear()
        self._initialized = False
        dependency_injector.clear()


# 创建全局服务注册中心实例
service_registry = ServiceRegistry()


# 服务注册装饰器
def register_service(interface: Optional[Union[Type, str]] = None, **kwargs):
    """服务注册装饰器，用于自动注册服务
    
    Args:
        interface: 接口类型或服务名称，默认为类本身
        **kwargs: 其他参数，传递给register_service方法
    
    Returns:
        Callable: 装饰器函数
    """
    def decorator(cls):
        nonlocal interface
        if interface is None:
            interface = cls
        service_registry.register_service(interface, cls, **kwargs)
        return cls
    return decorator


def register_singleton(interface: Optional[Union[Type, str]] = None, **kwargs):
    """单例服务注册装饰器
    
    Args:
        interface: 接口类型或服务名称，默认为类本身
        **kwargs: 其他参数，传递给register_singleton方法
    
    Returns:
        Callable: 装饰器函数
    """
    def decorator(cls):
        nonlocal interface
        if interface is None:
            interface = cls
        service_registry.register_singleton(interface, cls, **kwargs)
        return cls
    return decorator


def register_transient(interface: Optional[Union[Type, str]] = None, **kwargs):
    """原型服务注册装饰器
    
    Args:
        interface: 接口类型或服务名称，默认为类本身
        **kwargs: 其他参数，传递给register_transient方法
    
    Returns:
        Callable: 装饰器函数
    """
    def decorator(cls):
        nonlocal interface
        if interface is None:
            interface = cls
        service_registry.register_transient(interface, cls, **kwargs)
        return cls
    return decorator


def inject(interface: Optional[Union[Type, str]] = None):
    """依赖注入装饰器，用于自动注入依赖
    
    Args:
        interface: 接口类型或服务名称，默认为类本身
        **kwargs: 其他参数，传递给dependency_injector.inject方法
    
    Returns:
        Callable: 装饰器函数
    """
    def decorator(cls):
        # 使用已有的inject装饰器
        from .dependency_injector import inject as di_inject
        return di_inject(cls)
    return decorator
