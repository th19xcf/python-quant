#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
依赖注入容器，用于管理各层之间的依赖关系
"""

from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, Union

T = TypeVar('T')


class DependencyInjector:
    """依赖注入容器，用于管理和解析依赖关系"""
    
    def __init__(self):
        """初始化依赖注入容器"""
        self._services: Dict[Union[Type, str], Dict[str, Any]] = {}
        self._instances: Dict[Union[Type, str], Any] = {}
        self._singletons: Dict[Union[Type, str], Any] = {}
    
    def register(self, interface: Union[Type, str], implementation: Any, **kwargs):
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
            'singleton': kwargs.get('singleton', True),
            'factory': kwargs.get('factory', None),
            'args': kwargs.get('args', []),
            'kwargs': kwargs.get('kwargs', {})
        }
    
    def register_singleton(self, interface: Union[Type, str], implementation: Any, **kwargs):
        """注册单例服务
        
        Args:
            interface: 接口类型或服务名称
            implementation: 实现类或实例
            **kwargs: 其他参数
        """
        self.register(interface, implementation, singleton=True, **kwargs)
    
    def register_transient(self, interface: Union[Type, str], implementation: Any, **kwargs):
        """注册原型服务（每次请求创建新实例）
        
        Args:
            interface: 接口类型或服务名称
            implementation: 实现类或实例
            **kwargs: 其他参数
        """
        self.register(interface, implementation, singleton=False, **kwargs)
    
    def resolve(self, interface: Union[Type, str], **kwargs) -> Any:
        """解析服务
        
        Args:
            interface: 接口类型或服务名称
            **kwargs: 额外的构造函数参数
        
        Returns:
            Any: 服务实例
        """
        if interface in self._singletons:
            return self._singletons[interface]
        
        if interface not in self._services:
            raise ValueError(f"服务未注册: {interface}")
        
        service = self._services[interface]
        implementation = service['implementation']
        singleton = service['singleton']
        factory = service['factory']
        args = service['args']
        service_kwargs = service['kwargs'].copy()
        service_kwargs.update(kwargs)
        
        # 创建实例
        if factory is not None:
            # 使用工厂函数创建实例
            instance = factory(*args, **service_kwargs)
        elif isinstance(implementation, type):
            # 实现类，实例化
            instance = implementation(*args, **service_kwargs)
        else:
            # 已经是实例
            instance = implementation
        
        # 如果是单例模式，缓存实例
        if singleton:
            self._singletons[interface] = instance
        
        return instance
    
    def get_instance(self, interface: Union[Type, str], **kwargs) -> Any:
        """获取服务实例（与resolve方法相同，只是别名）
        
        Args:
            interface: 接口类型或服务名称
            **kwargs: 额外的构造函数参数
        
        Returns:
            Any: 服务实例
        """
        return self.resolve(interface, **kwargs)
    
    def has_service(self, interface: Union[Type, str]) -> bool:
        """检查服务是否已注册
        
        Args:
            interface: 接口类型或服务名称
        
        Returns:
            bool: 是否已注册
        """
        return interface in self._services
    
    def clear(self, interface: Optional[Union[Type, str]] = None):
        """清除服务实例
        
        Args:
            interface: 接口类型或服务名称，None表示清除所有实例
        """
        if interface:
            if interface in self._singletons:
                del self._singletons[interface]
            if interface in self._instances:
                del self._instances[interface]
        else:
            self._singletons.clear()
            self._instances.clear()
    
    def remove(self, interface: Union[Type, str]):
        """移除服务注册
        
        Args:
            interface: 接口类型或服务名称
        """
        if interface in self._services:
            del self._services[interface]
        self.clear(interface)
    
    def get_all_services(self) -> Dict[Union[Type, str], Dict[str, Any]]:
        """获取所有注册的服务
        
        Returns:
            Dict[Union[Type, str], Dict[str, Any]]: 所有注册的服务
        """
        return self._services.copy()
    
    def get_all_instances(self) -> Dict[Union[Type, str], Any]:
        """获取所有已创建的单例实例
        
        Returns:
            Dict[Union[Type, str], Any]: 所有已创建的单例实例
        """
        return self._singletons.copy()


# 创建全局依赖注入容器实例
dependency_injector = DependencyInjector()


def inject(cls: Type[T]) -> Type[T]:
    """依赖注入装饰器，用于自动注入依赖
    
    Args:
        cls: 要注入依赖的类
    
    Returns:
        Type[T]: 注入后的类
    """
    original_init = cls.__init__
    
    def __init__(self, *args, **kwargs):
        # 获取构造函数参数
        import inspect
        sig = inspect.signature(original_init)
        params = sig.parameters
        
        # 准备注入的依赖
        injected_kwargs = {}
        
        # 处理位置参数
        args_list = list(args)
        param_keys = list(params.keys())[1:]  # 跳过self
        
        # 为位置参数创建映射
        positional_kwargs = {}
        for i, key in enumerate(param_keys):
            if i < len(args_list):
                positional_kwargs[key] = args_list[i]
        
        # 检查参数是否需要注入
        for name, param in list(params.items())[1:]:  # 跳过self
            # 如果参数已经通过位置参数提供，跳过注入
            if name in positional_kwargs:
                injected_kwargs[name] = positional_kwargs[name]
                continue
                
            # 如果参数已经通过关键字参数提供，跳过注入
            if name in kwargs:
                injected_kwargs[name] = kwargs[name]
                continue
            
            # 尝试注入依赖
            if param.annotation != inspect.Parameter.empty:
                # 尝试从容器中解析依赖
                try:
                    injected_kwargs[name] = dependency_injector.resolve(param.annotation)
                except ValueError:
                    # 如果容器中没有注册，使用默认值
                    if param.default != inspect.Parameter.empty:
                        injected_kwargs[name] = param.default
                    else:
                        # 如果没有默认值且不是可选参数，抛出异常
                        raise ValueError(f"无法解析依赖: {param.annotation}，参数名: {name}")
            elif param.default != inspect.Parameter.empty:
                # 如果没有注解但有默认值，使用默认值
                injected_kwargs[name] = param.default
            elif name not in kwargs:
                # 如果没有注解、没有默认值且没有提供值，抛出异常
                raise ValueError(f"缺少必填参数: {name}")
        
        # 调用原始构造函数，只传递关键字参数
        original_init(self, **injected_kwargs)
    
    cls.__init__ = __init__
    return cls