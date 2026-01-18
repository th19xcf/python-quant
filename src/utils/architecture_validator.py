#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
架构验证工具，用于验证分层架构的正确性
"""

import importlib
import inspect
import os
import sys
from typing import Any, Dict, List, Optional, Set, Type
from loguru import logger


class Layer:
    """架构层次类"""
    
    def __init__(self, name: str, modules: List[str], allowed_dependencies: List[str] = None):
        """初始化架构层次
        
        Args:
            name: 层次名称
            modules: 层次包含的模块列表
            allowed_dependencies: 允许依赖的层次列表
        """
        self.name = name
        self.modules = modules
        self.allowed_dependencies = allowed_dependencies or []
    
    def __str__(self) -> str:
        return self.name


class ArchitectureValidator:
    """架构验证器"""
    
    def __init__(self):
        """初始化架构验证器"""
        self.layers: Dict[str, Layer] = {}
        self.violations: List[Dict[str, str]] = []
    
    def register_layer(self, layer: Layer):
        """注册架构层次
        
        Args:
            layer: 架构层次
        """
        self.layers[layer.name] = layer
    
    def _get_layer_for_module(self, module_name: str) -> Optional[Layer]:
        """获取模块所属的层次
        
        Args:
            module_name: 模块名称
        
        Returns:
            Optional[Layer]: 所属层次，如果不属于任何层次则返回None
        """
        for layer in self.layers.values():
            for module_pattern in layer.modules:
                if module_name.startswith(module_pattern):
                    return layer
        return None
    
    def _get_imports_for_module(self, module_name: str) -> List[str]:
        """获取模块的导入
        
        Args:
            module_name: 模块名称
        
        Returns:
            List[str]: 导入的模块列表
        """
        try:
            # 导入模块
            module = importlib.import_module(module_name)
            
            # 获取模块的所有导入
            imports = []
            for name, value in inspect.getmembers(module):
                if inspect.ismodule(value):
                    imports.append(value.__name__)
            
            # 获取模块的__all__属性
            if hasattr(module, '__all__'):
                for name in module.__all__:
                    if hasattr(module, name):
                        attr = getattr(module, name)
                        if inspect.ismodule(attr):
                            imports.append(attr.__name__)
            
            return imports
        except Exception as e:
            logger.exception(f"获取模块{module_name}的导入失败: {e}")
            return []
    
    def validate_dependencies(self) -> List[Dict[str, str]]:
        """验证分层依赖关系
        
        Returns:
            List[Dict[str, str]]: 依赖违规列表
        """
        violations = []
        
        # 遍历所有层次
        for layer in self.layers.values():
            # 遍历层次中的所有模块
            for module_pattern in layer.modules:
                try:
                    # 查找匹配的模块
                    module_files = self._find_modules(module_pattern)
                    for module_file in module_files:
                        # 转换为模块名称
                        module_name = self._file_to_module_name(module_file)
                        
                        # 获取模块的导入
                        imports = self._get_imports_for_module(module_name)
                        
                        # 检查每个导入是否合法
                        for import_name in imports:
                            # 获取导入所属的层次
                            import_layer = self._get_layer_for_module(import_name)
                            
                            if import_layer and import_layer.name != layer.name:
                                # 检查是否允许依赖该层次
                                if import_layer.name not in layer.allowed_dependencies:
                                    violations.append({
                                        'source_layer': layer.name,
                                        'source_module': module_name,
                                        'target_layer': import_layer.name,
                                        'target_module': import_name,
                                        'violation_type': 'dependency',
                                        'message': f"层次{layer.name}的模块{module_name}依赖了不允许的层次{import_layer.name}的模块{import_name}"
                                    })
                except Exception as e:
                    logger.exception(f"验证模块{module_pattern}的依赖失败: {e}")
        
        self.violations.extend(violations)
        return violations
    
    def validate_interface_implementations(self, interfaces: List[Type]) -> List[Dict[str, str]]:
        """验证接口实现
        
        Args:
            interfaces: 要验证的接口列表
        
        Returns:
            List[Dict[str, str]]: 接口实现违规列表
        """
        violations = []
        
        for interface in interfaces:
            # 查找所有实现该接口的类
            implementations = self._find_interface_implementations(interface)
            
            # 检查每个实现是否完整
            for impl in implementations:
                missing_methods = self._get_missing_methods(interface, impl)
                if missing_methods:
                    violations.append({
                        'interface': interface.__name__,
                        'implementation': impl.__name__,
                        'missing_methods': missing_methods,
                        'violation_type': 'interface',
                        'message': f"类{impl.__name__}缺少接口{interface.__name__}的方法: {', '.join(missing_methods)}"
                    })
        
        self.violations.extend(violations)
        return violations
    
    def validate_service_registration(self, service_registry: Any) -> List[Dict[str, str]]:
        """验证服务注册
        
        Args:
            service_registry: 服务注册中心实例
        
        Returns:
            List[Dict[str, str]]: 服务注册违规列表
        """
        violations = []
        
        try:
            # 获取所有注册的服务
            if hasattr(service_registry, 'get_all_services'):
                services = service_registry.get_all_services()
                
                for service_name, service_info in services.items():
                    # 检查服务是否有对应的接口
                    if 'interface' not in service_info:
                        violations.append({
                            'service_name': service_name,
                            'violation_type': 'service',
                            'message': f"服务{service_name}没有指定接口"
                        })
        except Exception as e:
            logger.exception(f"验证服务注册失败: {e}")
        
        self.violations.extend(violations)
        return violations
    
    def validate_dependency_injection(self, modules: List[str]) -> List[Dict[str, str]]:
        """验证依赖注入使用
        
        Args:
            modules: 要验证的模块列表
        
        Returns:
            List[Dict[str, str]]: 依赖注入违规列表
        """
        violations = []
        
        for module_pattern in modules:
            try:
                # 查找匹配的模块
                module_files = self._find_modules(module_pattern)
                for module_file in module_files:
                    # 转换为模块名称
                    module_name = self._file_to_module_name(module_file)
                    
                    # 导入模块
                    module = importlib.import_module(module_name)
                    
                    # 检查模块中的类
                    for name, obj in inspect.getmembers(module):
                        if inspect.isclass(obj):
                            # 检查构造函数是否使用了依赖注入
                            if self._has_dependency_injection(obj):
                                # 检查是否有@inject装饰器
                                if not self._has_inject_decorator(obj):
                                    violations.append({
                                        'module': module_name,
                                        'class_name': obj.__name__,
                                        'violation_type': 'dependency_injection',
                                        'message': f"类{obj.__name__}的构造函数使用了依赖注入，但没有@inject装饰器"
                                    })
            except Exception as e:
                logger.exception(f"验证模块{module_pattern}的依赖注入失败: {e}")
        
        self.violations.extend(violations)
        return violations
    
    def _find_modules(self, module_pattern: str) -> List[str]:
        """查找匹配的模块文件
        
        Args:
            module_pattern: 模块模式
        
        Returns:
            List[str]: 匹配的模块文件列表
        """
        module_files = []
        base_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "..")
        
        # 转换模块模式为文件模式
        file_pattern = module_pattern.replace(".", os.sep)
        
        # 查找匹配的文件
        for root, dirs, files in os.walk(base_path):
            for file in files:
                if file.endswith(".py"):
                    file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(file_path, base_path)
                    relative_path = relative_path.replace(os.sep, ".")[:-3]
                    
                    if relative_path.startswith(module_pattern):
                        module_files.append(file_path)
        
        return module_files
    
    def _file_to_module_name(self, file_path: str) -> str:
        """将文件路径转换为模块名称
        
        Args:
            file_path: 文件路径
        
        Returns:
            str: 模块名称
        """
        base_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "..")
        relative_path = os.path.relpath(file_path, base_path)
        module_name = relative_path.replace(os.sep, ".")[:-3]
        return module_name
    
    def _find_interface_implementations(self, interface: Type) -> List[Type]:
        """查找接口的所有实现
        
        Args:
            interface: 接口类型
        
        Returns:
            List[Type]: 实现类列表
        """
        implementations = []
        
        # 遍历所有已加载的模块
        for module_name, module in sys.modules.items():
            if module_name.startswith('src.'):
                try:
                    for name, obj in inspect.getmembers(module):
                        if inspect.isclass(obj):
                            # 检查是否实现了接口
                            if interface in obj.__bases__:
                                implementations.append(obj)
                except Exception as e:
                    logger.exception(f"检查模块{module_name}的接口实现失败: {e}")
        
        return implementations
    
    def _get_missing_methods(self, interface: Type, implementation: Type) -> List[str]:
        """获取实现类缺少的接口方法
        
        Args:
            interface: 接口类型
            implementation: 实现类
        
        Returns:
            List[str]: 缺少的方法列表
        """
        missing = []
        
        # 获取接口的所有方法
        interface_methods = {
            name for name, _ in inspect.getmembers(interface, predicate=inspect.isfunction)
        }
        
        # 获取实现类的所有方法
        implementation_methods = {
            name for name, _ in inspect.getmembers(implementation, predicate=inspect.isfunction)
        }
        
        # 检查缺少的方法
        for method in interface_methods:
            if method not in implementation_methods:
                missing.append(method)
        
        return missing
    
    def _has_dependency_injection(self, cls: Type) -> bool:
        """检查类的构造函数是否使用了依赖注入
        
        Args:
            cls: 类类型
        
        Returns:
            bool: 是否使用了依赖注入
        """
        try:
            init = cls.__init__
            sig = inspect.signature(init)
            
            # 检查是否有非自参数
            params = list(sig.parameters.values())
            if len(params) > 1:  # 至少有self和一个参数
                # 检查参数是否有类型注解
                for param in params[1:]:  # 跳过self
                    if param.annotation != inspect.Parameter.empty:
                        return True
        except Exception as e:
            logger.exception(f"检查类{cls.__name__}的依赖注入失败: {e}")
        
        return False
    
    def _has_inject_decorator(self, cls: Type) -> bool:
        """检查类是否有@inject装饰器
        
        Args:
            cls: 类类型
        
        Returns:
            bool: 是否有@inject装饰器
        """
        # 检查类的__init__方法是否有装饰器
        if hasattr(cls.__init__, '__wrapped__'):
            return True
        
        # 检查类是否有装饰器标记
        if hasattr(cls, '__inject_decorated__'):
            return True
        
        return False
    
    def validate(self) -> bool:
        """执行完整的架构验证
        
        Returns:
            bool: 是否通过验证
        """
        logger.info("开始执行架构验证")
        
        # 验证依赖关系
        self.validate_dependencies()
        
        logger.info(f"架构验证完成，共发现{len(self.violations)}个违规")
        
        # 输出所有违规
        for violation in self.violations:
            logger.warning(f"[{violation['violation_type']}] {violation['message']}")
        
        return len(self.violations) == 0
    
    def get_violations(self) -> List[Dict[str, str]]:
        """获取所有违规
        
        Returns:
            List[Dict[str, str]]: 违规列表
        """
        return self.violations.copy()


def create_default_layers() -> ArchitectureValidator:
    """创建默认的架构层次
    
    Returns:
        ArchitectureValidator: 架构验证器实例
    """
    validator = ArchitectureValidator()
    
    # 定义架构层次
    # 基础设施层
    infra_layer = Layer(
        name="infrastructure",
        modules=["src.utils", "src.plugin", "src.event_bus"],
        allowed_dependencies=[]  # 基础设施层不依赖其他层次
    )
    
    # 数据访问层
    data_layer = Layer(
        name="data_access",
        modules=["src.data", "src.database"],
        allowed_dependencies=["infrastructure"]  # 数据访问层只能依赖基础设施层
    )
    
    # 业务逻辑层
    business_layer = Layer(
        name="business_logic",
        modules=["src.tech_analysis", "src.business"],
        allowed_dependencies=["data_access", "infrastructure"]  # 业务逻辑层可以依赖数据访问层和基础设施层
    )
    
    # API层
    api_layer = Layer(
        name="api",
        modules=["src.api"],
        allowed_dependencies=["business_logic", "data_access", "infrastructure"]  # API层可以依赖业务逻辑层、数据访问层和基础设施层
    )
    
    # UI层
    ui_layer = Layer(
        name="ui",
        modules=["src.ui"],
        allowed_dependencies=["api", "business_logic", "data_access", "infrastructure"]  # UI层可以依赖所有上层
    )
    
    # 注册层次
    validator.register_layer(infra_layer)
    validator.register_layer(data_layer)
    validator.register_layer(business_layer)
    validator.register_layer(api_layer)
    validator.register_layer(ui_layer)
    
    return validator


# 创建默认的架构验证器
default_validator = create_default_layers()
