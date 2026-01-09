#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
插件管理器，负责插件的加载、初始化和管理
"""

import os
import sys
import importlib
import inspect
from pathlib import Path
from typing import Dict, List, Any, Optional
from loguru import logger
from src.plugin.plugin_base import PluginBase
from src.plugin.plugin_registry import PluginRegistry, global_registry
from src.utils.config import Config
from src.utils.plugin_config_manager import get_plugin_config_manager
from src.utils.event_bus import EventBus


class PluginManager:
    """
    插件管理器，负责插件的加载、初始化和管理
    """
    
    def __init__(self, config: Config):
        """
        初始化插件管理器
        
        Args:
            config: 配置对象
        """
        self.config = config
        self.registry = global_registry
        
        # 插件实例字典，按插件类型和名称分类
        self.plugin_instances: Dict[str, Dict[str, PluginBase]] = {
            'datasource': {},
            'indicator': {},
            'strategy': {},
            'visualization': {},
            'other': {}
        }
        
        # 插件目录
        self.plugin_dirs = self._get_plugin_dirs()
        
        # 初始化插件配置管理器
        self.plugin_config_manager = get_plugin_config_manager(config)
        
        # 插件初始化顺序，由_resolve_plugin_order方法生成
        self._plugin_initialization_order = []
        
        # 订阅配置变更事件
        EventBus.subscribe('plugin_config_changed', self._on_plugin_config_changed)
        EventBus.subscribe('plugin_config_reloaded', self._on_plugin_config_reloaded)
    
    def _get_plugin_dirs(self) -> List[str]:
        """
        获取插件目录列表
        
        Returns:
            List[str]: 插件目录列表
        """
        # 默认插件目录
        default_dir = Path(__file__).parent.parent.parent / "plugins"
        
        # 配置文件中指定的插件目录
        config_dirs = getattr(self.config, 'plugin_dirs', [])
        
        # 合并目录列表，去重
        plugin_dirs = []
        if default_dir.exists():
            plugin_dirs.append(str(default_dir))
        
        for dir_path in config_dirs:
            if os.path.exists(dir_path) and dir_path not in plugin_dirs:
                plugin_dirs.append(dir_path)
        
        return plugin_dirs
    
    def load_plugins(self) -> int:
        """
        加载所有插件
        
        Returns:
            int: 成功加载的插件数量
        """
        logger.info(f"开始加载插件，插件目录: {self.plugin_dirs}")
        
        loaded_count = 0
        
        for plugin_dir in self.plugin_dirs:
            if not os.path.exists(plugin_dir):
                logger.warning(f"插件目录不存在: {plugin_dir}")
                continue
            
            # 将插件目录添加到Python路径
            if plugin_dir not in sys.path:
                sys.path.insert(0, plugin_dir)
            
            # 遍历插件目录
            for item in os.listdir(plugin_dir):
                item_path = os.path.join(plugin_dir, item)
                
                # 处理Python文件
                if os.path.isfile(item_path) and item.endswith('.py') and not item.startswith('__'):
                    loaded = self._load_plugin_from_file(item_path)
                    if loaded:
                        loaded_count += 1
                
                # 处理目录
                elif os.path.isdir(item_path) and os.path.exists(os.path.join(item_path, '__init__.py')):
                    loaded = self._load_plugin_from_package(item_path)
                    if loaded:
                        loaded_count += 1
        
        logger.info(f"插件加载完成，共加载 {loaded_count} 个插件")
        return loaded_count
    
    def _load_plugin_from_file(self, file_path: str) -> bool:
        """
        从Python文件加载插件
        
        Args:
            file_path: Python文件路径
            
        Returns:
            bool: 加载是否成功
        """
        try:
            # 获取模块名
            module_name = os.path.splitext(os.path.basename(file_path))[0]
            
            # 导入模块
            module = importlib.import_module(module_name)
            
            # 查找插件类
            for name, obj in inspect.getmembers(module, inspect.isclass):
                if issubclass(obj, PluginBase) and obj is not PluginBase:
                    # 注册插件
                    if self.registry.register_plugin(obj):
                        logger.info(f"成功从文件加载插件: {file_path} -> {obj.__name__}")
                        return True
        except Exception as e:
            logger.error(f"从文件加载插件失败: {file_path}, 错误: {e}")
        
        return False
    
    def _load_plugin_from_package(self, package_path: str) -> bool:
        """
        从Python包加载插件
        
        Args:
            package_path: 包路径
            
        Returns:
            bool: 加载是否成功
        """
        try:
            # 获取包名
            package_name = os.path.basename(package_path)
            
            # 导入包
            module = importlib.import_module(package_name)
            
            # 查找插件类
            for name, obj in inspect.getmembers(module, inspect.isclass):
                if issubclass(obj, PluginBase) and obj is not PluginBase:
                    # 注册插件
                    if self.registry.register_plugin(obj):
                        logger.info(f"成功从包加载插件: {package_path} -> {obj.__name__}")
                        return True
        except Exception as e:
            logger.error(f"从包加载插件失败: {package_path}, 错误: {e}")
        
        return False
    
    def _on_plugin_config_changed(self, plugin_name: str, old_config: Dict[str, Any], new_config: Dict[str, Any]):
        """
        处理插件配置变更事件
        
        Args:
            plugin_name: 插件名称
            old_config: 旧配置
            new_config: 新配置
        """
        logger.debug(f"插件配置已变更: {plugin_name}")
        
    def _on_plugin_config_reloaded(self, plugin_name: str, config: Dict[str, Any]):
        """
        处理插件配置重新加载事件
        
        Args:
            plugin_name: 插件名称
            config: 新配置
        """
        logger.debug(f"插件配置已重新加载: {plugin_name}")
        
        # 通知插件配置已变更
        plugin_instance = self.get_plugin_instance(plugin_name)
        if plugin_instance:
            plugin_instance.on_config_changed({}, config)
    
    def _build_dependency_graph(self) -> Dict[str, List[str]]:
        """
        构建插件依赖图
        
        Returns:
            Dict[str, List[str]]: 依赖图，键为插件名称，值为依赖插件名称列表
        """
        dependency_graph = {}
        
        # 获取所有插件元数据
        all_metadata = self.registry.get_all_plugin_metadata()
        
        for plugin_meta in all_metadata:
            plugin_name = plugin_meta['name']
            dependencies = plugin_meta.get('dependencies', [])
            dependency_names = [dep['name'] for dep in dependencies]
            dependency_graph[plugin_name] = dependency_names
        
        return dependency_graph
    
    def _topological_sort(self, graph: Dict[str, List[str]]) -> List[str] or None:
        """
        拓扑排序，确定插件初始化顺序
        
        Args:
            graph: 依赖图，键为插件名称，值为依赖插件名称列表
            
        Returns:
            List[str] or None: 拓扑排序结果，或None表示存在循环依赖
        """
        # 计算每个节点的入度
        in_degree = {node: 0 for node in graph}
        for node in graph:
            for neighbor in graph[node]:
                if neighbor in in_degree:
                    in_degree[neighbor] += 1
        
        # 初始化队列，包含所有入度为0的节点
        queue = [node for node in in_degree if in_degree[node] == 0]
        result = []
        
        # 执行拓扑排序
        while queue:
            node = queue.pop(0)
            result.append(node)
            
            # 减少所有邻居节点的入度
            for neighbor in graph[node]:
                if neighbor in in_degree:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)
        
        # 检查是否所有节点都被处理
        if len(result) != len(in_degree):
            # 存在循环依赖
            return None
        
        return result
    
    def _resolve_plugin_order(self) -> List[Dict[str, Any]] or None:
        """
        解析插件初始化顺序
        
        Returns:
            List[Dict[str, Any]] or None: 排序后的插件列表，或None表示存在循环依赖
        """
        # 构建依赖图
        dependency_graph = self._build_dependency_graph()
        
        # 拓扑排序
        sorted_plugin_names = self._topological_sort(dependency_graph)
        if not sorted_plugin_names:
            logger.error("检测到插件循环依赖，无法确定初始化顺序")
            return None
        
        # 获取所有插件元数据
        all_metadata = self.registry.get_all_plugin_metadata()
        plugin_meta_dict = {meta['name']: meta for meta in all_metadata}
        
        # 按排序结果组织插件列表
        sorted_plugins = []
        for plugin_name in sorted_plugin_names:
            if plugin_name in plugin_meta_dict:
                sorted_plugins.append(plugin_meta_dict[plugin_name])
        
        return sorted_plugins
    
    def initialize_plugins(self) -> int:
        """
        初始化插件系统，但不立即实例化所有插件
        
        Returns:
            int: 成功初始化的插件数量（仅返回已解析的插件数量）
        """
        logger.info("开始初始化插件系统")
        
        # 解析插件初始化顺序，确保依赖关系正确
        sorted_plugins = self._resolve_plugin_order()
        if not sorted_plugins:
            logger.error("无法确定插件初始化顺序，初始化失败")
            return 0
        
        # 存储插件初始化顺序，供后续懒加载使用
        self._plugin_initialization_order = sorted_plugins
        
        logger.info(f"插件系统初始化完成，共解析 {len(sorted_plugins)} 个插件的依赖关系")
        return len(sorted_plugins)
    
    def get_plugin_instance(self, plugin_name: str, plugin_type: str = None, lazy_load: bool = True) -> Optional[PluginBase]:
        """
        获取插件实例，支持懒加载和依赖自动加载
        
        Args:
            plugin_name: 插件名称
            plugin_type: 插件类型，如不指定则在所有类型中查找
            lazy_load: 是否支持懒加载，默认为True
            
        Returns:
            Optional[PluginBase]: 插件实例或None
        """
        # 首先尝试从已实例化的插件中查找
        if plugin_type:
            if plugin_name in self.plugin_instances[plugin_type]:
                return self.plugin_instances[plugin_type][plugin_name]
        else:
            # 在所有类型中查找
            for type_key in self.plugin_instances:
                if plugin_name in self.plugin_instances[type_key]:
                    return self.plugin_instances[type_key][plugin_name]
        
        # 如果不支持懒加载，直接返回None
        if not lazy_load:
            return None
        
        # 支持懒加载，尝试创建并初始化插件
        try:
            # 获取插件元数据
            plugin_meta = self.registry.get_plugin_metadata(plugin_name, plugin_type)
            if not plugin_meta:
                return None
            
            plugin_type = plugin_meta['type']
            plugin_class = plugin_meta['class']
            dependencies = plugin_meta.get('dependencies', [])
            
            # 首先加载所有依赖插件
            for dep in dependencies:
                dep_name = dep['name']
                dep_version = dep['version']
                
                # 递归加载依赖插件
                dep_instance = self.get_plugin_instance(dep_name, lazy_load=True)
                if not dep_instance:
                    logger.error(f"无法加载插件依赖: {dep_name}，插件{plugin_name}加载失败")
                    return None
            
            # 创建插件实例
            plugin_instance = plugin_class()
            
            # 设置插件管理器引用
            plugin_instance.plugin_manager = self
            
            # 设置加载状态
            plugin_instance.set_load_status('loaded')
            
            # 初始化插件
            if plugin_instance.initialize(self.config):
                # 更新加载状态
                plugin_instance.set_load_status('initialized')
                # 保存插件实例
                self.plugin_instances[plugin_type][plugin_name] = plugin_instance
                logger.info(f"懒加载插件成功: {plugin_name} (类型: {plugin_type})")
                return plugin_instance
            else:
                logger.warning(f"懒加载插件初始化失败: {plugin_name} (类型: {plugin_type})")
                return None
        except Exception as e:
            logger.error(f"懒加载插件异常: {plugin_name}, 错误: {e}")
            return None
    
    def update_plugin_config(self, plugin_name: str, config: Dict[str, Any]) -> bool:
        """
        更新插件配置
        
        Args:
            plugin_name: 插件名称
            config: 新的插件配置
            
        Returns:
            bool: 更新是否成功
        """
        # 更新到插件配置管理器
        success = self.plugin_config_manager.update_plugin_config(plugin_name, config)
        
        if success:
            # 通知插件重新加载配置
            plugin_instance = self.get_plugin_instance(plugin_name)
            if plugin_instance:
                plugin_instance.reload_config()
                logger.info(f"插件配置已更新: {plugin_name}")
        
        return success
    
    def get_plugin_config(self, plugin_name: str) -> Dict[str, Any]:
        """
        获取插件配置
        
        Args:
            plugin_name: 插件名称
            
        Returns:
            Dict[str, Any]: 插件配置
        """
        return self.plugin_config_manager.get_plugin_config(plugin_name)
    
    def save_plugin_configs(self, file_path: str = "plugins_config.json") -> bool:
        """
        保存所有插件配置到文件
        
        Args:
            file_path: 配置文件路径
            
        Returns:
            bool: 保存是否成功
        """
        return self.plugin_config_manager.save_plugin_configs(file_path)
    
    def load_plugin_configs(self, file_path: str = "plugins_config.json") -> bool:
        """
        从文件加载所有插件配置
        
        Args:
            file_path: 配置文件路径
            
        Returns:
            bool: 加载是否成功
        """
        success = self.plugin_config_manager.load_plugin_configs(file_path)
        
        if success:
            # 通知所有插件重新加载配置
            for type_key in self.plugin_instances:
                for plugin_instance in self.plugin_instances[type_key].values():
                    plugin_instance.reload_config()
            
            logger.info("所有插件配置已从文件加载")
        
        return success
    
    def validate_all_plugin_configs(self) -> bool:
        """
        验证所有插件配置
        
        Returns:
            bool: 所有配置是否都有效
        """
        all_valid = True
        
        for type_key in self.plugin_instances:
            for plugin_instance in self.plugin_instances[type_key].values():
                if not plugin_instance.validate_config():
                    all_valid = False
        
        return all_valid
    
    def get_plugin_instances_by_type(self, plugin_type: str) -> Dict[str, PluginBase]:
        """
        按类型获取插件实例
        
        Args:
            plugin_type: 插件类型
            
        Returns:
            Dict[str, PluginBase]: 插件实例字典
        """
        return self.plugin_instances.get(plugin_type, {})
    
    def enable_plugin(self, plugin_name: str, plugin_type: str = None) -> bool:
        """
        启用插件
        
        Args:
            plugin_name: 插件名称
            plugin_type: 插件类型，如不指定则在所有类型中查找
            
        Returns:
            bool: 启用是否成功
        """
        plugin_instance = self.get_plugin_instance(plugin_name, plugin_type)
        if plugin_instance:
            plugin_instance.set_enabled(True)
            logger.info(f"插件已启用: {plugin_name}")
            return True
        return False
    
    def disable_plugin(self, plugin_name: str, plugin_type: str = None) -> bool:
        """
        禁用插件
        
        Args:
            plugin_name: 插件名称
            plugin_type: 插件类型，如不指定则在所有类型中查找
            
        Returns:
            bool: 禁用是否成功
        """
        plugin_instance = self.get_plugin_instance(plugin_name, plugin_type)
        if plugin_instance:
            plugin_instance.set_enabled(False)
            logger.info(f"插件已禁用: {plugin_name}")
            return True
        return False
    
    def shutdown_plugins(self) -> int:
        """
        关闭所有插件
        
        Returns:
            int: 成功关闭的插件数量
        """
        logger.info("开始关闭插件")
        
        shutdown_count = 0
        
        # 遍历所有插件实例
        for type_key in self.plugin_instances:
            for plugin_name, plugin_instance in self.plugin_instances[type_key].items():
                try:
                    if plugin_instance.shutdown():
                        shutdown_count += 1
                        logger.info(f"插件关闭成功: {plugin_name} (类型: {type_key})")
                except Exception as e:
                    logger.error(f"插件关闭异常: {plugin_name} (类型: {type_key}), 错误: {e}")
        
        # 清空插件实例
        self.plugin_instances = {
            'datasource': {},
            'indicator': {},
            'strategy': {},
            'visualization': {},
            'other': {}
        }
        
        logger.info(f"插件关闭完成，共关闭 {shutdown_count} 个插件")
        return shutdown_count
    
    def reload_plugins(self) -> int:
        """
        重新加载所有插件
        
        Returns:
            int: 成功重新加载的插件数量
        """
        # 先关闭所有插件
        self.shutdown_plugins()
        
        # 清空注册表
        self.registry.clear()
        
        # 重新加载插件
        loaded_count = self.load_plugins()
        
        # 重新初始化插件
        self.initialize_plugins()
        
        return loaded_count
    
    def get_plugin_info(self, plugin_name: str, plugin_type: str = None) -> Optional[Dict[str, Any]]:
        """
        获取插件信息
        
        Args:
            plugin_name: 插件名称
            plugin_type: 插件类型，如不指定则在所有类型中查找
            
        Returns:
            Optional[Dict[str, Any]]: 插件信息或None
        """
        # 从注册表获取插件类信息
        plugin_class = self.registry.get_plugin_class(plugin_name, plugin_type)
        if not plugin_class:
            return None
        
        # 从实例获取运行时信息
        plugin_instance = self.get_plugin_instance(plugin_name, plugin_type)
        
        # 获取基础信息
        info = {
            'name': plugin_name,
            'type': plugin_type or self.registry._get_plugin_type(plugin_class),
            'enabled': False
        }
        
        # 如果有实例，获取更多信息
        if plugin_instance:
            info.update({
                'version': plugin_instance.get_version(),
                'author': plugin_instance.get_author(),
                'description': plugin_instance.get_description(),
                'enabled': plugin_instance.is_enabled(),
                'config': plugin_instance.get_config()
            })
        
        return info
    
    def get_all_plugin_info(self) -> List[Dict[str, Any]]:
        """
        获取所有插件信息
        
        Returns:
            List[Dict[str, Any]]: 插件信息列表
        """
        plugin_info_list = []
        
        # 遍历所有插件实例
        for type_key in self.plugin_instances:
            for plugin_name in self.plugin_instances[type_key]:
                plugin_info = self.get_plugin_info(plugin_name, type_key)
                if plugin_info:
                    plugin_info_list.append(plugin_info)
        
        return plugin_info_list
    
    def get_available_datasource_plugins(self) -> Dict[str, PluginBase]:
        """
        获取可用的数据源插件
        
        Returns:
            Dict[str, PluginBase]: 数据源插件实例字典
        """
        # 先获取所有已实例化的插件
        available_plugins = {name: plugin for name, plugin in self.plugin_instances['datasource'].items() if plugin.is_enabled()}
        
        # 再获取所有已注册但未实例化的插件
        for plugin_meta in self.registry.get_all_plugin_metadata('datasource'):
            plugin_name = plugin_meta['name']
            if plugin_name not in available_plugins:
                # 懒加载插件
                plugin_instance = self.get_plugin_instance(plugin_name, 'datasource', lazy_load=True)
                if plugin_instance and plugin_instance.is_enabled():
                    available_plugins[plugin_name] = plugin_instance
        
        return available_plugins
    
    def get_available_indicator_plugins(self) -> Dict[str, PluginBase]:
        """
        获取可用的技术指标插件
        
        Returns:
            Dict[str, PluginBase]: 技术指标插件实例字典
        """
        # 先获取所有已实例化的插件
        available_plugins = {name: plugin for name, plugin in self.plugin_instances['indicator'].items() if plugin.is_enabled()}
        
        # 再获取所有已注册但未实例化的插件
        for plugin_meta in self.registry.get_all_plugin_metadata('indicator'):
            plugin_name = plugin_meta['name']
            if plugin_name not in available_plugins:
                # 懒加载插件
                plugin_instance = self.get_plugin_instance(plugin_name, 'indicator', lazy_load=True)
                if plugin_instance and plugin_instance.is_enabled():
                    available_plugins[plugin_name] = plugin_instance
        
        return available_plugins
    
    def get_available_strategy_plugins(self) -> Dict[str, PluginBase]:
        """
        获取可用的策略插件
        
        Returns:
            Dict[str, PluginBase]: 策略插件实例字典
        """
        # 先获取所有已实例化的插件
        available_plugins = {name: plugin for name, plugin in self.plugin_instances['strategy'].items() if plugin.is_enabled()}
        
        # 再获取所有已注册但未实例化的插件
        for plugin_meta in self.registry.get_all_plugin_metadata('strategy'):
            plugin_name = plugin_meta['name']
            if plugin_name not in available_plugins:
                # 懒加载插件
                plugin_instance = self.get_plugin_instance(plugin_name, 'strategy', lazy_load=True)
                if plugin_instance and plugin_instance.is_enabled():
                    available_plugins[plugin_name] = plugin_instance
        
        return available_plugins
    
    def get_available_visualization_plugins(self) -> Dict[str, PluginBase]:
        """
        获取可用的可视化插件
        
        Returns:
            Dict[str, PluginBase]: 可视化插件实例字典
        """
        # 先获取所有已实例化的插件
        available_plugins = {name: plugin for name, plugin in self.plugin_instances['visualization'].items() if plugin.is_enabled()}
        
        # 再获取所有已注册但未实例化的插件
        for plugin_meta in self.registry.get_all_plugin_metadata('visualization'):
            plugin_name = plugin_meta['name']
            if plugin_name not in available_plugins:
                # 懒加载插件
                plugin_instance = self.get_plugin_instance(plugin_name, 'visualization', lazy_load=True)
                if plugin_instance and plugin_instance.is_enabled():
                    available_plugins[plugin_name] = plugin_instance
        
        return available_plugins
    
    def call_plugin_method(self, plugin_name: str, method_name: str, plugin_type: str = None, *args, **kwargs) -> Any:
        """
        直接调用插件方法
        
        Args:
            plugin_name: 插件名称
            method_name: 方法名称
            plugin_type: 插件类型，如不指定则在所有类型中查找
            *args: 位置参数
            **kwargs: 关键字参数
            
        Returns:
            Any: 方法返回值
            
        Raises:
            Exception: 调用错误
        """
        plugin = self.get_plugin_instance(plugin_name, plugin_type)
        if not plugin:
            raise Exception(f"插件{plugin_name}不存在或已禁用")
        
        if not hasattr(plugin, method_name):
            raise Exception(f"插件{plugin_name}没有方法: {method_name}")
        
        method = getattr(plugin, method_name)
        if not callable(method):
            raise Exception(f"插件{plugin_name}的{method_name}不是可调用方法")
        
        try:
            return method(*args, **kwargs)
        except Exception as e:
            raise Exception(f"调用插件{plugin_name}的{method_name}方法失败: {str(e)}") from e