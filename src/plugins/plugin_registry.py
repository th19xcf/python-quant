#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
插件注册表，用于管理所有已注册的插件
"""

from typing import Dict, List, Type, Any
from src.plugin.plugin_base import PluginBase, DataSourcePlugin, IndicatorPlugin, StrategyPlugin, VisualizationPlugin


class PluginRegistry:
    """
    插件注册表，负责插件的注册和查询
    """
    
    def __init__(self):
        # 插件注册表，按插件类型分类
        self.registry: Dict[str, Dict[str, Type[PluginBase]]] = {
            'datasource': {},
            'indicator': {},
            'strategy': {},
            'visualization': {},
            'other': {}
        }
    
    def register_plugin(self, plugin_class: Type[PluginBase]) -> bool:
        """
        注册插件类
        
        Args:
            plugin_class: 插件类
            
        Returns:
            bool: 注册是否成功
        """
        try:
            # 创建插件实例以获取插件信息
            plugin_instance = plugin_class()
            plugin_name = plugin_instance.get_name()
            
            # 确定插件类型
            plugin_type = self._get_plugin_type(plugin_class)
            
            # 检查插件名称是否已存在
            if plugin_name in self.registry[plugin_type]:
                return False
            
            # 注册插件
            self.registry[plugin_type][plugin_name] = plugin_class
            return True
        except Exception as e:
            return False
    
    def unregister_plugin(self, plugin_name: str, plugin_type: str = None) -> bool:
        """
        注销插件
        
        Args:
            plugin_name: 插件名称
            plugin_type: 插件类型，如不指定则在所有类型中查找
            
        Returns:
            bool: 注销是否成功
        """
        if plugin_type:
            if plugin_type in self.registry and plugin_name in self.registry[plugin_type]:
                del self.registry[plugin_type][plugin_name]
                return True
        else:
            # 在所有类型中查找并注销
            for type_key in self.registry:
                if plugin_name in self.registry[type_key]:
                    del self.registry[type_key][plugin_name]
                    return True
        return False
    
    def get_plugin_class(self, plugin_name: str, plugin_type: str = None) -> Type[PluginBase] or None:
        """
        获取插件类
        
        Args:
            plugin_name: 插件名称
            plugin_type: 插件类型，如不指定则在所有类型中查找
            
        Returns:
            Type[PluginBase] or None: 插件类或None
        """
        if plugin_type:
            return self.registry.get(plugin_type, {}).get(plugin_name)
        else:
            # 在所有类型中查找
            for type_key in self.registry:
                if plugin_name in self.registry[type_key]:
                    return self.registry[type_key][plugin_name]
        return None
    
    def get_all_plugins(self, plugin_type: str = None) -> List[Dict[str, Any]]:
        """
        获取所有插件信息
        
        Args:
            plugin_type: 插件类型，如不指定则返回所有类型
            
        Returns:
            List[Dict[str, Any]]: 插件信息列表
        """
        plugins_info = []
        
        if plugin_type:
            if plugin_type in self.registry:
                for plugin_name, plugin_class in self.registry[plugin_type].items():
                    plugin_info = self._get_plugin_info(plugin_class)
                    if plugin_info:
                        plugins_info.append(plugin_info)
        else:
            # 返回所有类型的插件
            for type_key in self.registry:
                for plugin_name, plugin_class in self.registry[type_key].items():
                    plugin_info = self._get_plugin_info(plugin_class)
                    if plugin_info:
                        plugins_info.append(plugin_info)
        
        return plugins_info
    
    def get_plugins_by_type(self, plugin_type: str) -> List[Dict[str, Any]]:
        """
        按类型获取插件信息
        
        Args:
            plugin_type: 插件类型
            
        Returns:
            List[Dict[str, Any]]: 插件信息列表
        """
        return self.get_all_plugins(plugin_type)
    
    def is_plugin_registered(self, plugin_name: str, plugin_type: str = None) -> bool:
        """
        检查插件是否已注册
        
        Args:
            plugin_name: 插件名称
            plugin_type: 插件类型，如不指定则在所有类型中查找
            
        Returns:
            bool: 是否已注册
        """
        return self.get_plugin_class(plugin_name, plugin_type) is not None
    
    def _get_plugin_type(self, plugin_class: Type[PluginBase]) -> str:
        """
        获取插件类型
        
        Args:
            plugin_class: 插件类
            
        Returns:
            str: 插件类型
        """
        if issubclass(plugin_class, DataSourcePlugin):
            return 'datasource'
        elif issubclass(plugin_class, IndicatorPlugin):
            return 'indicator'
        elif issubclass(plugin_class, StrategyPlugin):
            return 'strategy'
        elif issubclass(plugin_class, VisualizationPlugin):
            return 'visualization'
        else:
            return 'other'
    
    def _get_plugin_info(self, plugin_class: Type[PluginBase]) -> Dict[str, Any] or None:
        """
        获取插件详细信息
        
        Args:
            plugin_class: 插件类
            
        Returns:
            Dict[str, Any] or None: 插件信息或None
        """
        try:
            plugin_instance = plugin_class()
            return {
                'name': plugin_instance.get_name(),
                'version': plugin_instance.get_version(),
                'author': plugin_instance.get_author(),
                'description': plugin_instance.get_description(),
                'class': plugin_class,
                'type': self._get_plugin_type(plugin_class)
            }
        except Exception as e:
            return None
    
    def clear(self) -> None:
        """
        清空注册表
        """
        for type_key in self.registry:
            self.registry[type_key].clear()
    
    def get_plugin_count(self, plugin_type: str = None) -> int:
        """
        获取插件数量
        
        Args:
            plugin_type: 插件类型，如不指定则返回所有类型
            
        Returns:
            int: 插件数量
        """
        if plugin_type:
            return len(self.registry.get(plugin_type, {}))
        else:
            total = 0
            for type_key in self.registry:
                total += len(self.registry[type_key])
            return total


# 创建全局插件注册表实例
global_registry = PluginRegistry()