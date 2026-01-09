#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
插件配置管理器
负责插件配置的加载、验证、更新和管理
"""

import os
import json
from typing import Dict, Any, Optional, Type, TypeVar
from pydantic_settings import BaseSettings
from pydantic import ValidationError
from loguru import logger
from src.utils.config import Config, PluginSettings

T = TypeVar('T', bound=BaseSettings)


class PluginConfigManager:
    """
    插件配置管理器，负责插件配置的加载、验证和更新
    """
    
    def __init__(self, config: Config):
        """
        初始化插件配置管理器
        
        Args:
            config: 系统配置实例
        """
        self.config = config
        self.plugin_configs: Dict[str, Dict[str, Any]] = {}
        self.plugin_config_models: Dict[str, Type[BaseSettings]] = {}
        
        # 加载默认配置
        self._load_default_configs()
    
    def _load_default_configs(self):
        """
        加载默认配置
        """
        # 从系统配置中加载插件配置
        if hasattr(self.config, 'plugins') and hasattr(self.config.plugins, 'plugins'):
            for plugin_name, plugin_config in self.config.plugins.plugins.items():
                self.plugin_configs[plugin_name] = plugin_config.config
    
    def register_plugin_config(self, plugin_name: str, config_model: Type[BaseSettings]) -> None:
        """
        注册插件配置模型
        
        Args:
            plugin_name: 插件名称
            config_model: 插件配置模型类
        """
        self.plugin_config_models[plugin_name] = config_model
        logger.info(f"插件配置模型已注册: {plugin_name}")
    
    def get_plugin_config(self, plugin_name: str) -> Dict[str, Any]:
        """
        获取插件配置
        
        Args:
            plugin_name: 插件名称
            
        Returns:
            Dict[str, Any]: 插件配置
        """
        return self.plugin_configs.get(plugin_name, {})
    
    def get_plugin_config_model(self, plugin_name: str) -> Optional[Type[BaseSettings]]:
        """
        获取插件配置模型
        
        Args:
            plugin_name: 插件名称
            
        Returns:
            Optional[Type[BaseSettings]]: 插件配置模型类，不存在则返回None
        """
        return self.plugin_config_models.get(plugin_name)
    
    def create_plugin_config(self, plugin_name: str, default_config: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        创建插件配置，合并默认配置、系统配置和环境变量
        
        Args:
            plugin_name: 插件名称
            default_config: 插件默认配置
            
        Returns:
            Dict[str, Any]: 合并后的插件配置
        """
        # 1. 使用默认配置
        config = default_config.copy() if default_config else {}
        
        # 2. 合并系统配置
        system_plugin_config = self.get_plugin_config(plugin_name)
        if system_plugin_config:
            config.update(system_plugin_config)
        
        # 3. 合并环境变量配置
        env_config = self._load_env_config(plugin_name)
        if env_config:
            config.update(env_config)
        
        # 4. 验证配置
        if plugin_name in self.plugin_config_models:
            try:
                config_model = self.plugin_config_models[plugin_name]
                validated_config = config_model(**config)
                return validated_config.model_dump()
            except ValidationError as e:
                logger.error(f"插件配置验证失败: {plugin_name}, 错误: {e}")
                # 如果验证失败，返回原始配置
        
        return config
    
    def _load_env_config(self, plugin_name: str) -> Dict[str, Any]:
        """
        从环境变量加载插件配置
        
        Args:
            plugin_name: 插件名称
            
        Returns:
            Dict[str, Any]: 从环境变量加载的配置
        """
        env_config = {}
        plugin_env_prefix = f"PLUGIN_{plugin_name.upper()}__"
        
        for env_key, env_value in os.environ.items():
            if env_key.startswith(plugin_env_prefix):
                # 转换环境变量名为配置键
                config_key = env_key[len(plugin_env_prefix):].lower()
                env_config[config_key] = env_value
        
        return env_config
    
    def update_plugin_config(self, plugin_name: str, new_config: Dict[str, Any]) -> bool:
        """
        更新插件配置
        
        Args:
            plugin_name: 插件名称
            new_config: 新的插件配置
            
        Returns:
            bool: 更新是否成功
        """
        try:
            # 合并配置
            current_config = self.get_plugin_config(plugin_name)
            current_config.update(new_config)
            
            # 验证配置
            if plugin_name in self.plugin_config_models:
                config_model = self.plugin_config_models[plugin_name]
                validated_config = config_model(**current_config)
                self.plugin_configs[plugin_name] = validated_config.model_dump()
            else:
                self.plugin_configs[plugin_name] = current_config
            
            logger.info(f"插件配置已更新: {plugin_name}")
            return True
        except Exception as e:
            logger.error(f"更新插件配置失败: {plugin_name}, 错误: {e}")
            return False
    
    def validate_plugin_config(self, plugin_name: str, config: Dict[str, Any]) -> bool:
        """
        验证插件配置
        
        Args:
            plugin_name: 插件名称
            config: 插件配置
            
        Returns:
            bool: 配置是否有效
        """
        if plugin_name in self.plugin_config_models:
            try:
                config_model = self.plugin_config_models[plugin_name]
                config_model(**config)
                return True
            except ValidationError as e:
                logger.error(f"插件配置验证失败: {plugin_name}, 错误: {e}")
                return False
        
        # 如果没有配置模型，默认验证通过
        return True
    
    def get_all_plugin_configs(self) -> Dict[str, Dict[str, Any]]:
        """
        获取所有插件配置
        
        Returns:
            Dict[str, Dict[str, Any]]: 所有插件配置
        """
        return self.plugin_configs.copy()
    
    def save_plugin_configs(self, file_path: str = "plugins_config.json") -> bool:
        """
        保存插件配置到文件
        
        Args:
            file_path: 配置文件路径
            
        Returns:
            bool: 保存是否成功
        """
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(self.plugin_configs, f, ensure_ascii=False, indent=2)
            logger.info(f"插件配置已保存到文件: {file_path}")
            return True
        except Exception as e:
            logger.error(f"保存插件配置失败: {e}")
            return False
    
    def load_plugin_configs(self, file_path: str = "plugins_config.json") -> bool:
        """
        从文件加载插件配置
        
        Args:
            file_path: 配置文件路径
            
        Returns:
            bool: 加载是否成功
        """
        try:
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    loaded_configs = json.load(f)
                    
                # 合并加载的配置
                for plugin_name, config in loaded_configs.items():
                    self.update_plugin_config(plugin_name, config)
                
                logger.info(f"插件配置已从文件加载: {file_path}")
                return True
        except Exception as e:
            logger.error(f"加载插件配置失败: {e}")
        
        return False
    
    def get_config_documentation(self, plugin_name: str) -> Optional[str]:
        """
        获取插件配置文档
        
        Args:
            plugin_name: 插件名称
            
        Returns:
            Optional[str]: 插件配置文档，不存在则返回None
        """
        if plugin_name not in self.plugin_config_models:
            return None
        
        config_model = self.plugin_config_models[plugin_name]
        doc = f"# {plugin_name} 配置文档\n\n"
        
        for field_name, field in config_model.model_fields.items():
            field_info = field.field_info
            doc += f"## {field_name}\n"
            doc += f"- **类型**: {field.annotation.__name__}\n"
            doc += f"- **默认值**: {field.default}\n"
            if field_info.description:
                doc += f"- **描述**: {field_info.description}\n"
            doc += "\n"
        
        return doc


# 创建全局插件配置管理器实例
global_plugin_config_manager = None


def get_plugin_config_manager(config: Config = None) -> PluginConfigManager:
    """
    获取或创建全局插件配置管理器实例
    
    Args:
        config: 系统配置实例，仅在首次调用时需要
        
    Returns:
        PluginConfigManager: 插件配置管理器实例
    """
    global global_plugin_config_manager
    
    if global_plugin_config_manager is None:
        if config is None:
            from src.utils.config import config as default_config
            config = default_config
        global_plugin_config_manager = PluginConfigManager(config)
    
    return global_plugin_config_manager
