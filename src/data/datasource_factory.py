#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据源工厂，用于管理和切换不同的数据源
"""

from typing import Any, Dict, List, Optional, Type, Union
from loguru import logger
from src.api.data_api import IDataProvider


class DataSourceConfig:
    """数据源配置"""
    
    def __init__(self, name: str, handler_class: Type, params: Dict[str, Any] = None, enabled: bool = True, priority: int = 0):
        """初始化数据源配置
        
        Args:
            name: 数据源名称
            handler_class: 数据源处理器类
            params: 初始化参数
            enabled: 是否启用
            priority: 优先级，数值越大，优先级越高
        """
        self.name = name
        self.handler_class = handler_class
        self.params = params or {}
        self.enabled = enabled
        self.priority = priority


class DataSourceFactory:
    """数据源工厂，用于管理和切换不同的数据源"""
    
    def __init__(self):
        """初始化数据源工厂"""
        self._datasources: Dict[str, Any] = {}
        self._configs: Dict[str, DataSourceConfig] = {}
        self._primary_datasource: str = None
        self._initialized = False
    
    def register_datasource(self, config: DataSourceConfig):
        """注册数据源
        
        Args:
            config: 数据源配置
        """
        if config.name in self._configs:
            logger.warning(f"数据源{config.name}已存在，将被覆盖")
        
        self._configs[config.name] = config
    
    def initialize_datasources(self, **kwargs):
        """初始化所有注册的数据源
        
        Args:
            **kwargs: 传递给数据源处理器的初始化参数
        """
        if self._initialized:
            return
        
        for name, config in self._configs.items():
            if config.enabled:
                try:
                    # 合并初始化参数
                    merged_params = config.params.copy()
                    merged_params.update(kwargs)
                    
                    # 创建数据源处理器实例
                    handler = config.handler_class(**merged_params)
                    self._datasources[name] = handler
                    logger.info(f"数据源{name}初始化成功")
                except Exception as e:
                    logger.exception(f"数据源{name}初始化失败: {e}")
        
        # 设置默认主数据源（优先级最高的）
        if not self._primary_datasource:
            self._primary_datasource = self._get_highest_priority_datasource()
        
        self._initialized = True
    
    def _get_highest_priority_datasource(self) -> str:
        """获取优先级最高的数据源
        
        Returns:
            str: 数据源名称
        """
        if not self._configs:
            return None
        
        # 按优先级排序，返回优先级最高的数据源
        sorted_datasources = sorted(
            self._configs.items(),
            key=lambda x: x[1].priority,
            reverse=True
        )
        
        # 返回第一个启用的数据源
        for name, config in sorted_datasources:
            if config.enabled and name in self._datasources:
                return name
        
        return None
    
    def get_datasource(self, name: Optional[str] = None) -> Optional[Any]:
        """获取数据源处理器
        
        Args:
            name: 数据源名称，None表示获取主数据源
        
        Returns:
            Optional[Any]: 数据源处理器实例
        """
        if not self._initialized:
            self.initialize_datasources()
        
        if name:
            return self._datasources.get(name)
        
        # 如果没有指定名称，返回主数据源
        if self._primary_datasource:
            return self._datasources.get(self._primary_datasource)
        
        # 如果没有主数据源，返回第一个可用的数据源
        for datasource in self._datasources.values():
            return datasource
        
        return None
    
    def set_primary_datasource(self, name: str):
        """设置主数据源
        
        Args:
            name: 数据源名称
        
        Raises:
            ValueError: 如果数据源不存在或未启用
        """
        if name not in self._configs:
            raise ValueError(f"数据源{name}不存在")
        
        if name not in self._datasources:
            raise ValueError(f"数据源{name}未初始化")
        
        self._primary_datasource = name
        logger.info(f"已将{name}设为主数据源")
    
    def get_primary_datasource_name(self) -> Optional[str]:
        """获取主数据源名称
        
        Returns:
            Optional[str]: 主数据源名称
        """
        return self._primary_datasource
    
    def get_all_datasources(self) -> Dict[str, Any]:
        """获取所有初始化的数据源
        
        Returns:
            Dict[str, Any]: 数据源映射
        """
        return self._datasources.copy()
    
    def get_datasource_names(self) -> List[str]:
        """获取所有数据源名称
        
        Returns:
            List[str]: 数据源名称列表
        """
        return list(self._configs.keys())
    
    def get_enabled_datasource_names(self) -> List[str]:
        """获取所有启用的数据源名称
        
        Returns:
            List[str]: 启用的数据源名称列表
        """
        return [name for name, config in self._configs.items() if config.enabled]
    
    def health_check(self, name: Optional[str] = None) -> bool:
        """检查数据源健康状态
        
        Args:
            name: 数据源名称，None表示检查所有数据源
        
        Returns:
            bool: 健康检查结果
        """
        if name:
            datasource = self._datasources.get(name)
            if not datasource:
                return False
            
            try:
                # 调用数据源的健康检查方法
                if hasattr(datasource, 'health_check'):
                    return datasource.health_check()
                
                # 如果没有健康检查方法，尝试获取一些数据来验证
                if hasattr(datasource, 'get_stock_basic'):
                    datasource.get_stock_basic()
                    return True
                
                return False
            except Exception:
                return False
        else:
            # 检查所有数据源
            all_healthy = True
            for name, datasource in self._datasources.items():
                healthy = self.health_check(name)
                if not healthy:
                    logger.warning(f"数据源{name}健康检查失败")
                    all_healthy = False
                else:
                    logger.info(f"数据源{name}健康检查通过")
            
            return all_healthy
    
    def switch_datasource(self, name: str) -> bool:
        """切换主数据源
        
        Args:
            name: 新的主数据源名称
        
        Returns:
            bool: 切换是否成功
        """
        if name not in self._datasources:
            logger.error(f"数据源{name}不存在或未初始化")
            return False
        
        # 检查新数据源的健康状态
        if not self.health_check(name):
            logger.error(f"数据源{name}健康检查失败，无法切换")
            return False
        
        # 切换主数据源
        old_primary = self._primary_datasource
        self._primary_datasource = name
        logger.info(f"主数据源已从{old_primary}切换到{name}")
        return True
    
    def auto_switch_datasource(self) -> bool:
        """自动切换到健康的数据源
        
        Returns:
            bool: 切换是否成功
        """
        # 按优先级检查数据源，返回第一个健康的数据源
        for name in self.get_enabled_datasource_names():
            if self.health_check(name):
                return self.switch_datasource(name)
        
        logger.error("没有健康的数据源可用")
        return False
    
    def get_datasource_by_type(self, data_type: str) -> Optional[Any]:
        """根据数据类型获取合适的数据源
        
        Args:
            data_type: 数据类型，如'stock', 'index', 'macro'等
        
        Returns:
            Optional[Any]: 合适的数据源处理器
        """
        for name, datasource in self._datasources.items():
            try:
                if hasattr(datasource, 'supports_data_type') and datasource.supports_data_type(data_type):
                    return datasource
            except Exception:
                continue
        
        # 如果没有找到支持特定数据类型的数据源，返回主数据源
        return self.get_datasource()
    
    def close_datasources(self):
        """关闭所有数据源
        """
        for name, datasource in self._datasources.items():
            try:
                if hasattr(datasource, 'close'):
                    datasource.close()
                    logger.info(f"数据源{name}已关闭")
            except Exception as e:
                logger.exception(f"关闭数据源{name}时发生错误: {e}")
        
        self._datasources.clear()
        self._initialized = False


# 创建全局数据源工厂实例
datasource_factory = DataSourceFactory()
