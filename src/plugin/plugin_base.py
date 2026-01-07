#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
插件基类定义
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional


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
        self.config.update(config)


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