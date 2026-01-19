#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
指标注册中心模块，提供统一的指标注册和管理机制
"""

from typing import Dict, List, Callable, Any, Optional
import polars as pl
from loguru import logger


class IndicatorConfig:
    """
    指标配置类，用于存储指标的元数据和计算参数
    """
    def __init__(self, 
                 name: str, 
                 calculate_func: Callable, 
                 dependencies: List[str] = None, 
                 params: Dict[str, Any] = None,
                 description: str = "",
                 category: str = "技术指标"):
        """
        初始化指标配置
        
        Args:
            name: 指标名称
            calculate_func: 指标计算函数
            dependencies: 依赖的其他指标列表
            params: 指标计算参数
            description: 指标描述
            category: 指标分类
        """
        self.name = name
        self.calculate_func = calculate_func
        self.dependencies = dependencies or []
        self.params = params or {}
        self.description = description
        self.category = category
        

class IndicatorRegistry:
    """
    指标注册中心，用于统一管理和注册所有技术指标
    实现指标的注册、查询、依赖管理和批量计算
    """
    
    def __init__(self):
        """
        初始化指标注册中心
        """
        # 存储所有注册的指标配置
        self._indicators: Dict[str, IndicatorConfig] = {}
        # 指标计算顺序缓存（基于依赖关系的拓扑排序结果）
        self._calculation_order_cache: Dict[List[str], List[str]] = {}
    
    def register_indicator(self, config: IndicatorConfig):
        """
        注册一个指标
        
        Args:
            config: 指标配置对象
        """
        if config.name in self._indicators:
            logger.warning(f"指标{config.name}已存在，将被覆盖")
        
        self._indicators[config.name] = config
        # 清除计算顺序缓存，因为指标依赖关系可能发生变化
        self._calculation_order_cache.clear()
        logger.info(f"指标{config.name}注册成功")
    
    def unregister_indicator(self, name: str):
        """
        注销一个指标
        
        Args:
            name: 指标名称
        """
        if name in self._indicators:
            del self._indicators[name]
            # 清除计算顺序缓存
            self._calculation_order_cache.clear()
            logger.info(f"指标{name}注销成功")
        else:
            logger.warning(f"指标{name}不存在，无法注销")
    
    def get_indicator(self, name: str) -> Optional[IndicatorConfig]:
        """
        获取指定名称的指标配置
        
        Args:
            name: 指标名称
        
        Returns:
            IndicatorConfig: 指标配置对象，如果不存在则返回None
        """
        return self._indicators.get(name)
    
    def get_all_indicators(self) -> Dict[str, IndicatorConfig]:
        """
        获取所有注册的指标
        
        Returns:
            Dict[str, IndicatorConfig]: 所有指标配置的字典
        """
        return self._indicators.copy()
    
    def get_indicators_by_category(self, category: str) -> Dict[str, IndicatorConfig]:
        """
        根据分类获取指标
        
        Args:
            category: 指标分类
        
        Returns:
            Dict[str, IndicatorConfig]: 指定分类的指标配置字典
        """
        return {name: config for name, config in self._indicators.items() 
                if config.category == category}
    
    def _topological_sort(self, indicators: List[str]) -> List[str]:
        """
        对指标进行拓扑排序，确定计算顺序
        
        Args:
            indicators: 需要计算的指标列表
        
        Returns:
            List[str]: 排序后的指标计算顺序
        """
        # 检查缓存
        cache_key = tuple(sorted(indicators))
        if cache_key in self._calculation_order_cache:
            return self._calculation_order_cache[cache_key]
        
        # 构建依赖图
        graph = {}
        in_degree = {}
        
        # 收集所有相关指标（包括依赖项）
        all_related = set()
        
        def collect_dependencies(indicator_name: str):
            """递归收集指标的所有依赖项"""
            if indicator_name in all_related:
                return
            
            all_related.add(indicator_name)
            config = self.get_indicator(indicator_name)
            if config:
                for dep in config.dependencies:
                    collect_dependencies(dep)
        
        # 收集所有依赖项
        for indicator in indicators:
            collect_dependencies(indicator)
        
        # 构建图和入度表
        for indicator_name in all_related:
            config = self.get_indicator(indicator_name)
            if not config:
                continue
            
            graph[indicator_name] = config.dependencies
            in_degree[indicator_name] = in_degree.get(indicator_name, 0)
            
            for dep in config.dependencies:
                in_degree[dep] = in_degree.get(dep, 0) + 1
        
        # 执行拓扑排序
        from collections import deque
        queue = deque()
        
        # 找到所有入度为0的节点
        for node in in_degree:
            if in_degree[node] == 0:
                queue.append(node)
        
        result = []
        while queue:
            current = queue.popleft()
            result.append(current)
            
            if current in graph:
                for neighbor in graph[current]:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)
        
        # 检查是否有循环依赖
        if len(result) != len(all_related):
            logger.error(f"指标依赖关系中存在循环依赖，无法完成拓扑排序")
            # 返回原始列表，但不保证正确性
            return indicators
        
        # 确保所有请求的指标都在结果中，并且按照正确的依赖顺序
        final_order = []
        for item in result:
            if item in all_related:
                final_order.append(item)
        
        # 缓存结果
        self._calculation_order_cache[cache_key] = final_order
        return final_order
    
    def get_calculation_order(self, indicators: List[str]) -> List[str]:
        """
        获取指标的计算顺序，基于依赖关系
        
        Args:
            indicators: 需要计算的指标列表
        
        Returns:
            List[str]: 排序后的指标计算顺序
        """
        return self._topological_sort(indicators)
    
    def calculate_indicators(self, df: pl.DataFrame, indicators: List[str], **params) -> pl.DataFrame:
        """
        计算指定的指标，按照依赖关系顺序执行
        
        Args:
            df: Polars DataFrame
            indicators: 需要计算的指标列表
            **params: 指标计算参数
        
        Returns:
            pl.DataFrame: 包含计算结果的DataFrame
        """
        # 获取计算顺序
        calculation_order = self.get_calculation_order(indicators)
        
        result_df = df
        
        # 按照计算顺序执行指标计算
        for indicator_name in calculation_order:
            config = self.get_indicator(indicator_name)
            if not config:
                logger.warning(f"指标{indicator_name}未注册，跳过计算")
                continue
            
            # 合并指标特定参数和全局参数
            indicator_params = config.params.copy()
            indicator_params.update(params)
            
            # 调用指标计算函数
            try:
                result_df = config.calculate_func(result_df, **indicator_params)
                logger.debug(f"指标{indicator_name}计算完成")
            except Exception as e:
                logger.error(f"计算指标{indicator_name}失败: {str(e)}")
                raise e
        
        return result_df
    
    def get_supported_indicators(self) -> List[str]:
        """
        获取支持的指标列表
        
        Returns:
            List[str]: 支持的指标名称列表
        """
        return list(self._indicators.keys())
    
    def is_indicator_supported(self, indicator_name: str) -> bool:
        """
        检查指标是否支持
        
        Args:
            indicator_name: 指标名称
        
        Returns:
            bool: 是否支持该指标
        """
        return indicator_name in self._indicators


# 创建全局指标注册中心实例
global_indicator_registry = IndicatorRegistry()


def register_indicator(name: str, calculate_func: Callable, dependencies: List[str] = None, 
                      params: Dict[str, Any] = None, description: str = "", 
                      category: str = "技术指标"):
    """
    便捷装饰器，用于注册指标
    
    Args:
        name: 指标名称
        calculate_func: 指标计算函数
        dependencies: 依赖的其他指标列表
        params: 指标计算参数
        description: 指标描述
        category: 指标分类
    
    Returns:
        Callable: 装饰后的函数
    """
    def decorator(func):
        config = IndicatorConfig(
            name=name,
            calculate_func=func,
            dependencies=dependencies,
            params=params,
            description=description,
            category=category
        )
        global_indicator_registry.register_indicator(config)
        return func
    
    if calculate_func is not None:
        # 直接调用模式
        return decorator(calculate_func)
    
    # 装饰器模式
    return decorator
