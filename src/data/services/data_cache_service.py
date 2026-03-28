#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据缓存服务

负责管理数据缓存，包括缓存的设置、获取和失效
"""

from typing import Dict, Any, Optional
import polars as pl
from loguru import logger

from src.data.data_cache import global_data_cache


class DataCacheService:
    """
    数据缓存服务，负责管理数据缓存
    """
    
    def __init__(self):
        """
        初始化数据缓存服务
        """
        self.cache = global_data_cache
    
    def get(self, data_type: str, code: str, start_date: str, end_date: str, 
            **kwargs) -> Optional[pl.DataFrame]:
        """
        从缓存获取数据
        
        Args:
            data_type: 数据类型，如'stock', 'index'
            code: 代码
            start_date: 开始日期
            end_date: 结束日期
            **kwargs: 其他参数，如frequency, adjustment_type
        
        Returns:
            Optional[pl.DataFrame]: 缓存的数据，如果没有则返回None
        """
        return self.cache.get(data_type, code, start_date, end_date, **kwargs)
    
    def set(self, data: pl.DataFrame, data_type: str, code: str, start_date: str, end_date: str, 
            **kwargs) -> bool:
        """
        将数据存入缓存
        
        Args:
            data: 要缓存的数据
            data_type: 数据类型，如'stock', 'index'
            code: 代码
            start_date: 开始日期
            end_date: 结束日期
            **kwargs: 其他参数，如frequency, adjustment_type
        
        Returns:
            bool: 是否成功存入缓存
        """
        return self.cache.set(data, data_type, code, start_date, end_date, **kwargs)
    
    def invalidate(self, data_type: str, code: str, **kwargs):
        """
        使指定数据的缓存失效
        
        Args:
            data_type: 数据类型，如'stock', 'index'
            code: 代码
            **kwargs: 其他参数，如frequency, adjustment_type
        """
        self.cache.invalidate(data_type, code, **kwargs)
    
    def invalidate_by_type(self, data_type: str):
        """
        使指定类型的所有数据缓存失效
        
        Args:
            data_type: 数据类型，如'stock', 'index'
        """
        self.cache.invalidate_by_type(data_type)
    
    def clear(self):
        """
        清空所有缓存
        """
        self.cache.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取缓存统计信息
        
        Returns:
            Dict[str, Any]: 缓存统计信息
        """
        return self.cache.get_stats()
