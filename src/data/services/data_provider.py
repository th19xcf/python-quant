#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据提供服务

负责从多个数据源获取数据，处理数据源优先级和故障转移
"""

from typing import List, Dict, Optional
import polars as pl
from loguru import logger

from src.data.managers.data_fetcher import DataFetcher
from src.utils.memory_optimizer import MemoryOptimizer


class DataProvider:
    """
    数据提供服务，负责从多个数据源获取数据
    """
    
    def __init__(self, config, db_manager=None):
        """
        初始化数据提供服务
        
        Args:
            config: 配置对象
            db_manager: 数据库管理器实例
        """
        self.config = config
        self.db_manager = db_manager
        self.data_fetcher = DataFetcher(config, db_manager)
    
    def register_source(self, source):
        """
        注册数据源
        
        Args:
            source: 数据源实例
        """
        self.data_fetcher.register_source(source)
    
    def get_stock_data(self, stock_code: str, start_date: str, end_date: str, 
                      frequency: str = '1d', adjustment_type: str = 'qfq') -> pl.DataFrame:
        """
        获取股票历史数据
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 数据频率，默认：1d（日线），支持1d/1w/1m（日/周/月线）
            adjustment_type: 复权类型，qfq=前复权, hfq=后复权, none=不复权
        
        Returns:
            pl.DataFrame: 股票历史数据
        """
        return self.data_fetcher.get_stock_data(
            stock_code, start_date, end_date, frequency, adjustment_type
        )
    
    def get_index_data(self, index_code: str, start_date: str, end_date: str, 
                      frequency: str = '1d') -> pl.DataFrame:
        """
        获取指数历史数据
        
        Args:
            index_code: 指数代码
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 数据频率，默认：1d（日线）
        
        Returns:
            pl.DataFrame: 指数历史数据
        """
        return self.data_fetcher.get_index_data(
            index_code, start_date, end_date, frequency
        )
    
    def get_available_sources(self) -> List:
        """
        获取所有可用的数据源
        
        Returns:
            List: 可用数据源列表
        """
        return self.data_fetcher.get_available_sources()
