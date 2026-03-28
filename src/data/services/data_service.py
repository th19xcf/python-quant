#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据服务

数据服务的统一入口，整合数据提供、更新和缓存服务
"""

from typing import List, Dict, Optional
import polars as pl
from loguru import logger

from src.data.services.data_provider import DataProvider
from src.data.services.data_updater import DataUpdaterService
from src.data.services.data_cache_service import DataCacheService


class DataService:
    """
    数据服务，整合数据提供、更新和缓存服务
    """
    
    def __init__(self, config, db_manager=None, plugin_manager=None):
        """
        初始化数据服务
        
        Args:
            config: 配置对象
            db_manager: 数据库管理器实例
            plugin_manager: 插件管理器实例
        """
        self.config = config
        self.db_manager = db_manager
        self.plugin_manager = plugin_manager
        
        # 初始化各个服务
        self.data_provider = DataProvider(config, db_manager)
        self.data_updater = DataUpdaterService(config, db_manager)
        self.data_cache = DataCacheService()
        
        # 插件数据源映射
        self.plugin_datasources = {}
        
        # 初始化插件数据源
        self._init_plugin_datasources()
    
    def _init_plugin_datasources(self):
        """
        初始化插件数据源
        """
        if not self.plugin_manager:
            return
        
        # 获取所有可用的数据源插件
        datasource_plugins = self.plugin_manager.get_available_datasource_plugins()
        
        for plugin_name, plugin in datasource_plugins.items():
            self.plugin_datasources[plugin_name] = plugin
            # 注册到数据获取和更新模块
            self.data_provider.register_source(plugin)
            self.data_updater.register_source(plugin)
            logger.info(f"已注册插件数据源: {plugin_name}")
    
    # 数据获取方法
    def get_stock_data(self, stock_code: str, start_date: str, end_date: str, 
                      frequency: str = '1d', adjustment_type: str = 'qfq') -> pl.DataFrame:
        """
        获取股票历史数据
        """
        return self.data_provider.get_stock_data(
            stock_code, start_date, end_date, frequency, adjustment_type
        )
    
    def get_index_data(self, index_code: str, start_date: str, end_date: str, 
                      frequency: str = '1d') -> pl.DataFrame:
        """
        获取指数历史数据
        """
        return self.data_provider.get_index_data(
            index_code, start_date, end_date, frequency
        )
    
    # 数据更新方法
    def update_stock_basic(self):
        """
        更新股票基本信息
        """
        return self.data_updater.update_stock_basic()
    
    def update_stock_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新股票日线数据
        """
        return self.data_updater.update_stock_daily(ts_codes, start_date, end_date)
    
    def update_fund_basic(self):
        """
        更新基金基本信息
        """
        return self.data_updater.update_fund_basic()
    
    def update_fund_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新基金日线数据
        """
        return self.data_updater.update_fund_daily(ts_codes, start_date, end_date)
    
    def update_closed_fund_basic(self):
        """
        更新封闭式基金基本信息
        """
        return self.data_updater.update_closed_fund_basic()
    
    def update_closed_fund_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新封闭式基金日线数据
        """
        return self.data_updater.update_closed_fund_daily(ts_codes, start_date, end_date)
    
    def update_index_basic(self):
        """
        更新指数基本信息
        """
        return self.data_updater.update_index_basic()
    
    def update_index_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新指数日线数据
        """
        return self.data_updater.update_index_daily(ts_codes, start_date, end_date)
    
    def update_macro_data(self, indicators: List[str] = None):
        """
        更新宏观经济数据
        """
        return self.data_updater.update_macro_data(indicators)
    
    def update_news_data(self, sources: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新新闻数据
        """
        return self.data_updater.update_news_data(sources, start_date, end_date)
    
    def update_stock_dividend(self, ts_codes: List[str] = None):
        """
        更新股票分红配股数据
        """
        return self.data_updater.update_stock_dividend(ts_codes)
    
    # 缓存管理方法
    def get_cache_stats(self) -> Dict[str, any]:
        """
        获取缓存统计信息
        """
        return self.data_cache.get_stats()
    
    def clear_cache(self):
        """
        清空所有缓存
        """
        self.data_cache.clear()
    
    def invalidate_cache(self, data_type: str, code: str, **kwargs):
        """
        使指定数据的缓存失效
        """
        self.data_cache.invalidate(data_type, code, **kwargs)
    
    def invalidate_cache_by_type(self, data_type: str):
        """
        使指定类型的所有数据缓存失效
        """
        self.data_cache.invalidate_by_type(data_type)
    
    # 其他方法
    def get_available_sources(self) -> List:
        """
        获取所有可用的数据源
        """
        return self.data_provider.get_available_sources()
