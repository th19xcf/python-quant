#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据更新服务

负责更新各种数据源的数据，包括股票、基金、指数等
"""

from typing import List, Optional
from loguru import logger

from src.data.managers.data_updater import DataUpdater
from src.data.data_cache import global_data_cache
from src.utils.event_bus import publish


class DataUpdaterService:
    """
    数据更新服务，负责更新各种数据源的数据
    """
    
    def __init__(self, config, db_manager=None):
        """
        初始化数据更新服务
        
        Args:
            config: 配置对象
            db_manager: 数据库管理器实例
        """
        self.config = config
        self.db_manager = db_manager
        self.data_updater = DataUpdater(config, db_manager)
    
    def register_source(self, source):
        """
        注册数据源
        
        Args:
            source: 数据源实例
        """
        self.data_updater.register_source(source)
    
    def update_stock_basic(self):
        """
        更新股票基本信息
        """
        success = self.data_updater.update_stock_basic()
        if success:
            # 使股票基本信息缓存失效
            global_data_cache.invalidate_by_type('stock_basic')
            self._publish_data_updated_event('stock', 'basic')
        return success
    
    def update_stock_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新股票日线数据
        
        Args:
            ts_codes: 股票代码列表，None表示更新所有股票
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
        """
        success = self.data_updater.update_stock_daily(ts_codes, start_date, end_date)
        
        # 数据更新后，使相关缓存失效
        if success:
            if ts_codes:
                for ts_code in ts_codes:
                    global_data_cache.invalidate('stock', ts_code)
            else:
                # 如果更新所有股票，使所有股票缓存失效
                global_data_cache.invalidate_by_type('stock')
            self._publish_data_updated_event('stock', 'daily')
        return success
    
    def update_fund_basic(self):
        """
        更新基金基本信息
        """
        success = self.data_updater.update_fund_basic()
        if success:
            # 使基金基本信息缓存失效
            global_data_cache.invalidate_by_type('fund_basic')
            self._publish_data_updated_event('fund', 'basic')
        return success
    
    def update_fund_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新基金日线数据
        
        Args:
            ts_codes: 基金代码列表，None表示更新所有基金
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
        """
        success = self.data_updater.update_fund_daily(ts_codes, start_date, end_date)
        
        # 数据更新后，使相关缓存失效
        if success:
            if ts_codes:
                for ts_code in ts_codes:
                    global_data_cache.invalidate('fund', ts_code)
            else:
                # 如果更新所有基金，使所有基金缓存失效
                global_data_cache.invalidate_by_type('fund')
            self._publish_data_updated_event('fund', 'daily')
        return success
    
    def update_closed_fund_basic(self):
        """
        更新封闭式基金基本信息
        """
        success = self.data_updater.update_closed_fund_basic()
        if success:
            # 使封闭式基金基本信息缓存失效
            global_data_cache.invalidate_by_type('closed_fund_basic')
            self._publish_data_updated_event('closed_fund', 'basic')
        return success
    
    def update_closed_fund_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新封闭式基金日线数据
        
        Args:
            ts_codes: 封闭式基金代码列表，None表示更新所有封闭式基金
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
        """
        success = self.data_updater.update_closed_fund_daily(ts_codes, start_date, end_date)
        
        # 数据更新后，使相关缓存失效
        if success:
            if ts_codes:
                for ts_code in ts_codes:
                    global_data_cache.invalidate('closed_fund', ts_code)
            else:
                # 如果更新所有封闭式基金，使所有封闭式基金缓存失效
                global_data_cache.invalidate_by_type('closed_fund')
            self._publish_data_updated_event('closed_fund', 'daily')
        return success
    
    def update_index_basic(self):
        """
        更新指数基本信息
        """
        success = self.data_updater.update_index_basic()
        if success:
            # 使指数基本信息缓存失效
            global_data_cache.invalidate_by_type('index_basic')
            self._publish_data_updated_event('index', 'basic')
        return success
    
    def update_index_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新指数日线数据
        
        Args:
            ts_codes: 指数代码列表，None表示更新所有指数
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
        """
        success = self.data_updater.update_index_daily(ts_codes, start_date, end_date)
        
        # 数据更新后，使相关缓存失效
        if success:
            if ts_codes:
                for ts_code in ts_codes:
                    global_data_cache.invalidate('index', ts_code)
            else:
                # 如果更新所有指数，使所有指数缓存失效
                global_data_cache.invalidate_by_type('index')
            self._publish_data_updated_event('index', 'daily')
        return success
    
    def update_macro_data(self, indicators: List[str] = None):
        """
        更新宏观经济数据
        
        Args:
            indicators: 宏观经济指标列表，None表示更新所有指标
        """
        success = self.data_updater.update_macro_data(indicators)
        if success:
            self._publish_data_updated_event('macro', 'data')
        return success
    
    def update_news_data(self, sources: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新新闻数据
        
        Args:
            sources: 新闻来源列表，None表示更新所有来源
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
        """
        success = self.data_updater.update_news_data(sources, start_date, end_date)
        if success:
            self._publish_data_updated_event('news', 'data')
        return success
    
    def update_stock_dividend(self, ts_codes: List[str] = None):
        """
        更新股票分红配股数据

        Args:
            ts_codes: 股票代码列表，None表示更新所有股票
        """
        success = self.data_updater.update_stock_dividend(ts_codes)
        if success:
            # 使分红配股数据缓存失效
            if ts_codes:
                for ts_code in ts_codes:
                    global_data_cache.invalidate('stock_dividend', ts_code)
            else:
                global_data_cache.invalidate_by_type('stock_dividend')
            self._publish_data_updated_event('stock', 'dividend')
        return success
    
    def _publish_data_updated_event(self, data_type, data_subtype, status="success", message=""):
        """
        发布数据更新事件
        
        Args:
            data_type: 数据类型，如'stock', 'index', 'macro', 'news'
            data_subtype: 数据子类型，如'basic', 'daily', 'dividend'
            status: 更新状态，success或error
            message: 附加消息
        """
        publish(
            'data_updated' if status == 'success' else 'data_error',
            data_type=data_type,
            data_subtype=data_subtype,
            message=message
        )
