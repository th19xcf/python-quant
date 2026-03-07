#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据更新模块

负责更新数据到数据库，处理数据更新逻辑
"""

from typing import List, Dict, Any, Optional
from loguru import logger

from src.utils.event_bus import publish


class DataUpdater:
    """
    数据更新模块，负责更新数据到数据库
    """
    
    def __init__(self, config, db_manager=None):
        """
        初始化数据更新器
        
        Args:
            config: 配置对象
            db_manager: 数据库管理器实例
        """
        self.config = config
        self.db_manager = db_manager
        self.sources = []
    
    def register_source(self, source):
        """
        注册数据源
        
        Args:
            source: 数据源实例
        """
        if source not in self.sources:
            self.sources.append(source)
            logger.info(f"注册更新数据源: {source.name}")
    
    def update_stock_basic(self):
        """
        更新股票基本信息
        """
        for source in self.sources:
            try:
                logger.info(f"使用{source.name}更新股票基本信息")
                source.update_stock_basic()
                # 发布数据更新事件
                publish('data_updated', data_type='stock_basic', ts_code='all', message='股票基本信息更新完成')
                return True
            except Exception as e:
                logger.warning(f"{source.name}更新股票基本信息失败: {e}")
                continue
        
        logger.error("所有数据源更新股票基本信息失败")
        return False
    
    def update_stock_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新股票日线数据
        
        Args:
            ts_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            bool: 更新是否成功
        """
        for source in self.sources:
            try:
                logger.info(f"使用{source.name}更新股票日线数据")
                source.update_stock_daily(ts_codes, start_date, end_date)
                # 发布数据更新事件
                publish('data_updated', data_type='stock_daily', 
                        ts_code=ts_codes[0] if ts_codes else 'all', 
                        message='股票日线数据更新完成')
                return True
            except Exception as e:
                logger.warning(f"{source.name}更新股票日线数据失败: {e}")
                continue
        
        logger.error("所有数据源更新股票日线数据失败")
        return False
    
    def update_index_basic(self):
        """
        更新指数基本信息
        
        Returns:
            bool: 更新是否成功
        """
        for source in self.sources:
            try:
                logger.info(f"使用{source.name}更新指数基本信息")
                source.update_index_basic()
                # 发布数据更新事件
                publish('data_updated', data_type='index_basic', ts_code='all', message='指数基本信息更新完成')
                return True
            except Exception as e:
                logger.warning(f"{source.name}更新指数基本信息失败: {e}")
                continue
        
        logger.error("所有数据源更新指数基本信息失败")
        return False
    
    def update_index_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新指数日线数据
        
        Args:
            ts_codes: 指数代码列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            bool: 更新是否成功
        """
        for source in self.sources:
            try:
                logger.info(f"使用{source.name}更新指数日线数据")
                source.update_index_daily(ts_codes, start_date, end_date)
                # 发布数据更新事件
                publish('data_updated', data_type='index_daily', 
                        ts_code=ts_codes[0] if ts_codes else 'all', 
                        message='指数日线数据更新完成')
                return True
            except Exception as e:
                logger.warning(f"{source.name}更新指数日线数据失败: {e}")
                continue
        
        logger.error("所有数据源更新指数日线数据失败")
        return False
    
    def update_macro_data(self, indicators: List[str] = None):
        """
        更新宏观经济数据
        
        Args:
            indicators: 宏观经济指标列表
            
        Returns:
            bool: 更新是否成功
        """
        for source in self.sources:
            try:
                logger.info(f"使用{source.name}更新宏观经济数据")
                source.update_macro_data(indicators)
                # 发布数据更新事件
                publish('data_updated', data_type='macro', 
                        ts_code=indicators[0] if indicators else 'all', 
                        message='宏观经济数据更新完成')
                return True
            except Exception as e:
                logger.warning(f"{source.name}更新宏观经济数据失败: {e}")
                continue
        
        logger.error("所有数据源更新宏观经济数据失败")
        return False
    
    def update_news_data(self, sources: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新新闻数据
        
        Args:
            sources: 新闻来源列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            bool: 更新是否成功
        """
        for source in self.sources:
            try:
                logger.info(f"使用{source.name}更新新闻数据")
                source.update_news_data(sources, start_date, end_date)
                # 发布数据更新事件
                publish('data_updated', data_type='news', 
                        ts_code=sources[0] if sources else 'all', 
                        message='新闻数据更新完成')
                return True
            except Exception as e:
                logger.warning(f"{source.name}更新新闻数据失败: {e}")
                continue
        
        logger.error("所有数据源更新新闻数据失败")
        return False
    
    def update_stock_dividend(self, ts_codes: List[str] = None):
        """
        更新股票分红配股数据
        
        Args:
            ts_codes: 股票代码列表
            
        Returns:
            bool: 更新是否成功
        """
        for source in self.sources:
            try:
                logger.info(f"使用{source.name}更新股票分红配股数据")
                source.update_stock_dividend(ts_codes)
                # 发布数据更新事件
                publish('data_updated', data_type='stock_dividend', 
                        ts_code=ts_codes[0] if ts_codes else 'all', 
                        message='股票分红配股数据更新完成')
                return True
            except Exception as e:
                logger.warning(f"{source.name}更新股票分红配股数据失败: {e}")
                continue
        
        logger.error("所有数据源更新股票分红配股数据失败")
        return False
