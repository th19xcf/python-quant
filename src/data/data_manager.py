#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据获取与管理模块
"""

from loguru import logger
from typing import List, Dict, Any


class DataManager:
    """
    数据管理器，负责统一管理各种数据源的获取、清洗和存储
    """
    
    def __init__(self, config, db_manager):
        """
        初始化数据管理器
        
        Args:
            config: 配置对象
            db_manager: 数据库管理器实例
        """
        self.config = config
        self.db_manager = db_manager
        
        # 初始化各个数据源处理器
        self.tdx_handler = None
        self.akshare_handler = None
        self.macro_handler = None
        self.news_handler = None
        
        self._init_handlers()
    
    def _init_handlers(self):
        """
        初始化各个数据源处理器
        """
        try:
            # 初始化通达信数据处理器
            try:
                from src.data.tdx_handler import TdxHandler
                self.tdx_handler = TdxHandler(self.config, self.db_manager)
            except Exception as tdx_e:
                logger.warning(f"通达信数据处理器初始化失败（离线模式下正常）: {tdx_e}")
                self.tdx_handler = None
            
            # 初始化Baostock数据处理器
            try:
                from src.data.baostock_handler import BaostockHandler
                self.baostock_handler = BaostockHandler(self.config, self.db_manager)
                logger.info("Baostock数据处理器初始化成功")
            except Exception as e:
                logger.exception(f"Baostock数据处理器初始化失败: {e}")
                self.baostock_handler = None
            
            # 初始化宏观数据处理器
            try:
                from src.data.macro_handler import MacroHandler
                self.macro_handler = MacroHandler(self.config, self.db_manager)
            except Exception as macro_e:
                logger.warning(f"宏观数据处理器初始化失败（离线模式下正常）: {macro_e}")
                self.macro_handler = None
            
            # 初始化新闻数据处理器
            try:
                from src.data.news_handler import NewsHandler
                self.news_handler = NewsHandler(self.config, self.db_manager)
            except Exception as news_e:
                logger.warning(f"新闻数据处理器初始化失败（离线模式下正常）: {news_e}")
                self.news_handler = None
            
            logger.info("数据处理器初始化完成")
            
            # 初始化完成后，自动更新股票基本信息，确保stock_basic表有数据
            try:
                self.update_stock_basic()
                logger.info("自动更新股票基本信息完成")
            except Exception as update_e:
                logger.warning(f"自动更新股票基本信息失败: {update_e}")
            
        except Exception as e:
            logger.exception(f"数据处理器初始化失败: {e}")
            # 离线模式下不抛出异常，继续运行
            logger.info("离线模式下继续运行")
    
    def update_stock_basic(self):
        """
        更新股票基本信息
        """
        try:
            # 优先从Baostock获取最新数据
            if self.baostock_handler:
                self.baostock_handler.update_stock_basic()
            logger.info("股票基本信息更新完成")
            
        except Exception as e:
            logger.exception(f"股票基本信息更新失败: {e}")
            raise
    
    def update_stock_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新股票日线数据
        
        Args:
            ts_codes: 股票代码列表，None表示更新所有股票
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
        """
        try:
            # 优先从Baostock获取最新数据
            if self.baostock_handler:
                self.baostock_handler.update_stock_daily(ts_codes, start_date, end_date)
            logger.info("股票日线数据更新完成")
            
        except Exception as e:
            logger.exception(f"股票日线数据更新失败: {e}")
            raise
    
    def update_index_basic(self):
        """
        更新指数基本信息
        """
        try:
            if self.baostock_handler:
                self.baostock_handler.update_index_basic()
            logger.info("指数基本信息更新完成")
            
        except Exception as e:
            logger.exception(f"指数基本信息更新失败: {e}")
            raise
    
    def update_index_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新指数日线数据
        
        Args:
            ts_codes: 指数代码列表，None表示更新所有指数
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
        """
        try:
            if self.baostock_handler:
                self.baostock_handler.update_index_daily(ts_codes, start_date, end_date)
            logger.info("指数日线数据更新完成")
            
        except Exception as e:
            logger.exception(f"指数日线数据更新失败: {e}")
            raise
    
    def update_macro_data(self, indicators: List[str] = None):
        """
        更新宏观经济数据
        
        Args:
            indicators: 宏观经济指标列表，None表示更新所有指标
        """
        try:
            if self.macro_handler:
                self.macro_handler.update_macro_data(indicators)
            logger.info("宏观经济数据更新完成")
            
        except Exception as e:
            logger.exception(f"宏观经济数据更新失败: {e}")
            raise
    
    def update_news_data(self, sources: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新新闻数据
        
        Args:
            sources: 新闻来源列表，None表示更新所有来源
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
        """
        try:
            if self.news_handler:
                self.news_handler.update_news_data(sources, start_date, end_date)
            logger.info("新闻数据更新完成")
            
        except Exception as e:
            logger.exception(f"新闻数据更新失败: {e}")
            raise
    
    def get_stock_data(self, ts_code: str, start_date: str, end_date: str, freq: str = "daily"):
        """
        获取股票数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            freq: 周期，daily或minute
            
        Returns:
            pl.DataFrame: 股票数据
        """
        try:
            # TODO: 实现数据查询逻辑
            pass
            
        except Exception as e:
            logger.exception(f"获取股票数据失败: {e}")
            raise
    
    def get_index_data(self, ts_code: str, start_date: str, end_date: str, freq: str = "daily"):
        """
        获取指数数据
        
        Args:
            ts_code: 指数代码
            start_date: 开始日期
            end_date: 结束日期
            freq: 周期，daily或minute
            
        Returns:
            pl.DataFrame: 指数数据
        """
        try:
            # TODO: 实现数据查询逻辑
            pass
            
        except Exception as e:
            logger.exception(f"获取指数数据失败: {e}")
            raise
    
    def get_stock_basic(self, ts_code: str = None):
        """
        获取股票基本信息
        
        Args:
            ts_code: 股票代码，None表示获取所有股票基本信息
            
        Returns:
            dict: 股票代码到名称的映射
        """
        try:
            if not self.db_manager:
                logger.warning("数据库连接不可用，无法获取股票基本信息")
                return {}
            
            from src.database.models.stock import StockBasic
            
            session = self.db_manager.get_session()
            if not session:
                return {}
            
            try:
                # 检查stock_basic表是否有数据
                query = session.query(StockBasic)
                stock_basics = query.all()
                
                # 如果没有数据，插入默认股票信息
                if not stock_basics:
                    logger.info("stock_basic表为空，插入默认股票信息")
                    default_stocks = [
                        {"ts_code": "600000.SH", "name": "浦发银行"},
                        {"ts_code": "000001.SZ", "name": "平安银行"},
                        {"ts_code": "300001.SZ", "name": "特锐德"}
                    ]
                    
                    for stock_info in default_stocks:
                        # 检查是否已存在
                        existing_stock = session.query(StockBasic).filter_by(ts_code=stock_info["ts_code"]).first()
                        if not existing_stock:
                            new_stock = StockBasic(
                                ts_code=stock_info["ts_code"],
                                name=stock_info["name"]
                            )
                            session.add(new_stock)
                    
                    # 提交事务
                    session.commit()
                    # 重新查询数据
                    stock_basics = session.query(StockBasic).all()
                
                # 构建股票代码到名称的映射
                stock_map = {}
                for stock in stock_basics:
                    stock_map[stock.ts_code] = stock.name
                
                # 如果指定了ts_code但没有找到，添加到映射中
                if ts_code and ts_code not in stock_map:
                    default_map = {
                        "600000.SH": "浦发银行",
                        "000001.SZ": "平安银行",
                        "300001.SZ": "特锐德"
                    }
                    if ts_code in default_map:
                        stock_map[ts_code] = default_map[ts_code]
                
                return stock_map
            except Exception as query_e:
                # 如果查询失败，可能是表不存在，使用默认映射
                logger.warning(f"股票基本信息查询失败: {query_e}")
                # 返回默认映射，使用代码作为名称
                default_map = {
                    "600000.SH": "浦发银行",
                    "000001.SZ": "平安银行",
                    "300001.SZ": "特锐德"
                }
                return default_map
            
        except Exception as e:
            logger.exception(f"获取股票基本信息失败: {e}")
            # 返回默认映射，使用代码作为名称
            default_map = {
                "600000.SH": "浦发银行",
                "000001.SZ": "平安银行",
                "300001.SZ": "特锐德"
            }
            return default_map
