#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TDX数据源插件，封装通达信数据处理器功能
"""

from loguru import logger
from src.plugin.plugin_base import DataSourcePlugin


class TdxDataSourcePlugin(DataSourcePlugin):
    """
    TDX数据源插件，封装通达信数据处理器功能
    """
    
    def __init__(self):
        super().__init__()
        self.name = "TdxDataSource"
        self.version = "0.1.0"
        self.author = "Quant System"
        self.description = "通达信数据源插件，用于获取通达信数据文件中的股票数据"
        self.tdx_handler = None
    
    def get_name(self) -> str:
        return self.name
    
    def get_version(self) -> str:
        return self.version
    
    def get_author(self) -> str:
        return self.author
    
    def get_description(self) -> str:
        return self.description
    
    def initialize(self, config) -> bool:
        """
        初始化插件
        
        Args:
            config: 插件配置
            
        Returns:
            bool: 初始化是否成功
        """
        try:
            logger.info(f"初始化TDX数据源插件，配置: {config}")
            
            # 导入TdxHandler
            from src.data.tdx_handler import TdxHandler
            from src.database.db_manager import DatabaseManager
            
            # 初始化数据库管理器
            db_manager = None
            if hasattr(config, 'database'):
                try:
                    db_manager = DatabaseManager(config)
                    db_manager.connect()
                    logger.info("TDX数据源插件数据库连接成功")
                except Exception as db_e:
                    logger.warning(f"TDX数据源插件数据库连接失败，将以离线模式运行: {db_e}")
                    db_manager = None
            
            # 初始化TDX处理器
            self.tdx_handler = TdxHandler(config, db_manager)
            logger.info("TDX数据源插件初始化成功")
            return True
        except Exception as e:
            logger.exception(f"初始化TDX数据源插件失败: {e}")
            return False
    
    def get_stock_data(self, ts_code: str, start_date: str, end_date: str, freq: str = "daily"):
        """
        获取股票数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            freq: 周期，daily或minute
            
        Returns:
            Any: 股票数据，通常为DataFrame或字典列表
        """
        try:
            logger.info(f"TDX数据源插件获取股票数据: {ts_code}, 周期: {freq}")
            
            if freq == "daily":
                # 获取日线数据
                data = self.tdx_handler.get_kline_data(ts_code, start_date, end_date)
                return data
            else:
                # 获取分钟线数据
                logger.warning(f"TDX数据源插件暂不支持{freq}周期数据")
                return None
        except Exception as e:
            logger.exception(f"TDX数据源插件获取股票数据失败: {e}")
            return None
    
    def get_index_data(self, ts_code: str, start_date: str, end_date: str, freq: str = "daily"):
        """
        获取指数数据
        
        Args:
            ts_code: 指数代码
            start_date: 开始日期
            end_date: 结束日期
            freq: 周期，daily或minute
            
        Returns:
            Any: 指数数据，通常为DataFrame或字典列表
        """
        try:
            logger.info(f"TDX数据源插件获取指数数据: {ts_code}, 周期: {freq}")
            
            # 指数数据也使用get_kline_data方法获取
            return self.get_stock_data(ts_code, start_date, end_date, freq)
        except Exception as e:
            logger.exception(f"TDX数据源插件获取指数数据失败: {e}")
            return None
    
    def update_stock_basic(self) -> bool:
        """
        更新股票基本信息
        
        Returns:
            bool: 更新是否成功
        """
        try:
            logger.info("TDX数据源插件更新股票基本信息")
            # TDX数据源不直接提供股票基本信息更新功能
            logger.warning("TDX数据源插件不支持直接更新股票基本信息")
            return True
        except Exception as e:
            logger.exception(f"TDX数据源插件更新股票基本信息失败: {e}")
            return False