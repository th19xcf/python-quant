#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Baostock数据源插件，封装Baostock数据处理器功能
"""

from loguru import logger
from src.plugin.plugin_base import DataSourcePlugin


class BaostockDataSourcePlugin(DataSourcePlugin):
    """
    Baostock数据源插件，封装Baostock数据处理器功能
    """
    
    def __init__(self):
        super().__init__()
        self.name = "BaostockDataSource"
        self.version = "0.1.0"
        self.author = "Quant System"
        self.description = "Baostock数据源插件，用于从Baostock获取股票和指数数据"
        self.baostock_handler = None
    
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
            logger.info(f"初始化Baostock数据源插件，配置: {config}")
            
            # 导入BaostockHandler
            from src.data.baostock_handler import BaostockHandler
            from src.database.db_manager import DatabaseManager
            
            # 初始化数据库管理器
            db_manager = None
            if hasattr(config, 'database'):
                try:
                    db_manager = DatabaseManager(config)
                    db_manager.connect()
                    logger.info("Baostock数据源插件数据库连接成功")
                except Exception as db_e:
                    logger.warning(f"Baostock数据源插件数据库连接失败，将以离线模式运行: {db_e}")
                    db_manager = None
            
            # 初始化Baostock处理器
            self.baostock_handler = BaostockHandler(config, db_manager)
            logger.info("Baostock数据源插件初始化成功")
            return True
        except Exception as e:
            logger.exception(f"初始化Baostock数据源插件失败: {e}")
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
            logger.info(f"Baostock数据源插件获取股票数据: {ts_code}, 周期: {freq}")
            
            # Baostock使用sh.600000格式的代码
            # 转换格式：600000.SH -> sh.600000
            if '.' in ts_code:
                code_part, market_part = ts_code.split('.')
                baostock_code = f"{market_part.lower()}.{code_part}"
            else:
                # 纯数字格式，如600000
                market = "sh" if ts_code.startswith("6") else "sz"
                baostock_code = f"{market}.{ts_code}"
            
            logger.info(f"转换为Baostock格式代码: {baostock_code}")
            
            if freq == "daily":
                # 获取日线数据
                return self.baostock_handler.download_stock_daily([baostock_code], start_date, end_date)
            else:
                # 获取分钟线数据
                # Baostock分钟线频率：1, 5, 15, 30, 60
                freq_map = {
                    "1min": "1",
                    "5min": "5",
                    "15min": "15",
                    "30min": "30",
                    "60min": "60"
                }
                
                baostock_freq = freq_map.get(freq, "5")
                return self.baostock_handler.download_stock_minute([baostock_code], start_date, end_date, baostock_freq)
        except Exception as e:
            logger.exception(f"Baostock数据源插件获取股票数据失败: {e}")
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
            logger.info(f"Baostock数据源插件获取指数数据: {ts_code}, 周期: {freq}")
            
            # Baostock使用sh.000001格式的代码
            # 转换格式：000001.SH -> sh.000001
            if '.' in ts_code:
                code_part, market_part = ts_code.split('.')
                baostock_code = f"{market_part.lower()}.{code_part}"
            else:
                # 纯数字格式，如000001
                market = "sh" if ts_code.startswith("0") else "sz"
                baostock_code = f"{market}.{ts_code}"
            
            logger.info(f"转换为Baostock格式代码: {baostock_code}")
            
            if freq == "daily":
                # 获取日线数据
                return self.baostock_handler.download_index_daily([baostock_code], start_date, end_date)
            else:
                # 获取分钟线数据
                # Baostock分钟线频率：1, 5, 15, 30, 60
                freq_map = {
                    "1min": "1",
                    "5min": "5",
                    "15min": "15",
                    "30min": "30",
                    "60min": "60"
                }
                
                baostock_freq = freq_map.get(freq, "5")
                return self.baostock_handler.download_index_minute([baostock_code], start_date, end_date, baostock_freq)
        except Exception as e:
            logger.exception(f"Baostock数据源插件获取指数数据失败: {e}")
            return None
    
    def update_stock_basic(self) -> bool:
        """
        更新股票基本信息
        
        Returns:
            bool: 更新是否成功
        """
        try:
            logger.info("Baostock数据源插件更新股票基本信息")
            self.baostock_handler.update_stock_basic()
            logger.info("Baostock数据源插件更新股票基本信息成功")
            return True
        except Exception as e:
            logger.exception(f"Baostock数据源插件更新股票基本信息失败: {e}")
            return False