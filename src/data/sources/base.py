#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据源基类模块

定义所有数据源的通用接口和基类
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union
import polars as pl
from loguru import logger


class DataSourceBase(ABC):
    """
    数据源基类，所有数据源都需要继承这个类并实现相应的方法
    """
    
    def __init__(self, config, db_manager=None):
        """
        初始化数据源
        
        Args:
            config: 配置对象
            db_manager: 数据库管理器实例
        """
        self.config = config
        self.db_manager = db_manager
        self.name = self.__class__.__name__
        self.available = False
        self.initialize()
    
    def initialize(self):
        """
        初始化数据源
        
        子类可以重写此方法进行初始化操作
        """
        try:
            self._initialize()
            self.available = True
            logger.info(f"{self.name} 初始化成功")
        except Exception as e:
            self.available = False
            logger.warning(f"{self.name} 初始化失败: {e}")
    
    def is_available(self) -> bool:
        """检查数据源是否可用（默认返回 self.available，子类可覆盖）"""
        return self.available

    # ------------------------------------------------------------------
    # 以下三个方法历史上是 @abstractmethod，现已降级为普通方法。
    # 原因：DataFetcher._get_data_from_sources 内部按 handler 名硬编码调用
    # 对应方法（tdx→get_kline_data, baostock→download_stock_daily），
    # 不走 DataSourceBase 标准接口；强制 abstractmethod 会阻碍 handler 继承。
    # 默认抛 NotImplementedError，子类可选覆盖。
    # ------------------------------------------------------------------

    def _initialize(self):
        """具体的初始化实现。建议子类覆盖（handler 若在 __init__ 里自行初始化，可不覆盖）。"""
        raise NotImplementedError(
            f"{self.__class__.__name__} 必须实现 _initialize() 或覆盖 initialize()")

    def get_stock_data(self, ts_code: str, start_date: str, end_date: str,
                       freq: str = 'daily', adjustment_type: str = 'qfq'):
        """获取股票数据（标准接口，子类可选覆盖）"""
        raise NotImplementedError(
            f"{self.__class__.__name__} 未实现 get_stock_data()，"
            f"DataFetcher 会按 handler 名调用对应方法"
            f"（如 get_kline_data / download_stock_daily）")

    def get_index_data(self, ts_code: str, start_date: str, end_date: str,
                       freq: str = 'daily'):
        """获取指数数据（标准接口，子类可选覆盖）"""
        raise NotImplementedError(
            f"{self.__class__.__name__} 未实现 get_index_data()")
    
    def update_stock_basic(self):
        """
        更新股票基本信息
        
        子类可以重写此方法
        """
        logger.info(f"{self.name} 不支持更新股票基本信息")
    
    def update_stock_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新股票日线数据
        
        子类可以重写此方法
        
        Args:
            ts_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
        """
        logger.info(f"{self.name} 不支持更新股票日线数据")
    
    def update_index_basic(self):
        """
        更新指数基本信息
        
        子类可以重写此方法
        """
        logger.info(f"{self.name} 不支持更新指数基本信息")
    
    def update_index_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新指数日线数据
        
        子类可以重写此方法
        
        Args:
            ts_codes: 指数代码列表
            start_date: 开始日期
            end_date: 结束日期
        """
        logger.info(f"{self.name} 不支持更新指数日线数据")
    
    def update_macro_data(self, indicators: List[str] = None):
        """
        更新宏观经济数据
        
        子类可以重写此方法
        
        Args:
            indicators: 宏观经济指标列表
        """
        logger.info(f"{self.name} 不支持更新宏观经济数据")
    
    def update_news_data(self, sources: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新新闻数据
        
        子类可以重写此方法
        
        Args:
            sources: 新闻来源列表
            start_date: 开始日期
            end_date: 结束日期
        """
        logger.info(f"{self.name} 不支持更新新闻数据")
    
    def update_stock_dividend(self, ts_codes: List[str] = None):
        """
        更新股票分红配股数据
        
        子类可以重写此方法
        
        Args:
            ts_codes: 股票代码列表
        """
        logger.info(f"{self.name} 不支持更新股票分红配股数据")
    
    def get_stock_dividend(self, ts_code: str) -> Union[pl.DataFrame, None]:
        """
        获取股票分红配股数据
        
        子类可以重写此方法
        
        Args:
            ts_code: 股票代码
            
        Returns:
            pl.DataFrame: 分红配股数据
        """
        logger.info(f"{self.name} 不支持获取股票分红配股数据")
        return None
    
    def get_stock_basic(self, exchange: Optional[str] = None) -> Union[pl.DataFrame, None]:
        """
        获取股票基本信息
        
        子类可以重写此方法
        
        Args:
            exchange: 交易所
            
        Returns:
            pl.DataFrame: 股票基本信息
        """
        logger.info(f"{self.name} 不支持获取股票基本信息")
        return None
    
    def get_index_basic(self, exchange: Optional[str] = None) -> Union[pl.DataFrame, None]:
        """
        获取指数基本信息
        
        子类可以重写此方法
        
        Args:
            exchange: 交易所
            
        Returns:
            pl.DataFrame: 指数基本信息
        """
        logger.info(f"{self.name} 不支持获取指数基本信息")
        return None
    
    def update_fund_basic(self):
        """
        更新基金基本信息
        
        子类可以重写此方法
        """
        logger.info(f"{self.name} 不支持更新基金基本信息")
    
    def update_fund_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新基金日线数据
        
        子类可以重写此方法
        
        Args:
            ts_codes: 基金代码列表
            start_date: 开始日期
            end_date: 结束日期
        """
        logger.info(f"{self.name} 不支持更新基金日线数据")
    
    def get_fund_data(self, ts_code: str, start_date: str, end_date: str, freq: str = 'daily', adjustment_type: str = 'qfq') -> Union[pl.DataFrame, None]:
        """
        获取基金数据
        
        Args:
            ts_code: 基金代码
            start_date: 开始日期
            end_date: 结束日期
            freq: 频率，daily或minute
            adjustment_type: 复权类型
            
        Returns:
            pl.DataFrame: 基金数据
        """
        logger.info(f"{self.name} 不支持获取基金数据")
        return None
    
    def get_fund_basic(self, exchange: Optional[str] = None) -> Union[pl.DataFrame, None]:
        """
        获取基金基本信息
        
        子类可以重写此方法
        
        Args:
            exchange: 交易所
            
        Returns:
            pl.DataFrame: 基金基本信息
        """
        logger.info(f"{self.name} 不支持获取基金基本信息")
        return None
    
    def update_closed_fund_basic(self):
        """
        更新封闭式基金基本信息
        
        子类可以重写此方法
        """
        logger.info(f"{self.name} 不支持更新封闭式基金基本信息")
    
    def update_closed_fund_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新封闭式基金日线数据
        
        子类可以重写此方法
        
        Args:
            ts_codes: 封闭式基金代码列表
            start_date: 开始日期
            end_date: 结束日期
        """
        logger.info(f"{self.name} 不支持更新封闭式基金日线数据")
    
    def get_closed_fund_data(self, ts_code: str, start_date: str, end_date: str, freq: str = 'daily', adjustment_type: str = 'qfq') -> Union[pl.DataFrame, None]:
        """
        获取封闭式基金数据
        
        Args:
            ts_code: 封闭式基金代码
            start_date: 开始日期
            end_date: 结束日期
            freq: 频率，daily或minute
            adjustment_type: 复权类型
            
        Returns:
            pl.DataFrame: 封闭式基金数据
        """
        logger.info(f"{self.name} 不支持获取封闭式基金数据")
        return None
    
    def get_closed_fund_basic(self, exchange: Optional[str] = None) -> Union[pl.DataFrame, None]:
        """
        获取封闭式基金基本信息
        
        子类可以重写此方法
        
        Args:
            exchange: 交易所
            
        Returns:
            pl.DataFrame: 封闭式基金基本信息
        """
        logger.info(f"{self.name} 不支持获取封闭式基金基本信息")
        return None
