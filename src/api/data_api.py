#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据访问层接口定义
"""

from typing import Any, Dict, List, Optional, Union
import polars as pl
import pandas as pd


class IDataProvider:
    """数据提供者接口，定义数据获取方法"""
    
    def get_stock_data(self, stock_code: str, start_date: str, end_date: str, frequency: str = '1d') -> Union[pl.DataFrame, pd.DataFrame]:
        """获取股票历史数据
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 数据频率，默认：1d（日线）
        
        Returns:
            pl.DataFrame或pd.DataFrame: 股票历史数据
        """
        pass
    
    def get_index_data(self, index_code: str, start_date: str, end_date: str, frequency: str = '1d') -> Union[pl.DataFrame, pd.DataFrame]:
        """获取指数历史数据
        
        Args:
            index_code: 指数代码
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 数据频率，默认：1d（日线）
        
        Returns:
            pl.DataFrame或pd.DataFrame: 指数历史数据
        """
        pass
    
    def get_stock_basic(self, exchange: Optional[str] = None) -> Union[pl.DataFrame, pd.DataFrame]:
        """获取股票基本信息
        
        Args:
            exchange: 交易所，可选值：'sh'（上海）、'sz'（深圳）、'bj'（北京）
        
        Returns:
            pl.DataFrame或pd.DataFrame: 股票基本信息
        """
        pass
    
    def get_index_basic(self, exchange: Optional[str] = None) -> Union[pl.DataFrame, pd.DataFrame]:
        """获取指数基本信息
        
        Args:
            exchange: 交易所，可选值：'sh'（上海）、'sz'（深圳）
        
        Returns:
            pl.DataFrame或pd.DataFrame: 指数基本信息
        """
        pass
    
    def update_stock_basic(self) -> bool:
        """更新股票基本信息
        
        Returns:
            bool: 更新是否成功
        """
        pass
    
    def update_index_basic(self) -> bool:
        """更新指数基本信息
        
        Returns:
            bool: 更新是否成功
        """
        pass


class IDataStorage:
    """数据存储接口，定义数据存储方法"""
    
    def save_stock_data(self, stock_code: str, data: Union[pl.DataFrame, pd.DataFrame], frequency: str = '1d') -> bool:
        """保存股票数据
        
        Args:
            stock_code: 股票代码
            data: 股票数据
            frequency: 数据频率，默认：1d（日线）
        
        Returns:
            bool: 保存是否成功
        """
        pass
    
    def save_index_data(self, index_code: str, data: Union[pl.DataFrame, pd.DataFrame], frequency: str = '1d') -> bool:
        """保存指数数据
        
        Args:
            index_code: 指数代码
            data: 指数数据
            frequency: 数据频率，默认：1d（日线）
        
        Returns:
            bool: 保存是否成功
        """
        pass
    
    def save_stock_basic(self, data: Union[pl.DataFrame, pd.DataFrame]) -> bool:
        """保存股票基本信息
        
        Args:
            data: 股票基本信息
        
        Returns:
            bool: 保存是否成功
        """
        pass
    
    def save_index_basic(self, data: Union[pl.DataFrame, pd.DataFrame]) -> bool:
        """保存指数基本信息
        
        Args:
            data: 指数基本信息
        
        Returns:
            bool: 保存是否成功
        """
        pass
    
    def delete_stock_data(self, stock_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None, frequency: str = '1d') -> bool:
        """删除股票数据
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 数据频率，默认：1d（日线）
        
        Returns:
            bool: 删除是否成功
        """
        pass
    
    def delete_index_data(self, index_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None, frequency: str = '1d') -> bool:
        """删除指数数据
        
        Args:
            index_code: 指数代码
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 数据频率，默认：1d（日线）
        
        Returns:
            bool: 删除是否成功
        """
        pass


class IDataProcessor:
    """数据处理器接口，定义数据处理方法"""
    
    def preprocess_data(self, data: Union[pl.DataFrame, pd.DataFrame]) -> Union[pl.DataFrame, pd.DataFrame]:
        """预处理数据
        
        Args:
            data: 原始数据
        
        Returns:
            pl.DataFrame或pd.DataFrame: 预处理后的数据
        """
        pass
    
    def sample_data(self, data: Union[pl.DataFrame, pd.DataFrame], target_points: int = 1000, strategy: str = 'adaptive') -> Union[pl.DataFrame, pd.DataFrame]:
        """采样数据，减少数据量
        
        Args:
            data: 原始数据
            target_points: 目标采样点数
            strategy: 采样策略，可选值：'uniform'（均匀采样）、'adaptive'（自适应采样）
        
        Returns:
            pl.DataFrame或pd.DataFrame: 采样后的数据
        """
        pass
    
    def convert_data_type(self, data: Union[pl.DataFrame, pd.DataFrame], target_type: str = 'float32') -> Union[pl.DataFrame, pd.DataFrame]:
        """转换数据类型
        
        Args:
            data: 原始数据
            target_type: 目标数据类型，默认：float32
        
        Returns:
            pl.DataFrame或pd.DataFrame: 转换后的数据
        """
        pass
    
    def clean_data(self, data: Union[pl.DataFrame, pd.DataFrame]) -> Union[pl.DataFrame, pd.DataFrame]:
        """清洗数据，处理缺失值、异常值等
        
        Args:
            data: 原始数据
        
        Returns:
            pl.DataFrame或pd.DataFrame: 清洗后的数据
        """
        pass


class IDataCache:
    """数据缓存接口，定义数据缓存方法"""
    
    def get_cache(self, key: str) -> Optional[Any]:
        """获取缓存数据
        
        Args:
            key: 缓存键
        
        Returns:
            Any: 缓存数据，如果不存在则返回None
        """
        pass
    
    def set_cache(self, key: str, value: Any, expire_time: Optional[int] = None) -> bool:
        """设置缓存数据
        
        Args:
            key: 缓存键
            value: 缓存值
            expire_time: 过期时间（秒），默认：None（永不过期）
        
        Returns:
            bool: 设置是否成功
        """
        pass
    
    def delete_cache(self, key: str) -> bool:
        """删除缓存数据
        
        Args:
            key: 缓存键
        
        Returns:
            bool: 删除是否成功
        """
        pass
    
    def clear_cache(self) -> bool:
        """清空所有缓存数据
        
        Returns:
            bool: 清空是否成功
        """
        pass
    
    def get_cache_keys(self, pattern: str) -> List[str]:
        """获取匹配模式的缓存键
        
        Args:
            pattern: 匹配模式，如：'stock:*'
        
        Returns:
            List[str]: 匹配的缓存键列表
        """
        pass