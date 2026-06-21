#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据访问层接口定义
统一使用Polars DataFrame作为数据格式
"""

from typing import Any, Dict, List, Optional, Union
import polars as pl


class IDataProvider:
    """数据提供者接口，定义数据获取方法"""

    def get_stock_data(
        self,
        stock_code: str,
        start_date: str,
        end_date: str,
        frequency: str = '1d',
        adjustment_type: str = 'qfq',
    ) -> pl.DataFrame:
        """获取股票历史数据

        Args:
            stock_code: 股票代码
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 数据频率，默认：1d（日线）
            adjustment_type: 复权类型，默认：qfq（前复权）；可选：qfq/hfq/none

        Returns:
            pl.DataFrame: 股票历史数据
        """
        raise NotImplementedError("子类必须实现此方法")

    def get_index_data(
        self,
        index_code: str,
        start_date: str,
        end_date: str,
        frequency: str = '1d',
    ) -> pl.DataFrame:
        """获取指数历史数据

        Args:
            index_code: 指数代码
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 数据频率，默认：1d（日线）

        Returns:
            pl.DataFrame: 指数历史数据
        """
        raise NotImplementedError("子类必须实现此方法")

    def get_fund_data(
        self,
        fund_code: str,
        start_date: str,
        end_date: str,
        frequency: str = '1d',
    ) -> pl.DataFrame:
        """获取基金历史数据

        Args:
            fund_code: 基金代码
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 数据频率，默认：1d（日线）

        Returns:
            pl.DataFrame: 基金历史数据
        """
        raise NotImplementedError("子类必须实现此方法")

    def get_stock_basic(self, exchange: Optional[str] = None) -> pl.DataFrame:
        """获取股票基本信息

        Args:
            exchange: 交易所，可选值：'sh'（上海）、'sz'（深圳）、'bj'（北京）

        Returns:
            pl.DataFrame: 股票基本信息
        """
        raise NotImplementedError("子类必须实现此方法")

    def get_index_basic(self, exchange: Optional[str] = None) -> pl.DataFrame:
        """获取指数基本信息

        Args:
            exchange: 交易所，可选值：'sh'（上海）、'sz'（深圳）

        Returns:
            pl.DataFrame: 指数基本信息
        """
        raise NotImplementedError("子类必须实现此方法")

    def get_fund_basic(self, fund_type: Optional[str] = None) -> pl.DataFrame:
        """获取基金基本信息

        Args:
            fund_type: 基金类型，可选值：'open'（开放式）、'closed'（封闭式）、None（全部）

        Returns:
            pl.DataFrame: 基金基本信息
        """
        raise NotImplementedError("子类必须实现此方法")

    def get_stock_dividend(
        self,
        stock_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pl.DataFrame:
        """获取股票分红配股数据

        Args:
            stock_code: 股票代码
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD

        Returns:
            pl.DataFrame: 分红配股数据
        """
        raise NotImplementedError("子类必须实现此方法")

    def update_stock_basic(self) -> bool:
        """更新股票基本信息

        Returns:
            bool: 更新是否成功
        """
        raise NotImplementedError("子类必须实现此方法")

    def update_index_basic(self) -> bool:
        """更新指数基本信息

        Returns:
            bool: 更新是否成功
        """
        raise NotImplementedError("子类必须实现此方法")

    def update_fund_basic(self) -> bool:
        """更新基金基本信息

        Returns:
            bool: 更新是否成功
        """
        raise NotImplementedError("子类必须实现此方法")

    def update_stock_daily(
        self,
        ts_codes: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> bool:
        """更新日线数据

        Args:
            ts_codes: 股票代码列表，None 表示全部
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD

        Returns:
            bool: 更新是否成功
        """
        raise NotImplementedError("子类必须实现此方法")


class IDataStorage:
    """数据存储接口，定义数据存储方法"""

    def save_stock_data(self, stock_code: str, data: pl.DataFrame, frequency: str = '1d') -> bool:
        """保存股票数据

        Args:
            stock_code: 股票代码
            data: 股票数据（Polars DataFrame）
            frequency: 数据频率，默认：1d（日线）

        Returns:
            bool: 保存是否成功
        """
        raise NotImplementedError("子类必须实现此方法")

    def save_index_data(self, index_code: str, data: pl.DataFrame, frequency: str = '1d') -> bool:
        """保存指数数据

        Args:
            index_code: 指数代码
            data: 指数数据（Polars DataFrame）
            frequency: 数据频率，默认：1d（日线）

        Returns:
            bool: 保存是否成功
        """
        raise NotImplementedError("子类必须实现此方法")

    def save_stock_basic(self, data: pl.DataFrame) -> bool:
        """保存股票基本信息

        Args:
            data: 股票基本信息（Polars DataFrame）

        Returns:
            bool: 保存是否成功
        """
        raise NotImplementedError("子类必须实现此方法")

    def save_index_basic(self, data: pl.DataFrame) -> bool:
        """保存指数基本信息

        Args:
            data: 指数基本信息（Polars DataFrame）

        Returns:
            bool: 保存是否成功
        """
        raise NotImplementedError("子类必须实现此方法")

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
        raise NotImplementedError("子类必须实现此方法")

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
        raise NotImplementedError("子类必须实现此方法")


class IDataProcessor:
    """数据处理器接口，定义数据处理方法"""

    def preprocess_data(
        self,
        data: Union[pl.DataFrame, pl.LazyFrame],
    ) -> Union[pl.DataFrame, pl.LazyFrame]:
        """预处理数据

        Args:
            data: 原始数据（Polars DataFrame 或 LazyFrame）

        Returns:
            Union[pl.DataFrame, pl.LazyFrame]: 预处理后的数据
        """
        raise NotImplementedError("子类必须实现此方法")

    def sample_data(self, data: pl.DataFrame, target_points: int = 1000, strategy: str = 'adaptive') -> pl.DataFrame:
        """采样数据，减少数据量

        Args:
            data: 原始数据（Polars DataFrame）
            target_points: 目标采样点数
            strategy: 采样策略，可选值：'uniform'（均匀采样）、'adaptive'（自适应采样）

        Returns:
            pl.DataFrame: 采样后的数据
        """
        raise NotImplementedError("子类必须实现此方法")

    def convert_data_type(self, data: pl.DataFrame, target_type: str = 'float32') -> pl.DataFrame:
        """转换数据类型

        Args:
            data: 原始数据（Polars DataFrame）
            target_type: 目标数据类型，默认：float32

        Returns:
            pl.DataFrame: 转换后的数据
        """
        raise NotImplementedError("子类必须实现此方法")

    def clean_data(self, data: pl.DataFrame) -> pl.DataFrame:
        """清洗数据，处理缺失值、异常值等

        Args:
            data: 原始数据（Polars DataFrame）

        Returns:
            pl.DataFrame: 清洗后的数据
        """
        raise NotImplementedError("子类必须实现此方法")


class IDataCache:
    """数据缓存接口，定义数据缓存方法"""

    def get_cache(self, key: str) -> Optional[Any]:
        """获取缓存数据

        Args:
            key: 缓存键

        Returns:
            Any: 缓存数据，如果不存在则返回None
        """
        raise NotImplementedError("子类必须实现此方法")

    def set_cache(self, key: str, value: Any, expire_time: Optional[int] = None) -> bool:
        """设置缓存数据

        Args:
            key: 缓存键
            value: 缓存值
            expire_time: 过期时间（秒），默认：None（永不过期）

        Returns:
            bool: 设置是否成功
        """
        raise NotImplementedError("子类必须实现此方法")

    def delete_cache(self, key: str) -> bool:
        """删除缓存数据

        Args:
            key: 缓存键

        Returns:
            bool: 删除是否成功
        """
        raise NotImplementedError("子类必须实现此方法")

    def clear_cache(self) -> bool:
        """清空所有缓存数据

        Returns:
            bool: 清空是否成功
        """
        raise NotImplementedError("子类必须实现此方法")

    def get_cache_keys(self, pattern: str) -> List[str]:
        """获取匹配模式的缓存键

        Args:
            pattern: 匹配模式，如：'stock:*'

        Returns:
            List[str]: 匹配的缓存键列表
        """
        raise NotImplementedError("子类必须实现此方法")
