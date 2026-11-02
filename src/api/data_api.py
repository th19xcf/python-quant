#!/usr/bin/env python3

"""
数据访问层接口定义
统一使用Polars DataFrame作为数据格式
"""

from typing import List, Protocol, Union

import polars as pl


class IDataProvider(Protocol):
    """数据提供者契约（结构化子类型）

    实现类只需具备同名方法即可通过类型检查，不要求继承本类。刻意不采用
    普通基类继承：基类提供的同名桩方法会让类型检查器把"未实现"误判为
    "已存在"，从而掩盖真实缺口。

    注意：本契约不含 get_fund_data —— 全仓既无实现也无调用（fund_daily
    表只有写入路径，无读取接口），保留它只会虚增契约面。
    """

    def get_stock_data(
        self,
        stock_code: str,
        start_date: str,
        end_date: str,
        frequency: str = '1d',
        adjustment_type: str = 'hfq',
    ) -> pl.DataFrame:
        """获取股票历史数据

        Args:
            stock_code: 股票代码
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 数据频率，默认：1d（日线）
            adjustment_type: 复权类型，默认：hfq（后复权，PIT 安全，供计算使用）；
                            展示场景请显式传 qfq；可选：qfq/hfq/none

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

    def get_stock_basic(self, exchange: str | None = None) -> pl.DataFrame:
        """获取股票基本信息

        Args:
            exchange: 交易所，可选值：'sh'（上海）、'sz'（深圳）、'bj'（北京）

        Returns:
            pl.DataFrame: 股票基本信息
        """
        raise NotImplementedError("子类必须实现此方法")

    def get_index_basic(self, exchange: str | None = None) -> pl.DataFrame:
        """获取指数基本信息

        Args:
            exchange: 交易所，可选值：'sh'（上海）、'sz'（深圳）

        Returns:
            pl.DataFrame: 指数基本信息
        """
        raise NotImplementedError("子类必须实现此方法")

    def get_fund_basic(self, fund_type: str | None = None) -> pl.DataFrame:
        """获取基金基本信息

        Args:
            fund_type: 基金类型，可选值：'open'（开放式）、'closed'（封闭式）、None（全部）

        Returns:
            pl.DataFrame: 基金基本信息
        """
        raise NotImplementedError("子类必须实现此方法")

    def get_stock_dividend(self, ts_code: str) -> pl.DataFrame:
        """获取股票分红配股数据

        Args:
            ts_code: 股票代码（如 000001.SZ）；实现方按单标的返回全部分红记录

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
        ts_codes: List[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
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


class IDataProcessor(Protocol):
    """数据处理器契约（结构化子类型）

    实现类无需继承本类，类型检查器按方法签名做结构化一致性校验。
    """

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

    def sample_data(self, data: Union[pl.DataFrame, pl.LazyFrame], target_points: int = 1000, strategy: str = 'adaptive') -> Union[pl.DataFrame, pl.LazyFrame]:
        """采样数据，减少数据量

        Args:
            data: 原始数据（Polars DataFrame 或 LazyFrame）
            target_points: 目标采样点数
            strategy: 采样策略，可选值：'uniform'（均匀采样）、'adaptive'（自适应采样）

        Returns:
            Union[pl.DataFrame, pl.LazyFrame]: 采样后的数据（与入参类型一致）
        """
        raise NotImplementedError("子类必须实现此方法")

    def convert_data_type(self, data: Union[pl.DataFrame, pl.LazyFrame], target_type: str = 'float64') -> Union[pl.DataFrame, pl.LazyFrame]:
        """转换数据类型

        Args:
            data: 原始数据（Polars DataFrame 或 LazyFrame）
            target_type: 目标数据类型，默认：float32

        Returns:
            Union[pl.DataFrame, pl.LazyFrame]: 转换后的数据（与入参类型一致）
        """
        raise NotImplementedError("子类必须实现此方法")

    def clean_data(self, data: Union[pl.DataFrame, pl.LazyFrame]) -> Union[pl.DataFrame, pl.LazyFrame]:
        """清洗数据，处理缺失值、异常值等

        Args:
            data: 原始数据（Polars DataFrame 或 LazyFrame）

        Returns:
            Union[pl.DataFrame, pl.LazyFrame]: 清洗后的数据（与入参类型一致）
        """
        raise NotImplementedError("子类必须实现此方法")

