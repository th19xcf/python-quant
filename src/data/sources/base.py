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

# K线数据统一输出列定义（所有数据源归一化后的标准列集合）
KLINE_STANDARD_COLUMNS = [
    'date', 'ts_code', 'open', 'high', 'low', 'close', 'pre_close',
    'volume', 'amount', 'pct_chg', 'change', 'qfq_factor', 'hfq_factor',
]

# 每个数据源必须实现的契约方法（DataFetcher 依赖它们做多源调度）
REQUIRED_SOURCE_METHODS = ('get_stock_data', 'get_index_data')


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
        return getattr(self, 'available', True)

    @classmethod
    def missing_contract_methods(cls) -> List[str]:
        """
        检查本类是否覆盖了全部契约方法。

        Returns:
            List[str]: 未覆盖的方法名；空列表表示契约完整
        """
        return [name for name in REQUIRED_SOURCE_METHODS
                if getattr(cls, name) is getattr(DataSourceBase, name)]

    # ------------------------------------------------------------------
    # 统一数据归一化收口点
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_kline(
        df: pl.DataFrame,
        column_map: Dict[str, str],
        ts_code: str = '',
        volume_unit: str = 'hand',
        date_format: Optional[str] = None,
        include_factors: bool = True,
    ) -> pl.DataFrame:
        """
        将各数据源的原始K线数据归一化为统一格式（收口点）。

        统一输出列（KLINE_STANDARD_COLUMNS）：
            date, ts_code, open, high, low, close, pre_close,
            volume, amount, pct_chg, change, qfq_factor, hfq_factor

        统一规则：
            - date: date 类型
            - open/high/low/close/pre_close: float64（原始价，不复权）
            - volume: float64，单位统一为"手"
            - amount: float64，单位为"元"
            - pct_chg/change: float64
            - qfq_factor/hfq_factor: float64，缺失时填 1.0

        Args:
            df: 原始 DataFrame
            column_map: 标准列名 -> 原始列名 的映射，如 {'open': '开盘', ...}
            ts_code: 统一格式的股票代码（如 600000.SH）
            volume_unit: 原始成交量单位，'hand'(手) 或 'share'(股)
            date_format: 日期字符串格式，None 表示 date 列已是 date 类型
            include_factors: 是否补齐 qfq_factor/hfq_factor 列

        Returns:
            归一化后的 DataFrame（按 date 升序排列）
        """
        if df.is_empty():
            return df

        # 1. 列名映射：标准名 <- 原始名
        rename_map = {orig: std for std, orig in column_map.items() if orig in df.columns}
        if rename_map:
            df = df.rename(rename_map)

        # 2. 日期统一为 date 类型
        if date_format and 'date' in df.columns:
            df = df.with_columns(
                pl.col('date').cast(pl.Utf8)
                .str.strptime(pl.Date, format=date_format, strict=False)
                .alias('date')
            )
        elif 'date' in df.columns and df.schema['date'] != pl.Date:
            df = df.with_columns(pl.col('date').cast(pl.Date, strict=False).alias('date'))

        # 3. 数值列统一为 float64
        numeric_cols = ['open', 'high', 'low', 'close', 'pre_close',
                        'volume', 'amount', 'pct_chg', 'change']
        cast_exprs = [
            pl.col(c).cast(pl.Float64, strict=False).alias(c)
            for c in numeric_cols if c in df.columns
        ]
        if cast_exprs:
            df = df.with_columns(cast_exprs)

        # 4. 成交量单位统一为"手"（原始为"股"时除以 100）
        if volume_unit == 'share' and 'volume' in df.columns:
            df = df.with_columns((pl.col('volume') / 100.0).alias('volume'))

        # 5. 补齐 ts_code
        if ts_code and 'ts_code' not in df.columns:
            df = df.with_columns(pl.lit(ts_code).alias('ts_code'))

        # 6. 补齐复权因子列（缺失时填 1.0）
        if include_factors:
            factor_exprs = []
            if 'qfq_factor' not in df.columns:
                factor_exprs.append(pl.lit(1.0).alias('qfq_factor'))
            if 'hfq_factor' not in df.columns:
                factor_exprs.append(pl.lit(1.0).alias('hfq_factor'))
            if factor_exprs:
                df = df.with_columns(factor_exprs)
            df = df.with_columns([
                pl.col('qfq_factor').cast(pl.Float64, strict=False).fill_null(1.0),
                pl.col('hfq_factor').cast(pl.Float64, strict=False).fill_null(1.0),
            ])

        # 7. 价格/量/额缺失值填 0
        fill_cols = ['open', 'high', 'low', 'close', 'pre_close', 'volume', 'amount',
                     'pct_chg', 'change']
        fill_exprs = [
            pl.col(c).fill_null(0.0).alias(c)
            for c in fill_cols if c in df.columns
        ]
        if fill_exprs:
            df = df.with_columns(fill_exprs)

        # 8. 只保留标准列并按 date 排序
        keep_cols = [c for c in KLINE_STANDARD_COLUMNS if c in df.columns]
        df = df.select(keep_cols)

        if 'date' in df.columns:
            df = df.sort('date')

        return df

    # ------------------------------------------------------------------
    # 标准接口契约
    #
    # DataFetcher 通过 hasattr + 签名探测按标准接口调用数据源
    # （见 managers/data_fetcher.py::_get_data_from_sources），不再按
    # handler 名硬编码方法。因此 get_stock_data / get_index_data 是
    # 每个数据源必须覆盖的契约方法，未覆盖时调用会抛 NotImplementedError。
    #
    # 这里保留普通方法而非 @abstractmethod：历史 handler 在 __init__ 中
    # 自行完成初始化，不调用基类 __init__，也不实现 _initialize()；
    # 强制抽象会让它们无法实例化。契约由 validate_contract() 与
    # tests/test_data_source_contract.py 共同保证。
    # ------------------------------------------------------------------

    def _initialize(self):
        """具体初始化实现。在 __init__ 中自行初始化的 handler 可不实现。"""
        raise NotImplementedError(
            f"{self.__class__.__name__} 未实现 _initialize()；"
            "若在 __init__ 中自行初始化，请显式覆盖 initialize() 或直接设置 available")

    def get_stock_data(self, ts_code: str, start_date: str, end_date: str,
                       freq: str = 'daily', adjustment_type: str = 'qfq'):
        """获取股票数据（标准接口，子类必须覆盖）"""
        raise NotImplementedError(
            f"{self.__class__.__name__} 未实现标准接口 get_stock_data()")

    def get_index_data(self, ts_code: str, start_date: str, end_date: str,
                       freq: str = 'daily'):
        """获取指数数据（标准接口，子类必须覆盖）"""
        raise NotImplementedError(
            f"{self.__class__.__name__} 未实现标准接口 get_index_data()")
    
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
