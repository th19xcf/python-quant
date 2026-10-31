#!/usr/bin/env python3

"""
数据源基类模块

定义所有数据源的通用接口和基类
"""

from abc import ABC
from datetime import datetime, timedelta
from typing import Dict, List, Union

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
        # 兜底理由：数据源初始化失败标记不可用，不阻断启动
        except Exception as e:  # noqa: BLE001
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
        date_format: str | None = None,
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
        df = DataSourceBase._map_kline_columns(df, column_map)

        # 2. 日期统一为 date 类型
        df = DataSourceBase._normalize_kline_date(df, date_format)

        # 3. 数值列统一为 float64
        df = DataSourceBase._cast_kline_numeric(df)

        # 4. 成交量单位统一为"手"（原始为"股"时除以 100）
        df = DataSourceBase._convert_kline_volume(df, volume_unit)

        # 5. 补齐 ts_code
        if ts_code and 'ts_code' not in df.columns:
            df = df.with_columns(pl.lit(ts_code).alias('ts_code'))

        # 6. 补齐复权因子列（缺失时填 1.0）
        df = DataSourceBase._fill_kline_factors(df, include_factors)

        # 7. 价格/量/额缺失值填 0
        df = DataSourceBase._fill_kline_nulls(df)

        # 8. 只保留标准列并按 date 排序
        keep_cols = [c for c in KLINE_STANDARD_COLUMNS if c in df.columns]
        df = df.select(keep_cols)

        if 'date' in df.columns:
            df = df.sort('date')

        # 9. 质量校验：OHLC 硬错误剔除 + pct_chg 重算 + 停牌日统计
        return DataSourceBase._validate_kline_quality(df)

    @staticmethod
    def _map_kline_columns(df: pl.DataFrame, column_map: Dict[str, str]) -> pl.DataFrame:
        """1. 列名映射：标准名 <- 原始名"""
        rename_map = {orig: std for std, orig in column_map.items() if orig in df.columns}
        if rename_map:
            df = df.rename(rename_map)
        return df

    @staticmethod
    def _normalize_kline_date(df: pl.DataFrame, date_format: str | None) -> pl.DataFrame:
        """2. 日期统一为 date 类型"""
        if date_format and 'date' in df.columns:
            return df.with_columns(
                pl.col('date').cast(pl.Utf8)
                .str.strptime(pl.Date, format=date_format, strict=False)
                .alias('date')
            )
        if 'date' in df.columns and df.schema['date'] != pl.Date:
            return df.with_columns(pl.col('date').cast(pl.Date, strict=False).alias('date'))
        return df

    @staticmethod
    def _cast_kline_numeric(df: pl.DataFrame) -> pl.DataFrame:
        """3. 数值列统一为 float64"""
        numeric_cols = ['open', 'high', 'low', 'close', 'pre_close',
                        'volume', 'amount', 'pct_chg', 'change']
        cast_exprs = [
            pl.col(c).cast(pl.Float64, strict=False).alias(c)
            for c in numeric_cols if c in df.columns
        ]
        if cast_exprs:
            df = df.with_columns(cast_exprs)
        return df

    @staticmethod
    def _convert_kline_volume(df: pl.DataFrame, volume_unit: str) -> pl.DataFrame:
        """4. 成交量单位统一为"手"（原始为"股"时除以 100）"""
        if volume_unit == 'share' and 'volume' in df.columns:
            return df.with_columns((pl.col('volume') / 100.0).alias('volume'))
        return df

    @staticmethod
    def _fill_kline_factors(df: pl.DataFrame, include_factors: bool) -> pl.DataFrame:
        """6. 补齐复权因子列（缺失时填 1.0）"""
        if not include_factors:
            return df

        factor_exprs = []
        if 'qfq_factor' not in df.columns:
            factor_exprs.append(pl.lit(1.0).alias('qfq_factor'))
        if 'hfq_factor' not in df.columns:
            factor_exprs.append(pl.lit(1.0).alias('hfq_factor'))
        if factor_exprs:
            df = df.with_columns(factor_exprs)
        return df.with_columns([
            pl.col('qfq_factor').cast(pl.Float64, strict=False).fill_null(1.0),
            pl.col('hfq_factor').cast(pl.Float64, strict=False).fill_null(1.0),
        ])

    @staticmethod
    def _fill_kline_nulls(df: pl.DataFrame) -> pl.DataFrame:
        """7. 价格/量/额缺失值填 0"""
        fill_cols = ['open', 'high', 'low', 'close', 'pre_close', 'volume', 'amount',
                     'pct_chg', 'change']
        fill_exprs = [
            pl.col(c).fill_null(0.0).alias(c)
            for c in fill_cols if c in df.columns
        ]
        if fill_exprs:
            df = df.with_columns(fill_exprs)
        return df

    @staticmethod
    def _validate_kline_quality(df: pl.DataFrame) -> pl.DataFrame:
        """
        K 线质量校验（归一化收口点的最后一道闸门）

        规则：
            1. 停牌日（volume <= 0）保留，仅计数，不做价格结构校验
            2. 非停牌日 OHLC 硬错误整行剔除并告警：
               价格非正、low > high、low/high 未包住 open/close
            3. 存在合法 pre_close 且非停牌时，change/pct_chg 按收盘价重算，
               覆盖上游数据源的错误值

        Args:
            df: 已归一化、按 date 升序的 K 线数据

        Returns:
            pl.DataFrame: 校验后的 K 线数据
        """
        if df.is_empty():
            return df

        required = {'open', 'high', 'low', 'close'}
        if not required.issubset(set(df.columns)):
            return df

        suspended = pl.col('volume') <= 0 if 'volume' in df.columns else pl.lit(False)

        invalid_price = (
            (pl.col('open') <= 0) | (pl.col('high') <= 0) |
            (pl.col('low') <= 0) | (pl.col('close') <= 0) |
            (pl.col('high') < pl.col('low')) |
            (pl.col('low') > pl.min_horizontal('open', 'close')) |
            (pl.col('high') < pl.max_horizontal('open', 'close'))
        )
        bad_mask = invalid_price & ~suspended
        bad_count = df.filter(bad_mask).height
        if bad_count:
            sample = []
            if 'date' in df.columns:
                sample = df.filter(bad_mask).select('date').head(3).to_series().to_list()
            logger.warning(f"K线质量校验剔除 {bad_count} 行非法 OHLC 数据，样例日期: {sample}")
            df = df.filter(~bad_mask)

        if 'pre_close' in df.columns:
            valid_base = (pl.col('pre_close') > 0) & ~suspended
            recompute_exprs = []
            if 'change' in df.columns:
                recompute_exprs.append(
                    pl.when(valid_base)
                    .then(pl.col('close') - pl.col('pre_close'))
                    .otherwise(pl.col('change'))
                    .alias('change')
                )
            if 'pct_chg' in df.columns:
                recompute_exprs.append(
                    pl.when(valid_base)
                    .then((pl.col('close') / pl.col('pre_close') - 1) * 100)
                    .otherwise(pl.col('pct_chg'))
                    .alias('pct_chg')
                )
            if recompute_exprs:
                df = df.with_columns(recompute_exprs)

        if 'volume' in df.columns:
            suspend_count = df.filter(pl.col('volume') <= 0).height
            if suspend_count:
                logger.info(f"K线含 {suspend_count} 个停牌交易日（volume<=0），已保留原始记录")

        return df

    # ------------------------------------------------------------------
    # 通用参数处理
    #
    # 默认代码列表、日期默认值、分钟线频率校验在各数据源中语义一致，
    # 统一收口到基类，避免每个 handler/Mixin 各写一份实现而漂移。
    # ------------------------------------------------------------------

    def _get_default_codes(self, code_type: str):
        """
        获取默认代码列表

        Args:
            code_type: 代码类型，'stock'或'index'

        Returns:
            list: 默认代码列表
        """
        if code_type == 'stock':
            return getattr(self, 'default_stock_codes', [])
        elif code_type == 'index':
            return getattr(self, 'default_index_codes', [])
        else:
            return []

    def _process_dates(self, start_date: str, end_date: str,
                       default_days: int = 30, date_format: str = "%Y-%m-%d"):
        """
        处理日期参数，设置默认值

        Args:
            start_date: 开始日期
            end_date: 结束日期
            default_days: 默认天数
            date_format: 日期字符串格式（baostock 用 %Y-%m-%d，akshare 用 %Y%m%d）

        Returns:
            tuple: (start_date, end_date)
        """
        if not end_date:
            end_date = datetime.now().strftime(date_format)
        if not start_date:
            # 默认更新最近 default_days 天的数据
            start_date = (datetime.now() - timedelta(days=default_days)).strftime(date_format)

        return start_date, end_date

    def _validate_frequency(self, frequency: str):
        """
        验证分钟线频率

        子类未声明 supported_frequencies 时原样返回，不做校验。

        Args:
            frequency: 分钟线频率

        Returns:
            str: 验证后的分钟线频率
        """
        supported = getattr(self, 'supported_frequencies', None)
        if supported is None:
            return frequency
        if frequency not in supported:
            logger.warning(f"不支持的分钟线频率: {frequency}，使用默认频率5分钟")
            return "5"
        return frequency

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

    def get_stock_basic(self, exchange: str | None = None) -> Union[pl.DataFrame, None]:
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

    def get_index_basic(self, exchange: str | None = None) -> Union[pl.DataFrame, None]:
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

    def get_fund_basic(self, exchange: str | None = None) -> Union[pl.DataFrame, None]:
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

    def get_closed_fund_basic(self, exchange: str | None = None) -> Union[pl.DataFrame, None]:
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
