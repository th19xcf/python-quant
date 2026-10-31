#!/usr/bin/env python3

"""
Baostock 指数数据 Mixin

指数基本信息、日线同步/下载与标准接口取数。
"""

from collections.abc import Callable
from typing import Any, List

import baostock as bs
import polars as pl
from loguru import logger

from src.data.sources import baostock_schema as schema


class BaostockIndexMixin:
    """Baostock 指数数据同步与标准接口实现。"""

    session: Any
    config: Any

    _ensure_baostock_login: Callable[..., bool]
    _get_default_codes: Callable[..., list]
    _process_dates: Callable[..., tuple]
    _to_baostock_code: Callable[..., str]
    _normalize_kline: Callable[..., pl.DataFrame]

    def update_index_basic(self):
        """
        更新指数基本信息
        """
        try:
            if not self._ensure_baostock_login():
                logger.warning("Baostock登录失败，无法获取指数基本信息")
                return

            logger.info("开始从Baostock获取指数基本信息")

            # 使用Baostock获取所有股票和指数的基本信息
            rs = bs.query_stock_basic(code_name="", fields="code,code_name,ipoDate,outDate,type,status")

            # 获取数据
            stock_basic_pd = rs.get_data()

            # 转换为Polars DataFrame
            stock_basic_df = pl.from_pandas(stock_basic_pd)

            if stock_basic_df.is_empty():
                logger.warning("从Baostock获取的基本信息为空")
                return

            # 过滤出指数数据（指数代码以sh.000或sz.399开头）
            index_basic_df = stock_basic_df.filter(
                (pl.col('code').str.starts_with('sh.000')) |
                (pl.col('code').str.starts_with('sz.399'))
            )

            if index_basic_df.is_empty():
                logger.warning("从Baostock获取的指数基本信息为空")
                return

            logger.info(f"从Baostock获取到{index_basic_df.height}条指数基本信息")

            # 离线模式下，只获取数据不存储
            if not self.session:
                logger.info("离线模式下，跳过指数基本信息存储")
                return index_basic_df

            # 数据清洗和标准化
            from src.database.models.index import IndexBasic

            # 遍历数据，进行清洗和存储
            for row in index_basic_df.iter_rows(named=True):
                try:
                    # 提取指数代码和名称
                    ts_code = row['code']
                    name = row['code_name']

                    # 查询指数是否已存在
                    index_basic = self.session.query(IndexBasic).filter_by(ts_code=ts_code).first()

                    if index_basic:
                        # 更新现有指数信息
                        index_basic.name = name
                    else:
                        # 创建新指数信息
                        index_basic = IndexBasic(
                            ts_code=ts_code,
                            name=name
                        )
                        self.session.add(index_basic)

                except (ConnectionError, TimeoutError, OSError) as row_e:
                    logger.exception(f"处理指数基本信息失败: {row_e}")
                    continue

            # 提交事务
            self.session.commit()
            logger.info("指数基本信息更新完成")
            return index_basic_df

        except (ConnectionError, TimeoutError, OSError) as e:
            if self.session:
                self.session.rollback()
            logger.exception(f"从Baostock获取指数基本信息失败: {e}")
            raise

    def update_index_daily(self, ts_codes: List[str] | None = None, start_date: str | None = None, end_date: str | None = None):
        """
        更新指数日线数据

        Args:
            ts_codes: 指数代码列表，None表示更新所有指数
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD

        Returns:
            dict: 指数代码到日线数据的映射（离线模式下）
        """
        try:
            if not self._ensure_baostock_login():
                logger.warning("Baostock登录失败，无法获取指数日线数据")
                return

            ts_codes = self._resolve_index_daily_codes(ts_codes)

            if not ts_codes:
                logger.warning("没有需要更新的指数代码")
                return

            # 处理日期参数
            start_date, end_date = self._process_dates(start_date, end_date, default_days=30)

            logger.info(f"开始更新{len(ts_codes)}个指数的日线数据，时间范围: {start_date} 至 {end_date}")

            # 存储结果（离线模式下返回）
            result: dict[str, Any] = {}

            # 遍历指数代码，获取日线数据
            for ts_code in ts_codes:
                try:
                    self._sync_index_daily_one(ts_code, start_date, end_date, result)

                except (ConnectionError, TimeoutError, OSError) as e:
                    if self.session:
                        self.session.rollback()
                    logger.exception(f"获取{ts_code}的日线数据失败: {e}")
                    continue

            # 离线模式下返回结果
            if not self.session and result:
                return result

        except (ConnectionError, TimeoutError, OSError) as e:
            if self.session:
                self.session.rollback()
            logger.exception(f"更新指数日线数据失败: {e}")
            raise

    def _resolve_index_daily_codes(self, ts_codes: List[str] | None = None):
        """解析待更新的指数代码：未指定时回落到数据库或离线默认列表"""
        # 如果没有指定指数代码，从数据库获取所有指数代码
        if ts_codes:
            return ts_codes
        if self.session:
            from src.database.models.index import IndexBasic
            index_basics = self.session.query(IndexBasic.ts_code).all()
            return [ib.ts_code for ib in index_basics]
        # 离线模式下使用默认指数代码
        codes = self._get_default_codes('index')
        logger.info(f"离线模式下，使用默认指数代码列表: {codes}")
        return codes

    @staticmethod
    def _build_index_daily_rows(index_daily_df, ts_code):
        """遍历原始行，转换为待写入行（不合格行跳过）"""
        daily_rows = []
        for row in index_daily_df.iter_rows(named=True):
            try:
                daily_rows.append(schema.daily_row_to_record(row, ts_code))
            except (ConnectionError, TimeoutError, OSError) as row_e:
                logger.exception(f"处理指数日线数据失败: {row_e}")
                continue
        return daily_rows

    def _persist_index_daily(self, ts_code, daily_rows):
        """批量查重写入并提交（每个指数一次事务）"""
        # 数据清洗和标准化
        from src.database.models.index import IndexDaily
        from src.database.upsert import upsert_rows

        # 批量查重后写入（已存在则更新，不存在则新增）
        upsert_rows(
            self.session, IndexDaily, daily_rows,
            key_fields=('ts_code', 'trade_date'),
            value_fields=('open', 'high', 'low', 'close', 'pre_close',
                          'change', 'pct_chg', 'vol', 'amount'),
        )

        # 每个指数提交一次事务
        self.session.commit()
        logger.info(f"{ts_code}的日线数据更新完成")

    def _sync_index_daily_one(self, ts_code, start_date, end_date, result):
        """同步单个指数的日线数据（离线模式仅收集结果）"""
        logger.info(f"正在获取{ts_code}的日线数据")

        # 使用Baostock获取指数日线数据
        rs = bs.query_history_k_data_plus(
            ts_code,
            schema.DAILY_FIELDS_BASIC,
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="3"  # 不复权
        )

        # 检查rs是否为None
        if rs is None:
            logger.warning(f"Baostock API返回None，无法获取{ts_code}的日线数据")
            return

        # 获取数据
        try:
            index_daily_pd = rs.get_data()
        # 兜底理由：单只指数取数失败跳过，不中断批量
        except Exception as e:  # noqa: BLE001
            logger.warning(f"获取{ts_code}数据失败: {e}")
            return

        # 转换为Polars DataFrame
        index_daily_df = pl.from_pandas(index_daily_pd)

        if index_daily_df.is_empty():
            logger.warning(f"{ts_code}在{start_date}至{end_date}期间没有日线数据")
            return

        logger.info(f"获取到{ts_code}的{index_daily_df.height}条日线数据")

        # 离线模式下，只获取数据不存储
        if not self.session:
            result[ts_code] = index_daily_pd
            logger.info(f"离线模式下，获取{ts_code}的日线数据完成")
            return

        daily_rows = self._build_index_daily_rows(index_daily_df, ts_code)
        self._persist_index_daily(ts_code, daily_rows)

    def download_index_daily(self, ts_codes: List[str] | None = None, start_date: str | None = None, end_date: str | None = None):
        """
        下载指数日线数据

        Args:
            ts_codes: 指数代码列表，None表示下载所有指数
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD

        Returns:
            dict: 指数代码到日线数据的映射
        """
        return self.update_index_daily(ts_codes, start_date, end_date)

    def get_index_data(self, ts_code: str, start_date: str, end_date: str,
                       freq: str = 'daily'):
        """
        标准接口：获取指数日线数据，返回统一格式。

        Returns:
            pl.DataFrame: 归一化后的指数K线数据
        """
        try:
            if not self._ensure_baostock_login():
                return pl.DataFrame()

            bs_code = self._to_baostock_code(ts_code)
            start_date, end_date = self._process_dates(start_date, end_date,
                                                       default_days=self.config.data.default_days)

            rs = bs.query_history_k_data_plus(
                bs_code,
                schema.DAILY_FIELDS_BASIC,
                start_date=start_date, end_date=end_date,
                frequency="d", adjustflag="3"
            )
            if rs is None:
                return pl.DataFrame()

            raw = rs.get_data()
            if raw is None or raw.empty:
                return pl.DataFrame()

            df = pl.from_pandas(raw)

            column_map = schema.BAOSTOCK_KLINE_COLUMN_MAP
            # 指数无复权因子，volume_unit 指数成交量单位为手
            return self._normalize_kline(
                df, column_map=column_map, ts_code=ts_code,
                volume_unit='hand', date_format='%Y-%m-%d', include_factors=True,
            )

        # 兜底理由：第三方 SDK 异常类型不可预知，失败返回空表
        except Exception as e:  # noqa: BLE001
            logger.warning(f"baostock get_index_data({ts_code}) 失败: {e}")
            return pl.DataFrame()
