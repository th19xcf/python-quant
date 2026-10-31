#!/usr/bin/env python3

"""
AkShare 指数数据 Mixin

指数基本信息、日线同步与标准接口取数。
"""

from collections.abc import Callable
from datetime import datetime
from typing import Any, List

import polars as pl
from loguru import logger

from src.data.sources import akshare_schema as schema
from src.data.sources.akshare.constants import _DEFAULT_INDEX_CODES


class AkShareIndexMixin:
    """AkShare 指数数据同步与标准接口实现。"""

    session: Any
    config: Any
    db_manager: Any

    _normalize_date_str: Callable[..., str]
    _normalize_kline: Callable[..., pl.DataFrame]

    def update_index_basic(self):
        """
        更新指数基本信息
        """
        try:
            logger.info("开始从AkShare获取指数基本信息")

            # 使用AkShare获取指数基本信息（契约见 akshare_schema.INDEX_BASIC）
            index_basic_df = schema.resolve(
                schema.fetch_spec(schema.INDEX_BASIC), schema.INDEX_BASIC)

            if index_basic_df.is_empty():
                logger.warning("从AkShare获取的指数基本信息为空")
                return

            logger.info(f"从AkShare获取到{index_basic_df.height}条指数基本信息")

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
                    ts_code = row['ts_code']
                    name = row['name']

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
            logger.exception(f"从AkShare获取指数基本信息失败: {e}")
            raise

    def update_index_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新指数日线数据

        Args:
            ts_codes: 指数代码列表，None表示更新所有指数
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD

        Returns:
            dict: 指数代码到日线数据的映射（离线模式下）
        """
        try:
            # 离线模式下的默认指数代码列表（使用带前缀的格式）
            default_ts_codes = _DEFAULT_INDEX_CODES

            # 如果没有指定指数代码，从数据库获取所有指数代码
            if not ts_codes:
                if self.session:
                    from src.database.models.index import IndexBasic
                    index_basics = self.session.query(IndexBasic.ts_code).all()
                    ts_codes = [ib.ts_code for ib in index_basics]
                else:
                    # 离线模式下使用默认指数代码
                    ts_codes = default_ts_codes
                    logger.info(f"离线模式下，使用默认指数代码列表: {ts_codes}")

            if not ts_codes:
                logger.warning("没有需要更新的指数代码")
                return

            logger.info(f"开始更新{len(ts_codes)}个指数的日线数据")

            # 存储结果（离线模式下返回）
            result = {}

            # 遍历指数代码，获取日线数据
            for ts_code in ts_codes:
                try:
                    logger.info(f"正在获取{ts_code}的日线数据")

                    # 使用AkShare获取指数日线数据（契约见 akshare_schema.INDEX_DAILY）
                    index_daily_df = schema.resolve(
                        schema.fetch_spec(schema.INDEX_DAILY, symbol=ts_code),
                        schema.INDEX_DAILY,
                    )

                    if index_daily_df.is_empty():
                        logger.warning(f"{ts_code}没有日线数据")
                        continue

                    logger.info(f"获取到{ts_code}的{index_daily_df.height}条日线数据")

                    # 离线模式下，只获取数据不存储
                    if not self.session:
                        result[ts_code] = index_daily_df
                        logger.info(f"离线模式下，获取{ts_code}的日线数据完成")
                        continue

                    # 数据清洗和标准化
                    from src.database.models.index import IndexDaily
                    from src.database.upsert import upsert_rows

                    # 遍历数据，转换为待写入行（不合格行跳过）
                    daily_rows = []
                    for row in index_daily_df.iter_rows(named=True):
                        try:
                            daily_rows.append({
                                'ts_code': ts_code,
                                'trade_date': datetime.strptime(row['date'], "%Y-%m-%d").date(),
                                'open': row['open'],
                                'high': row['high'],
                                'low': row['low'],
                                'close': row['close'],
                                'vol': row['volume'],
                            })
                        except (ConnectionError, TimeoutError, OSError) as row_e:
                            logger.exception(f"处理指数日线数据失败: {row_e}")
                            continue

                    # 批量查重后写入（已存在则更新，不存在则新增）
                    upsert_rows(
                        self.session, IndexDaily, daily_rows,
                        key_fields=('ts_code', 'trade_date'),
                        value_fields=('open', 'high', 'low', 'close', 'vol'),
                        create_defaults={
                            'pre_close': 0.0,  # stock_zh_index_daily 不提供昨收价
                            'change': 0.0,  # 不提供涨跌额
                            'pct_chg': 0.0,  # 不提供涨跌幅
                            'amount': 0.0,  # 不提供成交额
                        },
                    )

                    # 每个指数提交一次事务
                    self.session.commit()
                    logger.info(f"{ts_code}的日线数据更新完成")

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

    def get_index_data(self, ts_code: str, start_date: str, end_date: str,
                       freq: str = 'daily'):
        """
        标准接口：获取指数日线数据，返回统一格式。

        Returns:
            pl.DataFrame: 归一化后的指数K线数据
        """
        try:
            sd = self._normalize_date_str(start_date) if start_date else None
            ed = self._normalize_date_str(end_date) if end_date else None

            df = schema.fetch_raw(
                schema.INDEX_DAILY_EM_EN.name, symbol=ts_code,
                start_date=sd, end_date=ed,
            )
            if df.is_empty():
                return pl.DataFrame()

            # 上游中英文列名随版本切换，由 select_spec 选择匹配的规格
            spec = schema.select_spec(
                (schema.INDEX_DAILY_EM_EN, schema.INDEX_DAILY_EM_CN), df)
            return self._normalize_kline(
                df, column_map=dict(spec.column_map), ts_code=ts_code,
                volume_unit=spec.volume_unit, date_format=spec.date_format,
                include_factors=True,
            )

        # 兜底理由：第三方接口异常类型不可预知，失败返回空表
        except Exception as e:  # noqa: BLE001
            logger.warning(f"akshare get_index_data({ts_code}) 失败: {e}")
            return pl.DataFrame()
