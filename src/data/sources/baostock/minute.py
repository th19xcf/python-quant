#!/usr/bin/env python3

"""
Baostock 分钟线与实时数据 Mixin

实时数据替代、分钟线下载与入库。
"""

from collections.abc import Callable
from typing import Any, List

import baostock as bs
import pandas as pd
import polars as pl
from loguru import logger

from src.data.sources import baostock_schema as schema


class BaostockMinuteMixin:
    """Baostock 实时行情与分钟线数据处理。"""

    session: Any
    config: Any

    _ensure_baostock_login: Callable[..., bool]
    _get_default_codes: Callable[..., list]
    _process_dates: Callable[..., tuple]
    _validate_frequency: Callable[..., str]

    def _get_realtime_data(self, ts_codes: List[str] | None, code_type: str, noun: str,
                           default_days: int, adjustflag: str):
        """
        通用实现：以最近若干日历史数据代替实时数据（股票/指数共用）

        Args:
            ts_codes: 代码列表，None 表示使用默认代码
            code_type: 默认代码类型（stock/index）
            noun: 日志中的中文名词（股票/指数）
            default_days: 未指定日期时回溯的天数
            adjustflag: 复权标志（2 前复权 / 3 不复权）

        Returns:
            pl.DataFrame: 按日期降序排列的行情数据
        """
        try:
            if not self._ensure_baostock_login():
                logger.warning(f"Baostock登录失败，无法获取实时{noun}数据")
                return pl.DataFrame()

            if not ts_codes:
                ts_codes = self._get_default_codes(code_type)

            logger.info(f"开始从Baostock获取{len(ts_codes)}个{noun}的历史数据作为实时数据替代")

            # 获取最近若干天数据，确保能取到
            start_date, end_date = self._process_dates(None, None, default_days=default_days)

            pd_frames = []

            for ts_code in ts_codes:
                try:
                    rs = bs.query_history_k_data_plus(
                        ts_code,
                        schema.DAILY_FIELDS_BASIC,
                        start_date=start_date,
                        end_date=end_date,
                        frequency="d",
                        adjustflag=adjustflag
                    )

                    frame = rs.get_data()
                    if frame is not None and not frame.empty:
                        pd_frames.append(frame)
                    else:
                        logger.warning(f"{ts_code}没有{noun}历史数据")
                        continue

                except (ConnectionError, TimeoutError, OSError) as e:
                    logger.exception(f"获取{ts_code}的历史{noun}数据失败: {e}")
                    continue

            if not pd_frames:
                logger.warning(f"从Baostock获取的实时{noun}数据为空")
                return pl.DataFrame()

            # 循环内只收集 pandas 帧，循环结束后一次性 from_pandas：
            # 原实现每个标的都调一次 from_pandas，需重复付 Arrow 互操作与 schema 推断的固定开销
            combined_df = pl.from_pandas(
                pd.concat(pd_frames, ignore_index=True)
            ).sort(by="date", descending=True)

            logger.info(f"从Baostock获取到{combined_df.height}条{noun}实时数据")
            return combined_df

        except (ConnectionError, TimeoutError, OSError) as e:
            logger.exception(f"获取实时{noun}数据失败: {e}")
            return pl.DataFrame()

    def get_realtime_stock_data(self, ts_codes: List[str] | None = None):
        """获取实时股票数据（以最近历史数据替代）。"""
        return self._get_realtime_data(
            ts_codes, 'stock', '股票', self.config.data.default_realtime_days, "2")

    def get_realtime_index_data(self, ts_codes: List[str] | None = None):
        """获取实时指数数据（以最近历史数据替代）。"""
        return self._get_realtime_data(ts_codes, 'index', '指数', 5, "3")

    def _download_minute(self, model, code_type: str, noun: str, default_days: int,
                         ts_codes: List[str] | None, start_date: str | None,
                         end_date: str | None, frequency: str):
        """
        通用实现：分钟线下载并入库（股票/指数共用）

        写库统一存原始价（adjustflag=3），复权由查询侧按因子统一处理。

        Args:
            model: 目标 ORM 模型（StockMinute / IndexMinute）
            code_type: 默认代码类型（stock/index）
            noun: 日志中的中文名词（股票/指数）
            default_days: 未指定日期时回溯的天数
            ts_codes: 代码列表，None 表示使用默认代码
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 分钟线频率，支持1、5、15、30、60分钟

        Returns:
            dict: 离线模式下返回 代码 -> DataFrame 的映射，在线模式返回 None
        """
        try:
            if not self._ensure_baostock_login():
                logger.warning(f"Baostock登录失败，无法获取{noun}分钟线数据")
                return

            if not ts_codes:
                ts_codes = self._get_default_codes(code_type)

            if not ts_codes:
                logger.warning(f"没有需要下载的{noun}代码")
                return

            # 验证分钟线频率
            frequency = self._validate_frequency(frequency)

            # 处理日期参数
            start_date, end_date = self._process_dates(start_date, end_date, default_days=default_days)

            logger.info(f"开始下载{len(ts_codes)}个{noun}的{frequency}分钟线数据，时间范围: {start_date} 至 {end_date}")

            result: dict[str, Any] = {}

            for ts_code in ts_codes:
                try:
                    self._download_minute_one(
                        model, ts_code, noun, start_date, end_date, frequency, result)

                except (ConnectionError, TimeoutError, OSError) as e:
                    if self.session:
                        self.session.rollback()
                    logger.exception(f"下载{ts_code}的{frequency}分钟线数据失败: {e}")
                    continue

            # 离线模式下返回结果
            if not self.session and result:
                return result

        except (ConnectionError, TimeoutError, OSError) as e:
            if self.session:
                self.session.rollback()
            logger.exception(f"下载{noun}分钟线数据失败: {e}")
            raise

    def _download_minute_one(self, model, ts_code, noun, start_date, end_date,
                             frequency, result):
        """下载单个标的的分钟线并入库（离线模式仅收集结果）"""
        logger.info(f"正在下载{ts_code}的{frequency}分钟线数据")

        rs = bs.query_history_k_data_plus(
            ts_code,
            schema.MINUTE_FIELDS,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            adjustflag="3"  # 不复权，写库存原始价
        )

        if rs is None:
            logger.warning(f"Baostock API返回None，无法获取{ts_code}的{frequency}分钟线数据")
            return

        try:
            minute_pd = rs.get_data()
        # 兜底理由：单只股票分钟线取数失败跳过
        except Exception as e:  # noqa: BLE001
            logger.warning(f"获取{ts_code}的{frequency}分钟线数据失败: {e}")
            return

        minute_df = pl.from_pandas(minute_pd)

        if minute_df.is_empty():
            logger.warning(f"{ts_code}在{start_date}至{end_date}期间没有{frequency}分钟线数据")
            return

        logger.info(f"获取到{ts_code}的{minute_df.height}条{frequency}分钟线数据")

        # 离线模式下，只获取数据不存储
        if not self.session:
            result[ts_code] = minute_pd
            logger.info(f"离线模式下，获取{ts_code}的{frequency}分钟线数据完成")
            return

        minute_rows = self._build_minute_rows(minute_df, ts_code, noun)
        self._persist_minute_rows(model, ts_code, minute_rows, frequency)

    @staticmethod
    def _build_minute_rows(minute_df, ts_code, noun):
        """遍历原始行，转换为待写入行（不合格行跳过）"""
        minute_rows = []
        for row in minute_df.iter_rows(named=True):
            try:
                minute_rows.append(
                    schema.minute_row_to_record(row, ts_code))
            except (ConnectionError, TimeoutError, OSError) as row_e:
                logger.exception(f"处理{noun}分钟线数据失败: {row_e}")
                continue
        return minute_rows

    def _persist_minute_rows(self, model, ts_code, minute_rows, frequency):
        """批量查重写入并提交（每个标的一次事务）"""
        from src.database.upsert import upsert_rows

        # 批量查重后写入（已存在则更新，不存在则新增）
        upsert_rows(
            self.session, model, minute_rows,
            key_fields=('ts_code', 'trade_date', 'trade_time'),
            value_fields=('open', 'high', 'low', 'close', 'vol', 'amount'),
            create_defaults={'frequency': frequency},
        )

        # 每个标的提交一次事务
        self.session.commit()
        logger.info(f"{ts_code}的{frequency}分钟线数据下载完成")

    def download_stock_minute(self, ts_codes: List[str] | None = None, start_date: str | None = None, end_date: str | None = None, frequency: str = "5"):
        """下载股票分钟线数据并入库（写库存原始价）。"""
        from src.database.models.stock import StockMinute
        return self._download_minute(
            StockMinute, 'stock', '股票', self.config.data.default_minute_days,
            ts_codes, start_date, end_date, frequency)

    def download_index_minute(self, ts_codes: List[str] | None = None, start_date: str | None = None, end_date: str | None = None, frequency: str = "5"):
        """下载指数分钟线数据并入库（写库存原始价）。"""
        # 存量问题：IndexMinute 模型尚未在 src.database.models.index 中实现，
        # 该接口一旦被调用会在运行期失败；此处仅按既有约定保留导入以避免改动行为
        from src.database.models.index import IndexMinute  # type: ignore[attr-defined]
        return self._download_minute(
            IndexMinute, 'index', '指数', 3,
            ts_codes, start_date, end_date, frequency)
