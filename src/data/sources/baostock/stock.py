#!/usr/bin/env python3

"""
Baostock 股票数据 Mixin

股票基本信息、日线同步/下载、标准接口取数与入库。
"""

from collections.abc import Callable
from typing import Any, List, Tuple

import baostock as bs
import polars as pl
from loguru import logger

from src.data.sources import baostock_schema as schema
from src.utils.code_converter import to_baostock_code, to_ts_code


class BaostockStockMixin:
    """Baostock 股票数据同步与标准接口实现。"""

    session: Any
    config: Any
    db_manager: Any

    _ensure_baostock_login: Callable[..., bool]
    _get_default_codes: Callable[..., list]
    _process_dates: Callable[..., tuple]
    _normalize_kline: Callable[..., pl.DataFrame]

    def update_stock_basic(self):
        """
        更新股票基本信息
        """
        try:
            if not self._ensure_baostock_login():
                logger.warning("Baostock登录失败，无法获取股票基本信息")
                return

            logger.info("开始从Baostock获取股票基本信息")

            # 使用Baostock获取股票基本信息，移除不支持的fields参数
            rs = bs.query_stock_basic(code_name="")

            # 转换为DataFrame
            stock_basic_pd = schema.result_to_frame(rs)

            # 转换为Polars DataFrame
            stock_basic_df = pl.from_pandas(stock_basic_pd)

            if stock_basic_df.is_empty():
                logger.warning("从Baostock获取的股票基本信息为空")
                return

            logger.info(f"从Baostock获取到{stock_basic_df.height}条股票基本信息")

            # 打印前几行数据，了解数据结构
            logger.debug(f"Baostock返回的数据结构: {stock_basic_df.columns}")
            if stock_basic_df.height > 0:
                logger.debug(f"前5行数据: {stock_basic_df.head(5)}")

            # stock_basic 唯一写入方是同步流程：本 handler 只返回数据，不写库
            if self.session:
                logger.info(
                    "BaostockHandler.update_stock_basic 只返回数据，stock_basic 由同步流程写入"
                )
            return stock_basic_df  # 返回Polars DataFrame

        except (ConnectionError, TimeoutError, OSError) as e:
            if self.session:
                try:
                    self.session.rollback()
                except (ConnectionError, TimeoutError, OSError) as rollback_e:
                    logger.warning(f"回滚事务失败: {rollback_e}")
            logger.exception(f"从Baostock获取股票基本信息失败: {e}")
            raise

    def update_stock_daily(self, ts_codes: List[str] | None = None, start_date: str | None = None, end_date: str | None = None, adjustflag: str = "3"):
        """
        更新股票日线数据

        Args:
            ts_codes: 股票代码列表，None表示更新所有股票
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            adjustflag: 复权类型，"2"=前复权，"1"=后复权，"3"=不复权。
                默认 "3"（不复权）：写库统一存原始价，复权因子由
                AdjFactorCalculator 单独写入，查询侧由 DataFetcher 统一复权，
                避免前复权价入库后被二次复权。

        Returns:
            dict: 股票代码到日线数据的映射（离线模式下）
        """
        try:
            if not self._ensure_baostock_login():
                logger.warning("Baostock登录失败，无法获取股票日线数据")
                return

            ts_codes = self._resolve_stock_daily_codes(ts_codes)

            if not ts_codes:
                logger.warning("没有需要更新的股票代码")
                return

            # 处理日期参数
            start_date, end_date = self._process_dates(start_date, end_date, default_days=self.config.data.default_days)

            logger.info(f"开始更新{len(ts_codes)}只股票的日线数据，时间范围: {start_date} 至 {end_date}")

            # 存储结果（离线模式下返回）
            result: dict[str, pl.DataFrame] = {}

            # 遍历股票代码，获取日线数据
            for ts_code in ts_codes:
                try:
                    self._sync_stock_daily_one(
                        ts_code, start_date, end_date, adjustflag, result)

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
            logger.exception(f"更新股票日线数据失败: {e}")
            raise

    def _resolve_stock_daily_codes(self, ts_codes: List[str] | None = None):
        """解析待更新的股票代码：未指定时回落到数据库或离线默认列表"""
        # 如果没有指定股票代码，从数据库获取所有股票代码
        if ts_codes:
            return ts_codes
        if self.session:
            from src.database.models.stock import StockBasic
            stock_basics = self.session.query(StockBasic.ts_code).all()
            # 保持标准 ts_code 格式（600000.SH），查询时再转换为 baostock 格式
            return [sb.ts_code for sb in stock_basics]
        # 离线模式下使用默认股票代码
        codes = self._get_default_codes('stock')
        logger.info(f"离线模式下，使用默认股票代码列表: {codes}")
        return codes

    @staticmethod
    def _build_stock_daily_rows(stock_daily_df, std_code):
        """转换为待写入行（不合格行跳过），并返回最早交易日期"""
        # 初始化最早交易日期
        earliest_date = None
        daily_rows = []

        # 遍历数据，转换为待写入行（不合格行跳过）
        for row in stock_daily_df.iter_rows(named=True):
            try:
                record = schema.daily_row_to_record(row, std_code)
                trade_date = record['trade_date']

                # 更新最早交易日期
                if earliest_date is None or trade_date < earliest_date:
                    earliest_date = trade_date

                daily_rows.append(record)
            except (ConnectionError, TimeoutError, OSError) as row_e:
                logger.exception(f"处理股票日线数据失败: {row_e}")
                continue

        return daily_rows, earliest_date

    def _sync_stock_daily_one(self, ts_code, start_date, end_date, adjustflag, result):
        """同步单只股票的日线数据（离线模式仅收集结果）"""
        logger.info(f"正在获取{ts_code}的日线数据")

        # 统一转换：写库用标准 ts_code，API 查询用 baostock 格式
        std_code = to_ts_code(ts_code)
        bs_code = to_baostock_code(ts_code)

        # 使用Baostock获取股票日线数据
        rs = bs.query_history_k_data_plus(
            bs_code,
            schema.DAILY_FIELDS,
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag=adjustflag  # 使用传入的复权参数
        )

        # 检查rs是否为None
        if rs is None:
            logger.warning(f"Baostock API返回None，无法获取{ts_code}的日线数据")
            return

        # 获取数据
        try:
            stock_daily_pd = schema.result_to_frame(rs)
        # 兜底理由：单只股票取数失败跳过，不中断批量
        except Exception as e:  # noqa: BLE001
            logger.warning(f"获取{ts_code}数据失败: {e}")
            return

        # 转换为Polars DataFrame
        stock_daily_df = pl.from_pandas(stock_daily_pd)

        if stock_daily_df.is_empty():
            logger.warning(f"{ts_code}在{start_date}至{end_date}期间没有日线数据")
            return

        logger.info(f"获取到{ts_code}的{stock_daily_df.height}条日线数据")

        # 离线模式下，只获取数据不存储
        if not self.session:
            result[std_code] = stock_daily_df
            logger.info(f"离线模式下，获取{std_code}的日线数据完成")
            return

        daily_rows, earliest_date = self._build_stock_daily_rows(
            stock_daily_df, std_code)

        # 跨表写（stock_daily + stock_basic）必须同一事务提交
        self._persist_stock_daily(std_code, daily_rows, earliest_date)

        logger.info(f"{ts_code}的日线数据更新完成")

    def download_stock_daily(self, ts_codes: List[str] | None = None, start_date: str | None = None, end_date: str | None = None):
        """
        下载股票日线数据

        Args:
            ts_codes: 股票代码列表，None表示下载所有股票
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD

        Returns:
            dict: 股票代码到日线数据的映射
        """
        return self.update_stock_daily(ts_codes, start_date, end_date)

    # ------------------------------------------------------------------
    # 标准 DataSource 接口（统一列名/单位/复权口径，供 DataFetcher 调用）
    # ------------------------------------------------------------------

    def _to_baostock_code(self, ts_code: str) -> str:
        """将标准 ts_code 转换为 baostock 格式（委托统一转换工具）"""
        return to_baostock_code(ts_code)

    def get_stock_data(self, ts_code: str, start_date: str, end_date: str,
                       freq: str = 'daily', adjustment_type: str = 'hfq'):
        """
        标准接口：获取股票日线数据，返回统一格式（原始价 + 复权因子）。

        复权口径：baostock 侧获取不复权原始价(adjustflag=3) + 复权因子，
        由 DataFetcher._apply_adjustment 统一计算复权价，避免各源复权口径不一致。

        Returns:
            pl.DataFrame: 归一化后的K线数据
        """
        try:
            if not self._ensure_baostock_login():
                logger.warning("Baostock登录失败，无法获取股票数据")
                return pl.DataFrame()

            bs_code = self._to_baostock_code(ts_code)
            start_date, end_date = self._process_dates(start_date, end_date,
                                                       default_days=self.config.data.default_days)

            # 获取不复权原始价格（adjustflag=3）
            rs = bs.query_history_k_data_plus(
                bs_code,
                schema.DAILY_FIELDS_BASIC,
                start_date=start_date, end_date=end_date,
                frequency="d", adjustflag="3"
            )
            if rs is None:
                return pl.DataFrame()

            raw = schema.result_to_frame(rs)
            if raw is None or raw.empty:
                return pl.DataFrame()

            df = pl.from_pandas(raw)

            # 获取逐日累计复权因子（前复权/后复权）
            qfq_factor, hfq_factor = self._fetch_baostock_factors(
                bs_code, start_date, end_date, df)

            if qfq_factor is not None and not qfq_factor.is_empty():
                df = df.join(qfq_factor, on='date', how='left')
            if hfq_factor is not None and not hfq_factor.is_empty():
                if 'hfq_factor' in df.columns:
                    df = df.drop('hfq_factor')
                df = df.join(hfq_factor, on='date', how='left')

            # 归一化：列名映射 + 成交量(股→手) + 日期格式 + 复权因子补齐
            column_map = schema.BAOSTOCK_KLINE_COLUMN_MAP
            return self._normalize_kline(
                df, column_map=column_map, ts_code=ts_code,
                volume_unit='share', date_format='%Y-%m-%d', include_factors=True,
            )

        # 兜底理由：第三方 SDK 异常类型不可预知，失败返回空表
        except Exception as e:  # noqa: BLE001
            logger.warning(f"baostock get_stock_data({ts_code}) 失败: {e}")
            return pl.DataFrame()

    def _fetch_baostock_factors(self, bs_code: str, start_date: str, end_date: str,
                                raw_df: pl.DataFrame
                                ) -> Tuple[pl.DataFrame | None, pl.DataFrame | None]:
        """
        获取逐日累计复权因子（qfq_factor, hfq_factor）。

        原实现把 query_adjust_factor 的"除权事件表"直接 left join 到日线，
        只有除权日当天能匹配到因子，其余交易日缺失并被 _normalize_kline
        补齐为 1.0。这样"原始价 × 因子"只在除权日跳变、平日保持原始价，
        价格序列断裂。

        改为用后复权价 / 原始价逐日求累计因子：
            hfq_factor_t = close_hfq_t / close_raw_t = F_t
            qfq_factor_t = F_t / F_T   （T 为本区间最后一根）

        baostock 的 adjustflag=1（后复权）以上市日为锚，F_t 与查询区间无关，
        可直接落库；qfq 以区间末为锚，仅用于展示，读侧由 as_of 截断。
        """
        rs = bs.query_history_k_data_plus(
            bs_code, "date,close", start_date=start_date, end_date=end_date,
            frequency="d", adjustflag="1",
        )
        if rs is None:
            return None, None
        hfq_raw = schema.result_to_frame(rs)
        if hfq_raw is None or hfq_raw.empty:
            return None, None

        hfq_df = pl.from_pandas(hfq_raw).select([
            pl.col('date'),
            pl.col('close').cast(pl.Float64).alias('close_hfq'),
        ])
        return schema.compute_daily_factors(raw_df, hfq_df)

    def _persist_stock_daily(self, std_code, daily_rows, earliest_date):
        """同一事务内写入股票日线，并回填上市日期（跨表写入收敛到单一 commit 边界）"""
        from src.database.models.stock import StockBasic, StockDaily
        from src.database.upsert import upsert_rows

        with self.db_manager.transaction():
            # 批量查重后写入（已存在则更新，不存在则新增）
            upsert_rows(
                self.session, StockDaily, daily_rows,
                key_fields=('ts_code', 'trade_date'),
                value_fields=('open', 'high', 'low', 'close', 'pre_close',
                              'change', 'pct_chg', 'vol', 'amount'),
            )

            # 更新股票的上市日期为最早交易日期
            if earliest_date:
                # 查询股票基本信息
                stock_basic = self.session.query(StockBasic).filter_by(ts_code=std_code).first()
                if stock_basic:
                    # 如果还没有上市日期，或者找到更早的日期，则更新
                    if not stock_basic.list_date or earliest_date < stock_basic.list_date:
                        stock_basic.list_date = earliest_date
                        logger.info(f"更新股票{std_code}的上市日期为: {earliest_date}")
                else:
                    # stock_basic 唯一写入方是同步流程：名称拿不到就跳过，
                    # 不能把代码伪装成名称写库
                    logger.warning(
                        f"股票{std_code}的基本信息不存在，跳过上市日期回填；"
                        "stock_basic 由同步流程写入"
                    )
