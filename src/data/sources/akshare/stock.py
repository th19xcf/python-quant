#!/usr/bin/env python3

"""
AkShare 股票数据 Mixin

股票基本信息、市场宽度、日线同步与标准接口取数。
"""

from collections.abc import Callable
from datetime import datetime
from typing import Any, List

import polars as pl
from loguru import logger

from src.data.sources import akshare_schema as schema
from src.data.sources.akshare.constants import _DEFAULT_STOCK_CODES
from src.utils.code_converter import to_symbol, to_ts_code


class AkShareStockMixin:
    """AkShare 股票数据同步与标准接口实现。"""

    session: Any
    config: Any
    db_manager: Any
    _factor_warning_logged: bool

    _normalize_date_str: Callable[..., str]
    _normalize_kline: Callable[..., pl.DataFrame]
    _process_dates: Callable[..., tuple]

    def update_stock_basic(self):
        """
        更新股票基本信息
        """
        try:
            logger.info("开始从AkShare获取股票基本信息")

            # 使用AkShare获取股票基本信息（上游契约见 akshare_schema.STOCK_BASIC）
            stock_basic_df = schema.resolve(
                schema.fetch_spec(schema.STOCK_BASIC), schema.STOCK_BASIC)

            if stock_basic_df.is_empty():
                logger.warning("从AkShare获取的股票基本信息为空")
                return

            logger.info(f"从AkShare获取到{stock_basic_df.height}条股票基本信息")

            # 离线模式下，只获取数据不存储
            if not self.session:
                logger.info("离线模式下，跳过股票基本信息存储")
                return stock_basic_df  # 离线模式下返回Polars DataFrame

            # 数据清洗和标准化
            from src.database.models.stock import StockBasic

            # 遍历数据，进行清洗和存储
            for row in stock_basic_df.iter_rows(named=True):
                try:
                    # 提取股票代码和名称
                    symbol = row['symbol']
                    name = row['name']

                    # 构建完整的ts_code（统一市场推断，含北交所 4/8/92 开头）
                    ts_code = to_ts_code(symbol)

                    # 查询股票是否已存在
                    stock = self.session.query(StockBasic).filter_by(ts_code=ts_code).first()

                    if stock:
                        # 更新现有股票信息
                        stock.name = name[:45] if len(name) > 45 else name
                        stock.symbol = symbol[:9] if len(symbol) > 9 else symbol
                    else:
                        # 创建新股票信息
                        stock = StockBasic(
                            ts_code=ts_code,
                            symbol=symbol[:9] if len(symbol) > 9 else symbol,
                            name=name[:45] if len(name) > 45 else name
                        )
                        self.session.add(stock)

                except (ConnectionError, TimeoutError, OSError) as row_e:
                    logger.exception(f"处理股票基本信息失败: {row_e}")
                    continue

            # 提交事务
            self.session.commit()
            logger.info("股票基本信息更新完成")
            return stock_basic_df  # 返回Polars DataFrame

        except (ConnectionError, TimeoutError, OSError) as e:
            if self.session:
                self.session.rollback()
            logger.exception(f"从AkShare获取股票基本信息失败: {e}")
            raise

    def update_market_breadth(self, trade_date: str = None):
        """
        更新市场涨跌家数数据

        Args:
            trade_date: 交易日期，格式YYYYMMDD，默认获取最新数据
        """
        try:
            logger.info("开始获取市场涨跌家数数据")

            # 使用AkShare获取A股实时行情数据（契约见 akshare_schema.MARKET_BREADTH）
            df = schema.resolve(
                schema.fetch_spec(schema.MARKET_BREADTH), schema.MARKET_BREADTH)

            if df.is_empty():
                logger.warning("获取市场涨跌家数数据为空")
                return None

            # 统计涨跌家数并计算涨跌比例
            stats = self._calc_market_breadth(df)

            # 转换日期格式
            trade_date_obj = self._resolve_breadth_date(trade_date)

            # 离线模式下返回结果
            if not self.session:
                return self._market_breadth_result(trade_date_obj, stats)

            self._save_market_breadth(trade_date_obj, stats)
            return self._market_breadth_result(trade_date_obj, stats)

        except (ConnectionError, TimeoutError, OSError) as e:
            if self.session:
                self.session.rollback()
            logger.exception(f"获取市场涨跌家数数据失败: {e}")
            raise

    @staticmethod
    def _calc_market_breadth(df):
        """统计涨跌家数并计算涨跌比例"""
        up_count = df.filter(pl.col('pct_chg') > 0).height
        down_count = df.filter(pl.col('pct_chg') < 0).height
        flat_count = df.filter(pl.col('pct_chg') == 0).height
        total_count = df.height

        # 计算涨跌比例
        up_rate = (up_count / total_count * 100) if total_count > 0 else 0
        down_rate = (down_count / total_count * 100) if total_count > 0 else 0

        logger.info(f"市场涨跌家数: 上涨={up_count}, 下跌={down_count}, 平盘={flat_count}, 总计={total_count}")

        return {
            'up_count': up_count,
            'down_count': down_count,
            'flat_count': flat_count,
            'total_count': total_count,
            'up_rate': up_rate,
            'down_rate': down_rate,
        }

    @staticmethod
    def _resolve_breadth_date(trade_date: str = None):
        """转换日期格式：YYYYMMDD -> date，未指定时取当天"""
        if trade_date:
            return datetime.strptime(trade_date, "%Y%m%d").date()
        return datetime.now().date()

    @staticmethod
    def _market_breadth_result(trade_date_obj, stats):
        """构造返回结果（字段顺序保持与原接口一致）"""
        return {
            'trade_date': trade_date_obj,
            'up_count': stats['up_count'],
            'down_count': stats['down_count'],
            'flat_count': stats['flat_count'],
            'total_count': stats['total_count'],
            'up_rate': stats['up_rate'],
            'down_rate': stats['down_rate'],
        }

    def _save_market_breadth(self, trade_date_obj, stats):
        """写入市场涨跌家数（已存在则更新）并提交事务"""
        # 存储到数据库
        from src.database.models.stock import MarketBreadth

        # 查询是否已存在
        existing = self.session.query(MarketBreadth).filter_by(trade_date=trade_date_obj).first()

        if existing:
            existing.up_count = stats['up_count']
            existing.down_count = stats['down_count']
            existing.flat_count = stats['flat_count']
            existing.total_count = stats['total_count']
            existing.up_rate = stats['up_rate']
            existing.down_rate = stats['down_rate']
            logger.info(f"更新市场涨跌家数数据: {trade_date_obj}")
        else:
            market_breadth = MarketBreadth(
                trade_date=trade_date_obj,
                up_count=stats['up_count'],
                down_count=stats['down_count'],
                flat_count=stats['flat_count'],
                total_count=stats['total_count'],
                up_rate=stats['up_rate'],
                down_rate=stats['down_rate'],
            )
            self.session.add(market_breadth)
            logger.info(f"新增市场涨跌家数数据: {trade_date_obj}")

        self.session.commit()

    def update_stock_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新股票日线数据

        Args:
            ts_codes: 股票代码列表，None表示更新所有股票
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD

        Returns:
            dict: 股票代码到日线数据的映射（离线模式下）
        """
        try:
            ts_codes = self._resolve_stock_daily_codes(ts_codes)

            if not ts_codes:
                logger.warning("没有需要更新的股票代码")
                return

            # 未指定日期时，默认更新最近 30 天（akshare 接口用 YYYYMMDD）
            start_date, end_date = self._process_dates(
                start_date, end_date, default_days=30, date_format="%Y%m%d")

            logger.info(f"开始更新{len(ts_codes)}只股票的日线数据，时间范围: {start_date} 至 {end_date}")

            # 存储结果（离线模式下返回）
            result = {}

            # 遍历股票代码，获取日线数据
            for ts_code in ts_codes:
                try:
                    self._sync_stock_daily_one(ts_code, start_date, end_date, result)

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

    def _resolve_stock_daily_codes(self, ts_codes: List[str] = None):
        """解析待更新的股票代码：未指定时回落到数据库或离线默认列表"""
        # 如果没有指定股票代码，从数据库获取所有股票代码
        if ts_codes:
            return ts_codes
        if self.session:
            from src.database.models.stock import StockBasic
            stock_basics = self.session.query(StockBasic.ts_code).all()
            return [sb.ts_code for sb in stock_basics]
        # 离线模式下使用默认股票代码
        logger.info(f"离线模式下，使用默认股票代码列表: {_DEFAULT_STOCK_CODES}")
        return _DEFAULT_STOCK_CODES

    @staticmethod
    def _build_akshare_daily_rows(stock_daily_df, ts_code):
        """遍历原始行，转换为待写入行（不合格行跳过）"""
        daily_rows = []
        for row in stock_daily_df.iter_rows(named=True):
            try:
                daily_rows.append({
                    'ts_code': ts_code,
                    'trade_date': datetime.strptime(row['date'], "%Y-%m-%d").date(),
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close'],
                    'pre_close': row['pre_close'],
                    'change': row['change'],
                    'pct_chg': row['pct_chg'],
                    'vol': row['volume'],
                    'amount': row['amount'],
                })
            except (ConnectionError, TimeoutError, OSError) as row_e:
                logger.exception(f"处理股票日线数据失败: {row_e}")
                continue
        return daily_rows

    def _persist_akshare_daily(self, ts_code, daily_rows):
        """批量查重写入并提交（每只股票一次事务）"""
        # 数据清洗和标准化
        from src.database.models.stock import StockDaily
        from src.database.upsert import upsert_rows

        # 批量查重后写入（已存在则更新，不存在则新增）
        upsert_rows(
            self.session, StockDaily, daily_rows,
            key_fields=('ts_code', 'trade_date'),
            value_fields=('open', 'high', 'low', 'close', 'pre_close',
                          'change', 'pct_chg', 'vol', 'amount'),
        )

        # 每只股票提交一次事务
        self.session.commit()
        logger.info(f"{ts_code}的日线数据更新完成")

    def _sync_stock_daily_one(self, ts_code, start_date, end_date, result):
        """同步单只股票的日线数据（离线模式仅收集结果）"""
        logger.info(f"正在获取{ts_code}的日线数据")

        # 使用AkShare获取股票日线数据（契约见 akshare_schema.STOCK_HIST）
        stock_daily_df = schema.resolve(
            schema.fetch_spec(
                schema.STOCK_HIST,
                symbol=to_symbol(ts_code),  # AkShare使用数字代码
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="",  # 不复权：写库统一存原始价，复权由查询侧 DataFetcher 按因子计算
            ),
            schema.STOCK_HIST,
        )

        if stock_daily_df.is_empty():
            logger.warning(f"{ts_code}在{start_date}至{end_date}期间没有日线数据")
            return

        logger.info(f"获取到{ts_code}的{stock_daily_df.height}条日线数据")

        # 离线模式下，只获取数据不存储
        if not self.session:
            result[ts_code] = stock_daily_df
            logger.info(f"离线模式下，获取{ts_code}的日线数据完成")
            return

        daily_rows = self._build_akshare_daily_rows(stock_daily_df, ts_code)
        self._persist_akshare_daily(ts_code, daily_rows)

    def get_stock_data(self, ts_code: str, start_date: str, end_date: str,
                       freq: str = 'daily', adjustment_type: str = 'qfq'):
        """
        标准接口：获取股票日线数据，返回统一格式（原始价 + 复权因子）。

        akshare 的复权在 API 侧完成，无独立复权因子接口。
        这里获取不复权原始价(adjust="")，复权因子由 _normalize_kline 补齐 1.0，
        复权计算交由 DataFetcher._apply_adjustment 统一处理。

        Returns:
            pl.DataFrame: 归一化后的K线数据
        """
        try:
            symbol = to_symbol(ts_code)
            sd = self._normalize_date_str(start_date) if start_date else None
            ed = self._normalize_date_str(end_date) if end_date else None

            # 获取不复权原始价格
            df = schema.fetch_spec(
                schema.STOCK_HIST, symbol=symbol, period="daily",
                start_date=sd, end_date=ed, adjust="",
            )
            if df.is_empty():
                return pl.DataFrame()

            # 列名/单位/日期格式均取自上游契约，akshare 成交量单位为"手"
            normalized = self._normalize_kline(
                df, column_map=dict(schema.STOCK_HIST.column_map), ts_code=ts_code,
                volume_unit=schema.STOCK_HIST.volume_unit,
                date_format=schema.STOCK_HIST.date_format, include_factors=True,
            )

            # akshare 无复权因子接口，normalize 只能补 1.0；
            # 此时复权请求不会生效，显式告警避免上游误以为拿到的是复权价
            if adjustment_type in ('qfq', 'hfq') and not self._factor_warning_logged:
                self._factor_warning_logged = True
                logger.warning(
                    "akshare 数据源无真实复权因子（qfq/hfq_factor 均为 1.0），"
                    f"adjustment_type={adjustment_type} 请求将返回未复权价；"
                    "如需复权请改用 baostock 或 tdx 数据源")
            return normalized

        # 兜底理由：第三方接口异常类型不可预知，失败返回空表
        except Exception as e:  # noqa: BLE001
            logger.warning(f"akshare get_stock_data({ts_code}) 失败: {e}")
            return pl.DataFrame()
