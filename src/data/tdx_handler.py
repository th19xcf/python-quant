#!/usr/bin/env python3

"""
通达信数据处理器
"""

import threading
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, cast

import polars as pl
from loguru import logger

from src.data.sources.base import DataSourceBase
from src.data.tdx_day_parser import parse_day_file as parse_tdx_day_file
from src.data.tdx_day_parser import read_recent_records
from src.utils.code_converter import (
    parse_code,
    to_symbol,
    to_tdx_market,
)


def _is_valid_tdx_stock_code(code) -> bool:
    """验证通达信文件名中的代码是否有效。"""
    if len(code) != 6:
        return False
    if not code.isdigit():
        return False
    # 忽略 B 股：900 开头（沪市 B 股）、200 开头（深市 B 股）
    if code.startswith('900') or code.startswith('200'):
        return False
    # 沪市指数：000001-000999
    if code.startswith('000'):
        return True
    # 沪市股票：600000-603999, 688000-688999
    elif code.startswith('6'):
        num = int(code)
        return (600000 <= num <= 603999) or (688000 <= num <= 688999)
    # 深市指数：399001-399999
    elif code.startswith('399'):
        return True
    # 深市股票：000000-002999, 300000-300999
    elif code.startswith('0') or code.startswith('3'):
        num = int(code)
        return (0 <= num <= 2999) or (300000 <= num <= 300999)
    # 北交所：800000-899999（原新三板精选层）
    elif code.startswith('8'):
        num = int(code)
        return 800000 <= num <= 899999
    # 北交所新股：920000-920999
    elif code.startswith('92'):
        num = int(code)
        return 920000 <= num <= 920999
    # 上海 ETF：510xxx, 511xxx, 512xxx, 513xxx, 515xxx, 588xxx
    elif code.startswith('51') or code.startswith('58') or code.startswith('15') or code.startswith('16'):
        return True
    return False


def _collect_tdx_market_codes(stock_codes, lday_dir, prefix, suffix, market_label):
    """扫描单个市场目录，把有效代码追加到 stock_codes 并记录数量日志。"""
    files = list(lday_dir.glob(f'{prefix}*.day'))
    for f in files:
        code = f.stem[2:]
        if _is_valid_tdx_stock_code(code):
            stock_codes.append(f"{code}.{suffix}")
    logger.info(f"从{market_label}通达信数据中找到 {len([c for c in stock_codes if c.endswith(f'.{suffix}')])} 个有效股票文件")


class TdxHandler(DataSourceBase):
    """
    通达信数据处理器，负责解析通达信数据文件并存储到数据库

    继承 DataSourceBase 获得统一接口；DataFetcher 通过标准接口
    get_stock_data / get_index_data 调度。get_kline_data 为本地文件级
    底层方法，仍供内部与增量同步使用。提供 close() 方法（基类没有）。
    """

    def __init__(self, config, db_manager):
        """
        初始化通达信数据处理器

        Args:
            config: 配置对象
            db_manager: 数据库管理器实例
        """
        self.config = config
        self.db_manager = db_manager
        self.name = "TdxHandler"
        self.is_local = True  # 本地文件源，优先于远程源
        self.tdx_data_path = Path(config.data.tdx_data_path)

        # 检查通达信数据路径是否存在；路径缺失时标记为不可用
        self.available = self.tdx_data_path.exists()
        if not self.available:
            logger.warning(f"通达信数据路径不存在: {self.tdx_data_path}")

        # 线程本地存储，用于保存每个线程的数据库会话
        self.thread_local = threading.local()

        # 离线模式支持
        self.offline_mode = db_manager is None

    def __del__(self):
        """
        析构函数，确保线程池正确关闭
        """
        self.close()

    def close(self):
        """
        关闭处理器，清理资源
        """
        try:
            # 关闭线程本地存储的数据库会话
            if hasattr(self.thread_local, 'session') and self.thread_local.session:
                try:
                    self.thread_local.session.close()
                    logger.debug(f"线程 {threading.current_thread().name} 的数据库会话已关闭")
                except OSError as e:
                    logger.warning(f"关闭线程数据库会话时出错: {e}")
        except OSError as e:
            logger.exception(f"关闭TdxHandler时出错: {e}")

    def _get_thread_safe_session(self):
        """
        获取线程安全的数据库会话

        Returns:
            session: 当前线程的数据库会话
        """
        if not hasattr(self.thread_local, 'session') or not self.thread_local.session:
            if self.db_manager:
                try:
                    self.thread_local.session = self.db_manager.get_session()
                    logger.debug(f"线程 {threading.current_thread().name} 获取新的数据库会话")
                except (OSError, RuntimeError) as e:
                    logger.warning(f"线程 {threading.current_thread().name} 获取数据库会话失败: {e}")
                    self.thread_local.session = None
            else:
                self.thread_local.session = None
        return self.thread_local.session

    def get_stock_list(self) -> List[str]:  # noqa: C901
        """
        从通达信数据源获取所有股票代码列表

        Returns:
            List[str]: 股票代码列表 (格式: 600000.SH, 000001.SZ 等)
        """
        stock_codes: List[str] = []

        try:
            if not self.tdx_data_path.exists():
                logger.warning(f"通达信数据路径不存在: {self.tdx_data_path}")
                return stock_codes

            sh_lday = self.tdx_data_path / 'sh' / 'lday'
            sz_lday = self.tdx_data_path / 'sz' / 'lday'
            bj_lday = self.tdx_data_path / 'bj' / 'lday'

            if sh_lday.exists():
                _collect_tdx_market_codes(stock_codes, sh_lday, 'sh', 'SH', '沪市')

            if sz_lday.exists():
                _collect_tdx_market_codes(stock_codes, sz_lday, 'sz', 'SZ', '深市')

            # 北市（科创板）
            if bj_lday.exists():
                _collect_tdx_market_codes(stock_codes, bj_lday, 'bj', 'BJ', '北市')

            logger.info(f"通达信股票代码列表获取完成，共 {len(stock_codes)} 只有效股票")

        # 兜底理由：本地文件扫描异常不可预知，降级为空代码列表
        except Exception as e:  # noqa: BLE001
            logger.exception(f"获取通达信股票代码列表失败: {e}")

        return stock_codes

    def parse_day_file(self, file_path: Path, max_days: int | None = None):
        """
        解析通达信日线数据文件

        Args:
            file_path: 日线数据文件路径
            max_days: 最大天数，只解析最近的max_days天数据，None表示解析所有数据

        Returns:
            polars.DataFrame: 解析后的日线数据
        """
        try:
            logger.info(f"开始解析通达信日线数据文件: {file_path}，max_days: {max_days}")
            df = parse_tdx_day_file(file_path, max_days)
            logger.info(f"成功解析通达信日线数据文件: {file_path}，获取{len(df)}条数据")
            return df
        except OSError as e:
            logger.exception(f"解析通达信日线数据文件失败: {e}")
            raise

    def _get_adj_factors(self, ts_code: str, start_date: date | None, end_date: date | None) -> pl.DataFrame | None:
        """
        从数据库获取复权因子
        如果没有预计算的复权因子，则根据分红数据实时计算

        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            Polars DataFrame包含复权因子
        """
        try:
            from src.database.models.stock import StockAdjFactor

            session = self._get_thread_safe_session()
            if not session:
                logger.warning("无法获取数据库会话，无法获取复权因子")
                return None

            # 首先尝试获取预计算的复权因子
            query = session.query(StockAdjFactor).filter_by(ts_code=ts_code)

            if start_date:
                query = query.filter(StockAdjFactor.trade_date >= start_date)
            if end_date:
                query = query.filter(StockAdjFactor.trade_date <= end_date)

            factors = query.order_by(StockAdjFactor.trade_date).all()

            if factors:
                # 使用预计算的复权因子
                logger.debug(f"{ts_code} 使用预计算的复权因子数据")
                data = {
                    'trade_date': [f.trade_date for f in factors],
                    'qfq_factor': [f.qfq_factor for f in factors],
                    'hfq_factor': [f.hfq_factor for f in factors],
                }
                return pl.DataFrame(data)

            # 如果没有预计算的复权因子，根据分红数据实时计算
            logger.info(f"{ts_code} 没有预计算的复权因子，根据分红数据实时计算")
            return self._calculate_adj_factors_from_dividends(ts_code, start_date, end_date)

        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"获取复权因子失败: {e}")
            return None

    def _load_dividend_records(self, ts_code: str):
        """读取该股票全部历史分红记录（无会话或无数据返回 None）。"""
        from src.database.models.stock import StockDividend

        session = self._get_thread_safe_session()
        if not session:
            return None

        # 获取分红数据（获取所有历史分红数据，不限制日期范围）
        query = session.query(StockDividend).filter_by(ts_code=ts_code)
        query = query.filter(StockDividend.ex_date is not None)

        # 注意：这里不限制日期范围，因为复权因子计算需要所有历史分红数据
        # 即使当前显示的K线范围内没有分红，历史分红也会影响当前价格的复权计算

        dividends = query.order_by(StockDividend.ex_date).all()

        if not dividends:
            logger.debug(f"{ts_code} 没有找到分红数据")
            return None

        logger.info(f"{ts_code} 找到 {len(dividends)} 条分红记录，开始计算复权因子")

        # 打印第一条和最后一条分红记录用于调试
        first_div = dividends[0]
        last_div = dividends[-1]
        logger.info(f"第一条分红: 日期={first_div.ex_date}, 现金={first_div.cash_div}, 送转={first_div.share_div}")
        logger.info(f"最后一条分红: 日期={last_div.ex_date}, 现金={last_div.cash_div}, 送转={last_div.share_div}")
        return dividends

    def _load_adj_price_records(self, ts_code: str):
        """读取参与复权因子计算的收盘价记录（文件缺失或无数据返回 None）。"""
        # 获取价格数据用于计算复权因子
        symbol = to_symbol(ts_code)
        market = to_tdx_market(ts_code)
        file_name = f"{market}{symbol}.day"
        file_path = self.tdx_data_path / market / "lday" / file_name

        if not file_path.exists():
            logger.warning(f"价格数据文件不存在: {file_path}")
            return None

        # 读取价格数据（复用统一的 .day 解析器，只需日期与收盘价）
        prices = [
            {'trade_date': r['date'], 'close': r['close']}
            for r in read_recent_records(file_path)
        ]

        if not prices:
            return None

        # 按日期排序
        prices.sort(key=lambda x: x['trade_date'])
        return prices

    def _compute_adj_factor_frame(self, ts_code: str, prices, dividends) -> pl.DataFrame | None:
        """用统一复权计算器把价格与分红适配后计算复权因子。"""
        # 复权因子计算统一收口到 AdjFactorCalculator.calculate_factors
        # （向量化实现 + 含配股的统一公式），此处仅做数据适配
        from src.data.adj_factor_calculator import AdjFactorCalculator

        prices_df = pl.DataFrame({
            'ts_code': [ts_code] * len(prices),
            'trade_date': [p['trade_date'] for p in prices],
            'close': [p['close'] for p in prices],
        })
        dividends_df = pl.DataFrame({
            'ts_code': [d.ts_code for d in dividends],
            'ex_date': [d.ex_date for d in dividends],
            'cash_div': [d.cash_div or 0 for d in dividends],
            'share_div': [d.share_div or 0 for d in dividends],
            'rights_issue_ratio': [d.rights_issue_ratio or 0 for d in dividends],
            'rights_issue_price': [d.rights_issue_price or 0 for d in dividends],
        })

        result = AdjFactorCalculator.calculate_factors(prices_df, dividends_df)
        if result is None or result.is_empty():
            return None

        df = result.select(['trade_date', 'qfq_factor', 'hfq_factor'])

        # 打印复权因子统计信息用于调试
        if len(df) > 0:
            min_qfq = cast(float, df['qfq_factor'].min())
            max_qfq = cast(float, df['qfq_factor'].max())
            min_hfq = cast(float, df['hfq_factor'].min())
            max_hfq = cast(float, df['hfq_factor'].max())
            logger.info(f"{ts_code} 复权因子统计: 前复权=[{min_qfq:.6f}, {max_qfq:.6f}], 后复权=[{min_hfq:.6f}, {max_hfq:.6f}]")

        logger.info(f"{ts_code} 复权因子计算完成，共 {len(df)} 条记录")
        return df

    def _calculate_adj_factors_from_dividends(self, ts_code: str, start_date: date | None, end_date: date | None) -> pl.DataFrame | None:
        """
        根据分红数据实时计算复权因子

        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            Polars DataFrame包含复权因子
        """
        try:
            dividends = self._load_dividend_records(ts_code)
            if not dividends:
                return None

            prices = self._load_adj_price_records(ts_code)
            if not prices:
                return None

            return self._compute_adj_factor_frame(ts_code, prices, dividends)

        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"根据分红数据计算复权因子失败: {e}")
            return None

    def _resolve_kline_path(self, stock_code: str):
        """解析股票代码并定位 .day 文件；代码非法或文件缺失返回 None。"""
        # 统一代码解析（支持 600000.SH / sh.600000 / sh600000 / 600000）
        try:
            code, market_upper = parse_code(stock_code)
            market = market_upper.lower()
        except ValueError:
            logger.warning(f"无效的股票代码格式: {stock_code}")
            return None

        logger.info(f"解析后的股票代码: {code}，市场: {market}")

        # 构建通达信数据文件名
        file_name = f"{market}{code}.day"

        # 构建数据文件路径
        lday_path = self.tdx_data_path / market / "lday"
        file_path = lday_path / file_name

        logger.info(f"尝试从路径 {file_path} 读取数据")

        # 检查文件是否存在
        if not file_path.exists():
            logger.warning(f"股票 {stock_code} 的通达信数据文件不存在: {file_path}")
            return None

        return code, market, file_path

    def _read_kline_records(self, file_path):
        """解析 .day 文件；文件读取失败返回 None。"""
        logger.info(f"开始解析文件 {file_path}")
        try:
            data = read_recent_records(file_path)
        except OSError as file_e:
            logger.exception(f"文件操作失败: {file_e}")
            return None

        logger.info(f"成功解析{len(data)}条数据")
        return data

    def _apply_adj_factors_to_kline(self, lazy_df, ts_code: str):
        """把复权因子对齐到 K 线日期，缺失时填充默认值 1.0。"""
        # 获取复权因子（使用完整日期范围，因为复权因子计算需要所有历史数据）
        # 传入None作为日期范围，让_get_adj_factors获取所有历史数据
        factors_df = self._get_adj_factors(ts_code, None, None)

        if factors_df is not None and not factors_df.is_empty():
            # 转换复权因子为LazyFrame
            factors_lazy = factors_df.lazy()
            lazy_df = lazy_df.join(factors_lazy, left_on='date', right_on='trade_date', how='left')

            # 填充缺失的复权因子
            for adj_type in ['qfq', 'hfq']:
                factor_col = f'{adj_type}_factor'
                lazy_df = lazy_df.with_columns([
                    pl.col(factor_col).fill_null(strategy='forward').fill_null(1.0).alias(factor_col)
                ])

            logger.info(f"{ts_code} 复权因子已对齐到K线日期，复权价由数据出口统一计算")
        else:
            logger.warning(f"{ts_code} 没有获取到复权因子，使用默认复权因子1.0")
            for adj_type in ['qfq', 'hfq']:
                lazy_df = lazy_df.with_columns([
                    pl.lit(1.0).alias(f'{adj_type}_factor')
                ])

        return lazy_df

    def get_kline_data(self, stock_code: str, start_date: str, end_date: str, adjust: str = "none"):
        """
        获取指定股票在指定日期范围内的K线数据，支持复权

        Args:
            stock_code: 股票代码，如"600000"或"sh.600000"
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            adjust: 复权类型，"qfq"=前复权，"hfq"=后复权，"none"=不复权（默认）

        Returns:
            pl.LazyFrame: K线数据的LazyFrame，包含date, open, high, low, close, volume, amount字段
                           如果指定了adjust，还会包含复权因子（qfq_factor/hfq_factor），
                           复权价由调用方实时计算
        """
        try:
            logger.info(f"开始获取股票 {stock_code} 在 {start_date} 到 {end_date} 期间的K线数据")

            resolved = self._resolve_kline_path(stock_code)
            if resolved is None:
                return pl.LazyFrame({})
            code, market, file_path = resolved

            # 复用统一的 .day 解析器（成交额元、成交量手）
            data = self._read_kline_records(file_path)
            if not data:
                return pl.LazyFrame({})

            # 转换为polars DataFrame
            df = pl.DataFrame(data)

            # 转换为LazyFrame
            lazy_df = df.lazy()

            # 转换start_date和end_date为date对象
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()

            # 使用Lazy API过滤日期范围内的数据
            lazy_df = lazy_df.filter(
                (pl.col('date') >= start_dt) & (pl.col('date') <= end_dt)
            )

            # 复权统一由 DataFetcher._apply_adjustment 在数据出口处理：
            # 此处只把复权因子对齐到 K 线日期，不再生成 qfq_*/hfq_* 预计算价格列，
            # 避免同一份复权逻辑在多处实现导致口径漂移。
            ts_code = f"{code}.{market.upper()}"
            lazy_df = self._apply_adj_factors_to_kline(lazy_df, ts_code)

            logger.info(f"成功获取股票 {stock_code} 的K线数据，返回LazyFrame")
            return lazy_df

        except (OSError, ValueError) as e:
            logger.exception(f"获取股票 {stock_code} 的K线数据失败: {e}")
            return pl.LazyFrame({})

    # ------------------------------------------------------------------
    # 标准 DataSource 接口（统一列名/单位/复权口径，供 DataFetcher 调用）
    # ------------------------------------------------------------------

    def get_stock_data(self, ts_code: str, start_date: str, end_date: str,
                       freq: str = 'daily', adjustment_type: str = 'qfq'):
        """
        标准接口：获取股票日线数据，返回统一格式（原始价 + 复权因子）。

        复用 get_kline_data() 的本地文件解析 + 复权因子计算，
        补齐 pre_close/change/pct_chg 衍生列后归一化。

        Returns:
            pl.DataFrame: 归一化后的K线数据
        """
        try:
            # 多取一段前置数据，保证区间首根K线能拿到真实的昨收
            start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
            fetch_start = (start_dt - timedelta(days=30)).strftime('%Y-%m-%d')

            # get_kline_data 返回 LazyFrame，含 date/open/high/low/close/volume/amount
            # 以及 qfq_factor/hfq_factor 列
            lazy_df = self.get_kline_data(ts_code, fetch_start, end_date, adjust='none')
            if lazy_df is None:
                return pl.DataFrame()

            df = lazy_df.collect()
            if df.is_empty():
                return pl.DataFrame()

            df = df.sort('date')

            # 昨收按交易所口径推导：除权日需用复权因子比值还原除权参考价
            #   pre_close_t = close_{t-1} * qfq_factor_{t-1} / qfq_factor_t
            # 非除权日因子相同，退化为前一日收盘价
            prev_close = pl.col('close').shift(1)
            if 'qfq_factor' in df.columns:
                pre_close_expr = prev_close * (
                    pl.col('qfq_factor').shift(1) / pl.col('qfq_factor'))
            else:
                pre_close_expr = prev_close

            df = df.with_columns(pre_close_expr.alias('pre_close'))
            df = df.with_columns([
                (pl.col('close') - pl.col('pre_close')).alias('change'),
                pl.when(pl.col('pre_close').is_not_null() & (pl.col('pre_close') != 0))
                .then((pl.col('close') / pl.col('pre_close') - 1) * 100)
                .otherwise(None).alias('pct_chg'),
            ])

            # 丢弃为补齐昨收而多取的前置数据
            df = df.filter(
                (pl.col('date') >= start_dt) & (pl.col('date') <= end_dt))

            # tdx 列名已为标准名，成交量单位为"手"
            column_map = {
                'date': 'date', 'open': 'open', 'high': 'high', 'low': 'low',
                'close': 'close', 'pre_close': 'pre_close', 'volume': 'volume',
                'amount': 'amount', 'pct_chg': 'pct_chg', 'change': 'change',
                'qfq_factor': 'qfq_factor', 'hfq_factor': 'hfq_factor',
            }
            return self._normalize_kline(
                df, column_map=column_map, ts_code=ts_code,
                volume_unit='hand', date_format=None, include_factors=True,
            )

        # 兜底理由：本地文件解析异常不可预知，失败返回空表
        except Exception as e:  # noqa: BLE001
            logger.warning(f"tdx get_stock_data({ts_code}) 失败: {e}")
            return pl.DataFrame()

    def get_index_data(self, ts_code: str, start_date: str, end_date: str,
                       freq: str = 'daily'):
        """
        标准接口：获取指数日线数据，返回统一格式。

        指数无复权因子，复用 get_kline_data 解析本地文件。

        Returns:
            pl.DataFrame: 归一化后的指数K线数据
        """
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
            fetch_start = (start_dt - timedelta(days=30)).strftime('%Y-%m-%d')

            lazy_df = self.get_kline_data(ts_code, fetch_start, end_date, adjust='none')
            if lazy_df is None:
                return pl.DataFrame()

            df = lazy_df.collect()
            if df.is_empty():
                return pl.DataFrame()

            df = df.sort('date')

            # 指数无除权，昨收即前一日收盘；多取的前置数据用于补齐区间首行
            df = df.with_columns([
                pl.col('close').shift(1).alias('pre_close'),
            ])
            df = df.with_columns([
                (pl.col('close') - pl.col('pre_close')).alias('change'),
                pl.when(pl.col('pre_close').is_not_null() & (pl.col('pre_close') != 0))
                .then((pl.col('close') / pl.col('pre_close') - 1) * 100)
                .otherwise(None).alias('pct_chg'),
            ])
            df = df.filter(
                (pl.col('date') >= start_dt) & (pl.col('date') <= end_dt))

            column_map = {
                'date': 'date', 'open': 'open', 'high': 'high', 'low': 'low',
                'close': 'close', 'pre_close': 'pre_close', 'volume': 'volume',
                'amount': 'amount', 'pct_chg': 'pct_chg', 'change': 'change',
            }
            return self._normalize_kline(
                df, column_map=column_map, ts_code=ts_code,
                volume_unit='hand', date_format=None, include_factors=True,
            )

        # 兜底理由：本地文件解析异常不可预知，失败返回空表
        except Exception as e:  # noqa: BLE001
            logger.warning(f"tdx get_index_data({ts_code}) 失败: {e}")
            return pl.DataFrame()
