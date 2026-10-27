#!/usr/bin/env python3

"""
Baostock数据处理器
"""

from datetime import datetime
from typing import List, Tuple

import baostock as bs
import pandas as pd
import polars as pl
from loguru import logger

from src.data.sources.base import DataSourceBase
from src.utils.code_converter import to_baostock_code, to_symbol, to_ts_code
from src.utils.exception_handler import handle_exception_with_retry
from src.utils.exceptions import DataSourceConnectionError


class BaostockHandler(DataSourceBase):
    """
    Baostock数据处理器，负责从Baostock获取数据并存储到数据库

    继承 DataSourceBase 获得统一接口；DataFetcher 通过标准接口
    get_stock_data / get_index_data 调度，download_stock_daily 等
    方法供内部与批量同步使用。
    """

    def __init__(self, config, db_manager):
        """
        初始化Baostock数据处理器

        Args:
            config: 配置对象
            db_manager: 数据库管理器实例
        """
        self.config = config
        self.db_manager = db_manager
        self.name = "BaostockHandler"
        self.available = True
        self.session = None
        self.bs_login = False

        # 离线模式支持
        if db_manager:
            try:
                self.session = db_manager.get_session()
            except (ConnectionError, TimeoutError, OSError) as e:
                logger.warning(f"数据库会话获取失败（离线模式下正常）: {e}")
                self.session = None
        else:
            logger.info("BaostockHandler在离线模式下初始化")

        # 根据配置决定是否在初始化时登录Baostock
        if hasattr(self.config.data, 'auto_login_baostock') and self.config.data.auto_login_baostock:
            self._login_baostock()
        else:
            logger.info("跳过自动登录Baostock（未开启或配置不允许）")

        # 从配置文件获取默认配置
        self.default_stock_codes = self.config.data.default_stock_codes
        self.default_index_codes = self.config.data.default_index_codes
        self.supported_frequencies = self.config.data.supported_frequencies

    def _get_default_codes(self, code_type: str):
        """
        获取默认代码列表

        Args:
            code_type: 代码类型，'stock'或'index'

        Returns:
            list: 默认代码列表
        """
        if code_type == 'stock':
            return self.default_stock_codes
        elif code_type == 'index':
            return self.default_index_codes
        else:
            return []

    def _process_dates(self, start_date: str, end_date: str, default_days: int = 30):
        """
        处理日期参数，设置默认值

        Args:
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            default_days: 默认天数

        Returns:
            tuple: (start_date, end_date)
        """
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            # 默认更新最近default_days天的数据
            start_date = (datetime.now() - datetime.timedelta(days=default_days)).strftime("%Y-%m-%d")

        return start_date, end_date

    def _validate_frequency(self, frequency: str):
        """
        验证分钟线频率

        Args:
            frequency: 分钟线频率

        Returns:
            str: 验证后的分钟线频率
        """
        if frequency not in self.supported_frequencies:
            logger.warning(f"不支持的分钟线频率: {frequency}，使用默认频率5分钟")
            return "5"
        return frequency

    def _login_baostock(self):
        """
        登录Baostock
        """
        try:
            # 设置登录超时
            import socket
            # 设置全局超时为30秒
            socket.setdefaulttimeout(30)

            logger.info("正在连接Baostock服务器...")
            lg = bs.login()

            if lg is None:
                logger.warning("Baostock API返回None")
                self.bs_login = False
                return

            if lg.error_code == '0':
                logger.info("Baostock登录成功")
                self.bs_login = True
            else:
                logger.warning(f"Baostock登录失败: {lg.error_msg}")
                self.bs_login = False
        except (ConnectionError, TimeoutError, OSError) as e:
            logger.exception(f"Baostock登录异常: {e}")
            self.bs_login = False
        except Exception as e:
            # 捕获其他所有异常
            logger.exception(f"Baostock登录发生未知异常: {e}")
            self.bs_login = False

    @handle_exception_with_retry(max_retries=3, retry_delay=2)
    def _login_with_retry(self):
        """登录Baostock（带统一重试装饰器），登录失败抛出可重试异常"""
        self._login_baostock()
        if not self.bs_login:
            raise DataSourceConnectionError(
                source="baostock", message="登录失败")

    def _ensure_baostock_login(self, max_retries=3, retry_delay=2):
        """
        确保Baostock已登录，如果未登录则尝试登录

        Args:
            max_retries: 最大重试次数（保留兼容参数，实际由装饰器固定为3次）
            retry_delay: 重试间隔（秒）（保留兼容参数，实际由装饰器固定为2秒）

        Returns:
            bool: 登录状态，True表示已登录，False表示登录失败
        """
        if self.bs_login:
            return True

        logger.info("Baostock未登录，尝试登录...")
        try:
            self._login_with_retry()
            logger.info("Baostock登录成功")
            return True
        except Exception as e:
            logger.warning(f"Baostock登录失败，已达到最大重试次数: {e}")
            return False

    def _logout_baostock(self):
        """
        登出Baostock
        """
        if self.bs_login:
            try:
                bs.logout()
                logger.info("Baostock登出成功")
                self.bs_login = False
            except (ConnectionError, TimeoutError, OSError) as e:
                logger.exception(f"Baostock登出异常: {e}")

    def __del__(self):
        """
        析构函数，确保登出Baostock
        """
        self._logout_baostock()

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
            stock_basic_pd = rs.get_data()

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

            # 离线模式下，只获取数据不存储
            if not self.session:
                logger.info("离线模式下，跳过股票基本信息存储")
                return stock_basic_df  # 离线模式下返回Polars DataFrame

            # 数据清洗和标准化
            from src.database.models.stock import StockBasic

            # 为了避免事务问题，只更新默认股票
            default_stocks = ["600000", "000001", "300001"]

            # 遍历默认股票，进行清洗和存储
            for symbol in default_stocks:
                try:
                    # 从Baostock数据中查找对应的股票信息（统一代码转换工具）
                    baostock_code = to_baostock_code(symbol)
                    stock_row = stock_basic_df.filter(pl.col('code') == baostock_code)

                    if stock_row.is_empty():
                        logger.warning(f"未找到股票{symbol}的基本信息")
                        continue

                    # 获取第一条匹配的记录
                    row = stock_row.row(named=True)
                    name = row['code_name']
                    baostock_code = row['code']  # 格式如 sh.600000, sz.000001, bj.920181

                    # 统一转换：baostock 格式 -> 标准 ts_code
                    ts_code = to_ts_code(baostock_code)
                    symbol = to_symbol(baostock_code)

                    # 截断过长的股票名称（数据库字段限制20个字符）
                    if len(name) > 20:
                        name = name[:20]  # 截断到20个字符

                    # 查询股票是否已存在
                    stock = self.session.query(StockBasic).filter_by(ts_code=ts_code).first()

                    if stock:
                        # 更新现有股票信息
                        stock.name = name
                        stock.symbol = symbol
                    else:
                        # 创建新股票信息
                        stock = StockBasic(
                            ts_code=ts_code,
                            symbol=symbol,
                            name=name
                        )
                        self.session.add(stock)

                except (ConnectionError, TimeoutError, OSError) as row_e:
                    logger.exception(f"处理股票{symbol}基本信息失败: {row_e}")
                    continue

            # 提交事务
            self.session.commit()
            logger.info("股票基本信息更新完成")
            return stock_basic_df  # 返回Polars DataFrame

        except (ConnectionError, TimeoutError, OSError) as e:
            if self.session:
                try:
                    self.session.rollback()
                except (ConnectionError, TimeoutError, OSError) as rollback_e:
                    logger.warning(f"回滚事务失败: {rollback_e}")
            logger.exception(f"从Baostock获取股票基本信息失败: {e}")
            raise

    def update_stock_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None, adjustflag: str = "3"):
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

            # 如果没有指定股票代码，从数据库获取所有股票代码
            if not ts_codes:
                if self.session:
                    from src.database.models.stock import StockBasic
                    stock_basics = self.session.query(StockBasic.ts_code).all()
                    # 保持标准 ts_code 格式（600000.SH），查询时再转换为 baostock 格式
                    ts_codes = [sb.ts_code for sb in stock_basics]
                else:
                    # 离线模式下使用默认股票代码
                    ts_codes = self._get_default_codes('stock')
                    logger.info(f"离线模式下，使用默认股票代码列表: {ts_codes}")

            if not ts_codes:
                logger.warning("没有需要更新的股票代码")
                return

            # 处理日期参数
            start_date, end_date = self._process_dates(start_date, end_date, default_days=self.config.data.default_days)

            logger.info(f"开始更新{len(ts_codes)}只股票的日线数据，时间范围: {start_date} 至 {end_date}")

            # 存储结果（离线模式下返回）
            result = {}

            # 遍历股票代码，获取日线数据
            for ts_code in ts_codes:
                try:
                    logger.info(f"正在获取{ts_code}的日线数据")

                    # 统一转换：写库用标准 ts_code，API 查询用 baostock 格式
                    std_code = to_ts_code(ts_code)
                    bs_code = to_baostock_code(ts_code)

                    # 使用Baostock获取股票日线数据
                    rs = bs.query_history_k_data_plus(
                        bs_code,
                        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
                        start_date=start_date,
                        end_date=end_date,
                        frequency="d",
                        adjustflag=adjustflag  # 使用传入的复权参数
                    )

                    # 检查rs是否为None
                    if rs is None:
                        logger.warning(f"Baostock API返回None，无法获取{ts_code}的日线数据")
                        continue

                    # 获取数据
                    try:
                        stock_daily_pd = rs.get_data()
                    except Exception as e:
                        logger.warning(f"获取{ts_code}数据失败: {e}")
                        continue

                    # 转换为Polars DataFrame
                    stock_daily_df = pl.from_pandas(stock_daily_pd)

                    if stock_daily_df.is_empty():
                        logger.warning(f"{ts_code}在{start_date}至{end_date}期间没有日线数据")
                        continue

                    logger.info(f"获取到{ts_code}的{stock_daily_df.height}条日线数据")

                    # 离线模式下，只获取数据不存储
                    if not self.session:
                        result[std_code] = stock_daily_df
                        logger.info(f"离线模式下，获取{std_code}的日线数据完成")
                        continue

                    # 数据清洗和标准化
                    from src.database.models.stock import StockBasic, StockDaily
                    from src.database.upsert import upsert_rows

                    # 初始化最早交易日期
                    earliest_date = None
                    daily_rows = []

                    # 遍历数据，转换为待写入行（不合格行跳过）
                    for row in stock_daily_df.iter_rows(named=True):
                        try:
                            # 转换日期格式
                            trade_date = datetime.strptime(row['date'], "%Y-%m-%d").date()

                            # 更新最早交易日期
                            if earliest_date is None or trade_date < earliest_date:
                                earliest_date = trade_date

                            # 转换数值类型
                            close_val = float(row['close']) if row['close'] else 0.0
                            pre_close_val = float(row['preclose']) if row['preclose'] else 0.0
                            daily_rows.append({
                                'ts_code': std_code,
                                'trade_date': trade_date,
                                'open': float(row['open']) if row['open'] else 0.0,
                                'high': float(row['high']) if row['high'] else 0.0,
                                'low': float(row['low']) if row['low'] else 0.0,
                                'close': close_val,
                                'pre_close': pre_close_val,
                                'change': close_val - pre_close_val,
                                'pct_chg': float(row['pctChg']) if row['pctChg'] else 0.0,
                                'vol': float(row['volume']) if row['volume'] else 0.0,
                                'amount': float(row['amount']) if row['amount'] else 0.0,
                            })
                        except (ConnectionError, TimeoutError, OSError) as row_e:
                            logger.exception(f"处理股票日线数据失败: {row_e}")
                            continue

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
                            # 如果股票基本信息不存在，创建一个
                            logger.warning(f"股票{std_code}的基本信息不存在，将创建一条新记录")
                            # 由于Baostock不提供股票名称，使用代码作为名称
                            new_stock = StockBasic(
                                ts_code=std_code,
                                symbol=to_symbol(std_code),
                                name=std_code,
                                list_date=earliest_date
                            )
                            self.session.add(new_stock)
                            logger.info(f"创建股票{std_code}的基本信息，上市日期为: {earliest_date}")

                    # 每只股票提交一次事务
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
            logger.exception(f"更新股票日线数据失败: {e}")
            raise

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

    def update_index_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
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

            # 如果没有指定指数代码，从数据库获取所有指数代码
            if not ts_codes:
                if self.session:
                    from src.database.models.index import IndexBasic
                    index_basics = self.session.query(IndexBasic.ts_code).all()
                    ts_codes = [ib.ts_code for ib in index_basics]
                else:
                    # 离线模式下使用默认指数代码
                    ts_codes = self._get_default_codes('index')
                    logger.info(f"离线模式下，使用默认指数代码列表: {ts_codes}")

            if not ts_codes:
                logger.warning("没有需要更新的指数代码")
                return

            # 处理日期参数
            start_date, end_date = self._process_dates(start_date, end_date, default_days=30)

            logger.info(f"开始更新{len(ts_codes)}个指数的日线数据，时间范围: {start_date} 至 {end_date}")

            # 存储结果（离线模式下返回）
            result = {}

            # 遍历指数代码，获取日线数据
            for ts_code in ts_codes:
                try:
                    logger.info(f"正在获取{ts_code}的日线数据")

                    # 使用Baostock获取指数日线数据
                    rs = bs.query_history_k_data_plus(
                        ts_code,
                        "date,code,open,high,low,close,preclose,volume,amount,pctChg",
                        start_date=start_date,
                        end_date=end_date,
                        frequency="d",
                        adjustflag="3"  # 不复权
                    )

                    # 检查rs是否为None
                    if rs is None:
                        logger.warning(f"Baostock API返回None，无法获取{ts_code}的日线数据")
                        continue

                    # 获取数据
                    try:
                        index_daily_pd = rs.get_data()
                    except Exception as e:
                        logger.warning(f"获取{ts_code}数据失败: {e}")
                        continue

                    # 转换为Polars DataFrame
                    index_daily_df = pl.from_pandas(index_daily_pd)

                    if index_daily_df.is_empty():
                        logger.warning(f"{ts_code}在{start_date}至{end_date}期间没有日线数据")
                        continue

                    logger.info(f"获取到{ts_code}的{index_daily_df.height}条日线数据")

                    # 离线模式下，只获取数据不存储
                    if not self.session:
                        result[ts_code] = index_daily_pd
                        logger.info(f"离线模式下，获取{ts_code}的日线数据完成")
                        continue

                    # 数据清洗和标准化
                    from src.database.models.index import IndexDaily
                    from src.database.upsert import upsert_rows

                    # 遍历数据，转换为待写入行（不合格行跳过）
                    daily_rows = []
                    for row in index_daily_df.iter_rows(named=True):
                        try:
                            # 转换日期格式
                            trade_date = datetime.strptime(row['date'], "%Y-%m-%d").date()

                            # 转换数值类型
                            close_val = float(row['close']) if row['close'] else 0.0
                            pre_close_val = float(row['preclose']) if row['preclose'] else 0.0
                            daily_rows.append({
                                'ts_code': ts_code,
                                'trade_date': trade_date,
                                'open': float(row['open']) if row['open'] else 0.0,
                                'high': float(row['high']) if row['high'] else 0.0,
                                'low': float(row['low']) if row['low'] else 0.0,
                                'close': close_val,
                                'pre_close': pre_close_val,
                                'change': close_val - pre_close_val,
                                'pct_chg': float(row['pctChg']) if row['pctChg'] else 0.0,
                                'vol': float(row['volume']) if row['volume'] else 0.0,
                                'amount': float(row['amount']) if row['amount'] else 0.0,
                            })
                        except (ConnectionError, TimeoutError, OSError) as row_e:
                            logger.exception(f"处理指数日线数据失败: {row_e}")
                            continue

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

    def _get_realtime_data(self, ts_codes: List[str], code_type: str, noun: str,
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
                        "date,code,open,high,low,close,preclose,volume,amount,pctChg",
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

    def get_realtime_stock_data(self, ts_codes: List[str] = None):
        """获取实时股票数据（以最近历史数据替代）。"""
        return self._get_realtime_data(
            ts_codes, 'stock', '股票', self.config.data.default_realtime_days, "2")

    def get_realtime_index_data(self, ts_codes: List[str] = None):
        """获取实时指数数据（以最近历史数据替代）。"""
        return self._get_realtime_data(ts_codes, 'index', '指数', 5, "3")

    def download_stock_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
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

    def download_index_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
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

    # ------------------------------------------------------------------
    # 标准 DataSource 接口（统一列名/单位/复权口径，供 DataFetcher 调用）
    # ------------------------------------------------------------------

    def _to_baostock_code(self, ts_code: str) -> str:
        """将标准 ts_code 转换为 baostock 格式（委托统一转换工具）"""
        return to_baostock_code(ts_code)

    def get_stock_data(self, ts_code: str, start_date: str, end_date: str,
                       freq: str = 'daily', adjustment_type: str = 'qfq'):
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
                "date,code,open,high,low,close,preclose,volume,amount,pctChg",
                start_date=start_date, end_date=end_date,
                frequency="d", adjustflag="3"
            )
            if rs is None:
                return pl.DataFrame()

            raw = rs.get_data()
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
            column_map = {
                'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close',
                'pre_close': 'preclose', 'volume': 'volume', 'amount': 'amount',
                'pct_chg': 'pctChg',
            }
            return self._normalize_kline(
                df, column_map=column_map, ts_code=ts_code,
                volume_unit='share', date_format='%Y-%m-%d', include_factors=True,
            )

        except Exception as e:
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
        hfq_raw = rs.get_data()
        if hfq_raw is None or hfq_raw.empty:
            return None, None

        hfq_df = pl.from_pandas(hfq_raw).select([
            pl.col('date'),
            pl.col('close').cast(pl.Float64).alias('close_hfq'),
        ])
        return self._compute_daily_factors(raw_df, hfq_df)

    @staticmethod
    def _compute_daily_factors(raw_df: pl.DataFrame, hfq_df: pl.DataFrame
                               ) -> Tuple[pl.DataFrame | None, pl.DataFrame | None]:
        """纯计算：由原始价与后复权价求每日 qfq_factor / hfq_factor。"""
        raw = raw_df.select([
            pl.col('date'),
            pl.col('close').cast(pl.Float64).alias('close_raw'),
        ])
        joined = (raw.join(hfq_df, on='date', how='inner')
                     .filter((pl.col('close_raw') > 0)
                             & pl.col('close_hfq').is_not_null())
                     .sort('date'))
        if joined.is_empty():
            return None, None

        factor = joined['close_hfq'] / joined['close_raw']
        anchor = factor[-1]  # 区间末锚点，与 baostock 前复权口径一致
        if anchor is None or anchor <= 0:
            return None, None

        hfq_factor = pl.DataFrame({'date': joined['date'], 'hfq_factor': factor})
        qfq_factor = pl.DataFrame({'date': joined['date'], 'qfq_factor': factor / anchor})
        return qfq_factor, hfq_factor

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
                "date,code,open,high,low,close,preclose,volume,amount,pctChg",
                start_date=start_date, end_date=end_date,
                frequency="d", adjustflag="3"
            )
            if rs is None:
                return pl.DataFrame()

            raw = rs.get_data()
            if raw is None or raw.empty:
                return pl.DataFrame()

            df = pl.from_pandas(raw)

            column_map = {
                'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close',
                'pre_close': 'preclose', 'volume': 'volume', 'amount': 'amount',
                'pct_chg': 'pctChg',
            }
            # 指数无复权因子，volume_unit 指数成交量单位为手
            return self._normalize_kline(
                df, column_map=column_map, ts_code=ts_code,
                volume_unit='hand', date_format='%Y-%m-%d', include_factors=True,
            )

        except Exception as e:
            logger.warning(f"baostock get_index_data({ts_code}) 失败: {e}")
            return pl.DataFrame()

    def _download_minute(self, model, code_type: str, noun: str, default_days: int,
                         ts_codes: List[str], start_date: str, end_date: str,
                         frequency: str):
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

            result = {}

            for ts_code in ts_codes:
                try:
                    logger.info(f"正在下载{ts_code}的{frequency}分钟线数据")

                    rs = bs.query_history_k_data_plus(
                        ts_code,
                        "date,time,code,open,high,low,close,volume,amount",
                        start_date=start_date,
                        end_date=end_date,
                        frequency=frequency,
                        adjustflag="3"  # 不复权，写库存原始价
                    )

                    if rs is None:
                        logger.warning(f"Baostock API返回None，无法获取{ts_code}的{frequency}分钟线数据")
                        continue

                    try:
                        minute_pd = rs.get_data()
                    except Exception as e:
                        logger.warning(f"获取{ts_code}的{frequency}分钟线数据失败: {e}")
                        continue

                    minute_df = pl.from_pandas(minute_pd)

                    if minute_df.is_empty():
                        logger.warning(f"{ts_code}在{start_date}至{end_date}期间没有{frequency}分钟线数据")
                        continue

                    logger.info(f"获取到{ts_code}的{minute_df.height}条{frequency}分钟线数据")

                    # 离线模式下，只获取数据不存储
                    if not self.session:
                        result[ts_code] = minute_pd
                        logger.info(f"离线模式下，获取{ts_code}的{frequency}分钟线数据完成")
                        continue

                    from src.database.upsert import upsert_rows

                    # 遍历数据，转换为待写入行（不合格行跳过）
                    minute_rows = []
                    for row in minute_df.iter_rows(named=True):
                        try:
                            minute_rows.append({
                                'ts_code': ts_code,
                                'trade_date': datetime.strptime(row['date'], "%Y-%m-%d").date(),
                                'trade_time': row['time'],
                                'open': float(row['open']) if row['open'] else 0.0,
                                'high': float(row['high']) if row['high'] else 0.0,
                                'low': float(row['low']) if row['low'] else 0.0,
                                'close': float(row['close']) if row['close'] else 0.0,
                                'vol': float(row['volume']) if row['volume'] else 0.0,
                                'amount': float(row['amount']) if row['amount'] else 0.0,
                            })
                        except (ConnectionError, TimeoutError, OSError) as row_e:
                            logger.exception(f"处理{noun}分钟线数据失败: {row_e}")
                            continue

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

    def download_stock_minute(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None, frequency: str = "5"):
        """下载股票分钟线数据并入库（写库存原始价）。"""
        from src.database.models.stock import StockMinute
        return self._download_minute(
            StockMinute, 'stock', '股票', self.config.data.default_minute_days,
            ts_codes, start_date, end_date, frequency)

    def download_index_minute(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None, frequency: str = "5"):
        """下载指数分钟线数据并入库（写库存原始价）。"""
        from src.database.models.index import IndexMinute
        return self._download_minute(
            IndexMinute, 'index', '指数', 3,
            ts_codes, start_date, end_date, frequency)
