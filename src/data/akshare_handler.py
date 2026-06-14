#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
AkShare数据处理器
"""

from datetime import datetime
from typing import List, Optional

import akshare as ak
import polars as pl
from loguru import logger


class AkShareHandler:
    """
    AkShare数据处理器，负责从AkShare获取数据并存储到数据库
    """
    
    def __init__(self, config, db_manager):
        """
        初始化AkShare数据处理器
        
        Args:
            config: 配置对象
            db_manager: 数据库管理器实例
        """
        self.config = config
        self.db_manager = db_manager
        self.name = "AkShareHandler"
        self.session = None
        
        # 离线模式支持
        if db_manager:
            try:
                self.session = db_manager.get_session()
            except (ConnectionError, TimeoutError, OSError) as e:
                logger.warning(f"数据库会话获取失败（离线模式下正常）: {e}")
                self.session = None
        else:
            logger.info("AkShareHandler在离线模式下初始化")
    
    def update_stock_basic(self):
        """
        更新股票基本信息
        """
        try:
            logger.info("开始从AkShare获取股票基本信息")
            
            # 使用AkShare获取股票基本信息
            stock_basic_pd = ak.stock_info_a_code_name()
            
            # 转换为Polars DataFrame
            stock_basic_df = pl.from_pandas(stock_basic_pd)
            
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
                    symbol = row['code']
                    name = row['name']
                    
                    # 构建完整的ts_code（例如：600000.SH）
                    # 北交所新股：920000-920999
                    if symbol.startswith('92') and len(symbol) == 6:
                        num = int(symbol)
                        if 920000 <= num <= 920999:
                            ts_code = f"{symbol}.BJ"
                        else:
                            ts_code = f"{symbol}.SZ"
                    # 北交所股票：800000-899999
                    elif symbol.startswith('8') and len(symbol) == 6:
                        num = int(symbol)
                        if 800000 <= num <= 899999:
                            ts_code = f"{symbol}.BJ"
                        else:
                            ts_code = f"{symbol}.SZ"
                    elif symbol.startswith('6'):
                        ts_code = f"{symbol}.SH"
                    else:
                        ts_code = f"{symbol}.SZ"
                    
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

            # 使用AkShare获取A股实时行情数据
            df = ak.stock_zh_a_spot_em()

            if df is None or df.empty:
                logger.warning("获取市场涨跌家数数据为空")
                return None

            # 统计涨跌家数
            up_count = len(df[df['涨跌幅'] > 0])
            down_count = len(df[df['涨跌幅'] < 0])
            flat_count = len(df[df['涨跌幅'] == 0])
            total_count = len(df)

            # 计算涨跌比例
            up_rate = (up_count / total_count * 100) if total_count > 0 else 0
            down_rate = (down_count / total_count * 100) if total_count > 0 else 0

            logger.info(f"市场涨跌家数: 上涨={up_count}, 下跌={down_count}, 平盘={flat_count}, 总计={total_count}")

            # 转换日期格式
            from datetime import datetime
            if trade_date:
                trade_date_obj = datetime.strptime(trade_date, "%Y%m%d").date()
            else:
                trade_date_obj = datetime.now().date()

            # 离线模式下返回结果
            if not self.session:
                return {
                    'trade_date': trade_date_obj,
                    'up_count': up_count,
                    'down_count': down_count,
                    'flat_count': flat_count,
                    'total_count': total_count,
                    'up_rate': up_rate,
                    'down_rate': down_rate
                }

            # 存储到数据库
            from src.database.models.stock import MarketBreadth

            # 查询是否已存在
            existing = self.session.query(MarketBreadth).filter_by(trade_date=trade_date_obj).first()

            if existing:
                existing.up_count = up_count
                existing.down_count = down_count
                existing.flat_count = flat_count
                existing.total_count = total_count
                existing.up_rate = up_rate
                existing.down_rate = down_rate
                logger.info(f"更新市场涨跌家数数据: {trade_date_obj}")
            else:
                market_breadth = MarketBreadth(
                    trade_date=trade_date_obj,
                    up_count=up_count,
                    down_count=down_count,
                    flat_count=flat_count,
                    total_count=total_count,
                    up_rate=up_rate,
                    down_rate=down_rate
                )
                self.session.add(market_breadth)
                logger.info(f"新增市场涨跌家数数据: {trade_date_obj}")

            self.session.commit()
            return {
                'trade_date': trade_date_obj,
                'up_count': up_count,
                'down_count': down_count,
                'flat_count': flat_count,
                'total_count': total_count,
                'up_rate': up_rate,
                'down_rate': down_rate
            }

        except (ConnectionError, TimeoutError, OSError) as e:
            if self.session:
                self.session.rollback()
            logger.exception(f"获取市场涨跌家数数据失败: {e}")
            raise
    
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
            # 离线模式下的默认股票代码列表
            default_ts_codes = ["600000.SH", "000001.SZ", "300001.SZ"]
            
            # 如果没有指定股票代码，从数据库获取所有股票代码
            if not ts_codes:
                if self.session:
                    from src.database.models.stock import StockBasic
                    stock_basics = self.session.query(StockBasic.ts_code).all()
                    ts_codes = [sb.ts_code for sb in stock_basics]
                else:
                    # 离线模式下使用默认股票代码
                    ts_codes = default_ts_codes
                    logger.info(f"离线模式下，使用默认股票代码列表: {ts_codes}")
            
            if not ts_codes:
                logger.warning("没有需要更新的股票代码")
                return
            
            # 如果没有指定日期，默认更新最近30天的数据
            if not end_date:
                end_date = datetime.now().strftime("%Y%m%d")
            if not start_date:
                # 默认更新最近30天的数据
                start_date = (datetime.now() - datetime.timedelta(days=30)).strftime("%Y%m%d")
            
            logger.info(f"开始更新{len(ts_codes)}只股票的日线数据，时间范围: {start_date} 至 {end_date}")
            
            # 存储结果（离线模式下返回）
            result = {}
            
            # 遍历股票代码，获取日线数据
            for ts_code in ts_codes:
                try:
                    logger.info(f"正在获取{ts_code}的日线数据")
                    
                    # 使用AkShare获取股票日线数据
                    stock_daily_pd = ak.stock_zh_a_hist(
                        symbol=ts_code.split('.')[0],  # AkShare使用数字代码
                        period="daily",
                        start_date=start_date,
                        end_date=end_date,
                        adjust="qfq"  # 前复权
                    )
                    
                    # 转换为Polars DataFrame
                    stock_daily_df = pl.from_pandas(stock_daily_pd)
                    
                    if stock_daily_df.is_empty():
                        logger.warning(f"{ts_code}在{start_date}至{end_date}期间没有日线数据")
                        continue
                    
                    logger.info(f"获取到{ts_code}的{stock_daily_df.height}条日线数据")
                    
                    # 离线模式下，只获取数据不存储
                    if not self.session:
                        result[ts_code] = stock_daily_df
                        logger.info(f"离线模式下，获取{ts_code}的日线数据完成")
                        continue
                    
                    # 数据清洗和标准化
                    from src.database.models.stock import StockDaily

                    # 遍历数据，进行清洗和存储
                    for row in stock_daily_df.iter_rows(named=True):
                        try:
                            # 转换日期格式
                            trade_date = datetime.strptime(row['日期'], "%Y-%m-%d").date()
                            
                            # 查询数据是否已存在
                            daily_data = self.session.query(StockDaily).filter_by(
                                ts_code=ts_code,
                                trade_date=trade_date
                            ).first()
                            
                            if daily_data:
                                # 更新现有数据
                                daily_data.open = row['开盘']
                                daily_data.high = row['最高']
                                daily_data.low = row['最低']
                                daily_data.close = row['收盘']
                                daily_data.pre_close = row['昨收']
                                daily_data.change = row['涨跌额']
                                daily_data.pct_chg = row['涨跌幅']
                                daily_data.vol = row['成交量']
                                daily_data.amount = row['成交额']
                            else:
                                # 创建新数据
                                daily_data = StockDaily(
                                    ts_code=ts_code,
                                    trade_date=trade_date,
                                    open=row['开盘'],
                                    high=row['最高'],
                                    low=row['最低'],
                                    close=row['收盘'],
                                    pre_close=row['昨收'],
                                    change=row['涨跌额'],
                                    pct_chg=row['涨跌幅'],
                                    vol=row['成交量'],
                                    amount=row['成交额']
                                )
                                self.session.add(daily_data)
                            
                        except (ConnectionError, TimeoutError, OSError) as row_e:
                            logger.exception(f"处理股票日线数据失败: {row_e}")
                            continue
                    
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
            logger.info("开始从AkShare获取指数基本信息")
            
            # 使用AkShare获取指数基本信息
            index_basic_pd = ak.index_stock_info()
            
            # 转换为Polars DataFrame
            index_basic_df = pl.from_pandas(index_basic_pd)
            
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
                    ts_code = row['代码']
                    name = row['名称']
                    
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
            default_ts_codes = ["sh000001", "sz399001", "sz399006"]
            
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
                    
                    # 使用AkShare获取指数日线数据
                    index_daily_pd = ak.stock_zh_index_daily(
                        symbol=ts_code
                    )
                    
                    # 转换为Polars DataFrame
                    index_daily_df = pl.from_pandas(index_daily_pd)
                    
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

                    # 遍历数据，进行清洗和存储
                    for row in index_daily_df.iter_rows(named=True):
                        try:
                            # 转换日期格式
                            trade_date = datetime.strptime(row['date'], "%Y-%m-%d").date()
                            
                            # 查询数据是否已存在
                            daily_data = self.session.query(IndexDaily).filter_by(
                                ts_code=ts_code,
                                trade_date=trade_date
                            ).first()
                            
                            if daily_data:
                                # 更新现有数据
                                daily_data.open = row['open']
                                daily_data.high = row['high']
                                daily_data.low = row['low']
                                daily_data.close = row['close']
                                daily_data.vol = row['volume']
                            else:
                                # 创建新数据
                                daily_data = IndexDaily(
                                    ts_code=ts_code,
                                    trade_date=trade_date,
                                    open=row['open'],
                                    high=row['high'],
                                    low=row['low'],
                                    close=row['close'],
                                    pre_close=0.0,  # stock_zh_index_daily不提供昨收价
                                    change=0.0,  # stock_zh_index_daily不提供涨跌额
                                    pct_chg=0.0,  # stock_zh_index_daily不提供涨跌幅
                                    vol=row['volume'],
                                    amount=0.0  # stock_zh_index_daily不提供成交额
                                )
                                self.session.add(daily_data)
                            
                        except (ConnectionError, TimeoutError, OSError) as row_e:
                            logger.exception(f"处理指数日线数据失败: {row_e}")
                            continue
                    
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

    def update_stock_dividend(self, ts_codes: List[str] = None):
        """
        更新股票分红配股数据
        
        Args:
            ts_codes: 股票代码列表，None表示更新所有股票
        
        Returns:
            dict: 股票代码到分红数据的映射（离线模式下）
        """
        try:
            # 离线模式下的默认股票代码列表
            default_ts_codes = ["600000.SH", "000001.SZ", "300001.SZ"]
            
            # 如果没有指定股票代码，从数据库获取所有股票代码
            if not ts_codes:
                if self.session:
                    from src.database.models.stock import StockBasic
                    stock_basics = self.session.query(StockBasic.ts_code).all()
                    ts_codes = [sb.ts_code for sb in stock_basics]
                else:
                    # 离线模式下使用默认股票代码
                    ts_codes = default_ts_codes
                    logger.info(f"离线模式下，使用默认股票代码列表: {ts_codes}")
            
            if not ts_codes:
                logger.warning("没有需要更新的股票代码")
                return
            
            logger.info(f"开始更新{len(ts_codes)}只股票的分红配股数据")
            
            # 存储结果（离线模式下返回）
            result = {}
            
            # 遍历股票代码，获取分红配股数据
            for ts_code in ts_codes:
                try:
                    logger.info(f"正在获取{ts_code}的分红配股数据")

                    symbol = ts_code.split('.')[0]

                    # 使用AkShare获取股票分红数据
                    # 接口: stock_fhps_detail_em - 获取历史分红数据(东方财富)
                    try:
                        dividend_pd = ak.stock_fhps_detail_em(symbol=symbol)
                    except TypeError as e:
                        logger.warning(f"{ts_code}的分红配股数据获取失败，发生类型错误: {e}")
                        continue
                    except Exception as e:
                        logger.warning(f"{ts_code}的分红配股数据获取失败: {e}")
                        continue

                    # 检查dividend_pd是否为None
                    if dividend_pd is None:
                        logger.warning(f"{ts_code}的分红配股数据获取失败，返回None")
                        continue

                    # 转换为Polars DataFrame
                    dividend_df = pl.from_pandas(dividend_pd)

                    # 检查dividend_df是否为None或为空
                    if dividend_df is None or dividend_df.is_empty():
                        logger.warning(f"{ts_code}没有分红配股数据")
                        continue

                    logger.info(f"获取到{ts_code}的{dividend_df.height}条分红配股数据")

                    # 离线模式下，只获取数据不存储
                    if not self.session:
                        result[ts_code] = dividend_df
                        logger.info(f"离线模式下，获取{ts_code}的分红配股数据完成")
                        continue

                    # 数据清洗和标准化
                    from src.database.models.stock import (StockBasic,
                                                           StockDividend)

                    def _parse_date(value):
                        if value is None:
                            return None
                        if isinstance(value, datetime):
                            return value.date()
                        text = str(value).strip()
                        if not text or text in {'NaT', 'None', 'nan'}:
                            return None
                        try:
                            return datetime.strptime(text, "%Y-%m-%d").date()
                        except Exception:
                            return None

                    def _parse_float(value, default=0.0):
                        if value is None:
                            return default
                        if isinstance(value, (int, float)):
                            return float(value)
                        text = str(value).strip().replace(',', '')
                        if not text or text in {'None', 'nan', 'NaN', '--'}:
                            return default
                        for suffix in ['元', '股', '%']:
                            text = text.replace(suffix, '')
                        try:
                            return float(text)
                        except Exception:
                            return default

                    def _pick(row_dict, keys, default=0.0):
                        for key in keys:
                            if key in row_dict and row_dict.get(key) not in (None, '', 'NaT'):
                                return _parse_float(row_dict.get(key), default)
                        return default

                    # 获取股票名称
                    stock_basic = self.session.query(StockBasic).filter_by(ts_code=ts_code).first()
                    stock_name = stock_basic.name if stock_basic else ""

                    # 遍历数据，进行清洗和存储
                    for row in dividend_df.iter_rows(named=True):
                        try:
                            # 检查row是否为None
                            if row is None:
                                logger.warning(f"处理{ts_code}的分红配股数据时，row为None，跳过")
                                continue

                            # 报告期作为分红年度
                            dividend_year = None
                            if '报告期' in row and row['报告期']:
                                report_date_val = row['报告期']
                                if isinstance(report_date_val, str):
                                    dividend_year = report_date_val[:4]  # 提取年份
                                elif report_date_val is not None:
                                    dividend_year = str(report_date_val.year)

                            report_date = _parse_date(row.get('业绩披露日期') or row.get('公告日期'))
                            record_date = _parse_date(row.get('股权登记日'))
                            ex_date = _parse_date(row.get('除权除息日') or row.get('除权日'))

                            # 派息日（使用除权除息日作为派息日）
                            pay_date = ex_date

                            # 现金分红（10派X -> 每股X/10）
                            cash_div_ratio = _pick(row, ['现金分红-现金分红比例', '每10股派息(税前)', '每10股派息'])
                            cash_div = cash_div_ratio / 10.0 if cash_div_ratio else 0.0

                            # 送转比例（10送转X -> 每股X/10）
                            share_div_ratio = _pick(row, ['送转股份-送转总比例', '每10股送转', '每10股送股', '每10股转增'])
                            share_div = share_div_ratio / 10.0 if share_div_ratio else 0.0

                            # 配股信息（10配X -> 每股X/10）
                            rights_issue_price = _pick(row, ['配股价格', '配股价'])
                            rights_issue_ratio_raw = _pick(row, ['配股比例', '每10股配股比例', '10配股'])
                            rights_issue_ratio = rights_issue_ratio_raw / 10.0 if rights_issue_ratio_raw else 0.0

                            # 总派现金额（单位存在差异，仅在字段存在时保存）
                            total_div = _pick(row, ['总派现金额(含税)', '派现总额', '分红总额'], default=0.0)

                            # 优先按 ts_code + ex_date 去重，缺失 ex_date 再回退到 ts_code + dividend_year
                            dividend_data = None
                            if ex_date:
                                dividend_data = self.session.query(StockDividend).filter_by(
                                    ts_code=ts_code,
                                    ex_date=ex_date
                                ).first()
                            if dividend_data is None and dividend_year:
                                dividend_data = self.session.query(StockDividend).filter_by(
                                    ts_code=ts_code,
                                    dividend_year=dividend_year
                                ).order_by(StockDividend.updated_at.desc(), StockDividend.id.desc()).first()

                            if dividend_data:
                                # 更新现有数据
                                dividend_data.report_date = report_date
                                dividend_data.record_date = record_date
                                dividend_data.ex_date = ex_date
                                dividend_data.pay_date = pay_date
                                dividend_data.cash_div = cash_div
                                dividend_data.share_div = share_div
                                dividend_data.total_div = total_div
                                dividend_data.rights_issue_price = rights_issue_price if rights_issue_price > 0 else None
                                dividend_data.rights_issue_ratio = rights_issue_ratio if rights_issue_ratio > 0 else None
                            else:
                                # 创建新数据
                                dividend_data = StockDividend(
                                    ts_code=ts_code,
                                    symbol=symbol,
                                    name=stock_name,
                                    dividend_year=dividend_year,
                                    report_date=report_date,
                                    record_date=record_date,
                                    ex_date=ex_date,
                                    pay_date=pay_date,
                                    cash_div=cash_div,
                                    share_div=share_div,
                                    total_div=total_div,
                                    rights_issue_price=rights_issue_price if rights_issue_price > 0 else None,
                                    rights_issue_ratio=rights_issue_ratio if rights_issue_ratio > 0 else None,
                                )
                                self.session.add(dividend_data)

                        except TypeError as row_e:
                            logger.exception(f"处理{ts_code}的分红配股数据时发生类型错误: {row_e}")
                            continue
                        except (ConnectionError, TimeoutError, OSError) as row_e:
                            logger.exception(f"处理股票分红配股数据失败: {row_e}")
                            continue
                        except Exception as row_e:
                            logger.exception(f"处理{ts_code}的分红配股数据时发生未知错误: {row_e}")
                            continue

                    # 每只股票提交一次事务
                    self.session.commit()
                    logger.info(f"{ts_code}的分红配股数据更新完成")

                except TypeError as e:
                    if self.session:
                        self.session.rollback()
                    logger.exception(f"获取{ts_code}的分红配股数据时发生类型错误: {e}")
                    continue
                except (ConnectionError, TimeoutError, OSError) as e:
                    if self.session:
                        self.session.rollback()
                    logger.exception(f"获取{ts_code}的分红配股数据失败: {e}")
                    continue
                except Exception as e:
                    if self.session:
                        self.session.rollback()
                    logger.exception(f"获取{ts_code}的分红配股数据时发生未知错误: {e}")
                    continue
            
            # 离线模式下返回结果
            if not self.session and result:
                return result
            
        except (ConnectionError, TimeoutError, OSError) as e:
            if self.session:
                self.session.rollback()
            logger.exception(f"更新股票分红配股数据失败: {e}")
            raise

    def update_etf_basic(self, max_retries=3):
        """
        更新 ETF 基本信息
        使用 AkShare 的 fund_etf_spot_em() 获取 ETF 列表和名称

        Args:
            max_retries: 最大重试次数，默认3次
        """
        import time

        etf_spot_pd = None
        last_error = None

        for attempt in range(max_retries):
            try:
                logger.info(f"开始从 AkShare 获取 ETF 基本信息 (尝试 {attempt + 1}/{max_retries})")

                # 使用 AkShare 获取 ETF 实时行情
                etf_spot_pd = ak.fund_etf_spot_em()
                break  # 成功获取数据，跳出重试循环

            except Exception as e:
                last_error = e
                logger.warning(f"从 AkShare 获取 ETF 信息失败 (尝试 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # 指数退避: 1, 2, 4 秒
                    logger.info(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                continue

        if etf_spot_pd is None:
            logger.error(f"从 AkShare 获取 ETF 基本信息失败，已重试 {max_retries} 次: {last_error}")
            return None

        if etf_spot_pd.empty:
            logger.warning("从 AkShare 获取的 ETF 信息为空")
            return None

        logger.info(f"从 AkShare 获取到 {len(etf_spot_pd)} 条 ETF 信息")

        # 转换为 Polars DataFrame
        etf_df = pl.from_pandas(etf_spot_pd)

        if etf_df.is_empty():
            logger.warning("ETF DataFrame 为空")
            return None

        logger.info(f"ETF DataFrame 列名: {etf_df.columns}")

        # 离线模式下，只获取数据不存储
        if not self.session:
            logger.info("离线模式下，跳过 ETF 基本信息存储")
            return etf_df

        # 数据清洗和标准化
        from src.database.models.stock import StockBasic

        # 遍历数据，进行清洗和存储
        updated_count = 0
        for row in etf_df.iter_rows(named=True):
            try:
                # 提取 ETF 代码和名称
                # AkShare ETF 实时行情字段：基金代码, 基金简称, 最新价, 涨跌幅, 成交量, 成交额, 最新份额, 更新时间
                symbol = str(row.get('基金代码', ''))
                name = row.get('基金简称', '')

                if not symbol:
                    continue

                # 判断 ETF 所属市场
                # 上海 ETF：510xxx, 511xxx, 512xxx, 513xxx, 515xxx, 588xxx
                # 深圳 ETF：159xxx, 150xxx, 160xxx
                if symbol.startswith('51') or symbol.startswith('58'):
                    ts_code = f"{symbol}.SH"
                elif symbol.startswith('15') or symbol.startswith('16'):
                    ts_code = f"{symbol}.SZ"
                else:
                    # 未知市场，默认跳过
                    logger.warning(f"无法判断 ETF {symbol} 的市场，跳过")
                    continue

                # 查询 ETF 是否已存在
                stock = self.session.query(StockBasic).filter_by(ts_code=ts_code).first()

                if stock:
                    # 更新现有 ETF 名称
                    if stock.name != name:
                        logger.info(f"更新 ETF 名称: {ts_code} - {stock.name} -> {name}")
                        stock.name = name
                        updated_count += 1
                else:
                    # 创建新 ETF 信息
                    logger.info(f"新增 ETF: {ts_code} - {name}")
                    stock = StockBasic(
                        ts_code=ts_code,
                        symbol=symbol[:9] if len(symbol) > 9 else symbol,
                        name=name[:45] if len(name) > 45 else name,
                        status='L'
                    )
                    self.session.add(stock)
                    updated_count += 1

            except Exception as row_e:
                logger.exception(f"处理 ETF 基本信息失败: {row_e}")
                continue

        # 提交事务
        self.session.commit()
        logger.info(f"ETF 基本信息更新完成，共更新 {updated_count} 条记录")
        return etf_df
