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
        self.session = None
        
        # 离线模式支持
        if db_manager:
            try:
                self.session = db_manager.get_session()
            except Exception as e:
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
                    if symbol.startswith('6'):
                        ts_code = f"{symbol}.SH"  # 沪市
                    else:
                        ts_code = f"{symbol}.SZ"  # 深市
                    
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
                    
                except Exception as row_e:
                    logger.exception(f"处理股票基本信息失败: {row_e}")
                    continue
            
            # 提交事务
            self.session.commit()
            logger.info("股票基本信息更新完成")
            return stock_basic_df  # 返回Polars DataFrame
            
        except Exception as e:
            if self.session:
                self.session.rollback()
            logger.exception(f"从AkShare获取股票基本信息失败: {e}")
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
                            
                        except Exception as row_e:
                            logger.exception(f"处理股票日线数据失败: {row_e}")
                            continue
                    
                    # 每只股票提交一次事务
                    self.session.commit()
                    logger.info(f"{ts_code}的日线数据更新完成")
                    
                except Exception as e:
                    if self.session:
                        self.session.rollback()
                    logger.exception(f"获取{ts_code}的日线数据失败: {e}")
                    continue
            
            # 离线模式下返回结果
            if not self.session and result:
                return result
            
        except Exception as e:
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
                    
                except Exception as row_e:
                    logger.exception(f"处理指数基本信息失败: {row_e}")
                    continue
            
            # 提交事务
            self.session.commit()
            logger.info("指数基本信息更新完成")
            return index_basic_df
            
        except Exception as e:
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
                            
                        except Exception as row_e:
                            logger.exception(f"处理指数日线数据失败: {row_e}")
                            continue
                    
                    # 每个指数提交一次事务
                    self.session.commit()
                    logger.info(f"{ts_code}的日线数据更新完成")
                    
                except Exception as e:
                    if self.session:
                        self.session.rollback()
                    logger.exception(f"获取{ts_code}的日线数据失败: {e}")
                    continue
            
            # 离线模式下返回结果
            if not self.session and result:
                return result
            
        except Exception as e:
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
                    dividend_pd = ak.stock_fhps_detail_em(symbol=symbol)

                    # 转换为Polars DataFrame
                    dividend_df = pl.from_pandas(dividend_pd)

                    if dividend_df.is_empty():
                        logger.warning(f"{ts_code}没有分红配股数据")
                        continue

                    logger.info(f"获取到{ts_code}的{dividend_df.height}条分红配股数据")

                    # 离线模式下，只获取数据不存储
                    if not self.session:
                        result[ts_code] = dividend_df
                        logger.info(f"离线模式下，获取{ts_code}的分红配股数据完成")
                        continue

                    # 数据清洗和标准化
                    from src.database.models.stock import StockDividend, StockBasic

                    # 获取股票名称
                    stock_basic = self.session.query(StockBasic).filter_by(ts_code=ts_code).first()
                    stock_name = stock_basic.name if stock_basic else ""

                    # 遍历数据，进行清洗和存储
                    for row in dividend_df.iter_rows(named=True):
                        try:
                            # 转换日期格式 - 新接口字段映射
                            # 报告期作为分红年度
                            dividend_year = None
                            if '报告期' in row and row['报告期']:
                                report_date_val = row['报告期']
                                if isinstance(report_date_val, str):
                                    dividend_year = report_date_val[:4]  # 提取年份
                                else:
                                    dividend_year = str(report_date_val.year)

                            # 业绩披露日期作为公告日期
                            report_date = None
                            if '业绩披露日期' in row and row['业绩披露日期']:
                                report_date_val = row['业绩披露日期']
                                if isinstance(report_date_val, str):
                                    report_date = datetime.strptime(report_date_val, "%Y-%m-%d").date()
                                elif isinstance(report_date_val, datetime):
                                    report_date = report_date_val.date()
                                else:
                                    report_date = report_date_val  # 已经是date类型

                            # 股权登记日
                            record_date = None
                            if '股权登记日' in row and row['股权登记日']:
                                record_date_val = row['股权登记日']
                                if isinstance(record_date_val, str) and record_date_val != 'NaT':
                                    record_date = datetime.strptime(record_date_val, "%Y-%m-%d").date()
                                elif isinstance(record_date_val, datetime):
                                    record_date = record_date_val.date()
                                else:
                                    record_date = record_date_val  # 已经是date类型

                            # 除权除息日
                            ex_date = None
                            if '除权除息日' in row and row['除权除息日']:
                                ex_date_val = row['除权除息日']
                                if isinstance(ex_date_val, str) and ex_date_val != 'NaT':
                                    ex_date = datetime.strptime(ex_date_val, "%Y-%m-%d").date()
                                elif isinstance(ex_date_val, datetime):
                                    ex_date = ex_date_val.date()
                                else:
                                    ex_date = ex_date_val  # 已经是date类型

                            # 派息日（使用除权除息日作为派息日）
                            pay_date = ex_date

                            # 提取分红方案数据 - 新接口字段
                            # 现金分红-现金分红比例: 10派X元，需要转换为每股派现
                            cash_div_ratio = row.get('现金分红-现金分红比例', 0)
                            cash_div = float(cash_div_ratio) / 10 if cash_div_ratio else 0  # 转换为每股派现

                            # 送转股份-送转总比例: 10送转X股，需要转换为每股送转
                            share_div_ratio = row.get('送转股份-送转总比例', 0)
                            share_div = float(share_div_ratio) / 10 if share_div_ratio else 0  # 转换为每股送转

                            # 送转股份-送股比例
                            gift_share_ratio = row.get('送转股份-送股比例', 0)
                            gift_share = float(gift_share_ratio) / 10 if gift_share_ratio else 0

                            # 送转股份-转股比例
                            transfer_share_ratio = row.get('送转股份-转股比例', 0)
                            transfer_share = float(transfer_share_ratio) / 10 if transfer_share_ratio else 0

                            # 总派现金额（从描述中提取或使用每股收益和总股本计算）
                            total_div = 0

                            # 查询数据是否已存在（根据股票代码和分红年度）
                            dividend_data = self.session.query(StockDividend).filter_by(
                                ts_code=ts_code,
                                dividend_year=dividend_year
                            ).first()

                            if dividend_data:
                                # 更新现有数据
                                dividend_data.report_date = report_date
                                dividend_data.record_date = record_date
                                dividend_data.ex_date = ex_date
                                dividend_data.pay_date = pay_date
                                dividend_data.cash_div = cash_div
                                dividend_data.share_div = share_div
                                dividend_data.total_div = total_div
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
                                    total_div=total_div
                                )
                                self.session.add(dividend_data)

                        except Exception as row_e:
                            logger.exception(f"处理股票分红配股数据失败: {row_e}")
                            continue

                    # 每只股票提交一次事务
                    self.session.commit()
                    logger.info(f"{ts_code}的分红配股数据更新完成")

                except Exception as e:
                    if self.session:
                        self.session.rollback()
                    logger.exception(f"获取{ts_code}的分红配股数据失败: {e}")
                    continue
            
            # 离线模式下返回结果
            if not self.session and result:
                return result
            
        except Exception as e:
            if self.session:
                self.session.rollback()
            logger.exception(f"更新股票分红配股数据失败: {e}")
            raise
