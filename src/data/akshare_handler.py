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
