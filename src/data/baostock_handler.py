#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Baostock数据处理器
"""

from datetime import datetime
from typing import List, Optional

import baostock as bs
import polars as pl
from loguru import logger


class BaostockHandler:
    """
    Baostock数据处理器，负责从Baostock获取数据并存储到数据库
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
        self.session = None
        self.bs_login = False
        
        # 离线模式支持
        if db_manager:
            try:
                self.session = db_manager.get_session()
            except Exception as e:
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
            lg = bs.login()
            if lg.error_code == '0':
                logger.info("Baostock登录成功")
                self.bs_login = True
            else:
                logger.warning(f"Baostock登录失败: {lg.error_msg}")
                self.bs_login = False
        except Exception as e:
            logger.exception(f"Baostock登录异常: {e}")
            self.bs_login = False
    
    def _ensure_baostock_login(self):
        """
        确保Baostock已登录，如果未登录则尝试登录
        
        Returns:
            bool: 登录状态，True表示已登录，False表示登录失败
        """
        if self.bs_login:
            return True
        
        logger.info("Baostock未登录，尝试登录...")
        self._login_baostock()
        return self.bs_login
    
    def _logout_baostock(self):
        """
        登出Baostock
        """
        if self.bs_login:
            try:
                bs.logout()
                logger.info("Baostock登出成功")
                self.bs_login = False
            except Exception as e:
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
                    # 从Baostock数据中查找对应的股票信息
                    stock_row = stock_basic_df.filter(pl.col('code') == symbol)
                    if stock_row.is_empty():
                        logger.warning(f"未找到股票{symbol}的基本信息")
                        continue
                    
                    # 获取第一条匹配的记录
                    row = stock_row.row(named=True)
                    name = row['code_name']
                    
                    # 构建完整的ts_code（例如：600000.SH）
                    if symbol.startswith('6'):
                        ts_code = f"{symbol}.SH"  # 沪市
                    else:
                        ts_code = f"{symbol}.SZ"  # 深市
                    
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
                    
                except Exception as row_e:
                    logger.exception(f"处理股票{symbol}基本信息失败: {row_e}")
                    continue
            
            # 提交事务
            self.session.commit()
            logger.info("股票基本信息更新完成")
            return stock_basic_df  # 返回Polars DataFrame
            
        except Exception as e:
            if self.session:
                try:
                    self.session.rollback()
                except Exception as rollback_e:
                    logger.warning(f"回滚事务失败: {rollback_e}")
            logger.exception(f"从Baostock获取股票基本信息失败: {e}")
            raise
    
    def update_stock_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新股票日线数据
        
        Args:
            ts_codes: 股票代码列表，None表示更新所有股票
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
        
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
                    # 转换为Baostock格式（从600000.SH转换为sh.600000）
                    ts_codes = []
                    for sb in stock_basics:
                        # 转换格式：600000.SH -> sh.600000
                        parts = sb.ts_code.split('.')
                        if len(parts) == 2:
                            baostock_code = f"{parts[1].lower()}.{parts[0]}"
                            ts_codes.append(baostock_code)
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
                    
                    # 提取基础代码（去掉.SH/.SZ后缀）
                    base_code = ts_code.split('.')[0]
                    
                    # 使用Baostock获取股票日线数据
                    rs = bs.query_history_k_data_plus(
                        base_code,
                        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
                        start_date=start_date,
                        end_date=end_date,
                        frequency="d",
                        adjustflag="2"  # 前复权
                    )
                    
                    # 获取数据
                    stock_daily_pd = rs.get_data()
                    
                    # 转换为Polars DataFrame
                    stock_daily_df = pl.from_pandas(stock_daily_pd)
                    
                    if stock_daily_df.is_empty():
                        logger.warning(f"{ts_code}在{start_date}至{end_date}期间没有日线数据")
                        continue
                    
                    logger.info(f"获取到{ts_code}的{stock_daily_df.height}条日线数据")
                    
                    # 离线模式下，只获取数据不存储
                    # 离线模式下，只获取数据不存储
                    if not self.session:
                        result[ts_code] = stock_daily_df
                        logger.info(f"离线模式下，获取{ts_code}的日线数据完成")
                        continue
                    
                    # 数据清洗和标准化
                    from src.database.models.stock import StockDaily, StockBasic
                    
                    # 初始化最早交易日期
                    earliest_date = None
                    
                    # 遍历数据，进行清洗和存储
                    for row in stock_daily_df.iter_rows(named=True):
                        try:
                            # 转换日期格式
                            trade_date = datetime.strptime(row['date'], "%Y-%m-%d").date()
                            
                            # 更新最早交易日期
                            if earliest_date is None or trade_date < earliest_date:
                                earliest_date = trade_date
                            
                            # 查询数据是否已存在
                            daily_data = self.session.query(StockDaily).filter_by(
                                ts_code=ts_code,
                                trade_date=trade_date
                            ).first()
                            
                            # 转换数值类型
                            open_val = float(row['open']) if row['open'] else 0.0
                            high_val = float(row['high']) if row['high'] else 0.0
                            low_val = float(row['low']) if row['low'] else 0.0
                            close_val = float(row['close']) if row['close'] else 0.0
                            pre_close_val = float(row['preclose']) if row['preclose'] else 0.0
                            change_val = close_val - pre_close_val
                            pct_chg_val = float(row['pctChg']) if row['pctChg'] else 0.0
                            vol_val = float(row['volume']) if row['volume'] else 0.0
                            amount_val = float(row['amount']) if row['amount'] else 0.0
                            
                            if daily_data:
                                # 更新现有数据
                                daily_data.open = open_val
                                daily_data.high = high_val
                                daily_data.low = low_val
                                daily_data.close = close_val
                                daily_data.pre_close = pre_close_val
                                daily_data.change = change_val
                                daily_data.pct_chg = pct_chg_val
                                daily_data.vol = vol_val
                                daily_data.amount = amount_val
                            else:
                                # 创建新数据
                                daily_data = StockDaily(
                                    ts_code=ts_code,
                                    trade_date=trade_date,
                                    open=open_val,
                                    high=high_val,
                                    low=low_val,
                                    close=close_val,
                                    pre_close=pre_close_val,
                                    change=change_val,
                                    pct_chg=pct_chg_val,
                                    vol=vol_val,
                                    amount=amount_val
                                )
                                self.session.add(daily_data)
                            
                        except Exception as row_e:
                            logger.exception(f"处理股票日线数据失败: {row_e}")
                            continue
                    
                    # 更新股票的上市日期为最早交易日期
                    if earliest_date:
                        # 查询股票基本信息
                        stock_basic = self.session.query(StockBasic).filter_by(ts_code=ts_code).first()
                        if stock_basic:
                            # 如果还没有上市日期，或者找到更早的日期，则更新
                            if not stock_basic.list_date or earliest_date < stock_basic.list_date:
                                stock_basic.list_date = earliest_date
                                logger.info(f"更新股票{ts_code}的上市日期为: {earliest_date}")
                        else:
                            # 如果股票基本信息不存在，创建一个
                            logger.warning(f"股票{ts_code}的基本信息不存在，将创建一条新记录")
                            # 提取股票代码和名称（从Baostock返回的数据中获取）
                            first_row = stock_daily_df.row(named=True)
                            symbol = first_row['code']
                            # 由于Baostock不提供股票名称，使用代码作为名称
                            name = symbol
                            # 构建完整的ts_code
                            if symbol.startswith('6'):
                                full_ts_code = f"{symbol}.SH"
                            else:
                                full_ts_code = f"{symbol}.SZ"
                            # 创建新股票基本信息
                            new_stock = StockBasic(
                                ts_code=full_ts_code,
                                symbol=symbol,
                                name=name,
                                list_date=earliest_date
                            )
                            self.session.add(new_stock)
                            logger.info(f"创建股票{full_ts_code}的基本信息，上市日期为: {earliest_date}")
                    
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
                    
                    # 获取数据
                    index_daily_pd = rs.get_data()
                    
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
                    
                    # 遍历数据，进行清洗和存储
                    for row in index_daily_df.iter_rows(named=True):
                        try:
                            # 转换日期格式
                            trade_date = datetime.strptime(row['date'], "%Y-%m-%d").date()
                            
                            # 转换数值类型
                            open_val = float(row['open']) if row['open'] else 0.0
                            high_val = float(row['high']) if row['high'] else 0.0
                            low_val = float(row['low']) if row['low'] else 0.0
                            close_val = float(row['close']) if row['close'] else 0.0
                            pre_close_val = float(row['preclose']) if row['preclose'] else 0.0
                            change_val = close_val - pre_close_val
                            pct_chg_val = float(row['pctChg']) if row['pctChg'] else 0.0
                            vol_val = float(row['volume']) if row['volume'] else 0.0
                            amount_val = float(row['amount']) if row['amount'] else 0.0
                            
                            # 查询数据是否已存在
                            daily_data = self.session.query(IndexDaily).filter_by(
                                ts_code=ts_code,
                                trade_date=trade_date
                            ).first()
                            
                            if daily_data:
                                # 更新现有数据
                                daily_data.open = open_val
                                daily_data.high = high_val
                                daily_data.low = low_val
                                daily_data.close = close_val
                                daily_data.pre_close = pre_close_val
                                daily_data.change = change_val
                                daily_data.pct_chg = pct_chg_val
                                daily_data.vol = vol_val
                                daily_data.amount = amount_val
                            else:
                                # 创建新数据
                                daily_data = IndexDaily(
                                    ts_code=ts_code,
                                    trade_date=trade_date,
                                    open=open_val,
                                    high=high_val,
                                    low=low_val,
                                    close=close_val,
                                    pre_close=pre_close_val,
                                    change=change_val,
                                    pct_chg=pct_chg_val,
                                    vol=vol_val,
                                    amount=amount_val
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
    
    def get_realtime_stock_data(self, ts_codes: List[str] = None):
        """
        获取实时股票数据
        
        Args:
            ts_codes: 股票代码列表，None表示获取默认股票
        
        Returns:
            pl.DataFrame: 实时股票数据
        """
        try:
            if not self._ensure_baostock_login():
                logger.warning("Baostock登录失败，无法获取实时股票数据")
                return pl.DataFrame()
            
            if not ts_codes:
                ts_codes = self._get_default_codes('stock')
            
            logger.info(f"开始从Baostock获取{len(ts_codes)}只股票的历史数据作为实时数据替代")
            
            # 直接使用历史数据作为替代
            
            # 获取最近5天的数据，确保能获取到数据
            start_date, end_date = self._process_dates(None, None, default_days=self.config.data.default_realtime_days)
            
            # 存储结果
            all_data = []
            
            for ts_code in ts_codes:
                try:
                    # 使用Baostock获取最近一个交易日的数据
                    rs = bs.query_history_k_data_plus(
                        ts_code,
                        "date,code,open,high,low,close,preclose,volume,amount,pctChg",
                        start_date=start_date,
                        end_date=end_date,
                        frequency="d",
                        adjustflag="2"  # 前复权
                    )
                    
                    # 获取数据
                    stock_data_pd = rs.get_data()
                    
                    # 转换为Polars DataFrame
                    stock_data_df = pl.from_pandas(stock_data_pd)
                    
                    if not stock_data_df.is_empty():
                        all_data.append(stock_data_df)
                    
                except Exception as e:
                    logger.exception(f"获取{ts_code}的历史数据失败: {e}")
                    continue
            
            if not all_data:
                logger.warning("从Baostock获取的实时股票数据为空")
                return pl.DataFrame()
            
            # 合并所有数据
            if all_data:
                # 合并Polars DataFrames
                combined_df = pl.concat(all_data, how="vertical")
                # 按日期降序排序，确保最新的数据在前面
                combined_df = combined_df.sort(by="date", descending=True)
            else:
                combined_df = pl.DataFrame()
            
            logger.info(f"从Baostock获取到{combined_df.height}条股票实时数据")
            return combined_df
            
        except Exception as e:
            logger.exception(f"获取实时股票数据失败: {e}")
            return pl.DataFrame()
    
    def get_realtime_index_data(self, ts_codes: List[str] = None):
        """
        获取实时指数数据
        
        Args:
            ts_codes: 指数代码列表，None表示获取默认指数
        
        Returns:
            pl.DataFrame: 实时指数数据
        """
        try:
            if not self._ensure_baostock_login():
                logger.warning("Baostock登录失败，无法获取实时指数数据")
                return pl.DataFrame()
            
            if not ts_codes:
                ts_codes = self._get_default_codes('index')
            
            logger.info(f"开始从Baostock获取{len(ts_codes)}个指数的历史数据作为实时数据替代")
            
            # 直接使用历史数据作为替代
            
            # 获取最近5天的数据，确保能获取到数据
            start_date, end_date = self._process_dates(None, None, default_days=5)
            
            # 存储结果
            all_data = []
            
            for ts_code in ts_codes:
                try:
                    # 使用Baostock获取最近一个交易日的数据
                    rs = bs.query_history_k_data_plus(
                        ts_code,
                        "date,code,open,high,low,close,preclose,volume,amount,pctChg",
                        start_date=start_date,
                        end_date=end_date,
                        frequency="d",
                        adjustflag="3"  # 不复权
                    )
                    
                    # 获取数据
                    index_data_pd = rs.get_data()
                    
                    # 转换为Polars DataFrame
                    index_data_df = pl.from_pandas(index_data_pd)
                    
                    if not index_data_df.is_empty():
                        all_data.append(index_data_df)
                    
                except Exception as e:
                    logger.exception(f"获取{ts_code}的历史指数数据失败: {e}")
                    continue
            
            if not all_data:
                logger.warning("从Baostock获取的实时指数数据为空")
                return pl.DataFrame()
            
            # 合并所有数据
            if all_data:
                # 合并Polars DataFrames
                combined_df = pl.concat(all_data, how="vertical")
                # 按日期降序排序，确保最新的数据在前面
                combined_df = combined_df.sort(by="date", descending=True)
            else:
                combined_df = pl.DataFrame()
            
            logger.info(f"从Baostock获取到{combined_df.height}条指数实时数据")
            
            # 输出所有上证指数的数据，以便分析
            logger.info("所有上证指数数据:")
            for row in combined_df.iter_rows(named=True):
                if row['code'] == 'sh.000001':
                    logger.info(f"日期={row['date']}, 代码={row['code']}, 收盘={row['close']}, 涨跌幅={row['pctChg']}%")
            
            # 输出最新的上证指数数据
            for row in combined_df.iter_rows(named=True):
                if row['code'] == 'sh.000001':
                    logger.info(f"最新上证指数数据: 日期={row['date']}, 代码={row['code']}, 收盘={row['close']}, 涨跌幅={row['pctChg']}%")
                    break
            
            return combined_df
            
        except Exception as e:
            logger.exception(f"获取实时指数数据失败: {e}")
            return pl.DataFrame()
    
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
    
    def download_stock_minute(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None, frequency: str = "5"):
        """
        下载股票分钟线数据
        
        Args:
            ts_codes: 股票代码列表，None表示下载默认股票
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 分钟线频率，支持1、5、15、30、60分钟
        
        Returns:
            dict: 股票代码到分钟线数据的映射
        """
        try:
            if not self._ensure_baostock_login():
                logger.warning("Baostock登录失败，无法获取股票分钟线数据")
                return
            
            if not ts_codes:
                ts_codes = self._get_default_codes('stock')
            
            if not ts_codes:
                logger.warning("没有需要下载的股票代码")
                return
            
            # 验证分钟线频率
            frequency = self._validate_frequency(frequency)
            
            # 处理日期参数
            start_date, end_date = self._process_dates(start_date, end_date, default_days=self.config.data.default_minute_days)
            
            logger.info(f"开始下载{len(ts_codes)}只股票的{frequency}分钟线数据，时间范围: {start_date} 至 {end_date}")
            
            # 存储结果
            result = {}
            
            # 遍历股票代码，获取分钟线数据
            for ts_code in ts_codes:
                try:
                    logger.info(f"正在下载{ts_code}的{frequency}分钟线数据")
                    
                    # 使用Baostock获取股票分钟线数据
                    rs = bs.query_history_k_data_plus(
                        ts_code,
                        "date,time,code,open,high,low,close,volume,amount",
                        start_date=start_date,
                        end_date=end_date,
                        frequency=frequency,
                        adjustflag="2"  # 前复权
                    )
                    
                    # 获取数据
                    stock_minute_pd = rs.get_data()
                    
                    # 转换为Polars DataFrame
                    stock_minute_df = pl.from_pandas(stock_minute_pd)
                    
                    if stock_minute_df.is_empty():
                        logger.warning(f"{ts_code}在{start_date}至{end_date}期间没有{frequency}分钟线数据")
                        continue
                    
                    logger.info(f"获取到{ts_code}的{stock_minute_df.height}条{frequency}分钟线数据")
                    
                    # 离线模式下，只获取数据不存储
                    if not self.session:
                        result[ts_code] = stock_minute_pd
                        logger.info(f"离线模式下，获取{ts_code}的{frequency}分钟线数据完成")
                        continue
                    
                    # 数据清洗和标准化
                    from src.database.models.stock import StockMinute
                    
                    # 遍历数据，进行清洗和存储
                    for row in stock_minute_df.iter_rows(named=True):
                        try:
                            # 转换日期时间格式
                            trade_date = datetime.strptime(row['date'], "%Y-%m-%d").date()
                            trade_time = row['time']
                            
                            # 查询数据是否已存在
                            minute_data = self.session.query(StockMinute).filter_by(
                                ts_code=ts_code,
                                trade_date=trade_date,
                                trade_time=trade_time
                            ).first()
                            
                            # 转换数值类型
                            open_val = float(row['open']) if row['open'] else 0.0
                            high_val = float(row['high']) if row['high'] else 0.0
                            low_val = float(row['low']) if row['low'] else 0.0
                            close_val = float(row['close']) if row['close'] else 0.0
                            vol_val = float(row['volume']) if row['volume'] else 0.0
                            amount_val = float(row['amount']) if row['amount'] else 0.0
                            
                            if minute_data:
                                # 更新现有数据
                                minute_data.open = open_val
                                minute_data.high = high_val
                                minute_data.low = low_val
                                minute_data.close = close_val
                                minute_data.vol = vol_val
                                minute_data.amount = amount_val
                            else:
                                # 创建新数据
                                minute_data = StockMinute(
                                    ts_code=ts_code,
                                    trade_date=trade_date,
                                    trade_time=trade_time,
                                    open=open_val,
                                    high=high_val,
                                    low=low_val,
                                    close=close_val,
                                    vol=vol_val,
                                    amount=amount_val,
                                    frequency=frequency
                                )
                                self.session.add(minute_data)
                            
                        except Exception as row_e:
                            logger.exception(f"处理股票分钟线数据失败: {row_e}")
                            continue
                    
                    # 每只股票提交一次事务
                    self.session.commit()
                    logger.info(f"{ts_code}的{frequency}分钟线数据下载完成")
                    
                except Exception as e:
                    if self.session:
                        self.session.rollback()
                    logger.exception(f"下载{ts_code}的{frequency}分钟线数据失败: {e}")
                    continue
            
            # 离线模式下返回结果
            if not self.session and result:
                return result
            
        except Exception as e:
            if self.session:
                self.session.rollback()
            logger.exception(f"下载股票分钟线数据失败: {e}")
            raise
    
    def download_index_minute(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None, frequency: str = "5"):
        """
        下载指数分钟线数据
        
        Args:
            ts_codes: 指数代码列表，None表示下载默认指数
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 分钟线频率，支持1、5、15、30、60分钟
        
        Returns:
            dict: 指数代码到分钟线数据的映射
        """
        try:
            if not self._ensure_baostock_login():
                logger.warning("Baostock登录失败，无法获取指数分钟线数据")
                return
            
            if not ts_codes:
                ts_codes = self._get_default_codes('index')
            
            if not ts_codes:
                logger.warning("没有需要下载的指数代码")
                return
            
            # 验证分钟线频率
            frequency = self._validate_frequency(frequency)
            
            # 处理日期参数
            start_date, end_date = self._process_dates(start_date, end_date, default_days=3)
            
            logger.info(f"开始下载{len(ts_codes)}个指数的{frequency}分钟线数据，时间范围: {start_date} 至 {end_date}")
            
            # 存储结果
            result = {}
            
            # 遍历指数代码，获取分钟线数据
            for ts_code in ts_codes:
                try:
                    logger.info(f"正在下载{ts_code}的{frequency}分钟线数据")
                    
                    # 使用Baostock获取指数分钟线数据
                    rs = bs.query_history_k_data_plus(
                        ts_code,
                        "date,time,code,open,high,low,close,volume,amount",
                        start_date=start_date,
                        end_date=end_date,
                        frequency=frequency,
                        adjustflag="3"  # 不复权
                    )
                    
                    # 获取数据
                    index_minute_pd = rs.get_data()
                    
                    # 转换为Polars DataFrame
                    index_minute_df = pl.from_pandas(index_minute_pd)
                    
                    if index_minute_df.is_empty():
                        logger.warning(f"{ts_code}在{start_date}至{end_date}期间没有{frequency}分钟线数据")
                        continue
                    
                    logger.info(f"获取到{ts_code}的{index_minute_df.height}条{frequency}分钟线数据")
                    
                    # 离线模式下，只获取数据不存储
                    if not self.session:
                        result[ts_code] = index_minute_pd
                        logger.info(f"离线模式下，获取{ts_code}的{frequency}分钟线数据完成")
                        continue
                    
                    # 数据清洗和标准化
                    from src.database.models.index import IndexMinute
                    
                    # 遍历数据，进行清洗和存储
                    for row in index_minute_df.iter_rows(named=True):
                        try:
                            # 转换日期时间格式
                            trade_date = datetime.strptime(row['date'], "%Y-%m-%d").date()
                            trade_time = row['time']
                            
                            # 查询数据是否已存在
                            minute_data = self.session.query(IndexMinute).filter_by(
                                ts_code=ts_code,
                                trade_date=trade_date,
                                trade_time=trade_time
                            ).first()
                            
                            # 转换数值类型
                            open_val = float(row['open']) if row['open'] else 0.0
                            high_val = float(row['high']) if row['high'] else 0.0
                            low_val = float(row['low']) if row['low'] else 0.0
                            close_val = float(row['close']) if row['close'] else 0.0
                            vol_val = float(row['volume']) if row['volume'] else 0.0
                            amount_val = float(row['amount']) if row['amount'] else 0.0
                            
                            if minute_data:
                                # 更新现有数据
                                minute_data.open = open_val
                                minute_data.high = high_val
                                minute_data.low = low_val
                                minute_data.close = close_val
                                minute_data.vol = vol_val
                                minute_data.amount = amount_val
                            else:
                                # 创建新数据
                                minute_data = IndexMinute(
                                    ts_code=ts_code,
                                    trade_date=trade_date,
                                    trade_time=trade_time,
                                    open=open_val,
                                    high=high_val,
                                    low=low_val,
                                    close=close_val,
                                    vol=vol_val,
                                    amount=amount_val,
                                    frequency=frequency
                                )
                                self.session.add(minute_data)
                            
                        except Exception as row_e:
                            logger.exception(f"处理指数分钟线数据失败: {row_e}")
                            continue
                    
                    # 每个指数提交一次事务
                    self.session.commit()
                    logger.info(f"{ts_code}的{frequency}分钟线数据下载完成")
                    
                except Exception as e:
                    if self.session:
                        self.session.rollback()
                    logger.exception(f"下载{ts_code}的{frequency}分钟线数据失败: {e}")
                    continue
            
            # 离线模式下返回结果
            if not self.session and result:
                return result
            
        except Exception as e:
            if self.session:
                self.session.rollback()
            logger.exception(f"下载指数分钟线数据失败: {e}")
            raise