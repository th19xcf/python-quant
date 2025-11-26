#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
股票数据模型
"""

from sqlalchemy import Column, Integer, String, Float, Date, DateTime
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

from src.database.db_manager import Base


class StockBasic(Base):
    """
    股票基本信息表
    """
    __tablename__ = "stock_basic"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), unique=True, nullable=False, comment="股票代码")
    symbol = Column(String(10), comment="股票代码(数字)")
    name = Column(String(20), comment="股票名称")
    area = Column(String(10), comment="地域")
    industry = Column(String(20), comment="行业")
    market = Column(String(10), comment="市场类型:主板/中小板/创业板/科创板")
    list_date = Column(Date, comment="上市日期")
    delist_date = Column(Date, comment="退市日期")
    status = Column(String(10), comment="上市状态: L上市 D退市 P暂停上市")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")
    
    def __repr__(self):
        return f"<StockBasic(ts_code='{self.ts_code}', name='{self.name}')>"


class StockDaily(Base):
    """
    股票日线行情表
    """
    __tablename__ = "stock_daily"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), nullable=False, comment="股票代码")
    trade_date = Column(Date, nullable=False, comment="交易日期")
    open = Column(Float, comment="开盘价")
    high = Column(Float, comment="最高价")
    low = Column(Float, comment="最低价")
    close = Column(Float, comment="收盘价")
    pre_close = Column(Float, comment="昨收价")
    change = Column(Float, comment="涨跌额")
    pct_chg = Column(Float, comment="涨跌幅(%)")
    vol = Column(Float, comment="成交量(手)")
    amount = Column(Float, comment="成交额(千元)")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")
    
    __table_args__ = (
        {
            "comment": "股票日线行情表",
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_general_ci",
            "extend_existing": True
        }
    )
    
    def __repr__(self):
        return f"<StockDaily(ts_code='{self.ts_code}', trade_date='{self.trade_date}', close={self.close})>"


class StockMinute(Base):
    """
    股票分钟线行情表
    """
    __tablename__ = "stock_minute"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), nullable=False, comment="股票代码")
    trade_time = Column(DateTime, nullable=False, comment="交易时间")
    open = Column(Float, comment="开盘价")
    high = Column(Float, comment="最高价")
    low = Column(Float, comment="最低价")
    close = Column(Float, comment="收盘价")
    vol = Column(Float, comment="成交量(手)")
    amount = Column(Float, comment="成交额(千元)")
    freq = Column(String(10), nullable=False, comment="周期: 1min, 5min, 15min, 30min, 60min")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")
    
    __table_args__ = (
        {
            "comment": "股票分钟线行情表",
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_general_ci",
            "extend_existing": True
        }
    )
    
    def __repr__(self):
        return f"<StockMinute(ts_code='{self.ts_code}', trade_time='{self.trade_time}', freq='{self.freq}')>"
