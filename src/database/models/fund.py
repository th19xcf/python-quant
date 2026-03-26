#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
基金数据模型
"""

from datetime import datetime

from sqlalchemy import Column, Integer, String, Float, Date, DateTime

from src.database.db_manager import Base


class FundBasic(Base):
    """
    基金基本信息表
    """
    __tablename__ = "fund_basic"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), unique=True, nullable=False, comment="基金代码")
    symbol = Column(String(10), comment="基金代码(数字)")
    name = Column(String(20), comment="基金名称")
    fund_type = Column(String(10), comment="基金类型")
    market = Column(String(10), comment="市场类型")
    list_date = Column(Date, comment="上市日期")
    delist_date = Column(Date, comment="退市日期")
    status = Column(String(10), comment="上市状态: L上市 D退市")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")
    
    __table_args__ = (
        {
            "comment": "基金基本信息表",
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_general_ci",
            "extend_existing": True
        }
    )

    def __repr__(self):
        return f"<FundBasic(ts_code='{self.ts_code}', name='{self.name}')>"


class FundDaily(Base):
    """
    基金日线行情表
    """
    __tablename__ = "fund_daily"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), nullable=False, comment="基金代码")
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
            "comment": "基金日线行情表",
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_general_ci",
            "extend_existing": True
        }
    )
    
    def __repr__(self):
        return f"<FundDaily(ts_code='{self.ts_code}', trade_date='{self.trade_date}', close={self.close})>"


class ClosedFundBasic(Base):
    """
    封闭式基金基本信息表
    """
    __tablename__ = "closed_fund_basic"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), unique=True, nullable=False, comment="基金代码")
    symbol = Column(String(10), comment="基金代码(数字)")
    name = Column(String(20), comment="基金名称")
    fund_type = Column(String(10), comment="基金类型")
    market = Column(String(10), comment="市场类型")
    list_date = Column(Date, comment="上市日期")
    delist_date = Column(Date, comment="退市日期")
    status = Column(String(10), comment="上市状态: L上市 D退市")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")
    
    __table_args__ = (
        {
            "comment": "封闭式基金基本信息表",
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_general_ci",
            "extend_existing": True
        }
    )

    def __repr__(self):
        return f"<ClosedFundBasic(ts_code='{self.ts_code}', name='{self.name}')>"


class ClosedFundDaily(Base):
    """
    封闭式基金日线行情表
    """
    __tablename__ = "closed_fund_daily"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), nullable=False, comment="基金代码")
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
            "comment": "封闭式基金日线行情表",
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_general_ci",
            "extend_existing": True
        }
    )
    
    def __repr__(self):
        return f"<ClosedFundDaily(ts_code='{self.ts_code}', trade_date='{self.trade_date}', close={self.close})>"
