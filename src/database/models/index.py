#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
指数数据模型
"""

from sqlalchemy import Column, Integer, String, Float, Date, DateTime
from datetime import datetime

from database.db_manager import Base


class IndexBasic(Base):
    """
    指数基本信息表
    """
    __tablename__ = "index_basic"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), unique=True, nullable=False, comment="指数代码")
    name = Column(String(20), comment="指数名称")
    market = Column(String(10), comment="市场类型")
    publisher = Column(String(20), comment="发布商")
    index_type = Column(String(20), comment="指数类型")
    category = Column(String(20), comment="指数分类")
    base_date = Column(Date, comment="基期")
    base_point = Column(Float, comment="基点")
    list_date = Column(Date, comment="发布日期")
    weight_rule = Column(String(100), comment="加权方式")
    desc = Column(String(200), comment="描述")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")
    
    def __repr__(self):
        return f"<IndexBasic(ts_code='{self.ts_code}', name='{self.name}')>"


class IndexDaily(Base):
    """
    指数日线行情表
    """
    __tablename__ = "index_daily"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), nullable=False, comment="指数代码")
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
            "comment": "指数日线行情表",
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_general_ci",
            "extend_existing": True
        }
    )
    
    def __repr__(self):
        return f"<IndexDaily(ts_code='{self.ts_code}', trade_date='{self.trade_date}', close={self.close})>"
