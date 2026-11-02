#!/usr/bin/env python3

"""
指数数据模型
"""

from datetime import datetime

from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Double,
    Float,
    Integer,
    String,
    UniqueConstraint,
)

from src.database.db_manager import Base


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
    # 指数点位与成交量统一用双精度：MySQL FLOAT(4 字节) 落库会引入
    # ~1e-7 相对误差，K 线比对与回测信号会受尾差影响。
    open = Column(Double, comment="开盘价")
    high = Column(Double, comment="最高价")
    low = Column(Double, comment="最低价")
    close = Column(Double, comment="收盘价")
    pre_close = Column(Double, comment="昨收价")
    change = Column(Double, comment="涨跌额")
    pct_chg = Column(Double, comment="涨跌幅(%)")
    # 单位契约同 stock_daily：vol=手（1 手=100 股），amount=元
    vol = Column(Double, comment="成交量(手，1手=100股)")
    # 成交额量级可达 1e12，用 Double 保存 float64 精度；
    # NULL 表示上游未提供成交额（如 AkShare 指数日线），不得用 0 冒充
    amount = Column(Double, comment="成交额(元)，NULL=上游未提供")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")

    __table_args__ = (
        # 业务唯一键：一个指数一个交易日只能有一行，供原子 upsert 的
        # ON DUPLICATE KEY / ON CONFLICT 命中
        UniqueConstraint('ts_code', 'trade_date', name='uq_index_daily_ts_code_trade_date'),
        {
            "comment": "指数日线行情表",
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_general_ci",
            "extend_existing": True
        }
    )

    def __repr__(self):
        return f"<IndexDaily(ts_code='{self.ts_code}', trade_date='{self.trade_date}', close={self.close})>"
