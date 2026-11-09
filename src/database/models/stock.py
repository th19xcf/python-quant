#!/usr/bin/env python3

"""
股票数据模型
"""

from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Double,
    Float,
    Index,
    Integer,
    String,
    UniqueConstraint,
)

from src.database.db_manager import Base


class StockBasic(Base):
    """
    股票基本信息表
    """
    __tablename__ = "stock_basic"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), unique=True, nullable=False, comment="股票代码")
    symbol = Column(String(10), nullable=False, server_default='', comment="股票代码(数字)")
    name = Column(String(50), comment="股票名称")
    area = Column(String(20), comment="地域")
    industry = Column(String(50), comment="行业")
    market = Column(String(20), comment="市场类型:主板/中小板/创业板/科创板")
    list_date = Column(Date, comment="上市日期")
    delist_date = Column(Date, comment="退市日期")
    status = Column(String(10), comment="上市状态: L上市 D退市 P暂停上市")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")

    __table_args__ = (
        {
            "comment": "股票基本信息表",
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_general_ci",
            "extend_existing": True
        }
    )

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
    # 价格与复权因子必须保持双精度：MySQL float(4 字节) 落库会引入
    # ~1e-7 相对误差，复权因子连乘后放大，影响回测收益与信号。
    open = Column(Double, comment="开盘价")
    high = Column(Double, comment="最高价")
    low = Column(Double, comment="最低价")
    close = Column(Double, comment="收盘价")
    pre_close = Column(Double, comment="昨收价")
    change = Column(Float, comment="涨跌额")
    pct_chg = Column(Float, comment="涨跌幅(%)")
    # 单位契约（与数据源归一化、upsert 写入、DataFetcher 读取路径一致）：
    # vol 为「手」（1 手 = 100 股），amount 为「元」，两者都不得再二次换算。
    vol = Column(Float, comment="成交量(手，1手=100股)")
    amount = Column(Float, comment="成交额(元)")

    # 复权因子（用于实时计算复权价格）；Double 对应 MySQL DOUBLE(8 字节)
    qfq_factor = Column(Double, default=1.0, comment="前复权因子")
    hfq_factor = Column(Double, default=1.0, comment="后复权因子")

    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")

    __table_args__ = (
        # 业务唯一键：一个代码一个交易日只能有一行，供原子 upsert 的
        # ON DUPLICATE KEY / ON CONFLICT 命中
        UniqueConstraint('ts_code', 'trade_date', name='uq_stock_daily_ts_code_trade_date'),
        # 添加ts_code和trade_date的组合索引，优化按股票代码和日期范围查询
        Index('idx_stock_daily_ts_code_trade_date', 'ts_code', 'trade_date'),
        # 添加trade_date的索引，优化按日期范围查询所有股票
        Index('idx_stock_daily_trade_date', 'trade_date'),
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
    # 单位契约同 stock_daily：vol=手（1 手=100 股），amount=元
    vol = Column(Float, comment="成交量(手，1手=100股)")
    amount = Column(Float, comment="成交额(元)")
    freq = Column(String(10), nullable=False, comment="周期: 1min, 5min, 15min, 30min, 60min")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")

    __table_args__ = (
        # 业务唯一键：分钟线身份为「代码 + 周期 + 时间」，缺一不可
        UniqueConstraint('ts_code', 'freq', 'trade_time',
                         name='uq_stock_minute_ts_code_freq_trade_time'),
        # 添加ts_code、freq和trade_time的组合索引，优化按股票代码、周期和时间范围查询
        Index('idx_stock_minute_ts_code_freq_trade_time', 'ts_code', 'freq', 'trade_time'),
        # 添加trade_time的索引，优化按时间范围查询所有股票
        Index('idx_stock_minute_trade_time', 'trade_time'),
        {
            "comment": "股票分钟线行情表",
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_general_ci",
            "extend_existing": True
        }
    )

    def __repr__(self):
        return f"<StockMinute(ts_code='{self.ts_code}', trade_time='{self.trade_time}', freq='{self.freq}')>"


class StockDividend(Base):
    """
    股票分红配股表
    """
    __tablename__ = "stock_dividend"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), nullable=False, comment="股票代码")
    symbol = Column(String(10), comment="股票代码(数字)")
    name = Column(String(20), comment="股票名称")

    # 分红信息
    dividend_year = Column(String(10), comment="分红年度")
    report_date = Column(Date, comment="公告日期")
    record_date = Column(Date, comment="股权登记日")
    ex_date = Column(Date, comment="除权除息日")
    pay_date = Column(Date, comment="派息日")

    # 分红方案
    cash_div = Column(Float, comment="每股派现(元)")
    share_div = Column(Float, comment="每股送转(股)")
    total_div = Column(Float, comment="派现总额(亿元)")

    # 配股信息
    rights_issue_price = Column(Float, comment="配股价")
    rights_issue_ratio = Column(Float, comment="配股比例")

    # 人工修正列：非空时覆盖同名原始字段（原始值保留可追溯），
    # 供复权因子计算使用，避免远程刷新把修正抹掉
    cash_div_fix = Column(Float, nullable=True, comment="每股派现修正值(元)")
    share_div_fix = Column(Float, nullable=True, comment="每股送转修正值(股)")

    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")

    __table_args__ = (
        # 添加ts_code的索引，优化按股票代码查询
        Index('idx_stock_dividend_ts_code', 'ts_code'),
        # 添加dividend_year的索引，优化按年度查询
        Index('idx_stock_dividend_dividend_year', 'dividend_year'),
        {
            "comment": "股票分红配股表",
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_general_ci",
            "extend_existing": True
        }
    )

    def __repr__(self):
        return f"<StockDividend(ts_code='{self.ts_code}', dividend_year='{self.dividend_year}', cash_div={self.cash_div})>"


class StockAdjFactor(Base):
    """
    股票复权因子表
    存储每日的复权因子，用于计算前复权和后复权价格
    """
    __tablename__ = "stock_adj_factor"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), nullable=False, comment="股票代码")
    trade_date = Column(Date, nullable=False, comment="交易日期")

    # 复权因子；Double 对应 MySQL DOUBLE(8 字节)
    qfq_factor = Column(Double, default=1.0, comment="前复权因子")
    hfq_factor = Column(Double, default=1.0, comment="后复权因子")

    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")

    __table_args__ = (
        # 添加ts_code和trade_date的组合索引，优化按股票代码和日期范围查询
        Index('idx_stock_adj_factor_ts_code_trade_date', 'ts_code', 'trade_date'),
        # 添加trade_date的索引，优化按日期范围查询所有股票
        Index('idx_stock_adj_factor_trade_date', 'trade_date'),
        # 业务唯一键：一个股票一个交易日只能有一行，供原子 upsert 的
        # ON DUPLICATE KEY / ON CONFLICT 命中（迁移 0012 落库）
        UniqueConstraint('ts_code', 'trade_date',
                         name='uq_stock_adj_factor_ts_code_trade_date'),
        {
            "comment": "股票复权因子表",
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_general_ci",
            "extend_existing": True
        }
    )

    def __repr__(self):
        return f"<StockAdjFactor(ts_code='{self.ts_code}', trade_date='{self.trade_date}', qfq={self.qfq_factor}, hfq={self.hfq_factor})>"


class StockStatusHistory(Base):
    """
    股票 ST 状态时间维表

    记录 (ts_code, start_date, end_date) 区间内的 ST 状态，供回测按
    trade_date 精确判定涨跌停幅度。end_date 为 None 表示当前仍有效。

    数据源限制：akshare 仅提供"当前"风险警示板名单，无历史戴帽/摘帽日期；
    因此当前实现只能记录从 coverage_start 起的近似区间，更早的历史日期
    仍按未知（None）处理，由回测结果 st_unknown 标记数据质量。
    """
    __tablename__ = "stock_status_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), nullable=False, comment="股票代码")
    start_date = Column(Date, nullable=False, comment="区间开始日期")
    end_date = Column(Date, nullable=True, comment="区间结束日期，None 表示当前仍有效")
    is_st = Column(Boolean, default=True, nullable=False, comment="是否 ST")
    source = Column(String(30), comment="数据来源")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")

    __table_args__ = (
        Index('idx_stock_status_history_ts_code_start', 'ts_code', 'start_date', unique=True),
        Index('idx_stock_status_history_range', 'start_date', 'end_date'),
        {
            "comment": "股票 ST 状态时间维表",
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_general_ci",
            "extend_existing": True
        }
    )

    def __repr__(self):
        return (f"<StockStatusHistory(ts_code='{self.ts_code}', "
                f"start={self.start_date}, end={self.end_date}, is_st={self.is_st})>")


class MarketBreadth(Base):
    """
    市场涨跌家数表
    存储每日A股市场的上涨、下跌、平盘家数
    """
    __tablename__ = "market_breadth"

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(Date, nullable=False, unique=True, comment="交易日期")

    # 涨跌家数统计
    up_count = Column(Integer, default=0, comment="上涨家数")
    down_count = Column(Integer, default=0, comment="下跌家数")
    flat_count = Column(Integer, default=0, comment="平盘家数")
    total_count = Column(Integer, default=0, comment="总成交股票数")

    # 计算指标
    up_rate = Column(Float, default=0.0, comment="上涨占比(%)")
    down_rate = Column(Float, default=0.0, comment="下跌占比(%)")

    # 市场类型
    market_type = Column(String(10), default="A股", comment="市场类型: A股/创业板/科创板")

    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")

    __table_args__ = (
        {
            "comment": "市场涨跌家数表",
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_general_ci",
            "extend_existing": True
        }
    )

    def __repr__(self):
        return f"<MarketBreadth(trade_date='{self.trade_date}', up={self.up_count}, down={self.down_count})>"
