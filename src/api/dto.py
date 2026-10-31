#!/usr/bin/env python3

"""
数据传输对象(DTO)定义，用于各层之间的数据传输
统一使用Polars DataFrame作为数据格式

预留契约：本文件全部 DTO 当前无实现方与调用方（全仓仅本文件引用），
保留供未来扩展，不要把它们当作已生效的约束。
"""

from dataclasses import dataclass, field
from typing import Any, Dict

import polars as pl


def _resolve_date_range(data: pl.DataFrame, start_date: str | None,
                        end_date: str | None) -> tuple[str | None, str | None]:
    """data 非空时用实际首末日期补齐缺失的起止日期"""
    if len(data) == 0:
        return start_date, end_date
    if start_date is None:
        start_date = str(data['date'].min())
    if end_date is None:
        end_date = str(data['date'].max())
    return start_date, end_date


@dataclass
class StockDataDTO:
    """股票数据传输对象"""
    stock_code: str
    frequency: str
    data: pl.DataFrame
    metadata: Dict[str, Any] = field(default_factory=dict)
    start_date: str | None = None
    end_date: str | None = None
    source: str | None = None
    update_time: str | None = None

    def __post_init__(self):
        """初始化后处理：未显式提供起止日期时按数据实际范围补齐"""
        self.start_date, self.end_date = _resolve_date_range(
            self.data, self.start_date, self.end_date)


@dataclass
class IndexDataDTO:
    """指数数据传输对象"""
    index_code: str
    frequency: str
    data: pl.DataFrame
    metadata: Dict[str, Any] = field(default_factory=dict)
    start_date: str | None = None
    end_date: str | None = None
    source: str | None = None
    update_time: str | None = None

    def __post_init__(self):
        """初始化后处理：未显式提供起止日期时按数据实际范围补齐"""
        self.start_date, self.end_date = _resolve_date_range(
            self.data, self.start_date, self.end_date)


@dataclass
class IndicatorResultDTO:
    """指标结果传输对象"""
    indicator_type: str
    params: Dict[str, Any] = field(default_factory=dict)
    data: pl.DataFrame | None = None
    values: Dict[str, Any] | None = None
    signal: Dict[str, Any] | None = None
    calculate_time: float | None = None

    def __post_init__(self):
        """初始化后处理"""
        if self.data is None and self.values is None:
            raise ValueError("至少需要提供data或values中的一个")


@dataclass
class SignalDTO:
    """信号传输对象"""
    signal_type: str  # 'buy'（买入）、'sell'（卖出）、'hold'（持有）、'wait'（观望）
    stock_code: str
    timestamp: str
    price: float
    indicators: Dict[str, Any] = field(default_factory=dict)
    confidence: float = 0.5  # 信号置信度（0-1）
    strategy_name: str | None = None
    params: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """初始化后处理"""
        if self.confidence < 0 or self.confidence > 1:
            raise ValueError("置信度必须在0-1之间")


@dataclass
class BacktestResultDTO:
    """回测结果传输对象"""
    strategy_name: str
    total_return: float
    annual_return: float
    max_drawdown: float
    sharpe_ratio: float
    win_rate: float
    trade_count: int
    backtest_data: pl.DataFrame | None = None
    equity_curve: pl.DataFrame | None = None
    params: Dict[str, Any] = field(default_factory=dict)
    start_date: str | None = None
    end_date: str | None = None

    def __post_init__(self):
        """初始化后处理"""
        # 确保数值类型正确
        self.total_return = float(self.total_return)
        self.annual_return = float(self.annual_return)
        self.max_drawdown = float(self.max_drawdown)
        self.sharpe_ratio = float(self.sharpe_ratio)
        self.win_rate = float(self.win_rate)
        self.trade_count = int(self.trade_count)


@dataclass
class FactorResultDTO:
    """因子计算结果传输对象"""
    factor_name: str
    factor_data: pl.DataFrame
    params: Dict[str, Any] = field(default_factory=dict)
    calculate_time: float | None = None
    source: str | None = None

    def __post_init__(self):
        """初始化后处理"""
        if 'date' not in self.factor_data.columns:
            raise ValueError("因子数据必须包含date列")
        if self.factor_name not in self.factor_data.columns:
            raise ValueError(f"因子数据必须包含{self.factor_name}列")


@dataclass
class ProgressDTO:
    """进度传输对象"""
    progress: int  # 进度值（0-100）
    message: str = ""
    task_name: str | None = None
    task_id: str | None = None

    def __post_init__(self):
        """初始化后处理"""
        if self.progress < 0:
            self.progress = 0
        elif self.progress > 100:
            self.progress = 100


@dataclass
class MessageDTO:
    """消息传输对象"""
    message: str
    message_type: str = 'info'  # 'info'、'warning'、'error'、'success'
    title: str | None = None
    timeout: int | None = None  # 自动关闭时间（秒）

    def __post_init__(self):
        """初始化后处理"""
        if self.message_type not in ['info', 'warning', 'error', 'success']:
            self.message_type = 'info'
