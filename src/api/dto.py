#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据传输对象(DTO)定义，用于各层之间的数据传输
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union
import polars as pl
import pandas as pd


@dataclass
class StockDataDTO:
    """股票数据传输对象"""
    stock_code: str
    frequency: str
    data: Union[pl.DataFrame, pd.DataFrame]
    metadata: Dict[str, Any] = field(default_factory=dict)
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    source: Optional[str] = None
    update_time: Optional[str] = None
    
    def __post_init__(self):
        """初始化后处理"""
        if self.start_date is None and len(self.data) > 0:
            self.start_date = str(self.data['date'].min())
        if self.end_date is None and len(self.data) > 0:
            self.end_date = str(self.data['date'].max())


@dataclass
class IndexDataDTO:
    """指数数据传输对象"""
    index_code: str
    frequency: str
    data: Union[pl.DataFrame, pd.DataFrame]
    metadata: Dict[str, Any] = field(default_factory=dict)
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    source: Optional[str] = None
    update_time: Optional[str] = None
    
    def __post_init__(self):
        """初始化后处理"""
        if self.start_date is None and len(self.data) > 0:
            self.start_date = str(self.data['date'].min())
        if self.end_date is None and len(self.data) > 0:
            self.end_date = str(self.data['date'].max())


@dataclass
class IndicatorResultDTO:
    """指标结果传输对象"""
    indicator_type: str
    params: Dict[str, Any] = field(default_factory=dict)
    data: Optional[Union[pl.DataFrame, pd.DataFrame]] = None
    values: Optional[Dict[str, Any]] = None
    signal: Optional[Dict[str, Any]] = None
    calculate_time: Optional[float] = None
    
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
    strategy_name: Optional[str] = None
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
    backtest_data: Optional[Union[pl.DataFrame, pd.DataFrame]] = None
    equity_curve: Optional[Union[pl.DataFrame, pd.DataFrame]] = None
    params: Dict[str, Any] = field(default_factory=dict)
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    
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
    factor_data: Union[pl.DataFrame, pd.DataFrame]
    params: Dict[str, Any] = field(default_factory=dict)
    calculate_time: Optional[float] = None
    source: Optional[str] = None
    
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
    task_name: Optional[str] = None
    task_id: Optional[str] = None
    
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
    title: Optional[str] = None
    timeout: Optional[int] = None  # 自动关闭时间（秒）
    
    def __post_init__(self):
        """初始化后处理"""
        if self.message_type not in ['info', 'warning', 'error', 'success']:
            self.message_type = 'info'