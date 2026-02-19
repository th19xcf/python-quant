#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
业务逻辑层接口定义
统一使用Polars DataFrame作为数据格式
"""

from typing import Any, Dict, List, Optional
import polars as pl


class ITechnicalAnalyzer:
    """技术分析器接口，定义技术指标计算方法"""
    
    def calculate_indicator(self, data: pl.DataFrame, indicator_type: str, **params) -> pl.DataFrame:
        """计算单个技术指标
        
        Args:
            data: 股票数据（Polars DataFrame）
            indicator_type: 指标类型，如：'ma'、'macd'、'kdj'等
            **params: 指标计算参数
        
        Returns:
            pl.DataFrame: 包含计算结果的数据
        """
        pass
    
    def calculate_all_indicators(self, data: pl.DataFrame, indicator_types: Optional[List[str]] = None, **params) -> pl.DataFrame:
        """计算多个技术指标
        
        Args:
            data: 股票数据（Polars DataFrame）
            indicator_types: 指标类型列表，默认计算所有指标
            **params: 指标计算参数
        
        Returns:
            pl.DataFrame: 包含所有计算结果的数据
        """
        pass
    
    def get_supported_indicators(self) -> List[str]:
        """获取支持的技术指标列表
        
        Returns:
            List[str]: 支持的技术指标列表
        """
        pass
    
    def is_indicator_supported(self, indicator_type: str) -> bool:
        """检查是否支持指定的技术指标
        
        Args:
            indicator_type: 指标类型
        
        Returns:
            bool: 是否支持
        """
        pass
    
    def clear_calculation_cache(self, indicator_type: Optional[str] = None) -> bool:
        """清除指标计算缓存
        
        Args:
            indicator_type: 指标类型，None表示清除所有指标缓存
        
        Returns:
            bool: 清除是否成功
        """
        pass


class IStrategyAnalyzer:
    """策略分析器接口，定义策略分析和回测方法"""
    
    def backtest(self, data: pl.DataFrame, strategy: Any, **params) -> Dict[str, Any]:
        """回测策略
        
        Args:
            data: 股票数据（Polars DataFrame）
            strategy: 策略对象或策略名称
            **params: 回测参数
        
        Returns:
            Dict[str, Any]: 回测结果，包含收益率、最大回撤等指标
        """
        pass
    
    def evaluate_performance(self, backtest_results: Dict[str, Any]) -> Dict[str, Any]:
        """评估策略绩效
        
        Args:
            backtest_results: 回测结果
        
        Returns:
            Dict[str, Any]: 绩效评估结果
        """
        pass
    
    def optimize_parameters(self, data: pl.DataFrame, strategy: Any, param_space: Dict[str, List[Any]]) -> Dict[str, Any]:
        """优化策略参数
        
        Args:
            data: 股票数据（Polars DataFrame）
            strategy: 策略对象或策略名称
            param_space: 参数空间，如：{'fast_period': [5, 10, 15], 'slow_period': [20, 25, 30]}
        
        Returns:
            Dict[str, Any]: 优化结果，包含最优参数和对应的绩效指标
        """
        pass
    
    def get_supported_strategies(self) -> List[str]:
        """获取支持的策略列表
        
        Returns:
            List[str]: 支持的策略列表
        """
        pass


class ISignalGenerator:
    """信号生成器接口，定义买卖信号生成方法"""
    
    def generate_signals(self, data: pl.DataFrame, indicators: Optional[List[str]] = None, **params) -> pl.DataFrame:
        """生成买卖信号
        
        Args:
            data: 股票数据，包含技术指标（Polars DataFrame）
            indicators: 用于生成信号的指标列表
            **params: 信号生成参数
        
        Returns:
            pl.DataFrame: 包含信号的数据
        """
        pass
    
    def get_signal_rules(self) -> List[Dict[str, Any]]:
        """获取信号生成规则
        
        Returns:
            List[Dict[str, Any]]: 信号生成规则列表
        """
        pass
    
    def validate_signals(self, signals_data: pl.DataFrame) -> Dict[str, Any]:
        """验证信号有效性
        
        Args:
            signals_data: 包含信号的数据（Polars DataFrame）
        
        Returns:
            Dict[str, Any]: 验证结果
        """
        pass


class IFactorAnalyzer:
    """因子分析器接口，定义因子计算和分析方法"""
    
    def calculate_factor(self, data: pl.DataFrame, factor_type: str, **params) -> pl.DataFrame:
        """计算因子值
        
        Args:
            data: 股票数据（Polars DataFrame）
            factor_type: 因子类型
            **params: 因子计算参数
        
        Returns:
            pl.DataFrame: 包含因子值的数据
        """
        pass
    
    def evaluate_factor(self, factor_data: pl.DataFrame) -> Dict[str, Any]:
        """评估因子有效性
        
        Args:
            factor_data: 包含因子值的数据（Polars DataFrame）
        
        Returns:
            Dict[str, Any]: 因子评估结果
        """
        pass
    
    def get_supported_factors(self) -> List[str]:
        """获取支持的因子列表
        
        Returns:
            List[str]: 支持的因子列表
        """
        pass
