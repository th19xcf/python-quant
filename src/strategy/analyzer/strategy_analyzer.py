#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
策略分析器，用于分析技术指标并生成交易信号
"""

from typing import List, Dict, Optional, Any
import polars as pl
import pandas as pd
from loguru import logger

from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
from src.strategy.signals.signal_generator import SignalGenerator
from src.strategy.visualization.strategy_visualizer import StrategyVisualizer


class StrategyAnalyzer:
    """
    策略分析器类，提供技术指标分析和信号生成功能
    """
    
    def __init__(self, data):
        """
        初始化策略分析器
        
        Args:
            data: 股票数据，可以是Polars DataFrame、LazyFrame或Pandas DataFrame
        """
        # 初始化技术分析器
        self.tech_analyzer = TechnicalAnalyzer(data)
        # 初始化信号生成器
        self.signal_generator = SignalGenerator()
        # 初始化可视化工具
        self.visualizer = StrategyVisualizer()
        # 存储计算结果
        self.analysis_results = {}
        # 存储生成的信号
        self.signals = {}
    
    def calculate_indicators(self, indicators: List[str], **params) -> pl.DataFrame:
        """
        计算指定的技术指标
        
        Args:
            indicators: 要计算的指标列表
            **params: 指标计算参数
            
        Returns:
            pl.DataFrame: 包含计算结果的Polars DataFrame
        """
        for indicator in indicators:
            if indicator in self.tech_analyzer.indicator_mapping:
                try:
                    # 计算指标
                    result = self.tech_analyzer.calculate_indicator(indicator, **params)
                    self.analysis_results[indicator] = result
                    logger.info(f"成功计算指标: {indicator}")
                except Exception as e:
                    logger.error(f"计算指标 {indicator} 失败: {e}")
            else:
                logger.warning(f"不支持的指标: {indicator}")
        
        return self.tech_analyzer.get_data()
    
    def generate_signals(self, strategy_type: str, **params) -> Dict[str, Any]:
        """
        生成交易信号
        
        Args:
            strategy_type: 策略类型
            **params: 策略参数
            
        Returns:
            Dict[str, Any]: 包含信号的字典
        """
        data = self.tech_analyzer.get_data()
        
        if strategy_type == 'trend_following':
            # 趋势跟踪策略
            signals = self.signal_generator.generate_trend_following_signals(data, **params)
        elif strategy_type == 'mean_reversion':
            # 均值回归策略
            signals = self.signal_generator.generate_mean_reversion_signals(data, **params)
        elif strategy_type == 'momentum':
            # 动量策略
            signals = self.signal_generator.generate_momentum_signals(data, **params)
        elif strategy_type == 'volatility_breakout':
            # 波动率突破策略
            signals = self.signal_generator.generate_volatility_breakout_signals(data, **params)
        else:
            logger.error(f"不支持的策略类型: {strategy_type}")
            return {}
        
        self.signals[strategy_type] = signals
        return signals
    
    def analyze_multi_indicator(self, indicators: List[str], weights: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
        """
        多指标组合分析
        
        Args:
            indicators: 指标列表
            weights: 指标权重
            
        Returns:
            Dict[str, Any]: 分析结果
        """
        # 确保所有指标都已计算
        self.calculate_indicators(indicators)
        
        # 默认权重
        if weights is None:
            weights = {indicator: 1.0/len(indicators) for indicator in indicators}
        
        # 计算综合评分
        data = self.tech_analyzer.get_data()
        score = 0.0
        
        for indicator, weight in weights.items():
            if indicator in data.columns:
                # 根据指标类型计算得分
                if indicator in ['rsi', 'wr']:
                    # 超买超卖指标
                    values = data[indicator].to_numpy()
                    # 归一化到[-1, 1]
                    normalized = (values - 50) / 50
                    score += normalized[-1] * weight
                elif indicator in ['macd']:
                    # 趋势指标
                    values = data['macd'].to_numpy()
                    score += (values[-1] / max(abs(values))) * weight
                elif indicator in ['kdj']:
                    # KDJ指标
                    k_values = data['k'].to_numpy()
                    d_values = data['d'].to_numpy()
                    score += ((k_values[-1] - d_values[-1]) / 100) * weight
                else:
                    logger.warning(f"未实现指标 {indicator} 的评分逻辑")
        
        # 生成信号
        signal = 'buy' if score > 0.3 else 'sell' if score < -0.3 else 'hold'
        
        return {
            'score': score,
            'signal': signal,
            'weights': weights
        }
    
    def visualize_strategy(self, strategy_type: str, **params) -> Any:
        """
        可视化策略结果
        
        Args:
            strategy_type: 策略类型
            **params: 可视化参数
            
        Returns:
            Any: 可视化结果
        """
        if strategy_type not in self.signals:
            # 如果信号不存在，先生成
            self.generate_signals(strategy_type, **params)
        
        signals = self.signals[strategy_type]
        data = self.tech_analyzer.get_data()
        
        return self.visualizer.visualize_strategy(data, signals, strategy_type, **params)
    
    def get_analysis_results(self) -> Dict[str, Any]:
        """
        获取分析结果
        
        Returns:
            Dict[str, Any]: 分析结果
        """
        return {
            'indicators': self.analysis_results,
            'signals': self.signals
        }
