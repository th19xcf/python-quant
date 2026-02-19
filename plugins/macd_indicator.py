#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MACD指标插件
通过IndicatorManager调用核心计算模块，避免重复实现
"""

from loguru import logger
from src.plugin.plugin_base import IndicatorPlugin
from src.tech_analysis.indicator_manager import global_indicator_manager


class MACDIndicatorPlugin(IndicatorPlugin):
    """
    MACD指标插件
    委托给IndicatorManager进行实际计算，避免重复代码
    """
    
    def __init__(self):
        super().__init__()
        self.name = "MACDIndicator"
        self.version = "0.2.0"
        self.author = "Quant System"
        self.description = "MACD指标插件，通过IndicatorManager统一计算"
    
    def get_name(self) -> str:
        return self.name
    
    def get_version(self) -> str:
        return self.version
    
    def get_author(self) -> str:
        return self.author
    
    def get_description(self) -> str:
        return self.description
    
    def supports_polars(self) -> bool:
        return True
    
    def calculate(self, data, **kwargs):
        """
        计算MACD指标
        委托给IndicatorManager统一计算
        
        Args:
            data: 股票数据，通常为DataFrame
            **kwargs: 指标参数，包括fast_period, slow_period, signal_period
            
        Returns:
            Any: 包含MACD指标的数据
        """
        try:
            # 使用IndicatorManager统一计算
            fast_period = kwargs.get('fast_period', 12)
            slow_period = kwargs.get('slow_period', 26)
            signal_period = kwargs.get('signal_period', 9)
            
            result_df = global_indicator_manager.calculate_indicator(
                data, 'macd', return_polars=False,
                fast_period=fast_period, slow_period=slow_period, signal_period=signal_period
            )
            
            logger.debug(f"MACD指标插件通过IndicatorManager计算完成")
            return result_df
        except Exception as e:
            logger.exception(f"MACD指标插件计算失败: {e}")
            raise
    
    def calculate_polars(self, data, **kwargs):
        """
        使用polars计算MACD指标
        委托给IndicatorManager统一计算
        
        Args:
            data: 股票数据，polars DataFrame
            **kwargs: 指标参数，包括fast_period, slow_period, signal_period
            
        Returns:
            Any: 包含MACD指标的polars DataFrame
        """
        try:
            # 使用IndicatorManager统一计算
            fast_period = kwargs.get('fast_period', 12)
            slow_period = kwargs.get('slow_period', 26)
            signal_period = kwargs.get('signal_period', 9)
            
            result_df = global_indicator_manager.calculate_indicator(
                data, 'macd', return_polars=True,
                fast_period=fast_period, slow_period=slow_period, signal_period=signal_period
            )
            
            logger.debug(f"MACD指标插件通过IndicatorManager计算完成")
            return result_df
        except Exception as e:
            logger.exception(f"MACD指标插件Polars计算失败: {e}")
            raise
    
    def get_required_columns(self) -> list:
        """
        获取计算MACD指标所需的列名
        
        Returns:
            list: 所需列名列表
        """
        return ['close']
    
    def get_output_columns(self) -> list:
        """
        获取MACD指标计算输出的列名
        
        Returns:
            list: 输出列名列表
        """
        return ['macd', 'macd_signal', 'macd_hist']
