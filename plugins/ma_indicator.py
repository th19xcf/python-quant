#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
移动平均线(MA)指标插件
通过IndicatorManager调用核心计算模块，避免重复实现
"""

from loguru import logger
from src.plugin.plugin_base import IndicatorPlugin
from src.tech_analysis.indicator_manager import global_indicator_manager


class MAIndicatorPlugin(IndicatorPlugin):
    """
    移动平均线(MA)指标插件
    委托给IndicatorManager进行实际计算，避免重复代码
    """
    
    def __init__(self):
        super().__init__()
        self.name = "MAIndicator"
        self.version = "0.2.0"
        self.author = "Quant System"
        self.description = "移动平均线(MA)指标插件，通过IndicatorManager统一计算"
    
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
        计算移动平均线指标
        委托给IndicatorManager统一计算
        
        Args:
            data: 股票数据，通常为DataFrame
            **kwargs: 指标参数，包括windows(移动平均窗口列表)
            
        Returns:
            Any: 包含MA指标的数据
        """
        try:
            import pandas as pd
            
            # 使用IndicatorManager统一计算
            windows = kwargs.get('windows', [5, 10, 20, 60])
            result_df = global_indicator_manager.calculate_indicator(
                data, 'ma', return_polars=False, windows=windows
            )
            
            logger.debug(f"MA指标插件通过IndicatorManager计算完成，窗口: {windows}")
            return result_df
        except Exception as e:
            logger.exception(f"MA指标插件计算失败: {e}")
            raise
    
    def calculate_polars(self, data, **kwargs):
        """
        使用polars计算移动平均线指标
        委托给IndicatorManager统一计算
        
        Args:
            data: 股票数据，polars DataFrame
            **kwargs: 指标参数，包括windows(移动平均窗口列表)
            
        Returns:
            Any: 包含MA指标的polars DataFrame
        """
        try:
            # 使用IndicatorManager统一计算
            windows = kwargs.get('windows', [5, 10, 20, 60])
            result_df = global_indicator_manager.calculate_indicator(
                data, 'ma', return_polars=True, windows=windows
            )
            
            logger.debug(f"MA指标插件通过IndicatorManager计算完成，窗口: {windows}")
            return result_df
        except Exception as e:
            logger.exception(f"MA指标插件Polars计算失败: {e}")
            raise
    
    def get_required_columns(self) -> list:
        """
        获取计算MA指标所需的列名
        
        Returns:
            list: 所需列名列表
        """
        return ['close']
    
    def get_output_columns(self) -> list:
        """
        获取MA指标计算输出的列名
        
        Returns:
            list: 输出列名列表
        """
        # 由于MA窗口是动态的，返回基本格式
        return [f'ma{window}' for window in [5, 10, 20, 60]]
