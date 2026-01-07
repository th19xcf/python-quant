#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
成交量MA指标插件，封装成交量MA计算功能
"""

from loguru import logger
from src.plugin.plugin_base import IndicatorPlugin


class VolMAIndicatorPlugin(IndicatorPlugin):
    """
    成交量MA指标插件，封装成交量MA计算功能
    """
    
    def __init__(self):
        super().__init__()
        self.name = "VolMAIndicator"
        self.version = "0.1.0"
        self.author = "Quant System"
        self.description = "成交量MA指标插件，用于计算成交量移动平均线"
    
    def get_name(self) -> str:
        return self.name
    
    def get_version(self) -> str:
        return self.version
    
    def get_author(self) -> str:
        return self.author
    
    def get_description(self) -> str:
        return self.description
    
    def calculate(self, data, **kwargs):
        """
        计算成交量MA指标
        
        Args:
            data: 股票数据，通常为DataFrame
            **kwargs: 指标参数，包括windows(成交量MA窗口列表)
            
        Returns:
            Any: 包含成交量MA指标的数据，通常为DataFrame
        """
        try:
            import pandas as pd
            import ta
            
            # 确保数据为DataFrame类型
            df = data.copy()
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)
            
            # 获取参数
            windows = kwargs.get('windows', [5, 10])
            if not isinstance(windows, list):
                windows = [windows]
            
            # 计算成交量MA指标
            for window in windows:
                df[f'vol_ma{window}'] = ta.trend.sma_indicator(df['volume'], window=window, fillna=True)
            
            logger.info(f"成功计算成交量MA指标，窗口: {windows}")
            return df
        except Exception as e:
            logger.exception(f"计算成交量MA指标失败: {e}")
            raise
    
    def get_required_columns(self) -> list:
        """
        获取计算成交量MA指标所需的列名
        
        Returns:
            list: 所需列名列表
        """
        return ['volume']
    
    def get_output_columns(self) -> list:
        """
        获取成交量MA指标计算输出的列名
        
        Returns:
            list: 输出列名列表
        """
        return ['vol_ma5', 'vol_ma10']