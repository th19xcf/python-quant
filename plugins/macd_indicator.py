#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MACD指标插件，封装MACD计算功能
"""

from loguru import logger
from src.plugin.plugin_base import IndicatorPlugin


class MACDIndicatorPlugin(IndicatorPlugin):
    """
    MACD指标插件，封装MACD计算功能
    """
    
    def __init__(self):
        super().__init__()
        self.name = "MACDIndicator"
        self.version = "0.1.0"
        self.author = "Quant System"
        self.description = "MACD指标插件，用于计算MACD、信号线和柱状图"
    
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
        计算MACD指标
        
        Args:
            data: 股票数据，通常为DataFrame
            **kwargs: 指标参数，包括fast_period, slow_period, signal_period
            
        Returns:
            Any: 包含MACD指标的数据，通常为DataFrame
        """
        try:
            import pandas as pd
            import ta
            
            # 确保数据为DataFrame类型
            df = data.copy()
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)
            
            # 获取参数
            fast_period = kwargs.get('fast_period', 12)
            slow_period = kwargs.get('slow_period', 26)
            signal_period = kwargs.get('signal_period', 9)
            
            # 计算MACD指标
            df['macd'] = ta.trend.macd(df['close'], window_slow=slow_period, window_fast=fast_period, fillna=True)
            df['macd_signal'] = ta.trend.macd_signal(df['close'], window_slow=slow_period, window_fast=fast_period, window_sign=signal_period, fillna=True)
            df['macd_hist'] = ta.trend.macd_diff(df['close'], window_slow=slow_period, window_fast=fast_period, window_sign=signal_period, fillna=True)
            
            logger.info(f"成功计算MACD指标，参数: fast={fast_period}, slow={slow_period}, signal={signal_period}")
            return df
        except Exception as e:
            logger.exception(f"计算MACD指标失败: {e}")
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