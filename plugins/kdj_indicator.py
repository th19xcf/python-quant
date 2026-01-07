#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
KDJ指标插件，封装KDJ计算功能
"""

from loguru import logger
from src.plugin.plugin_base import IndicatorPlugin


class KDJIndicatorPlugin(IndicatorPlugin):
    """
    KDJ指标插件，封装KDJ计算功能
    """
    
    def __init__(self):
        super().__init__()
        self.name = "KDJIndicator"
        self.version = "0.1.0"
        self.author = "Quant System"
        self.description = "KDJ指标插件，用于计算随机指标"
    
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
        计算KDJ指标
        
        Args:
            data: 股票数据，通常为DataFrame
            **kwargs: 指标参数，包括window(KDJ窗口)
            
        Returns:
            Any: 包含KDJ指标的数据，通常为DataFrame
        """
        try:
            import pandas as pd
            import ta
            
            # 确保数据为DataFrame类型
            df = data.copy()
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)
            
            # 获取参数
            window = kwargs.get('window', 14)
            
            # 计算KDJ指标
            df['k'] = ta.momentum.stoch(df['high'], df['low'], df['close'], window=window, fillna=True)
            df['d'] = ta.momentum.stoch_signal(df['high'], df['low'], df['close'], window=window, fillna=True)
            df['j'] = 3 * df['k'] - 2 * df['d']
            
            logger.info(f"成功计算KDJ指标，窗口: {window}")
            return df
        except Exception as e:
            logger.exception(f"计算KDJ指标失败: {e}")
            raise
    
    def get_required_columns(self) -> list:
        """
        获取计算KDJ指标所需的列名
        
        Returns:
            list: 所需列名列表
        """
        return ['high', 'low', 'close']
    
    def get_output_columns(self) -> list:
        """
        获取KDJ指标计算输出的列名
        
        Returns:
            list: 输出列名列表
        """
        return ['k', 'd', 'j']