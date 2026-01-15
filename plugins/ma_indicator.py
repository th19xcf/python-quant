#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
移动平均线(MA)指标插件，封装移动平均线计算功能
"""

from loguru import logger
from src.plugin.plugin_base import IndicatorPlugin


class MAIndicatorPlugin(IndicatorPlugin):
    """
    移动平均线(MA)指标插件，封装移动平均线计算功能
    """
    
    def __init__(self):
        super().__init__()
        self.name = "MAIndicator"
        self.version = "0.1.0"
        self.author = "Quant System"
        self.description = "移动平均线(MA)指标插件，用于计算不同窗口的移动平均线"
    
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
        
        Args:
            data: 股票数据，通常为DataFrame
            **kwargs: 指标参数，包括windows(移动平均窗口列表)
            
        Returns:
            Any: 包含MA指标的数据，通常为DataFrame
        """
        try:
            import pandas as pd
            import ta
            
            # 确保数据为DataFrame类型
            df = data.copy()
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)
            
            # 获取参数
            windows = kwargs.get('windows', [5, 10, 20, 60])
            
            # 计算MA指标
            for window in windows:
                df[f'ma{window}'] = ta.trend.sma_indicator(df['close'], window=window, fillna=True)
            
            logger.info(f"成功计算MA指标，窗口: {windows}")
            return df
        except Exception as e:
            logger.exception(f"计算MA指标失败: {e}")
            raise
    
    def calculate_polars(self, data, **kwargs):
        """
        使用polars计算移动平均线指标
        
        Args:
            data: 股票数据，polars DataFrame
            **kwargs: 指标参数，包括windows(移动平均窗口列表)
            
        Returns:
            Any: 包含MA指标的polars DataFrame
        """
        try:
            import polars as pl
            
            # 获取参数
            windows = kwargs.get('windows', [5, 10, 20, 60])
            
            # 计算MA指标
            result = data
            for window in windows:
                result = result.with_columns(
                    pl.col('close').rolling_mean(window_size=window, min_periods=1).alias(f'ma{window}')
                )
            
            logger.info(f"成功使用polars计算MA指标，窗口: {windows}")
            return result
        except Exception as e:
            logger.exception(f"使用polars计算MA指标失败: {e}")
            # 回退到pandas实现
            import pandas as pd
            df_pd = data.to_pandas()
            return pl.from_pandas(self.calculate(df_pd, **kwargs))
    
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