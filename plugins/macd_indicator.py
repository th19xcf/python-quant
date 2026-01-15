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
    
    def supports_polars(self) -> bool:
        return True
    
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
    
    def calculate_polars(self, data, **kwargs):
        """
        使用polars计算MACD指标
        
        Args:
            data: 股票数据，polars DataFrame
            **kwargs: 指标参数，包括fast_period, slow_period, signal_period
            
        Returns:
            Any: 包含MACD指标的polars DataFrame
        """
        try:
            import polars as pl
            
            # 获取参数
            fast_period = kwargs.get('fast_period', 12)
            slow_period = kwargs.get('slow_period', 26)
            signal_period = kwargs.get('signal_period', 9)
            
            # 计算MACD指标
            # 计算快速EMA
            result = data.with_columns(
                pl.col('close').ewm_mean(alpha=2/(fast_period+1), adjust=False, min_periods=1).alias('ema_fast'),
                pl.col('close').ewm_mean(alpha=2/(slow_period+1), adjust=False, min_periods=1).alias('ema_slow')
            )
            
            # 计算MACD线
            result = result.with_columns(
                (pl.col('ema_fast') - pl.col('ema_slow')).alias('macd')
            )
            
            # 计算信号线
            result = result.with_columns(
                pl.col('macd').ewm_mean(alpha=2/(signal_period+1), adjust=False, min_periods=1).alias('macd_signal')
            )
            
            # 计算柱状图
            result = result.with_columns(
                (pl.col('macd') - pl.col('macd_signal')).alias('macd_hist')
            )
            
            # 删除中间列
            result = result.drop(['ema_fast', 'ema_slow'])
            
            logger.info(f"成功使用polars计算MACD指标，参数: fast={fast_period}, slow={slow_period}, signal={signal_period}")
            return result
        except Exception as e:
            logger.exception(f"使用polars计算MACD指标失败: {e}")
            # 回退到pandas实现
            import pandas as pd
            df_pd = data.to_pandas()
            return pl.from_pandas(self.calculate(df_pd, **kwargs))
    
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