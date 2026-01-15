#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
RSI指标插件，封装RSI计算功能
"""

from loguru import logger
from src.plugin.plugin_base import IndicatorPlugin


class RSIIndicatorPlugin(IndicatorPlugin):
    """
    RSI指标插件，封装RSI计算功能
    """
    
    def __init__(self):
        super().__init__()
        self.name = "RSIIndicator"
        self.version = "0.1.0"
        self.author = "Quant System"
        self.description = "RSI指标插件，用于计算相对强弱指标"
    
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
        计算RSI指标
        
        Args:
            data: 股票数据，通常为DataFrame
            **kwargs: 指标参数，包括windows(RSI窗口列表)
            
        Returns:
            Any: 包含RSI指标的数据，通常为DataFrame
        """
        try:
            import pandas as pd
            import ta
            
            # 确保数据为DataFrame类型
            df = data.copy()
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)
            
            # 获取参数
            windows = kwargs.get('windows', [14])
            if not isinstance(windows, list):
                windows = [windows]
            
            # 计算RSI指标
            for window in windows:
                df[f'rsi{window}'] = ta.momentum.rsi(df['close'], window=window, fillna=True)
            
            logger.info(f"成功计算RSI指标，窗口: {windows}")
            return df
        except Exception as e:
            logger.exception(f"计算RSI指标失败: {e}")
            raise
    
    def calculate_polars(self, data, **kwargs):
        """
        使用polars计算RSI指标
        
        Args:
            data: 股票数据，polars DataFrame
            **kwargs: 指标参数，包括windows(RSI窗口列表)
            
        Returns:
            Any: 包含RSI指标的polars DataFrame
        """
        try:
            import polars as pl
            
            # 获取参数
            windows = kwargs.get('windows', [14])
            if not isinstance(windows, list):
                windows = [windows]
            
            # 计算RSI指标
            result = data
            
            for window in windows:
                # 计算价格变化
                result = result.with_columns(
                    pl.col('close').diff().alias('price_change')
                )
                
                # 计算上涨和下跌
                result = result.with_columns(
                    pl.when(pl.col('price_change') > 0)
                    .then(pl.col('price_change'))
                    .otherwise(0)
                    .alias('gain'),
                    pl.when(pl.col('price_change') < 0)
                    .then(pl.col('price_change').abs())
                    .otherwise(0)
                    .alias('loss')
                )
                
                # 计算平均上涨和平均下跌
                result = result.with_columns(
                    pl.col('gain').rolling_mean(window_size=window, min_periods=1).alias('avg_gain'),
                    pl.col('loss').rolling_mean(window_size=window, min_periods=1).alias('avg_loss')
                )
                
                # 计算RSI
                result = result.with_columns(
                    pl.when(pl.col('avg_loss') == 0)
                    .then(100)
                    .otherwise(100 - (100 / (1 + pl.col('avg_gain') / pl.col('avg_loss'))))
                    .alias(f'rsi{window}')
                )
            
            # 删除中间列
            result = result.drop(['price_change', 'gain', 'loss', 'avg_gain', 'avg_loss'])
            
            logger.info(f"成功使用polars计算RSI指标，窗口: {windows}")
            return result
        except Exception as e:
            logger.exception(f"使用polars计算RSI指标失败: {e}")
            # 回退到pandas实现
            import pandas as pd
            df_pd = data.to_pandas()
            return pl.from_pandas(self.calculate(df_pd, **kwargs))
    
    def get_required_columns(self) -> list:
        """
        获取计算RSI指标所需的列名
        
        Returns:
            list: 所需列名列表
        """
        return ['close']
    
    def get_output_columns(self) -> list:
        """
        获取RSI指标计算输出的列名
        
        Returns:
            list: 输出列名列表
        """
        return ['rsi14']