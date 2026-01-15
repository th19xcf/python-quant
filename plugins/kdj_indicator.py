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
    
    def supports_polars(self) -> bool:
        return True
    
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
    
    def calculate_polars(self, data, **kwargs):
        """
        使用polars计算KDJ指标
        
        Args:
            data: 股票数据，polars DataFrame
            **kwargs: 指标参数，包括window(KDJ窗口)
            
        Returns:
            Any: 包含KDJ指标的polars DataFrame
        """
        try:
            import polars as pl
            
            # 获取参数
            window = kwargs.get('window', 14)
            
            # 计算KDJ指标
            # 计算RSV值
            result = data.with_columns(
                # 计算n日内最高价
                pl.col('high').rolling_max(window_size=window, min_periods=1).alias('highest_high'),
                # 计算n日内最低价
                pl.col('low').rolling_min(window_size=window, min_periods=1).alias('lowest_low')
            )
            
            # 计算RSV
            result = result.with_columns(
                pl.when(pl.col('highest_high') == pl.col('lowest_low'))
                .then(0)
                .otherwise((pl.col('close') - pl.col('lowest_low')) / (pl.col('highest_high') - pl.col('lowest_low')) * 100)
                .alias('rsv')
            )
            
            # 计算K、D、J值
            result = result.with_columns(
                # 计算K值，使用平滑因子2/3
                pl.col('rsv').ewm_mean(alpha=1/3, adjust=False, min_periods=1).alias('k')
            )
            
            result = result.with_columns(
                # 计算D值，使用K值的EWMA
                pl.col('k').ewm_mean(alpha=1/3, adjust=False, min_periods=1).alias('d')
            )
            
            result = result.with_columns(
                # 计算J值
                (3 * pl.col('k') - 2 * pl.col('d')).alias('j')
            )
            
            # 删除中间列
            result = result.drop(['highest_high', 'lowest_low', 'rsv'])
            
            logger.info(f"成功使用polars计算KDJ指标，窗口: {window}")
            return result
        except Exception as e:
            logger.exception(f"使用polars计算KDJ指标失败: {e}")
            # 回退到pandas实现
            import pandas as pd
            df_pd = data.to_pandas()
            return pl.from_pandas(self.calculate(df_pd, **kwargs))
    
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