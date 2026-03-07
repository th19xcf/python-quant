#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据处理模块

负责数据预处理、清洗、采样等操作
"""

from typing import List, Dict, Any, Optional, Union
import polars as pl
from loguru import logger

from src.utils.memory_optimizer import MemoryOptimizer


class DataProcessor:
    """
    数据处理模块，负责数据预处理、清洗、采样等操作
    """
    
    def __init__(self, config):
        """
        初始化数据处理器
        
        Args:
            config: 配置对象
        """
        self.config = config
    
    def preprocess_data(self, data: Union[pl.DataFrame, pl.LazyFrame]) -> Union[pl.DataFrame, pl.LazyFrame]:
        """
        预处理数据
        
        Args:
            data: 原始数据（Polars DataFrame或LazyFrame）
            
        Returns:
            Union[pl.DataFrame, pl.LazyFrame]: 预处理后的数据
        """
        # 确保数据包含必要的列
        required_columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']
        
        if not all(col in data.columns for col in required_columns):
            logger.warning(f"数据缺少必要列，当前列: {data.columns}")
            return data
        
        # 排序数据
        if 'date' in data.columns:
            data = data.sort('date')
        
        # 去除重复数据
        data = data.unique(subset=['date'])
        
        return data
    
    def sample_data(self, data: Union[pl.DataFrame, pl.LazyFrame], target_points: int = 1000, strategy: str = 'adaptive') -> Union[pl.DataFrame, pl.LazyFrame]:
        """
        采样数据，减少数据量
        
        Args:
            data: 原始数据（Polars DataFrame或LazyFrame）
            target_points: 目标采样点数
            strategy: 采样策略，可选值：'uniform'（均匀采样）、'adaptive'（自适应采样）
            
        Returns:
            Union[pl.DataFrame, pl.LazyFrame]: 采样后的数据
        """
        # 检查是否为LazyFrame
        is_lazy = isinstance(data, pl.LazyFrame)
        
        if is_lazy:
            # 对于LazyFrame，使用nth_sample进行均匀采样
            return data.nth_sample(target_points)
        else:
            # 对于DataFrame
            if len(data) <= target_points:
                return data
            
            if strategy == 'uniform':
                # 均匀采样
                step = len(data) // target_points
                return data[::step]
            elif strategy == 'adaptive':
                # 自适应采样 - 这里使用简单的均匀采样作为默认实现
                # 实际自适应采样可以根据数据波动率进行调整
                step = len(data) // target_points
                return data[::step]
            else:
                logger.warning(f"不支持的采样策略: {strategy}，使用默认均匀采样")
                step = len(data) // target_points
                return data[::step]
    
    def convert_data_type(self, data: Union[pl.DataFrame, pl.LazyFrame], target_type: str = 'float32') -> Union[pl.DataFrame, pl.LazyFrame]:
        """
        转换数据类型
        
        Args:
            data: 原始数据（Polars DataFrame或LazyFrame）
            target_type: 目标数据类型，默认：float32
            
        Returns:
            Union[pl.DataFrame, pl.LazyFrame]: 转换后的数据
        """
        # 转换数值列的数据类型
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'amount', 'pct_chg']
        
        for col in data.columns:
            if col in numeric_columns:
                if target_type == 'float32':
                    data = data.with_columns(pl.col(col).cast(pl.Float32))
                elif target_type == 'float64':
                    data = data.with_columns(pl.col(col).cast(pl.Float64))
        
        return data
    
    def clean_data(self, data: Union[pl.DataFrame, pl.LazyFrame]) -> Union[pl.DataFrame, pl.LazyFrame]:
        """
        清洗数据，处理缺失值、异常值等
        
        Args:
            data: 原始数据（Polars DataFrame或LazyFrame）
            
        Returns:
            Union[pl.DataFrame, pl.LazyFrame]: 清洗后的数据
        """
        # 去除包含空值的行
        data = data.drop_nulls()
        
        # 去除成交量为0的行
        if 'volume' in data.columns:
            data = data.filter(pl.col('volume') > 0)
        
        return data
    
    def convert_frequency(self, df: pl.DataFrame, frequency: str) -> pl.DataFrame:
        """
        将日线数据转换为周线或月线数据
        
        Args:
            df: 日线数据
            frequency: 目标频率，1w=周线，1m=月线
            
        Returns:
            pl.DataFrame: 转换后的数据
        """
        if df.is_empty():
            return df
        
        try:
            # 使用 Lazy API 进行数据处理
            lazy_df = df.lazy()
            
            # 确保有日期列
            if 'trade_date' in df.columns:
                lazy_df = lazy_df.with_columns(pl.col('trade_date').alias('date'))
            elif 'date' not in df.columns:
                logger.error("DataFrame中没有日期列")
                return df
            
            # 转换日期列为datetime类型
            if str(lazy_df.schema['date']) == 'String':
                lazy_df = lazy_df.with_columns(pl.col('date').str.strptime(pl.Date, "%Y-%m-%d"))
            elif str(lazy_df.schema['date']) == 'Datetime':
                lazy_df = lazy_df.with_columns(pl.col('date').dt.date().alias('date'))
            
            # 根据频率确定分组方式
            if frequency == '1w':
                # 周线：按周分组
                lazy_df = lazy_df.with_columns(
                    pl.col('date').dt.week().alias('week'),
                    pl.col('date').dt.year().alias('year')
                )
                group_cols = ['year', 'week']
            elif frequency == '1m':
                # 月线：按月分组
                lazy_df = lazy_df.with_columns(
                    pl.col('date').dt.month().alias('month'),
                    pl.col('date').dt.year().alias('year')
                )
                group_cols = ['year', 'month']
            else:
                return df
            
            # 聚合数据
            agg_columns = [
                pl.col('date').first().alias('date'),
                pl.col('open').first().alias('open'),
                pl.col('high').max().alias('high'),
                pl.col('low').min().alias('low'),
                pl.col('close').last().alias('close'),
                pl.col('volume').sum().alias('volume'),
                pl.col('amount').sum().alias('amount')
            ]
            
            # 检查是否存在pct_chg列
            if 'pct_chg' in df.columns:
                agg_columns.append(pl.col('pct_chg').sum().alias('pct_chg'))
            
            # 检查是否存在change列
            if 'change' in df.columns:
                agg_columns.append(pl.col('change').sum().alias('change'))
            
            lazy_df = lazy_df.group_by(group_cols).agg(agg_columns)
            
            # 按日期排序
            lazy_df = lazy_df.sort('date')
            
            # 将日期转换回字符串格式
            lazy_df = lazy_df.with_columns(
                pl.col('date').dt.strftime("%Y-%m-%d").alias('date')
            )
            
            # 执行计算
            result = lazy_df.collect()
            
            # 内存优化
            optimized_result = MemoryOptimizer.optimize_dataframe(result, enable_sparse=True)
            logger.info(f"将日线数据转换为{frequency}数据，从{df.height}条转换为{optimized_result.height}条")
            return optimized_result

        except (ValueError, TypeError) as e:
            logger.exception(f"转换数据频率失败: {e}")
            return df
    
    def optimize_memory(self, data: pl.DataFrame, enable_sparse: bool = True) -> pl.DataFrame:
        """
        优化数据内存使用
        
        Args:
            data: 原始数据
            enable_sparse: 是否启用稀疏存储
            
        Returns:
            pl.DataFrame: 优化后的数据
        """
        optimized_data = MemoryOptimizer.optimize_dataframe(data, enable_sparse=enable_sparse)
        return optimized_data
