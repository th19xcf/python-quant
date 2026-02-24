#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
惰性计算优化模块
提供深度的Polars Lazy API优化功能
"""

import polars as pl
from typing import List, Dict, Any, Optional, Callable, Union
from loguru import logger
from .memory_optimizer import MemoryOptimizer


class LazyOptimizer:
    """
    惰性计算优化器
    提供深度的Polars Lazy API优化功能
    """
    
    @classmethod
    def optimize_lazy_pipeline(cls, df: Union[pl.DataFrame, pl.LazyFrame]) -> pl.LazyFrame:
        """
        优化惰性计算流水线
        
        Args:
            df: Polars DataFrame或LazyFrame
            
        Returns:
            pl.LazyFrame: 优化后的LazyFrame
        """
        if isinstance(df, pl.LazyFrame):
            return df
        else:
            return df.lazy()
    
    @classmethod
    def batch_optimize(cls, operations: List[Callable[[pl.LazyFrame], pl.LazyFrame]]) -> Callable[[pl.LazyFrame], pl.LazyFrame]:
        """
        批量优化操作
        
        Args:
            operations: 操作函数列表
            
        Returns:
            Callable: 组合后的优化函数
        """
        def optimized_pipeline(lazy_df: pl.LazyFrame) -> pl.LazyFrame:
            result = lazy_df
            for operation in operations:
                result = operation(result)
            return result
        return optimized_pipeline
    
    @classmethod
    def optimize_data_acquisition(cls, data_source: str, query_params: Dict[str, Any]) -> pl.LazyFrame:
        """
        优化数据获取过程
        
        Args:
            data_source: 数据源名称
            query_params: 查询参数
            
        Returns:
            pl.LazyFrame: 优化后的数据获取LazyFrame
        """
        # 这里可以根据不同的数据源实现不同的优化策略
        # 例如：对于CSV文件使用scan_csv，对于数据库使用scan_sql等
        logger.info(f"优化数据源{data_source}的数据获取")
        
        # 示例实现，实际需要根据具体数据源扩展
        if data_source == 'csv':
            file_path = query_params.get('file_path')
            return pl.scan_csv(file_path)
        elif data_source == 'parquet':
            file_path = query_params.get('file_path')
            return pl.scan_parquet(file_path)
        else:
            # 默认返回空的LazyFrame
            return pl.LazyFrame({})
    
    @classmethod
    def optimize_indicator_calculation(cls, lazy_df: pl.LazyFrame, indicator_types: List[str], **params) -> pl.LazyFrame:
        """
        优化指标计算过程
        
        Args:
            lazy_df: 输入LazyFrame
            indicator_types: 指标类型列表
            **params: 指标计算参数
            
        Returns:
            pl.LazyFrame: 包含指标计算的LazyFrame
        """
        # 导入指标计算函数
        from src.tech_analysis.indicator_calculator import calculate_multiple_indicators_polars
        
        # 使用现有的多指标计算函数
        return calculate_multiple_indicators_polars(lazy_df, indicator_types, **params)
    
    @classmethod
    def optimize_data_transformation(cls, lazy_df: pl.LazyFrame, transformations: List[Dict[str, Any]]) -> pl.LazyFrame:
        """
        优化数据转换过程
        
        Args:
            lazy_df: 输入LazyFrame
            transformations: 转换配置列表
            
        Returns:
            pl.LazyFrame: 转换后的LazyFrame
        """
        result = lazy_df
        
        for transform in transformations:
            transform_type = transform.get('type')
            
            if transform_type == 'filter':
                condition = transform.get('condition')
                if condition:
                    result = result.filter(condition)
            
            elif transform_type == 'select':
                columns = transform.get('columns')
                if columns:
                    result = result.select(columns)
            
            elif transform_type == 'with_columns':
                columns = transform.get('columns')
                if columns:
                    result = result.with_columns(columns)
            
            elif transform_type == 'group_by':
                by = transform.get('by')
                aggregations = transform.get('aggregations', [])
                if by and aggregations:
                    result = result.group_by(by).agg(aggregations)
            
            elif transform_type == 'sort':
                by = transform.get('by')
                descending = transform.get('descending', False)
                if by:
                    result = result.sort(by, descending=descending)
        
        return result
    
    @classmethod
    def optimize_window_functions(cls, lazy_df: pl.LazyFrame, window_specs: List[Dict[str, Any]]) -> pl.LazyFrame:
        """
        优化窗口函数
        
        Args:
            lazy_df: 输入LazyFrame
            window_specs: 窗口函数配置列表
            
        Returns:
            pl.LazyFrame: 包含窗口函数计算的LazyFrame
        """
        result = lazy_df
        
        # 收集所有需要的窗口大小
        window_sizes = set()
        for spec in window_specs:
            window_size = spec.get('window_size')
            if window_size:
                window_sizes.add(window_size)
        
        # 批量创建窗口列
        for window_size in window_sizes:
            # 示例：创建最高价和最低价的滚动窗口
            result = result.with_columns(
                pl.col('high').rolling_max(window_size=window_size, min_periods=1).alias(f'high_roll_{window_size}'),
                pl.col('low').rolling_min(window_size=window_size, min_periods=1).alias(f'low_roll_{window_size}'),
                pl.col('close').rolling_mean(window_size=window_size, min_periods=1).alias(f'close_roll_{window_size}')
            )
        
        return result
    
    @classmethod
    def optimize_join_operations(cls, left: pl.LazyFrame, right: pl.LazyFrame, on: Union[str, List[str]], how: str = 'inner') -> pl.LazyFrame:
        """
        优化连接操作
        
        Args:
            left: 左表LazyFrame
            right: 右表LazyFrame
            on: 连接键
            how: 连接方式
            
        Returns:
            pl.LazyFrame: 连接后的LazyFrame
        """
        # 优化连接操作，例如：
        # 1. 只选择需要的列
        # 2. 确保连接键的数据类型一致
        # 3. 对于大表，考虑使用分区连接
        
        return left.join(right, on=on, how=how)
    
    @classmethod
    def parallel_process(cls, lazy_df: pl.LazyFrame, batch_size: int = 10000) -> pl.LazyFrame:
        """
        并行处理大型数据集
        
        Args:
            lazy_df: 输入LazyFrame
            batch_size: 批处理大小
            
        Returns:
            pl.LazyFrame: 处理后的LazyFrame
        """
        # 使用Polars的并行处理能力
        # Polars会自动处理并行计算
        # 对于大型数据集，可以考虑使用分区处理
        
        return lazy_df
    
    @classmethod
    def optimize_memory_usage(cls, lazy_df: pl.LazyFrame) -> pl.LazyFrame:
        """
        优化内存使用
        
        Args:
            lazy_df: 输入LazyFrame
            
        Returns:
            pl.LazyFrame: 优化后的LazyFrame
        """
        # 优化数据类型
        # 对于数值列，使用更小的数据类型
        numeric_cols = [col for col in lazy_df.columns if lazy_df.schema[col].is_numeric()]
        
        for col in numeric_cols:
            # 尝试使用更小的数据类型
            lazy_df = lazy_df.with_columns(
                pl.col(col).cast(pl.Float32)
            )
        
        return lazy_df
    
    @classmethod
    def optimize_query_plan(cls, lazy_df: pl.LazyFrame) -> pl.LazyFrame:
        """
        优化查询计划
        
        Args:
            lazy_df: 输入LazyFrame
            
        Returns:
            pl.LazyFrame: 优化后的LazyFrame
        """
        # Polars会自动优化查询计划
        # 这里添加一些手动优化
        
        # 1. 优化窗口函数执行顺序
        # Polars会自动处理，但我们可以确保使用正确的窗口规范
        
        # 2. 优化连接操作
        # 确保连接键的数据类型一致
        
        # 3. 优化过滤操作，将过滤条件提前
        # Polars会自动优化，但我们可以确保过滤条件正确
        
        return lazy_df
    
    @classmethod
    def create_optimized_pipeline(cls, steps: List[Dict[str, Any]]) -> Callable[[pl.LazyFrame], pl.LazyFrame]:
        """
        创建优化的处理流水线
        
        Args:
            steps: 处理步骤列表
            
        Returns:
            Callable: 优化后的处理函数
        """
        def pipeline(lazy_df: pl.LazyFrame) -> pl.LazyFrame:
            result = lazy_df
            
            for step in steps:
                step_type = step.get('type')
                
                if step_type == 'filter':
                    condition = step.get('condition')
                    if condition is not None:
                        result = result.filter(condition)
                
                elif step_type == 'select':
                    columns = step.get('columns')
                    if columns:
                        result = result.select(columns)
                
                elif step_type == 'with_columns':
                    columns = step.get('columns')
                    if columns:
                        result = result.with_columns(columns)
                
                elif step_type == 'group_by':
                    by = step.get('by')
                    aggregations = step.get('aggregations', [])
                    if by and aggregations:
                        result = result.group_by(by).agg(aggregations)
                
                elif step_type == 'sort':
                    by = step.get('by')
                    descending = step.get('descending', False)
                    if by:
                        result = result.sort(by, descending=descending)
                
                elif step_type == 'join':
                    right = step.get('right')
                    on = step.get('on')
                    how = step.get('how', 'inner')
                    if right and on:
                        result = result.join(right, on=on, how=how)
            
            return result
        
        return pipeline


def create_lazy_pipeline(steps: List[Dict[str, Any]]) -> Callable[[pl.LazyFrame], pl.LazyFrame]:
    """
    创建惰性计算流水线
    
    Args:
        steps: 处理步骤列表
        
    Returns:
        Callable: 流水线函数
    """
    return LazyOptimizer.create_optimized_pipeline(steps)


def optimize_lazy_frame(lazy_df: pl.LazyFrame) -> pl.LazyFrame:
    """
    优化LazyFrame
    
    Args:
        lazy_df: 输入LazyFrame
        
    Returns:
        pl.LazyFrame: 优化后的LazyFrame
    """
    return LazyOptimizer.optimize_query_plan(lazy_df)


def execute_optimized_pipeline(lazy_df: pl.LazyFrame, optimize_memory: bool = True, parallel: bool = True) -> pl.DataFrame:
    """
    执行优化的惰性计算流水线
    
    Args:
        lazy_df: 输入LazyFrame
        optimize_memory: 是否优化内存使用
        parallel: 是否启用并行处理
        
    Returns:
        pl.DataFrame: 执行结果
    """
    # 应用并行处理
    if parallel:
        lazy_df = LazyOptimizer.parallel_process(lazy_df)
    
    # 优化内存使用
    if optimize_memory:
        lazy_df = LazyOptimizer.optimize_memory_usage(lazy_df)
    
    # 执行计算
    result = lazy_df.collect()
    
    # 进一步优化内存使用
    if optimize_memory:
        result = MemoryOptimizer.optimize_dataframe(result, enable_sparse=True)
    
    return result


# 便捷函数
def lazy_pipeline(steps: List[Dict[str, Any]]) -> Callable[[pl.LazyFrame], pl.LazyFrame]:
    """
    便捷创建惰性计算流水线
    """
    return create_lazy_pipeline(steps)


def lazy_exec(lazy_df: pl.LazyFrame, optimize_memory: bool = True) -> pl.DataFrame:
    """
    便捷执行惰性计算
    """
    return execute_optimized_pipeline(lazy_df, optimize_memory)
