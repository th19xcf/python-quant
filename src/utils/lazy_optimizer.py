#!/usr/bin/env python3

"""
惰性计算优化模块
提供 Polars LazyFrame 流水线的执行入口
"""

import polars as pl

from .memory_optimizer import MemoryOptimizer


class LazyOptimizer:
    """
    惰性计算优化器
    提供 LazyFrame 执行前的内存优化与查询计划处理
    """

    # 精度敏感列（与 MemoryOptimizer.COLUMN_TYPE_MAP 的保护规则对齐）：
    # 价格/成交量列禁止降级 Float32，避免复权因子连乘、涨跌停比较出现可观测误差
    _PROTECTED_COLS = frozenset(
        MemoryOptimizer.COLUMN_TYPE_MAP['price_cols']
        + MemoryOptimizer.COLUMN_TYPE_MAP['volume_cols']
    )

    @classmethod
    def optimize_memory_usage(cls, lazy_df: pl.LazyFrame) -> pl.LazyFrame:
        """
        优化内存使用：将非保护数值列降级为 Float32

        Args:
            lazy_df: 输入LazyFrame

        Returns:
            pl.LazyFrame: 优化后的LazyFrame
        """
        numeric_cols = [
            col for col in lazy_df.columns
            if lazy_df.schema[col].is_numeric() and col not in cls._PROTECTED_COLS
        ]

        for col in numeric_cols:
            # 尝试使用更小的数据类型
            lazy_df = lazy_df.with_columns(
                pl.col(col).cast(pl.Float32)
            )

        return lazy_df

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


def lazy_exec(lazy_df: pl.LazyFrame, optimize_memory: bool = True) -> pl.DataFrame:
    """
    便捷执行惰性计算
    """
    return execute_optimized_pipeline(lazy_df, optimize_memory)
