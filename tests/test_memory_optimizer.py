#!/usr/bin/env python3

"""
MemoryOptimizer 测试脚本
"""

import sys
from pathlib import Path

from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from datetime import datetime, timedelta

import numpy as np
import polars as pl

from utils.memory_optimizer import (
    MemoryOptimizer,
    optimize_df,
    print_df_memory,
    process_large_df,
)


def test_basic_optimization():
    """测试基本的内存优化功能"""
    logger.info("=== 测试基本内存优化功能 ===")

    # 创建测试数据
    dates = [datetime(2020, 1, 1) + timedelta(days=i) for i in range(1000)]
    df = pl.DataFrame({
        'open': np.random.randn(1000),
        'volume': np.random.randint(1, 10000, 1000),
        'is_st': [True]*500 + [False]*500,
        'industry': ['tech']*200 + ['finance']*200 + ['health']*200 + ['energy']*200 + ['consumer']*200,
        'date': dates
    })

    logger.info("原始数据类型:")
    logger.info(df.dtypes)
    logger.info("")

    logger.info("原始数据内存使用:")
    print_df_memory(df)

    # 优化数据
    optimized_df = optimize_df(df)

    logger.info("优化后数据类型:")
    logger.info(optimized_df.dtypes)
    logger.info("")

    logger.info("优化后数据内存使用:")
    print_df_memory(optimized_df)

    return df, optimized_df


def test_large_dataset():
    """测试大数据集处理"""
    logger.info("\n=== 测试大数据集处理 ===")

    # 创建测试数据
    dates = [datetime(2020, 1, 1) + timedelta(days=i) for i in range(1000)]
    df = pl.DataFrame({
        'open': np.random.randn(1000),
        'volume': np.random.randint(1, 10000, 1000),
        'is_st': [True]*500 + [False]*500,
        'industry': ['tech']*200 + ['finance']*200 + ['health']*200 + ['energy']*200 + ['consumer']*200,
        'date': dates
    })

    # 构建大数据集
    large_df = pl.concat([df]*100)
    logger.info(f"大数据集大小: {len(large_df):,}行")

    # 分块处理
    optimized_large_df = process_large_df(large_df, chunk_size=50000)
    logger.info("大数据集优化完成")

    logger.info("优化后数据类型:")
    logger.info(optimized_large_df.dtypes)

    return optimized_large_df


def test_optimization_suggestions():
    """测试优化建议功能"""
    logger.info("\n=== 测试优化建议功能 ===")

    # 创建测试数据
    dates = [datetime(2020, 1, 1) + timedelta(days=i) for i in range(1000)]
    df = pl.DataFrame({
        'open': np.random.randn(1000),
        'volume': np.random.randint(1, 10000, 1000),
        'is_st': [True]*500 + [False]*500,
        'industry': ['tech']*200 + ['finance']*200 + ['health']*200 + ['energy']*200 + ['consumer']*200,
        'date': dates
    })

    # 获取优化建议
    suggestions = MemoryOptimizer.suggest_optimization(df)
    logger.info(f"当前内存使用: {suggestions['current_memory_mb']:.2f}MB")
    logger.info(f"内存级别: {suggestions['memory_level']}")
    logger.info("优化建议:")
    for i, suggestion in enumerate(suggestions['suggestions'], 1):
        logger.info(f"{i}. {suggestion}")
    logger.info(f"估计可节省内存: {suggestions['estimated_savings_mb']:.2f}MB")

    return suggestions


def test_adj_factor_keep_float64():
    """回归：qfq_factor/hfq_factor 必须保留 Float64，禁止按百分比列降 Float32。

    复权因子参与连乘与"原始价×因子"还原，Float32 的 ~7 位有效位会在
    连乘中累积出可观测误差（曾位于 pct_cols 被降精度，属回归防护）。
    """
    logger.info("\n=== 测试复权因子 Float64 白名单 ===")

    n = 500
    df = pl.DataFrame({
        'close': np.random.uniform(5, 50, n),
        'qfq_factor': np.cumprod(np.full(n, 0.9997)),
        'hfq_factor': np.cumprod(np.full(n, 1.0003)),
        'pct_chg': np.random.randn(n),
    })

    optimized_df = MemoryOptimizer.optimize_dataframe(df)

    assert optimized_df['qfq_factor'].dtype == pl.Float64, \
        f"qfq_factor 被降为 {optimized_df['qfq_factor'].dtype}，复权连乘会累积精度误差"
    assert optimized_df['hfq_factor'].dtype == pl.Float64, \
        f"hfq_factor 被降为 {optimized_df['hfq_factor'].dtype}，复权连乘会累积精度误差"
    # 因子值不得有任何舍入改动
    assert optimized_df['qfq_factor'].equals(df['qfq_factor'])
    assert optimized_df['hfq_factor'].equals(df['hfq_factor'])
    # 对照：百分比列仍按设计降 Float32
    assert optimized_df['pct_chg'].dtype == pl.Float32
    logger.info("复权因子保持 Float64，pct_chg 仍为 Float32，符合预期")


def test_volume_large_value_stays_int64():
    """P1-4 回归：成交量超过 Int32 上限（2**31-1）时必须保持 Int64。

    回归：判据曾误用 2**32（4294967296）作为 Int32 上限，成交量 3e9
    （落在 2**31-1 与 2**32 之间）被错误降为 Int32，cast 直接抛
    InvalidOperationError（溢出）。
    """
    logger.info("\n=== 测试超大成交量 Int64 回归 ===")

    df = pl.DataFrame({
        'volume': pl.Series('volume', [1, 3_000_000_000], dtype=pl.Int64),
    })

    optimized_df = MemoryOptimizer.optimize_dataframe(df)

    assert optimized_df['volume'].dtype == pl.Int64, \
        f"volume=3e9 被降为 {optimized_df['volume'].dtype}，将触发 Int32 溢出"
    assert optimized_df['volume'].to_list() == [1, 3_000_000_000]


def test_int_boundaries_use_signed_limits():
    """P1-4 回归：整数降型必须按有符号上限判断（Int8=127 / Int16=32767）。

    回归：原先只按 2**32 判 Int32、其余一律 Int32，跨越 127/32767 边界
    的整数列会在 cast 时溢出。
    """
    logger.info("\n=== 测试有符号整数边界 ===")

    df = pl.DataFrame({
        'up_count': pl.Series('up_count', [0, 127], dtype=pl.Int64),
        'down_count': pl.Series('down_count', [0, 200], dtype=pl.Int64),
        'total_count': pl.Series('total_count', [0, 3_000_000_000], dtype=pl.Int64),
    })

    optimized_df = MemoryOptimizer.optimize_dataframe(df)

    assert optimized_df['up_count'].dtype == pl.Int8
    assert optimized_df['down_count'].dtype == pl.Int16
    assert optimized_df['total_count'].dtype == pl.Int64
    for col in ('up_count', 'down_count', 'total_count'):
        assert optimized_df[col].to_list() == df[col].to_list()


def test_alpha_factor_columns_keep_float64():
    """P1-4 回归：通用 Float64 不再降 Float32，Alpha 因子列必须保持 Float64。

    回归：通用分支曾把所有未识别的 Float64 列降为 Float32（~7 位有效位），
    alpha_001 等因子列在排序/加权时出现可观测偏差。仅 COLUMN_TYPE_MAP 中
    显式列（pct_chg / 指标 / 估值）允许按设计降 Float32。
    """
    logger.info("\n=== 测试 Alpha 因子 Float64 白名单 ===")

    n = 500
    rng = np.random.default_rng(20260305)
    df = pl.DataFrame({
        'alpha_001': rng.normal(0, 1e-4, n),
        'alpha_101': rng.normal(0, 1e-4, n),
        'ma5': rng.normal(size=n),        # 显式指标列，仍按设计降 Float32
        'pct_chg': rng.normal(size=n),    # 显式百分比列，仍按设计降 Float32
    })

    optimized_df = MemoryOptimizer.optimize_dataframe(df)

    assert optimized_df['alpha_001'].dtype == pl.Float64, \
        f"alpha_001 被降为 {optimized_df['alpha_001'].dtype}，因子精度受损"
    assert optimized_df['alpha_101'].dtype == pl.Float64, \
        f"alpha_101 被降为 {optimized_df['alpha_101'].dtype}，因子精度受损"
    # 因子值不得有任何舍入改动
    assert optimized_df['alpha_001'].equals(df['alpha_001'])
    assert optimized_df['alpha_101'].equals(df['alpha_101'])
    # 对照：显式列仍按设计降 Float32
    assert optimized_df['ma5'].dtype == pl.Float32
    assert optimized_df['pct_chg'].dtype == pl.Float32


if __name__ == "__main__":
    logger.info("MemoryOptimizer 测试开始")
    logger.info("=" * 60)

    # 运行各个测试
    df, optimized_df = test_basic_optimization()
    optimized_large_df = test_large_dataset()
    suggestions = test_optimization_suggestions()
    test_adj_factor_keep_float64()
    test_volume_large_value_stays_int64()
    test_int_boundaries_use_signed_limits()
    test_alpha_factor_columns_keep_float64()

    logger.info("\n" + "=" * 60)
    logger.info("MemoryOptimizer 测试完成")
