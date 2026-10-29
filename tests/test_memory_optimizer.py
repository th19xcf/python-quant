#!/usr/bin/env python3

"""
MemoryOptimizer 测试脚本
"""

import sys
from pathlib import Path

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
    print("=== 测试基本内存优化功能 ===")

    # 创建测试数据
    dates = [datetime(2020, 1, 1) + timedelta(days=i) for i in range(1000)]
    df = pl.DataFrame({
        'open': np.random.randn(1000),
        'volume': np.random.randint(1, 10000, 1000),
        'is_st': [True]*500 + [False]*500,
        'industry': ['tech']*200 + ['finance']*200 + ['health']*200 + ['energy']*200 + ['consumer']*200,
        'date': dates
    })

    print("原始数据类型:")
    print(df.dtypes)
    print()

    print("原始数据内存使用:")
    print_df_memory(df)

    # 优化数据
    optimized_df = optimize_df(df)

    print("优化后数据类型:")
    print(optimized_df.dtypes)
    print()

    print("优化后数据内存使用:")
    print_df_memory(optimized_df)

    return df, optimized_df


def test_large_dataset():
    """测试大数据集处理"""
    print("\n=== 测试大数据集处理 ===")

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
    print(f"大数据集大小: {len(large_df):,}行")

    # 分块处理
    optimized_large_df = process_large_df(large_df, chunk_size=50000)
    print("大数据集优化完成")

    print("优化后数据类型:")
    print(optimized_large_df.dtypes)

    return optimized_large_df


def test_optimization_suggestions():
    """测试优化建议功能"""
    print("\n=== 测试优化建议功能 ===")

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
    print(f"当前内存使用: {suggestions['current_memory_mb']:.2f}MB")
    print(f"内存级别: {suggestions['memory_level']}")
    print("优化建议:")
    for i, suggestion in enumerate(suggestions['suggestions'], 1):
        print(f"{i}. {suggestion}")
    print(f"估计可节省内存: {suggestions['estimated_savings_mb']:.2f}MB")

    return suggestions


def test_adj_factor_keep_float64():
    """回归：qfq_factor/hfq_factor 必须保留 Float64，禁止按百分比列降 Float32。

    复权因子参与连乘与"原始价×因子"还原，Float32 的 ~7 位有效位会在
    连乘中累积出可观测误差（曾位于 pct_cols 被降精度，属回归防护）。
    """
    print("\n=== 测试复权因子 Float64 白名单 ===")

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
    print("复权因子保持 Float64，pct_chg 仍为 Float32，符合预期")


if __name__ == "__main__":
    print("MemoryOptimizer 测试开始")
    print("=" * 60)

    # 运行各个测试
    df, optimized_df = test_basic_optimization()
    optimized_large_df = test_large_dataset()
    suggestions = test_optimization_suggestions()
    test_adj_factor_keep_float64()

    print("\n" + "=" * 60)
    print("MemoryOptimizer 测试完成")
