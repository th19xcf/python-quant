#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MemoryOptimizer 测试脚本
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from utils.memory_optimizer import MemoryOptimizer, MemoryMonitor, optimize_df, process_large_df, print_df_memory, get_memory_monitor
import polars as pl
import numpy as np
from datetime import datetime, timedelta


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


def test_memory_monitor():
    """测试内存监控功能"""
    print("\n=== 测试内存监控功能 ===")
    
    # 获取全局内存监控器
    monitor = get_memory_monitor()
    
    # 启动监控
    monitor.start()
    print("内存监控已启动")
    
    # 模拟内存使用
    import time
    time.sleep(2)
    
    # 获取当前内存使用
    current_memory = monitor.get_current_memory()
    print(f"当前内存使用: {current_memory:.2f}MB")
    
    # 停止监控
    monitor.stop()
    print("内存监控已停止")
    
    return current_memory


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


if __name__ == "__main__":
    print("MemoryOptimizer 测试开始")
    print("=" * 60)
    
    # 运行各个测试
    df, optimized_df = test_basic_optimization()
    optimized_large_df = test_large_dataset()
    current_memory = test_memory_monitor()
    suggestions = test_optimization_suggestions()
    
    print("\n" + "=" * 60)
    print("MemoryOptimizer 测试完成")
