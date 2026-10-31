#!/usr/bin/env python3

"""
测试计算优化效果
"""

import time

import numpy as np
import pandas as pd
import polars as pl
from loguru import logger

from src.tech_analysis.gpu_acceleration import is_gpu_available
from src.tech_analysis.streaming_processor import (
    create_streaming_processor,
)
from src.tech_analysis.technical_analyzer import TechnicalAnalyzer


def generate_test_data(days: int = 1000) -> pl.DataFrame:
    """
    生成测试数据

    Args:
        days: 数据天数

    Returns:
        pl.DataFrame: 测试数据
    """
    dates = pd.date_range('2020-01-01', periods=days)

    # 生成随机价格数据
    close = np.random.normal(100, 10, days)
    high = close * np.random.uniform(1.0, 1.02, days)
    low = close * np.random.uniform(0.98, 1.0, days)
    open_ = close * np.random.uniform(0.99, 1.01, days)
    volume = np.random.normal(1000000, 500000, days)

    # 创建DataFrame并转换日期类型为Date，数值类型为Float32
    df = pl.DataFrame({
        'date': dates,
        'open': open_.astype(np.float32),
        'high': high.astype(np.float32),
        'low': low.astype(np.float32),
        'close': close.astype(np.float32),
        'volume': volume.astype(np.float32)
    })

    # 转换日期类型为Date
    df = df.with_columns(
        pl.col('date').dt.date()
    )

    return df


def test_gpu_acceleration():
    """
    测试GPU加速计算
    """
    logger.info("=== 测试GPU加速计算 ===")

    # 生成测试数据
    data = generate_test_data(1000)

    # 创建技术分析器
    analyzer = TechnicalAnalyzer(data)

    # 测试指标列表
    indicators = ['ma', 'macd', 'rsi', 'sar']

    for indicator in indicators:
        start_time = time.time()
        analyzer.calculate_indicator(indicator)
        end_time = time.time()
        logger.info(f"{indicator} 计算时间: {end_time - start_time:.4f}秒")

    logger.info("")


def test_incremental_calculation():
    """
    测试增量计算
    """
    logger.info("=== 测试增量计算 ===")

    # 生成历史数据
    history_data = generate_test_data(990)

    # 创建技术分析器
    analyzer = TechnicalAnalyzer(history_data)

    # 预先计算指标
    analyzer.calculate_indicator('ma')

    # 生成新数据
    new_data = generate_test_data(10)

    # 测试增量计算
    start_time = time.time()
    analyzer.calculate_indicator('ma', incremental_data=new_data)
    end_time = time.time()
    logger.info(f"增量计算MA时间: {end_time - start_time:.4f}秒")

    logger.info("")


def test_streaming_processing():
    """
    测试流式处理
    """
    logger.info("=== 测试流式处理 ===")

    # 创建流式处理器
    processor = create_streaming_processor(['ma', 'macd', 'rsi'])

    # 生成测试数据
    all_data = generate_test_data(100)

    # 模拟实时数据流式处理
    batch_size = 10
    total_time = 0

    for i in range(0, len(all_data), batch_size):
        batch = all_data[i:i+batch_size]
        start_time = time.time()
        processor.process_data(batch)
        end_time = time.time()
        batch_time = end_time - start_time
        total_time += batch_time
        logger.info(f"批次 {i//batch_size + 1} 处理时间: {batch_time:.4f}秒")

    logger.info(f"总处理时间: {total_time:.4f}秒")
    logger.info("")


def test_performance_comparison():
    """
    性能比较测试
    """
    logger.info("=== 性能比较测试 ===")

    # 生成测试数据
    data = generate_test_data(2000)

    # 创建技术分析器
    analyzer = TechnicalAnalyzer(data)

    # 测试多个指标的计算时间
    indicators = ['ma', 'macd', 'rsi', 'kdj', 'boll', 'wr', 'obv']

    start_time = time.time()
    for indicator in indicators:
        analyzer.calculate_indicator(indicator)
    end_time = time.time()

    logger.info(f"计算 {len(indicators)} 个指标总时间: {end_time - start_time:.4f}秒")
    logger.info("")


if __name__ == "__main__":
    import pandas as pd

    logger.info("开始测试计算优化效果...")
    logger.info(f"GPU可用: {is_gpu_available()}")
    logger.info("")

    # 测试GPU加速
    test_gpu_acceleration()

    # 测试增量计算
    test_incremental_calculation()

    # 测试流式处理
    test_streaming_processing()

    # 测试性能比较
    test_performance_comparison()

    logger.info("测试完成！")
