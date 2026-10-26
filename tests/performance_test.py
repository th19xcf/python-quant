#!/usr/bin/env python3

"""
性能测试脚本，用于测试优化前后的性能对比
"""

import os
import sys

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import time

import numpy as np
import polars as pl
from loguru import logger

from src.data.data_manager import DataManager
from src.database.db_manager import DatabaseManager
from src.tech_analysis.indicator_manager import global_indicator_manager
from src.utils.config import config


def generate_test_data(days=1000):
    """
    生成测试数据

    Args:
        days: 数据天数

    Returns:
        pl.DataFrame: 测试数据
    """
    # 生成日期
    import pandas as pd
    dates = pd.date_range(start="2020-01-01", periods=days, freq="D")

    # 生成随机价格数据
    np.random.seed(42)
    close = np.random.normal(100, 10, days)
    open = close * (1 + np.random.normal(0, 0.01, days))
    high = np.maximum(open, close) * (1 + np.random.normal(0, 0.01, days))
    low = np.minimum(open, close) * (1 - np.random.normal(0, 0.01, days))
    volume = np.random.normal(1000000, 500000, days)
    amount = volume * close

    # 创建DataFrame
    df = pl.DataFrame({
        'trade_date': dates.strftime('%Y%m%d'),
        'open': open,
        'high': high,
        'low': low,
        'close': close,
        'volume': volume,
        'amount': amount
    })

    return df


def test_indicator_calculation_performance():
    """
    测试技术指标计算性能
    """
    logger.info("开始测试技术指标计算性能")

    # 生成测试数据
    data = generate_test_data(1000)
    logger.info(f"生成测试数据完成，共{len(data)}条记录")

    # 测试指标列表
    indicators = ['ma', 'macd', 'rsi', 'kdj', 'wr', 'boll', 'dmi', 'cci', 'roc', 'mtm', 'obv', 'vr', 'psy', 'trix', 'brar', 'asi', 'emv', 'mcst']
    logger.info(f"测试指标数量: {len(indicators)}")

    # 测试串行计算
    logger.info("开始测试串行计算")
    start_time = time.time()
    result_serial = global_indicator_manager.calculate_indicators(data, indicators, parallel=False)
    serial_time = time.time() - start_time
    logger.info(f"串行计算完成，耗时: {serial_time:.2f}秒")

    # 测试并行计算
    logger.info("开始测试并行计算")
    start_time = time.time()
    result_parallel = global_indicator_manager.calculate_indicators(data, indicators, parallel=True)
    parallel_time = time.time() - start_time
    logger.info(f"并行计算完成，耗时: {parallel_time:.2f}秒")

    # 验证结果一致性
    logger.info("验证结果一致性")
    columns = [col for col in result_serial.columns if col not in data.columns]
    for col in columns:
        if col in result_parallel.columns:
            serial_values = result_serial[col].to_numpy()
            parallel_values = result_parallel[col].to_numpy()
            # 检查是否有NaN值
            serial_nan = np.isnan(serial_values).sum()
            parallel_nan = np.isnan(parallel_values).sum()
            if serial_nan != parallel_nan:
                logger.warning(f"列{col}的NaN值数量不一致: {serial_nan} vs {parallel_nan}")
            # 检查非NaN值是否接近
            mask = ~np.isnan(serial_values) & ~np.isnan(parallel_values)
            if mask.any():
                diff = np.abs(serial_values[mask] - parallel_values[mask])
                max_diff = np.max(diff)
                np.mean(diff)
                if max_diff > 1e-6:
                    logger.warning(f"列{col}的最大值差异: {max_diff:.6f}")
                else:
                    logger.debug(f"列{col}的差异在可接受范围内，最大差异: {max_diff:.6f}")
        else:
            logger.warning(f"并行计算结果中缺少列: {col}")

    # 计算性能提升
    speedup = serial_time / parallel_time
    logger.info(f"性能提升: {speedup:.2f}x")

    return {
        'serial_time': serial_time,
        'parallel_time': parallel_time,
        'speedup': speedup
    }


def test_data_fetch_performance():
    """
    测试数据获取性能
    """
    logger.info("开始测试数据获取性能")

    # 初始化数据管理器
    db_manager = DatabaseManager(config.database)
    data_manager = DataManager(config, db_manager)

    # 测试股票列表
    stock_codes = [
        "600000.SH", "600519.SH", "000858.SZ", "000333.SZ", "601318.SH",
        "601888.SH", "600036.SH", "601398.SH", "600276.SH", "002594.SZ",
        "000001.SZ", "601857.SH", "600887.SH", "000651.SZ", "600031.SH"
    ]
    logger.info(f"测试股票数量: {len(stock_codes)}")

    # 测试日期范围
    start_date = "2023-01-01"
    end_date = "2023-12-31"

    # 测试串行获取（使用同步方法）
    logger.info("开始测试串行数据获取")
    start_time = time.time()
    result_serial = {}
    for stock_code in stock_codes:
        result_serial[stock_code] = data_manager.get_stock_data(stock_code, start_date, end_date)
    serial_time = time.time() - start_time
    logger.info(f"串行数据获取完成，耗时: {serial_time:.2f}秒")

    # 测试并行获取（使用异步方法）
    import asyncio
    logger.info("开始测试并行数据获取")
    start_time = time.time()
    result_parallel = asyncio.run(data_manager.get_multiple_stocks_data_async(stock_codes, start_date, end_date))
    parallel_time = time.time() - start_time
    logger.info(f"并行数据获取完成，耗时: {parallel_time:.2f}秒")

    # 验证结果一致性
    logger.info("验证结果一致性")
    for stock_code in stock_codes:
        if stock_code in result_serial and stock_code in result_parallel:
            serial_df = result_serial[stock_code]
            parallel_df = result_parallel[stock_code]
            if len(serial_df) != len(parallel_df):
                logger.warning(f"股票{stock_code}的数据长度不一致: {len(serial_df)} vs {len(parallel_df)}")
            else:
                logger.debug(f"股票{stock_code}的数据长度一致: {len(serial_df)}")
        else:
            logger.warning(f"股票{stock_code}的数据获取失败")

    # 计算性能提升
    speedup = serial_time / parallel_time
    logger.info(f"性能提升: {speedup:.2f}x")

    # 关闭数据库连接
    try:
        if hasattr(db_manager, 'close'):
            db_manager.close()
    except Exception as e:
        logger.warning(f"关闭数据库连接时出错: {e}")

    return {
        'serial_time': serial_time,
        'parallel_time': parallel_time,
        'speedup': speedup
    }


def main():
    """
    主函数
    """
    logger.info("开始性能测试")

    # 测试技术指标计算性能
    indicator_results = test_indicator_calculation_performance()

    # 测试数据获取性能
    data_results = test_data_fetch_performance()

    # 打印测试结果
    logger.info("\n性能测试结果汇总")
    logger.info("======================")
    logger.info("技术指标计算:")
    logger.info(f"  串行时间: {indicator_results['serial_time']:.2f}秒")
    logger.info(f"  并行时间: {indicator_results['parallel_time']:.2f}秒")
    logger.info(f"  性能提升: {indicator_results['speedup']:.2f}x")
    logger.info("数据获取:")
    logger.info(f"  串行时间: {data_results['serial_time']:.2f}秒")
    logger.info(f"  并行时间: {data_results['parallel_time']:.2f}秒")
    logger.info(f"  性能提升: {data_results['speedup']:.2f}x")

    logger.info("性能测试完成")


if __name__ == "__main__":
    main()
