#!/usr/bin/env python3

"""
测试周线成交量计算修复
"""

import polars as pl
from loguru import logger

from src.data.managers.data_processor import DataProcessor
from src.utils.memory_optimizer import MemoryOptimizer

# 创建测试数据
df = pl.DataFrame({
    'date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05'],
    'open': [100, 101, 102, 103, 104],
    'high': [105, 106, 107, 108, 109],
    'low': [95, 96, 97, 98, 99],
    'close': [102, 103, 104, 105, 106],
    'volume': [1000000000, 2000000000, 3000000000, 4000000000, 5000000000],
    'amount': [100000000000, 200000000000, 300000000000, 400000000000, 500000000000]
})

logger.info("原始日线数据:")
logger.info(df)
logger.info("\n成交量类型:")
logger.info(df['volume'].dtype)
logger.info("\n成交量值:")
logger.info(df['volume'].to_list())

# 转换为周线数据
processor = DataProcessor(None)
weekly_df = processor.convert_frequency(df, '1w')

logger.info("\n周线数据:")
logger.info(weekly_df)
logger.info("\n成交量类型:")
logger.info(weekly_df['volume'].dtype)
logger.info("\n成交量值:")
logger.info(weekly_df['volume'][0])

# 验证成交量是否正确
expected_volume = sum(df['volume'])
actual_volume = weekly_df['volume'][0]

logger.info("\n验证结果:")
logger.info(f"预期成交量: {expected_volume}")
logger.info(f"实际成交量: {actual_volume}")
logger.info(f"是否正确: {expected_volume == actual_volume}")

# 测试内存优化后的结果
optimized_df = MemoryOptimizer.optimize_dataframe(weekly_df, enable_sparse=True)

logger.info("\n内存优化后:")
logger.info("成交量类型:")
logger.info(optimized_df['volume'].dtype)
logger.info("成交量值:")
logger.info(optimized_df['volume'][0])

# 再次验证
optimized_volume = optimized_df['volume'][0]
logger.info("\n优化后验证结果:")
logger.info(f"预期成交量: {expected_volume}")
logger.info(f"实际成交量: {optimized_volume}")
logger.info(f"是否正确: {expected_volume == optimized_volume}")
