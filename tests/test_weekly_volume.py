#!/usr/bin/env python3

"""
测试周线成交量数据
"""

import polars as pl
from loguru import logger

from src.data.managers.data_processor import DataProcessor
from src.tech_analysis.indicators.volume import calculate_vol_ma

# 创建测试数据 - 模拟不同成交量的日线数据
df = pl.DataFrame({
    'date': [
        '2023-01-09', '2023-01-10', '2023-01-11', '2023-01-12', '2023-01-13',  # 第一周
        '2023-01-16', '2023-01-17', '2023-01-18', '2023-01-19', '2023-01-20',  # 第二周
        '2023-01-23', '2023-01-24', '2023-01-25', '2023-01-26', '2023-01-27'   # 第三周
    ],
    'open': [100] * 15,
    'high': [110] * 15,
    'low': [90] * 15,
    'close': [105, 106, 107, 108, 109,  # 第一周
              110, 111, 112, 113, 114,  # 第二周
              115, 116, 117, 118, 119], # 第三周
    'volume': [1000000000, 1500000000, 2000000000, 2500000000, 3000000000,  # 第一周
               3500000000, 4000000000, 4500000000, 5000000000, 5500000000,  # 第二周
               6000000000, 6500000000, 7000000000, 7500000000, 8000000000], # 第三周
    'amount': [100000000000] * 15
})

logger.info("原始日线数据:")
logger.info(df)
logger.info("\n成交量值:")
logger.info(df['volume'].to_list())

# 转换为周线数据
processor = DataProcessor(None)
weekly_df = processor.convert_frequency(df, '1w')

logger.info("\n周线数据:")
logger.info(weekly_df)
logger.info("\n周线成交量值:")
logger.info(weekly_df['volume'].to_list())

# 计算成交量均线
lazy_df = weekly_df.lazy()
lazy_df = calculate_vol_ma(lazy_df, [5, 10])
weekly_df_with_ma = lazy_df.collect()

logger.info("\n周线数据（含成交量均线）:")
logger.info(weekly_df_with_ma)
logger.info("\n成交量均线值:")
logger.info("{} {}", "vol_ma5:", weekly_df_with_ma['vol_ma5'].to_list())
logger.info("{} {}", "vol_ma10:", weekly_df_with_ma['vol_ma10'].to_list())

# 验证周线成交量计算是否正确
expected_volumes = [
    sum(df['volume'][0:5]),   # 第一周
    sum(df['volume'][5:10]),  # 第二周
    sum(df['volume'][10:15])  # 第三周
]

logger.info("\n验证结果:")
logger.info(f"预期成交量: {expected_volumes}")
logger.info(f"实际成交量: {weekly_df['volume'].to_list()}")
logger.info(f"是否正确: {expected_volumes == weekly_df['volume'].to_list()}")
