#!/usr/bin/env python3

"""
异步数据获取管理器

只负责在 asyncio.run_in_executor 中把 data_manager 包一层异步接口；
不复刻 handler 调度或复权口径，与 DataFetcher._apply_adjustment 保持单一路径。

【asyncio 孤岛】生产代码没有任何调用方接入本模块：src 内仅 DataManager 的
async 包装方法转发到此，而这些 async 方法全仓唯一的调用点是
tests/performance_test.py 的 asyncio.run(...)。也就是说这条 10 线程长驻池
（本项目长驻池中占用最大者）目前只为性能测试存在。若接入真实异步调用方，
需要同时确认 UI/回测主循环不被 asyncio 事件循环阻塞；若长期不接入，应连同
DataManager 的 async 包装方法一并移除。
"""

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

import polars as pl
from loguru import logger

from src.data.data_cache import global_data_cache


class AsyncDataManager:
    """
    异步数据管理器，使用asyncio实现高效的并行数据获取
    """

    def __init__(self, data_manager):
        """
        初始化异步数据管理器

        Args:
            data_manager: 同步数据管理器实例
        """
        self.data_manager = data_manager
        self.config = data_manager.config
        self.db_manager = data_manager.db_manager
        self.plugin_manager = data_manager.plugin_manager

        # 插件数据源
        self.plugin_datasources = data_manager.plugin_datasources

        # 数据源优先级缓存（基于历史响应时间）
        self.source_priorities = {}

        # 线程池用于执行同步操作（长驻 10 线程，当前仅性能测试路径使用，见模块文档）
        self.executor = ThreadPoolExecutor(max_workers=10)

    async def _get_data_from_sources_async(self, data_type: str, ts_code: str, start_date: str, end_date: str, freq: str = "daily", adjustment_type: str = "hfq"):
        """
        异步获取单标的数据。统一委托 data_manager，后者走 DataFetcher 单一路径
        （DB 查询 + DataFetcher._apply_adjustment + 数据源故障转移）。
        不复刻 handler 调度或复权口径，避免与 DataFetcher 的 _apply_adjustment 漂移。
        """
        if data_type not in ('stock', 'index'):
            raise ValueError(f"不支持的数据类型: {data_type}")

        freq_map = {'daily': '1d', 'minute': '1m'}
        frequency = freq_map.get(freq, '1d')

        def fetch_data_sync():
            if data_type == 'stock':
                return self.data_manager.get_stock_data(
                    ts_code, start_date, end_date, frequency, adjustment_type)
            return self.data_manager.get_index_data(
                ts_code, start_date, end_date, frequency)

        return await asyncio.get_event_loop().run_in_executor(self.executor, fetch_data_sync)

    async def get_stock_data_async(self, stock_code: str, start_date: str, end_date: str, frequency: str = '1d', adjustment_type: str = 'hfq') -> pl.DataFrame:
        """
        异步获取股票历史数据

        Args:
            stock_code: 股票代码
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 数据频率，默认：1d（日线），支持1d/1w/1m（日/周/月线）
            adjustment_type: 复权类型，qfq=前复权, hfq=后复权, none=不复权

        Returns:
            pl.DataFrame: 股票历史数据
        """
        # 尝试从缓存获取数据
        cached_data = global_data_cache.get('stock', stock_code, start_date, end_date,
                                         frequency=frequency, adjustment_type=adjustment_type)
        if cached_data is not None:
            logger.info(f"从缓存获取股票数据: {stock_code} {start_date} to {end_date}")
            return cached_data

        # 将周线和月线转换为日线获取，然后进行聚合
        if frequency in ['1w', '1m']:
            # 获取日线数据
            freq = 'daily'
            df = await self._get_data_from_sources_async("stock", stock_code, start_date, end_date, freq, adjustment_type)

            # 将日线数据转换为周线或月线
            if not df.is_empty():
                # 使用惰性计算优化转换过程
                lazy_df = df.lazy()

                # 确保有日期列
                if 'trade_date' in df.columns and 'date' not in df.columns:
                    lazy_df = lazy_df.with_columns(pl.col('trade_date').alias('date'))
                elif 'date' not in df.columns:
                    logger.error("DataFrame中没有日期列")
                    return df

                # 统一日期列为 Date，兼容 Date/Datetime/字符串
                if df.schema['date'] == pl.Utf8:
                    lazy_df = lazy_df.with_columns(
                        pl.coalesce([
                            pl.col('date').str.strptime(pl.Date, "%Y-%m-%d", strict=False),
                            pl.col('date').str.strptime(pl.Date, "%Y%m%d", strict=False),
                        ]).alias('date')
                    )
                else:
                    lazy_df = lazy_df.with_columns(pl.col('date').cast(pl.Date))

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
                lazy_df = lazy_df.group_by(group_cols).agg([
                    pl.col('date').first().alias('date'),
                    pl.col('open').first().alias('open'),
                    pl.col('high').max().alias('high'),
                    pl.col('low').min().alias('low'),
                    pl.col('close').last().alias('close'),
                    pl.col('vol').sum().alias('vol'),
                    pl.col('amount').sum().alias('amount'),
                    pl.col('pct_chg').sum().alias('pct_chg'),
                    pl.col('change').sum().alias('change')
                ])

                # 按日期排序
                lazy_df = lazy_df.sort('date')

                # 将日期转换回字符串格式
                lazy_df = lazy_df.with_columns(
                    pl.col('date').dt.strftime("%Y-%m-%d").alias('date')
                )

                # 执行优化的惰性计算（lazy_exec 内部已完成 dtype 压缩，无需重复优化）
                from src.utils.lazy_optimizer import lazy_exec
                result = lazy_exec(lazy_df)
                logger.info(f"将日线数据转换为{frequency}数据，从{df.height}条转换为{result.height}条")
            else:
                result = df
        else:
            # 日线或分钟线，直接获取
            freq_map = {'1d': 'daily', '1m': 'minute'}
            freq = freq_map.get(frequency, 'daily')
            result = await self._get_data_from_sources_async("stock", stock_code, start_date, end_date, freq, adjustment_type)

        # 将结果存入缓存
        if not result.is_empty():
            global_data_cache.set(result, 'stock', stock_code, start_date, end_date,
                                frequency=frequency, adjustment_type=adjustment_type)
            logger.info(f"股票数据缓存已更新: {stock_code} {start_date} to {end_date}")

        return result

    async def get_index_data_async(self, index_code: str, start_date: str, end_date: str, frequency: str = '1d') -> pl.DataFrame:
        """
        异步获取指数历史数据

        Args:
            index_code: 指数代码
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 数据频率，默认：1d（日线）

        Returns:
            pl.DataFrame: 指数历史数据
        """
        # 尝试从缓存获取数据
        cached_data = global_data_cache.get('index', index_code, start_date, end_date,
                                         frequency=frequency)
        if cached_data is not None:
            logger.info(f"从缓存获取指数数据: {index_code} {start_date} to {end_date}")
            return cached_data

        freq_map = {'1d': 'daily', '1m': 'minute'}
        freq = freq_map.get(frequency, 'daily')
        result = await self._get_data_from_sources_async("index", index_code, start_date, end_date, freq)

        # 将结果存入缓存
        if not result.is_empty():
            global_data_cache.set(result, 'index', index_code, start_date, end_date,
                                frequency=frequency)
            logger.info(f"指数数据缓存已更新: {index_code} {start_date} to {end_date}")

        return result

    async def get_multiple_stocks_data_async(self, stock_codes: List[str], start_date: str, end_date: str, frequency: str = '1d', adjustment_type: str = 'hfq', batch_size: int = 10, timeout: int = 30) -> Dict[str, pl.DataFrame]:
        """
        异步并行获取多只股票数据

        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 数据频率，默认：1d（日线）
            adjustment_type: 复权类型，qfq=前复权, hfq=后复权, none=不复权
            batch_size: 批量处理大小，默认10
            timeout: 每个任务的超时时间（秒），默认30

        Returns:
            Dict[str, pl.DataFrame]: 股票代码到数据的映射
        """

        stock_data = {}
        total_stocks = len(stock_codes)
        processed_stocks = 0

        logger.info(f"开始并行获取{total_stocks}只股票数据，批量大小: {batch_size}")
        start_time = time.time()

        # 分批次处理
        for i in range(0, total_stocks, batch_size):
            batch_codes = stock_codes[i:i+batch_size]
            batch_size_current = len(batch_codes)
            logger.info(f"处理批次 {i//batch_size + 1}/{(total_stocks + batch_size - 1)//batch_size}，包含{batch_size_current}只股票")

            # 创建批量任务
            tasks = {}
            for stock_code in batch_codes:
                # 创建带超时的任务
                async def fetch_with_timeout(code):
                    try:
                        return await asyncio.wait_for(
                            self.get_stock_data_async(code, start_date, end_date, frequency, adjustment_type),
                            timeout=timeout
                        )
                    except TimeoutError:
                        logger.error(f"获取{code}数据超时")
                        return pl.DataFrame()
                    # 兜底理由：单只股票异步取数失败返回空表
                    except Exception as e:  # noqa: BLE001
                        logger.error(f"获取{code}数据失败: {e}")
                        return pl.DataFrame()

                task = fetch_with_timeout(stock_code)
                tasks[stock_code] = task

            # 并行执行当前批次的任务
            results = await asyncio.gather(*tasks.values())

            # 处理结果
            for j, stock_code in enumerate(tasks.keys()):
                result = results[j]
                stock_data[stock_code] = result
                processed_stocks += 1

                # 打印进度
                if processed_stocks % 5 == 0 or processed_stocks == total_stocks:
                    progress = (processed_stocks / total_stocks) * 100
                    elapsed_time = time.time() - start_time
                    remaining_time = (elapsed_time / processed_stocks) * (total_stocks - processed_stocks)
                    logger.info(f"进度: {processed_stocks}/{total_stocks} ({progress:.1f}%)，耗时: {elapsed_time:.2f}秒，预计剩余: {remaining_time:.2f}秒")

        total_time = time.time() - start_time
        logger.info(f"并行获取{total_stocks}只股票数据完成，总耗时: {total_time:.2f}秒")

        return stock_data

    def close(self):
        """
        关闭线程池
        """
        self.executor.shutdown(wait=False)
        logger.info("AsyncDataManager线程池已关闭")
