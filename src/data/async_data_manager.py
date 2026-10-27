#!/usr/bin/env python3

"""
异步数据获取管理器，使用asyncio实现高效的并行数据获取
"""

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

import polars as pl
from loguru import logger

from src.data.data_cache import global_data_cache
from src.utils.memory_optimizer import MemoryOptimizer
from src.utils.monitoring import global_monitoring_system


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

        # 线程池用于执行同步操作
        self.executor = ThreadPoolExecutor(max_workers=10)

    async def _get_data_from_sources_async(self, data_type: str, ts_code: str, start_date: str, end_date: str, freq: str = "daily", adjustment_type: str = "hfq"):
        """
        异步通用数据获取方法

        Args:
            data_type: 数据类型，stock或index
            ts_code: 代码
            start_date: 开始日期
            end_date: 结束日期
            freq: 周期，daily或minute
            adjustment_type: 复权类型，qfq=前复权, hfq=后复权, none=不复权

        Returns:
            pl.DataFrame: 数据
        """
        try:
            # 数据类型映射
            data_type_map = {
                'stock': {
                    'model': 'StockDaily',
                    'module': 'stock',
                    'handler_methods': {
                        'tdx': 'get_kline_data',
                        'akshare': 'get_stock_data',
                        'baostock': 'download_stock_daily',
                        'plugin': 'get_stock_data'
                    }
                },
                'index': {
                    'model': 'IndexDaily',
                    'module': 'index',
                    'handler_methods': {
                        'tdx': 'get_kline_data',
                        'akshare': 'get_index_data',
                        'baostock': 'download_index_daily',
                        'plugin': 'get_index_data'
                    }
                }
            }

            if data_type not in data_type_map:
                raise ValueError(f"不支持的数据类型: {data_type}")

            data_type_map[data_type]
            type_name = "股票" if data_type == "stock" else "指数"

            def process_result(result):
                """
                统一处理数据源结果，转换为Polars DataFrame
                """
                if result is None:
                    return None

                # 已经是Polars LazyFrame，执行并返回DataFrame
                if hasattr(result, 'collect'):
                    return result.collect()

                # 已经是Polars DataFrame
                if hasattr(result, 'to_pandas'):
                    return result

                # 是pandas DataFrame，转换为Polars
                if hasattr(result, 'to_dict'):
                    if hasattr(result, 'empty') and result.empty:
                        return None
                    return pl.from_pandas(result)

                # 是dict类型（Baostock离线模式返回），取第一个值
                if isinstance(result, dict):
                    if len(result) > 0:
                        first_value = list(result.values())[0]
                        if hasattr(first_value, 'to_dict'):
                            if hasattr(first_value, 'empty') and first_value.empty:
                                return None
                            return pl.from_pandas(first_value)
                        elif isinstance(first_value, list):
                            return pl.DataFrame(first_value)
                    return None

                # 是list类型，转换为Polars
                if isinstance(result, list):
                    if len(result) == 0:
                        return None
                    return pl.DataFrame(result)

                # 其他类型，尝试直接转换
                try:
                    return pl.DataFrame(result)
                except (ValueError, TypeError):
                    return None

            async def fetch_from_source_async(source_name, handler, method_name, **kwargs):
                """
                从单个数据源异步获取数据
                """
                start_time = time.time()
                success = False

                try:
                    logger.info(f"异步获取: 从{source_name}获取{type_name}{ts_code}数据")

                    # 在线程池中执行同步方法
                    # 创建一个包装函数来处理关键字参数
                    def wrapper():
                        return getattr(handler, method_name)(**kwargs)

                    result = await asyncio.get_event_loop().run_in_executor(
                        self.executor,
                        wrapper
                    )

                    processed_result = process_result(result)
                    if processed_result is not None:
                        # 内存优化
                        optimized_result = MemoryOptimizer.optimize_dataframe(processed_result, enable_sparse=True)
                        success = True
                        return True, optimized_result
                    else:
                        return False, f"{source_name}返回空数据"
                except (OSError, RuntimeError, ValueError) as e:
                    logger.warning(f"从{source_name}获取{type_name}数据失败: {e}")
                    return False, str(e)
                finally:
                    # 记录监控数据
                    response_time = time.time() - start_time
                    global_monitoring_system.record_data_source_request(source_name, response_time, success)

                    # 更新数据源优先级
                    if source_name not in self.source_priorities:
                        self.source_priorities[source_name] = []
                    self.source_priorities[source_name].append(response_time)
                    # 只保留最近10次的响应时间
                    if len(self.source_priorities[source_name]) > 10:
                        self.source_priorities[source_name] = self.source_priorities[source_name][-10:]

            # 数据获取统一委托给 data_manager，后者走 DataFetcher 单一路径：
            # DataFetcher._get_data_from_database + _apply_adjustment（含双复权列 + 因子回退）
            # + 数据源故障转移。AsyncDataManager 不再自己做 DB 查询和复权，
            # 避免与 DataFetcher 的 _apply_adjustment 形成行为漂移。
            # 直接调用data_manager的方法获取数据
            def fetch_data_sync():
                if data_type == 'stock':
                    # 转换频率格式
                    freq_map = {'daily': '1d', 'minute': '1m'}
                    frequency = freq_map.get(freq, '1d')
                    return self.data_manager.get_stock_data(ts_code, start_date, end_date, frequency, adjustment_type)
                else:
                    freq_map = {'daily': '1d', 'minute': '1m'}
                    frequency = freq_map.get(freq, '1d')
                    return self.data_manager.get_index_data(ts_code, start_date, end_date, frequency)

            # 在线程池中执行同步方法
            result = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                fetch_data_sync
            )

            return result

        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"获取{type_name}数据失败: {e}")
            raise

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
            freq_map = {'1d': 'daily', '1m': 'minute'}
            freq = freq_map.get('1d', 'daily')
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
                    except Exception as e:
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
