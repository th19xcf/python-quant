#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
异步数据获取管理器，使用asyncio实现高效的并行数据获取
"""

import asyncio
import time
from typing import List, Dict, Any, Optional, Union
import polars as pl
from loguru import logger
from concurrent.futures import ThreadPoolExecutor

from src.utils.exceptions import (
    DataSourceConnectionError,
    DataSourceNotAvailableError,
    DataValidationError,
    DataNotFoundError,
    DataSaveError
)
from src.data.data_cache import global_data_cache
from src.utils.monitoring import global_monitoring_system
from src.utils.memory_optimizer import MemoryOptimizer


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
        
        # 数据源处理器
        self.tdx_handler = data_manager.tdx_handler
        self.akshare_handler = data_manager.akshare_handler
        self.baostock_handler = data_manager.baostock_handler
        self.macro_handler = data_manager.macro_handler
        self.news_handler = data_manager.news_handler
        
        # 插件数据源
        self.plugin_datasources = data_manager.plugin_datasources
        
        # 数据源优先级缓存（基于历史响应时间）
        self.source_priorities = {}
        
        # 线程池用于执行同步操作
        self.executor = ThreadPoolExecutor(max_workers=10)
    
    async def _get_data_from_sources_async(self, data_type: str, ts_code: str, start_date: str, end_date: str, freq: str = "daily", adjustment_type: str = "qfq"):
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
            
            type_map = data_type_map[data_type]
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
            
            # 优先从数据库获取数据
            if self.db_manager and self.db_manager.is_connected():
                logger.info(f"从数据库获取{type_name}{ts_code}数据")
                try:
                    # 动态导入模型
                    module_path = f"src.database.models.{type_map['module']}"
                    module = __import__(module_path, fromlist=[type_map['model']])
                    model_class = getattr(module, type_map['model'])
                    
                    session = self.db_manager.get_session()
                    if session:
                        query = session.query(model_class).filter(
                            getattr(model_class, 'ts_code') == ts_code,
                            getattr(model_class, 'trade_date') >= start_date,
                            getattr(model_class, 'trade_date') <= end_date
                        ).order_by(getattr(model_class, 'trade_date'))
                        
                        data = query.all()
                        if data:
                            # 提取数据记录，直接构建适合Polars的字典列表
                            has_pct_chg = hasattr(data[0], 'pct_chg')
                            
                            # 检查是否有复权因子字段
                            has_qfq_factor = hasattr(data[0], 'qfq_factor')
                            has_hfq_factor = hasattr(data[0], 'hfq_factor')
                            
                            # 根据复权类型选择对应的价格字段
                            if adjustment_type == 'qfq':
                                # 前复权
                                data_records = []
                                for item in data:
                                    # 优先使用预计算的复权价格
                                    if hasattr(item, 'qfq_open') and item.qfq_open:
                                        open_price = item.qfq_open
                                        high_price = item.qfq_high
                                        low_price = item.qfq_low
                                        close_price = item.qfq_close
                                    # 其次使用复权因子实时计算
                                    elif has_qfq_factor and item.qfq_factor and item.qfq_factor != 1.0:
                                        open_price = item.open * item.qfq_factor
                                        high_price = item.high * item.qfq_factor
                                        low_price = item.low * item.qfq_factor
                                        close_price = item.close * item.qfq_factor
                                    # 回退到原始价格
                                    else:
                                        open_price = item.open
                                        high_price = item.high
                                        low_price = item.low
                                        close_price = item.close

                                    record = {
                                        'trade_date': item.trade_date,
                                        'open': open_price,
                                        'high': high_price,
                                        'low': low_price,
                                        'close': close_price,
                                        'volume': item.vol,
                                        'amount': item.amount,
                                        **({'pct_chg': item.pct_chg} if has_pct_chg else {})
                                    }
                                    # 同时添加复权价格列，供chart_data_preparer使用
                                    if hasattr(item, 'qfq_open') and item.qfq_open:
                                        record['qfq_open'] = item.qfq_open
                                        record['qfq_high'] = item.qfq_high
                                        record['qfq_low'] = item.qfq_low
                                        record['qfq_close'] = item.qfq_close
                                    else:
                                        record['qfq_open'] = open_price
                                        record['qfq_high'] = high_price
                                        record['qfq_low'] = low_price
                                        record['qfq_close'] = close_price
                                    data_records.append(record)
                            elif adjustment_type == 'hfq':
                                # 后复权
                                data_records = []
                                for item in data:
                                    # 优先使用预计算的复权价格
                                    if hasattr(item, 'hfq_open') and item.hfq_open:
                                        open_price = item.hfq_open
                                        high_price = item.hfq_high
                                        low_price = item.hfq_low
                                        close_price = item.hfq_close
                                    # 其次使用复权因子实时计算
                                    elif has_hfq_factor and item.hfq_factor and item.hfq_factor != 1.0:
                                        open_price = item.open * item.hfq_factor
                                        high_price = item.high * item.hfq_factor
                                        low_price = item.low * item.hfq_factor
                                        close_price = item.close * item.hfq_factor
                                    # 回退到原始价格
                                    else:
                                        open_price = item.open
                                        high_price = item.high
                                        low_price = item.low
                                        close_price = item.close

                                    record = {
                                        'trade_date': item.trade_date,
                                        'open': open_price,
                                        'high': high_price,
                                        'low': low_price,
                                        'close': close_price,
                                        'volume': item.vol,
                                        'amount': item.amount,
                                        **({'pct_chg': item.pct_chg} if has_pct_chg else {})
                                    }
                                    # 同时添加复权价格列，供chart_data_preparer使用
                                    if hasattr(item, 'hfq_open') and item.hfq_open:
                                        record['hfq_open'] = item.hfq_open
                                        record['hfq_high'] = item.hfq_high
                                        record['hfq_low'] = item.hfq_low
                                        record['hfq_close'] = item.hfq_close
                                    else:
                                        record['hfq_open'] = open_price
                                        record['hfq_high'] = high_price
                                        record['hfq_low'] = low_price
                                        record['hfq_close'] = close_price
                                    data_records.append(record)
                            else:
                                # 不复权
                                data_records = [{
                                    'trade_date': item.trade_date,
                                    'open': item.open,
                                    'high': item.high,
                                    'low': item.low,
                                    'close': item.close,
                                    'volume': item.vol,
                                    'amount': item.amount,
                                    **({'pct_chg': item.pct_chg} if has_pct_chg else {})
                                } for item in data]
                            
                            # 使用pl.from_dicts创建Polars DataFrame，效率更高
                            df = pl.from_dicts(data_records)
                            
                            # 转换为标准格式，使用向量化的str.strptime替代apply，效率更高
                            df = df.with_columns(
                                pl.col('trade_date').str.strptime(pl.Datetime, format='%Y%m%d').alias('date')
                            )
                            
                            # 内存优化：转换数据类型
                            optimized_df = MemoryOptimizer.optimize_dataframe(df, enable_sparse=True)
                            return optimized_df
                except (OSError, RuntimeError, ValueError) as db_e:
                    logger.warning(f"从数据库获取{type_name}数据失败: {db_e}")
            
            # 数据库获取失败或无数据，并行尝试从其他数据源获取
            data_sources = []
            
            # 内置数据源
            if self.tdx_handler:
                method_name = type_map['handler_methods']['tdx']
                data_sources.append(('tdx', self.tdx_handler, method_name, {
                    'stock_code': ts_code, 'start_date': start_date, 'end_date': end_date, 'adjust': adjustment_type
                }))
            
            if self.akshare_handler:
                method_name = type_map['handler_methods']['akshare']
                data_sources.append(('akshare', self.akshare_handler, method_name, {
                    'ts_code': ts_code, 'start_date': start_date, 'end_date': end_date, 'freq': freq
                }))
            
            if self.baostock_handler:
                method_name = type_map['handler_methods']['baostock']
                # 转换股票代码格式：600000.SH -> sh.600000
                if '.' in ts_code:
                    parts = ts_code.split('.')
                    if len(parts) == 2:
                        baostock_code = f"{parts[1].lower()}.{parts[0]}"
                    else:
                        baostock_code = ts_code
                else:
                    baostock_code = ts_code
                data_sources.append(('baostock', self.baostock_handler, method_name, {
                    'ts_codes': [baostock_code], 'start_date': start_date, 'end_date': end_date
                }))
            
            # 插件数据源
            for plugin_name, plugin in self.plugin_datasources.items():
                method_name = type_map['handler_methods']['plugin']
                data_sources.append((f'plugin_{plugin_name}', plugin, method_name, {
                    'ts_code': ts_code, 'start_date': start_date, 'end_date': end_date, 'freq': freq
                }))
            
            if not data_sources:
                logger.warning(f"没有可用的数据源来获取{type_name}{ts_code}数据")
                return pl.DataFrame()
            
            # 根据历史响应时间排序数据源（优先级）
            def get_source_priority(source_name):
                """
                获取数据源优先级（响应时间越短优先级越高）
                """
                if source_name not in self.source_priorities or not self.source_priorities[source_name]:
                    return 9999  # 默认优先级
                return sum(self.source_priorities[source_name]) / len(self.source_priorities[source_name])
            
            # 按优先级排序数据源
            data_sources.sort(key=lambda x: get_source_priority(x[0]))
            
            logger.info(f"异步从{len(data_sources)}个数据源获取{type_name}{ts_code}数据（已按响应时间排序）")
            
            # 并行获取数据
            tasks = []
            for source_name, handler, method_name, kwargs in data_sources:
                task = asyncio.create_task(fetch_from_source_async(source_name, handler, method_name, **kwargs))
                tasks.append(task)
            
            # 等待任何一个成功的结果
            done, pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED,
                timeout=30  # 30秒超时
            )
            
            # 取消未完成的任务
            for task in pending:
                task.cancel()
            
            # 检查完成的任务
            for task in done:
                try:
                    success, result = await task
                    if success:
                        logger.info(f"从{data_sources[tasks.index(task)][0]}成功获取{type_name}{ts_code}数据，返回结果")
                        return result
                except Exception as e:
                    logger.warning(f"处理数据源结果时出错: {e}")
            
            # 所有数据源都失败，返回空DataFrame
            logger.warning(f"无法从任何数据源获取{type_name}{ts_code}数据")
            return pl.DataFrame()

        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"获取{type_name}数据失败: {e}")
            raise
    
    async def get_stock_data_async(self, stock_code: str, start_date: str, end_date: str, frequency: str = '1d', adjustment_type: str = 'qfq') -> pl.DataFrame:
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
                if 'trade_date' in df.columns:
                    lazy_df = lazy_df.with_columns(pl.col('trade_date').alias('date'))
                elif 'date' not in df.columns:
                    logger.error("DataFrame中没有日期列")
                    return df
                
                # 转换日期列为datetime类型
                lazy_df = lazy_df.with_columns(pl.col('date').str.strptime(pl.Date, "%Y-%m-%d"))
                
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
                
                # 执行优化的惰性计算
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
    
    async def get_multiple_stocks_data_async(self, stock_codes: List[str], start_date: str, end_date: str, frequency: str = '1d', adjustment_type: str = 'qfq') -> Dict[str, pl.DataFrame]:
        """
        异步并行获取多只股票数据
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 数据频率，默认：1d（日线）
            adjustment_type: 复权类型，qfq=前复权, hfq=后复权, none=不复权
        
        Returns:
            Dict[str, pl.DataFrame]: 股票代码到数据的映射
        """
        tasks = {}
        for stock_code in stock_codes:
            task = self.get_stock_data_async(stock_code, start_date, end_date, frequency, adjustment_type)
            tasks[stock_code] = task
        
        # 并行执行所有任务
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        
        # 处理结果
        stock_data = {}
        for i, stock_code in enumerate(tasks.keys()):
            result = results[i]
            if isinstance(result, Exception):
                logger.error(f"获取{stock_code}数据失败: {result}")
                stock_data[stock_code] = pl.DataFrame()
            else:
                stock_data[stock_code] = result
        
        return stock_data
    
    def close(self):
        """
        关闭线程池
        """
        self.executor.shutdown(wait=False)
        logger.info("AsyncDataManager线程池已关闭")
