#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据获取模块

负责从多个数据源获取数据，处理数据源优先级和故障转移
"""

from typing import List, Dict, Any, Optional, Union
import polars as pl
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent.futures
from loguru import logger

from src.utils.memory_optimizer import MemoryOptimizer
from src.utils.monitoring import global_monitoring_system
from src.data.data_cache import global_data_cache


class DataFetcher:
    """
    数据获取模块，负责从多个数据源获取数据
    """
    
    def __init__(self, config, db_manager=None):
        """
        初始化数据获取器
        
        Args:
            config: 配置对象
            db_manager: 数据库管理器实例
        """
        self.config = config
        self.db_manager = db_manager
        self.sources = []
        self.source_priorities = {}
    
    def register_source(self, source):
        """
        注册数据源
        
        Args:
            source: 数据源实例
        """
        if source not in self.sources:
            self.sources.append(source)
            # 获取数据源名称，兼容传统处理器和插件数据源
            source_name = getattr(source, 'name', source.__class__.__name__)
            logger.info(f"注册数据源: {source_name}")
    
    def get_sources(self) -> List:
        """
        获取所有注册的数据源
        
        Returns:
            List: 数据源列表
        """
        return self.sources
    
    def get_available_sources(self) -> List:
        """
        获取所有可用的数据源
        
        Returns:
            List: 可用数据源列表
        """
        # 兼容传统处理器和插件数据源，默认所有数据源都可用
        available_sources = []
        for source in self.sources:
            # 检查是否有is_available方法
            if hasattr(source, 'is_available'):
                try:
                    if source.is_available():
                        available_sources.append(source)
                except Exception:
                    # 如果is_available方法调用失败，默认可用
                    available_sources.append(source)
            else:
                # 传统处理器没有is_available方法，默认可用
                available_sources.append(source)
        return available_sources
    
    def get_stock_data(self, stock_code: str, start_date: str, end_date: str, frequency: str = '1d', adjustment_type: str = 'qfq') -> pl.DataFrame:
        """
        获取股票数据
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            frequency: 数据频率
            adjustment_type: 复权类型
            
        Returns:
            pl.DataFrame: 股票数据
        """
        # 尝试从缓存获取
        cached_data = global_data_cache.get('stock', stock_code, start_date, end_date, 
                                         frequency=frequency, adjustment_type=adjustment_type)
        if cached_data is not None:
            logger.info(f"从缓存获取股票数据: {stock_code} {start_date} to {end_date}")
            return cached_data
        
        # 从数据库获取
        db_data = self._get_data_from_database('stock', stock_code, start_date, end_date, frequency, adjustment_type)
        if not db_data.is_empty():
            # 存入缓存
            global_data_cache.set(db_data, 'stock', stock_code, start_date, end_date, 
                                frequency=frequency, adjustment_type=adjustment_type)
            return db_data
        
        # 从数据源获取
        data = self._get_data_from_sources('stock', stock_code, start_date, end_date, frequency, adjustment_type)
        
        # 存入缓存
        if not data.is_empty():
            global_data_cache.set(data, 'stock', stock_code, start_date, end_date, 
                                frequency=frequency, adjustment_type=adjustment_type)
        
        return data
    
    def get_index_data(self, index_code: str, start_date: str, end_date: str, frequency: str = '1d') -> pl.DataFrame:
        """
        获取指数数据
        
        Args:
            index_code: 指数代码
            start_date: 开始日期
            end_date: 结束日期
            frequency: 数据频率
            
        Returns:
            pl.DataFrame: 指数数据
        """
        # 尝试从缓存获取
        cached_data = global_data_cache.get('index', index_code, start_date, end_date, 
                                         frequency=frequency)
        if cached_data is not None:
            logger.info(f"从缓存获取指数数据: {index_code} {start_date} to {end_date}")
            return cached_data
        
        # 从数据库获取
        db_data = self._get_data_from_database('index', index_code, start_date, end_date, frequency)
        if not db_data.is_empty():
            # 存入缓存
            global_data_cache.set(db_data, 'index', index_code, start_date, end_date, 
                                frequency=frequency)
            return db_data
        
        # 从数据源获取
        data = self._get_data_from_sources('index', index_code, start_date, end_date, frequency)
        
        # 存入缓存
        if not data.is_empty():
            global_data_cache.set(data, 'index', index_code, start_date, end_date, 
                                frequency=frequency)
        
        return data
    
    def _get_data_from_database(self, data_type: str, code: str, start_date: str, end_date: str, frequency: str, adjustment_type: str = 'qfq') -> pl.DataFrame:
        """
        从数据库获取数据
        
        Args:
            data_type: 数据类型
            code: 代码
            start_date: 开始日期
            end_date: 结束日期
            frequency: 数据频率
            adjustment_type: 复权类型
            
        Returns:
            pl.DataFrame: 数据
        """
        if not self.db_manager or not self.db_manager.is_connected():
            return pl.DataFrame()
        
        try:
            # 动态导入模型
            module_path = f"src.database.models.{data_type}"
            module = __import__(module_path, fromlist=[f"{data_type.capitalize()}Daily"])
            model_class = getattr(module, f"{data_type.capitalize()}Daily")
            
            session = self.db_manager.get_session()
            if not session:
                return pl.DataFrame()
            
            query = session.query(model_class).filter(
                getattr(model_class, 'ts_code') == code,
                getattr(model_class, 'trade_date') >= start_date.replace('-', ''),
                getattr(model_class, 'trade_date') <= end_date.replace('-', '')
            ).order_by(getattr(model_class, 'trade_date'))
            
            data = query.all()
            if not data:
                return pl.DataFrame()
            
            # 构建数据记录
            data_records = []
            for item in data:
                record = {
                    'trade_date': item.trade_date,
                    'open': item.open,
                    'high': item.high,
                    'low': item.low,
                    'close': item.close,
                    'volume': item.vol,
                    'amount': item.amount
                }
                
                # 添加复权价格
                if adjustment_type == 'qfq' and hasattr(item, 'qfq_close'):
                    record['qfq_open'] = item.qfq_open
                    record['qfq_high'] = item.qfq_high
                    record['qfq_low'] = item.qfq_low
                    record['qfq_close'] = item.qfq_close
                elif adjustment_type == 'hfq' and hasattr(item, 'hfq_close'):
                    record['hfq_open'] = item.hfq_open
                    record['hfq_high'] = item.hfq_high
                    record['hfq_low'] = item.hfq_low
                    record['hfq_close'] = item.hfq_close
                
                data_records.append(record)
            
            # 创建DataFrame
            df = pl.from_dicts(data_records)
            
            # 转换日期格式
            if 'trade_date' in df.columns:
                df = df.with_columns(
                    pl.col('trade_date').str.strptime(pl.Datetime, format='%Y%m%d').alias('date')
                )
            
            # 内存优化
            optimized_df = MemoryOptimizer.optimize_dataframe(df, enable_sparse=True)
            return optimized_df
            
        except Exception as e:
            logger.warning(f"从数据库获取{data_type}数据失败: {e}")
            return pl.DataFrame()
    
    def _get_data_from_sources(self, data_type: str, code: str, start_date: str, end_date: str, frequency: str, adjustment_type: str = 'qfq') -> pl.DataFrame:
        """
        从多个数据源获取数据
        
        Args:
            data_type: 数据类型
            code: 代码
            start_date: 开始日期
            end_date: 结束日期
            frequency: 数据频率
            adjustment_type: 复权类型
            
        Returns:
            pl.DataFrame: 数据
        """
        available_sources = self.get_available_sources()
        if not available_sources:
            logger.warning(f"没有可用的数据源来获取{data_type}数据")
            return pl.DataFrame()
        
        # 按优先级排序数据源
        available_sources.sort(key=lambda x: self._get_source_priority(getattr(x, 'name', x.__class__.__name__)))
        
        # 并行获取数据
        max_workers = min(len(available_sources), 5)
        type_name = "股票" if data_type == "stock" else "指数"
        
        logger.info(f"并行从{len(available_sources)}个数据源获取{type_name}{code}数据")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for source in available_sources:
                # 检查数据源类型，使用不同的方法和参数
                source_name = getattr(source, 'name', source.__class__.__name__)
                
                if data_type == 'stock':
                    # 检查是否有get_stock_data方法
                    if hasattr(source, 'get_stock_data'):
                        # 插件数据源
                        future = executor.submit(
                            self._fetch_from_source,
                            source, 'get_stock_data',
                            ts_code=code, start_date=start_date, end_date=end_date, 
                            freq='daily' if frequency == '1d' else 'minute',
                            adjustment_type=adjustment_type
                        )
                    elif hasattr(source, 'get_kline_data'):
                        # 传统TdxHandler
                        future = executor.submit(
                            self._fetch_from_source,
                            source, 'get_kline_data',
                            stock_code=code, start_date=start_date, end_date=end_date, 
                            adjust=adjustment_type
                        )
                    else:
                        logger.warning(f"数据源{source_name}不支持股票数据获取")
                        continue
                else:
                    # 检查是否有get_index_data方法
                    if hasattr(source, 'get_index_data'):
                        # 插件数据源
                        future = executor.submit(
                            self._fetch_from_source,
                            source, 'get_index_data',
                            ts_code=code, start_date=start_date, end_date=end_date, 
                            freq='daily' if frequency == '1d' else 'minute'
                        )
                    elif hasattr(source, 'get_kline_data'):
                        # 传统TdxHandler
                        future = executor.submit(
                            self._fetch_from_source,
                            source, 'get_kline_data',
                            stock_code=code, start_date=start_date, end_date=end_date, 
                            adjust='none'
                        )
                    else:
                        logger.warning(f"数据源{source_name}不支持指数数据获取")
                        continue
                futures[future] = source
            
            # 等待第一个成功的结果
            for future in as_completed(futures, timeout=30):
                source = futures[future]
                source_name = getattr(source, 'name', source.__class__.__name__)
                try:
                    success, result = future.result()
                    if success and result is not None and not result.is_empty():
                        logger.info(f"从{source_name}成功获取{type_name}{code}数据")
                        return result
                except concurrent.futures.TimeoutError:
                    logger.warning(f"从{source_name}获取数据超时")
                except Exception as e:
                    logger.warning(f"处理{source_name}结果时出错: {e}")
        
        # 所有数据源都失败
        logger.warning(f"无法从任何数据源获取{type_name}{code}数据")
        return pl.DataFrame()
    
    def _fetch_from_source(self, source, method_name, **kwargs):
        """
        从单个数据源获取数据
        
        Args:
            source: 数据源
            method_name: 方法名
            **kwargs: 方法参数
            
        Returns:
            tuple: (成功标志, 数据)
        """
        start_time = time.time()
        success = False
        
        # 获取数据源名称，兼容传统处理器和插件数据源
        source_name = getattr(source, 'name', source.__class__.__name__)
        
        try:
            logger.info(f"从{source_name}获取数据")
            result = getattr(source, method_name)(**kwargs)
            
            if result is not None:
                # 检查是否为DataFrame
                if hasattr(result, 'is_empty'):
                    if not result.is_empty():
                        # 内存优化
                        optimized_result = MemoryOptimizer.optimize_dataframe(result, enable_sparse=True)
                        success = True
                        return True, optimized_result
                elif hasattr(result, 'collect'):
                    # 处理LazyFrame
                    collected_result = result.collect()
                    if not collected_result.is_empty():
                        # 内存优化
                        optimized_result = MemoryOptimizer.optimize_dataframe(collected_result, enable_sparse=True)
                        success = True
                        return True, optimized_result
                else:
                    # 处理其他类型的结果
                    try:
                        df = pl.DataFrame(result)
                        if not df.is_empty():
                            # 内存优化
                            optimized_result = MemoryOptimizer.optimize_dataframe(df, enable_sparse=True)
                            success = True
                            return True, optimized_result
                    except Exception:
                        pass
                return False, None
            else:
                return False, None
        except Exception as e:
            logger.warning(f"从{source_name}获取数据失败: {e}")
            return False, None
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
    
    def _get_source_priority(self, source_name: str) -> float:
        """
        获取数据源优先级
        
        Args:
            source_name: 数据源名称
            
        Returns:
            float: 优先级值，越小优先级越高
        """
        if source_name not in self.source_priorities or not self.source_priorities[source_name]:
            return 9999  # 默认优先级
        return sum(self.source_priorities[source_name]) / len(self.source_priorities[source_name])
