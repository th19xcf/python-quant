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
        # 复用的线程池，避免每次请求都创建/销毁
        self._executor = None
        # 源能力缓存：source -> {supports_stock, supports_index, is_local}
        self._source_capabilities = {}
        # 方法签名缓存：避免每次都做 inspect 反射
        self._method_sig_cache = {}
    
    def register_source(self, source):
        """
        注册数据源

        Args:
            source: 数据源实例
        """
        if source in self.sources:
            return

        self.sources.append(source)
        source_name = getattr(source, 'name', source.__class__.__name__)

        # 一次性识别并缓存源能力
        supports_stock = hasattr(source, 'get_stock_data') or hasattr(source, 'get_kline_data')
        supports_index = hasattr(source, 'get_index_data') or hasattr(source, 'get_kline_data')
        is_local = bool(getattr(source, 'is_local', False))
        self._source_capabilities[source] = {
            'supports_stock': supports_stock,
            'supports_index': supports_index,
            'is_local': is_local,
        }

        logger.info(
            f"注册数据源: {source_name} "
            f"(stock={supports_stock}, index={supports_index}, local={is_local})"
        )
    
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
        从多个数据源获取数据（优化版）

        策略：
        1. 优先尝试本地源（TDX）：同步调用，5s 超时，成功即返回
        2. 本地失败后，并行尝试远程源：每源 10s 超时，第一个成功就取消其他

        Args:
            data_type: 数据类型 ('stock' | 'index')
            code: 代码
            start_date: 开始日期
            end_date: 结束日期
            frequency: 数据频率
            adjustment_type: 复权类型

        Returns:
            pl.DataFrame: 数据
        """
        type_name = "股票" if data_type == "stock" else "指数"

        # 按能力 + 可用性过滤数据源
        candidates = self._get_candidate_sources(data_type)
        if not candidates:
            logger.warning(f"没有可用的数据源来获取{data_type}数据")
            return pl.DataFrame()

        # 分离本地与远程
        local_sources = [s for s in candidates if self._source_capabilities[s]['is_local']]
        remote_sources = [s for s in candidates if not self._source_capabilities[s]['is_local']]

        # 1. 本地优先（同步尝试，命中即返回）
        if local_sources:
            local_sources.sort(key=lambda s: self._get_source_priority(getattr(s, 'name', s.__class__.__name__)))
            for src in local_sources:
                data = self._try_source_with_timeout(
                    src, data_type, code, start_date, end_date, frequency, adjustment_type, timeout=5
                )
                if data is not None and not data.is_empty():
                    source_name = getattr(src, 'name', src.__class__.__name__)
                    logger.info(f"本地源 {source_name} 命中{type_name}{code}")
                    return data

        # 2. 远程并行（每源 10s 超时，命中即取消）
        if remote_sources:
            remote_sources.sort(key=lambda s: self._get_source_priority(getattr(s, 'name', s.__class__.__name__)))
            logger.info(f"并行从{len(remote_sources)}个远程源获取{type_name}{code}数据")
            return self._try_remote_sources_parallel(
                remote_sources, data_type, code, start_date, end_date, frequency, adjustment_type,
                per_source_timeout=10,
            )

        logger.warning(f"无法从任何数据源获取{type_name}{code}数据")
        return pl.DataFrame()

    def _get_candidate_sources(self, data_type: str) -> List:
        """获取支持该数据类型且当前可用的数据源"""
        available = self.get_available_sources()
        key = f'supports_{data_type}'
        return [s for s in available if self._source_capabilities.get(s, {}).get(key, False)]

    def _get_executor(self) -> ThreadPoolExecutor:
        """获取（或懒创建）复用的线程池"""
        if self._executor is None or getattr(self._executor, '_shutdown', False):
            max_workers = self.config.data.max_workers if hasattr(self.config, 'data') else 5
            self._executor = ThreadPoolExecutor(
                max_workers=max(2, min(max_workers, 8)),
                thread_name_prefix='DataFetcher',
            )
        return self._executor

    def _try_source_with_timeout(self, source, data_type, code, start_date, end_date,
                                  frequency, adjustment_type, timeout):
        """调用单个源，带超时。返回 DataFrame 或 None"""
        call = self._build_source_call(source, data_type, code, start_date, end_date,
                                        frequency, adjustment_type)
        if call is None:
            return None
        method_name, kwargs = call
        source_name = getattr(source, 'name', source.__class__.__name__)

        executor = self._get_executor()
        future = executor.submit(self._fetch_from_source, source, method_name, **kwargs)
        try:
            success, result = future.result(timeout=timeout)
            if success and result is not None and not result.is_empty():
                logger.info(f"从{source_name}成功获取数据")
                return result
            return None
        except concurrent.futures.TimeoutError:
            logger.warning(f"从{source_name}获取数据超时（{timeout}s）")
            future.cancel()
            return None
        except Exception as e:
            logger.warning(f"从{source_name}获取数据失败: {e}")
            return None

    def _try_remote_sources_parallel(self, sources, data_type, code, start_date, end_date,
                                      frequency, adjustment_type, per_source_timeout=10) -> pl.DataFrame:
        """并行调用多个远程源，命中即取消其他"""
        executor = self._get_executor()
        futures = {}
        type_name = "股票" if data_type == "stock" else "指数"

        for src in sources:
            call = self._build_source_call(src, data_type, code, start_date, end_date,
                                            frequency, adjustment_type)
            if call is None:
                continue
            method_name, kwargs = call
            f = executor.submit(self._fetch_from_source, src, method_name, **kwargs)
            futures[f] = (src, method_name)

        try:
            for future in concurrent.futures.as_completed(futures, timeout=per_source_timeout + 5):
                src, _ = futures[future]
                source_name = getattr(src, 'name', src.__class__.__name__)
                try:
                    success, result = future.result(timeout=0.1)
                    if success and result is not None and not result.is_empty():
                        # 命中：取消其他
                        for f in futures:
                            if f is not future and not f.done():
                                f.cancel()
                        logger.info(f"从{source_name}成功获取{type_name}{code}数据")
                        return result
                except concurrent.futures.TimeoutError:
                    logger.warning(f"从{source_name}获取数据超时")
                except Exception as e:
                    logger.warning(f"处理{source_name}结果时出错: {e}")
        except concurrent.futures.TimeoutError:
            # 所有源在 (per_source_timeout + 5) 秒内都没有完成
            logger.warning(f"远程源全部超时（>{per_source_timeout + 5}s）")
        finally:
            # 确保取消所有未完成的 future
            for f in futures:
                if not f.done():
                    f.cancel()

        return pl.DataFrame()

    def _build_source_call(self, source, data_type, code, start_date, end_date,
                            frequency, adjustment_type):
        """构造数据源调用参数

        Returns:
            (method_name, kwargs) 或 None（源不支持该数据类型）
        """
        freq = 'daily' if frequency == '1d' else 'minute'

        if data_type == 'stock':
            if hasattr(source, 'get_stock_data'):
                kwargs = {
                    'ts_code': code,
                    'start_date': start_date,
                    'end_date': end_date,
                    'freq': freq,
                }
                # 用缓存避免每次都做 inspect
                cache_key = ('adjust', id(source))
                if cache_key not in self._method_sig_cache:
                    try:
                        import inspect
                        params = inspect.signature(source.get_stock_data).parameters
                        self._method_sig_cache[cache_key] = 'adjustment_type' in params
                    except Exception:
                        self._method_sig_cache[cache_key] = False
                if self._method_sig_cache[cache_key]:
                    kwargs['adjustment_type'] = adjustment_type
                return 'get_stock_data', kwargs
            if hasattr(source, 'get_kline_data'):
                return 'get_kline_data', {
                    'stock_code': code,
                    'start_date': start_date,
                    'end_date': end_date,
                    'adjust': adjustment_type,
                }
        else:  # 'index'
            if hasattr(source, 'get_index_data'):
                return 'get_index_data', {
                    'ts_code': code,
                    'start_date': start_date,
                    'end_date': end_date,
                    'freq': freq,
                }
            if hasattr(source, 'get_kline_data'):
                return 'get_kline_data', {
                    'stock_code': code,
                    'start_date': start_date,
                    'end_date': end_date,
                    'adjust': 'none',
                }

        # 源不支持该数据类型
        source_name = getattr(source, 'name', source.__class__.__name__)
        logger.debug(f"数据源 {source_name} 不支持 {data_type} 数据获取")
        return None
    
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
