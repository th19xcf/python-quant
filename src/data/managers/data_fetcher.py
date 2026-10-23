#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据获取模块

负责从多个数据源获取数据，处理数据源优先级和故障转移
"""

from typing import List, Dict, Any, Optional, Union
import polars as pl
import time
from datetime import date, datetime
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
    
    def get_stock_data(self, stock_code: str, start_date: str, end_date: str,
                       frequency: str = '1d', adjustment_type: str = 'hfq',
                       as_of: Optional[date] = None) -> pl.DataFrame:
        """
        获取股票数据

        Args:
            stock_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            frequency: 数据频率
            adjustment_type: 复权类型（回测默认 hfq 即后复权，PIT 安全）
            as_of: 观察基准日。仅 qfq 场景使用——截断 ex_date > as_of 的
                   除权事件，保证观察者在 as_of 时点看到的
                   [start, as_of] 段前复权价与今天完全一致。
                   hfq 天然 PIT 安全，不需要 as_of。

        Returns:
            pl.DataFrame: 股票数据
        """
        # 尝试从缓存获取
        cache_extra = {'as_of': str(as_of) if as_of else None}
        cached_data = global_data_cache.get('stock', stock_code, start_date, end_date,
                                         frequency=frequency, adjustment_type=adjustment_type,
                                         **cache_extra)
        if cached_data is not None:
            logger.info(f"从缓存获取股票数据: {stock_code} {start_date} to {end_date}",
                        **{"symbol": stock_code, "start": start_date, "end": end_date,
                               "frequency": frequency, "adjustment": adjustment_type, "source": "cache"})
            return cached_data

        # 从数据库获取（_get_data_from_database 内部处理 as_of 截断）
        db_data = self._get_data_from_database('stock', stock_code, start_date, end_date,
                                                frequency, adjustment_type, as_of)
        if not db_data.is_empty():
            db_data = self._apply_adjustment(db_data, adjustment_type, as_of)
            global_data_cache.set(db_data, 'stock', stock_code, start_date, end_date,
                                frequency=frequency, adjustment_type=adjustment_type)
            return db_data

        # 从数据源获取（数据源一般不支持 as_of，直接传 None）
        data = self._get_data_from_sources('stock', stock_code, start_date, end_date, frequency, adjustment_type)

        if not data.is_empty():
            data = self._apply_adjustment(data, adjustment_type, as_of=None)
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
            logger.info(f"从缓存获取指数数据: {index_code} {start_date} to {end_date}",
                        **{"symbol": index_code, "start": start_date, "end": end_date,
                               "frequency": frequency, "source": "cache"})
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
    
    @staticmethod
    def _parse_date_bound(value: Union[str, date, datetime, None]) -> Optional[date]:
        """将日期字符串/对象统一转换为 date，供数据库查询使用"""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        for fmt in ('%Y-%m-%d', '%Y%m%d'):
            try:
                return datetime.strptime(str(value), fmt).date()
            except ValueError:
                continue
        return None

    @staticmethod
    def _normalize_trade_date(df: pl.DataFrame) -> pl.DataFrame:
        """将数据库的 trade_date 统一转换为 date 列，兼容 Date/Datetime/字符串"""
        if 'trade_date' not in df.columns:
            return df

        dtype = df.schema['trade_date']
        if dtype == pl.Date:
            # 统一为 Datetime，保持与原有返回结构一致
            return df.with_columns(pl.col('trade_date').cast(pl.Datetime).alias('date'))
        if isinstance(dtype, pl.Datetime):
            return df.with_columns(pl.col('trade_date').alias('date'))

        return df.with_columns(
            pl.col('trade_date').cast(pl.Utf8)
            .str.strptime(pl.Datetime, format='%Y%m%d', strict=False)
            .alias('date')
        )

    @staticmethod
    def _apply_adjustment(df: pl.DataFrame, adjustment_type: str,
                          as_of: Optional[date] = None) -> pl.DataFrame:
        """
        在数据出口应用复权价，使 open/high/low/close 即为请求的复权类型。

        统一走因子列（qfq_factor / hfq_factor）实时算——DB 层预计算的
        qfq_open/hfq_open 等 8 列已整体废弃删除。
        因子列不存在时保持原始价格。

        Args:
            df: 含原始 open/high/low/close +（可选）qfq_factor/hfq_factor 的 DataFrame
            adjustment_type: 'none' | 'qfq' | 'hfq'
            as_of: 观察基准日，仅 qfq 场景使用——截断 ex_date > as_of 的
                   除权事件，保证 [start, as_of] 段的前复权价与"今天"
                   观察到的同段价格一致（PIT 安全校验）。
                   hfq 本身 PIT 安全，不需要 as_of。
        """
        prefix = {'qfq': 'qfq_', 'hfq': 'hfq_'}.get(adjustment_type)
        if prefix is None:
            return df

        factor_col = f'{prefix}factor'   # qfq_factor / hfq_factor
        price_cols = ['open', 'high', 'low', 'close']

        if factor_col not in df.columns:
            logger.debug(f"数据未提供 {factor_col} 列，保持原始价格")
            return df

        # === 用因子列实时算复权价 ===
        factor_expr = pl.col(factor_col)
        exprs = []
        for col in price_cols:
            adjusted = (pl.col(col) * factor_expr).alias(col)
            exprs.append(adjusted)
        return df.with_columns(exprs)

    def _get_data_from_database(self, data_type: str, code: str,
                                 start_date: str, end_date: str,
                                 frequency: str,
                                 adjustment_type: str = 'hfq',
                                 as_of: Optional[date] = None) -> pl.DataFrame:
        """
        从数据库获取原始行情 + 复权因子，as_of 时截断 qfq_factor。

        DB 返回的 qfq_factor 是全量因子（含所有已知除权事件）。
        当 as_of 非空且 adjustment_type='qfq' 时，把 as_of 之后的除权
        事件从 qfq_factor 中回滚——保证观察者在 as_of 时点看到的
        [start, as_of] 段前复权价，与今天看到的同段价格完全一致。
        hfq_factor 永远不需要 as_of（后复权只影响 ex_date 之后的行，
        历史行天然 PIT 安全）。

        Returns:
            pl.DataFrame: 含原始 open/high/low/close + qfq_factor/hfq_factor
                          （as_of 截断后的 qfq_factor）
        """
        if not self.db_manager or not self.db_manager.is_connected():
            return pl.DataFrame()

        try:
            module_path = f"src.database.models.{data_type}"
            module = __import__(module_path, fromlist=[f"{data_type.capitalize()}Daily"])
            model_class = getattr(module, f"{data_type.capitalize()}Daily")

            session = self.db_manager.get_session()
            if not session:
                return pl.DataFrame()

            start_bound = self._parse_date_bound(start_date)
            end_bound = self._parse_date_bound(end_date)

            query = session.query(model_class).filter(
                getattr(model_class, 'ts_code') == code,
                getattr(model_class, 'trade_date') >= (start_bound or start_date),
                getattr(model_class, 'trade_date') <= (end_bound or end_date)
            ).order_by(getattr(model_class, 'trade_date'))

            data = query.all()
            if not data:
                return pl.DataFrame()

            has_qfq_factor = hasattr(data[0], 'qfq_factor')
            has_hfq_factor = hasattr(data[0], 'hfq_factor')

            # --- as_of 截断：查除权事件 ---
            # qfq_factor 回滚公式：截断后 = 原 qfq_factor / ∏{ex_date > max(trade_date, as_of)} event_factor
            # （ex_date > as_of 的事件是观察者还不知道的，应从因子中移除）
            rollback_factors: list = []   # [(event_factor, ex_date), ...]
            if as_of is not None and adjustment_type == 'qfq':
                try:
                    from src.database.models.stock import StockDividend
                    from sqlalchemy import select
                    stmt = select(StockDividend).where(
                        StockDividend.ts_code == code,
                        StockDividend.ex_date > as_of
                    ).order_by(StockDividend.ex_date)
                    for div in session.execute(stmt).scalars().all():
                        factor = self._dividend_event_factor(div)
                        if factor is not None and factor != 1.0:
                            rollback_factors.append((factor, div.ex_date))
                except Exception as e:
                    logger.debug(f"as_of 截断时查除权事件失败（不影响主流程）: {e}")

            data_records = []
            for item in data:
                record = {
                    'trade_date': item.trade_date,
                    'open': item.open,
                    'high': item.high,
                    'low': item.low,
                    'close': item.close,
                    'volume': item.vol,
                    'amount': item.amount,
                }

                # --- 始终带 factor 列 ---
                if has_qfq_factor:
                    raw_qfq = getattr(item, 'qfq_factor', 1.0) or 1.0
                    # as_of 截断：除以所有 ex_date > max(trade_date, as_of) 的事件 factor
                    if rollback_factors:
                        td = item.trade_date
                        for evt_factor, evt_ex_date in rollback_factors:
                            if evt_ex_date is not None and evt_ex_date > td:
                                # 该事件影响 trade_date 行，且观察者还不知道它
                                raw_qfq /= evt_factor
                    record['qfq_factor'] = raw_qfq

                if has_hfq_factor:
                    record['hfq_factor'] = getattr(item, 'hfq_factor', 1.0) or 1.0

                data_records.append(record)

            df = pl.from_dicts(data_records)
            df = self._normalize_trade_date(df)
            optimized_df = MemoryOptimizer.optimize_dataframe(df, enable_sparse=True)
            return optimized_df

        except Exception as e:
            logger.warning(f"从数据库获取{data_type}数据失败: {e}")
            return pl.DataFrame()

    @staticmethod
    def _dividend_event_factor(dividend) -> Optional[float]:
        """
        从除权除息事件对象计算单次事件的复权因子（参考 adj_factor_calculator._calculate_factors）。

        返回值 = numerator / denominator，前复权每次乘这个值。
        """
        share_div = getattr(dividend, 'share_div', None) or 0.0
        cash_div = getattr(dividend, 'cash_div', None) or 0.0
        rights_ratio = getattr(dividend, 'rights_issue_ratio', None) or 0.0
        rights_price = getattr(dividend, 'rights_issue_price', None) or 0.0
        prev_close = getattr(dividend, 'prev_close', None)

        if prev_close is None or prev_close == 0:
            return None

        numerator = prev_close - cash_div + rights_price * rights_ratio
        denominator = prev_close * (1 + share_div + rights_ratio)

        if denominator == 0:
            return None
        factor = numerator / denominator
        if factor <= 0 or factor > 10:
            return None
        return factor
    
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
                    logger.info(f"本地源 {source_name} 命中{type_name}{code}",
                                **{"symbol": code, "source": source_name, "data_type": data_type})
                    return data

        # 2. 远程并行（每源 10s 超时，命中即取消）
        if remote_sources:
            remote_sources.sort(key=lambda s: self._get_source_priority(getattr(s, 'name', s.__class__.__name__)))
            logger.info(f"并行从{len(remote_sources)}个远程源获取{type_name}{code}数据",
                        **{"symbol": code, "data_type": data_type, "remote_sources": len(remote_sources)})
            return self._try_remote_sources_parallel(
                remote_sources, data_type, code, start_date, end_date, frequency, adjustment_type,
                per_source_timeout=10,
            )

        logger.warning(f"无法从任何数据源获取{type_name}{code}数据",
                       **{"symbol": code, "data_type": data_type})
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
                logger.info(f"从{source_name}成功获取数据",
                            **{"symbol": code, "source": source_name, "rows": result.height})
                return result
            return None
        except concurrent.futures.TimeoutError:
            logger.warning(f"从{source_name}获取数据超时（{timeout}s）",
                           **{"symbol": code, "source": source_name, "timeout": timeout})
            future.cancel()
            return None
        except Exception as e:
            logger.warning(f"从{source_name}获取数据失败: {e}",
                           **{"symbol": code, "source": source_name, "error": str(e)})
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
                        logger.info(f"从{source_name}成功获取{type_name}{code}数据",
                                    **{"symbol": code, "source": source_name, "data_type": data_type, "rows": result.height})
                        return result
                except concurrent.futures.TimeoutError:
                    logger.warning(f"从{source_name}获取数据超时",
                                   **{"symbol": code, "source": source_name})
                except Exception as e:
                    logger.warning(f"处理{source_name}结果时出错: {e}",
                                   **{"symbol": code, "source": source_name, "error": str(e)})
        except concurrent.futures.TimeoutError:
            # 所有源在 (per_source_timeout + 5) 秒内都没有完成
            logger.warning(f"远程源全部超时（>{per_source_timeout + 5}s）",
                           **{"symbol": code, "data_type": data_type, "timeout": per_source_timeout + 5})
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
