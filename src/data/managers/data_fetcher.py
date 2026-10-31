#!/usr/bin/env python3

"""
数据获取模块

负责从多个数据源获取数据，处理数据源优先级和故障转移
"""

import concurrent.futures
import inspect
import multiprocessing
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import date, datetime
from typing import List, Union

import polars as pl
from loguru import logger

from src.data.data_cache import global_data_cache
from src.data.data_source_health import get_global_health_checker
from src.data.managers.source_worker import call_source
from src.utils.date_utils import parse_date
from src.utils.memory_optimizer import MemoryOptimizer
from src.utils.monitoring import global_monitoring_system

# 允许子进程隔离的远程纯网络源（handler 类路径）
# 本地源（TDX）依赖进程内状态，不走子进程；插件源无法保证可重建，也不走。
_PROCESS_ISOLATED_HANDLERS = {
    'src.data.akshare_handler:AkShareHandler',
    'src.data.baostock_handler:BaostockHandler',
}


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
        # 复用的线程池，避免每次请求都创建/销毁；上限 8 线程（本项目长驻池之一）
        self._executor = None
        # 线程池惰性创建的保护锁：_get_executor 可能被多个线程同时进入，
        # 无锁会出现 check-then-act 竞态（建出两个池，其中一个永不 shutdown）
        self._executor_lock = threading.Lock()
        # 源能力缓存：source -> {supports_stock, supports_index, is_local}
        self._source_capabilities = {}
        # 方法签名缓存：避免每次都做 inspect 反射
        self._method_sig_cache = {}
        # 数据源健康检查器（全局单例）：逐源记录成败，并在候选过滤时熔断
        self._health_checker = get_global_health_checker()
        # 熔断冷却窗口（秒）：数据源故障后仍定期放行探测，避免被永久锁死
        self._health_cooldown = 300
        # 远程纯网络源的子进程池（惰性创建，仅灰度开关开启时使用）
        self._process_executor = None
        # 子进程池的创建/终止锁：超时终止与并发取数会同时访问池
        self._process_lock = threading.Lock()

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
                # 兜底理由：数据源可用性探测失败默认视为可用
                except Exception:  # noqa: BLE001
                    # 如果is_available方法调用失败，默认可用
                    available_sources.append(source)
            else:
                # 传统处理器没有is_available方法，默认可用
                available_sources.append(source)
        return available_sources

    def get_stock_data(self, stock_code: str, start_date: str, end_date: str,
                       frequency: str = '1d', adjustment_type: str = 'hfq',
                       as_of: date | None = None) -> pl.DataFrame:
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
        # get/set 必须使用完全一致的参数，否则精确命中失效且会跨复权/as_of 串用
        cache_params = {
            'frequency': frequency,
            'adjustment_type': adjustment_type,
            'as_of': str(as_of) if as_of else None,
        }
        cached_data = global_data_cache.get('stock', stock_code, start_date, end_date,
                                         **cache_params)
        if cached_data is not None:
            logger.info(f"从缓存获取股票数据: {stock_code} {start_date} to {end_date}",
                        **{"symbol": stock_code, "start": start_date, "end": end_date,
                               "frequency": frequency, "adjustment": adjustment_type, "source": "cache"})
            return cached_data

        # 周/月线：先取日线（复用日线缓存），再聚合到目标频率后按目标频率缓存
        if frequency in ('1w', '1m'):
            daily = self.get_stock_data(stock_code, start_date, end_date,
                                        '1d', adjustment_type, as_of)
            if daily.is_empty():
                return daily
            converted = self._convert_frequency(daily, frequency)
            global_data_cache.set(converted, 'stock', stock_code, start_date, end_date,
                                  **cache_params)
            return converted

        # 从数据库获取（_get_data_from_database 内部处理 as_of 截断）
        db_data = self._get_data_from_database('stock', stock_code, start_date, end_date,
                                                frequency, adjustment_type, as_of)
        if not db_data.is_empty():
            db_data = self._apply_adjustment(db_data, adjustment_type, as_of)
            global_data_cache.set(db_data, 'stock', stock_code, start_date, end_date,
                                **cache_params)
            return db_data

        # 从数据源获取（数据源一般不支持 as_of，直接传 None）
        data = self._get_data_from_sources('stock', stock_code, start_date, end_date, frequency, adjustment_type)

        if not data.is_empty():
            data = self._apply_adjustment(data, adjustment_type, as_of=None)
            # 数据源路径的复权按 as_of=None 计算，缓存键也必须是 None，避免错标
            source_cache_params = dict(cache_params)
            source_cache_params['as_of'] = None
            global_data_cache.set(data, 'stock', stock_code, start_date, end_date,
                                **source_cache_params)

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
        cache_params = {'frequency': frequency}
        cached_data = global_data_cache.get('index', index_code, start_date, end_date,
                                         **cache_params)
        if cached_data is not None:
            logger.info(f"从缓存获取指数数据: {index_code} {start_date} to {end_date}",
                        **{"symbol": index_code, "start": start_date, "end": end_date,
                               "frequency": frequency, "source": "cache"})
            return cached_data

        # 周/月线：先取日线（复用日线缓存），再聚合到目标频率后按目标频率缓存
        if frequency in ('1w', '1m'):
            daily = self.get_index_data(index_code, start_date, end_date, '1d')
            if daily.is_empty():
                return daily
            converted = self._convert_frequency(daily, frequency)
            global_data_cache.set(converted, 'index', index_code, start_date, end_date,
                                  **cache_params)
            return converted

        # 从数据库获取
        db_data = self._get_data_from_database('index', index_code, start_date, end_date, frequency)
        if not db_data.is_empty():
            # 存入缓存
            global_data_cache.set(db_data, 'index', index_code, start_date, end_date,
                                **cache_params)
            return db_data

        # 从数据源获取
        data = self._get_data_from_sources('index', index_code, start_date, end_date, frequency)

        # 存入缓存
        if not data.is_empty():
            global_data_cache.set(data, 'index', index_code, start_date, end_date,
                                **cache_params)

        return data

    def _convert_frequency(self, df: pl.DataFrame, frequency: str) -> pl.DataFrame:
        """将日线数据聚合到周线/月线（失败时 DataProcessor 会记录日志并返回原数据）"""
        from src.data.managers.data_processor import DataProcessor

        return DataProcessor(self.config).convert_frequency(df, frequency)

    @staticmethod
    def _parse_date_bound(value: Union[str, date, datetime, None]) -> date | None:
        """将日期字符串/对象统一转换为 date，供数据库查询使用（复用公共工具）"""
        return parse_date(value)

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
                          as_of: date | None = None) -> pl.DataFrame:
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
        result = df.with_columns(exprs)

        # hfq 是回测/计算口径：涨跌停等交易所规则定义在原始价上，除权日
        # 因子跳变会让"复权价 / 前收复权价"的比值失真（误触发/误放行）。
        # 保留 raw_open / raw_close 双价格列：撮合内核用原始价判定涨跌停，
        # 指标与收益仍在复权空间计算。
        if adjustment_type == 'hfq':
            result = result.with_columns([
                df['open'].alias('raw_open'),
                df['close'].alias('raw_close'),
            ])
        return result

    def _get_data_from_database(self, data_type: str, code: str,
                                 start_date: str, end_date: str,
                                 frequency: str,
                                 adjustment_type: str = 'hfq',
                                 as_of: date | None = None) -> pl.DataFrame:
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
                model_class.ts_code == code,
                model_class.trade_date >= (start_bound or start_date),
                model_class.trade_date <= (end_bound or end_date)
            ).order_by(model_class.trade_date)

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
                    from sqlalchemy import select

                    from src.database.models.stock import StockDividend
                    stmt = select(StockDividend).where(
                        StockDividend.ts_code == code,
                        StockDividend.ex_date > as_of
                    ).order_by(StockDividend.ex_date)
                    for div in session.execute(stmt).scalars().all():
                        factor = self._dividend_event_factor(div)
                        if factor is not None and factor != 1.0:
                            rollback_factors.append((factor, div.ex_date))
                # 兜底理由：除权事件查询失败不影响主流程
                except Exception as e:  # noqa: BLE001
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

        # 兜底理由：数据库取数失败降级返回空表
        except Exception as e:  # noqa: BLE001
            logger.warning(f"从数据库获取{data_type}数据失败: {e}")
            return pl.DataFrame()

    @staticmethod
    def _dividend_event_factor(dividend) -> float | None:
        """
        从除权除息事件对象计算单次事件的复权因子。

        公式统一收口到 adj_factor_calculator.calc_dividend_event_factor，
        此处仅做 ORM 对象字段适配。

        返回值 = numerator / denominator，前复权每次乘这个值。
        """
        from src.data.adj_factor_calculator import calc_dividend_event_factor

        return calc_dividend_event_factor(
            prev_close=getattr(dividend, 'prev_close', None),
            cash_div=getattr(dividend, 'cash_div', None),
            share_div=getattr(dividend, 'share_div', None),
            rights_ratio=getattr(dividend, 'rights_issue_ratio', None),
            rights_price=getattr(dividend, 'rights_issue_price', None),
        )

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
        """获取支持该数据类型、当前可用且未处于熔断期的数据源"""
        available = self.get_available_sources()
        key = f'supports_{data_type}'
        candidates = [s for s in available if self._source_capabilities.get(s, {}).get(key, False)]
        healthy = [s for s in candidates if self._is_source_usable(s)]
        # 全部熔断时回退到原始候选：健康检查用于降权而非彻底锁死取数路径
        return healthy or candidates

    def _is_source_usable(self, source) -> bool:
        """数据源是否可用：健康检查通过，或已过熔断冷却期（半开探测）。"""
        name = getattr(source, 'name', source.__class__.__name__)
        if self._health_checker.is_source_available(name):
            return True
        info = self._health_checker.get_health_info(name)
        if not info or not info.last_failure_time:
            return True
        return time.time() - info.last_failure_time > self._health_cooldown

    def _get_executor(self) -> ThreadPoolExecutor:
        """获取（或懒创建）复用的线程池

        双检锁：先用一次廉价判断挡住绝大多数已创建的调用，仅在确实需要创建时
        再持锁复查。这样既保留热路径开销，又避免多线程并发首用时建出两个线程池
        （其中一个会失去引用、线程永不回收）。
        """
        executor = self._executor
        if executor is not None and not getattr(executor, '_shutdown', False):
            return executor

        with self._executor_lock:
            executor = self._executor
            if executor is None or getattr(executor, '_shutdown', False):
                max_workers = self.config.data.max_workers if hasattr(self.config, 'data') else 5
                executor = ThreadPoolExecutor(
                    max_workers=max(2, min(max_workers, 8)),
                    thread_name_prefix='DataFetcher',
                )
                self._executor = executor
            return executor

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
        future.add_done_callback(self._consume_late_result)
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
        # 兜底理由：单数据源取数失败降级为跳过
        except Exception as e:  # noqa: BLE001
            logger.warning(f"从{source_name}获取数据失败: {e}",
                           **{"symbol": code, "source": source_name, "error": str(e)})
            return None

    def _try_remote_sources_parallel(self, sources, data_type, code, start_date, end_date,
                                      frequency, adjustment_type, per_source_timeout=10) -> pl.DataFrame:
        """并行调用多个远程源，命中即取消其他"""
        futures = {}
        type_name = "股票" if data_type == "stock" else "指数"

        for src in sources:
            call = self._build_source_call(src, data_type, code, start_date, end_date,
                                            frequency, adjustment_type)
            if call is None:
                continue
            method_name, kwargs = call
            future, isolated = self._submit_source_call(src, method_name, kwargs)
            futures[future] = (src, isolated, time.time())

        try:
            for future in concurrent.futures.as_completed(futures, timeout=per_source_timeout + 5):
                src, isolated, start_time = futures[future]
                result = self._resolve_source_future(future, src, isolated, start_time)
                if result is not None and not result.is_empty():
                    # 命中：取消其他
                    for f in futures:
                        if f is not future and not f.done():
                            f.cancel()
                    source_name = getattr(src, 'name', src.__class__.__name__)
                    logger.info(f"从{source_name}成功获取{type_name}{code}数据",
                                **{"symbol": code, "source": source_name, "data_type": data_type, "rows": result.height})
                    return result
        except concurrent.futures.TimeoutError:
            # 所有源在 (per_source_timeout + 5) 秒内都没有完成
            logger.warning(f"远程源全部超时（>{per_source_timeout + 5}s）",
                           **{"symbol": code, "data_type": data_type, "timeout": per_source_timeout + 5})
            # 先记账再终止：终止后 future 会变成 broken，无法再判断真实状态
            pending_isolated = [
                (getattr(src, 'name', src.__class__.__name__), start_time)
                for future, (src, isolated, start_time) in futures.items()
                if isolated and not future.done()
            ]
            for source_name, start_time in pending_isolated:
                self._record_isolated_result(
                    source_name, start_time, False,
                    f"timeout>{per_source_timeout + 5}s",
                )
            # 子进程挂死时线程超时只能"不再等待"，必须真正终止子进程并重建池
            if any(isolated for _, isolated, _ in futures.values()):
                self._abort_process_pool()
        finally:
            # 确保取消所有未完成的 future
            for f in futures:
                if not f.done():
                    f.cancel()

        return pl.DataFrame()

    def _resolve_source_future(self, future, src, isolated: bool,
                               start_time: float) -> pl.DataFrame | None:
        """解析单个取数 future，成功返回 DataFrame（可能为空），失败/超时返回 None"""
        source_name = getattr(src, 'name', src.__class__.__name__)
        try:
            if isolated:
                # 子进程返回 (ok, error, df)，健康统计需在主进程记账
                success, error, result = future.result(timeout=0.1)
                self._record_isolated_result(source_name, start_time, success, error)
            else:
                success, result = future.result(timeout=0.1)

            if success and result is not None and not result.is_empty():
                return MemoryOptimizer.optimize_dataframe(result, enable_sparse=True)
            return None
        except concurrent.futures.TimeoutError:
            logger.warning(f"从{source_name}获取数据超时",
                           **{"symbol": source_name})
            return None
        # 兜底理由：单数据源结果处理失败降级为跳过
        except Exception as e:  # noqa: BLE001
            logger.warning(f"处理{source_name}结果时出错: {e}",
                           **{"source": source_name, "error": str(e)})
            if isolated:
                self._record_isolated_result(source_name, start_time, False, str(e))
            return None

    # ------------------------------------------------------------------
    # 子进程隔离（灰度）
    # ------------------------------------------------------------------

    def _process_isolation_enabled(self) -> bool:
        """读取灰度开关：默认关闭，保持原有线程路径"""
        data_cfg = getattr(self.config, 'data', None)
        return bool(getattr(data_cfg, 'use_subprocess_sources', False))

    def _can_isolate(self, source) -> bool:
        """是否允许该源走子进程：开关开启且属于受支持的远程纯网络 handler"""
        if not self._process_isolation_enabled():
            return False
        path = f"{type(source).__module__}:{type(source).__name__}"
        return path in _PROCESS_ISOLATED_HANDLERS

    def _get_process_executor(self) -> ProcessPoolExecutor:
        """获取（或懒创建）常驻子进程池

        使用 spawn 上下文：子进程不继承主进程的线程锁、数据库连接等状态。
        """
        pool = self._process_executor
        if pool is not None and not getattr(pool, '_shutdown', False):
            return pool

        with self._process_lock:
            pool = self._process_executor
            if pool is None or getattr(pool, '_shutdown', False):
                data_cfg = getattr(self.config, 'data', None)
                max_workers = getattr(data_cfg, 'max_workers', 4)
                workers = max(2, min(max_workers, 4))
                pool = ProcessPoolExecutor(
                    max_workers=workers,
                    mp_context=multiprocessing.get_context('spawn'),
                )
                self._process_executor = pool
            return pool

    def _abort_process_pool(self):
        """终止子进程池并丢弃（超时/挂死场景下唯一的真正中断手段）"""
        with self._process_lock:
            pool = self._process_executor
            self._process_executor = None
        if pool is None:
            return

        processes = []
        try:
            processes = list(getattr(pool, '_processes', {}).values())
        # 兜底理由：CPython 私有属性可能变化，拿不到进程句柄时仍要关闭池
        except Exception:  # noqa: BLE001
            processes = []

        for proc in processes:
            try:
                if proc.is_alive():
                    proc.terminate()
            # 兜底理由：单个进程终止失败不阻断其余
            except Exception as e:  # noqa: BLE001
                logger.debug(f"终止子进程失败（忽略）: {e}")
                continue

        try:
            pool.shutdown(wait=False, cancel_futures=True)
        # 兜底理由：旧版本不支持 cancel_futures
        except TypeError:
            pool.shutdown(wait=False)

        for proc in processes:
            try:
                proc.join(timeout=2)
            # 兜底理由：join 失败不影响主流程
            except Exception as e:  # noqa: BLE001
                logger.debug(f"回收子进程失败（忽略）: {e}")
                continue

        logger.warning("远程数据源子进程池因超时被终止并重建")

    def _submit_source_call(self, source, method_name, kwargs):
        """提交单源取数

        Returns:
            (future, isolated)：isolated=True 表示在子进程池中执行
        """
        if self._can_isolate(source):
            pool = self._get_process_executor()
            payload = {
                'handler': f"{type(source).__module__}:{type(source).__name__}",
                'method': method_name,
                'kwargs': kwargs,
                'config': self.config,
            }
            future = pool.submit(call_source, payload)
            future.add_done_callback(self._consume_late_result)
            return future, True

        future = self._get_executor().submit(
            self._fetch_from_source, source, method_name, **kwargs
        )
        future.add_done_callback(self._consume_late_result)
        return future, False

    def _record_isolated_result(self, source_name: str, start_time: float,
                                success: bool, error: str):
        """主进程侧记账：子进程无法直接更新监控与健康检查状态"""
        response_time = time.time() - start_time
        global_monitoring_system.record_data_source_request(source_name, response_time, success)
        # 空结果不算故障，与线程路径保持一致
        if success or error:
            self._health_checker.record_request(source_name, success, response_time, error)

        if source_name not in self.source_priorities:
            self.source_priorities[source_name] = []
        self.source_priorities[source_name].append(response_time)
        if len(self.source_priorities[source_name]) > 10:
            self.source_priorities[source_name] = self.source_priorities[source_name][-10:]

    def _build_source_call(self, source, data_type, code, start_date, end_date,
                            frequency, adjustment_type):
        """构造数据源调用参数

        Returns:
            (method_name, kwargs) 或 None（源不支持该数据类型）
        """
        # 日/周/月线在数据源层统一按日线获取，聚合由 _convert_frequency 负责
        freq = 'daily' if frequency in ('1d', '1w', '1m') else 'minute'

        if data_type == 'stock':
            if hasattr(source, 'get_stock_data'):
                kwargs = {
                    'ts_code': code,
                    'start_date': start_date,
                    'end_date': end_date,
                    'freq': freq,
                }
                adjust_param = self._resolve_adjust_param(source)
                if adjust_param:
                    kwargs[adjust_param] = adjustment_type
                else:
                    source_name = getattr(source, 'name', source.__class__.__name__)
                    logger.warning(
                        f"数据源 {source_name} 的 get_stock_data 不接受复权参数，"
                        f"将按源默认口径返回（可能为未复权价）"
                    )
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

    def _resolve_adjust_param(self, source) -> str | None:
        """
        解析数据源 get_stock_data 接受的复权参数名

        兼容两种命名约定：adjustment_type（标准）与 adjust（历史/第三方）。
        两者都不存在时返回 None，由调用方告警，避免复权参数被静默丢弃。

        Returns:
            str | None: 'adjustment_type' / 'adjust' / None
        """
        # 以类为缓存键：id(source) 会因对象回收被复用，导致不同数据源串用解析结果
        cache_key = ('adjust_param', type(source))
        if cache_key not in self._method_sig_cache:
            try:
                params = inspect.signature(source.get_stock_data).parameters
                if 'adjustment_type' in params:
                    self._method_sig_cache[cache_key] = 'adjustment_type'
                elif 'adjust' in params:
                    self._method_sig_cache[cache_key] = 'adjust'
                else:
                    self._method_sig_cache[cache_key] = None
            except (TypeError, ValueError):
                self._method_sig_cache[cache_key] = None
        return self._method_sig_cache[cache_key]

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
        error_message = ""

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
                    # 兜底理由：内存优化失败退回未优化数据
                    except Exception as exc:  # noqa: BLE001
                        # 内存优化失败不影响主流程，退回未优化的原始 DataFrame
                        logger.debug(f"内存优化失败，使用未优化数据: {exc}")
                return False, None
            else:
                return False, None
        # 兜底理由：多源取数整体兜底返回失败
        except Exception as e:  # noqa: BLE001
            error_message = str(e)
            logger.warning(f"从{source_name}获取数据失败: {e}")
            return False, None
        finally:
            # 记录监控数据
            response_time = time.time() - start_time
            global_monitoring_system.record_data_source_request(source_name, response_time, success)
            # 空结果（该源没有这只标的的数据）不算故障，否则会误熔断正常数据源
            if success or error_message:
                self._health_checker.record_request(source_name, success, response_time, error_message)

            # 更新数据源优先级
            if source_name not in self.source_priorities:
                self.source_priorities[source_name] = []
            self.source_priorities[source_name].append(response_time)
            # 只保留最近10次的响应时间
            if len(self.source_priorities[source_name]) > 10:
                self.source_priorities[source_name] = self.source_priorities[source_name][-10:]

    @staticmethod
    def _consume_late_result(future):
        """消费超时后才完成的 future。

        线程无法被中断，超时只是调用方不再等待；这里读取一次异常，
        避免 "exception was never retrieved" 泄漏，也便于排查慢源。
        """
        if future.cancelled():
            return
        try:
            future.exception()
        # 兜底理由：子进程池被终止后 exception() 会抛 BrokenProcessPool
        except Exception:  # noqa: BLE001
            return

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
