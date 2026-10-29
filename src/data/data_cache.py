#!/usr/bin/env python3

"""
数据缓存模块，提供高效的数据读取结果缓存机制
"""

import hashlib
import threading
import time
from datetime import date, datetime
from typing import Any, Dict, TypeVar

import polars as pl
from loguru import logger

# 定义缓存键和值的类型变量
K = TypeVar('K')
V = TypeVar('V')

# 非语义参数：只影响存放策略，不影响数据内容，不参与命中比对
_NON_SEMANTIC_PARAMS = frozenset({'ttl'})


def _semantic_params(params: Dict[str, Any] | None) -> Dict[str, Any]:
    """提取参与语义隔离的参数（如 frequency / adjustment_type / as_of）"""
    if not params:
        return {}
    return {k: v for k, v in params.items() if k not in _NON_SEMANTIC_PARAMS}


def _parse_date_bound(value) -> date | None:
    """将 YYYY-MM-DD / YYYYMMDD 解析为 date，供日期列比较使用"""
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(str(value), fmt).date()
        except (TypeError, ValueError):
            continue
    return None


class DataCacheEntry[V]:
    """
    缓存条目类，用于存储缓存值及其元数据
    """

    def __init__(self, value: V, expire_time: float | None = None,
                 data_type: str = '', code: str = '',
                 start_date: str = '', end_date: str = '',
                 params: Dict[str, Any] | None = None):
        """
        初始化缓存条目

        Args:
            value: 缓存值
            expire_time: 过期时间（时间戳），None表示永不过期
            data_type: 数据类型
            code: 代码
            start_date: 开始日期
            end_date: 结束日期
            params: 其他参数
        """
        self.value = value
        self.expire_time = expire_time
        self.access_time = time.time()  # 上次访问时间
        self.hit_count = 1  # 命中次数
        self.data_type = data_type
        self.code = code
        self.start_date = start_date
        self.end_date = end_date
        self.params = params or {}

    def is_expired(self) -> bool:
        """
        检查缓存是否过期

        Returns:
            bool: 是否过期
        """
        if self.expire_time is None:
            return False
        return time.time() > self.expire_time

    def update_access(self):
        """
        更新访问信息
        """
        self.access_time = time.time()
        self.hit_count += 1

    def contains_date_range(self, start_date: str, end_date: str) -> bool:
        """
        检查当前缓存是否包含指定的日期范围

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            bool: 是否包含
        """
        try:
            cache_start = datetime.strptime(self.start_date, '%Y-%m-%d')
            cache_end = datetime.strptime(self.end_date, '%Y-%m-%d')
            query_start = datetime.strptime(start_date, '%Y-%m-%d')
            query_end = datetime.strptime(end_date, '%Y-%m-%d')

            return cache_start <= query_start and cache_end >= query_end
        except Exception as e:
            logger.debug(f"缓存日期范围比较失败，视为不命中: {e}", exc_info=True)
            return False


class DataCache:
    """
    数据缓存类，提供高效的数据读取结果缓存机制
    支持自动过期、LRU淘汰、命中率统计等功能
    """

    def __init__(self, max_size: int = 500, default_ttl: int = 7200):
        """
        初始化数据缓存

        Args:
            max_size: 缓存最大条目数
            default_ttl: 默认过期时间（秒），0表示永不过期
        """
        # 缓存存储
        self._cache: Dict[str, DataCacheEntry[pl.DataFrame]] = {}
        # 缓存配置
        self._max_size = max_size
        self._default_ttl = default_ttl if default_ttl > 0 else None

        # 缓存统计
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._partial_hits = 0  # 部分命中次数

        # 内存监控线程会并发触发释放/淘汰，所有缓存读写必须串行化
        self._lock = threading.RLock()

    def set_max_size(self, max_size: int):
        """
        设置缓存最大条目数

        Args:
            max_size: 缓存最大条目数
        """
        with self._lock:
            self._max_size = max_size
            # 如果当前缓存大小超过新的最大大小，执行LRU淘汰
            while len(self._cache) > self._max_size:
                self._evict_lru()
            logger.info(f"缓存最大大小已设置为: {self._max_size}")

    def release_memory(self, target_ratio: float = 0.5) -> Dict[str, int]:
        """
        按目标水位释放缓存内存（内存管理器唯一允许的释放入口）

        Args:
            target_ratio: 目标水位占 max_size 的比例，0 表示清空全部

        Returns:
            Dict[str, int]: 释放结果，含过期删除数、LRU 淘汰数与释放后条目数
        """
        ratio = max(0.0, min(1.0, target_ratio))
        with self._lock:
            expired_keys = [k for k, e in self._cache.items() if e.is_expired()]
            for key in expired_keys:
                del self._cache[key]
            self._evictions += len(expired_keys)

            target_size = int(self._max_size * ratio)
            evicted = 0
            if target_size <= 0:
                evicted = len(self._cache)
                self._cache.clear()
                self._evictions += evicted
            else:
                while len(self._cache) > target_size:
                    self._evict_lru()
                    evicted += 1

            size = len(self._cache)

        logger.info(
            f"数据缓存释放内存: 目标水位={ratio:.0%}, 过期删除={len(expired_keys)}, "
            f"LRU淘汰={evicted}, 剩余={size}/{self._max_size}"
        )
        return {'expired': len(expired_keys), 'evicted': evicted, 'size': size}

    def _generate_cache_key(self, data_type: str, code: str, start_date: str, end_date: str, **params) -> str:
        """
        生成唯一的数据缓存键

        Args:
            data_type: 数据类型，如'stock'、'index'
            code: 代码，如股票代码、指数代码
            start_date: 开始日期
            end_date: 结束日期
            **params: 其他参数，如frequency、adjustment_type等

        Returns:
            str: 唯一的缓存键，格式为 "data_type:code:hash"
        """
        # 组合所有参数生成缓存键
        key_parts = [
            data_type,
            code,
            start_date,
            end_date
        ]

        # 添加其他参数
        for k, v in sorted(params.items()):
            key_parts.append(f"{k}={v}")

        # 生成哈希键
        key_str = "_".join(key_parts)
        hash_part = hashlib.md5(key_str.encode()).hexdigest()
        # 保留数据类型和代码信息，便于后续失效操作
        cache_key = f"{data_type}:{code}:{hash_part}"
        return cache_key

    def get(self, data_type: str, code: str, start_date: str, end_date: str, **params) -> pl.DataFrame | None:
        """
        获取缓存的数据

        Args:
            data_type: 数据类型，如'stock'、'index'
            code: 代码，如股票代码、指数代码
            start_date: 开始日期
            end_date: 结束日期
            **params: 其他参数，如frequency、adjustment_type等

        Returns:
            Optional[pl.DataFrame]: 缓存的数据，如果不存在或已过期则返回None
        """
        with self._lock:
            try:
                # 1. 尝试精确匹配
                cache_key = self._generate_cache_key(data_type, code, start_date, end_date, **params)
                if cache_key in self._cache:
                    entry = self._cache[cache_key]
                    if not entry.is_expired():
                        entry.update_access()
                        self._hits += 1
                        logger.debug(f"数据缓存精确命中: {data_type} {code}")
                        return entry.value
            except Exception as e:
                logger.warning(f"生成缓存键失败: {str(e)}")

            # 2. 尝试时间范围匹配（必须参数完全一致，避免跨复权/跨 as_of 串用）
            matched_entry = self._find_matching_cache(data_type, code, start_date, end_date, **params)
            if matched_entry:
                try:
                    filtered_df = self._filter_by_date_range(
                        matched_entry.value, start_date, end_date)
                except Exception as e:
                    logger.warning(f"从缓存中提取数据失败: {e}")
                else:
                    if filtered_df is not None and not filtered_df.is_empty():
                        self._partial_hits += 1
                        matched_entry.update_access()
                        logger.debug(f"数据缓存部分命中: {data_type} {code}")
                        return filtered_df

            # 缓存未命中
            self._misses += 1
            return None

    def _filter_by_date_range(self, df: pl.DataFrame, start_date: str,
                              end_date: str) -> pl.DataFrame | None:
        """
        按请求区间裁剪缓存数据，兼容 date / trade_date 及 Date / Datetime / Utf8 类型

        Returns:
            Optional[pl.DataFrame]: 裁剪结果；无日期列时返回 None
        """
        date_col = 'date' if 'date' in df.columns else (
            'trade_date' if 'trade_date' in df.columns else None)
        if date_col is None:
            return None

        dtype = df.schema[date_col]
        if dtype == pl.Date:
            col_expr = pl.col(date_col)
        elif dtype == pl.Datetime:
            col_expr = pl.col(date_col).dt.date()
        elif dtype == pl.String:
            # 字符串日期统一解析为 Date：兼容 YYYY-MM-DD 与 YYYYMMDD
            col_expr = pl.coalesce([
                pl.col(date_col).str.strptime(pl.Date, '%Y-%m-%d', strict=False),
                pl.col(date_col).str.strptime(pl.Date, '%Y%m%d', strict=False),
            ])
        else:
            col_expr = pl.col(date_col).cast(pl.Utf8)
            return df.filter(
                (col_expr >= start_date) & (col_expr <= end_date))

        start_value = _parse_date_bound(start_date)
        end_value = _parse_date_bound(end_date)
        if start_value is None or end_value is None:
            return None

        return df.filter(
            (col_expr >= start_value) & (col_expr <= end_value)
        )

    def _find_matching_cache(self, data_type: str, code: str, start_date: str, end_date: str, **params) -> DataCacheEntry[pl.DataFrame] | None:
        """
        查找包含指定时间范围的缓存

        Args:
            data_type: 数据类型
            code: 代码
            start_date: 开始日期
            end_date: 结束日期
            **params: 其他参数

        Returns:
            Optional[DataCacheEntry]: 匹配的缓存条目
        """
        # 过滤条件：相同数据类型、代码、参数，且未过期
        with self._lock:
            candidate_entries = []
            for _key, entry in self._cache.items():
                # 检查基本条件
                if (entry.data_type == data_type and
                    entry.code == code and
                    not entry.is_expired()):
                    # 语义参数必须完全一致：请求未带的参数不得与缓存中已有的参数混用
                    if (_semantic_params(entry.params) == _semantic_params(params)
                            and entry.contains_date_range(start_date, end_date)):
                        candidate_entries.append(entry)

        # 按命中次数和访问时间排序，优先选择使用频率高的
        if candidate_entries:
            candidate_entries.sort(key=lambda x: (x.hit_count, x.access_time), reverse=True)
            return candidate_entries[0]

        return None

    def set(self, data: pl.DataFrame, data_type: str, code: str, start_date: str, end_date: str, **params):
        """
        设置数据到缓存

        Args:
            data: 要缓存的数据
            data_type: 数据类型，如'stock'、'index'
            code: 代码，如股票代码、指数代码
            start_date: 开始日期
            end_date: 结束日期
            **params: 其他参数，如frequency、adjustment_type等
        """
        try:
            cache_key = self._generate_cache_key(data_type, code, start_date, end_date, **params)
        except Exception as e:
            logger.warning(f"生成缓存键失败: {str(e)}")
            return

        # 动态计算TTL，根据数据类型和使用频率调整
        base_ttl = params.get('ttl', self._default_ttl)
        # 对于高频访问的数据，延长过期时间
        expire_time = time.time() + base_ttl if base_ttl is not None else None

        # 内存优化：转换数据类型
        from src.utils.memory_optimizer import MemoryOptimizer
        optimized_data = MemoryOptimizer.optimize_dataframe(data, enable_sparse=True)

        # 创建缓存条目
        entry = DataCacheEntry(
            optimized_data,
            expire_time,
            data_type=data_type,
            code=code,
            start_date=start_date,
            end_date=end_date,
            params=params
        )

        # 添加到缓存（类型优化在锁外完成，避免长时间阻塞其他线程的读写）
        with self._lock:
            self._cache[cache_key] = entry

            # 检查缓存大小，超过上限则进行LRU淘汰
            if len(self._cache) > self._max_size:
                self._evict_lru()

    def prewarm_cache(self, data_type: str, code: str, start_date: str, end_date: str, **params):
        """
        预热缓存，提前缓存可能需要的数据

        Args:
            data_type: 数据类型
            code: 代码
            start_date: 开始日期
            end_date: 结束日期
            **params: 其他参数
        """
        # 这里可以实现缓存预热逻辑
        # 例如，预加载常用时间范围的数据
        logger.info(f"预热缓存: {data_type} {code} {start_date} to {end_date}")
        # 实际的预热逻辑需要根据具体业务场景实现

    def _evict_lru(self):
        """
        执行LRU淘汰策略，移除最少使用的缓存条目
        """
        with self._lock:
            if not self._cache:
                return

            # 找到最少使用的条目（按访问时间排序）
            lru_key = min(self._cache.keys(),
                         key=lambda k: self._cache[k].access_time)

            del self._cache[lru_key]
            self._evictions += 1
            logger.debug(f"LRU淘汰数据缓存: {lru_key}")

    def clear(self, data_type: str | None = None):
        """
        清除缓存

        Args:
            data_type: 可选，指定要清除的数据类型，None表示清除所有
        """
        with self._lock:
            if data_type:
                # 清除指定数据类型的缓存
                keys_to_remove = []
                for key in self._cache:
                    # 从缓存键中提取数据类型信息
                    # 缓存键格式: data_type:code:hash
                    parts = key.split(':', 1)
                    if len(parts) > 0:
                        cache_data_type = parts[0]
                        if cache_data_type == data_type:
                            keys_to_remove.append(key)

                # 清除相关缓存
                removed_count = 0
                for key in keys_to_remove:
                    if key in self._cache:
                        del self._cache[key]
                        removed_count += 1

                self._evictions += removed_count
                logger.info(f"清除{data_type}类型的{removed_count}个数据缓存条目")
            else:
                # 清除所有缓存
                evicted = len(self._cache)
                self._cache.clear()
                self._evictions += evicted
                logger.info(f"清除所有{evicted}个数据缓存条目")

    def invalidate(self, data_type: str, code: str):
        """
        使指定代码的数据缓存失效

        Args:
            data_type: 数据类型，如'stock'、'index'
            code: 代码，如股票代码、指数代码
        """
        # 遍历所有缓存条目，找到与指定数据类型和代码相关的缓存
        with self._lock:
            keys_to_remove = []
            for key in self._cache:
                # 从缓存键中提取数据类型和代码信息
                # 缓存键格式: data_type:code:hash
                parts = key.split(':', 2)
                if len(parts) >= 2:
                    cache_data_type = parts[0]
                    cache_code = parts[1]
                    if cache_data_type == data_type and cache_code == code:
                        keys_to_remove.append(key)

            # 清除相关缓存
            removed_count = 0
            for key in keys_to_remove:
                if key in self._cache:
                    del self._cache[key]
                    removed_count += 1

            self._evictions += removed_count
        logger.info(f"使{data_type} {code}的{removed_count}个缓存条目失效")

    def invalidate_by_type(self, data_type: str):
        """
        使指定数据类型的所有缓存失效

        Args:
            data_type: 数据类型，如'stock'、'index'
        """
        # 遍历所有缓存条目，找到与指定数据类型相关的缓存
        with self._lock:
            keys_to_remove = []
            for key in self._cache:
                # 从缓存键中提取数据类型信息
                # 缓存键格式: data_type:code:hash
                parts = key.split(':', 1)
                if len(parts) > 0:
                    cache_data_type = parts[0]
                    if cache_data_type == data_type:
                        keys_to_remove.append(key)

            # 清除相关缓存
            removed_count = 0
            for key in keys_to_remove:
                if key in self._cache:
                    del self._cache[key]
                    removed_count += 1

            self._evictions += removed_count
        logger.info(f"使{data_type}类型的{removed_count}个缓存条目失效")

    def get_stats(self) -> Dict[str, Any]:
        """
        获取缓存统计信息

        Returns:
            Dict[str, Any]: 缓存统计信息
        """
        with self._lock:
            total_requests = self._hits + self._misses + self._partial_hits
            hit_rate = (self._hits + self._partial_hits) / total_requests if total_requests > 0 else 0.0

            return {
                'size': len(self._cache),
                'max_size': self._max_size,
                'hits': self._hits,
                'partial_hits': self._partial_hits,
                'misses': self._misses,
                'hit_rate': hit_rate,
                'evictions': self._evictions,
                'total_requests': total_requests
            }

    def reset_stats(self):
        """
        重置缓存统计信息
        """
        with self._lock:
            self._hits = 0
            self._misses = 0
            self._partial_hits = 0
            self._evictions = 0

    def get_cache_info(self) -> Dict[str, Any]:
        """
        获取详细的缓存信息

        Returns:
            Dict[str, Any]: 详细的缓存信息
        """
        # 获取缓存条目的信息
        with self._lock:
            entries_info = []
            for key, entry in self._cache.items():
                entries_info.append({
                    'key': key,
                    'size': entry.value.shape[0] * entry.value.shape[1],
                    'access_time': entry.access_time,
                    'hit_count': entry.hit_count,
                    'expire_time': entry.expire_time,
                    'is_expired': entry.is_expired(),
                    'data_type': entry.data_type,
                    'code': entry.code,
                    'start_date': entry.start_date,
                    'end_date': entry.end_date,
                    'params': entry.params
                })

        return {
            'stats': self.get_stats(),
            'config': {
                'max_size': self._max_size,
                'default_ttl': self._default_ttl
            },
            'entries': entries_info
        }


# 创建全局数据缓存实例
global_data_cache = DataCache(max_size=500, default_ttl=7200)
