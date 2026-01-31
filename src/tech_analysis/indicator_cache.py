#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
指标缓存模块，提供高效的指标计算结果缓存机制
"""

from typing import Dict, Any, Optional, TypeVar, Generic
import hashlib
import polars as pl
from loguru import logger
import time

# 定义缓存键和值的类型变量
K = TypeVar('K')
V = TypeVar('V')


class CacheEntry(Generic[V]):
    """
    缓存条目类，用于存储缓存值及其元数据
    """
    
    def __init__(self, value: V, expire_time: Optional[float] = None):
        """
        初始化缓存条目
        
        Args:
            value: 缓存值
            expire_time: 过期时间（时间戳），None表示永不过期
        """
        self.value = value
        self.expire_time = expire_time
        self.access_time = time.time()  # 上次访问时间
        self.hit_count = 1  # 命中次数
    
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


class IndicatorCache:
    """
    指标缓存类，提供高效的指标计算结果缓存机制
    支持自动过期、LRU淘汰、命中率统计等功能
    """
    
    def __init__(self, max_size: int = 1000, default_ttl: int = 3600):
        """
        初始化指标缓存
        
        Args:
            max_size: 缓存最大条目数
            default_ttl: 默认过期时间（秒），0表示永不过期
        """
        # 缓存存储
        self._cache: Dict[str, CacheEntry[pl.DataFrame]] = {}
        # 缓存配置
        self._max_size = max_size
        self._default_ttl = default_ttl if default_ttl > 0 else None
        
        # 缓存统计
        self._hits = 0
        self._misses = 0
        self._evictions = 0
    
    def _generate_cache_key(self, data: pl.DataFrame, indicator_type: str, **params) -> str:
        """
        生成唯一的缓存键
        
        Args:
            data: 输入数据
            indicator_type: 指标类型
            **params: 指标计算参数
        
        Returns:
            str: 唯一的缓存键
        """
        # 1. 计算数据哈希
        # 只使用核心列进行哈希计算，提高效率
        core_cols = ['open', 'high', 'low', 'close', 'volume']
        data_cols = [col for col in core_cols if col in data.columns]
        
        if not data_cols:
            raise ValueError("数据中没有核心列，无法生成缓存键")
        
        # 计算数据摘要
        data_sample = data.select(data_cols).head(100)  # 使用前100行进行哈希计算
        try:
            data_str = data_sample.to_pandas().to_csv(index=False).encode('utf-8')
        except Exception:
            data_bytes = data_sample.to_numpy().tobytes()
            cols_bytes = ",".join(data_cols).encode('utf-8')
            data_str = cols_bytes + b"|" + data_bytes
        data_hash = hashlib.md5(data_str).hexdigest()
        
        # 2. 生成参数字符串
        # 对参数进行排序，确保相同参数不同顺序生成相同的键
        sorted_params = sorted(params.items(), key=lambda x: x[0])
        params_str = "".join([f"{k}={v}" for k, v in sorted_params])
        
        # 3. 组合生成最终缓存键
        cache_key = f"{indicator_type}_{data_hash}_{hashlib.md5(params_str.encode('utf-8')).hexdigest()}"
        return cache_key
    
    def get(self, data: pl.DataFrame, indicator_type: str, **params) -> Optional[pl.DataFrame]:
        """
        获取缓存的指标计算结果
        
        Args:
            data: 输入数据
            indicator_type: 指标类型
            **params: 指标计算参数
        
        Returns:
            Optional[pl.DataFrame]: 缓存的计算结果，如果不存在或已过期则返回None
        """
        try:
            cache_key = self._generate_cache_key(data, indicator_type, **params)
        except ValueError as e:
            logger.warning(f"生成缓存键失败: {str(e)}")
            self._misses += 1
            return None
        
        if cache_key not in self._cache:
            self._misses += 1
            return None
        
        entry = self._cache[cache_key]
        
        # 检查是否过期
        if entry.is_expired():
            del self._cache[cache_key]
            self._evictions += 1
            self._misses += 1
            return None
        
        # 更新访问信息
        entry.update_access()
        self._hits += 1
        
        logger.debug(f"缓存命中: {indicator_type}")
        return entry.value
    
    def set(self, data: pl.DataFrame, result: pl.DataFrame, indicator_type: str, **params):
        """
        设置指标计算结果到缓存
        
        Args:
            data: 输入数据
            result: 计算结果
            indicator_type: 指标类型
            **params: 指标计算参数
        """
        try:
            cache_key = self._generate_cache_key(data, indicator_type, **params)
        except ValueError as e:
            logger.warning(f"生成缓存键失败: {str(e)}")
            return
        
        # 计算过期时间
        ttl = params.get('ttl', self._default_ttl)
        expire_time = time.time() + ttl if ttl is not None else None
        
        # 创建缓存条目
        entry = CacheEntry(result, expire_time)
        
        # 添加到缓存
        self._cache[cache_key] = entry
        
        # 检查缓存大小，超过上限则进行LRU淘汰
        if len(self._cache) > self._max_size:
            self._evict_lru()
    
    def _evict_lru(self):
        """
        执行LRU淘汰策略，移除最少使用的缓存条目
        """
        if not self._cache:
            return
        
        # 找到最少使用的条目（按访问时间排序）
        lru_key = min(self._cache.keys(), 
                     key=lambda k: self._cache[k].access_time)
        
        del self._cache[lru_key]
        self._evictions += 1
        logger.debug(f"LRU淘汰缓存: {lru_key}")
    
    def clear(self, indicator_type: Optional[str] = None):
        """
        清除缓存
        
        Args:
            indicator_type: 可选，指定要清除的指标类型，None表示清除所有
        """
        if indicator_type:
            # 清除指定指标类型的缓存
            keys_to_remove = [key for key in self._cache.keys() 
                             if key.startswith(f"{indicator_type}_")]
            for key in keys_to_remove:
                del self._cache[key]
            self._evictions += len(keys_to_remove)
            logger.info(f"清除指标{indicator_type}的{len(keys_to_remove)}个缓存条目")
        else:
            # 清除所有缓存
            evicted = len(self._cache)
            self._cache.clear()
            self._evictions += evicted
            logger.info(f"清除所有{evicted}个缓存条目")
    
    def invalidate(self, data: pl.DataFrame, indicator_type: Optional[str] = None):
        """
        使相关缓存失效
        
        Args:
            data: 输入数据
            indicator_type: 可选，指定要失效的指标类型，None表示失效所有相关指标
        """
        # 生成数据哈希前缀
        core_cols = ['open', 'high', 'low', 'close', 'volume']
        data_cols = [col for col in core_cols if col in data.columns]
        
        if not data_cols:
            logger.warning("数据中没有核心列，无法生成缓存键前缀")
            return
        
        # 计算数据哈希
        data_sample = data.select(data_cols).head(100)
        data_str = data_sample.to_csv().encode('utf-8')
        data_hash = hashlib.md5(data_str).hexdigest()
        
        # 生成缓存键前缀
        if indicator_type:
            prefix = f"{indicator_type}_{data_hash}_"
        else:
            prefix = f"_data_hash_"
        
        # 清除相关缓存
        keys_to_remove = [key for key in self._cache.keys() 
                         if prefix in key]
        
        for key in keys_to_remove:
            del self._cache[key]
        
        self._evictions += len(keys_to_remove)
        logger.info(f"使{len(keys_to_remove)}个缓存条目失效")
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取缓存统计信息
        
        Returns:
            Dict[str, Any]: 缓存统计信息
        """
        total_requests = self._hits + self._misses
        hit_rate = self._hits / total_requests if total_requests > 0 else 0.0
        
        return {
            'size': len(self._cache),
            'max_size': self._max_size,
            'hits': self._hits,
            'misses': self._misses,
            'hit_rate': hit_rate,
            'evictions': self._evictions,
            'total_requests': total_requests
        }
    
    def reset_stats(self):
        """
        重置缓存统计信息
        """
        self._hits = 0
        self._misses = 0
        self._evictions = 0
    
    def get_cache_info(self) -> Dict[str, Any]:
        """
        获取详细的缓存信息
        
        Returns:
            Dict[str, Any]: 详细的缓存信息
        """
        # 获取缓存条目的信息
        entries_info = []
        for key, entry in self._cache.items():
            entries_info.append({
                'key': key,
                'size': entry.value.shape[0] * entry.value.shape[1],
                'access_time': entry.access_time,
                'hit_count': entry.hit_count,
                'expire_time': entry.expire_time,
                'is_expired': entry.is_expired()
            })
        
        return {
            'stats': self.get_stats(),
            'config': {
                'max_size': self._max_size,
                'default_ttl': self._default_ttl
            },
            'entries': entries_info
        }


# 创建全局缓存实例
global_indicator_cache = IndicatorCache(max_size=1000, default_ttl=3600)


def cached_calculation(cache: Optional[IndicatorCache] = None, ttl: Optional[int] = None):
    """
    缓存装饰器，用于缓存指标计算结果
    
    Args:
        cache: 缓存实例，None表示使用全局缓存
        ttl: 缓存过期时间（秒），None表示使用缓存默认值
    
    Returns:
        Callable: 装饰后的函数
    """
    cache_instance = cache or global_indicator_cache
    
    def decorator(func):
        def wrapper(data: pl.DataFrame, **params):
            # 获取指标类型（从函数名或参数中获取）
            indicator_type = params.get('indicator_type', func.__name__)
            
            # 尝试从缓存获取结果
            cache_result = cache_instance.get(data, indicator_type, **params)
            if cache_result is not None:
                return cache_result
            
            # 执行计算
            result = func(data, **params)
            
            # 设置缓存
            cache_params = params.copy()
            if ttl is not None:
                cache_params['ttl'] = ttl
            cache_instance.set(data, result, indicator_type, **cache_params)
            
            return result
        
        return wrapper
    
    return decorator
