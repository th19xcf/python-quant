#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据缓存模块，提供高效的数据读取结果缓存机制
"""

from typing import Dict, Any, Optional, TypeVar, Generic, List, Tuple
import hashlib
import polars as pl
from loguru import logger
import time
from datetime import datetime

# 定义缓存键和值的类型变量
K = TypeVar('K')
V = TypeVar('V')


class CacheEntry(Generic[V]):
    """
    缓存条目类，用于存储缓存值及其元数据
    """
    
    def __init__(self, value: V, expire_time: Optional[float] = None, 
                 data_type: str = '', code: str = '', 
                 start_date: str = '', end_date: str = '', 
                 params: Optional[Dict[str, Any]] = None):
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
        except:
            return False
    
    def can_support_frequency(self, target_frequency: str) -> bool:
        """
        检查当前缓存是否可以支持目标频率
        
        Args:
            target_frequency: 目标频率
            
        Returns:
            bool: 是否可以支持
        """
        current_freq = self.params.get('frequency', '1d')
        # 频率层次关系：1d -> 1w -> 1m
        freq_hierarchy = {'1d': ['1w', '1m'], '1w': ['1m'], '1m': []}
        
        return target_frequency in freq_hierarchy.get(current_freq, [])


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
        self._cache: Dict[str, CacheEntry[pl.DataFrame]] = {}
        # 缓存配置
        self._max_size = max_size
        self._default_ttl = default_ttl if default_ttl > 0 else None
        
        # 缓存统计
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._partial_hits = 0  # 部分命中次数
    
    def set_max_size(self, max_size: int):
        """
        设置缓存最大条目数
        
        Args:
            max_size: 缓存最大条目数
        """
        self._max_size = max_size
        # 如果当前缓存大小超过新的最大大小，执行LRU淘汰
        while len(self._cache) > self._max_size:
            self._evict_lru()
        logger.info(f"缓存最大大小已设置为: {self._max_size}")
    
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
    
    def get(self, data_type: str, code: str, start_date: str, end_date: str, **params) -> Optional[pl.DataFrame]:
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
        
        # 2. 尝试时间范围匹配
        matched_entry = self._find_matching_cache(data_type, code, start_date, end_date, **params)
        if matched_entry:
            self._partial_hits += 1
            logger.debug(f"数据缓存部分命中: {data_type} {code}")
            # 从匹配的缓存中提取需要的时间范围
            try:
                df = matched_entry.value
                # 过滤出需要的时间范围
                if 'date' in df.columns:
                    filtered_df = df.filter(
                        (pl.col('date') >= start_date) & 
                        (pl.col('date') <= end_date)
                    )
                    if not filtered_df.is_empty():
                        # 更新访问信息
                        matched_entry.update_access()
                        return filtered_df
            except Exception as e:
                logger.warning(f"从缓存中提取数据失败: {e}")
        
        # 3. 尝试频率层次匹配
        freq_matched_entry = self._find_frequency_matching_cache(data_type, code, start_date, end_date, **params)
        if freq_matched_entry:
            self._partial_hits += 1
            logger.debug(f"数据缓存频率匹配: {data_type} {code}")
            # 这里可以返回原始数据，由调用方进行频率转换
            freq_matched_entry.update_access()
            return freq_matched_entry.value
        
        # 缓存未命中
        self._misses += 1
        return None
    
    def _find_matching_cache(self, data_type: str, code: str, start_date: str, end_date: str, **params) -> Optional[CacheEntry[pl.DataFrame]]:
        """
        查找包含指定时间范围的缓存
        
        Args:
            data_type: 数据类型
            code: 代码
            start_date: 开始日期
            end_date: 结束日期
            **params: 其他参数
            
        Returns:
            Optional[CacheEntry]: 匹配的缓存条目
        """
        # 过滤条件：相同数据类型、代码、参数，且未过期
        candidate_entries = []
        for key, entry in self._cache.items():
            # 检查基本条件
            if (entry.data_type == data_type and 
                entry.code == code and 
                not entry.is_expired()):
                # 检查参数是否匹配
                params_match = True
                for k, v in params.items():
                    if entry.params.get(k) != v:
                        params_match = False
                        break
                
                if params_match and entry.contains_date_range(start_date, end_date):
                    candidate_entries.append(entry)
        
        # 按命中次数和访问时间排序，优先选择使用频率高的
        if candidate_entries:
            candidate_entries.sort(key=lambda x: (x.hit_count, x.access_time), reverse=True)
            return candidate_entries[0]
        
        return None
    
    def _find_frequency_matching_cache(self, data_type: str, code: str, start_date: str, end_date: str, **params) -> Optional[CacheEntry[pl.DataFrame]]:
        """
        查找可以支持目标频率的缓存
        
        Args:
            data_type: 数据类型
            code: 代码
            start_date: 开始日期
            end_date: 结束日期
            **params: 其他参数
            
        Returns:
            Optional[CacheEntry]: 匹配的缓存条目
        """
        target_frequency = params.get('frequency', '1d')
        
        # 过滤条件：相同数据类型、代码，且未过期
        candidate_entries = []
        for key, entry in self._cache.items():
            # 检查基本条件
            if (entry.data_type == data_type and 
                entry.code == code and 
                not entry.is_expired()):
                # 检查是否可以支持目标频率
                if entry.can_support_frequency(target_frequency):
                    # 检查日期范围
                    if entry.contains_date_range(start_date, end_date):
                        candidate_entries.append(entry)
        
        # 按命中次数和访问时间排序
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
        entry = CacheEntry(
            optimized_data, 
            expire_time, 
            data_type=data_type, 
            code=code, 
            start_date=start_date, 
            end_date=end_date, 
            params=params
        )
        
        # 添加到缓存
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
        if not self._cache:
            return
        
        # 找到最少使用的条目（按访问时间排序）
        lru_key = min(self._cache.keys(), 
                     key=lambda k: self._cache[k].access_time)
        
        del self._cache[lru_key]
        self._evictions += 1
        logger.debug(f"LRU淘汰数据缓存: {lru_key}")
    
    def clear(self, data_type: Optional[str] = None):
        """
        清除缓存
        
        Args:
            data_type: 可选，指定要清除的数据类型，None表示清除所有
        """
        if data_type:
            # 清除指定数据类型的缓存
            keys_to_remove = []
            for key in self._cache.keys():
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
        keys_to_remove = []
        for key in self._cache.keys():
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
        keys_to_remove = []
        for key in self._cache.keys():
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