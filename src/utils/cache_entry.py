#!/usr/bin/env python3

"""
缓存条目公共行为

data_cache.DataCacheEntry 与 indicator_cache.IndicatorCacheEntry 共用
过期判定与访问统计逻辑，统一在此收口，避免两处实现出现口径分叉。
"""

import time


class TimestampedCacheEntryMixin:
    """为缓存条目提供过期判定与访问统计（要求子类具备以下属性）"""

    expire_time: float | None
    access_time: float
    hit_count: int

    def is_expired(self) -> bool:
        """检查缓存是否过期（expire_time=None 表示永不过期）"""
        if self.expire_time is None:
            return False
        return time.time() > self.expire_time

    def update_access(self) -> None:
        """更新访问时间并累加命中次数"""
        self.access_time = time.time()
        self.hit_count += 1
