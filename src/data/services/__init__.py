#!/usr/bin/env python3

"""
数据服务模块

DataService：数据获取 + 更新 + 缓存的统一入口，直接持有
DataFetcher / DataUpdater / global_data_cache，无中间包装层。

2026-10 重构：删除 DataProvider / DataCacheService / DataUpdaterService
三层薄包装（services/data_provider.py / data_cache_service.py / data_updater.py），
统一到 DataService 单一路径，消除行为漂移。
"""

from .data_service import DataService

__all__ = ['DataService']
