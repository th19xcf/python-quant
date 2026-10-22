#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据服务

数据获取 + 更新 + 缓存的统一入口。

此模块是 data_manager.py 的后端实现：DataManager 是兼容外部的壳，
DataService 是真正干活的核心，持有 DataFetcher / DataUpdater + global_data_cache。

调用链（统一路径，无行为漂移）：
    get_stock_data → self.data_fetcher.get_stock_data
        ├─ global_data_cache 命中 → 返回
        ├─ _get_data_from_database → DB 双复权列 + DataFetcher._apply_adjustment
        └─ _get_data_from_sources → TDX / BaoStock / AkShare 故障转移
"""

from typing import List, Dict, Optional
import polars as pl
from loguru import logger

from src.data.managers.data_fetcher import DataFetcher
from src.data.managers.data_updater import DataUpdater
from src.data.data_cache import global_data_cache
from src.utils.event_bus import publish


class DataService:
    """
    数据服务，整合数据获取、更新和缓存

    直接持有底层实现，不再包装 DataProvider / DataCacheService / DataUpdaterService
    这三层薄壳（已于 2026-10 重构删除）。
    """

    def __init__(self, config, db_manager=None, plugin_manager=None):
        self.config = config
        self.db_manager = db_manager
        self.plugin_manager = plugin_manager

        # 直接持有底层实现
        self.data_fetcher = DataFetcher(config, db_manager)
        self.data_updater = DataUpdater(config, db_manager)

        # 插件数据源映射
        self.plugin_datasources = {}

        self._init_builtin_datasources()
        self._init_plugin_datasources()

    def _init_builtin_datasources(self):
        """初始化内置数据源"""
        for handler_name, import_path, module_name in [
            ('通达信', 'src.data.tdx_handler', 'TdxHandler'),
            ('BaoStock', 'src.data.baostock_handler', 'BaostockHandler'),
            ('AkShare', 'src.data.akshare_handler', 'AkShareHandler'),
            ('宏观数据', 'src.data.macro_handler', 'MacroHandler'),
            ('新闻数据', 'src.data.news_handler', 'NewsHandler'),
        ]:
            try:
                mod = __import__(import_path, fromlist=[module_name])
                handler_cls = getattr(mod, module_name)
                handler = handler_cls(self.config, self.db_manager)
                self.data_fetcher.register_source(handler)
                self.data_updater.register_source(handler)
            except ImportError as e:
                logger.info(f"{handler_name}模块未安装: {e}")
            except Exception as e:
                logger.warning(f"{handler_name}数据处理器初始化失败: {e}")

        logger.info("内置数据源初始化完成")

    def _init_plugin_datasources(self):
        """初始化插件数据源"""
        if not self.plugin_manager:
            return

        datasource_plugins = self.plugin_manager.get_available_datasource_plugins()
        for plugin_name, plugin in datasource_plugins.items():
            self.plugin_datasources[plugin_name] = plugin
            self.data_fetcher.register_source(plugin)
            self.data_updater.register_source(plugin)
            logger.info(f"已注册插件数据源: {plugin_name}")

    # ------------------------------------------------------------------
    # 数据获取（直接透传 DataFetcher）
    # ------------------------------------------------------------------

    def get_stock_data(self, stock_code: str, start_date: str, end_date: str,
                      frequency: str = '1d', adjustment_type: str = 'qfq') -> pl.DataFrame:
        return self.data_fetcher.get_stock_data(
            stock_code, start_date, end_date, frequency, adjustment_type
        )

    def get_index_data(self, index_code: str, start_date: str, end_date: str,
                      frequency: str = '1d') -> pl.DataFrame:
        return self.data_fetcher.get_index_data(
            index_code, start_date, end_date, frequency
        )

    def get_available_sources(self) -> List:
        return self.data_fetcher.get_available_sources()

    # ------------------------------------------------------------------
    # 数据更新（直接透传 DataUpdater + 事件发布）
    # ------------------------------------------------------------------

    def _publish_updated(self, data_type, data_subtype, cache_type=None,
                         ts_codes=None, status="success"):
        """统一的事件发布 + 缓存失效"""
        if cache_type is not None:
            if ts_codes:
                for code in ts_codes:
                    global_data_cache.invalidate(cache_type, code)
            else:
                global_data_cache.invalidate_by_type(cache_type)
        publish(
            'data_updated' if status == 'success' else 'data_error',
            data_type=data_type,
            data_subtype=data_subtype,
        )

    def update_stock_basic(self):
        success = self.data_updater.update_stock_basic()
        if success:
            self._publish_updated('stock', 'basic', cache_type='stock_basic')
        return success

    def update_stock_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        success = self.data_updater.update_stock_daily(ts_codes, start_date, end_date)
        if success:
            self._publish_updated('stock', 'daily', cache_type='stock', ts_codes=ts_codes)
        return success

    def update_fund_basic(self):
        success = self.data_updater.update_fund_basic()
        if success:
            self._publish_updated('fund', 'basic', cache_type='fund_basic')
        return success

    def update_fund_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        success = self.data_updater.update_fund_daily(ts_codes, start_date, end_date)
        if success:
            self._publish_updated('fund', 'daily', cache_type='fund', ts_codes=ts_codes)
        return success

    def update_closed_fund_basic(self):
        success = self.data_updater.update_closed_fund_basic()
        if success:
            self._publish_updated('closed_fund', 'basic', cache_type='closed_fund_basic')
        return success

    def update_closed_fund_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        success = self.data_updater.update_closed_fund_daily(ts_codes, start_date, end_date)
        if success:
            self._publish_updated('closed_fund', 'daily', cache_type='closed_fund', ts_codes=ts_codes)
        return success

    def update_index_basic(self):
        success = self.data_updater.update_index_basic()
        if success:
            self._publish_updated('index', 'basic', cache_type='index_basic')
        return success

    def update_index_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        success = self.data_updater.update_index_daily(ts_codes, start_date, end_date)
        if success:
            self._publish_updated('index', 'daily', cache_type='index', ts_codes=ts_codes)
        return success

    def update_macro_data(self, indicators: List[str] = None):
        success = self.data_updater.update_macro_data(indicators)
        if success:
            self._publish_updated('macro', 'data')
        return success

    def update_news_data(self, sources: List[str] = None, start_date: str = None, end_date: str = None):
        success = self.data_updater.update_news_data(sources, start_date, end_date)
        if success:
            self._publish_updated('news', 'data')
        return success

    def update_stock_dividend(self, ts_codes: List[str] = None):
        success = self.data_updater.update_stock_dividend(ts_codes)
        if success:
            self._publish_updated('stock', 'dividend',
                                  cache_type='stock_dividend', ts_codes=ts_codes)
        return success

    # ------------------------------------------------------------------
    # 缓存管理（直接透传 global_data_cache）
    # ------------------------------------------------------------------

    def get_cache_stats(self) -> Dict:
        return global_data_cache.get_stats()

    def clear_cache(self):
        global_data_cache.clear()

    def invalidate_cache(self, data_type: str, code: str, **kwargs):
        global_data_cache.invalidate(data_type, code, **kwargs)

    def invalidate_cache_by_type(self, data_type: str):
        global_data_cache.invalidate_by_type(data_type)
