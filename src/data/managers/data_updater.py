#!/usr/bin/env python3

"""
数据更新模块

单源故障转移更新（first-success-wins）。与 sync_service.py 的多源合并写入
职责不同：DataUpdater 面向增量刷新，只调一个 handler；sync_service 面向
首次拉取/全量校准，会同时调多个 handler 做并集合并。

调用方：DataService → DataUpdater → handler
"""

from typing import List

from loguru import logger


class DataUpdater:
    """
    单源故障转移更新器。

    对每个数据类型，按 self.sources 注册顺序依次调用同名方法，
    第一个不抛异常即视为成功，后续 source 不再尝试。
    全部失败则返回 False。
    """

    def __init__(self, config, db_manager=None):
        self.config = config
        self.db_manager = db_manager
        self.sources = []

    def register_source(self, source):
        if source not in self.sources:
            self.sources.append(source)
            logger.info(f"注册更新数据源: {source.name}")

    def _try_sources(self, method_name: str, type_name: str, **kwargs) -> bool:
        """按注册顺序依次尝试各 source 的 method_name，首个成功即返回 True。"""
        for source in self.sources:
            try:
                if not hasattr(source, method_name):
                    continue
                logger.info(f"使用{source.name}更新{type_name}")
                getattr(source, method_name)(**kwargs)
                return True
            except Exception as e:
                logger.warning(f"{source.name}更新{type_name}失败: {e}")
                continue

        logger.error(f"所有数据源更新{type_name}失败")
        return False

    # ------------------------------------------------------------------
    # 数据更新入口（全部委托 _try_sources）
    # ------------------------------------------------------------------

    def update_stock_basic(self) -> bool:
        return self._try_sources('update_stock_basic', '股票基本信息')

    def update_stock_daily(self, ts_codes: List[str] = None,
                           start_date: str = None, end_date: str = None) -> bool:
        return self._try_sources(
            'update_stock_daily', '股票日线数据',
            ts_codes=ts_codes, start_date=start_date, end_date=end_date,
        )

    def update_fund_basic(self) -> bool:
        return self._try_sources('update_fund_basic', '基金基本信息')

    def update_fund_daily(self, ts_codes: List[str] = None,
                          start_date: str = None, end_date: str = None) -> bool:
        return self._try_sources(
            'update_fund_daily', '基金日线数据',
            ts_codes=ts_codes, start_date=start_date, end_date=end_date,
        )

    def update_closed_fund_basic(self) -> bool:
        return self._try_sources('update_closed_fund_basic', '封闭式基金基本信息')

    def update_closed_fund_daily(self, ts_codes: List[str] = None,
                                  start_date: str = None, end_date: str = None) -> bool:
        return self._try_sources(
            'update_closed_fund_daily', '封闭式基金日线数据',
            ts_codes=ts_codes, start_date=start_date, end_date=end_date,
        )

    def update_index_basic(self) -> bool:
        return self._try_sources('update_index_basic', '指数基本信息')

    def update_index_daily(self, ts_codes: List[str] = None,
                           start_date: str = None, end_date: str = None) -> bool:
        return self._try_sources(
            'update_index_daily', '指数日线数据',
            ts_codes=ts_codes, start_date=start_date, end_date=end_date,
        )

    def update_macro_data(self, indicators: List[str] = None) -> bool:
        return self._try_sources('update_macro_data', '宏观经济数据', indicators=indicators)

    def update_news_data(self, source_names: List[str] = None,
                         start_date: str = None, end_date: str = None) -> bool:
        """更新新闻数据。

        参数改名 source_names 避免遮蔽 self.sources（已注册的 handler 列表）。
        """
        return self._try_sources(
            'update_news_data', '新闻数据',
            sources=source_names, start_date=start_date, end_date=end_date,
        )

    def update_stock_dividend(self, ts_codes: List[str] = None) -> bool:
        return self._try_sources('update_stock_dividend', '股票分红配股数据', ts_codes=ts_codes)
