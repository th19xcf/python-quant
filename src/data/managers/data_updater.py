#!/usr/bin/env python3

"""
数据更新模块

单源故障转移更新（first-success-wins）。与 sync_service.py 的多源合并写入
职责不同：DataUpdater 面向增量刷新，只调一个 handler；sync_service 面向
首次拉取/全量校准，会同时调多个 handler 做并集合并。

调用方：DataService → DataUpdater → handler
"""

from datetime import date, datetime
from typing import Dict, List

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
            # 兜底理由：单个数据源更新失败继续尝试下一个
            except Exception as e:  # noqa: BLE001
                logger.warning(f"{source.name}更新{type_name}失败: {e}")
                continue

        logger.error(f"所有数据源更新{type_name}失败")
        return False

    # ------------------------------------------------------------------
    # 数据更新入口（全部委托 _try_sources）
    # ------------------------------------------------------------------

    def update_stock_basic(self) -> bool:
        return self._try_sources('update_stock_basic', '股票基本信息')

    def update_stock_daily(self, ts_codes: List[str] | None = None,
                           start_date: str | None = None, end_date: str | None = None) -> bool:
        success = self._try_sources(
            'update_stock_daily', '股票日线数据',
            ts_codes=ts_codes, start_date=start_date, end_date=end_date,
        )
        if success and ts_codes and start_date and end_date:
            self.check_daily_gaps(ts_codes, start_date, end_date)
        return success

    def check_daily_gaps(self, ts_codes: List[str], start_date: str, end_date: str,
                         max_codes: int = 500) -> Dict[str, List]:
        """
        检测指定标的在区间内相对全市场交易日的缺失日期

        以全市场 stock_daily 中实际出现的 trade_date 作为"预期交易日"，无需额外日历表。
        若全市场在某日整体缺失（数据源整体故障），本方法无法发现，需由健康检查兜底。

        Args:
            ts_codes: 待检测标的
            start_date: 起始日期（YYYY-MM-DD / YYYYMMDD / date）
            end_date: 结束日期（同上）
            max_codes: 单次检测的标的数上限，避免全市场扫描

        Returns:
            Dict[str, List]: {ts_code: [缺失日期, ...]}，无缺口时为空字典
        """
        if not self.db_manager or not ts_codes or not start_date or not end_date:
            return {}
        if len(ts_codes) > max_codes:
            logger.info(f"缺口检测跳过：标的数 {len(ts_codes)} 超过上限 {max_codes}")
            return {}

        from src.data.managers.data_processor import find_missing_trade_dates
        from src.database.models.stock import StockDaily

        def _as_date(value):
            if isinstance(value, datetime):
                return value.date()
            if isinstance(value, date):
                return value
            return datetime.strptime(str(value).replace('-', ''), '%Y%m%d').date()

        start = _as_date(start_date)
        end = _as_date(end_date)

        session = self.db_manager.get_session()
        expected_rows = (
            session.query(StockDaily.trade_date)
            .filter(StockDaily.trade_date >= start, StockDaily.trade_date <= end)
            .distinct()
            .all()
        )
        expected_dates = [row[0] for row in expected_rows]
        if not expected_dates:
            return {}

        actual_rows = (
            session.query(StockDaily.ts_code, StockDaily.trade_date)
            .filter(
                StockDaily.ts_code.in_(ts_codes),
                StockDaily.trade_date >= start,
                StockDaily.trade_date <= end,
            )
            .all()
        )
        actual_map: Dict[str, List] = {}
        for ts_code, trade_date in actual_rows:
            actual_map.setdefault(ts_code, []).append(trade_date)

        gaps: Dict[str, List] = {}
        for ts_code in ts_codes:
            missing = find_missing_trade_dates(expected_dates, actual_map.get(ts_code, []))
            if missing:
                gaps[ts_code] = missing
                logger.warning(
                    f"{ts_code} 在 {start}~{end} 缺失 {len(missing)} 个交易日，样例: {missing[:5]}"
                )

        if not gaps:
            logger.info(f"缺口检测通过：{len(ts_codes)} 个标的无缺失交易日")
        return gaps

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
