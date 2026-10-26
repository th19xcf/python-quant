#!/usr/bin/env python3

"""
指数数据服务

负责指数名称/ts_code 解析、按分类过滤，以及从通达信日线文件
枚举指数最新行情。UI 层只负责展示格式化，不再直接执行 SQL。
"""

import contextlib
import time
from pathlib import Path
from typing import Dict, List

from loguru import logger

from src.data.tdx_day_parser import read_latest_quote
from src.utils.exceptions import DataException

# 各市场指数代码前缀
INDEX_CODE_PREFIXES = {'sh': '000', 'sz': '399', 'bj': '899'}


class IndexService:
    """指数元数据与最新行情查询服务。"""

    def __init__(self, tdx_data_path: str | None, db_manager=None, cache_ttl_seconds: int = 300):
        self._tdx_data_path = Path(tdx_data_path) if tdx_data_path else None
        self._db_manager = db_manager
        self._cache_ttl_seconds = cache_ttl_seconds
        self._cache_time = 0.0
        self._file_name_map: Dict[str, str] = {}
        self._name_ts_code_map: Dict[str, str] = {}
        self._normalized_ts_code_map: Dict[str, str] = {}

    # ------------------------------------------------------------------
    # 指数名称 / ts_code 解析
    # ------------------------------------------------------------------

    def _load_index_maps(self):
        """从 index_basic 加载名称映射（带 TTL 缓存，失败时回退上次结果）。"""
        now_ts = time.time()
        if now_ts - self._cache_time <= self._cache_ttl_seconds:
            return

        file_name_map: Dict[str, str] = {}
        name_ts_code_map: Dict[str, str] = {}
        normalized_ts_code_map: Dict[str, str] = {}

        try:
            if self._db_manager:
                from sqlalchemy import text

                session = self._db_manager.get_session()
                if session:
                    sql = text(
                        """
                        SELECT ts_code, name
                        FROM index_basic
                        WHERE name IS NOT NULL
                          AND (
                            ts_code LIKE '000%.SH'
                            OR ts_code LIKE '399%.SZ'
                            OR ts_code LIKE '899%.BJ'
                            OR ts_code LIKE '884%.BJ'
                          )
                        """
                    )

                    result = session.execute(sql)
                    for row in result:
                        ts_code, name = row[0], row[1]
                        if not ts_code or not name:
                            continue

                        symbol, _, market_suffix = ts_code.partition('.')
                        market_suffix = market_suffix.upper()
                        if market_suffix == 'SH' and symbol.startswith('000'):
                            file_name_map[f"sh{symbol}"] = name
                        elif market_suffix == 'SZ' and symbol.startswith('399'):
                            file_name_map[f"sz{symbol}"] = name
                        elif market_suffix == 'BJ' and symbol.startswith(('899', '884')):
                            file_name_map[f"bj{symbol}"] = name
                        else:
                            continue

                        name_ts_code_map[name] = ts_code
                        normalized_name = self.normalize_index_name(name)
                        if normalized_name and normalized_name not in normalized_ts_code_map:
                            normalized_ts_code_map[normalized_name] = ts_code

                    with contextlib.suppress(Exception):
                        result.close()

                logger.info(f"从数据表加载了 {len(file_name_map)} 个指数名称")
        except DataException as e:
            logger.warning(f"从数据库获取指数名称失败: {e}")
            file_name_map = self._file_name_map
            name_ts_code_map = self._name_ts_code_map
            normalized_ts_code_map = self._normalized_ts_code_map
            logger.info(f"数据库查询失败，回退使用缓存中的 {len(file_name_map)} 个指数名称")

        self._file_name_map = file_name_map
        self._name_ts_code_map = name_ts_code_map
        self._normalized_ts_code_map = normalized_ts_code_map
        self._cache_time = now_ts

    @staticmethod
    def normalize_index_name(name: str) -> str:
        """标准化指数名称，提升导航名称与表内名称的匹配成功率。"""
        if not name:
            return ""

        normalized = str(name).strip().replace(" ", "")
        for token in ("（", "）", "(", ")", "-", "_", ".", "·", "成份", "成分", "指数", "指"):
            normalized = normalized.replace(token, "")
        return normalized

    def resolve_ts_code(self, index_name_or_code: str) -> str:
        """
        根据指数名称或代码解析标准 ts_code。

        Args:
            index_name_or_code: 指数名称、纯数字代码或 ts_code

        Returns:
            str: 标准 ts_code（如 399006.SZ），无法解析时原样返回
        """
        if not index_name_or_code:
            return index_name_or_code

        if '.' in index_name_or_code:
            code_part, market_part = index_name_or_code.split('.', 1)
            if market_part.upper() in {'SH', 'SZ', 'BJ'}:
                return f"{code_part}.{market_part.upper()}"

        self._load_index_maps()
        if index_name_or_code in self._name_ts_code_map:
            return self._name_ts_code_map[index_name_or_code]

        normalized_input = self.normalize_index_name(index_name_or_code)
        if normalized_input in self._normalized_ts_code_map:
            return self._normalized_ts_code_map[normalized_input]

        # 名称不完全一致时，尝试模糊匹配（如“科创板指”匹配“上证科创板50成份指数”）
        candidates = [
            (normalized_name, ts_code)
            for normalized_name, ts_code in self._normalized_ts_code_map.items()
            if normalized_name
            and (normalized_input in normalized_name or normalized_name in normalized_input)
        ]
        if candidates:
            candidates.sort(key=lambda item: len(item[0]))
            return candidates[0][1]

        if index_name_or_code.isdigit() and len(index_name_or_code) == 6:
            if index_name_or_code.startswith('000'):
                return f"{index_name_or_code}.SH"
            if index_name_or_code.startswith('399'):
                return f"{index_name_or_code}.SZ"
            if index_name_or_code.startswith(('899', '884')):
                return f"{index_name_or_code}.BJ"

        return index_name_or_code

    # ------------------------------------------------------------------
    # 分类过滤
    # ------------------------------------------------------------------

    @staticmethod
    def _ts_code_to_file_name(ts_code: str) -> str | None:
        """将 ts_code 转换为通达信文件主名（如 000001.SH -> sh000001）。"""
        if not ts_code or '.' not in ts_code:
            return None
        symbol, _, market_suffix = ts_code.partition('.')
        market = market_suffix.lower()
        if market not in INDEX_CODE_PREFIXES:
            return None
        return f"{market}{symbol}"

    def _query_category_file_names(self, category: str) -> set:
        """查询指定 category 的指数文件主名集合。数据库不可用时返回空集合。"""
        file_names = set()
        if not self._db_manager:
            return file_names

        from sqlalchemy import text

        session = self._db_manager.get_session()
        if not session:
            return file_names

        try:
            sql = text("SELECT ts_code FROM index_basic WHERE category = :category_name")
            result = session.execute(sql, {"category_name": category})
            for row in result:
                file_name = self._ts_code_to_file_name(row[0])
                if file_name:
                    file_names.add(file_name)
            with contextlib.suppress(Exception):
                result.close()
        except DataException as e:
            logger.warning(f"按分类过滤指数时查询数据库失败: {e}")
            return set()

        return file_names

    def _query_excluded_file_names(self) -> set:
        """查询需要在沪/深指数总览中排除的创业板/科创板指数。"""
        file_names = set()
        if not self._db_manager:
            return file_names

        from sqlalchemy import text

        session = self._db_manager.get_session()
        if not session:
            return file_names

        try:
            sql = text("SELECT ts_code FROM index_basic WHERE category IN ('创业板', '科创板')")
            result = session.execute(sql)
            for row in result:
                file_name = self._ts_code_to_file_name(row[0])
                if file_name:
                    file_names.add(file_name)
            with contextlib.suppress(Exception):
                result.close()
        except DataException as e:
            logger.warning(f"构建创业板/科创板排除列表失败: {e}")
            return set()

        return file_names

    # ------------------------------------------------------------------
    # 最新行情
    # ------------------------------------------------------------------

    def _iter_index_files(self, market_filter: str | None):
        """按市场枚举指数 .day 文件，产出 (market, symbol, file_path)。"""
        if self._tdx_data_path is None:
            return

        markets = [market_filter] if market_filter in INDEX_CODE_PREFIXES else list(INDEX_CODE_PREFIXES)
        for market in markets:
            directory = self._tdx_data_path / market / 'lday'
            if not directory.exists():
                continue
            for file_path in sorted(directory.glob(f"{market}*.day")):
                symbol = file_path.stem[2:]
                if symbol.startswith(INDEX_CODE_PREFIXES[market]):
                    yield market, symbol, file_path

    def list_index_quotes(
        self, market_filter: str | None = None, category_filter: str | None = None
    ) -> List[Dict]:
        """
        枚举指数并读取最新行情。

        Args:
            market_filter: 'sh' / 'sz' / 'bj'，None 表示全部
            category_filter: index_basic.category，None 表示不过滤

        Returns:
            list: 原始行情字典（date/ts_code/name/open/high/low/close/preclose/
                  volume/amount/change/pct_chg/amplitude）
        """
        self._load_index_maps()

        allowed = self._query_category_file_names(category_filter) if category_filter else None
        excluded = (
            self._query_excluded_file_names()
            if category_filter is None and market_filter in {'sh', 'sz'}
            else set()
        )

        quotes = []
        for market, symbol, file_path in self._iter_index_files(market_filter):
            file_name = file_path.stem
            if allowed is not None and file_name not in allowed:
                continue
            if file_name in excluded:
                continue

            quote = read_latest_quote(file_path)
            if quote is None:
                continue

            quotes.append({
                'date': quote.date,
                'ts_code': f"{symbol}.{market.upper()}",
                'name': self._file_name_map.get(file_name, symbol),
                'open': quote.open,
                'high': quote.high,
                'low': quote.low,
                'close': quote.close,
                'preclose': quote.preclose,
                'volume': quote.volume,
                'amount': quote.amount,
                'change': quote.change,
                'pct_chg': quote.pct_chg,
                'amplitude': quote.amplitude,
            })

        return quotes
