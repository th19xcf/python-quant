#!/usr/bin/env python3

"""
指数数据服务

负责指数名称/ts_code 解析、按分类过滤，以及从通达信日线文件
枚举指数最新行情。UI 层只负责展示格式化，不再直接执行 SQL。
"""

import contextlib
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

from loguru import logger

from src.data.tdx_day_parser import read_latest_quote
from src.utils.code_converter import is_index_code, to_index_ts_code, to_tdx_file_stem
from src.utils.exceptions import DataException


class IndexService:
    """指数元数据与最新行情查询服务。"""

    def __init__(self, tdx_data_path: str | None, db_manager: Any = None,
                 cache_ttl_seconds: int = 300) -> None:
        self._tdx_data_path = Path(tdx_data_path) if tdx_data_path else None
        self._db_manager = db_manager
        self._cache_ttl_seconds = cache_ttl_seconds
        self._cache_time = 0.0
        self._file_name_map: dict[str, str] = {}
        self._name_ts_code_map: dict[str, str] = {}
        self._normalized_ts_code_map: dict[str, str] = {}
        self._local_basic_loaded = False
        self._local_basic_cache: Any = None

    def _local_basic(self) -> Any:
        """本地 ``meta/index_basic.parquet`` 快照；缺失时返回 None。"""
        if self._local_basic_loaded:
            return self._local_basic_cache
        self._local_basic_loaded = True
        try:
            from src.data.local_store.store import resolve_store_root  # noqa: PLC0415
            from src.utils.config import get_config  # noqa: PLC0415

            path = Path(resolve_store_root(get_config())) / 'meta' / 'index_basic.parquet'
            if path.is_file():
                import polars as pl  # noqa: PLC0415

                self._local_basic_cache = pl.read_parquet(path)
                logger.info(
                    f"从本地快照加载了 {self._local_basic_cache.height} 条指数基础信息")
        # 兜底理由：快照不可用时回退远程 index_basic，不阻断服务
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"读取本地 index_basic 快照失败: {exc}")
        return self._local_basic_cache

    # ------------------------------------------------------------------
    # 指数名称 / ts_code 解析
    # ------------------------------------------------------------------

    def _load_index_maps(self) -> None:
        """从 index_basic 加载名称映射（带 TTL 缓存，失败时回退上次结果）。"""
        now_ts = time.time()
        if now_ts - self._cache_time <= self._cache_ttl_seconds:
            return

        file_name_map: dict[str, str] = {}
        name_ts_code_map: dict[str, str] = {}
        normalized_ts_code_map: dict[str, str] = {}

        local = self._local_basic()
        if local is not None:
            for ts_code, name in local.select(['ts_code', 'name']).iter_rows():
                if not ts_code or not name or not is_index_code(ts_code):
                    continue
                try:
                    file_name = to_tdx_file_stem(ts_code)
                except (TypeError, ValueError):
                    continue
                file_name_map[file_name] = name
                name_ts_code_map[name] = ts_code
                normalized_ts_code_map[self.normalize_index_name(name)] = ts_code
            self._file_name_map = file_name_map
            self._name_ts_code_map = name_ts_code_map
            self._normalized_ts_code_map = normalized_ts_code_map
            self._cache_time = now_ts
            logger.info(f"从本地快照加载了 {len(file_name_map)} 个指数名称")
            return

        try:
            if self._db_manager:
                from sqlalchemy import text

                session = self._db_manager.get_session()
                if session:
                    # 指数口径统一委托 code_converter.is_index_code，
                    # 不再在 SQL / Python 里维护一份前缀表
                    sql = text(
                        "SELECT ts_code, name FROM index_basic WHERE name IS NOT NULL"
                    )

                    result = session.execute(sql)
                    for row in result:
                        ts_code, name = row[0], row[1]
                        if not ts_code or not name or not is_index_code(ts_code):
                            continue

                        try:
                            file_name = to_tdx_file_stem(ts_code)
                        except (TypeError, ValueError):
                            continue
                        file_name_map[file_name] = name

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
            try:
                return to_index_ts_code(index_name_or_code)
            except (TypeError, ValueError):
                return index_name_or_code

        return index_name_or_code

    # ------------------------------------------------------------------
    # 分类过滤
    # ------------------------------------------------------------------

    @staticmethod
    def _ts_code_to_file_name(ts_code: str) -> str | None:
        """将 ts_code 转换为通达信文件主名（如 000001.SH -> sh000001）。"""
        if not is_index_code(ts_code):
            return None
        try:
            return to_tdx_file_stem(ts_code)
        except (TypeError, ValueError):
            return None

    def _query_category_file_names(self, category: str) -> set[str]:
        """查询指定 category 的指数文件主名集合。数据库不可用时返回空集合。"""
        file_names: set[str] = set()
        local = self._local_basic()
        if local is not None:
            import polars as pl  # noqa: PLC0415

            codes = local.filter(
                pl.col('category') == category)['ts_code'].to_list()
            return {
                name for name in (self._ts_code_to_file_name(c) for c in codes)
                if name
            }
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

    def _query_excluded_file_names(self) -> set[str]:
        """查询需要在沪/深指数总览中排除的创业板/科创板指数。"""
        file_names: set[str] = set()
        local = self._local_basic()
        if local is not None:
            import polars as pl  # noqa: PLC0415

            codes = local.filter(pl.col('category').is_in(
                ['创业板', '科创板']))['ts_code'].to_list()
            return {
                name for name in (self._ts_code_to_file_name(c) for c in codes)
                if name
            }
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

    def _iter_index_files(self, market_filter: str | None) -> Iterator[tuple[str, str, Path]]:
        """按市场枚举指数 .day 文件，产出 (market, symbol, file_path)。"""
        if self._tdx_data_path is None:
            return

        all_markets = ('sh', 'sz', 'bj')
        markets = (cast(str, market_filter),) if market_filter in all_markets else all_markets
        for market in markets:
            directory = self._tdx_data_path / market / 'lday'
            if not directory.exists():
                continue
            for file_path in sorted(directory.glob(f"{market}*.day")):
                symbol = file_path.stem[2:]
                # 指数判定统一委托 code_converter，避免各处维护前缀表
                if is_index_code(f"{symbol}.{market.upper()}"):
                    yield market, symbol, file_path

    def list_index_quotes(
        self, market_filter: str | None = None, category_filter: str | None = None
    ) -> list[dict[str, Any]]:
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

        quotes: list[dict[str, Any]] = []
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
