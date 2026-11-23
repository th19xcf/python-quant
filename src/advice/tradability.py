#!/usr/bin/env python3

"""实盘建议层的可交易性过滤适配器（默认开启）。

只做两件事：把当日候选代码与成交额交给 :class:`src.risk.tradability.TradabilityFilter`，
并缓存同一交易日的重复调用。过滤口径、数据表与开关全部在核心模块，
本层不复制任何判定逻辑。
"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping

from src.advice.config import TradabilityFilterConfig
from src.risk.tradability import TradabilityFilter, TradabilityResult


class TradabilityLayer:
    def __init__(self, config: TradabilityFilterConfig, store_root: Any) -> None:
        self._filter = TradabilityFilter(config, store_root)
        self._cache: dict[tuple, TradabilityResult] = {}

    def status_for(self, day: date, codes: list[str],
                   amount: Mapping[str, float] | None = None) -> TradabilityResult:
        key = (day, frozenset(codes))
        if key not in self._cache:
            self._cache[key] = self._filter.filter_day(day, codes, amount=amount)
        return self._cache[key]
