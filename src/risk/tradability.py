#!/usr/bin/env python3

"""可交易性过滤：选股打分之后、建仓名单确定之前的「根本买不到」前置排除。

与撮合层（``src/backtest/engine/execution_core.py``）的边界
---------------------------------------------------------
撮合层负责「订单发出后能否成交」，本模块只负责「选股/建仓阶段先别把这只
票放进名单」。两者互不替代，也禁止把这里改写成第二份撮合实现。

过滤项与判定依据（各自独立开关）
--------------------------------
1. 停牌 —— ``meta/limit/daily/limit_daily_YYYYMM.parquet`` 的 ``suspended``；
2. ST/*ST —— 同一张表的 ``st_status == 'st'``；
3. 退市整理期 —— ``meta/name_history.parquet`` 的 ``name`` 含「退」，且
   ``start_date <= 交易日 <= end_date``（A 股整理期简称形如「XX退」/「退市XX」）；
4. 上市不足 N 个交易日 —— 同一张涨跌停表的 ``listing_days < N``（N 默认 60）；
5. 当日涨停封板（买不到）—— ``one_word_up or close_at_up``；
6. 当日跌停 —— ``one_word_down or close_at_down``；
7. 成交额下限 —— 调用方传入的窗口均额（元），低于阈值剔除；
8. 换手下限 —— ``factors/stock_turnover.parquet`` 的 ``turn``，低于阈值剔除。

所有开关关闭或总开关关闭时 **不读取任何文件**，输出空剔除集，调用方结果
与接入前逐值一致。数据表缺失、表内无该代码、或当日有效截面小于
``min_universe`` 时整体跳过对应项并 warning，返回空集（不放行「静默清空」）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping

import polars as pl
from loguru import logger

DEFAULT_STORE = Path(__file__).resolve().parents[2] / 'data' / 'local_store'

_ITEM_LABELS = {
    'suspend': '停牌',
    'st': 'ST',
    'delisting': '退市整理期',
    'new_listing': '上市不足交易日',
    'limit_up': '涨停封板',
    'limit_down': '跌停',
    'min_amount': '成交额下限',
    'min_turnover': '换手下限',
}


@dataclass(frozen=True)
class TradabilityConfig:
    """可交易性过滤配置：总开关 + 8 个独立子开关。

    默认（研究回测层）：``enabled=False``，全部关闭，行为与现状逐值等价。
    实盘建议层使用 :meth:`live_default` 打开总开关与全部子开关。
    """

    enabled: bool = False
    suspend: bool = True
    st: bool = True
    delisting: bool = True
    new_listing: bool = True
    #: 上市不足多少个交易日算「新股」
    new_listing_days: int = 60
    #: 当日涨停封板（收盘封死/一字板）→ 买不到
    limit_up: bool = True
    #: 当日跌停（次日仍可能受阻）
    limit_down: bool = True
    #: 近 window 日均成交额下限（元）；None = 不启用
    min_amount: float | None = None
    #: 当日换手率下限（小数，0.002 = 0.2%）；None = 不启用
    min_turnover: float | None = None
    #: 成交额均额窗口（交易日）
    amount_window: int = 20
    #: 当日有效截面少于该只数时整体跳过并 warning
    min_universe: int = 30

    def switch_enabled(self) -> bool:
        return bool(self.enabled and any((
            self.suspend, self.st, self.delisting, self.new_listing,
            self.limit_up, self.limit_down,
            self.min_amount is not None, self.min_turnover is not None,
        )))

    def validate(self) -> None:
        if self.new_listing_days < 0:
            raise ValueError(f'new_listing_days 不能为负：{self.new_listing_days}')
        if self.min_amount is not None and self.min_amount < 0:
            raise ValueError(f'min_amount 不能为负：{self.min_amount}')
        if self.min_turnover is not None and not 0.0 <= self.min_turnover <= 1.0:
            raise ValueError(f'min_turnover 必须是 0~1 的小数：{self.min_turnover}')
        if self.amount_window < 1:
            raise ValueError(f'amount_window 必须为正：{self.amount_window}')
        if self.min_universe < 1:
            raise ValueError(f'min_universe 必须为正：{self.min_universe}')

    @classmethod
    def live_default(cls) -> TradabilityConfig:
        """实盘建议层默认：打开过滤，先把买不到的票筛掉。"""
        return cls(enabled=True)

    @classmethod
    def research_default(cls) -> TradabilityConfig:
        """研究回测层默认：关闭，保持与历史结果可比。"""
        return cls(enabled=False)


@dataclass(frozen=True)
class ItemStatus:
    """单个过滤项在某个交易日的执行状态。"""

    item: str
    applied: bool
    reason: str = ''
    universe: int = 0
    excluded: int = 0

    @property
    def label(self) -> str:
        return _ITEM_LABELS.get(self.item, self.item)

    def note(self) -> str:
        if self.applied:
            return f'{self.label}剔除 {self.excluded}/{self.universe} 只'
        return f'{self.label}跳过（{self.reason}）'


@dataclass(frozen=True)
class TradabilityResult:
    """单个交易日的可交易性剔除结果（合并 + 逐项）。"""

    day: date
    codes: frozenset[str]
    by_item: Mapping[str, frozenset[str]] = field(default_factory=dict)
    statuses: tuple[ItemStatus, ...] = ()

    def notes(self) -> list[str]:
        return [s.note() for s in self.statuses if not s.applied]

    def summary(self) -> dict[str, int]:
        return {item: len(codes) for item, codes in self.by_item.items()}


class TradabilityFilter:
    """按交易日给出「不可交易」名单；只在启用时读取数据表。"""

    def __init__(self, config: TradabilityConfig,
                 store_root: str | Path | None = None) -> None:
        config.validate()
        self.config = config
        self.enabled = config.switch_enabled()
        self._store = Path(store_root) if store_root else DEFAULT_STORE
        self._limit_month = ''
        self._limit_rows: dict[tuple[str, str], dict[str, Any]] = {}
        self._limit_missing = False
        self._names: dict[str, list[tuple[date, date | None, str]]] | None = None
        self._names_missing = False

    # ------------------------------------------------------------------
    # 数据装载（按开关懒加载）
    # ------------------------------------------------------------------
    def _load_limit_month(self, month: str) -> None:
        if month == self._limit_month:
            return
        self._limit_month = month
        self._limit_rows = {}
        path = (self._store / 'meta' / 'limit' / 'daily'
                / f'limit_daily_{month}.parquet')
        if not path.exists():
            self._limit_missing = True
            return
        self._limit_missing = False
        frame = pl.read_parquet(path)
        self._limit_rows = {
            (str(row['trade_date'])[:10], str(row['ts_code'])): row
            for row in frame.iter_rows(named=True)
        }

    def _limit_row(self, day: date, code: str) -> dict[str, Any] | None:
        self._load_limit_month(day.strftime('%Y%m'))
        return self._limit_rows.get((day.isoformat(), code))

    def _load_names(self) -> None:
        if self._names is not None or self._names_missing:
            return
        path = self._store / 'meta' / 'name_history.parquet'
        if not path.exists():
            self._names_missing = True
            logger.warning('[tradability] 名称历史缺失，退市整理期过滤整体跳过：{}', path)
            return
        frame = pl.read_parquet(
            path, columns=['ts_code', 'name', 'start_date', 'end_date'])
        out: dict[str, list[tuple[date, date | None, str]]] = {}
        for code, name, start, end in frame.iter_rows():
            out.setdefault(str(code), []).append((start, end, str(name)))
        self._names = out

    def _load_turnover(self, day: date) -> dict[str, float] | None:
        path = self._store / 'factors' / 'stock_turnover.parquet'
        if not path.exists():
            logger.warning('[tradability] 换手表缺失，换手下限整体跳过：{}', path)
            return None
        frame = (pl.scan_parquet(path)
                 .filter(pl.col('trade_date') == day)
                 .select(['ts_code', 'turn']).collect())
        return {str(c): float(v) for c, v in frame.iter_rows() if v is not None}

    # ------------------------------------------------------------------
    # 主入口
    # ------------------------------------------------------------------
    def filter_day(self, day: date, codes: Iterable[str],  # noqa: C901 - 待拆分，见审查报告 7.4
                   amount: Mapping[str, float] | None = None,
                   turnover: Mapping[str, float] | None = None
                   ) -> TradabilityResult:
        """返回当日应剔除的代码集合；未启用/截面过小时返回空集。"""
        universe = set(codes)
        if not self.enabled:
            return TradabilityResult(day=day, codes=frozenset())

        if len(universe) < self.config.min_universe:
            logger.warning(
                '[tradability] {} 有效截面 {} < {}，整体跳过（不剔除）',
                day, len(universe), self.config.min_universe)
            return TradabilityResult(day=day, codes=frozenset())

        by_item: dict[str, frozenset[str]] = {}
        statuses: list[ItemStatus] = []
        use_table = any((self.config.suspend, self.config.st,
                         self.config.new_listing, self.config.limit_up,
                         self.config.limit_down))

        if use_table:
            self._load_limit_month(day.strftime('%Y%m'))
            if self._limit_missing or not self._limit_rows:
                logger.warning(
                    '[tradability] {} 涨跌停/停牌表缺失（{}），'
                    '停牌/ST/新股/涨跌停五项整体跳过',
                    day, self._limit_month)
                rows = None
            else:
                rows = self._limit_rows
        else:
            rows = None

        table_items = (
            ('suspend', self.config.suspend, lambda r: bool(r.get('suspended'))),
            ('st', self.config.st, lambda r: str(r.get('st_status')) == 'st'),
            ('new_listing', self.config.new_listing,
             lambda r: (r.get('listing_days') is not None
                        and int(r['listing_days']) < self.config.new_listing_days)),
            ('limit_up', self.config.limit_up,
             lambda r: bool(r.get('one_word_up') or r.get('close_at_up'))),
            ('limit_down', self.config.limit_down,
             lambda r: bool(r.get('one_word_down') or r.get('close_at_down'))),
        )
        if rows is not None:
            for item, on, predicate in table_items:
                if not on:
                    continue
                hit = {c for c in universe
                       if (row := rows.get((day.isoformat(), c))) is not None
                       and predicate(row)}
                by_item[item] = frozenset(hit & universe)
                statuses.append(ItemStatus(item, True, universe=len(universe),
                                           excluded=len(by_item[item])))
        elif any(on for _, on, _ in table_items):
            for item, on, _ in table_items:
                if on:
                    statuses.append(ItemStatus(item, False, reason='涨跌停/停牌表缺失'))

        if self.config.delisting:
            self._load_names()
            if self._names is None:
                statuses.append(ItemStatus('delisting', False, reason='名称历史缺失'))
            else:
                hit = set()
                for code in universe:
                    for start, end, name in self._names.get(code, ()):
                        if '退' in name and start <= day and (end is None or end >= day):
                            hit.add(code)
                            break
                by_item['delisting'] = frozenset(hit)
                statuses.append(ItemStatus('delisting', True, universe=len(universe),
                                           excluded=len(hit)))

        if self.config.min_amount is not None:
            if amount is None:
                statuses.append(ItemStatus('min_amount', False, reason='调用方未提供成交额'))
            else:
                valid = {c: float(v) for c, v in amount.items()
                         if c in universe and v is not None}
                if len(valid) < self.config.min_universe:
                    logger.warning(
                        '[tradability] {} 成交额有效 {} < {}，成交额下限整体跳过',
                        day, len(valid), self.config.min_universe)
                    statuses.append(ItemStatus('min_amount', False,
                                               reason=f'有效成交额仅 {len(valid)} 只'))
                else:
                    hit = {c for c, v in valid.items() if v < self.config.min_amount}
                    by_item['min_amount'] = frozenset(hit)
                    statuses.append(ItemStatus('min_amount', True,
                                               universe=len(valid), excluded=len(hit)))

        if self.config.min_turnover is not None:
            values = turnover if turnover is not None else self._load_turnover(day)
            if values is None:
                statuses.append(ItemStatus('min_turnover', False, reason='换手表缺失'))
            else:
                valid = {c: float(v) for c, v in values.items()
                         if c in universe and v is not None}
                if len(valid) < self.config.min_universe:
                    logger.warning(
                        '[tradability] {} 换手有效 {} < {}，换手下限整体跳过',
                        day, len(valid), self.config.min_universe)
                    statuses.append(ItemStatus('min_turnover', False,
                                               reason=f'有效换手仅 {len(valid)} 只'))
                else:
                    hit = {c for c, v in valid.items() if v < self.config.min_turnover}
                    by_item['min_turnover'] = frozenset(hit)
                    statuses.append(ItemStatus('min_turnover', True,
                                               universe=len(valid), excluded=len(hit)))

        merged: set[str] = set()
        for codes_hit in by_item.values():
            merged |= codes_hit
        return TradabilityResult(day=day, codes=frozenset(merged),
                                 by_item=by_item, statuses=tuple(statuses))


def latest_mean_amount(dates: Any, values: Any, day: date, window: int) -> float | None:
    """取 ``day`` 及之前 ``window`` 个有效成交额的均值；不足窗口返回 None。"""
    import bisect

    import numpy as np

    position = bisect.bisect_right(dates, np.datetime64(day)) - 1
    if position < window - 1:
        return None
    window_vals = values[position - window + 1:position + 1]
    window_vals = window_vals[np.isfinite(window_vals)]
    if len(window_vals) != window:
        return None
    return float(window_vals.mean())
