#!/usr/bin/env python3

"""除权除息事件（现金分红 / 送转 / 配股）的记账模型。

口径边界（撮合内核唯一口径，禁止混用）
--------------------------------------
* 信号 / 指标：后复权价（DataFetcher ``hfq`` 出口）；
* 撮合价、涨跌停判定、成本：原始价（``raw_open`` / ``raw_close``）；
* 权益估值：``总权益 = 现金 + Σ(持仓股数 × 原始收盘价)``，其中现金已包含
  除权日计提的现金分红。

因此本模块**只在原始价口径下使用**：把后复权价与现金分红同时计入就是
双重计入（后复权价本身已按除权事件上调过）。反过来，若只把分红计入现金
而不调整送转股数，10 送 10 的标的会出现「价格腰斩、股数不变」的虚假亏损；
送转必须同步增加股数，成本总额不变。

分红计入时点
------------
现金分红在**除权日**按 ``每股派息 × 持仓股数`` 计入现金（记为应收分红），
而不是等到**派息日**到账。理由：除权日开盘价已经扣除了派息，权益必须在
同一交易日对冲；若等派息日（通常晚数周）才入账，期间净值会被系统性压低。
派息日只影响现金到账时间，两种口径的差异 = 除权日至派息日之间的现金占用，
量级为 ``每股派息 × 持仓 × 占用天数``（真实数据中通常远小于 1% 净值）。

税率
----
``dividend_tax_rate`` 默认 0（不扣税），可配置。未建模 A 股按持股期限的
差异化红利税（持股 ≤1 年 20%、1~2 年 10%、>2 年免税），统一按单一税率近似。

送转
----
按每股送转股数增加持仓、成本总额不变；不足 1 股的零股向下取整（真实券商
可累积到下一次派发或折现，此处简化为舍去）。

配股
----
按配股比例认购、按配股价扣减现金。现金不足时由 ``rights_policy`` 显式选择，
不会静默跳过：

* ``'partial'``（默认）：按现金可承受的股数部分认购，未认购部分写入日志；
* ``'abandon'``：完全放弃本次配股，未认购部分写入日志。

数据源
------
只读本地分红快照 ``data/local_store/factors/stock_dividend.parquet``
（列与 ``stock_dividend`` 表一致），运行时主路径不发起远程查询；
指数无分红/送转，按 ``is_index_code`` 显式排除。

同日多笔除权事件
----------------
同一标的同一除权日若有多条记录，按经济口径**加总**（现金分红相加、送转
比例相加、配股比例相加、配股成本按比例加权），而不是把事件因子相乘。
真实快照中同一 ``(ts_code, ex_date)`` 不应出现重复记录；加总口径保证
「送转 + 分红」同日发生时的净值中性，不依赖因子乘积的近似。

验证口径（总闸门）
------------------
本模块的正确性基准是「引擎权益与独立现金账本逐日一致」：独立账本权益 =
买入后剩余现金 + Σ已计提现金分红 + 持仓股数 × 原始收盘价，本次实测
max_rel = 0。

**禁止**拿「后复权买入持有」净值作为现金口径回测的正确性基准：后复权
因子按事件累乘 ``1/f``，隐含「分红在理论除权价再投资」，与现金口径在
数学上不可比。同一区间实测两者 max_rel 为 37.63%（000002.SZ）与
5.25%（600036.SH），这是分红再投资的经济价值、不是记账缺陷；把分红按
理论除权价再投资后与后复权残差仅 0.002% / 0.95%，进一步佐证差异来源。

已知简化（如实披露）
--------------------
* 现金分红统一按单一税率 ``dividend_tax_rate``（默认 0）计提，未建模按
  持股期限分档（≤1 年 20%、1~2 年 10%、>2 年免税），高股息标的的税后
  收益因此仍被略高估；
* 配股现金不足默认 ``partial``（按现金能力部分认购），可切 ``abandon``；
* 快照缺少派息日字段，现金分红统一按除权日计提应收，未建模派息日到账。
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict

import polars as pl
from loguru import logger

from src.utils.code_converter import is_index_code

#: 合法的配股现金不足处置策略
RIGHTS_POLICIES = ('partial', 'abandon')


def _as_date(value: Any) -> date | None:
    """把 parquet 日期值（date/datetime/字符串）归一为 ``date``。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(text[:10] if '-' in text else text, fmt).date()
        except ValueError:
            continue
    return None


def _non_negative(value: Any) -> float:
    """数值归一到非负有限 float：None / NaN / inf / 负数 / 非数值统一为 0.0。

    ``share_div`` 的 0.0 是「无送转」的有效语义，不是缺失；此函数只把非法
    值归零，不做「0 视为空」的判断，避免把现金分红方案误判为无事件。
    """
    if value is None:
        return 0.0
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(number) or number <= 0:
        return 0.0
    return number


@dataclass(frozen=True, slots=True)
class CorporateAction:
    """单个除权日的合并事件（同日多条记录已按经济口径加总）。"""

    ex_date: date
    #: 每股现金分红（税前）
    cash_per_share: float = 0.0
    #: 每股送转股数（0.5 表示 10 送 5）
    share_ratio: float = 0.0
    #: 每股配股数（0.1 表示 10 配 1）
    rights_ratio: float = 0.0
    #: 配股价（同日多笔时按配股比例加权）
    rights_price: float = 0.0

    @property
    def has_cash(self) -> bool:
        return self.cash_per_share > 0

    @property
    def has_share(self) -> bool:
        return self.share_ratio > 0

    @property
    def has_rights(self) -> bool:
        return self.rights_ratio > 0


class CorporateActionSchedule:
    """按标的与除权日组织的除权事件表（只读，回测内共享）。"""

    def __init__(self, events: Mapping[str, Iterable[CorporateAction]]):
        self._events: Dict[str, tuple[CorporateAction, ...]] = {
            code: tuple(sorted(items, key=lambda item: item.ex_date))
            for code, items in events.items()
            if items
        }

    @classmethod
    def from_frames(cls, frames: Mapping[str, pl.DataFrame] | None) -> CorporateActionSchedule:
        """从「标的 → 分红记录 DataFrame」构建事件表。

        列与 ``stock_dividend`` 表一致；缺失的可选列按 0 处理。指数代码直接
        跳过（指数没有分红/送转，库里同号指数记录与个股同源）。
        """
        if not frames:
            return cls({})
        parsed: Dict[str, tuple[CorporateAction, ...]] = {}
        for code, frame in frames.items():
            if frame is None or frame.is_empty() or is_index_code(code):
                continue
            events = cls._parse_frame(frame)
            if events:
                parsed[code] = events
        return cls(parsed)

    @staticmethod
    def _parse_frame(frame: pl.DataFrame) -> tuple[CorporateAction, ...]:
        """解析单标的分红记录，按 ex_date 合并同日多笔事件。"""
        if 'ex_date' not in frame.columns:
            return ()
        optional = ('cash_div', 'share_div', 'rights_issue_ratio', 'rights_issue_price')
        columns = ['ex_date'] + [col for col in optional if col in frame.columns]
        cash_by_date: Dict[date, float] = {}
        share_by_date: Dict[date, float] = {}
        rights_ratio_by_date: Dict[date, float] = {}
        rights_cost_by_date: Dict[date, float] = {}

        for row in frame.select(columns).iter_rows(named=True):
            ex_date = _as_date(row.get('ex_date'))
            if ex_date is None:
                continue
            cash = _non_negative(row.get('cash_div'))
            share = _non_negative(row.get('share_div'))
            rights_ratio = _non_negative(row.get('rights_issue_ratio'))
            rights_price = _non_negative(row.get('rights_issue_price'))
            if cash <= 0 and share <= 0 and rights_ratio <= 0:
                continue
            cash_by_date[ex_date] = cash_by_date.get(ex_date, 0.0) + cash
            share_by_date[ex_date] = share_by_date.get(ex_date, 0.0) + share
            if rights_ratio > 0:
                rights_ratio_by_date[ex_date] = (
                    rights_ratio_by_date.get(ex_date, 0.0) + rights_ratio)
                rights_cost_by_date[ex_date] = (
                    rights_cost_by_date.get(ex_date, 0.0) + rights_ratio * rights_price)

        events = []
        for ex_date, cash in cash_by_date.items():
            rights_ratio = rights_ratio_by_date.get(ex_date, 0.0)
            rights_price = (
                rights_cost_by_date.get(ex_date, 0.0) / rights_ratio
                if rights_ratio > 0 else 0.0)
            events.append(CorporateAction(
                ex_date=ex_date,
                cash_per_share=cash,
                share_ratio=share_by_date.get(ex_date, 0.0),
                rights_ratio=rights_ratio,
                rights_price=rights_price,
            ))
        return tuple(sorted(events, key=lambda item: item.ex_date))

    def is_empty(self) -> bool:
        return not self._events

    def codes(self) -> tuple[str, ...]:
        return tuple(self._events)

    def events_for(self, code: str) -> tuple[CorporateAction, ...]:
        return self._events.get(code, ())


def load_local_dividend_frames(
    codes: Iterable[str], config: Any = None,
) -> dict[str, pl.DataFrame]:
    """从本地分红快照读取各标的的分红记录（只读，失败时返回空表）。

    运行时主路径只走 ``LocalFactorStore``，不做远程/数据库回退：分红缺失
    在回测侧等价于「无除权事件」，不会阻塞回测，但也不会静默补全。
    """
    from src.data.local_store import get_local_factor_store

    try:
        store = get_local_factor_store(config)
    except (ImportError, OSError, ValueError, TypeError) as exc:
        logger.warning(f"本地分红快照不可用，跳过除权事件: {exc}")
        return {}

    frames: dict[str, pl.DataFrame] = {}
    for code in codes:
        if not code or is_index_code(code):
            continue
        try:
            frame = store.dividends(code)
        except (OSError, ValueError, TypeError) as exc:
            logger.warning(f"读取 {code} 分红快照失败，跳过: {exc}")
            continue
        if frame is not None and not frame.is_empty():
            frames[code] = frame
    return frames
