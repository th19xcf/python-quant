#!/usr/bin/env python3

"""按日期求值的洁净集准入闸门（替代静态白名单）。

设计要点
--------
1. **判定按日期实时求值**：``eligible(ts_code, date)`` 回答的是"该标的在
   ``date`` 当天是否有可用数据、是否命中当日 ST"，而不是"该标的是否曾经
   出过问题"。2019 年停更的标的在 2018 年仍然可用，不会被提前剔除。
2. **缺陷分两类**：
   - 结构性缺陷（永久）：无本地行情文件、内容不一致、重复除权键、
     复权因子缺失/不可逆。这类缺陷与日期无关。
   - 时效性缺陷（按日）：源端停更、因子末端落后、停牌/缺口。只在
     ``date`` 落入缺陷区间时剔除。
3. **ST 按区间判定**：``meta/st/st_intervals.parquet`` 覆盖的区间命中当天
   即不可选；区间结束（摘帽）后自动回归。区间表未覆盖的日期返回
   ``unknown``，默认放行并在 ``eligible`` 返回的 flags 里标
   ``pending_st_history``，不编造历史状态。
   该 flag 是**纯诊断标记**：全仓没有任何过滤逻辑读取它，唯一落点是快照
   ``meta/clean/eligibility_<as_of>.parquet`` 的 ``reasons`` 列，用来把
   「确认非 ST」与「未知」区分开；需要收紧时由调用方自行判定，本模块不新增
   过滤口径。
4. 三档视图：
   - ``full``      ：当天有行情的全部标的，不做质量/ST过滤；整只占位桩
                     （``placeholder_quote_codes.csv`` 中 ``full=true``）除外——
                     恒为 1.0 的平线不是数据，计入会让可用性视图虚高；
   - ``standard``  ：剔除结构性缺陷 + 当日 ST，容忍因子末端落后与历史缺口；
   - ``strict``    ：在 standard 之上再要求当日因子可用，并剔除除权事件存疑
                     （配股无第二源、跳变/事件不匹配、因子台账标记）。
   strict 默认只对非 ETF 基金挂 ``pending_fund_dividend``（默认 scope=``etf``）。
   可用 ``close_fund_dividend_pending_scope``（``none``/``etf``/``all``）按资产
   子类放开：``etf`` 只放 ``fund_basic.fund_type='ETF'`` 的基金；名单落本地
   缓存 ``meta/clean/fund_etf_codes.txt``，缺失或超过 7 天才连远程库刷新，
   刷新失败沿用旧缓存，两者都没有时降级为 ``none``（宁严不宽）。
5. 本模块只读本地列存，产出写入 ``meta/clean/``，幂等可重跑。

读侧用法::

    from src.data.quality.clean_set import eligible, eligible_set

    ok, reasons = eligible('600000.SH', '2026-11-03', tier='strict')
    codes = eligible_set('2026-11-03', tier='strict', data_type='stock')

    # 回测主循环：每个交易日取一次当日严格池
    for d in trading_dates:
        pool = eligible_set(d, tier='strict', data_type='stock')

首次调用会构建行情/因子日期索引（约 30-60 秒），之后同一进程内查询为
毫秒级。快照文件仅供查看，**不要**把 2026 年的快照当作历史某日的名单。
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
import threading
from bisect import bisect_left, bisect_right
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Literal

import numpy as np
import polars as pl
from loguru import logger

from src.data.adj_factor_calculator import calc_dividend_event_factor
from src.data.atomic_write import write_parquet_atomic, write_text_atomic
from src.data.quality.anomalies import iter_anomalies
from src.data.quality.code_migration import load_entity_index
from src.data.quality.suspension_union import SuspensionUnion, load_suspension_union
from src.utils.code_converter import is_fund_code, is_index_code

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ROOT = PROJECT_ROOT / 'data' / 'local_store'

ViewTier = Literal['full', 'standard', 'strict']
VIEWS: tuple[ViewTier, ...] = ('full', 'standard', 'strict')
VS_FULL_VIEWS: tuple[ViewTier, ...] = ('standard', 'strict')
DATA_TYPES: tuple[str, ...] = ('stock', 'index', 'fund')

#: 横截面打分的可用下限：10 分组每组约 30 只；低于 300 只时分组内样本过少，
#: 因子 IC / 分层收益的置信区间会显著变宽。该值可按研究口径调整。
MIN_CROSS_SECTION = 300

#: 月末采样中连续满足下限的月数
MIN_STREAK_MONTHS = 12

#: 数据源陈旧度容忍上限（交易日）。判定时点 ``as_of`` 改为由数据决定后，仍要
#: 能识别真实停摆：数据最新日落后于日历最新交易日不超过该值，属正常取数窗口
#: （当日行情尚未落盘），超过才判 ``stopped``。
MAX_SOURCE_LAG_TRADING_DAYS = 1

#: strict 归零保护阈值：上一快照 strict 只数超过该值而本次算得 0 时拒绝落盘，
#: 避免「判定时点超前导致全市场停更」被当成正常结果静默写出。
STRICT_ZERO_GUARD_MIN_PREV = 1000

#: 1991-1992 年拆细/送转比例未能外部确认的代码：1993-01-01 之前不参与
#: standard/strict（full 视图仅做数据检查，不受影响）。
PRE_1993_BREAK_CODES: frozenset[str] = frozenset({
    '600601.SH', '600651.SH', '600653.SH', '600654.SH',
    '600605.SH', '600606.SH', '600609.SH', '600616.SH', '600621.SH',
})
PRE_1993_BREAK_CUTOFF = date(1993, 1, 1)
_PRE_1993_BREAK_ORD = PRE_1993_BREAK_CUTOFF.toordinal()


def _pre_1993_break(info: CodeInfo, o: int) -> bool:
    """该标的是否落在“1993 年前拆细比例未确认”的排除区间。"""
    return info.ts_code in PRE_1993_BREAK_CODES and o < _PRE_1993_BREAK_ORD

#: 基金分红复权 pending 的开关范围：``none`` 保持现状（strict 剔除全部基金）；
#: ``etf`` 只放开 ``fund_basic.fund_type='ETF'`` 的基金；``all`` 放开全部基金。
#: 默认 ``etf``。``etf`` 的名单优先读本地缓存，缓存缺失或过期才连远程库刷新，
#: 刷新失败沿用旧缓存，两者都没有时降级为 ``none``（宁严不宽）。
FUND_DIVIDEND_PENDING_SCOPES: tuple[str, ...] = ('all', 'etf', 'none')
FUND_DIVIDEND_PENDING_SCOPE_ENV = 'QUANT_CLOSE_FUND_DIVIDEND_PENDING_SCOPE'
#: 旧的布尔开关环境变量，保留兼容：真值映射 all，假值映射 none。
FUND_DIVIDEND_PENDING_ENV = 'QUANT_CLOSE_FUND_DIVIDEND_PENDING'
_TRUTHY = frozenset({'1', 'true', 'yes', 'on'})

#: ETF 名单本地缓存（UTF-8、一行一个 ts_code）。回测只读该文件，避免每次
#: 实例化都连远程 MySQL，也保证离线与联网跑出同一个 strict 池。
FUND_ETF_CODES_CACHE = DEFAULT_ROOT / 'meta' / 'clean' / 'fund_etf_codes.txt'
#: 本地基金基础表快照；存在时优先于远程库与 txt 缓存。
FUND_BASIC_SNAPSHOT = DEFAULT_ROOT / 'meta' / 'fund_basic.parquet'
#: 缓存有效天数：过期后下次加载尝试连远程库刷新，失败则继续用旧缓存。
FUND_ETF_CODES_TTL_DAYS = 7


def _normalize_scope(value: str | None, *, strict: bool = True) -> str | None:
    """校验范围取值；``strict=False`` 用于环境变量，非法值告警后忽略。"""
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in FUND_DIVIDEND_PENDING_SCOPES:
        return text
    if strict:
        raise ValueError(
            f'未知基金 pending 开关范围: {value!r}，'
            f'应为 {FUND_DIVIDEND_PENDING_SCOPES}')
    logger.warning(
        f'[clean_set] 忽略未知的 {FUND_DIVIDEND_PENDING_SCOPE_ENV} 取值: {value!r}')
    return None


def _resolve_fund_pending_scope(
    scope: str | None = None,
    legacy: bool | None = None,
) -> str:
    """解析开关范围。

    优先级：显式 ``scope`` > 显式旧布尔 ``legacy``
    > 环境变量 ``QUANT_CLOSE_FUND_DIVIDEND_PENDING_SCOPE``
    > 环境变量 ``QUANT_CLOSE_FUND_DIVIDEND_PENDING`` > 默认 ``etf``。
    """
    explicit = _normalize_scope(scope)
    if explicit is not None:
        return explicit
    if legacy is not None:
        return 'all' if legacy else 'none'
    env_scope = _normalize_scope(
        os.getenv(FUND_DIVIDEND_PENDING_SCOPE_ENV), strict=False)
    if env_scope is not None:
        return env_scope
    env_legacy = os.getenv(FUND_DIVIDEND_PENDING_ENV)
    if env_legacy is not None:
        return 'all' if env_legacy.strip().lower() in _TRUTHY else 'none'
    return 'etf'


def _fund_etf_codes_from_db() -> set[str]:
    """只读远程库 ``fund_basic`` 的 ETF 代码（不做任何写入）。"""
    from sqlalchemy import text  # noqa: PLC0415 - 延迟导入，离线场景不加载驱动

    from src.database.db_manager import DatabaseManager  # noqa: PLC0415
    from src.utils.config import get_config  # noqa: PLC0415

    db_manager = DatabaseManager(get_config())
    db_manager.connect()
    try:
        with db_manager.engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT ts_code FROM fund_basic WHERE fund_type = 'ETF'"))
            return {str(row[0]).strip() for row in rows if row[0]}
    finally:
        db_manager.disconnect()


def _fund_etf_codes_from_parquet(path: Path | None = None) -> set[str] | None:
    """读本地 ``meta/fund_basic.parquet`` 的 ETF 代码；快照缺失返回 None。"""
    target = path or FUND_BASIC_SNAPSHOT
    if not target.is_file():
        return None
    try:
        frame = pl.read_parquet(target, columns=['ts_code', 'fund_type'])
    # 兜底理由：快照损坏时退回 txt 缓存与远程库，不阻断 clean_set
    except Exception as exc:  # noqa: BLE001
        logger.warning(f'[clean_set] fund_basic 快照读取失败: {exc}')
        return None
    codes = {
        str(code).strip()
        for code in frame.filter(pl.col('fund_type') == 'ETF')['ts_code'].to_list()
        if code
    }
    return codes or None


def _read_fund_etf_cache(
    path: Path | None = None,
) -> tuple[set[str], float] | None:
    """读取本地 ETF 名单缓存，返回 ``(代码集合, mtime)``；缺失或为空返回 None。"""
    target = path or FUND_ETF_CODES_CACHE
    try:
        mtime = target.stat().st_mtime
        text = target.read_text(encoding='utf-8')
    except OSError:
        return None
    codes = {line.strip() for line in text.splitlines() if line.strip()}
    return (codes, mtime) if codes else None


def _write_fund_etf_cache(codes: set[str], path: Path | None = None) -> None:
    """原子写入 ETF 名单缓存；写失败只告警，不阻断回测。"""
    target = path or FUND_ETF_CODES_CACHE
    try:
        write_text_atomic('\n'.join(sorted(codes)) + '\n', target)
    except OSError as exc:
        logger.warning(f'[clean_set] ETF 名单缓存写入失败: {exc}')


def _load_fund_etf_codes(ttl_days: float | None = None) -> set[str] | None:
    """加载 ETF 名单。

    优先级：本地 ``meta/fund_basic.parquet`` 快照 > 本地 txt 缓存（未超过
    ``ttl_days`` 天）> 远程库刷新并回写 txt。远程刷新失败时沿用旧缓存；
    完全没有名单才返回 None 由调用方降级。
    """
    snapshot = _fund_etf_codes_from_parquet()
    if snapshot is not None:
        return snapshot
    ttl = FUND_ETF_CODES_TTL_DAYS if ttl_days is None else ttl_days
    cached = _read_fund_etf_cache()
    if cached is not None:
        codes, mtime = cached
        if datetime.now().timestamp() - mtime <= ttl * 86400:
            return codes
    try:
        fresh = _fund_etf_codes_from_db() or None
    # 兜底理由：离线/库不可用不应阻断 clean_set，先退回旧缓存再降级
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            f'[clean_set] fund_basic 不可用，尝试沿用本地缓存: {exc}')
        fresh = None
    if fresh:
        _write_fund_etf_cache(fresh)
        return fresh
    if cached is not None:
        logger.warning('[clean_set] 远程刷新失败，沿用过期 ETF 名单缓存')
        return cached[0]
    logger.warning('[clean_set] 无 ETF 名单缓存且远程不可用，etf 范围降级为不放开')
    return None


def _apply_scope_with_etf_codes(
    scope: str, etf_codes: set[str] | None
) -> str:
    """``etf`` 范围缺少 ETF 名单时降级为 ``none``（宁严不宽）。"""
    if scope == 'etf' and not etf_codes:
        logger.warning('[clean_set] etf 范围缺少 fund_basic ETF 名单，降级为 none')
        return 'none'
    return scope


# ----------------------------------------------------------------------
# 原因词表（结构化 / 时效性 / ST / pending）
# ----------------------------------------------------------------------
STRUCTURAL_REASONS: dict[str, str] = {
    'structural_no_local_quote': '无本地行情文件',
    'structural_content_inconsistent': '行情内容不一致（OHLC/重复日期/元数据不符）',
    'structural_duplicate_exdiv_key': '除权键重复（同代码同 ex_date 多行）',
    'structural_factor_bad': '复权因子缺失或非法值',
    'structural_factor_irreversible': '复权因子不可逆（qfq/hfq 比例失配）',
    'structural_event_jump_mismatch': '除权事件与因子跳变不一一对应',
    'structural_rights_no_second_source': '配股事件无独立第二源（strict 口径）',
    'structural_factor_base_shift': '后复权基期偏移台账标记（strict 口径）',
    'structural_date_anomaly': '当日命中已知源端异常行',
    'structural_pre_1993_break': '1991-1992 拆细比例未确认，1993 年前不参与回测',
    'structural_placeholder_quote_full': '整只占位桩（全部行为 close=1.0/volume=0/amount=0），不计入 full',
}

TEMPORAL_REASONS: dict[str, str] = {
    'temporal_not_listed': '当日尚未上市',
    'temporal_no_quote_on_date': '当日无行情行（停牌/缺口）',
    'temporal_source_stopped': '当日已超过本地行情末行（源端停更/退市）',
    'temporal_gap_or_suspension': '当日处于上市区间但无行情行（停牌/缺口）',
    'temporal_factor_tail': '当日超过复权因子末端',
}

ST_REASONS: dict[str, str] = {
    'st_confirmed': '当日处于确证 ST/*ST 区间',
    'st_suspected': '当日命中疑似 ST 标记（默认不参与 strict）',
}

PENDING_REASONS: dict[str, str] = {
    'pending_fund_dividend': 'ETF/基金分红是否纳入复权尚未结论',
    'pending_st_history': 'ST 区间表未覆盖该日期',
}

REASON_CATEGORY: dict[str, str] = {
    **dict.fromkeys(STRUCTURAL_REASONS, 'structural'),
    **dict.fromkeys(TEMPORAL_REASONS, 'temporal'),
    **dict.fromkeys(ST_REASONS, 'st'),
    **dict.fromkeys(PENDING_REASONS, 'pending'),
}

LAYER_LABELS = {
    'data_defect': '数据缺陷层',
    'st': 'ST 层',
    'div_doubt': '除权事件存疑层',
    'pending': '除权口径 pending（基金）',
    'remaining': '剩余层',
}


def _as_date(value: Any) -> date:
    """把 str/date/datetime/ordinal 统一成 ``date``。"""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return date.fromordinal(value)
    text = str(value).strip()
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        with contextlib.suppress(ValueError):
            return datetime.strptime(text[:10] if '-' in text else text, fmt).date()
    raise ValueError(f'无法解析日期: {value!r}')


def _ord(value: date | None) -> int | None:
    return value.toordinal() if value is not None else None


def _source_lag_trading_days(
    data_last: date | None,
    calendar_latest: date | None,
    trading_days: Iterable[date],
) -> int:
    """数据最新日到日历最新交易日之间的交易日数（半开区间 ``(data_last, latest]``）。"""
    if data_last is None or calendar_latest is None or calendar_latest <= data_last:
        return 0
    return sum(1 for d in trading_days if data_last < d <= calendar_latest)


def source_staleness(
    data_last: date | None,
    calendar_latest: date | None,
    trading_days: Iterable[date],
) -> dict[str, Any]:
    """独立陈旧度检查：滞后超过容忍上限才判数据源停摆。

    与 ``temporal_source_stopped``（逐标的、按评估日是否越过该标的最后一根
    bar）相互独立：这里回答的是「全市场最新行情是否已经落后日历」。
    """
    lag = _source_lag_trading_days(data_last, calendar_latest, trading_days)
    return {
        'data_last': str(data_last) if data_last else None,
        'calendar_latest': str(calendar_latest) if calendar_latest else None,
        'lag_trading_days': lag,
        'source_stopped': lag > MAX_SOURCE_LAG_TRADING_DAYS,
    }


def _code_type(ts_code: str) -> str:
    """按代码段推断类型；仅用于台账里没有类型字段的补充清单。"""
    if is_index_code(ts_code):
        return 'index'
    if is_fund_code(ts_code):
        return 'fund'
    return 'stock'


@dataclass
class CodeInfo:
    ts_code: str
    data_type: str
    rows: int
    start: date | None
    end: date | None
    market: str = ''
    start_ord: int | None = None
    end_ord: int | None = None
    gaps: int = 0
    invalid_ohlc: int = 0
    null_close: int = 0
    neg_values: int = 0
    dup_dates: int = 0
    meta_bad: bool = False
    quote_dates: np.ndarray = field(default_factory=lambda: np.array([], dtype=np.int64))
    factor_dates: np.ndarray = field(default_factory=lambda: np.array([], dtype=np.int64))
    factor_rows: int = 0
    factor_start: date | None = None
    factor_end: date | None = None
    factor_bad: int = 0
    factor_ratio_dev: float = 0.0
    structural: list[str] = field(default_factory=list)
    strict_static: list[str] = field(default_factory=list)
    stale_start: int | None = None
    factor_tail_start: int | None = None
    rights_start: int | None = None
    mismatch_start: int | None = None
    st_intervals: list[tuple[int, int]] = field(default_factory=list)
    st_coverage: tuple[int, int] | None = None
    suspected_st: bool = False
    date_excl_standard: set[int] = field(default_factory=set)
    date_excl_strict: set[int] = field(default_factory=set)
    date_suspended: set[int] = field(default_factory=set)


class EligibilityEngine:
    """加载本地列存索引，按日期回答准入问题。"""

    #: 手工组装引擎（测试）时的默认值，避免未跑 __init__ 的属性缺失。
    close_fund_dividend_pending_scope: str = 'none'
    etf_codes: set[str] | None = None
    #: 数据最新交易日（各标的最新 bar 的最大值）、日历最新交易日与陈旧度。
    data_last: date | None = None
    calendar_latest: date | None = None
    source_lag_days: int = 0
    source_stopped: bool = False
    explicit_as_of: date | None = None
    _calendar_all: tuple[date, ...] = ()
    suspension_union: SuspensionUnion | None = None
    #: 整只占位桩代码（``placeholder_quote_codes.csv`` 中 ``full=true``）：文件
    #: 全部行都是占位，只影响 ``full`` 视图；standard/strict 仍由
    #: ``structural_placeholder_quote`` 剔除，两者同源同一张清单。
    _placeholder_full_codes: frozenset[str] = frozenset()

    @property
    def close_fund_dividend_pending(self) -> bool:
        """旧布尔开关的只读视图：仅 ``all`` 视为打开。"""
        return self.close_fund_dividend_pending_scope == 'all'

    @close_fund_dividend_pending.setter
    def close_fund_dividend_pending(self, value: bool) -> None:
        """旧布尔开关的兼容写入：True→all，False→none。"""
        self.close_fund_dividend_pending_scope = 'all' if value else 'none'

    def __init__(
        self,
        root: str | Path | None = None,
        *,
        target_date: str | date | None = None,
        exclude_st: bool = True,
        include_suspected_st: bool = False,
        close_fund_dividend_pending: bool | None = None,
        close_fund_dividend_pending_scope: str | None = None,
    ) -> None:
        self.root = Path(root) if root else DEFAULT_ROOT
        self.exclude_st = exclude_st
        self.include_suspected_st = include_suspected_st
        scope = _resolve_fund_pending_scope(
            close_fund_dividend_pending_scope, close_fund_dividend_pending)
        etf_codes = _load_fund_etf_codes() if scope == 'etf' else None
        self.close_fund_dividend_pending_scope = _apply_scope_with_etf_codes(
            scope, etf_codes)
        self.etf_codes = etf_codes
        self.codes: dict[str, CodeInfo] = {}
        self.calendar: list[date] = []
        self.calendar_ord = np.array([], dtype=np.int64)
        self._calendar_all: tuple[date, ...] = ()
        self.explicit_as_of: date | None = _as_date(target_date) if target_date else None
        self.as_of: date | None = None
        self.data_last: date | None = None
        self.calendar_latest: date | None = None
        self.source_lag_days: int = 0
        self.source_stopped: bool = False
        self._load_meta()
        self._load_calendar()
        self._resolve_dates()
        self._load_quote_stats()
        self._load_implied_suspension()
        self._load_factor_stats()
        self._load_dividend_flags()
        self._load_anomaly_lists()
        self._load_anomaly_records()
        self._load_st()
        self._finalize_structural()
        logger.debug(
            f'准入索引就绪: {len(self.codes)} 只, '
            f'日历 {self.calendar[0]} ~ {self.calendar[-1]}, as_of={self.as_of}'
        )

    def _resolve_dates(self) -> None:
        """确定判定时点 ``as_of``（由数据决定）与数据源陈旧度。

        ``as_of`` 默认取数据内最新交易日，而不是系统当天：当日行情尚未落盘时
        用系统日会把逐标的的「评估日越过最后一根 bar」判据放大成全市场停更。
        调用方显式传入的 ``as_of`` 仍然保留，但记录来源并在超前于数据时告警。
        """
        ends = [i.end_ord for i in self.codes.values() if i.end_ord is not None]
        self.data_last = date.fromordinal(max(ends)) if ends else None
        calendar_all = self._calendar_all or tuple(self.calendar)
        past = [d for d in calendar_all if d <= date.today()]
        if past:
            self.calendar_latest = past[-1]
        elif self.calendar:
            self.calendar_latest = self.calendar[-1]
        status = source_staleness(self.data_last, self.calendar_latest, calendar_all)
        self.source_lag_days = int(status['lag_trading_days'])
        self.source_stopped = bool(status['source_stopped'])

        if self.explicit_as_of is not None:
            self.as_of = self.explicit_as_of
            logger.info(
                f'as_of 由调用方显式指定: {self.as_of}'
                f'（行情最新日 {self.data_last}）')
            if self.data_last is not None and self.as_of > self.data_last:
                logger.warning(
                    f'显式 as_of={self.as_of} 晚于行情最新日 {self.data_last}：'
                    f'该日的 temporal_* 反映的是「当日行情缺失」，不是标的停更')
        else:
            self.as_of = self.data_last or (
                self.calendar[-1] if self.calendar else None)
            logger.info(
                f'as_of 由数据决定: {self.as_of}'
                f'（行情最新日 {self.data_last}，系统日 {date.today()}）')
        if self.source_stopped:
            logger.warning(
                f'本地行情疑似停摆: 最新日 {self.data_last} 落后日历最新交易日 '
                f'{self.calendar_latest} 共 {self.source_lag_days} 个交易日'
                f'（容忍 {MAX_SOURCE_LAG_TRADING_DAYS} 个）')

    # ------------------------------------------------------------------
    # 加载
    # ------------------------------------------------------------------
    @property
    def as_of_date(self) -> date:
        """已初始化的 as_of 日期；索引构建完成后必然存在。"""
        if self.as_of is None:
            raise RuntimeError('as_of 尚未初始化')
        return self.as_of

    def _load_meta(self) -> None:
        meta = pl.read_parquet(self.root / 'meta/daily_index.parquet')
        entity_index = load_entity_index(self.root)
        for row in meta.iter_rows(named=True):
            code = str(row['ts_code'])
            if entity_index.is_superseded(code):
                # 旧码已被同一实体的新码完整覆盖，读侧只保留新码
                continue
            info = CodeInfo(
                ts_code=code,
                data_type=str(row['data_type']),
                rows=int(row['rows']),
                start=row['start'],
                end=row['end'],
                market=str(row['market']),
            )
            info.start_ord = _ord(info.start)
            info.end_ord = _ord(info.end)
            self.codes[code] = info

    def _load_calendar(self) -> None:
        """交易日历统一取自 ``src.data.trading_calendar`` 的权威段。

        旧实现直接读 000001.SH，与 freshness/tradable_universe 的口径可能
        不一致；这里只用 ``authoritative_dates()``（真实交易日），不把日历
        末端的近似尾部计入「应有交易日」，避免虚增缺口率。
        """
        from src.data.trading_calendar import get_calendar  # noqa: PLC0415

        calendar = get_calendar(self.root)
        self._calendar_all = tuple(calendar.dates)
        days = calendar.authoritative_dates()
        if not days:
            bound = self.as_of_date if self.as_of is not None else date.today()
            days = tuple(d for d in calendar.dates if d <= bound)
        self.calendar = list(days)
        self.calendar_ord = np.array([d.toordinal() for d in self.calendar], dtype=np.int64)

    def _load_quote_stats(self) -> None:
        # 只扫洁净集覆盖的三类资产：quotes/futures 用交易所原生列名
        # （trade_date / vol），混进通配会在 schema 合并阶段直接报错。
        globs = [
            str(self.root / 'quotes' / data_type / '**' / '*.parquet')
            for data_type in DATA_TYPES
        ]
        scan = pl.scan_parquet(globs).select(
            ['ts_code', 'date', 'open', 'high', 'low', 'close', 'volume']
        )
        null_any = (
            pl.col('open').is_null() | pl.col('high').is_null()
            | pl.col('low').is_null() | pl.col('close').is_null()
        )
        bad = (
            null_any
            | (pl.col('high') < pl.col('low'))
            | (pl.col('high') < pl.max_horizontal('open', 'close'))
            | (pl.col('low') > pl.min_horizontal('open', 'close'))
        ).fill_null(False)
        neg = (
            (pl.col('close') <= 0) | (pl.col('volume') < 0)
        ).fill_null(False)
        stats = (
            scan.with_columns(
                bad.alias('bad'),
                pl.col('close').is_null().alias('null_close'),
                neg.alias('neg'),
            )
            .group_by('ts_code')
            .agg(
                pl.len().alias('rows'),
                pl.col('date').min().alias('start'),
                pl.col('date').max().alias('end'),
                pl.col('bad').sum().alias('bad'),
                pl.col('null_close').sum().alias('null_close'),
                pl.col('neg').sum().alias('neg'),
            )
            .collect()
        )
        dups = (
            scan.group_by(['ts_code', 'date']).len()
            .filter(pl.col('len') > 1)
            .group_by('ts_code').agg(pl.len().alias('dup_dates'))
            .collect()
        )
        dup_map = dict(
            zip(dups['ts_code'].to_list(), dups['dup_dates'].to_list(), strict=True)
        )
        for row in stats.iter_rows(named=True):
            code = str(row['ts_code'])
            info = self.codes.get(code)
            if info is None:
                continue
            info.invalid_ohlc = int(row['bad'])
            info.null_close = int(row['null_close'])
            info.neg_values = int(row['neg'])
            info.dup_dates = int(dup_map.get(code, 0))
            info.meta_bad = (
                int(row['rows']) != info.rows
                or row['start'] != info.start
                or row['end'] != info.end
            )
        # 读取每只标的的日期索引（一次性扫描，避免 9600 次 open）
        dates = scan.select(['ts_code', 'date']).collect().sort(['ts_code', 'date'])
        for g in dates.partition_by('ts_code', maintain_order=True):
            code = str(g['ts_code'][0])
            info = self.codes.get(code)
            if info is None:
                continue
            info.quote_dates = np.array(
                [d.toordinal() for d in g['date'].to_list()], dtype=np.int64
            )
        # 缺口率：上市区间内应有交易日 - 实际行数（停牌也计入缺口）
        for info in self.codes.values():
            if not info.quote_dates.size or info.start_ord is None:
                continue
            lo = bisect_left(self.calendar_ord, info.start_ord)
            hi = bisect_right(self.calendar_ord, info.end_ord)
            expected = max(0, hi - lo)
            info.gaps = max(0, expected - int(info.quote_dates.size))

    def _load_factor_stats(self) -> None:
        path = self.root / 'factors/stock_adj_factor.parquet'
        if not path.exists():
            return
        scan = pl.scan_parquet(path).select(
            ['ts_code', 'trade_date', 'qfq_factor', 'hfq_factor']
        )
        bad_q = (
            pl.col('qfq_factor').is_null() | pl.col('qfq_factor').is_nan()
            | (pl.col('qfq_factor') <= 0)
        ).fill_null(False)
        bad_h = (
            pl.col('hfq_factor').is_null() | pl.col('hfq_factor').is_nan()
            | (pl.col('hfq_factor') <= 0)
        ).fill_null(False)
        fstats = (
            scan.with_columns(
                (bad_q | bad_h).alias('bad'),
                (pl.col('hfq_factor') / pl.col('qfq_factor')).alias('ratio'),
            )
            .group_by('ts_code')
            .agg(
                pl.len().alias('f_rows'),
                pl.col('trade_date').min().alias('f_start'),
                pl.col('trade_date').max().alias('f_end'),
                pl.col('bad').sum().alias('bad'),
                pl.col('ratio').min().alias('r_min'),
                pl.col('ratio').max().alias('r_max'),
            )
            .collect()
        )
        for row in fstats.iter_rows(named=True):
            info = self.codes.get(str(row['ts_code']))
            if info is None:
                continue
            info.factor_rows = int(row['f_rows'])
            info.factor_start = row['f_start']
            info.factor_end = row['f_end']
            info.factor_bad = int(row['bad'])
            rmin, rmax = row['r_min'], row['r_max']
            if rmin is None or rmax is None or rmax == 0:
                info.factor_ratio_dev = 0.0 if int(row['bad']) else 1.0
            else:
                info.factor_ratio_dev = abs(float(rmax) - float(rmin)) / abs(float(rmax))
        # 因子日期索引 + 跳变/事件匹配（读一次，算完即释放 hfq）
        factors = scan.select(['ts_code', 'trade_date', 'hfq_factor']).collect() \
            .sort(['ts_code', 'trade_date'])
        events = self._dividend_events()
        for g in factors.partition_by('ts_code', maintain_order=True):
            code = str(g['ts_code'][0])
            info = self.codes.get(code)
            if info is None:
                continue
            fd = np.array([d.toordinal() for d in g['trade_date'].to_list()], dtype=np.int64)
            info.factor_dates = fd
            hfq = g['hfq_factor'].to_numpy().astype(np.float64)
            if fd.size >= 2:
                jumps = fd[1:][np.diff(hfq) != 0]
            else:
                jumps = np.array([], dtype=np.int64)
            ev_frame = events.get(code)
            if ev_frame is not None and not ev_frame.is_empty():
                ev = np.array(
                    [d.toordinal() for d in ev_frame['ex_date'].to_list() if d],
                    dtype=np.int64)
            else:
                ev = np.array([], dtype=np.int64)
            if ev.size:
                pos = np.searchsorted(fd, ev, side='left')
                valid = (pos > 0) & (pos < fd.size)
                expected = np.unique(fd[pos[valid]])
            else:
                expected = np.array([], dtype=np.int64)
            extra_jumps = np.setdiff1d(jumps, expected, assume_unique=False)
            missing_jumps = np.setdiff1d(expected, jumps, assume_unique=False)
            if missing_jumps.size:
                # 配股价≈市价、空事件行、no_ex_rights 等事件的理论因子恰为 1，
                # 不算因子跳变，避免把「无台阶」误判为结构性缺陷。
                missing_jumps = self._drop_neutral_missing(
                    info, fd, missing_jumps, ev_frame)
            if extra_jumps.size:
                info.mismatch_start = int(extra_jumps.min())
            elif missing_jumps.size:
                info.mismatch_start = int(missing_jumps.min())

    def _drop_neutral_missing(
        self,
        info: CodeInfo,
        fd: np.ndarray,
        missing: np.ndarray,
        ev_frame: pl.DataFrame | None,
    ) -> np.ndarray:
        """剔除理论除权因子恰为 1 的事件（这类事件本就不产生因子台阶）。"""
        if ev_frame is None or ev_frame.is_empty():
            return missing
        path = self.root / 'quotes' / info.data_type / info.market / f'{info.ts_code}.parquet'
        if not path.is_file():
            return missing
        quote = pl.read_parquet(path, columns=['date', 'close'])
        quote = quote.with_columns(pl.col('date').cast(pl.Date)).sort('date')
        close_map = {
            d.toordinal(): float(c)
            for d, c in zip(quote['date'].to_list(), quote['close'].to_list(), strict=True)
            if c is not None
        }
        miss = {int(x) for x in missing}
        keep = set(miss)
        for row in ev_frame.iter_rows(named=True):
            ex = row['ex_date']
            if ex is None:
                continue
            pos = int(np.searchsorted(fd, ex.toordinal(), side='left'))
            if pos <= 0 or pos >= fd.size:
                continue
            hit = int(fd[pos])
            if hit not in miss:
                continue
            prev_close = close_map.get(int(fd[pos - 1]))
            if prev_close is None:
                continue
            if row.get('no_ex_rights'):
                keep.discard(hit)
                continue
            factor = calc_dividend_event_factor(
                prev_close, row.get('cash_div'), row.get('share_div'),
                row.get('rights_issue_ratio'), row.get('rights_issue_price'))
            if factor is None or abs(factor - 1.0) <= 1e-9:
                keep.discard(hit)
        return np.array(sorted(keep), dtype=np.int64)

    def _dividend_events(self) -> dict[str, pl.DataFrame]:
        path = self.root / 'factors/stock_dividend.parquet'
        if not path.exists():
            return {}
        div = pl.read_parquet(path).filter(pl.col('ex_date').is_not_null())
        keep = [
            c for c in (
                'ex_date', 'cash_div', 'share_div', 'rights_issue_ratio',
                'rights_issue_price', 'no_ex_rights')
            if c in div.columns
        ]
        return {
            str(g['ts_code'][0]): g.select(keep)
            for g in div.partition_by('ts_code', maintain_order=True)
        }

    def _load_dividend_flags(self) -> None:
        path = self.root / 'factors/stock_dividend.parquet'
        if not path.exists():
            return
        div = pl.read_parquet(path)
        # 同代码同非空 ex_date 多行且权益字段互相重叠 → 结构性重复键。
        # 互补行（如「配股」与「送股」同日、来自两个源各记一半）不是重复，
        # 合并后由复权公式统一消费，故不计入重复键缺陷。
        dup = (
            div.filter(pl.col('ex_date').is_not_null())
            .with_columns(
                (pl.col('cash_div').fill_null(0) != 0).cast(pl.Int8).alias('_c'),
                (pl.col('share_div').fill_null(0) != 0).cast(pl.Int8).alias('_s'),
                (pl.col('rights_issue_ratio').fill_null(0) > 0).cast(pl.Int8).alias('_r'),
            )
            .group_by(['ts_code', 'ex_date'])
            .agg(
                pl.len().alias('_n'),
                pl.col('_c').sum().alias('_cs'),
                pl.col('_s').sum().alias('_ss'),
                pl.col('_r').sum().alias('_rs'),
            )
            .filter(
                (pl.col('_n') > 1)
                & ((pl.col('_cs') > 1) | (pl.col('_ss') > 1) | (pl.col('_rs') > 1))
            )
            .select('ts_code').unique()['ts_code'].to_list()
        )
        self.dup_exdiv_codes = set(dup)
        # 空 ex_date 重复仅作台账标注，不参与准入
        self.null_exdiv_dup_codes = set(
            div.filter(pl.col('ex_date').is_null())
            .group_by(['ts_code', 'ex_date']).len()
            .filter(pl.col('len') > 1)
            .select('ts_code').unique()['ts_code'].to_list()
        )
        # rights_start 统一由 rights_issue_codes.csv 台账提供（该台账已排除
        # 经第二源核验的配股事件），此处不再直接从因子表推算，避免绕过核验。

    def _read_anomaly_list(self, name: str) -> pl.DataFrame | None:
        path = self.root / 'meta' / 'anomalies' / name
        if not path.exists():
            return None
        return pl.read_csv(path)

    def _load_anomaly_lists(self) -> None:
        # 补数线台账（规则级清单）
        self._load_stale_list(self._read_anomaly_list('source_stale_codes.csv'))
        self._load_factor_lag_list(self._read_anomaly_list('factor_tail_lag_codes.csv'))
        self._load_no_local_list(self._read_anomaly_list('no_local_quote_codes.csv'))
        self._load_rights_list(self._read_anomaly_list('rights_issue_codes.csv'))
        # 基金源端假收盘（收盘价严重偏离 VWAP 且次日大幅跳空）：当日价格不可信
        self._load_fake_bar_list(self._read_anomaly_list('fund_fake_bar_bars.csv'))
        # 价格恒为 1.0 的占位桩基金：数据本身无效，宁严不宽，整段不可准入
        placeholder = self._read_anomaly_list('placeholder_quote_codes.csv')
        self._placeholder_codes = (
            {str(c) for c in placeholder['ts_code'].to_list()}
            if placeholder is not None else set()
        )
        # 整只占位（``full=true``）：该文件全部行都是占位桩，full 视图不再计入
        # —— full 衡量「数据在不在」，恒为 1.0 的平线不是数据。
        self._placeholder_full_codes = frozenset()
        if placeholder is not None:
            self._placeholder_full_codes = frozenset(
                str(c) for c in placeholder.filter(
                    pl.col('full').cast(pl.Utf8).str.to_lowercase() == 'true'
                )['ts_code'].to_list()
            )

    def _load_stale_list(self, stale: pl.DataFrame | None) -> None:
        if stale is None:
            return
        for row in stale.iter_rows(named=True):
            info = self.codes.get(str(row['ts_code']))
            if info is None:
                continue
            if row.get('start'):
                info.stale_start = _as_date(row['start']).toordinal()
            else:
                info.stale_start = (info.end_ord + 1) if info.end_ord else None

    def _load_factor_lag_list(self, lag: pl.DataFrame | None) -> None:
        if lag is None:
            return
        for row in lag.iter_rows(named=True):
            info = self.codes.get(str(row['ts_code']))
            if info is None:
                continue
            if row.get('start'):
                info.factor_tail_start = _as_date(row['start']).toordinal()
            elif info.factor_end:
                info.factor_tail_start = info.factor_end.toordinal() + 1

    def _load_no_local_list(self, no_local: pl.DataFrame | None) -> None:
        if no_local is None:
            return
        for raw_code in no_local['ts_code'].to_list():
            code = str(raw_code)
            if code in self.codes:
                continue
            self.codes[code] = CodeInfo(
                ts_code=code, data_type=_code_type(code), rows=0,
                start=None, end=None, market=code.rsplit('.', 1)[-1].lower(),
            )

    def _load_implied_suspension(self) -> None:
        """并入「占位 bar 型停牌」日（volume/amount 为 NULL 且四价相等）。

        与既有 ``suspension_daily``（缺口型）取并集后按日标记，判据语义不变，
        只补上原先漏掉的日期；两表文件缺失时按空表处理，不阻断索引构建。
        """
        try:
            union = load_suspension_union(self.root)
        except Exception as exc:  # noqa: BLE001 - 缺表/坏表不应阻断准入索引
            logger.warning(f'停牌并集加载失败，按仅缺口口径继续: {exc}')
            self.suspension_union = None
            return
        self.suspension_union = union
        for code, day in zip(union.frame['ts_code'], union.frame['trade_date'],
                             strict=True):
            info = self.codes.get(str(code))
            if info is not None:
                info.date_suspended.add(day.toordinal())

    def _load_fake_bar_list(self, fake: pl.DataFrame | None) -> None:
        """基金源端假收盘：仅剔除该日，不整段剔除标的。"""
        if fake is None:
            return
        for row in fake.iter_rows(named=True):
            info = self.codes.get(str(row['ts_code']))
            if info is None or row.get('trade_date') is None:
                continue
            info.date_excl_standard.add(_as_date(row['trade_date']).toordinal())

    def _load_rights_list(self, rights: pl.DataFrame | None) -> None:
        if rights is None:
            return
        first = rights.group_by('ts_code').agg(pl.col('start').min().alias('d'))
        for row in first.iter_rows(named=True):
            info = self.codes.get(str(row['ts_code']))
            if info is None or row['d'] is None:
                continue
            value = _as_date(row['d']).toordinal()
            info.rights_start = (
                value if info.rights_start is None else min(info.rights_start, value)
            )

    def _load_anomaly_records(self) -> None:
        for rec in iter_anomalies():
            if rec.status != 'active' or rec.scope == 'rule':
                continue
            info = self.codes.get(rec.ts_code)
            if info is None:
                continue
            if rec.issue_type == 'duplicate_ex_date':
                # 带明确日期的重复键只影响该日因子台阶（如 000002.SZ 1991-06-08
                # 配股+送股同日的乘性近似），按日剔除；无日期的才按永久缺陷处理。
                if rec.dates:
                    info.date_excl_strict.update(d.toordinal() for d in rec.dates)
                else:
                    info.structural.append('structural_duplicate_exdiv_key')
            elif rec.issue_type == 'hfq_base_shift':
                info.strict_static.append('structural_factor_base_shift')
            elif rec.issue_type in ('ohlc_bounds', 'weekend_trade_date'):
                info.date_excl_standard.update(d.toordinal() for d in rec.dates)
            elif rec.issue_type == 'pre_close_gap':
                info.date_excl_strict.update(d.toordinal() for d in rec.dates)

    def _load_st(self) -> None:
        path = self.root / 'meta/st/st_intervals.parquet'
        if not path.exists():
            return
        intervals = pl.read_parquet(path)
        for g in intervals.partition_by('ts_code', maintain_order=True):
            code = str(g['ts_code'][0])
            info = self.codes.get(code)
            if info is None:
                continue
            for row in g.iter_rows(named=True):
                start = row['start_date']
                end = row['end_date']
                if start is None:
                    continue
                info.st_intervals.append((
                    start.toordinal(),
                    end.toordinal() if end is not None else 99999999,
                ))
        coverage = pl.read_parquet(self.root / 'meta/st/st_coverage.parquet')
        default_row = coverage.filter(pl.col('ts_code').is_null()).to_dicts()
        self.st_default = (
            (default_row[0]['coverage_start'].toordinal(),
             default_row[0]['coverage_end'].toordinal())
            if default_row else None
        )
        for row in coverage.filter(pl.col('ts_code').is_not_null()).iter_rows(named=True):
            info = self.codes.get(str(row['ts_code']))
            if info is None:
                continue
            info.st_coverage = (
                row['coverage_start'].toordinal(), row['coverage_end'].toordinal()
            )
        # 疑似 ST：当前名称带 ST 但不在确证区间内的标的（本快照为 0 只）
        names_path = self.root / 'meta/names.json'
        self.suspected_st: set[str] = set()
        if names_path.exists():
            names = json.loads(names_path.read_text('utf-8'))
            st_like = {
                code for code, name in names.items()
                if isinstance(name, str) and 'ST' in name.upper()
            }
            self.suspected_st = {
                code for code in st_like
                if code in self.codes
                and self.codes[code].data_type == 'stock'
                and not self.codes[code].st_intervals
            }
        snapshot = self.root / 'meta/st_status/st_status_20261103.parquet'
        if snapshot.exists():
            snap = pl.read_parquet(snapshot)
            self.snapshot_st = set(
                snap.filter(pl.col('status') == 'st')['code'].to_list()
            )
        else:
            self.snapshot_st = set()

    def _finalize_structural(self) -> None:
        placeholder: set[str] = getattr(self, '_placeholder_codes', set())
        for info in self.codes.values():
            # 无本地行情：后续检查无意义，直接短路
            if info.rows == 0 or not info.quote_dates.size:
                info.structural.append('structural_no_local_quote')
                continue
            if info.ts_code in placeholder:
                info.structural.append('structural_placeholder_quote')
            # 行情内容
            if (
                info.invalid_ohlc or info.null_close or info.neg_values
                or info.dup_dates or info.meta_bad
            ):
                info.structural.append('structural_content_inconsistent')
            # 因子
            if info.data_type == 'stock':
                if info.factor_rows == 0:
                    info.structural.append('structural_factor_bad')
                elif info.factor_bad:
                    info.structural.append('structural_factor_bad')
                elif info.factor_ratio_dev > 1e-12:
                    info.structural.append('structural_factor_irreversible')
            # 重复键（台账 + 快照）
            if info.ts_code in getattr(self, 'dup_exdiv_codes', set()):
                if 'structural_duplicate_exdiv_key' not in info.structural:
                    info.structural.append('structural_duplicate_exdiv_key')

    # ------------------------------------------------------------------
    # 日期判定
    # ------------------------------------------------------------------
    def _has_quote(self, info: CodeInfo, o: int) -> bool:
        arr = info.quote_dates
        if arr.size == 0:
            return False
        i = bisect_left(arr, o)
        return i < arr.size and int(arr[i]) == o

    def _has_factor(self, info: CodeInfo, o: int) -> bool:
        arr = info.factor_dates
        if arr.size == 0:
            return False
        i = bisect_left(arr, o)
        return i < arr.size and int(arr[i]) == o

    def _st_status(self, info: CodeInfo, o: int) -> bool | None:
        """返回 True=ST、False=非 ST、None=区间表未覆盖（unknown）。"""
        covered = True
        cov = info.st_coverage or self.st_default
        if cov is not None and (o < cov[0] or o > cov[1]):
            covered = False
        if info.st_intervals:
            for start, end in info.st_intervals:
                if start <= o <= end:
                    return True
        return False if covered else None

    def _structural_reasons(self, info: CodeInfo, o: int, tier: str) -> list[str]:
        reasons = list(info.structural)
        if _pre_1993_break(info, o):
            reasons.append('structural_pre_1993_break')
        if tier == 'strict':
            reasons.extend(info.strict_static)
            if o in info.date_excl_strict or o in info.date_excl_standard:
                reasons.append('structural_date_anomaly')
        elif tier == 'standard':
            if o in info.date_excl_standard:
                reasons.append('structural_date_anomaly')
        return reasons

    def _availability_reasons(self, info: CodeInfo, o: int) -> list[str]:
        if info.start_ord is None or info.end_ord is None:
            return ['temporal_no_quote_on_date']
        if o < info.start_ord:
            return ['temporal_not_listed']
        if o > info.end_ord:
            return ['temporal_source_stopped']
        if not self._has_quote(info, o):
            return ['temporal_gap_or_suspension']
        if o in info.date_suspended:
            return ['temporal_gap_or_suspension']
        return []

    def _fund_pending_open(self, info: CodeInfo) -> bool:
        """该基金的 pending 是否已按范围放开（none/etf/all）。"""
        scope = self.close_fund_dividend_pending_scope
        if scope == 'all':
            return True
        if scope == 'etf':
            return info.ts_code in (self.etf_codes or set())
        return False

    def _strict_reasons(self, info: CodeInfo, o: int) -> list[str]:
        """strict 档在 standard 之上的额外闸门。"""
        reasons: list[str] = []
        if info.data_type == 'stock' and not self._has_factor(info, o):
            reasons.append('temporal_factor_tail')
        if info.stale_start is not None and o >= info.stale_start:
            reasons.append('temporal_source_stopped')
        if info.rights_start is not None and o >= info.rights_start:
            reasons.append('structural_rights_no_second_source')
        if info.mismatch_start is not None and o >= info.mismatch_start:
            reasons.append('structural_event_jump_mismatch')
        if info.data_type == 'fund' and not self._fund_pending_open(info):
            reasons.append('pending_fund_dividend')
        return reasons

    def eligible(
        self,
        ts_code: str,
        trade_date: str | date | int,
        tier: ViewTier = 'strict',
        *,
        include_suspected_st: bool | None = None,
    ) -> tuple[bool, list[str]]:
        """判断某标的在某日是否进入指定档位。"""
        if tier not in VIEWS:
            raise ValueError(f'未知档位: {tier!r}，应为 {VIEWS}')
        info = self.codes.get(str(ts_code))
        if info is None:
            return False, ['unknown_code']
        d = _as_date(trade_date)
        o = d.toordinal()

        if tier == 'full':
            # 整只占位桩：文件全部行都是 close=1.0/volume=0/amount=0，
            # 恒为 1.0 的平线不是数据，不计入「数据在不在」的 full 可用性。
            # 判据与 standard/strict 的 structural_placeholder_quote 同源
            # （同一张 placeholder_quote_codes.csv），仅在 full 档生效。
            if info.ts_code in self._placeholder_full_codes:
                return False, ['structural_placeholder_quote_full']
            reasons = self._availability_reasons(info, o)
            return (not reasons, reasons)

        reasons = self._structural_reasons(info, o, tier)
        if reasons:
            return False, reasons

        avail = self._availability_reasons(info, o)
        if avail:
            return False, avail

        pending: list[str] = []
        if self.exclude_st:
            status = self._st_status(info, o)
            if status is True:
                return False, ['st_confirmed']
            if status is None:
                # 窗口外 unknown：仍然放行（不改变可买行为），只留诊断痕迹
                pending.append('pending_st_history')
            use_suspected = (
                self.include_suspected_st if include_suspected_st is None
                else include_suspected_st
            )
            if use_suspected and info.ts_code in self.suspected_st:
                return False, ['st_suspected']

        if tier == 'standard':
            return True, pending

        strict_reasons = self._strict_reasons(info, o)
        if strict_reasons:
            return False, strict_reasons
        return True, pending

    def eligible_set(
        self,
        trade_date: str | date,
        tier: ViewTier = 'strict',
        *,
        data_type: str | None = None,
        include_suspected_st: bool | None = None,
    ) -> list[str]:
        """返回某日指定档位的标的白名单。"""
        d = _as_date(trade_date).toordinal()
        out: list[str] = []
        for code, info in self.codes.items():
            if data_type and info.data_type != data_type:
                continue
            ok, _ = self.eligible(code, d, tier, include_suspected_st=include_suspected_st)
            if ok:
                out.append(code)
        return out

    # ------------------------------------------------------------------
    # 月末时间序列
    # ------------------------------------------------------------------
    def month_end_dates(
        self, start: str | date | None = None, end: str | date | None = None
    ) -> list[date]:
        lo = _as_date(start) if start else self.calendar[0]
        hi = _as_date(end) if end else self.calendar[-1]
        out: list[date] = []
        current: tuple[int, int] | None = None
        last: date | None = None
        for d in self.calendar:
            if d < lo or d > hi:
                continue
            key = (d.year, d.month)
            if current != key:
                if last is not None:
                    out.append(last)
                current = key
            last = d
        if last is not None:
            out.append(last)
        return out

    def timeline(
        self,
        start: str | date | None = None,
        end: str | date | None = None,
    ) -> list[dict[str, Any]]:
        """按月末采样计算三档可用池只数。"""
        rows: list[dict[str, Any]] = []
        for d in self.month_end_dates(start, end):
            o = d.toordinal()
            counts = {v: dict.fromkeys(DATA_TYPES, 0) for v in VIEWS}
            for info in self.codes.values():
                if not self._has_quote(info, o):
                    continue
                counts['full'][info.data_type] += 1
                if (info.structural or o in info.date_excl_standard
                        or _pre_1993_break(info, o)):
                    continue
                status = (
                    self._st_status(info, o)
                    if self.exclude_st and info.st_intervals else False
                )
                if status is True:
                    continue
                if include_suspected_check(self, info):
                    continue
                counts['standard'][info.data_type] += 1
                # strict
                if info.data_type == 'stock' and not self._has_factor(info, o):
                    continue
                if info.stale_start is not None and o >= info.stale_start:
                    continue
                if (o in info.date_excl_strict or o in info.date_excl_standard
                        or _pre_1993_break(info, o)):
                    continue
                if info.rights_start is not None and o >= info.rights_start:
                    continue
                if info.mismatch_start is not None and o >= info.mismatch_start:
                    continue
                if info.strict_static:
                    continue
                if info.data_type == 'fund' and not self._fund_pending_open(info):
                    continue
                counts['strict'][info.data_type] += 1
            item: dict[str, Any] = {'date': str(d)}
            for view in VIEWS:
                item[f'{view}_stock'] = counts[view]['stock']
                item[f'{view}_index'] = counts[view]['index']
                item[f'{view}_fund'] = counts[view]['fund']
                item[f'{view}_total'] = sum(counts[view].values())
            rows.append(item)
        return rows

    # ------------------------------------------------------------------
    # 快照 / 分层
    # ------------------------------------------------------------------
    def _layer_hits(self, code: str, trade_date: str | date | None = None) -> set[str]:
        """返回该标的在该日命中的分层标签集合（可同时命中多层）。"""
        info = self.codes[code]
        d = _as_date(trade_date) if trade_date else self.as_of_date
        o = d.toordinal()
        hits: set[str] = set()
        defect = list(info.structural)
        if info.stale_start is not None and o >= info.stale_start:
            defect.append('temporal_source_stopped')
        if info.factor_tail_start is not None and o >= info.factor_tail_start:
            defect.append('temporal_factor_tail')
        if info.data_type == 'stock' and self._has_quote(info, o) and not self._has_factor(info, o):
            defect.append('temporal_factor_tail')
        if o in info.date_excl_standard or o in info.date_excl_strict:
            defect.append('structural_date_anomaly')
        if info.mismatch_start is not None and o >= info.mismatch_start:
            defect.append('structural_event_jump_mismatch')
        if _pre_1993_break(info, o):
            defect.append('structural_pre_1993_break')
        if info.strict_static:
            defect.extend(info.strict_static)
        if defect:
            hits.add('data_defect')
        if info.st_intervals and self._st_status(info, o) is True:
            hits.add('st')
        if info.data_type == 'stock' and info.rights_start is not None and o >= info.rights_start:
            hits.add('div_doubt')
        if info.data_type == 'fund' and not self._fund_pending_open(info):
            hits.add('pending')
        return hits

    def layer_at(self, code: str, trade_date: str | date | None = None) -> str:
        """按 as_of 日期给出互斥分层（数据缺陷 > ST > 除权存疑 > pending > 剩余）。"""
        hits = self._layer_hits(code, trade_date)
        for name in ('data_defect', 'st', 'div_doubt', 'pending'):
            if name in hits:
                return name
        return 'remaining'

    def summary_at(self, trade_date: str | date | None = None) -> dict[str, Any]:  # noqa: C901
        """静态快照统计：三档只数/行数、分层、剔除原因、年份分布。

        这里按报告口径顺序列出全部统计项，拆成多个小函数会打断“同一份
        快照必须同源”的阅读路径，因此保留单函数并对圈复杂度做显式豁免。
        """
        d = _as_date(trade_date) if trade_date else self.as_of_date
        o = d.toordinal()

        def rows_of(codes: Iterable[str]) -> int:
            return sum(self.codes[c].rows for c in codes)

        all_codes = list(self.codes)
        view_codes: dict[str, list[str]] = {v: [] for v in VIEWS}
        view_reasons: dict[str, dict[str, set[str]]] = {
            v: {k: set() for k in REASON_CATEGORY} for v in VIEWS
        }
        multi_reason_set: set[str] = set()
        for code in all_codes:
            for view in VIEWS:
                ok, reasons = self.eligible(code, d, view)
                if ok:
                    view_codes[view].append(code)
                else:
                    for reason in reasons:
                        if reason in view_reasons[view]:
                            view_reasons[view][reason].add(code)
                    if view == 'strict' and len(reasons) > 1:
                        multi_reason_set.add(code)

        views: dict[str, Any] = {}
        for view in VIEWS:
            codes = view_codes[view]
            views[view] = {
                'codes': len(codes),
                'rows': rows_of(codes),
                'by_type': {
                    t: {
                        'codes': sum(1 for c in codes if self.codes[c].data_type == t),
                        'rows': sum(self.codes[c].rows for c in codes
                                    if self.codes[c].data_type == t),
                    }
                    for t in DATA_TYPES
                },
            }

        # 分层（互斥）
        layer_codes: dict[str, list[str]] = {
            'data_defect': [], 'st': [], 'div_doubt': [], 'pending': [], 'remaining': []
        }
        for code in all_codes:
            layer_codes[self.layer_at(code, d)].append(code)
        layers = {}
        for name, codes in layer_codes.items():
            starts = [s for c in codes if (s := self.codes[c].start) is not None]
            ends = [e for c in codes if (e := self.codes[c].end) is not None]
            layers[name] = {
                'codes': len(codes),
                'rows': rows_of(codes),
                'start': str(min(starts)) if starts else None,
                'end': str(max(ends)) if ends else None,
                'by_type': {
                    t: {
                        'codes': sum(1 for c in codes if self.codes[c].data_type == t),
                        'rows': sum(self.codes[c].rows for c in codes
                                    if self.codes[c].data_type == t),
                        'end_at_as_of': sum(
                            1 for c in codes
                            if self.codes[c].data_type == t and self.codes[c].end == d
                        ),
                    }
                    for t in DATA_TYPES
                },
            }

        # 剔除原因分类
        reason_summary: dict[str, Any] = {}
        for view in VIEWS:
            by_cat: dict[str, dict[str, int]] = {}
            for reason, hit_codes in view_reasons[view].items():
                cat = REASON_CATEGORY[reason]
                by_cat.setdefault(cat, {})[reason] = len(hit_codes)
            reason_summary[view] = by_cat

        # 相对 full（当日有行情）的剔除口径：只统计 full 内部被剔的标的，
        # 避免把"停更/无本地行情"这些 full 本来就没有的标的重复计入。
        full_set = set(view_codes['full'])
        vs_full: dict[str, Any] = {}
        for view in VS_FULL_VIEWS:
            counts: dict[str, int] = {}
            excluded: set[str] = set()
            multi = 0
            for code in full_set:
                ok, reasons = self.eligible(code, d, view)
                if ok:
                    continue
                excluded.add(code)
                if len(reasons) > 1:
                    multi += 1
                for reason in reasons:
                    counts[reason] = counts.get(reason, 0) + 1
            vs_full[view] = {
                'excluded_codes': len(excluded),
                'multi_reason_codes': multi,
                'reasons': counts,
            }

        # 交集矩阵：缺陷 / ST / 除权存疑 / 基金 pending 可同时命中
        matrix: dict[str, int] = {}
        for code in all_codes:
            hits = self._layer_hits(code, d)
            key = '+'.join(sorted(hits)) if hits else 'none'
            matrix[key] = matrix.get(key, 0) + 1

        # 缺陷原因命中面（含重叠，独立计数）
        defect_breakdown = {
            'no_local_quote': 0,
            'source_stopped_at_as_of': 0,
            'factor_tail_at_as_of': 0,
            'event_jump_mismatch': 0,
            'factor_base_shift_or_ledger_flag': 0,
            'date_anomaly_at_as_of': 0,
            'content_inconsistent': 0,
            'duplicate_exdiv_key': 0,
            'pre_1993_break': 0,
        }
        for code in all_codes:
            info = self.codes[code]
            if 'structural_no_local_quote' in info.structural:
                defect_breakdown['no_local_quote'] += 1
            if 'structural_content_inconsistent' in info.structural:
                defect_breakdown['content_inconsistent'] += 1
            if 'structural_duplicate_exdiv_key' in info.structural:
                defect_breakdown['duplicate_exdiv_key'] += 1
            if info.stale_start is not None and o >= info.stale_start:
                defect_breakdown['source_stopped_at_as_of'] += 1
            if (
                (info.factor_tail_start is not None and o >= info.factor_tail_start)
                or (info.data_type == 'stock' and self._has_quote(info, o)
                    and not self._has_factor(info, o))
            ):
                defect_breakdown['factor_tail_at_as_of'] += 1
            if info.mismatch_start is not None and o >= info.mismatch_start:
                defect_breakdown['event_jump_mismatch'] += 1
            if info.strict_static:
                defect_breakdown['factor_base_shift_or_ledger_flag'] += 1
            if _pre_1993_break(info, o):
                defect_breakdown['pre_1993_break'] += 1
            if o in info.date_excl_standard or o in info.date_excl_strict:
                defect_breakdown['date_anomaly_at_as_of'] += 1

        st_active = sum(
            1 for info in self.codes.values()
            if info.st_intervals and self._st_status(info, o) is True
        )
        st_active_with_quote = sum(
            1 for info in self.codes.values()
            if info.st_intervals and self._has_quote(info, o)
            and self._st_status(info, o) is True
        )

        # strict 年份分布（按行情起始年）
        def year_bucket(year: int) -> str:
            if year <= 1994:
                return '<=1994'
            if year <= 1999:
                return '1995-1999'
            if year <= 2004:
                return '2000-2004'
            if year <= 2009:
                return '2005-2009'
            if year <= 2014:
                return '2010-2014'
            if year <= 2019:
                return '2015-2019'
            if year <= 2024:
                return '2020-2024'
            return '2025+'

        buckets = ['<=1994', '1995-1999', '2000-2004', '2005-2009',
                   '2010-2014', '2015-2019', '2020-2024', '2025+']
        year_dist = {
            b: {'full': 0, 'strict': 0} for b in buckets
        }
        strict_set = set(view_codes['strict'])
        for code in all_codes:
            info = self.codes[code]
            if info.data_type != 'stock' or info.start is None:
                continue
            b = year_bucket(info.start.year)
            year_dist[b]['full'] += 1
            if code in strict_set:
                year_dist[b]['strict'] += 1

        total_rows = sum(i.rows for i in self.codes.values())
        strict_rows = views['strict']['rows']
        return {
            'as_of': str(d),
            'freshness': source_staleness(
                self.data_last, self.calendar_latest,
                self._calendar_all or tuple(self.calendar)),
            'universe': {
                'codes': len(self.codes),
                'rows': total_rows,
                'by_type': {
                    t: {
                        'codes': sum(1 for i in self.codes.values() if i.data_type == t),
                        'rows': sum(i.rows for i in self.codes.values() if i.data_type == t),
                    } for t in DATA_TYPES
                },
                'no_local_quote_codes': sum(
                    1 for i in self.codes.values()
                    if 'structural_no_local_quote' in i.structural
                ),
                'null_exdiv_duplicate_codes': len(getattr(self, 'null_exdiv_dup_codes', set())),
                'suspected_st_codes': len(getattr(self, 'suspected_st', set())),
            },
            'views': views,
            'layers': layers,
            'reasons': reason_summary,
            'vs_full': vs_full,
            'intersection_matrix': matrix,
            'defect_breakdown': defect_breakdown,
            'st_active_at_as_of': st_active,
            'st_active_with_quote_at_as_of': st_active_with_quote,
            'st_intervals_ongoing_codes': sum(
                1 for i in self.codes.values() if i.st_intervals
            ),
            'multi_reason_codes': len(multi_reason_set),
            'strict_year_distribution': year_dist,
            'strict_rows_pct': (strict_rows / total_rows) if total_rows else 0.0,
        }


def include_suspected_check(engine: EligibilityEngine, info: CodeInfo) -> bool:
    return bool(engine.include_suspected_st and info.ts_code in engine.suspected_st)


# ----------------------------------------------------------------------
# 进程级引擎缓存 + 便捷入口
# ----------------------------------------------------------------------
_ENGINES: dict[str, EligibilityEngine] = {}
_ENGINE_LOCK = threading.RLock()


def get_engine(
    root: str | Path | None = None,
    *,
    target_date: str | date | None = None,
    include_suspected_st: bool = False,
    close_fund_dividend_pending: bool | None = None,
    close_fund_dividend_pending_scope: str | None = None,
) -> EligibilityEngine:
    """获取（并缓存）默认引擎。首次调用会加载全部日期索引。"""
    scope = _resolve_fund_pending_scope(
        close_fund_dividend_pending_scope, close_fund_dividend_pending)
    key = (f'{Path(root) if root else DEFAULT_ROOT}|{target_date}|'
           f'{include_suspected_st}|{scope}')
    with _ENGINE_LOCK:
        engine = _ENGINES.get(key)
        if engine is None:
            engine = EligibilityEngine(
                root, target_date=target_date,
                include_suspected_st=include_suspected_st,
                close_fund_dividend_pending_scope=scope,
            )
            _ENGINES[key] = engine
        return engine


def eligible(
    ts_code: str,
    trade_date: str | date,
    tier: ViewTier = 'strict',
    *,
    include_suspected_st: bool | None = None,
    root: str | Path | None = None,
    close_fund_dividend_pending: bool | None = None,
    close_fund_dividend_pending_scope: str | None = None,
) -> tuple[bool, list[str]]:
    """单标的准入判断（便捷入口）。"""
    engine = get_engine(
        root,
        close_fund_dividend_pending=close_fund_dividend_pending,
        close_fund_dividend_pending_scope=close_fund_dividend_pending_scope,
    )
    return engine.eligible(
        ts_code, trade_date, tier, include_suspected_st=include_suspected_st
    )


def eligible_set(
    trade_date: str | date,
    tier: ViewTier = 'strict',
    *,
    data_type: str | None = None,
    include_suspected_st: bool | None = None,
    root: str | Path | None = None,
    close_fund_dividend_pending: bool | None = None,
    close_fund_dividend_pending_scope: str | None = None,
) -> list[str]:
    """某日某档位的白名单（便捷入口）。"""
    engine = get_engine(
        root,
        close_fund_dividend_pending=close_fund_dividend_pending,
        close_fund_dividend_pending_scope=close_fund_dividend_pending_scope,
    )
    return engine.eligible_set(
        trade_date, tier, data_type=data_type,
        include_suspected_st=include_suspected_st,
    )


# ----------------------------------------------------------------------
# 产物落盘
# ----------------------------------------------------------------------
def _atomic_write_json(path: Path, payload: Any) -> None:
    write_text_atomic(
        json.dumps(payload, ensure_ascii=False, indent=2) + '\n', path)


def _previous_strict_codes(out: Path, as_of: date) -> int | None:
    """同目录中 ``as_of`` 之前最近一份 summary 的 strict 只数。"""
    best: tuple[date, int] | None = None
    for path in out.glob('summary_*.json'):
        try:
            day = date.fromisoformat(path.stem.split('_', 1)[1])
        except ValueError:
            continue
        if day >= as_of:
            continue
        try:
            payload = json.loads(path.read_text('utf-8'))
            count = int(payload['views']['strict']['codes'])
        except (OSError, ValueError, KeyError, TypeError):
            continue
        if best is None or day > best[0]:
            best = (day, count)
    return best[1] if best is not None else None


def _guard_strict_zero(out: Path, as_of: date, strict_codes: int,
                       data_last: date | None) -> None:
    """strict 归零而上一快照显著非零时拒绝落盘（判定异常不得静默写出）。"""
    if strict_codes != 0:
        return
    prev = _previous_strict_codes(out, as_of)
    if prev is None or prev <= STRICT_ZERO_GUARD_MIN_PREV:
        return
    raise RuntimeError(
        f'strict 归零疑似判定异常，已拒绝落盘: as_of={as_of} strict=0，'
        f'上一快照 strict={prev}；行情最新日={data_last}，'
        f'请检查判定时点是否晚于数据末日')


def build_snapshot(
    root: str | Path | None = None,
    as_of: str | date | None = None,
    *,
    output_dir: str | Path | None = None,
    timeline_start: str | date | None = None,
    close_fund_dividend_pending: bool | None = None,
    close_fund_dividend_pending_scope: str | None = None,
) -> dict[str, Any]:
    """构建某日快照 + 月末时间序列 + 汇总，幂等覆盖 ``meta/clean/``。

    ``as_of`` 缺省取数据内最新交易日（见 ``_resolve_dates``）。三份产物先全部
    算完并通过 strict 归零保护，再逐个原子替换：parquet → csv → json，中途
    失败会留下「前几份已更新、后几份仍是旧值」的跨文件不一致，重跑本函数即可
    幂等修复（单文件原子，不是多文件事务）。
    """
    engine = EligibilityEngine(
        root, target_date=as_of,
        close_fund_dividend_pending=close_fund_dividend_pending,
        close_fund_dividend_pending_scope=close_fund_dividend_pending_scope,
    )
    as_of_date = engine.as_of_date
    out = Path(output_dir) if output_dir else (engine.root / 'meta' / 'clean')
    out.mkdir(parents=True, exist_ok=True)

    rows = []
    for code, info in engine.codes.items():
        layer = engine.layer_at(code, as_of_date)
        for view in VIEWS:
            ok, reasons = engine.eligible(code, as_of_date, view)
            rows.append({
                'as_of': as_of_date,
                'view': view,
                'data_type': info.data_type,
                'ts_code': code,
                'eligible': ok,
                'reasons': reasons,
                'layer': layer,
                'rows': info.rows,
                'start': info.start,
                'end': info.end,
            })
    snapshot = pl.DataFrame(rows)

    timeline_rows = engine.timeline(start=timeline_start)
    timeline = pl.DataFrame(timeline_rows)

    summary = engine.summary_at(as_of_date)
    # 样本量下限：strict 股票池连续 12 个月 >= 300 的首个月
    stock_series = [(r['date'], r['strict_stock']) for r in timeline_rows]
    adequate = None
    for idx, (d, count) in enumerate(stock_series):
        if count < MIN_CROSS_SECTION:
            continue
        window = stock_series[idx:idx + MIN_STREAK_MONTHS]
        if len(window) < MIN_STREAK_MONTHS:
            break
        if all(c >= MIN_CROSS_SECTION for _, c in window):
            adequate = d
            break
    summary['timeline'] = {
        'empty': False,
        'months': len(timeline_rows),
        'first_date': timeline_rows[0]['date'] if timeline_rows else None,
        'last_date': timeline_rows[-1]['date'] if timeline_rows else None,
        'min_cross_section': MIN_CROSS_SECTION,
        'min_streak_months': MIN_STREAK_MONTHS,
        'strict_stock_adequate_from': adequate,
        'strict_stock_first_300': next(
            (d for d, c in stock_series if c >= MIN_CROSS_SECTION), None
        ),
        'standard_stock_first_300': next(
            (r['date'] for r in timeline_rows if r['standard_stock'] >= MIN_CROSS_SECTION),
            None
        ),
    }

    snap_path = out / f'eligibility_{as_of_date}.parquet'
    time_path = out / f'timeline_{as_of_date}.csv'
    summary_path = out / f'summary_{as_of_date}.json'
    summary['outputs'] = {
        'snapshot': str(snap_path),
        'timeline': str(time_path),
    }
    _guard_strict_zero(
        out, as_of_date, int(summary['views']['strict']['codes']),
        engine.data_last)

    write_parquet_atomic(snapshot, snap_path)
    write_text_atomic(timeline.write_csv(), time_path)
    _atomic_write_json(summary_path, summary)
    logger.info(f'洁净集快照已写入 {out}')
    return summary


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------
def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='按日期求值的洁净集准入闸门')
    sub = parser.add_subparsers(dest='cmd', required=True)

    p_build = sub.add_parser('build', help='构建快照 + 时间序列 + 汇总')
    p_build.add_argument('--as-of', default=None, help='目标日期 YYYY-MM-DD')
    p_build.add_argument('--root', default=None)
    p_build.add_argument('--out', default=None)
    p_build.add_argument('--timeline-start', default=None)
    p_build.add_argument(
        '--close-fund-dividend-pending', action='store_true', default=None,
        help='旧布尔开关兼容：等价于 scope=all')
    p_build.add_argument(
        '--close-fund-dividend-pending-scope', default=None,
        choices=FUND_DIVIDEND_PENDING_SCOPES,
        help='基金分红 pending 开关范围：none（默认）/ etf（仅 fund_basic ETF）/ all')

    p_codes = sub.add_parser('codes', help='输出某日某档位白名单')
    p_codes.add_argument('--date', required=True)
    p_codes.add_argument('--tier', default='strict', choices=VIEWS)
    p_codes.add_argument('--data-type', default=None, choices=DATA_TYPES)
    p_codes.add_argument('--root', default=None)
    p_codes.add_argument(
        '--close-fund-dividend-pending', action='store_true', default=None)
    p_codes.add_argument(
        '--close-fund-dividend-pending-scope', default=None,
        choices=FUND_DIVIDEND_PENDING_SCOPES)

    p_check = sub.add_parser('check', help='检查单票单日')
    p_check.add_argument('--code', required=True)
    p_check.add_argument('--date', required=True)
    p_check.add_argument('--tier', default='strict', choices=VIEWS)
    p_check.add_argument('--root', default=None)
    p_check.add_argument(
        '--close-fund-dividend-pending', action='store_true', default=None)
    p_check.add_argument(
        '--close-fund-dividend-pending-scope', default=None,
        choices=FUND_DIVIDEND_PENDING_SCOPES)

    args = parser.parse_args(argv)
    if args.cmd == 'build':
        summary = build_snapshot(
            root=args.root, as_of=args.as_of, output_dir=args.out,
            timeline_start=args.timeline_start,
            close_fund_dividend_pending=args.close_fund_dividend_pending,
            close_fund_dividend_pending_scope=args.close_fund_dividend_pending_scope,
        )
        sys.stdout.write(json.dumps(summary, ensure_ascii=False, indent=2) + '\n')
        return 0
    if args.cmd == 'codes':
        codes = eligible_set(
            args.date, args.tier, data_type=args.data_type, root=args.root,
            close_fund_dividend_pending=args.close_fund_dividend_pending,
            close_fund_dividend_pending_scope=args.close_fund_dividend_pending_scope,
        )
        sys.stdout.write(json.dumps(codes, ensure_ascii=False) + '\n')
        return 0
    engine = get_engine(
        args.root,
        close_fund_dividend_pending=args.close_fund_dividend_pending,
        close_fund_dividend_pending_scope=args.close_fund_dividend_pending_scope,
    )
    ok, reasons = engine.eligible(args.code, args.date, args.tier)
    sys.stdout.write(json.dumps({'eligible': ok, 'reasons': reasons}, ensure_ascii=False) + '\n')
    return 0


if __name__ == '__main__':
    raise SystemExit(_main())
