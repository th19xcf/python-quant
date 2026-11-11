#!/usr/bin/env python3

r"""选池阶段通用过滤：可配置阈值 + 逐类剔除计数。

本模块是「每日推荐」在因子打分之前的**硬性选池**，与
:mod:`src.data.industry_universe` 的分工是：

1. 先按申万 PIT 定行业成分（``industry_universe.py``，本模块不参与）；
2. 再对行业成分调用 :func:`filter_universe` 做通用过滤。

所有规则只用 ``as_of`` 当日及之前可见的数据，逐条来源见
``docs/每日推荐筛选与判断清单.md``。

明确不做的事
------------
**不在选池里剔除涨停**：T 日涨停不代表 T+1 买不到，正确做法是撮合层用
T+1 开盘价判定能否成交。涨跌停拒单见
``src/backtest/execution_core.py::_check_order_executable``。

已知的过滤边界（不在本模块实现，调用方需知晓）
--------------------------------------------
- 涨跌停 / 一字板：``src/advice/eligibility.py`` 对 T 日涨停只做
  ``limit_up_risk`` 标注（不拦截），成交判定在撮合层；
- 退市整理板：只认 ``meta/stock_basic.parquet`` 的 ``status`` 与 ``delist_date``，
  不再用简称含「退」的字符串匹配。**数据缺口**：当前该表对整理期标的尚未回填
  ``delist_date``（status 仍为 ``L``），这类标的在表中没有退市痕迹，会漏剔；
- 财务类「审计非标」：本地无审计意见字段（``financials_pit`` 四张表都没有
  ``audit_opinion`` 一类列），**无数据支撑，未实现**；净资产为负与连续两年
  亏损已按公告日实现，但默认关闭。

读侧用法::

    from src.data.universe_filter import filter_universe

    result = filter_universe('2026-11-09', codes)
    result.kept              # 保留代码
    result.rule_counts       # 逐类剔除计数（按 RULE_ORDER）
    result.counts_text()     # 一行可读摘要

命令行（真实数据逐类计数）::

    .venv\Scripts\python.exe -m src.data.universe_filter --as-of 2026-11-09 --json
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import polars as pl
from loguru import logger

from src.data.quality.code_migration import load_entity_index
from src.data.quality.suspension_union import SuspensionUnion, load_suspension_union
from src.data.risk_flags import get_st_status

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ROOT = PROJECT_ROOT / 'data' / 'local_store'

#: 规则版本号：清单文件与每日输出都打印它，便于事后核对「这批名单按哪版规则筛出」
RULES_VERSION = '2026-11-11.2'
#: 规则说明文档（随每日输出一起打印路径）
DOC_PATH = 'docs/每日推荐筛选与判断清单.md'

# 逐类剔除码（顺序即判定顺序：命中第一条即停，计数互斥）
RULE_BOARD = 'board'
RULE_DELISTED = 'delisted'
RULE_DELISTING = 'delisting_period'
RULE_SUPERSEDED = 'superseded_code'
RULE_NOT_LISTED = 'not_listed_today'
RULE_ST = 'st'
RULE_SUSPENDED = 'suspended_today'
RULE_SUSPENSION_COUNT = 'suspension_days'
RULE_RESUMPTION = 'resumption_day'
RULE_NEW_LISTING = 'new_listing'
RULE_NO_QUOTE = 'no_quote'
RULE_LOW_PRICE = 'low_price'
RULE_ILLIQUID = 'illiquid'
RULE_LOW_CIRC_MV = 'low_circ_mv'
RULE_FINANCIAL_RISK = 'financial_risk'

RULE_ORDER: tuple[str, ...] = (
    RULE_BOARD,
    RULE_DELISTED,
    RULE_DELISTING,
    RULE_SUPERSEDED,
    RULE_NOT_LISTED,
    RULE_ST,
    RULE_SUSPENDED,
    RULE_SUSPENSION_COUNT,
    RULE_RESUMPTION,
    RULE_NEW_LISTING,
    RULE_NO_QUOTE,
    RULE_LOW_PRICE,
    RULE_ILLIQUID,
    RULE_LOW_CIRC_MV,
    RULE_FINANCIAL_RISK,
)

RULE_LABELS: dict[str, str] = {
    RULE_BOARD: '板块（B股/老三板/北交所/科创/创业开关）',
    RULE_DELISTED: '已退市（stock_basic.status=D 且 delist_date ≤ as_of）',
    RULE_DELISTING: '退市整理期（距 delist_date ≤ N 个交易日）',
    RULE_SUPERSEDED: '旧码/迁移码（由新码承载）',
    RULE_NOT_LISTED: '当日不在市（未上市或已退市）',
    RULE_ST: 'ST/*ST/退市风险警示',
    RULE_SUSPENDED: '当日停牌',
    RULE_SUSPENSION_COUNT: '近 N 日停牌天数超限',
    RULE_RESUMPTION: '停牌后复牌首日',
    RULE_NEW_LISTING: '上市未满 N 个交易日',
    RULE_NO_QUOTE: '当日无行情',
    RULE_LOW_PRICE: '收盘价低于面值门槛',
    RULE_ILLIQUID: '成交额不足',
    RULE_LOW_CIRC_MV: '流通市值不足（默认关闭）',
    RULE_FINANCIAL_RISK: '财务类风险（默认关闭）',
}

BOARD_MAIN = 'main'
BOARD_BSE = 'bse'
BOARD_STAR = 'star'
BOARD_CHINEXT = 'chinext'
BOARD_B_SHARE = 'b_share'
BOARD_OLD_THREE = 'old_three_board'
BOARD_DELISTING = 'delisting_period'

BOARD_LABELS: dict[str, str] = {
    BOARD_MAIN: '沪深主板',
    BOARD_BSE: '北交所',
    BOARD_STAR: '科创板',
    BOARD_CHINEXT: '创业板',
    BOARD_B_SHARE: 'B股',
    BOARD_OLD_THREE: '老三板',
    BOARD_DELISTING: '退市整理板',
}


@dataclass(frozen=True)
class UniverseFilterConfig:
    """选池过滤配置：所有阈值集中在这里，不写进 ``src/advice/config.py``。

    想改阈值只改本对象（或构造时覆盖），不要在调用点散落魔法数字。
    """

    # --- ST / 停牌 ---
    enable_st_filter: bool = True
    enable_suspension_filter: bool = True
    suspension_window: int = 20
    max_suspension_days: int = 5
    exclude_resumption_day: bool = True
    # --- 次新 ---
    min_listed_trading_days: int = 120
    # --- 流动性 / 面值 ---
    amount_window: int = 20
    min_amount: float = 1.0e7
    min_price: float = 1.5
    # --- 板块 ---
    exclude_b_share: bool = True
    exclude_delisting_period: bool = True
    exclude_delisted: bool = True
    #: 退市整理期判定窗口：距 delist_date 剩余不超过 N 个交易日
    delisting_window_trading_days: int = 30
    exclude_old_three_board: bool = True
    include_bse: bool = False
    include_star: bool = True
    include_chinext: bool = True
    # --- 旧码 ---
    exclude_superseded: bool = True
    # --- 市值下限（默认关闭） ---
    enable_circ_mv_floor: bool = False
    min_circ_mv: float = 1.0e9
    # --- 财务类风险（默认关闭） ---
    enable_financial_risk: bool = False
    max_annual_loss_years: int = 2


@dataclass
class UniverseFilterResult:
    """一次过滤的完整结果：保留代码 + 逐类剔除计数 + 数据缺口。"""

    as_of: date
    input_codes: list[str]
    kept: list[str]
    dropped: dict[str, list[str]] = field(default_factory=dict)
    unknowns: dict[str, int] = field(default_factory=dict)
    config: UniverseFilterConfig = field(default_factory=UniverseFilterConfig)
    version: str = RULES_VERSION
    doc_path: str = DOC_PATH

    @property
    def input_count(self) -> int:
        return len(self.input_codes)

    @property
    def kept_count(self) -> int:
        return len(self.kept)

    @property
    def dropped_count(self) -> int:
        return self.input_count - self.kept_count

    @property
    def rule_counts(self) -> dict[str, int]:
        """按 ``RULE_ORDER`` 的逐类剔除计数（未触发的规则计 0，不省略）。"""
        return {rule: len(self.dropped.get(rule, ())) for rule in RULE_ORDER}

    def counts_text(self) -> str:
        """一行摘要，供日志与每日输出使用。"""
        parts = [f'{rule}={count}' for rule, count in self.rule_counts.items() if count]
        detail = '，'.join(parts) if parts else '无剔除'
        return (f'选池 {self.input_count} → {self.kept_count}（剔除 {self.dropped_count}）：'
                f'{detail}')

    def as_dict(self) -> dict[str, Any]:
        return {
            'as_of': self.as_of.isoformat(),
            'version': self.version,
            'doc_path': self.doc_path,
            'input': self.input_count,
            'kept': self.kept_count,
            'dropped_total': self.dropped_count,
            'rule_order': list(RULE_ORDER),
            'rule_labels': {rule: RULE_LABELS[rule] for rule in RULE_ORDER},
            'rule_counts': self.rule_counts,
            'dropped_codes': {rule: codes for rule, codes in self.dropped.items() if codes},
            'unknowns': self.unknowns,
            'config': {
                'suspension_window': self.config.suspension_window,
                'max_suspension_days': self.config.max_suspension_days,
                'min_listed_trading_days': self.config.min_listed_trading_days,
                'amount_window': self.config.amount_window,
                'min_amount': self.config.min_amount,
                'min_price': self.config.min_price,
                'include_bse': self.config.include_bse,
                'include_star': self.config.include_star,
                'include_chinext': self.config.include_chinext,
                'exclude_delisted': self.config.exclude_delisted,
                'exclude_delisting_period': self.config.exclude_delisting_period,
                'delisting_window_trading_days':
                    self.config.delisting_window_trading_days,
                'enable_circ_mv_floor': self.config.enable_circ_mv_floor,
                'min_circ_mv': (self.config.min_circ_mv
                                if self.config.enable_circ_mv_floor else None),
                'enable_financial_risk': self.config.enable_financial_risk,
            },
        }


# ----------------------------------------------------------------------
# 板块判定
# ----------------------------------------------------------------------
def board_of(ts_code: str) -> str:
    """按代码段判定板块（纯代码规则，不用名称）。"""
    text = str(ts_code).strip().upper()
    digits = text.split('.')[0]
    suffix = text.rsplit('.', 1)[-1] if '.' in text else ''
    if suffix == 'BJ' or digits.startswith(('83', '87', '88', '920')):
        return BOARD_BSE
    if suffix == 'SH' and digits.startswith('900'):
        return BOARD_B_SHARE
    if suffix == 'SZ' and digits.startswith('200'):
        return BOARD_B_SHARE
    if digits.startswith(('400', '420')):
        return BOARD_OLD_THREE
    if digits.startswith(('688', '689')):
        return BOARD_STAR
    if digits.startswith(('300', '301')):
        return BOARD_CHINEXT
    return BOARD_MAIN


def _board_excluded(board: str, config: UniverseFilterConfig) -> bool:
    if board == BOARD_B_SHARE:
        return config.exclude_b_share
    if board == BOARD_OLD_THREE:
        return config.exclude_old_three_board
    if board == BOARD_BSE:
        return not config.include_bse
    if board == BOARD_STAR:
        return not config.include_star
    if board == BOARD_CHINEXT:
        return not config.include_chinext
    return False


# ----------------------------------------------------------------------
# 数据读取（全部只读，不写任何文件）
# ----------------------------------------------------------------------
def _default_store_root() -> Path:
    try:
        from src.data.local_store.store import resolve_store_root  # noqa: PLC0415
        from src.utils.config import get_config  # noqa: PLC0415

        return resolve_store_root(get_config())
    except (OSError, ValueError, KeyError, AttributeError):  # pragma: no cover
        return DEFAULT_ROOT


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _load_listing_spans(root: Path) -> dict[str, tuple[date | None, date | None]]:
    """``meta/daily_index.parquet`` 的个股在市区间（首末日行情）。"""
    path = root / 'meta' / 'daily_index.parquet'
    if not path.is_file():
        return {}
    frame = pl.read_parquet(path, columns=['ts_code', 'data_type', 'start', 'end'])
    frame = frame.filter(pl.col('data_type') == 'stock')
    spans: dict[str, tuple[date | None, date | None]] = {}
    for row in frame.to_dicts():
        start = row['start'] if isinstance(row['start'], date) else None
        end = row['end'] if isinstance(row['end'], date) else None
        spans[str(row['ts_code'])] = (start, end)
    return spans


def _load_stock_basic(root: Path) -> dict[str, tuple[str | None, date | None]]:
    """``meta/stock_basic.parquet`` 的退市字段：``code → (status, delist_date)``。

    退市整理板不再用简称含「退」这类字符串匹配，只认这份表的
    ``status``（``L``/``D``）与 ``delist_date``。
    """
    path = root / 'meta' / 'stock_basic.parquet'
    if not path.is_file():
        return {}
    try:
        frame = pl.read_parquet(path, columns=['ts_code', 'status', 'delist_date'])
    except (OSError, pl.exceptions.PolarsError):
        return {}
    basic: dict[str, tuple[str | None, date | None]] = {}
    for row in frame.to_dicts():
        status = row.get('status')
        delist_day = row.get('delist_date')
        basic[str(row['ts_code']).upper()] = (
            str(status).strip().upper() if status else None,
            delist_day if isinstance(delist_day, date) else None,
        )
    return basic


def _read_quotes(store: Any, code: str, start: date) -> pl.DataFrame | None:
    """读单只标的的原始日线（只取判定需要的列），不做因子覆盖。"""
    frame = store.read_file('stock', code)
    if frame is None or frame.is_empty():
        return None
    keep = [name for name in ('date', 'close', 'amount') if name in frame.columns]
    if 'date' not in keep:
        return None
    return frame.select(keep).filter(pl.col('date') >= start)


def _suspension_counts(union: SuspensionUnion, code: str,
                       window: tuple[date, ...]) -> int:
    dates = union.dates_for(code)
    if not dates:
        return 0
    return sum(1 for day in window if day in dates)


def _financial_risk_codes(codes: list[str], as_of: date, root: Path,
                          config: UniverseFilterConfig) -> tuple[set[str], int]:
    """财务类风险：净资产为负 + 连续 N 个年报亏损（按公告日可见）。

    「审计非标」本地无字段，未实现——这里只做前两条，且严格按 ``ann_date``
    取「截至 as_of 已公告」的报表，不用 ``end_date`` 凑。
    返回 (命中代码集合, 无数据代码数)。
    """
    from src.data.financials.store import (  # noqa: PLC0415
        latest_announced_panel,
        read_table,
    )

    risky: set[str] = set()
    missing = 0
    balance = latest_announced_panel(codes, as_of, 'balance', store_root=root)
    equity_col = 'total_equity'
    equity: dict[str, float | None] = {}
    if not balance.is_empty() and equity_col in balance.columns:
        for row in balance.select(['ts_code', equity_col]).to_dicts():
            value = row[equity_col]
            equity[str(row['ts_code'])] = float(value) if value is not None else None
    for code in codes:
        value = equity.get(code)
        if value is None:
            missing += 1
        elif value < 0:
            risky.add(code)

    income = read_table('income', store_root=root, codes=codes)
    if income.is_empty():
        return risky, missing
    annual = income.filter(
        pl.col('ann_date').is_not_null()
        & (pl.col('ann_date') <= as_of)
        & (pl.col('end_date').dt.month() == 12)
        & (pl.col('end_date').dt.day() == 31)
    ).select(['ts_code', 'ann_date', 'end_date', 'parent_net_profit'])
    if annual.is_empty():
        return risky, missing
    annual = annual.sort(['ts_code', 'end_date', 'ann_date']).group_by(
        ['ts_code', 'end_date'], maintain_order=True).agg(
        pl.col('parent_net_profit').last().alias('profit'))
    for row in annual.group_by('ts_code', maintain_order=True).agg(
            pl.all().sort_by('end_date').tail(config.max_annual_loss_years)
    ).to_dicts():
        profits = [value for value in row['profit'] if value is not None]
        if len(profits) >= config.max_annual_loss_years and all(p < 0 for p in profits):
            risky.add(str(row['ts_code']))
    return risky, missing


def _circ_mv_codes(codes: list[str], as_of: date, root: Path,
                   config: UniverseFilterConfig) -> tuple[set[str], int]:
    """流通市值下限命中；返回 (命中代码集合, 无估值数据代码数)。

    本地无「自由流通市值」口径（缺自由流通股本表），退用 ``circ_mv``
    （流通市值，口径更宽、剔除更少），默认关闭。口径差异见清单文档。
    """
    from src.data.financials.store import latest_valuation  # noqa: PLC0415

    frame = latest_valuation(as_of=as_of, codes=codes, store_root=root)
    if frame.is_empty() or 'circ_mv' not in frame.columns:
        return set(), len(codes)
    risky: set[str] = set()
    seen: set[str] = set()
    for row in frame.select(['ts_code', 'circ_mv']).to_dicts():
        code = str(row['ts_code'])
        seen.add(code)
        value = row['circ_mv']
        if value is not None and float(value) < config.min_circ_mv:
            risky.add(code)
    return risky, len(codes) - len(seen)


# ----------------------------------------------------------------------
# 主过滤
# ----------------------------------------------------------------------
def filter_universe(as_of: Any, codes: Iterable[str], *,
                    config: UniverseFilterConfig | None = None,
                    store_root: Path | str | None = None) -> UniverseFilterResult:
    """对候选代码做通用选池过滤，返回保留代码与逐类剔除统计。

    Args:
        as_of: 信号日（含当日），全部判定只看该日及之前可见的数据。
        codes: 候选代码集合（通常来自 ``industry_universe`` 的行业成分）。
        config: 阈值配置；缺省用 :class:`UniverseFilterConfig` 的默认值。
        store_root: 本地列存根目录，缺省按项目配置解析。

    Returns:
        :class:`UniverseFilterResult`：``kept`` 为保留代码，``dropped`` 按规则
        分组，``rule_counts`` 为逐类剔除计数（互斥，命中首条规则即停）。
    """
    cfg = config or UniverseFilterConfig()
    day = _as_date(as_of)
    root = Path(store_root) if store_root is not None else _default_store_root()

    ordered: list[str] = []
    seen: set[str] = set()
    for item in codes:
        text = str(item).strip().upper()
        if text and text not in seen:
            seen.add(text)
            ordered.append(text)

    dropped: dict[str, list[str]] = {rule: [] for rule in RULE_ORDER}
    unknowns: dict[str, int] = {'st': 0, 'listing': 0, 'amount': 0,
                                'circ_mv': 0, 'financial': 0, 'delist': 0}

    from src.data.local_store.store import LocalQuoteStore  # noqa: PLC0415
    from src.data.trading_calendar import get_calendar  # noqa: PLC0415

    calendar = get_calendar(root, as_of=day)
    entity_index = load_entity_index(root)
    spans = _load_listing_spans(root)
    basic = _load_stock_basic(root)
    union = load_suspension_union(root) if cfg.enable_suspension_filter else None
    window = _trading_window(calendar, day, cfg.suspension_window)
    prev_day = calendar.previous(day, inclusive=False)

    survivors = _apply_static_rules(
        ordered, cfg=cfg, day=day, calendar=calendar, window=window,
        prev_day=prev_day, union=union, entity_index=entity_index,
        spans=spans, basic=basic, root=root, dropped=dropped, unknowns=unknowns)
    kept = _apply_quote_rules(
        survivors, cfg=cfg, day=day, calendar=calendar,
        store=LocalQuoteStore(root), dropped=dropped, unknowns=unknowns)
    kept = _apply_market_cap_floor(kept, cfg=cfg, day=day, root=root,
                                   dropped=dropped, unknowns=unknowns)
    kept = _apply_financial_risk(kept, cfg=cfg, day=day, root=root,
                                 dropped=dropped, unknowns=unknowns)

    logger.info('[universe-filter] as_of={} {} {}；保留 {}/{}', day, RULES_VERSION,
                '，'.join(f'{rule}={len(codes_)}'
                          for rule, codes_ in dropped.items() if codes_) or '无剔除',
                len(kept), len(ordered))
    return UniverseFilterResult(as_of=day, input_codes=ordered, kept=kept,
                                dropped=dropped, unknowns=unknowns, config=cfg)


def _trading_window(calendar: Any, day: date, size: int) -> tuple[date, ...]:
    """``day`` 及之前最近 ``size`` 个交易日（含当日）。"""
    start = day - timedelta(days=size * 3)
    return tuple(calendar.between(start, day))[-size:]


def _apply_static_rules(
    ordered: list[str], *, cfg: UniverseFilterConfig, day: date,
    calendar: Any, window: tuple[date, ...], prev_day: date | None,
    union: SuspensionUnion | None, entity_index: Any,
    spans: dict[str, tuple[date | None, date | None]],
    basic: dict[str, tuple[str | None, date | None]],
    root: Path, dropped: dict[str, list[str]], unknowns: dict[str, int],
) -> list[str]:
    """板块 / 旧码 / 在市 / ST / 停牌 / 复牌首日 / 次新：不读行情即可判定。"""
    survivors: list[str] = []
    for code in ordered:
        reason = _static_drop_reason(
            code, cfg=cfg, day=day, calendar=calendar, window=window,
            prev_day=prev_day, union=union, entity_index=entity_index,
            spans=spans, basic=basic, root=root, unknowns=unknowns)
        if reason is None:
            survivors.append(code)
        else:
            dropped[reason].append(code)
    return survivors


def _board_reason(code: str, cfg: UniverseFilterConfig) -> str | None:
    """板块类剔除：B股 / 老三板 / 北交所开关 / 科创·创业开关（纯代码段判定）。"""
    return RULE_BOARD if _board_excluded(board_of(code), cfg) else None


def _delisting_reason(code: str, cfg: UniverseFilterConfig, day: date,
                      basic: dict[str, tuple[str | None, date | None]],
                      calendar: Any,
                      unknowns: dict[str, int]) -> str | None:
    """退市类剔除：已退市 / 退市整理期，只认 ``stock_basic`` 的 status 与 delist_date。

    - 已退市：``status == 'D'`` 且 ``delist_date`` 为空或不晚于 ``day``；
    - 退市整理期：``delist_date`` 晚于 ``day``，且剩余交易日不超过
      ``delisting_window_trading_days``（默认 30，与交易所整理期长度一致）。
    """
    if not (cfg.exclude_delisted or cfg.exclude_delisting_period):
        return None
    info = basic.get(code)
    if info is None:
        unknowns['delist'] += 1
        return None
    status, delist_day = info
    if (cfg.exclude_delisted and status == 'D'
            and (delist_day is None or delist_day <= day)):
        return RULE_DELISTED
    if cfg.exclude_delisting_period and delist_day is not None and delist_day > day:
        window = cfg.delisting_window_trading_days
        if window <= 0 or calendar.count(day, delist_day) <= window:
            return RULE_DELISTING
    return None


def _state_drop_reason(code: str, *, cfg: UniverseFilterConfig, day: date,
                       window: tuple[date, ...], prev_day: date | None,
                       union: SuspensionUnion | None, root: Path,
                       unknowns: dict[str, int]) -> str | None:
    """ST / 当日停牌 / 近 N 日停牌超限 / 复牌首日。"""
    if cfg.enable_st_filter:
        status = get_st_status(code, day, store_root=root)
        if status == 'st':
            return RULE_ST
        if status == 'unknown':
            unknowns['st'] += 1
    if cfg.enable_suspension_filter and union is not None:
        if union.is_suspended(code, day):
            return RULE_SUSPENDED
        if _suspension_counts(union, code, window) > cfg.max_suspension_days:
            return RULE_SUSPENSION_COUNT
        if (cfg.exclude_resumption_day and prev_day is not None
                and union.is_suspended(code, prev_day)):
            return RULE_RESUMPTION
    return None


def _static_drop_reason(
    code: str, *, cfg: UniverseFilterConfig, day: date, calendar: Any,
    window: tuple[date, ...], prev_day: date | None,
    union: SuspensionUnion | None, entity_index: Any,
    spans: dict[str, tuple[date | None, date | None]],
    basic: dict[str, tuple[str | None, date | None]],
    root: Path, unknowns: dict[str, int],
) -> str | None:
    """按 RULE_ORDER 依次判定，返回首个命中的剔除码（互斥计数）。"""
    reason = _board_reason(code, cfg)
    if reason is not None:
        return reason
    reason = _delisting_reason(code, cfg, day, basic, calendar, unknowns)
    if reason is not None:
        return reason
    if cfg.exclude_superseded and entity_index.is_superseded(code):
        return RULE_SUPERSEDED
    first_day, last_day = spans.get(code, (None, None))
    if first_day is None or last_day is None:
        unknowns['listing'] += 1
    elif day < first_day or day > last_day:
        return RULE_NOT_LISTED
    reason = _state_drop_reason(code, cfg=cfg, day=day, window=window,
                                prev_day=prev_day, union=union, root=root,
                                unknowns=unknowns)
    if reason is not None:
        return reason
    if (first_day is not None
            and calendar.count(first_day, day) + 1 < cfg.min_listed_trading_days):
        return RULE_NEW_LISTING
    return None


def _apply_quote_rules(
    survivors: list[str], *, cfg: UniverseFilterConfig, day: date,
    calendar: Any, store: Any, dropped: dict[str, list[str]],
    unknowns: dict[str, int],
) -> list[str]:
    """当日无行情 / 面值 / 20 日均额：读原始日线判定。"""
    trade_window = _trading_window(calendar, day, cfg.amount_window)
    quote_start = trade_window[0] if trade_window else day
    dates_in_window = list(trade_window)
    kept: list[str] = []
    for code in survivors:
        frame = _read_quotes(store, code, quote_start)
        if frame is None or frame.filter(pl.col('date') == day).is_empty():
            dropped[RULE_NO_QUOTE].append(code)
            continue
        row = frame.filter(pl.col('date') == day).row(-1, named=True)
        close = row.get('close')
        if close is None or float(close) < cfg.min_price:
            dropped[RULE_LOW_PRICE].append(code)
            continue
        if 'amount' not in frame.columns:
            dropped[RULE_ILLIQUID].append(code)
            unknowns['amount'] += 1
            continue
        amounts = frame.filter(pl.col('date').is_in(dates_in_window)
                               & pl.col('amount').is_not_null())['amount']
        if amounts.len() == 0:
            dropped[RULE_ILLIQUID].append(code)
            unknowns['amount'] += 1
            continue
        mean_amount = amounts.mean()
        if not isinstance(mean_amount, (int, float)) or mean_amount < cfg.min_amount:
            dropped[RULE_ILLIQUID].append(code)
            continue
        kept.append(code)
    return kept


def _apply_market_cap_floor(
    codes: list[str], *, cfg: UniverseFilterConfig, day: date, root: Path,
    dropped: dict[str, list[str]], unknowns: dict[str, int],
) -> list[str]:
    """流通市值下限（默认关闭）。"""
    if not cfg.enable_circ_mv_floor or not codes:
        return codes
    risky, missing = _circ_mv_codes(codes, day, root, cfg)
    unknowns['circ_mv'] += missing
    kept = [code for code in codes if code not in risky]
    dropped[RULE_LOW_CIRC_MV].extend(code for code in codes if code in risky)
    return kept


def _apply_financial_risk(
    codes: list[str], *, cfg: UniverseFilterConfig, day: date, root: Path,
    dropped: dict[str, list[str]], unknowns: dict[str, int],
) -> list[str]:
    """财务类风险（默认关闭，按公告日判定）。"""
    if not cfg.enable_financial_risk or not codes:
        return codes
    risky, missing = _financial_risk_codes(codes, day, root, cfg)
    unknowns['financial'] += missing
    kept = [code for code in codes if code not in risky]
    dropped[RULE_FINANCIAL_RISK].extend(code for code in codes if code in risky)
    return kept


# ----------------------------------------------------------------------
# 命令行：真实数据逐类计数
# ----------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='选池阶段通用过滤（逐类剔除计数）')
    parser.add_argument('--as-of', default=None, help='信号日 YYYY-MM-DD（默认最新行情日）')
    parser.add_argument('--store-root', default=str(DEFAULT_ROOT))
    parser.add_argument('--json', action='store_true', help='输出完整 JSON')
    parser.add_argument('--include-bse', action='store_true', help='保留北交所')
    parser.add_argument('--enable-circ-mv', action='store_true',
                        help='启用流通市值下限（默认关闭）')
    parser.add_argument('--enable-financial-risk', action='store_true',
                        help='启用财务类风险（默认关闭）')
    args = parser.parse_args(argv)

    root = Path(args.store_root)
    from src.data.local_store.store import LocalQuoteStore  # noqa: PLC0415

    store = LocalQuoteStore(root)
    codes = store.codes('stock')
    day: date
    if args.as_of:
        day = _as_date(args.as_of)
    else:
        latest = pl.read_parquet(root / 'meta' / 'daily_index.parquet',
                                 columns=['data_type', 'end'])
        day = max(item for item in latest.filter(
            pl.col('data_type') == 'stock')['end'].to_list() if isinstance(item, date))

    config = UniverseFilterConfig(
        include_bse=args.include_bse,
        enable_circ_mv_floor=args.enable_circ_mv,
        enable_financial_risk=args.enable_financial_risk,
    )
    result = filter_universe(day, codes, config=config, store_root=root)
    if args.json:
        sys.stdout.write(json.dumps(result.as_dict(), ensure_ascii=False,
                                    indent=2, default=str) + '\n')
        return 0
    lines = [f'规则版本 {RULES_VERSION}（{DOC_PATH}）',
             f'as_of={day} 输入 {result.input_count} 只 → 保留 {result.kept_count} 只']
    for rule in RULE_ORDER:
        count = len(result.dropped[rule])
        if count:
            lines.append(f'  {RULE_LABELS[rule]:<28} {count:>5}')
    lines.append(f'  数据缺口（保留但不确定）: {result.unknowns}')
    sys.stdout.write('\n'.join(lines) + '\n')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
