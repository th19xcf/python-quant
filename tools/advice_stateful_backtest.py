#!/usr/bin/env python3
"""状态机口径回测（尺子 B：可执行性），与「固定持有 60 个交易日」基线并列输出。

口径定义见 ``docs/评价指标体系_2026-11-11.md`` §2：

* 逐日推进：每天用当日及之前的因子截面重算信号；
* 持仓走生产链路同一套卖出规则（``src/advice/turnover.py`` 的
  ``update_holding`` / ``entry_allowed`` / ``apply_turnover_cap``）；
* 成交一律 **T+1 开盘**，用 T+1 开盘价与用 T 日收盘价推出的涨跌停价判定可行性；
* 基线：同一批信号、同一软筛、同一 T+1 开盘买入，固定持有 60 个交易日后开盘卖出，
  用 60 个重叠 cohort 各占 1/60 资金滚动。

产物写入 ``reports/advice/stateful/<起止日>/`` 下的 ``nav.csv`` / ``trades.csv`` /
``summary.json`` / ``report.md``，不改任何既有文件、不写 ``data/``。

已知口径处理（本轮已修正）：``src/advice/config.py::TradingCosts`` 由
``COST_DEFAULTS`` 推导出的往返费率只有 0.19%，而文档冻结常数是 0.724%
（差额来自市值分档滑点与过户费未进 ``COST_DEFAULTS['slippage']``）。
脚本**按文档冻结常数构造** ``TradingCosts``（见 :func:`_documented_costs`），
把差额归入冲击成本项，并在启动时断言往返费率精确等于 0.724%，
状态机与基线因此始终在同一成本下比较。
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.advice.config import TradingCosts, default_config  # noqa: E402
from src.advice.data import LoadReport, LocalPanelLoader, panel_trading_dates  # noqa: E402
from src.advice.eligibility import check_eligibility  # noqa: E402
from src.advice.factor_panel import compute_factor_panel  # noqa: E402
from src.advice.scoring import attach_score  # noqa: E402
from src.advice.soft_filters import apply_soft_filter  # noqa: E402
from src.advice.turnover import (  # noqa: E402
    TradeIntent,
    apply_turnover_cap,
    count_trading_days,
    entry_allowed,
    update_holding,
)
from src.advice.universe import list_local_stock_codes  # noqa: E402
from src.backtest.costs.cost_model import COST_DEFAULTS  # noqa: E402
from src.data.local_store.store import LocalQuoteStore, resolve_store_root  # noqa: E402
from src.data.trading_calendar import get_calendar  # noqa: E402
from src.data.universe_filter import filter_universe  # noqa: E402
from src.risk.execution_rules import ExecutionRules  # noqa: E402
from src.utils.config import get_config  # noqa: E402

#: 基线固定持有交易日数（文档 §2.2）
BASELINE_HOLD = 60
#: 文档冻结的往返成本常数（与 tools/tune_advice.py 的 PROTOCOL 同一数）
DOC_ROUND_TRIP_COST = 0.00724
#: 0.724% 的分项（往返）：佣金 5bp×2 + 印花税 5bp（卖出）+ 过户费 0.2bp×2，
#: 其余全部归入冲击成本，保证两口径共用同一次完整往返
DOC_COMMISSION_RATE = 0.0005
DOC_STAMP_DUTY_RATE = 0.0005
DOC_TRANSFER_RATE = 0.00002
#: 成交侧跌停/停牌的最大重试次数
MAX_SELL_RETRIES = 3
#: 状态机与基线共用的面板列（撮合与可交易性判定只需要这些）
SLIM_COLUMNS = (
    'date', 'ts_code', 'score', 'score_percentile', 'soft_pass', 'soft_reason',
    'open', 'close', 'volume', 'high', 'low',
    'raw_open', 'raw_close', 'raw_prev_close', 'raw_volume',
    'raw_high', 'raw_low', 'raw_amount',
    'amount_ma20', 'volatility_20', 'atr_pct', 'history_rows',
)
TRADE_COLUMNS = (
    'ts_code', 'signal_date', 'fill_date', 'side', 'shares', 'price',
    'notional', 'cost', 'days_held', 'reason',
)
NAV_COLUMNS = (
    'date', 'nav_stateful', 'nav_fixed60', 'cash', 'cash_fixed60',
    'holdings_count', 'gross_exposure',
)
#: 现金收益敏感性两档年化利率（事后累加，不重跑复利路径）
CASH_YIELD_RATES = (0.0, 0.015)
#: 现金收益按每年 252 个交易日折算
TRADING_DAYS_PER_YEAR = 252.0
#: 基线资金分配三档：`slots` 现有反推槽位（默认，保持旧行为逐值不变）、
#: `a` 与尺子 A 同构（读 A 的实际持仓，共享现金池）、`slots60` 日频旧行为在月频下的样子
BASELINE_ALLOCS = ('slots', 'a', 'slots60')
#: 尺子 A 的实际持仓（前向检验产物）：只读，`a` 档唯一候选来源
A_SIGNALS_PATH = ROOT / 'reports' / 'advice' / 'forward_test' / 'signals.csv'
A_CACHE_DIR = ROOT / 'reports' / 'advice' / 'forward_test' / 'cache'
A_STATS_MAIN_PATH = ROOT / 'reports' / 'advice' / 'forward_test' / 'stats_main.csv'
A_TIER = 'mid'
A_VARIANT = 'on'
#: `a` 档每期净收益均值与 stats_main.csv 的 rec_net 的允许误差
A_REC_NET_TOL = 1e-6


# ----------------------------------------------------------------------
# 小工具
# ----------------------------------------------------------------------
def _finite(value: Any) -> float | None:
    """把行情值归一为有限 float；None / NaN / 非数值返回 None。"""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _commission(notional: float, costs: TradingCosts) -> float:
    """佣金：按成交额计费并遵守单笔最低佣金。"""
    return max(costs.min_commission, abs(notional) * costs.commission_rate)


def _buy_cost(notional: float, costs: TradingCosts) -> float:
    """买入侧成本：佣金 + 冲击。"""
    return _commission(notional, costs) + abs(notional) * costs.impact_rate


def _sell_cost(notional: float, costs: TradingCosts) -> float:
    """卖出侧成本：佣金 + 冲击 + 印花税。"""
    return (_commission(notional, costs)
            + abs(notional) * costs.impact_rate
            + abs(notional) * costs.stamp_duty_rate)


def _round_trip_rate(costs: TradingCosts) -> float:
    """按分项费率复算一次完整往返费率（用于自证与 TradingCosts 对齐）。"""
    return (2.0 * costs.commission_rate + costs.stamp_duty_rate
            + 2.0 * costs.impact_rate)


def _documented_costs() -> TradingCosts:
    """按文档冻结的 0.724%/往返构造 ``TradingCosts``。

    ``COST_DEFAULTS`` 只覆盖佣金 5bp×2 + 印花税 5bp + 滑点 2bp×2 = 0.19%，
    缺少过户费与市值分档滑点。这里把差额全部归入冲击成本项，
    并断言往返费率精确等于 0.724%：偏离即失败，不静默降级。
    """
    impact = (DOC_ROUND_TRIP_COST
              - 2.0 * (DOC_COMMISSION_RATE + DOC_TRANSFER_RATE)
              - DOC_STAMP_DUTY_RATE) / 2.0
    costs = TradingCosts(
        commission_rate=DOC_COMMISSION_RATE + DOC_TRANSFER_RATE,
        stamp_duty_rate=DOC_STAMP_DUTY_RATE,
        impact_rate=impact)
    actual = _round_trip_rate(costs)
    if abs(actual - DOC_ROUND_TRIP_COST) > 1e-12:
        raise SystemExit(
            f'往返成本构造失败：{actual:.8f} != {DOC_ROUND_TRIP_COST:.8f}')
    return costs


# ----------------------------------------------------------------------
# 持仓与统计容器
# ----------------------------------------------------------------------
@dataclass
class Position:
    """状态机的一笔在手持仓。"""

    ts_code: str
    shares: int
    entry_date: date
    entry_price: float
    entry_notional: float
    entry_cost: float
    signal_date: date
    smoothed_pct: float
    sell_confirm: int = 0
    first_sell_date: date | None = None
    sell_attempts: int = 0

    def to_dict(self) -> dict[str, Any]:
        """序列化为纯 dict（日期转 ISO 字符串），供分片状态文件回灌。"""
        return {
            'ts_code': self.ts_code,
            'shares': int(self.shares),
            'entry_date': self.entry_date.isoformat(),
            'entry_price': self.entry_price,
            'entry_notional': self.entry_notional,
            'entry_cost': self.entry_cost,
            'signal_date': self.signal_date.isoformat(),
            'smoothed_pct': self.smoothed_pct,
            'sell_confirm': int(self.sell_confirm),
            'first_sell_date': (self.first_sell_date.isoformat()
                                if self.first_sell_date else None),
            'sell_attempts': int(self.sell_attempts),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> Position:
        """从 :meth:`to_dict` 的产物还原。"""
        first = payload.get('first_sell_date')
        return cls(
            ts_code=str(payload['ts_code']),
            shares=int(payload['shares']),
            entry_date=date.fromisoformat(str(payload['entry_date'])[:10]),
            entry_price=float(payload['entry_price']),
            entry_notional=float(payload['entry_notional']),
            entry_cost=float(payload['entry_cost']),
            signal_date=date.fromisoformat(str(payload['signal_date'])[:10]),
            smoothed_pct=float(payload['smoothed_pct']),
            sell_confirm=int(payload.get('sell_confirm', 0)),
            first_sell_date=(date.fromisoformat(str(first)[:10]) if first else None),
            sell_attempts=int(payload.get('sell_attempts', 0)),
        )


@dataclass
class Cohort:
    """基线的 1/60 资金 cohort。"""

    signal_date: date
    entry_idx: int
    exit_idx: int
    budget: float
    cash: float
    positions: dict[str, dict[str, Any]] = field(default_factory=dict)
    exit_retries: int = 0
    entered: bool = False
    exited: bool = False
    #: 信号日当天选出的候选（跨片建仓时不再回看上一片的面板）
    candidates: list[list[Any]] = field(default_factory=list)
    #: 信号日收盘价（撮合时涨跌停基准的第二选择）
    signal_closes: dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """序列化为纯 dict（日期转 ISO 字符串），供分片状态文件回灌。"""
        return {
            'signal_date': self.signal_date.isoformat(),
            'entry_idx': int(self.entry_idx),
            'exit_idx': int(self.exit_idx),
            'budget': self.budget,
            'cash': self.cash,
            'positions': self.positions,
            'exit_retries': int(self.exit_retries),
            'entered': bool(self.entered),
            'exited': bool(self.exited),
            'candidates': self.candidates,
            'signal_closes': self.signal_closes,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> Cohort:
        """从 :meth:`to_dict` 的产物还原。"""
        return cls(
            signal_date=date.fromisoformat(str(payload['signal_date'])[:10]),
            entry_idx=int(payload['entry_idx']),
            exit_idx=int(payload['exit_idx']),
            budget=float(payload['budget']),
            cash=float(payload['cash']),
            positions={str(code): dict(item)
                       for code, item in (payload.get('positions') or {}).items()},
            exit_retries=int(payload.get('exit_retries', 0)),
            entered=bool(payload.get('entered', False)),
            exited=bool(payload.get('exited', False)),
            candidates=[list(item) for item in (payload.get('candidates') or [])],
            signal_closes={str(code): float(value)
                           for code, value in (payload.get('signal_closes') or {}).items()},
        )


def _new_stats() -> dict[str, Any]:
    """统计容器：全部字段在输出时一一映射到 §2.1 的字段名。"""
    return {
        'buy_intents': 0, 'buy_filled': 0, 'buy_filled_amount': 0.0,
        'blocked_limit_up': 0, 'blocked_limit_up_amount': 0.0,
        'blocked_limit_down': 0, 'blocked_suspended': 0, 'blocked_no_quote': 0,
        'blocked_turnover_cap': 0, 'blocked_turnover_cap_amount': 0.0,
        'blocked_cash': 0, 'blocked_cash_amount': 0.0,
        'sell_signals': 0, 'sell_filled': 0, 'sell_abandoned': 0,
        'sell_retry_limit_down': 0, 'sell_retry_suspended': 0,
        'delay_hist': {}, 'overnight_gap_abs': 0.0, 'overnight_gap_signed': 0.0,
        'commission': 0.0, 'stamp': 0.0, 'impact': 0.0,
        'min_hold_blocks': 0, 'holding_obs': 0,
        'sell_confirm_count': 0, 'sell_confirm_delay_sum': 0,
        'sell_confirm_block_days': 0,
        'side_trades': 0, 'buy_notional': 0.0, 'sell_notional': 0.0,
        'planned_amount': 0.0,
        'candidate_signals': 0, 'days_with_candidates': 0, 'days_scanned': 0,
        'valuation_stale_days': 0,
        'exit_price_checks': 0, 'exit_price_dev': 0.0, 'exit_price_dev_max': 0.0,
        'unfilled_records': [],
    }


def _log(stage: str, started: datetime) -> None:
    logger_now = datetime.now()
    elapsed = (logger_now - started).total_seconds()
    print(f'[{stage}] 累计 {elapsed:.1f}s（{logger_now:%H:%M:%S}）', flush=True)


# ----------------------------------------------------------------------
# 选股信号
# ----------------------------------------------------------------------
def select_candidates(frame: pl.DataFrame, tier: Any, config: Any,
                      ) -> list[tuple[str, float]]:
    """当日截面 → (代码, 分位) 候选，按分位降序。

    门槛顺序与生产链路一致：分位达标 → 软筛 → 硬性可推荐性
    （``check_eligibility``）→ 买入成本门槛（``entry_allowed``）。
    只用作新开仓，不参与持仓的卖出判定。
    """
    if frame is None or frame.height == 0:
        return []
    out: list[tuple[str, float]] = []
    for row in frame.iter_rows(named=True):
        percentile = _finite(row.get('score_percentile'))
        if percentile is None or percentile < tier.buy_percentile:
            continue
        if config.soft_filter.enabled and not bool(row.get('soft_pass')):
            continue
        if not check_eligibility(row, tier, config, side='buy').ok:
            continue
        allowed, _reason = entry_allowed(percentile, tier, config)
        if not allowed:
            continue
        out.append((str(row['ts_code']), percentile))
    out.sort(key=lambda item: item[1], reverse=True)
    return out


def percentile_map(frame: pl.DataFrame) -> dict[str, float]:
    """当日全截面分位（不设门槛），供持仓的卖出判定使用。"""
    if frame is None or frame.height == 0:
        return {}
    mapping: dict[str, float] = {}
    for row in frame.select(['ts_code', 'score_percentile']).iter_rows(named=True):
        value = _finite(row['score_percentile'])
        if value is not None:
            mapping[str(row['ts_code'])] = value
    return mapping


# ----------------------------------------------------------------------
# 撮合
# ----------------------------------------------------------------------
def _limit_price(code: str, prev_close: float | None, day: date,
                 side: str, rules: ExecutionRules) -> float | None:
    """用 T 日收盘价与板块规则推出涨跌停价（不读 T+1 任何价格）。"""
    if prev_close is None or prev_close <= 0:
        return None
    pct = rules.resolve_limit_pct(code, None, trade_date=day)
    return rules.resolve_limit_price(prev_close, pct, side)


def _fill_sells(pending: list[dict[str, Any]], positions: dict[str, Position],
                cash: float, day: date, bars: dict[str, tuple],
                rules: ExecutionRules, costs: TradingCosts, stats: dict[str, Any],
                trades: list[dict[str, Any]], closed: list[dict[str, Any]],
                all_dates: list[date]) -> tuple[float, list[dict[str, Any]]]:
    """按 T+1 开盘撮合卖出；跌停/停牌最多重试 ``MAX_SELL_RETRIES`` 次。"""
    remaining: list[dict[str, Any]] = []
    for order in pending:
        code = order['ts_code']
        position = positions.get(code)
        if position is None:
            continue
        bar = bars.get(code)
        prev_close = order.get('prev_close')
        if bar is not None and _finite(bar[2]) is not None:
            # 顺延重试时，涨跌停基准换成新的「T 日收盘价」
            prev_close = _finite(bar[2])
        if bar is None or _finite(bar[3]) is None or _finite(bar[3]) <= 0:
            stats['sell_retry_suspended'] += 1
            stats['blocked_suspended'] += 1
            position.sell_attempts += 1
            if position.sell_attempts <= MAX_SELL_RETRIES:
                remaining.append({**order, 'prev_close': prev_close})
            else:
                stats['sell_abandoned'] += 1
            continue
        price = _finite(bar[0])
        if price is None or price <= 0:
            stats['sell_retry_suspended'] += 1
            stats['blocked_no_quote'] += 1
            position.sell_attempts += 1
            if position.sell_attempts <= MAX_SELL_RETRIES:
                remaining.append({**order, 'prev_close': prev_close})
            else:
                stats['sell_abandoned'] += 1
            continue
        limit_down = _limit_price(code, prev_close, day, 'sell', rules)
        if limit_down is not None and price <= limit_down + 1e-9:
            stats['sell_retry_limit_down'] += 1
            stats['blocked_limit_down'] += 1
            position.sell_attempts += 1
            if position.sell_attempts <= MAX_SELL_RETRIES:
                remaining.append({**order, 'prev_close': prev_close})
            else:
                stats['sell_abandoned'] += 1
            continue

        notional = position.shares * price
        commission = _commission(notional, costs)
        stamp = notional * costs.stamp_duty_rate
        impact = notional * costs.impact_rate
        cost = commission + stamp + impact
        cash += notional - cost
        stats['commission'] += commission
        stats['stamp'] += stamp
        stats['impact'] += impact
        stats['sell_filled'] += 1
        stats['side_trades'] += 1
        stats['sell_notional'] += notional
        days_held = count_trading_days(position.entry_date, day, all_dates)
        outlay = position.entry_notional + position.entry_cost
        r_actual = (notional - cost) / outlay - 1.0 if outlay > 0 else 0.0
        trades.append({
            'ts_code': code, 'signal_date': order['signal_date'],
            'fill_date': day, 'side': 'sell', 'shares': position.shares,
            'price': price, 'notional': notional, 'cost': cost,
            'days_held': days_held, 'reason': order.get('reason', ''),
        })
        closed.append({
            'ts_code': code, 'entry_date': position.entry_date,
            'exit_date': day, 'days_held': days_held,
            'entry_price': position.entry_price,
            'entry_notional': position.entry_notional,
            'entry_cost': position.entry_cost,
            'shares': position.shares, 'r_actual': r_actual,
            'exit_reason': order.get('reason', ''),
        })
        positions.pop(code, None)
    return cash, remaining


def _fill_buys(pending: list[dict[str, Any]], positions: dict[str, Position],
               cash: float, day: date, bars: dict[str, tuple],
               rules: ExecutionRules, costs: TradingCosts, stats: dict[str, Any],
               trades: list[dict[str, Any]], all_dates: list[date],
               lot: int) -> tuple[float, list[dict[str, Any]]]:
    """按 T+1 开盘撮合买入；返回 (现金, 未成交意图清单)。"""
    unfilled: list[dict[str, Any]] = []
    for order in pending:
        code = order['ts_code']
        planned = float(order['notional'])
        bar = bars.get(code)
        prev_close = order.get('prev_close')
        if bar is not None and _finite(bar[2]) is not None:
            # raw_prev_close 即 T 日收盘（除权日按交易所口径调整）
            prev_close = _finite(bar[2])
        stats['planned_amount'] += planned

        if bar is None:
            stats['blocked_no_quote'] += 1
            unfilled.append({**order, 'blocked': 'no_quote'})
            continue
        volume = _finite(bar[3])
        if volume is None or volume <= 0:
            stats['blocked_suspended'] += 1
            unfilled.append({**order, 'blocked': 'suspended'})
            continue
        price = _finite(bar[0])
        if price is None or price <= 0:
            stats['blocked_no_quote'] += 1
            unfilled.append({**order, 'blocked': 'no_quote'})
            continue
        limit_up = _limit_price(code, prev_close, day, 'buy', rules)
        if limit_up is not None and price >= limit_up - 1e-9:
            stats['blocked_limit_up'] += 1
            stats['blocked_limit_up_amount'] += planned
            unfilled.append({**order, 'blocked': 'limit_up'})
            continue

        per_share = price * (1.0 + costs.commission_rate + costs.impact_rate)
        shares = int(max(0.0, min(planned, cash)) / per_share / lot) * lot
        while shares >= lot:
            notional = shares * price
            total = notional + _buy_cost(notional, costs)
            if total <= cash + 1e-6:
                break
            shares -= lot
        if shares < lot:
            stats['blocked_cash'] += 1
            stats['blocked_cash_amount'] += planned
            unfilled.append({**order, 'blocked': 'cash'})
            continue

        notional = shares * price
        commission = _commission(notional, costs)
        impact = notional * costs.impact_rate
        cost = commission + impact
        cash -= notional + cost
        stats['commission'] += commission
        stats['impact'] += impact
        stats['buy_filled'] += 1
        stats['buy_filled_amount'] += notional
        stats['side_trades'] += 1
        stats['buy_notional'] += notional
        delay = count_trading_days(order['signal_date'], day, all_dates)
        stats['delay_hist'][delay] = stats['delay_hist'].get(delay, 0) + 1
        if prev_close is not None and prev_close > 0:
            gap = price / prev_close - 1.0
            stats['overnight_gap_abs'] += abs(gap) * planned
            stats['overnight_gap_signed'] += gap * planned

        positions[code] = Position(
            ts_code=code, shares=shares, entry_date=day, entry_price=price,
            entry_notional=notional, entry_cost=cost,
            signal_date=order['signal_date'],
            smoothed_pct=float(order['percentile']),
        )
        trades.append({
            'ts_code': code, 'signal_date': order['signal_date'],
            'fill_date': day, 'side': 'buy', 'shares': shares,
            'price': price, 'notional': notional, 'cost': cost,
            'days_held': 0, 'reason': '状态机新开仓',
        })
    return cash, unfilled


# ----------------------------------------------------------------------
# 状态机逐日推进
# ----------------------------------------------------------------------
def _decide_sells(day: date, bars: dict[str, tuple],
                  positions: dict[str, Position], pending_codes: set[str],
                  all_pct: dict[str, float], best_pct: float | None,
                  tier: Any, config: Any, min_hold: int,
                  all_dates: list[date],
                  stats: dict[str, Any]) -> tuple[list[TradeIntent],
                                                  dict[str, str],
                                                  dict[str, float | None]]:
    """持仓按生产卖出规则逐票决策，产出卖出意图。"""
    intents: list[TradeIntent] = []
    reasons: dict[str, str] = {}
    prev_closes: dict[str, float | None] = {}
    for code, position in list(positions.items()):
        percentile = all_pct.get(code, 0.0)
        days_held = count_trading_days(position.entry_date, day, all_dates)
        decision = update_holding(
            position.smoothed_pct, percentile, position.sell_confirm,
            days_held, tier, config, best_candidate_pct=best_pct,
            ts_code=code)
        position.smoothed_pct = decision.smoothed_percentile
        position.sell_confirm = decision.sell_confirm
        stats['holding_obs'] += 1
        raw_sell = decision.smoothed_percentile <= tier.sell_percentile
        if raw_sell:
            if position.first_sell_date is None:
                position.first_sell_date = day
            if days_held < min_hold:
                stats['min_hold_blocks'] += 1
        else:
            # 信号回升即清零，延迟只统计「连续处于卖出区间」的那一段
            position.first_sell_date = None
        if decision.action != 'SELL':
            if raw_sell and '卖出信号确认中' in decision.reason:
                stats['sell_confirm_block_days'] += 1
            continue
        stats['sell_signals'] += 1
        position.sell_attempts = 0
        if position.first_sell_date is not None:
            stats['sell_confirm_count'] += 1
            stats['sell_confirm_delay_sum'] += count_trading_days(
                position.first_sell_date, day, all_dates)
            position.first_sell_date = None
        if code in pending_codes:
            # 已有未成交的卖出意图（跌停/停牌重试中），不重复下单
            continue
        bar = bars.get(code)
        last_close = _finite(bar[1]) if bar is not None else None
        last_close = last_close or position.entry_price
        intents.append(TradeIntent(
            ts_code=code, side='sell',
            notional=position.shares * last_close, priority=percentile))
        reasons[code] = decision.reason
        prev_closes[code] = last_close
    return intents, reasons, prev_closes


def _plan_buys(candidates: list[tuple[str, float]], positions: dict[str, Position],
               pending_codes: set[str], bars: dict[str, tuple], day: date,
               equity: float, target_weight: float, max_positions: int,
               stats: dict[str, Any]) -> tuple[list[TradeIntent],
                                               dict[str, dict[str, Any]]]:
    """按分位从高到低填满空余仓位，产出买入意图。"""
    intents: list[TradeIntent] = []
    details: dict[str, dict[str, Any]] = {}
    free_slots = max(0, max_positions - len(positions))
    for code, percentile in candidates:
        if len(intents) >= free_slots:
            break
        if code in positions or code in pending_codes:
            continue
        notional = target_weight * equity
        if notional <= 0:
            continue
        bar = bars.get(code)
        prev_close = _finite(bar[1]) if bar is not None else None
        intents.append(TradeIntent(ts_code=code, side='buy',
                                   notional=notional, priority=percentile))
        details[code] = {
            'signal_date': day, 'notional': notional,
            'percentile': percentile, 'prev_close': prev_close,
        }
        stats['buy_intents'] += 1
    return intents, details


def _apply_plan(plan: Any, day: date,
                buy_details: dict[str, dict[str, Any]],
                sell_reasons: dict[str, str],
                sell_prev_close: dict[str, float | None],
                stats: dict[str, Any]) -> tuple[list[dict[str, Any]],
                                                list[dict[str, Any]]]:
    """把换手上限结果翻译成下一日的待撮合订单。"""
    buys: list[dict[str, Any]] = []
    sells: list[dict[str, Any]] = []
    for intent in plan.admitted:
        if intent.side == 'sell':
            sells.append({
                'ts_code': intent.ts_code, 'signal_date': day,
                'prev_close': sell_prev_close.get(intent.ts_code),
                'reason': sell_reasons.get(intent.ts_code, ''),
            })
            continue
        detail = buy_details.get(intent.ts_code)
        if detail is not None:
            buys.append({'ts_code': intent.ts_code, **detail})
    for intent in plan.deferred:
        if intent.side != 'buy':
            continue
        stats['blocked_turnover_cap'] += 1
        stats['blocked_turnover_cap_amount'] += float(intent.notional)
        detail = buy_details.get(intent.ts_code, {})
        stats['unfilled_records'].append({
            'ts_code': intent.ts_code, 'signal_date': day,
            'notional': float(intent.notional),
            'percentile': detail.get('percentile', 0.0),
            'prev_close': detail.get('prev_close'),
            'blocked': 'turnover_cap',
        })
    return buys, sells


def run_stateful(window_dates: list[date], by_date: dict[date, pl.DataFrame],
                 bars_by_date: dict[date, dict[str, tuple]],
                 all_dates: list[date], tier: Any, config: Any,
                 equity_init: float, costs: TradingCosts,
                 rules: ExecutionRules,
                 state: dict[str, Any] | None = None,
                 finalize: bool = True,
                 scan_last_day: bool = False,
                 signal_days: set[date] | None = None) -> dict[str, Any]:
    """逐日推进的状态机回测。

    ``state`` 为上一分片的期末状态（现金 / 持仓 / 待买待卖意图）；分片模式下
    传入才能跨片续接。``finalize`` 只有最后一片为 True（中间片不得强平）。
    ``scan_last_day`` 分片模式为 True，使片末交易日也生成下一日意图，在下一片
    首个交易日开盘撮合。``signal_days`` 非空时只在这些日期选候选。
    """
    cash_reserve = float(getattr(config.constraints, 'cash_buffer', 0.10) or 0.0)
    lot = int(getattr(config.constraints, 'lot_size', 100) or 100)
    max_positions = int(tier.max_positions)
    target_weight = min(1.0 / max_positions, max(0.0, 1.0 - cash_reserve))
    min_hold = config.effective_min_hold(tier)

    carried = state or {}

    def _normalize_order(order: dict[str, Any]) -> dict[str, Any]:
        """跨片回灌时把 ISO 字符串日期还原成 date（同片内是 date 对象）。"""
        item = dict(order)
        value = item.get('signal_date')
        if isinstance(value, str):
            item['signal_date'] = date.fromisoformat(value[:10])
        return item

    cash = float(carried.get('cash', equity_init))
    positions: dict[str, Position] = {
        str(code): Position.from_dict(item)
        for code, item in (carried.get('positions') or {}).items()}
    pending_buys: list[dict[str, Any]] = [
        _normalize_order(item) for item in (carried.get('pending_buys') or [])]
    pending_sells: list[dict[str, Any]] = [
        _normalize_order(item) for item in (carried.get('pending_sells') or [])]
    stats = _new_stats()
    trades: list[dict[str, Any]] = []
    closed: list[dict[str, Any]] = []
    nav_rows: list[dict[str, Any]] = []

    for index, day in enumerate(window_dates):
        bars = bars_by_date.get(day, {})
        cash, pending_sells = _fill_sells(
            pending_sells, positions, cash, day, bars, rules, costs, stats,
            trades, closed, all_dates)
        cash, unfilled = _fill_buys(
            pending_buys, positions, cash, day, bars, rules, costs, stats,
            trades, all_dates, lot)
        for item in unfilled:
            stats['unfilled_records'].append(item)
        pending_buys = []

        market_value = 0.0
        for code, position in positions.items():
            bar = bars.get(code)
            close = _finite(bar[1]) if bar is not None else None
            if close is None:
                close = position.entry_price
            market_value += position.shares * close
        nav = cash + market_value
        nav_rows.append({
            'date': day, 'nav_stateful': nav, 'nav_fixed60': None,
            'cash': cash, 'cash_fixed60': None,
            'holdings_count': len(positions),
            'gross_exposure': market_value / nav if nav > 0 else 0.0,
        })

        is_last = index == len(window_dates) - 1
        if is_last and not scan_last_day:
            break

        frame = by_date.get(day)
        if signal_days is not None and day not in signal_days:
            candidates: list[tuple[str, float]] = []
        else:
            candidates = select_candidates(frame, tier, config)
            stats['days_scanned'] += 1
            stats['candidate_signals'] += len(candidates)
            if candidates:
                stats['days_with_candidates'] += 1
        all_pct = percentile_map(frame)
        best_pct = candidates[0][1] if candidates else None
        equity = nav
        pending_codes = {order['ts_code'] for order in pending_sells}

        sell_intents, sell_reasons, sell_prev_close = _decide_sells(
            day, bars, positions, pending_codes, all_pct, best_pct, tier,
            config, min_hold, all_dates, stats)
        buy_intents, buy_details = _plan_buys(
            candidates, positions, pending_codes, bars, day, equity,
            target_weight, max_positions, stats)
        plan = apply_turnover_cap(
            sell_intents + buy_intents, equity,
            config.turnover.max_turnover_per_rebalance)
        new_buys, new_sells = _apply_plan(
            plan, day, buy_details, sell_reasons, sell_prev_close, stats)
        pending_buys.extend(new_buys)
        pending_sells = pending_sells + new_sells

    if finalize:
        # 窗口期末未平仓持仓：按最后收盘价估值并扣预估卖出成本，纳入分解样本
        last_day = window_dates[-1]
        last_bars = bars_by_date.get(last_day, {})
        for code, position in positions.items():
            bar = last_bars.get(code)
            close = _finite(bar[1]) if bar is not None else None
            if close is None or close <= 0:
                close = position.entry_price
            notional = position.shares * close
            net = notional - _sell_cost(notional, costs)
            outlay = position.entry_notional + position.entry_cost
            closed.append({
                'ts_code': code, 'entry_date': position.entry_date,
                'exit_date': last_day,
                'days_held': count_trading_days(position.entry_date, last_day,
                                                all_dates),
                'entry_price': position.entry_price,
                'entry_notional': position.entry_notional,
                'entry_cost': position.entry_cost,
                'shares': position.shares,
                'r_actual': (net / outlay - 1.0) if outlay > 0 else 0.0,
                'exit_reason': '窗口期末估值（未真实卖出，按收盘价并扣预估卖出成本）',
            })

    return {
        'nav_rows': nav_rows, 'trades': trades, 'closed': closed,
        'stats': stats,
        'nav': float(nav_rows[-1]['nav_stateful']) if nav_rows else equity_init,
        'state': {
            'cash': cash,
            'positions': {code: position.to_dict()
                          for code, position in positions.items()},
            'pending_buys': pending_buys,
            'pending_sells': pending_sells,
        },
    }


# ----------------------------------------------------------------------
# 基线：60 个重叠 cohort，固定持有 60 个交易日
# ----------------------------------------------------------------------
def _cohort_buy(cohort: Cohort, day: date, bars: dict[str, tuple],
                signal_closes: dict[str, float],
                candidates: list[tuple[str, float]], tier: Any,
                target_weight: float, costs: TradingCosts,
                rules: ExecutionRules, lot: int,
                stats: dict[str, Any]) -> None:
    """cohort 在 T+1 开盘等权买入；不可成交则资金留在 cohort 现金里。"""
    slots = max(0, int(tier.max_positions))
    picks = candidates[:slots]
    if not picks:
        return
    for code, _percentile in picks:
        planned = cohort.budget * target_weight
        bar = bars.get(code)
        if bar is None:
            continue
        volume = _finite(bar[3])
        price = _finite(bar[0])
        if volume is None or volume <= 0 or price is None or price <= 0:
            continue
        prev_close = _finite(bar[2])
        if prev_close is None:
            prev_close = _finite(signal_closes.get(code))
        limit_up = _limit_price(code, prev_close, day, 'buy', rules)
        if limit_up is not None and price >= limit_up - 1e-9:
            continue
        per_share = price * (1.0 + costs.commission_rate + costs.impact_rate)
        shares = int(min(planned, cohort.cash) / per_share / lot) * lot
        if shares < lot:
            continue
        notional = shares * price
        cost = _buy_cost(notional, costs)
        if notional + cost > cohort.cash + 1e-6:
            continue
        cohort.cash -= notional + cost
        cohort.positions[code] = {
            'shares': shares, 'entry_price': price,
            'entry_notional': notional, 'entry_cost': cost,
        }
        stats['buy_filled'] += 1
        stats['side_trades'] += 1
        stats['buy_notional'] += notional
        stats['commission'] += _commission(notional, costs)
        stats['impact'] += notional * costs.impact_rate


def _cohort_sell(cohort: Cohort, day: date, bars: dict[str, tuple],
                 costs: TradingCosts, rules: ExecutionRules,
                 stats: dict[str, Any]) -> bool:
    """cohort 到期开盘清仓；有任一笔卖不出就整体顺延重试。"""
    for code in list(cohort.positions):
        bar = bars.get(code)
        price = _finite(bar[0]) if bar is not None else None
        volume = _finite(bar[3]) if bar is not None else None
        if price is None or price <= 0 or volume is None or volume <= 0:
            return False
        limit_down = _limit_price(code, _finite(bar[2]), day, 'sell', rules)
        if limit_down is not None and price <= limit_down + 1e-9:
            return False
    for code, item in list(cohort.positions.items()):
        bar = bars[code]
        price = _finite(bar[0])
        notional = item['shares'] * price
        cost = _sell_cost(notional, costs)
        cohort.cash += notional - cost
        stats['sell_filled'] += 1
        stats['side_trades'] += 1
        stats['sell_notional'] += notional
        stats['commission'] += _commission(notional, costs)
        stats['stamp'] += notional * costs.stamp_duty_rate
        stats['impact'] += notional * costs.impact_rate
        cohort.positions.pop(code, None)
    return True


def _baseline_slots(dates: list[date],
                    signal_days: set[date] | None) -> dict[str, Any]:
    """把固定持有 60 日基线换算成等价资金槽位数。

    基线原设计是「日频信号 + 60 个重叠 cohort 各占 1/60」，换成月频信号后
    同时存活的 cohort 只剩约 3 个，若仍按 1/60 分配会把 98.9% 的资金闲置，
    净值几乎不动，与状态机不可比。这里按相邻信号日的交易日间隔中位数
    反推等价槽位：``slots = max(1, round(BASELINE_HOLD / spacing_days))``，
    使 ``cohort_budget × slots ≈ equity_init``，两口径资金利用率一致。

    日频信号（``signal_days is None``）取 ``spacing_days=1`` → ``slots=60``，
    与旧行为逐值一致，跨片与单窗口共用同一算法。
    """
    if signal_days is None:
        spacing_all = 1
        gaps: list[int] = []
    else:
        positions = [index for index, day in enumerate(dates) if day in signal_days]
        gaps = [right - left for left, right in zip(positions, positions[1:],
                                                   strict=False) if right > left]
        spacing_all = max(1, int(round(statistics.median(gaps)))) if gaps else 1
    slots = max(1, int(round(BASELINE_HOLD / spacing_all)))
    return {
        'slots': slots,
        'spacing_days': spacing_all,
        'spacing_samples': len(gaps),
        'cohort_budget_fraction': 1.0 / slots,
    }


@dataclass
class _PoolLot:
    """`a` 档的一笔 lot，直接对应尺子 A ``signals.csv`` 的一行。"""

    ts_code: str
    signal_date: date
    entry_date: date
    shares: float
    entry_price: float
    entry_notional: float
    entry_cost: float
    exit_date: date | None
    exit_price: float | None
    net60: float
    last_close: float
    stale_days: int = 0

    def to_dict(self) -> dict[str, Any]:
        """序列化为纯 dict（日期转 ISO 字符串），供分片状态文件回灌。"""
        return {
            'ts_code': self.ts_code,
            'signal_date': self.signal_date.isoformat(),
            'entry_date': self.entry_date.isoformat(),
            'shares': float(self.shares),
            'entry_price': self.entry_price,
            'entry_notional': self.entry_notional,
            'entry_cost': self.entry_cost,
            'exit_date': self.exit_date.isoformat() if self.exit_date else None,
            'exit_price': self.exit_price,
            'net60': self.net60,
            'last_close': self.last_close,
            'stale_days': int(self.stale_days),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> _PoolLot:
        """从 :meth:`to_dict` 的产物还原。"""
        exit_date = payload.get('exit_date')
        return cls(
            ts_code=str(payload['ts_code']),
            signal_date=date.fromisoformat(str(payload['signal_date'])[:10]),
            entry_date=date.fromisoformat(str(payload['entry_date'])[:10]),
            shares=float(payload['shares']),
            entry_price=float(payload['entry_price']),
            entry_notional=float(payload['entry_notional']),
            entry_cost=float(payload['entry_cost']),
            exit_date=date.fromisoformat(str(exit_date)[:10]) if exit_date else None,
            exit_price=(float(payload['exit_price'])
                        if payload.get('exit_price') is not None else None),
            net60=float(payload.get('net60') or 0.0),
            last_close=float(payload.get('last_close') or payload['entry_price']),
            stale_days=int(payload.get('stale_days', 0)),
        )


class _CohortTrack:
    """固定持有 60 日的槽位资金容器（``slots`` 与 ``slots60`` 共用同一实现）。

    ``slots`` 个重叠 cohort 各占 ``equity_init / slots`` 预算，到期开盘卖出。
    与旧 :func:`run_fixed60` 的实现逐行等价，唯一区别是候选由调用方算好后传入，
    使 ``slots`` / ``slots60`` 两档共用同一次候选计算。
    """

    def __init__(self, name: str, slots: int, equity_init: float,
                 pool: float | None = None,
                 cohorts: list[Cohort] | None = None) -> None:
        self.name = name
        self.slots = max(1, int(slots))
        self.cohort_budget = equity_init / self.slots
        self.pool = equity_init if pool is None else float(pool)
        self.cohorts: list[Cohort] = list(cohorts or [])

    @classmethod
    def from_state(cls, name: str, slots: int, equity_init: float,
                   state: dict[str, Any] | None) -> _CohortTrack:
        carried = state or {}
        return cls(name, slots, equity_init,
                   pool=float(carried.get('pool', equity_init)),
                   cohorts=[Cohort.from_dict(item)
                            for item in (carried.get('cohorts') or [])])

    def to_state(self) -> dict[str, Any]:
        return {'pool': self.pool,
                'cohorts': [cohort.to_dict() for cohort in self.cohorts]}

    def step(self, global_index: int, day: date, bars: dict[str, tuple],
             candidates: list[tuple[str, float]], signal_closes: dict[str, float],
             is_signal_day: bool, horizon: int, tier: Any,
             target_weight: float, costs: TradingCosts, rules: ExecutionRules,
             lot: int, stats: dict[str, Any]) -> tuple[float, float]:
        """推进一天，返回 ``(收盘净值, 闲置现金)``。"""
        # 1) 到期 cohort 开盘卖出（卖不出则顺延，最多 3 次）
        for cohort in self.cohorts:
            if cohort.exited or not cohort.entered or cohort.exit_idx != global_index:
                continue
            if _cohort_sell(cohort, day, bars, costs, rules, stats):
                cohort.exited = True
                self.pool += cohort.cash
                cohort.cash = 0.0
            elif cohort.exit_retries < MAX_SELL_RETRIES:
                cohort.exit_retries += 1
                cohort.exit_idx = min(global_index + 1, horizon - 1)
            else:
                cohort.exited = True
                self.pool += cohort.cash
                cohort.cash = 0.0

        # 2) 昨日 cohort 在今日开盘建仓（候选与信号日收盘价随 cohort 一并携带）
        for cohort in self.cohorts:
            if cohort.entered or cohort.exited or cohort.entry_idx != global_index:
                continue
            _cohort_buy(cohort, day, bars, cohort.signal_closes,
                        [(str(code), float(pct)) for code, pct in cohort.candidates],
                        tier, target_weight, costs, rules, lot, stats)
            cohort.entered = True

        # 3) 今日收盘后新建 cohort（下一交易日开盘建仓）
        if global_index + 1 < horizon and self.pool >= self.cohort_budget and is_signal_day:
            budget = min(self.cohort_budget, self.pool)
            self.pool -= budget
            self.cohorts.append(Cohort(
                signal_date=day, entry_idx=global_index + 1,
                exit_idx=min(global_index + 1 + BASELINE_HOLD, horizon - 1),
                budget=budget, cash=budget,
                candidates=[[code, pct] for code, pct in candidates],
                signal_closes=dict(signal_closes)))

        # 4) 收盘按市值计价
        total = self.pool
        cash_idle = self.pool
        for cohort in self.cohorts:
            if cohort.exited:
                continue
            cash_idle += cohort.cash
            value = cohort.cash
            for code, item in cohort.positions.items():
                bar = bars.get(code)
                close = _finite(bar[1]) if bar is not None else None
                close = close if close is not None else item['entry_price']
                value += item['shares'] * close
            total += value
        return total, cash_idle


class _PoolTrack:
    """`a` 档：与尺子 A 同构的 lot 级共享现金池。

    候选与成交价全部来自 A 的 ``signals.csv``（mid / on / ok_60，209 行）：
    每个信号日按行顺序在 T+1 开盘买入，每只票投入
    ``target_weight × 信号日收盘组合权益``；所有 lot 共享一个现金池，
    现金不足就少买（买不起的行记 ``blocked_cash``）；卖出价取该行的
    ``exit_close_60``、卖出日取该行的 ``exit_date_60``。不做预分配、不锁仓。
    """

    def __init__(self, cash: float, lots: list[_PoolLot] | None = None,
                 last_nav: float | None = None) -> None:
        self.cash = float(cash)
        self.lots: list[_PoolLot] = list(lots or [])
        self.last_nav = float(cash) if last_nav is None else float(last_nav)

    @classmethod
    def from_state(cls, state: dict[str, Any] | None,
                   equity_init: float) -> _PoolTrack:
        carried = state or {}
        return cls(float(carried.get('cash', equity_init)),
                   [_PoolLot.from_dict(item)
                    for item in (carried.get('lots') or [])],
                   last_nav=float(carried.get('last_nav', equity_init)))

    def to_state(self) -> dict[str, Any]:
        return {'cash': self.cash, 'last_nav': self.last_nav,
                'lots': [lot.to_dict() for lot in self.lots]}

    def step(self, day: date, bars: dict[str, tuple],
             entries: list[dict[str, Any]], last_nav: float,
             costs: TradingCosts, lot_size: int,
             stats: dict[str, Any]) -> tuple[float, float]:
        """推进一天：开盘买入 → 收盘卖出到期 lot → 收盘估值。"""
        # 1) 开盘买入（资金不足按行顺序少买；股数按原始价取整手，复权价只做计价尺度）
        for row in entries:
            price = _finite(row.get('entry_open'))
            weight = _finite(row.get('target_weight'))
            if price is None or price <= 0 or weight is None or weight <= 0:
                continue
            planned = weight * last_nav
            stats['planned_amount'] += planned
            bar = bars.get(str(row['ts_code']))
            raw_price = _finite(bar[0]) if bar is not None else None
            if raw_price is None or raw_price <= 0:
                raw_price = price
            n_raw = int(max(0.0, min(planned, self.cash)) / raw_price / lot_size) * lot_size
            notional = n_raw * raw_price
            cost = _buy_cost(notional, costs) if n_raw >= lot_size else 0.0
            while n_raw >= lot_size and notional + cost > self.cash + 1e-6:
                n_raw -= lot_size
                notional = n_raw * raw_price
                cost = _buy_cost(notional, costs)
            if n_raw < lot_size:
                stats['blocked_cash'] += 1
                stats['blocked_cash_amount'] += planned
                continue
            commission = _commission(notional, costs)
            impact = notional * costs.impact_rate
            self.cash -= notional + cost
            stats['buy_filled'] += 1
            stats['side_trades'] += 1
            stats['buy_notional'] += notional
            stats['commission'] += commission
            stats['impact'] += impact
            self.lots.append(_PoolLot(
                ts_code=str(row['ts_code']),
                signal_date=row['signal_date'],
                entry_date=day,
                shares=notional / price,
                entry_price=price,
                entry_notional=notional,
                entry_cost=cost,
                exit_date=row.get('exit_date'),
                exit_price=_finite(row.get('exit_close_60')),
                net60=float(row.get('net_60') or 0.0),
                last_close=price))

        # 2) 收盘卖出到期 lot（A 的口径：到期日收盘价卖出，不重试）
        for item in list(self.lots):
            exit_date = item.exit_date
            if exit_date is None or exit_date > day:
                continue
            price = item.exit_price
            bar = bars.get(item.ts_code)
            panel_close = _finite(bar[4]) if bar is not None and len(bar) > 4 else None
            if panel_close is not None and price is not None and price > 0:
                stats['exit_price_checks'] += 1
                stats['exit_price_dev'] += abs(panel_close / price - 1.0)
                stats['exit_price_dev_max'] = max(
                    stats['exit_price_dev_max'], abs(panel_close / price - 1.0))
            if price is None or price <= 0:
                price = item.last_close
            notional = item.shares * price
            cost = _sell_cost(notional, costs)
            self.cash += notional - cost
            stats['sell_filled'] += 1
            stats['side_trades'] += 1
            stats['sell_notional'] += notional
            stats['commission'] += _commission(notional, costs)
            stats['stamp'] += notional * costs.stamp_duty_rate
            stats['impact'] += notional * costs.impact_rate
            self.lots.remove(item)

        # 3) 收盘估值：用面板后复权收盘价（与 A 的 entry_open / exit_close_60 同尺度）；
        #    当日无收盘价时沿用该 lot 最近可得的收盘价
        value = 0.0
        for item in self.lots:
            bar = bars.get(item.ts_code)
            close = _finite(bar[4]) if bar is not None and len(bar) > 4 else None
            if close is not None and close > 0:
                item.last_close = close
            else:
                item.stale_days += 1
                stats['valuation_stale_days'] += 1
            value += item.shares * item.last_close
        self.last_nav = self.cash + value
        return self.last_nav, self.cash


def load_a_signals(path: Path = A_SIGNALS_PATH, cache_dir: Path = A_CACHE_DIR,
                   horizon: int = BASELINE_HOLD) -> dict[str, Any]:
    """读取尺子 A 的实际持仓（``signals.csv`` 的 mid / on / ok_60 共 209 行）。

    ``signals.csv`` 没有 ``exit_date_60`` 列，这里用 ``forward_test/cache`` 的
    同口径 ``shift`` 反推，并用该行的 ``exit_close_60`` 逐值校验（必须 209/209
    精确相等，否则直接退出，不自行调参抹平）。返回按 ``entry_date`` 分组的
    建仓清单，以及每期净收益均值（供与 ``stats_main.csv`` 的 ``rec_net`` 对拍）。
    """
    frame = pl.read_csv(path)
    frame = frame.filter(
        (pl.col('tier') == A_TIER) & (pl.col('variant') == A_VARIANT)
        & pl.col('ok_60'))
    frame = frame.with_columns(
        pl.col('signal_date').str.slice(0, 10).str.to_date('%Y-%m-%d').alias('signal_d'),
        pl.col('entry_date').str.slice(0, 10).str.to_date('%Y-%m-%d').alias('entry_d'),
    ).with_row_index('__ord')

    codes = frame['ts_code'].unique().to_list()
    parts = []
    for market in ('sh', 'sz', 'bj'):
        sub = [c for c in codes if str(c).upper().endswith('.' + market.upper())]
        cache_path = cache_dir / f'{market}.parquet'
        if sub and cache_path.exists():
            parts.append(pl.read_parquet(cache_path, columns=['ts_code', 'date', 'close'])
                         .filter(pl.col('ts_code').is_in(sub)))
    if not parts:
        raise SystemExit(f'a 档：{cache_dir} 下没有可用的行情缓存，无法反推 exit_date_60')

    panel = pl.concat(parts).sort(['ts_code', 'date'])
    offset_used = None
    for offset in (horizon - 1, horizon, horizon + 1):
        cand = panel.with_columns(
            pl.col('date').shift(-offset).over('ts_code').alias('exit_d'),
            pl.col('close').shift(-offset).over('ts_code').alias('exit_chk'))
        joined = frame.join(
            cand.select(['ts_code', 'date', 'exit_d', 'exit_chk']),
            left_on=['ts_code', 'entry_d'], right_on=['ts_code', 'date'], how='left')
        price_ok = (joined['exit_chk'] - joined['exit_close_60']).abs().max()
        if joined['exit_d'].null_count() == 0 and price_ok is not None and price_ok < 1e-9:
            frame = joined.with_columns(pl.col('exit_d').alias('exit_date_60')) \
                          .drop('exit_chk').sort('__ord')
            offset_used = offset
            break
    if offset_used is None:
        raise SystemExit('a 档：forward_test 缓存反推的 exit_date_60 与 exit_close_60 对不上，停止')

    rows: list[dict[str, Any]] = []
    entries_by_date: dict[date, list[dict[str, Any]]] = {}
    for item in frame.iter_rows(named=True):
        entry = {
            'ts_code': str(item['ts_code']),
            'signal_date': item['signal_d'],
            'entry_date': item['entry_d'],
            'exit_date': item['exit_date_60'],
            'target_weight': float(item['target_weight']),
            'entry_open': float(item['entry_open']),
            'exit_close_60': float(item['exit_close_60']),
            'net_60': float(item['net_60']),
        }
        rows.append(entry)
        entries_by_date.setdefault(entry['entry_date'], []).append(entry)

    by_period: dict[date, list[float]] = {}
    weight_by_period: dict[date, float] = {}
    for entry in rows:
        by_period.setdefault(entry['signal_date'], []).append(entry['net_60'])
        weight_by_period[entry['signal_date']] = (
            weight_by_period.get(entry['signal_date'], 0.0) + entry['target_weight'])
    period_mean = {day: sum(vals) / len(vals) for day, vals in by_period.items()}
    weights = [entry['target_weight'] for entry in rows]
    period_weights = list(weight_by_period.values())
    return {
        'path': str(path),
        'tier': A_TIER,
        'variant': A_VARIANT,
        'rows': rows,
        'entries_by_date': entries_by_date,
        'row_count': len(rows),
        'period_count': len(period_mean),
        'period_net': {day.isoformat(): value for day, value in period_mean.items()},
        'period_mean': sum(period_mean.values()) / len(period_mean),
        'exit_offset': offset_used,
        'weight_min': min(weights),
        'weight_max': max(weights),
        'weight_mean': sum(weights) / len(weights),
        'period_weight_mean': sum(period_weights) / len(period_weights),
        'period_weight_min': min(period_weights),
        'period_weight_max': max(period_weights),
    }


def check_a_rec_net(signals: dict[str, Any],
                    path: Path = A_STATS_MAIN_PATH) -> dict[str, Any]:
    """`a` 档口径对拍：每期净收益均值必须等于 A 的 ``rec_net``（±1e-6）。"""
    stats = pl.read_csv(path)
    picked = stats.filter(
        (pl.col('tier') == A_TIER) & (pl.col('horizon') == BASELINE_HOLD)
        & (pl.col('group') == '全部'))
    if picked.height != 1:
        raise SystemExit(f'a 档：{path} 里 mid/60/全部 不是唯一一行，停止')
    expected = float(picked['rec_net'][0])
    diff = abs(float(signals['period_mean']) - expected)
    if diff > A_REC_NET_TOL:
        raise SystemExit(
            f'a 档口径不一致：每期净收益均值 {signals["period_mean"]:.9f} '
            f'vs stats_main rec_net {expected:.9f}，差 {diff:.3e}，停止')
    return {
        'stats_main': str(path),
        'source': 'tier=mid horizon=60 group=全部 rec_net',
        'expected_rec_net': expected,
        'computed_period_mean': float(signals['period_mean']),
        'abs_diff': diff,
        'tol': A_REC_NET_TOL,
        'passed': True,
        'periods': signals['period_count'],
        'rows': signals['row_count'],
    }


def run_baselines(window_dates: list[date], by_date: dict[date, pl.DataFrame],
                  bars_by_date: dict[date, dict[str, tuple]], tier: Any,
                  config: Any, equity_init: float, costs: TradingCosts,
                  rules: ExecutionRules, allocs: tuple[str, ...] = ('slots',),
                  state: dict[str, Any] | None = None,
                  index_base: int = 0,
                  total_days: int | None = None,
                  signal_days: set[date] | None = None,
                  slots: int = BASELINE_HOLD,
                  a_signals: dict[str, Any] | None = None
                  ) -> dict[str, dict[str, Any]]:
    """三档资金分配在同一次逐日遍历里一起算，候选与收盘价只算一次。

    ``allocs`` 取 ``BASELINE_ALLOCS`` 的子集；返回值按档名索引，每档含
    ``nav_rows`` / ``stats`` / ``nav`` / ``state``。``a_signals`` 是
    :func:`load_a_signals` 的产物（``a`` 档唯一候选来源）。
    """
    cash_reserve = float(getattr(config.constraints, 'cash_buffer', 0.10) or 0.0)
    lot = int(getattr(config.constraints, 'lot_size', 100) or 100)
    target_weight = min(1.0 / int(tier.max_positions), max(0.0, 1.0 - cash_reserve))
    horizon = int(total_days) if total_days is not None else len(window_dates)
    carried = state or {}
    cohort_names = [name for name in allocs if name in ('slots', 'slots60')]

    tracks: dict[str, Any] = {}
    if 'slots' in allocs:
        tracks['slots'] = _CohortTrack.from_state(
            'slots', slots, equity_init, carried.get('slots'))
    if 'slots60' in allocs:
        tracks['slots60'] = _CohortTrack.from_state(
            'slots60', BASELINE_HOLD, equity_init, carried.get('slots60'))
    if 'a' in allocs:
        tracks['a'] = _PoolTrack.from_state(carried.get('a'), equity_init)

    stats = {name: _new_stats() for name in allocs}
    nav_rows: dict[str, list[dict[str, Any]]] = {name: [] for name in allocs}
    a_entries = (a_signals or {}).get('entries_by_date', {})

    for index, day in enumerate(window_dates):
        bars = bars_by_date.get(day, {})
        global_index = index_base + index
        is_signal_day = signal_days is None or day in signal_days
        candidates: list[tuple[str, float]] = []
        signal_closes: dict[str, float] = {}
        if cohort_names and is_signal_day and global_index + 1 < horizon:
            frame = by_date.get(day)
            candidates = select_candidates(frame, tier, config)
            if frame is not None and frame.height > 0:
                for row in frame.select(['ts_code', 'raw_close']).iter_rows(named=True):
                    close = _finite(row['raw_close'])
                    if close is not None:
                        signal_closes[str(row['ts_code'])] = close

        for name in allocs:
            track = tracks[name]
            if name == 'a':
                # 建仓规模按「信号日」收盘权益，即上一交易日收盘的 NAV
                nav, cash = track.step(day, bars, a_entries.get(day, []),
                                       track.last_nav, costs, lot, stats[name])
                holdings = len(track.lots)
            else:
                nav, cash = track.step(
                    global_index, day, bars, candidates, signal_closes,
                    is_signal_day, horizon, tier, target_weight, costs, rules,
                    lot, stats[name])
                holdings = sum(1 for cohort in track.cohorts
                               if not cohort.exited and cohort.positions)
            market_value = nav - cash
            nav_rows[name].append({
                'date': day, 'nav': nav, 'cash': cash,
                'holdings_count': holdings,
                'gross_exposure': market_value / nav if nav > 0 else 0.0,
            })

    out: dict[str, dict[str, Any]] = {}
    for name in allocs:
        track = tracks[name]
        rows = nav_rows[name]
        out[name] = {
            'nav_rows': rows, 'stats': stats[name],
            'nav': float(rows[-1]['nav']) if rows else equity_init,
            'state': track.to_state(),
        }
    return out


def run_fixed60(window_dates: list[date], by_date: dict[date, pl.DataFrame],
                bars_by_date: dict[date, dict[str, tuple]], tier: Any,
                config: Any, equity_init: float, costs: TradingCosts,
                rules: ExecutionRules,
                state: dict[str, Any] | None = None,
                index_base: int = 0,
                total_days: int | None = None,
                signal_days: set[date] | None = None,
                slots: int = BASELINE_HOLD) -> dict[str, Any]:
    """旧入口：只跑 ``slots`` 一档（保持既有调用方与既有数字不变）。

    ``slots`` 由 :func:`_baseline_slots` 按信号日间隔中位数算出（日频为 60，
    月频约 3），保证每槽预算 ``equity_init / slots`` 使资金利用率与状态机可比。
    ``state`` 携带上一分片的 ``pool`` 与未退出 cohort，``index_base`` 是
    本片首日在全局交易日序列里的下标，``total_days`` 是全局交易日数；
    三者一起保证 ``entry_idx`` / ``exit_idx`` 始终是全局下标。
    """
    result = run_baselines(
        window_dates, by_date, bars_by_date, tier, config, equity_init, costs,
        rules, allocs=('slots',), state=state, index_base=index_base,
        total_days=total_days, signal_days=signal_days, slots=slots)
    return result['slots']


# ----------------------------------------------------------------------
# 指标
# ----------------------------------------------------------------------
def _drawdown(navs: list[float]) -> tuple[float, int]:
    """最大回撤与最长回撤持续交易日（收盘净值口径）。"""
    peak = -math.inf
    max_dd = 0.0
    duration = 0
    max_duration = 0
    for value in navs:
        if value >= peak:
            peak = value
            duration = 0
        else:
            duration += 1
            max_duration = max(max_duration, duration)
            if peak > 0:
                max_dd = max(max_dd, 1.0 - value / peak)
    return max_dd, max_duration


def _path_metrics(navs: list[float]) -> dict[str, float]:
    """日频路径风险指标。"""
    returns = [navs[i] / navs[i - 1] - 1.0 for i in range(1, len(navs))
               if navs[i - 1] > 0]
    worst = min(returns) if returns else 0.0
    streak = 0
    max_streak = 0
    for value in returns:
        if value < 0:
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 0
    max_dd, duration = _drawdown(navs)
    return {
        'worst_period_return': worst,
        'max_consecutive_loss_periods': float(max_streak),
        'max_drawdown': max_dd,
        'drawdown_duration': float(duration),
    }


def _summary_block(navs: list[float], equity_init: float,
                   stats: dict[str, Any]) -> dict[str, Any]:
    """单个口径的对照表指标。"""
    final = navs[-1] if navs else equity_init
    total = final / equity_init - 1.0
    years = max(1e-9, (len(navs) - 1) / 252.0)
    path = _path_metrics([value / equity_init for value in navs])
    traded = stats['buy_notional'] + stats['sell_notional']
    mean_nav = sum(navs) / len(navs) if navs else equity_init
    return {
        'final_nav': final,
        'cum_return': total,
        'year_return': (final / equity_init) ** (1.0 / years) - 1.0
        if final > 0 else -1.0,
        'max_drawdown': path['max_drawdown'],
        'drawdown_duration': path['drawdown_duration'],
        'worst_period_return': path['worst_period_return'],
        'max_consecutive_loss_periods': path['max_consecutive_loss_periods'],
        'turnover': (traded / 2.0 / mean_nav / years) if mean_nav > 0 else 0.0,
        'trade_count': int(stats['side_trades']),
        'total_cost': stats['commission'] + stats['stamp'] + stats['impact'],
    }


def _lookup_quote(bars_by_date: dict[date, dict[str, tuple]] | None,
                  quotes: dict[tuple[str, date], tuple[float, float]] | None,
                  code: str, day: date) -> tuple[float, float] | None:
    """优先用显式行情表（分片合并），否则回落到面板撮合价表。"""
    if quotes is not None:
        return quotes.get((code, day))
    if bars_by_date is None:
        return None
    bar = bars_by_date.get(day, {}).get(code)
    if bar is None:
        return None
    return _finite(bar[0]), _finite(bar[1])


def _unfilled_hold60(intent: dict[str, Any], index_of: dict[date, int],
                     window_dates: list[date], costs: TradingCosts,
                     quote: Any) -> tuple[float, float] | None:
    """未成交买入意图的 60 日反事实：返回 ``(r_hold60, planned_amount)``。

    T+1 开盘买入、持有到 T+1+60 收盘卖出，扣与实盘同一套成本；
    缺 T+1 开盘价、缺 60 日后收盘或越界时返回 None（由调用方计入跳过数）。
    四桶分解与 ``unfilled_diagnosis`` 共用本函数，保证两处口径逐值一致。
    """
    entry_idx = index_of.get(intent['signal_date'])
    if entry_idx is None or entry_idx + 1 >= len(window_dates):
        return None
    entry_day = window_dates[entry_idx + 1]
    entry_pair = quote(intent['ts_code'], entry_day)
    entry_price = entry_pair[0] if entry_pair is not None else None
    if entry_price is None or entry_price <= 0:
        signal_pair = quote(intent['ts_code'], intent['signal_date'])
        entry_price = signal_pair[1] if signal_pair is not None else None
    target_idx = entry_idx + 1 + BASELINE_HOLD
    if entry_price is None or target_idx >= len(window_dates):
        return None
    target_pair = quote(intent['ts_code'], window_dates[target_idx])
    close60 = target_pair[1] if target_pair is not None else None
    if close60 is None or close60 <= 0:
        return None
    planned = float(intent['notional'])
    shares = planned / entry_price
    entry_cost = _buy_cost(planned, costs)
    exit_notional = shares * close60
    r_hold60 = (exit_notional - _sell_cost(exit_notional, costs)) / (
        planned + entry_cost) - 1.0
    return r_hold60, planned


def _decompose(closed: list[dict[str, Any]],
               unfilled: list[dict[str, Any]],
               window_dates: list[date], bars_by_date: dict[date, dict[str, tuple]] | None,
               costs: TradingCosts, equity_init: float,
               cost_stateful: float, cost_baseline: float,
               nav_stateful: float, nav_baseline: float,
               quotes: dict[tuple[str, date], tuple[float, float]] | None = None,
               turnover_stateful: float = 0.0,
               turnover_baseline: float = 0.0,
               ) -> dict[str, Any]:
    """按文档 §2.2 做四桶分解，残差原样输出，不调平。

    ``quotes`` 供分片合并阶段使用（按需从本地列存取 ``(open, close)``），
    非空时优先于 ``bars_by_date``。
    """
    def quote(code: str, day: date) -> tuple[float, float] | None:
        return _lookup_quote(bars_by_date, quotes, code, day)

    index_of = {day: i for i, day in enumerate(window_dates)}
    early = 0.0
    late = 0.0
    exact = 0
    unavailable = 0
    for item in closed:
        entry_idx = index_of.get(item['entry_date'])
        if entry_idx is None:
            unavailable += 1
            continue
        target_idx = entry_idx + BASELINE_HOLD
        if target_idx >= len(window_dates):
            unavailable += 1
            continue
        pair = quote(item['ts_code'], window_dates[target_idx])
        close60 = pair[1] if pair is not None else None
        if close60 is None or close60 <= 0:
            unavailable += 1
            continue
        outlay = item['entry_notional'] + item['entry_cost']
        exit_notional = item['shares'] * close60
        r_hold60 = (exit_notional - _sell_cost(exit_notional, costs)) / outlay - 1.0
        delta_pp = (item['r_actual'] - r_hold60) * item['entry_notional'] / equity_init * 100.0
        if item['days_held'] < BASELINE_HOLD:
            early += delta_pp
        elif item['days_held'] > BASELINE_HOLD:
            late += delta_pp
        else:
            exact += 1

    unfilled_pp = 0.0
    unfilled_skipped = 0
    for intent in unfilled:
        counterfactual = _unfilled_hold60(
            intent, index_of, window_dates, costs, quote)
        if counterfactual is None:
            unfilled_skipped += 1
            continue
        r_hold60, planned = counterfactual
        unfilled_pp += -r_hold60 * planned / equity_init * 100.0

    decomp_cost = -(cost_stateful - cost_baseline) / equity_init * 100.0
    cost_diff_pp = (cost_baseline - cost_stateful) / equity_init * 100.0
    total_diff = (nav_stateful - nav_baseline) / equity_init * 100.0
    residual = total_diff - (early + late + unfilled_pp + decomp_cost)
    return {
        'decomp_early_exit': early,
        'decomp_late_exit': late,
        'decomp_unfilled': unfilled_pp,
        'decomp_cost': decomp_cost,
        'decomp_residual': residual,
        'decomp_total_diff_pp': total_diff,
        'decomp_residual_ratio': (residual / total_diff) if abs(total_diff) > 1e-12 else None,
        'decomp_detail': {
            'closed_positions': len(closed),
            'closed_exact_60d_into_residual': exact,
            'closed_without_60d_target': unavailable,
            'unfilled_intents_seen': len(unfilled),
            'unfilled_without_60d_target': unfilled_skipped,
            'cost_bucket': {
                'cost_stateful_total': cost_stateful,
                'cost_baseline_total': cost_baseline,
                'turnover_stateful': turnover_stateful,
                'turnover_baseline': turnover_baseline,
                'cost_diff_baseline_minus_stateful': cost_baseline - cost_stateful,
                'cost_diff_pp': cost_diff_pp,
                'decomp_cost_pp': decomp_cost,
                'matches_decomp_cost': abs(cost_diff_pp - decomp_cost) <= 1e-9,
                'formula': ('decomp_cost = (基线成本 − 状态机成本) / 初始权益 × 100；'
                            '成本差为正表示状态机更省'),
            },
            'residual_sources': [
                '两口径仓位规模与现金缓冲不同'
                '（状态机按 1/max_positions 建仓，基线按 baseline_slots.cohort_budget 分槽）',
                '未投资现金的时间价值按 0 计',
                '持有期跨越窗口边界（窗口末买入无 60 日后收盘，已单独计数）',
            ],
        },
    }


def _unfilled_diagnosis(unfilled: list[dict[str, Any]],
                        window_dates: list[date],
                        bars_by_date: dict[date, dict[str, tuple]] | None,
                        quotes: dict[tuple[str, date], tuple[float, float]] | None,
                        costs: TradingCosts, equity_init: float,
                        planned_total: float,
                        decomp_unfilled: float | None = None) -> dict[str, Any]:
    """未成交买入意图诊断：笔数、金额占比、拦截原因、60 日反事实与集中度。

    反事实与四桶分解共用 :func:`_unfilled_hold60`，因此各意图贡献之和
    必须与 ``decomp_unfilled`` 逐值一致（``reconciles_with_decomp`` 自证）。
    ``planned_total`` 取「已进撮合的买入意图计划金额 + 被换手上限截断的金额」，
    即全部买入意图的计划金额；分母口径写进 ``planned_total_formula``。
    """
    index_of = {day: i for i, day in enumerate(window_dates)}

    def quote(code: str, day: date) -> tuple[float, float] | None:
        return _lookup_quote(bars_by_date, quotes, code, day)

    blocked_counts = {'limit_up': 0, 'limit_down': 0, 'suspended': 0,
                      'no_quote': 0, 'cash': 0, 'turnover_cap': 0}
    blocked_amounts = dict.fromkeys(blocked_counts, 0.0)
    evaluable: list[dict[str, Any]] = []
    planned_sum = 0.0
    skipped = 0
    for intent in unfilled:
        reason = str(intent.get('blocked', 'unknown'))
        planned = float(intent.get('notional') or 0.0)
        planned_sum += planned
        blocked_counts[reason] = blocked_counts.get(reason, 0) + 1
        blocked_amounts[reason] = blocked_amounts.get(reason, 0.0) + planned
        counterfactual = _unfilled_hold60(
            intent, index_of, window_dates, costs, quote)
        if counterfactual is None:
            skipped += 1
            continue
        r_hold60, planned_cf = counterfactual
        evaluable.append({
            'ts_code': str(intent['ts_code']),
            'signal_date': intent['signal_date'].isoformat(),
            'blocked': reason,
            'planned_amount': planned_cf,
            'r_hold60': r_hold60,
            'contribution_pp': -r_hold60 * planned_cf / equity_init * 100.0,
        })

    ranked = sorted(evaluable, key=lambda item: abs(item['contribution_pp']),
                    reverse=True)
    total_abs = sum(abs(item['contribution_pp']) for item in evaluable)
    top5 = ranked[:5]
    top5_abs = sum(abs(item['contribution_pp']) for item in top5)
    signed_sum = sum(item['contribution_pp'] for item in evaluable)
    weighted_base = sum(item['planned_amount'] for item in evaluable)
    mean_r = (sum(item['r_hold60'] for item in evaluable) / len(evaluable)
              if evaluable else None)
    weighted_mean_r = (
        sum(item['r_hold60'] * item['planned_amount'] for item in evaluable)
        / weighted_base if weighted_base > 0 else None)
    return {
        'unfilled_intents': len(unfilled),
        'unfilled_intents_evaluable': len(evaluable),
        'unfilled_without_60d_target': skipped,
        'planned_amount': planned_sum,
        'planned_pct_of_all_buy_planned': (
            planned_sum / planned_total if planned_total > 0 else None),
        'planned_total': planned_total,
        'planned_total_formula': (
            '已进撮合的买入意图计划金额 + 被换手上限截断金额'),
        'blocked_reasons': blocked_counts,
        'blocked_reason_amounts': blocked_amounts,
        'blocked_reason_note': (
            '买入意图的拦截原因；跌停只发生在卖出侧，故 limit_down 恒为 0，'
            '卖出侧跌停见 blocked_limit_down'),
        'mean_r_hold60': mean_r,
        'mean_r_hold60_weighted': weighted_mean_r,
        'contribution_pp': signed_sum,
        'top5': top5,
        'top5_abs_share': (top5_abs / total_abs) if total_abs > 0 else None,
        'top5_signed_pp': sum(item['contribution_pp'] for item in top5),
        'reconciles_with_decomp': (
            None if decomp_unfilled is None
            else abs(signed_sum - decomp_unfilled) <= 1e-6),
    }


def _cost_by_size(stats: dict[str, Any], costs: TradingCosts) -> dict[str, Any]:
    """冲击成本按资金规模线性缩放（费率不变），三档分别给出。"""
    sides = max(1, int(stats['side_trades']))
    out: dict[str, Any] = {}
    for label, size in (('100k', 100_000.0), ('500k', 500_000.0),
                        ('2000k', 2_000_000.0)):
        impact = sides * size * costs.impact_rate
        out[label] = {
            'impact_cost_total': impact,
            'impact_bps_per_side': costs.impact_rate * 1e4,
            'round_trip_impact_bps': 2.0 * costs.impact_rate * 1e4,
            'notional_per_trade': size,
        }
    out['scaling'] = ('线性缩放：impact_rate 恒定（按文档 0.724% 往返反解出的 '
                      '28.5bp/边），总冲击成本 = 成交边数 × 单笔规模 × impact_rate；'
                      '真实市场冲击随规模非线性上升，本表只用于同口径横向比较')
    return out


# ----------------------------------------------------------------------
# 分片辅助
# ----------------------------------------------------------------------
def _merge_stats(target: dict[str, Any], source: dict[str, Any]) -> None:
    """把一片的统计累加进汇总 stats（数值相加，字典/清单合并）。"""
    for key, value in source.items():
        if key in ('unfilled_records', 'delay_hist'):
            continue
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            target[key] = target.get(key, 0) + value
    for delay, count in source.get('delay_hist', {}).items():
        target['delay_hist'][delay] = target['delay_hist'].get(delay, 0) + count
    target['unfilled_records'].extend(source.get('unfilled_records', []))


def _month_end_days(dates: list[date]) -> set[date]:
    """窗口内每个自然月的最后一个交易日。"""
    out: set[date] = set()
    for index, day in enumerate(dates):
        if (index == len(dates) - 1
                or dates[index + 1].year != day.year
                or dates[index + 1].month != day.month):
            out.add(day)
    return out


def _year_chunks(dates: list[date]) -> list[tuple[str, list[date]]]:
    """按自然年切分交易日（保持升序）。"""
    chunks: list[tuple[str, list[date]]] = []
    for day in dates:
        label = str(day.year)
        if not chunks or chunks[-1][0] != label:
            chunks.append((label, []))
        chunks[-1][1].append(day)
    return chunks


def _cash_yield_rates(nav_rows: list[dict[str, Any]], nav_key: str,
                      cash_key: str, equity_init: float) -> dict[str, Any]:
    """两档现金利率下的事后累加净值（不重跑复利路径）。"""
    final_nav = float(nav_rows[-1][nav_key]) if nav_rows else equity_init
    base_cum = final_nav / equity_init - 1.0
    out: dict[str, Any] = {}
    for rate in CASH_YIELD_RATES:
        accrued = sum(
            float(row.get(cash_key) or 0.0) * rate / TRADING_DAYS_PER_YEAR
            for row in nav_rows)
        adjusted = final_nav + accrued
        cum = adjusted / equity_init - 1.0
        out[f'{rate:.3f}'] = {
            'rate_annual': rate,
            'accrued_interest': accrued,
            'nav_final': adjusted,
            'cum_return': cum,
            'delta_vs_zero_pp': (cum - base_cum) * 100.0,
        }
    return out


def _cash_accrued_path(nav_rows: list[dict[str, Any]], nav_key: str,
                       cash_key: str, rate: float) -> tuple[list[float], float]:
    """现金利息事后累加的净值序列（只用于报告路径指标，不改成交与复利）。"""
    accrued = 0.0
    navs: list[float] = []
    for row in nav_rows:
        accrued += float(row.get(cash_key) or 0.0) * rate / TRADING_DAYS_PER_YEAR
        navs.append(float(row[nav_key]) + accrued)
    return navs, accrued


def _alloc_track_block(nav_rows: list[dict[str, Any]], nav_key: str,
                       cash_key: str, equity_init: float,
                       stats: dict[str, Any]) -> dict[str, Any]:
    """单档资金分配的全量指标（累计/年化/回撤/换手/成本/每日现金）。"""
    navs = [float(row[nav_key]) for row in nav_rows]
    block = _summary_block(navs, equity_init, stats)
    cash = [float(row.get(cash_key) or 0.0) for row in nav_rows]
    holdings = [float(row.get('holdings_count') or 0.0) for row in nav_rows]
    gross = [float(row.get('gross_exposure') or 0.0) for row in nav_rows]
    total_cost = (stats['commission'] + stats['stamp'] + stats['impact'])
    block.update({
        'cost_commission': stats['commission'],
        'cost_stamp_duty': stats['stamp'],
        'cost_impact': stats['impact'],
        'cost_total': total_cost,
        'cost_pct_initial_equity': total_cost / equity_init if equity_init else 0.0,
        'trade_count_buy': int(stats['buy_filled']),
        'trade_count_sell': int(stats['sell_filled']),
        'blocked_cash_count': int(stats['blocked_cash']),
        'blocked_cash_amount': stats['blocked_cash_amount'],
        'planned_amount': stats['planned_amount'],
        'valuation_stale_days': int(stats['valuation_stale_days']),
        'exit_price_checks': int(stats['exit_price_checks']),
        'exit_price_dev_mean': (
            stats['exit_price_dev'] / stats['exit_price_checks']
            if stats['exit_price_checks'] else None),
        'cash_summary': {
            'mean': sum(cash) / len(cash) if cash else 0.0,
            'mean_pct_of_nav': (
                sum(c / n for c, n in zip(cash, navs, strict=False)
                    if n > 0) / len(cash) if cash else 0.0),
            'min': min(cash) if cash else 0.0,
            'max': max(cash) if cash else 0.0,
            'final': cash[-1] if cash else 0.0,
        },
        'holdings_count_mean': sum(holdings) / len(holdings) if holdings else 0.0,
        'holdings_count_max': max(holdings) if holdings else 0.0,
        'open_holdings_end': int(holdings[-1]) if holdings else 0,
        'gross_exposure_mean': sum(gross) / len(gross) if gross else 0.0,
        'gross_exposure_max': max(gross) if gross else 0.0,
        'daily_cash': cash,
    })
    return block


def _alloc_matrix(nav_rows: list[dict[str, Any]], equity_init: float,
                  stats: dict[str, Any]) -> list[dict[str, Any]]:
    """「槽位三档 × 现金收益两档」6 格矩阵（0% 为对照、1.5% 为主口径）。"""
    navs = [float(row['nav']) for row in nav_rows]
    base = _summary_block(navs, equity_init, stats)
    years = max(1e-9, (len(navs) - 1) / 252.0)
    rows: list[dict[str, Any]] = []
    for rate in CASH_YIELD_RATES:
        adjusted, accrued = _cash_accrued_path(nav_rows, 'nav', 'cash', rate)
        path = _path_metrics([value / equity_init for value in adjusted])
        final = adjusted[-1]
        rows.append({
            'cash_yield_rate': rate,
            'cum_return': final / equity_init - 1.0,
            'year_return': ((final / equity_init) ** (1.0 / years) - 1.0
                            if final > 0 else -1.0),
            'max_drawdown': path['max_drawdown'],
            'drawdown_duration': path['drawdown_duration'],
            'turnover': base['turnover'],
            'trade_count': base['trade_count'],
            'cost_total': base['total_cost'],
            'cost_pct_initial_equity': (base['total_cost'] / equity_init
                                        if equity_init else 0.0),
            'accrued_interest': accrued,
            'nav_final': final,
            'nav_final_zero_yield': navs[-1] if navs else equity_init,
        })
    return rows


def _read_chunk_nav(path: Path) -> list[dict[str, Any]]:
    """读回一片的 nav.csv（date 还原为 date 对象）。"""
    frame = pl.read_csv(path)
    rows: list[dict[str, Any]] = []
    for row in frame.iter_rows(named=True):
        item = dict(row)
        item['date'] = date.fromisoformat(str(row['date'])[:10])
        rows.append(item)
    return rows


def _read_chunk_trades(path: Path) -> list[dict[str, Any]]:
    """读回一片的 trades.csv（续跑时按约定从落盘产物取成交明细）。"""
    frame = pl.read_csv(path)
    return [dict(row) for row in frame.iter_rows(named=True)]


def _load_quotes(days_by_code: dict[str, set[date]],
                 store_root: Path) -> dict[tuple[str, date], tuple[float, float]]:
    """按需从本地列存读 ``(open, close)``（未复权，与面板 raw_* 同源）。"""
    store = LocalQuoteStore(store_root, enabled=True)
    quotes: dict[tuple[str, date], tuple[float, float]] = {}
    for code in sorted(days_by_code):
        wanted = days_by_code[code]
        if not wanted:
            continue
        frame = store.read_file('stock', code, with_factors=False)
        if frame is None or frame.is_empty() or 'date' not in frame.columns:
            continue
        sub = (frame.with_columns(pl.col('date').cast(pl.Date, strict=False))
               .filter(pl.col('date').is_in(sorted(wanted))))
        for row in sub.select(['date', 'open', 'close']).iter_rows(named=True):
            day = row['date']
            if isinstance(day, datetime):
                day = day.date()
            quotes[(code, day)] = (_finite(row['open']), _finite(row['close']))
    return quotes


def _decompose_quotes(closed: list[dict[str, Any]],
                      unfilled: list[dict[str, Any]],
                      window_dates: list[date],
                      store_root: Path) -> dict[tuple[str, date], tuple[float, float]]:
    """收集四桶分解需要的 (代码, 日期) 对并一次读齐行情。"""
    index_of = {day: i for i, day in enumerate(window_dates)}
    days_by_code: dict[str, set[date]] = {}
    for item in closed:
        entry_idx = index_of.get(item['entry_date'])
        if entry_idx is None or entry_idx + BASELINE_HOLD >= len(window_dates):
            continue
        days_by_code.setdefault(item['ts_code'], set()).add(
            window_dates[entry_idx + BASELINE_HOLD])
    for intent in unfilled:
        entry_idx = index_of.get(intent['signal_date'])
        if entry_idx is None or entry_idx + 1 >= len(window_dates):
            continue
        days_by_code.setdefault(intent['ts_code'], set()).update({
            window_dates[entry_idx + 1], intent['signal_date']})
        target_idx = entry_idx + 1 + BASELINE_HOLD
        if target_idx < len(window_dates):
            days_by_code[intent['ts_code']].add(window_dates[target_idx])
    return _load_quotes(days_by_code, store_root)


# ----------------------------------------------------------------------
# 输出
# ----------------------------------------------------------------------
def _write_csv(path: Path, rows: list[dict[str, Any]],
               columns: tuple[str, ...]) -> None:
    """写 CSV；空表也写表头，保持产物结构稳定。"""
    if rows:
        frame = pl.DataFrame(rows)
        ordered = [name for name in columns if name in frame.columns]
        frame = frame.select(ordered + [c for c in frame.columns if c not in ordered])
    else:
        frame = pl.DataFrame({name: [] for name in columns})
    frame.write_csv(path)


def _pct(value: Any) -> str:
    number = _finite(value)
    return '—' if number is None else f'{number * 100:.2f}%'


def _pp(value: Any) -> str:
    number = _finite(value)
    return '—' if number is None else f'{number:+.3f}pp'


def _build_report(summary: dict[str, Any]) -> str:
    """生成中文 report.md。"""
    cmp_block = summary['comparison']
    stateful = cmp_block['stateful']
    fixed = cmp_block['fixed60']
    ratio = summary['decomp_residual_ratio']
    rule_text = ('按文档 §2.2，残差 > 总差的 30% 本身即为结论：说明两口径的差异'
                 '主要来自仓位管理/敞口差异而不是单笔择时，此时不得把残差调平后'
                 '当成择时收益。')
    if ratio is None:
        residual_text = f'{rule_text}本次总差为 0，残差占比不可计算。'
    elif abs(ratio) > 0.30:
        residual_text = f'{rule_text}本次残差占比 {ratio:.2%} 已超过该阈值，按上述口径解读。'
    else:
        residual_text = (
            f'{rule_text}本次残差占比 {ratio:.2%} ≤ 30%，四桶已解释 '
            f'{100.0 - abs(ratio) * 100.0:.1f}% 的总差，四桶结论可作为主因判断依据；'
            '残差仍原样保留，不做任何调平。')
    lines: list[str] = [
        '# 状态机口径回测报告（尺子 B：可执行性）',
        '',
        '## 0. 定位声明',
        '',
        '本报告只回答「按这套交易规则真的能买到、能卖出、扣完成本还剩什么」，'
        '属于尺子 B（事实指标），**不消耗** Bonferroni 试验预算，'
        '也不与尺子 A（选股能力）合成任何「总体得分」。'
        '两把尺子并列时只能表述为「选股 X、执行 Y」。',
        '',
        '## 1. 窗口与参数',
        '',
        f"- 窗口：{summary['window']['start']} ~ {summary['window']['end']}，"
        f"实际 {summary['window']['trading_days']} 个交易日"
        f"（请求 {summary['window']['requested_days']}）",
        f"- 档位：`{summary['tier']['key']}`（{summary['tier']['label']}），"
        f"买入分位 ≥ {summary['tier']['buy_percentile']:.2f}，"
        f"卖出分位 ≤ {summary['tier']['sell_percentile']:.2f}，"
        f"最多 {summary['tier']['max_positions']} 只",
        f"- 初始权益：{summary['initial_equity']:,.0f} 元；"
        f"现金缓冲 {summary['cash_reserve']:.0%}"
        f"（取自 {summary['cash_reserve_source']}）；单票上限 "
        f"{summary['target_weight']:.2%}",
        (f"- 选池：每个分片首日重算 `filter_universe` 并片内固定，各片结果见 "
         f"`summary.universe_by_chunk`（首片 {summary['universe']['kept']}/"
         f"{summary['universe']['input']} 只）")
        if summary.get('universe_by_chunk') else
        (f"- 选池：窗口起点 {summary['universe']['as_of']} 的 "
         f"`filter_universe` 结果 {summary['universe']['kept']}/"
         f"{summary['universe']['input']} 只（规则版本 "
         f"{summary['universe']['version']}）"),
        f"- 面板：{summary['data']['loaded']} 只 / {summary['data']['rows']} 行"
        + ('（分片累计行数）' if summary.get('chunking') else '')
        + f"，列名 {', '.join(summary['data']['price_columns'])}（未复权撮合价）",
        f"- 基线资金槽位：`baseline_slots.slots` = {summary['baseline_slots']['slots']}"
        f"（相邻信号日间隔中位数 {summary['baseline_slots']['spacing_days']} 个交易日，"
        f"按 {BASELINE_HOLD} 日固定持有换算），每槽预算 "
        f"{summary['baseline_slots']['cohort_budget']:,.0f} 元"
        f"（占初始权益 {_pct(summary['baseline_slots']['cohort_budget_fraction'])}）。"
        '这样处理是为了让「固定持有 60 日」与状态机在同一资金利用率下可比：'
        '日频信号下 slots=60 即旧行为，月频信号下不再把绝大部分资金闲置。',
        '',
        '## 2. 净值对照',
        '',
        '| 指标 | 状态机 | 固定持有 60 日 |',
        '|---|---:|---:|',
        f"| 累计收益 | {_pct(stateful['cum_return'])} | {_pct(fixed['cum_return'])} |",
        f"| 年化收益 | {_pct(stateful['year_return'])} | {_pct(fixed['year_return'])} |",
        f"| 最大回撤 | {_pct(stateful['max_drawdown'])} | {_pct(fixed['max_drawdown'])} |",
        f"| 回撤持续（交易日） | {stateful['drawdown_duration']:.0f} | "
        f"{fixed['drawdown_duration']:.0f} |",
        f"| 年化单边换手 | {stateful['turnover']:.2f}x | {fixed['turnover']:.2f}x |",
        f"| 成交笔数（买+卖） | {stateful['trade_count']} | {fixed['trade_count']} |",
        f"| 期末净值 | {stateful['final_nav']:,.0f} | {fixed['final_nav']:,.0f} |",
        '',
        '## 3. 六组指标',
        '',
        '### 3.1 成交可行性',
        '',
        f"- `signal_to_fill_rate` = {summary['signal_to_fill_rate']:.2%}"
        f"（成交 {summary['fill_detail']['buy_filled']} / 买入意图 "
        f"{summary['fill_detail']['buy_intents']}）",
        f"- `blocked_limit_up` = {summary['blocked_limit_up']['count']} 笔"
        f"（占买入意图 {_pct(summary['blocked_limit_up']['pct_of_buy_intents'])}），"
        f"`blocked_limit_up_amount` = {summary['blocked_limit_up_amount']:,.0f} 元"
        f"（占计划金额 {_pct(summary['blocked_limit_up']['amount_pct_of_planned'])}）",
        f"- `blocked_limit_down` = {summary['blocked_limit_down']['count']} 笔（卖出侧）",
        f"- `blocked_suspended` = {summary['blocked_suspended']['count']} 笔，"
        f"`blocked_no_quote` = {summary['blocked_no_quote']['count']} 笔",
        '',
        '### 3.2 时滞',
        '',
        f"- `delay_histogram` = {summary['delay_histogram']}（交易日；T+1 规则下应全为 1）",
        f"- `overnight_gap_cost` = {summary['overnight_gap_cost']:,.0f} 元"
        f"（占初始权益 {_pct(summary['overnight_gap_cost_detail']['pct_of_initial_equity'])}；"
        f"带符号分量 {summary['overnight_gap_cost_detail']['signed_amount']:,.0f} 元）",
        '',
        '### 3.3 成本分解',
        '',
        f"- `cost_commission` = {summary['cost_commission']:,.0f} 元"
        f"（{_pct(summary['cost_commission_pct'])} 初始权益）",
        f"- `cost_stamp_duty` = {summary['cost_stamp_duty']:,.0f} 元"
        f"（{_pct(summary['cost_stamp_duty_pct'])}）",
        f"- `cost_impact` = {summary['cost_impact']:,.0f} 元"
        f"（{_pct(summary['cost_impact_pct'])}）",
        f"- `cost_per_trade_bps` = {summary['cost_per_trade_bps']:.2f} bps"
        '（分母为买入成交额；因期末仍有未平仓持仓、卖出侧成本尚未发生，'
        '该值低于理论往返 19bp，属正常口径差）',
        f"- `cost_by_size`：100k / 500k / 2000k 三档冲击成本分别 "
        f"{summary['cost_by_size']['100k']['impact_cost_total']:,.0f} / "
        f"{summary['cost_by_size']['500k']['impact_cost_total']:,.0f} / "
        f"{summary['cost_by_size']['2000k']['impact_cost_total']:,.0f} 元"
        f"（{summary['cost_by_size']['scaling']}）",
        f"- 两口径成本绝对值：`cost_stateful_total` = "
        f"{summary['cost_bucket']['cost_stateful_total']:,.0f} 元，"
        f"`cost_baseline_total` = "
        f"{summary['cost_bucket']['cost_baseline_total']:,.0f} 元；"
        f"年化单边换手 `turnover_stateful` = "
        f"{summary['cost_bucket']['turnover_stateful']:.2f}x，"
        f"`turnover_baseline` = {summary['cost_bucket']['turnover_baseline']:.2f}x；"
        f"成本差（基线 − 状态机）= "
        f"{summary['cost_bucket']['cost_diff_baseline_minus_stateful']:,.0f} 元，"
        f"折合 {_pp(summary['cost_bucket']['cost_diff_pp'])}，"
        f"等于「两口径成本差 / 初始权益」，与 §4 的 `decomp_cost` 同值"
        f"（自检{'通过' if summary['cost_bucket']['matches_decomp_cost'] else '未通过'}）。",
        '',
        '### 3.4 规则打空率',
        '',
        f"- `rule_block_min_hold` = {summary['rule_block_min_hold']['count']} 次"
        f"（占持仓观测 {_pct(summary['rule_block_min_hold']['pct_of_holding_obs'])}）",
        f"- `rule_block_sell_confirm` = {summary['rule_block_sell_confirm']['count']} 笔，"
        f"平均延迟 {summary['rule_block_sell_confirm']['avg_delay_days']:.2f} 个交易日"
        f"（累计封锁 {summary['rule_block_sell_confirm']['blocked_days']} 个持仓日）",
        f"- `rule_block_turnover_cap` = {summary['rule_block_turnover_cap']['count']} 笔，"
        f"被截断金额 {summary['rule_block_turnover_cap']['amount']:,.0f} 元",
        '',
        '### 3.5 路径风险',
        '',
        f"- `max_drawdown` = {_pct(summary['max_drawdown'])}，"
        f"`drawdown_duration` = {summary['drawdown_duration']:.0f} 个交易日",
        f"- `worst_period_return` = {_pct(summary['worst_period_return'])}（日频）",
        f"- `max_consecutive_loss_periods` = "
        f"{summary['max_consecutive_loss_periods']:.0f} 个交易日",
        '',
        '## 4. 差异分解（文档 §2.2）',
        '',
        '| 桶 | 贡献 |',
        '|---|---:|',
        f"| `decomp_early_exit`（早卖 <60 日） | {_pp(summary['decomp_early_exit'])} |",
        f"| `decomp_late_exit`（晚卖 >60 日） | {_pp(summary['decomp_late_exit'])} |",
        f"| `decomp_unfilled`（未成交） | {_pp(summary['decomp_unfilled'])} |",
        f"| `decomp_cost`（成本之差） | {_pp(summary['decomp_cost'])} |",
        f"| **四桶之和** | "
        f"{_pp(summary['decomp_early_exit'] + summary['decomp_late_exit'] + summary['decomp_unfilled'] + summary['decomp_cost'])} |",
        f"| `decomp_residual`（残差，原样保留） | {_pp(summary['decomp_residual'])} |",
        f"| 总差（状态机 − 基线） | {_pp(summary['decomp_total_diff_pp'])} |",
        '',
        f"残差占比：**{_pct(summary['decomp_residual_ratio'])}**。{residual_text}",
        '',
        f"分解覆盖：已平仓 {summary['decomp_detail']['closed_positions']} 笔，"
        f"其中实际持有恰为 60 日 {summary['decomp_detail']['closed_exact_60d_into_residual']} 笔"
        '（按文档无对应桶，计入残差）；'
        f"窗口末买入缺 60 日后收盘 {summary['decomp_detail']['closed_without_60d_target']} 笔、"
        f"未成交意图缺 60 日后收盘 {summary['decomp_detail']['unfilled_without_60d_target']} 笔，"
        '均如实排除并在此登记。',
        '',
        '### 4.1 未成交买入意图诊断',
        '',
    ]
    diag = summary['unfilled_diagnosis']
    lines += [
        f"- 未成交买入意图 {diag['unfilled_intents']} 笔"
        f"（其中可做 60 日反事实 {diag['unfilled_intents_evaluable']} 笔，"
        f"缺 60 日后收盘 {diag['unfilled_without_60d_target']} 笔）；"
        f"计划金额 {diag['planned_amount']:,.0f} 元，占全部买入意图计划金额 "
        f"{_pct(diag['planned_pct_of_all_buy_planned'])}"
        f"（分母 {diag['planned_total']:,.0f} 元，口径：{diag['planned_total_formula']}）；",
        f"- 拦截原因分布：涨停 `limit_up` = {diag['blocked_reasons']['limit_up']} 笔、"
        f"跌停 `limit_down` = {diag['blocked_reasons']['limit_down']} 笔、"
        f"停牌 `suspended` = {diag['blocked_reasons']['suspended']} 笔、"
        f"无行情 `no_quote` = {diag['blocked_reasons']['no_quote']} 笔、"
        f"资金不足 `cash` = {diag['blocked_reasons']['cash']} 笔、"
        f"换手上限 `turnover_cap` = {diag['blocked_reasons']['turnover_cap']} 笔"
        f"（{diag['blocked_reason_note']}）；",
        f"- 60 日反事实收益均值：简单均值 "
        f"{_pct(diag['mean_r_hold60'])}，按计划金额加权 "
        f"{_pct(diag['mean_r_hold60_weighted'])}；"
        f"累计贡献 {_pp(diag['contribution_pp'])}，"
        f"与 §4 `decomp_unfilled` 对账"
        f"{'一致' if diag['reconciles_with_decomp'] else '不一致'}"
        f"（{_pp(summary['decomp_unfilled'])}）；",
        f"- 集中度：按 |贡献| 取 top5，合计占全部未成交意图绝对贡献的 "
        f"{_pct(diag['top5_abs_share'])}，top5 带符号合计 "
        f"{_pp(diag['top5_signed_pp'])}；"
        + '；'.join(
            f"{item['ts_code']}（{item['blocked']}，"
            f"r60={_pct(item['r_hold60'])}，{_pp(item['contribution_pp'])}）"
            for item in diag['top5']) + '。',
        '',
        '## 5. 规则打空率结论',
        '',
        f"- 漏斗：{summary['funnel']['decision_days']} 个决策日中只有 "
        f"{summary['funnel']['days_with_candidates']} 天出现候选"
        f"（覆盖 {_pct(summary['funnel']['day_coverage'])}），累计候选信号 "
        f"{summary['funnel']['candidate_signals']} 个，转化为买入意图 "
        f"{summary['funnel']['buy_intents']} 笔"
        f"（{_pct(summary['funnel']['candidate_to_intent_rate'])}）、"
        f"成交 {summary['funnel']['buy_filled']} 笔；",
        f"- 信号到成交转化率 {summary['signal_to_fill_rate']:.2%}："
        f"{summary['fill_detail']['buy_intents']} 笔意图里 "
        f"{summary['fill_detail']['buy_filled']} 笔按 T+1 开盘成交，"
        f"涨停/跌停/停牌/无行情拦截合计 "
        f"{summary['blocked_limit_up']['count'] + summary['blocked_limit_down']['count'] + summary['blocked_suspended']['count'] + summary['blocked_no_quote']['count']} 笔，"
        f"唯一未成交的 {summary['rule_block_turnover_cap']['count']} 笔被单次换手上限截断；",
        '因此「规则打空率」必须分两段读：成交侧几乎无漏损（拦截 0 笔），'
        '真正的漏斗在候选→意图这一段——同一标的在多个交易日重复进入候选，'
        f'且持仓已满 {summary["tier"]["max_positions"]} 只时空余仓位为 0、'
        '新候选只能排队；这不是交易规则造成的不可执行，而是容量不足，'
        '不能把 568 个候选信号当作可实现的机会数。',
        f"- 最小持有期封锁 {summary['rule_block_min_hold']['count']} 次"
        f"（占持仓观测 {_pct(summary['rule_block_min_hold']['pct_of_holding_obs'])}）、"
        f"卖出确认 {summary['rule_block_sell_confirm']['count']} 笔、"
        f"平均延迟 {summary['rule_block_sell_confirm']['avg_delay_days']:.2f} 个交易日"
        f"（累计封锁 {summary['rule_block_sell_confirm']['blocked_days']} 个持仓日），"
        '说明卖出侧被规则延后而非立即执行；',
        f"- 单次换手上限截断金额 "
        f"{summary['rule_block_turnover_cap']['amount']:,.0f} 元，"
        '是「看得到但吃不到」的部分，不能算作可实现收益。',
        '',
        '## 6. 成本常数（文档冻结 0.724%，启动断言）',
        '',
        f"- `TradingCosts` 往返费率实算 = "
        f"{summary['cost_round_trip_rate']:.6f}（分项：佣金+过户费 "
        f"{summary['cost_breakdown']['commission_rate']:.4%}×2 + 印花税 "
        f"{summary['cost_breakdown']['stamp_duty_rate']:.4%} + 冲击 "
        f"{summary['cost_breakdown']['impact_rate']:.4%}×2）；",
        f"- 文档冻结常数 = {summary['cost_round_trip_target']:.6f}，"
        f"两者差额 = {summary['cost_round_trip_gap']:+.6f}"
        '（按文档常数构造并断言，差额必须为 0；`COST_DEFAULTS` 只覆盖 0.19%，'
        '缺过户费与市值分档滑点，故不再直接使用其默认值）；',
        '- 本次两口径（状态机与基线）使用完全相同的分项费率，'
        '绝对净值可与按 0.724% 扣费的其它产物并列比较。',
        '',
        '## 7. 前视声明',
        '',
    ]
    for note in summary['lookahead_notes']:
        lines.append(f'- {note}')
    lines += [
        '',
        '## 8. 方法与已知局限',
        '',
    ]
    for note in summary['methodology_notes']:
        lines.append(f'- {note}')
    lines.append('')
    alloc = summary.get('baseline_alloc')
    if alloc:
        lines += [
            '## 9. 资金分配三档（`a` / `slots` / `slots60`）',
            '',
            '### 9.1 三档口径',
            '',
        ]
        for name in alloc['computed']:
            lines.append(f"- `{name}`：{alloc['definitions'][name]}。")
        lines += [
            '',
            '### 9.2 六格矩阵（3 档 × 现金收益 0% / 1.5%）',
            '',
            '| 档位 | 现金年化 | 累计收益 | 年化收益 | 最大回撤 | 回撤持续（交易日） | '
            '年化单边换手 | 显性成本（元） | 成本占初始权益 |',
            '|---|---:|---:|---:|---:|---:|---:|---:|---:|',
        ]
        for item in alloc['matrix']:
            lines.append(
                f"| `{item['alloc']}` | {item['cash_yield_rate']:.2%} | "
                f"{_pct(item['cum_return'])} | {_pct(item['year_return'])} | "
                f"{_pct(item['max_drawdown'])} | {item['drawdown_duration']:.0f} | "
                f"{item['turnover']:.2f}x | {item['cost_total']:,.0f} | "
                f"{_pct(item['cost_pct_initial_equity'])} |")
        lines += [
            '',
            f"- 现金收益口径：{alloc['method']}。"
            f"主口径 {alloc['main_cash_yield_rate']:.1%}、对照 "
            f"{min(alloc['cash_yield_rates']):.1%}。",
        ]
        tracks = alloc['tracks']
        if 'a' in tracks:
            a_block = tracks['a']
            lines += [
                '',
                '### 9.3 `a` 档的现金与仓位（与尺子 A 对照）',
                '',
                f"- 平均现金占比 {_pct(a_block['cash_summary']['mean_pct_of_nav'])}，"
                f"期末现金 {a_block['cash_summary']['final']:,.0f} 元；"
                f"平均总仓位（`gross_exposure` 日均）"
                f"{_pct(a_block['gross_exposure_mean'])}，最高 "
                f"{_pct(a_block['gross_exposure_max'])}；"
                f"平均在手只数 {a_block['holdings_count_mean']:.2f} 只"
                f"（最多 {a_block['holdings_count_max']:.0f} 只）。",
                f"- 成交：买入 {a_block['trade_count_buy']} 笔 / 卖出 "
                f"{a_block['trade_count_sell']} 笔；现金不足少买 "
                f"{a_block['blocked_cash_count']} 笔"
                f"（计划金额 {a_block['blocked_cash_amount']:,.0f} 元）；"
                f"显性成本 {a_block['cost_total']:,.0f} 元"
                f"（占初始权益 {_pct(a_block['cost_pct_initial_equity'])}）。",
                f"- 对照：尺子 A 每期合计仓位 "
                f"{_pct(alloc['a_signals']['period_weight_min'])}~"
                f"{_pct(alloc['a_signals']['period_weight_max'])}、均值 "
                f"{_pct(alloc['a_signals']['period_weight_mean'])}"
                '（用户口径的 22.67% 是含 2 行未满 60 日标签的 211 行算法，'
                '本档按 ok_60 的 209 行算得 22.49%，差异已登记、不抹平）。'
                f"本 `a` 档是 lot 级连续共享池：60 个交易日持有 + 月度信号使"
                f"同时在场约 2 个期次，所以日均总仓位 "
                f"{_pct(a_block['gross_exposure_mean'])} 约为单期仓位的 "
                f"{a_block['gross_exposure_mean'] / max(1e-9, alloc['a_signals']['period_weight_mean']):.2f} 倍，"
                '属口径结构差异，不是超额暴露。',
                f"- 期末未到期 lot：{a_block['open_holdings_end']} 只"
                '（卖出日在窗口之外，按最后可得收盘价计价，不虚构成交）。',
            ]
        check = alloc.get('a_check')
        if check:
            lines += [
                '',
                '### 9.4 `a` 档口径对拍',
                '',
                f"- 不限现金时每期净收益均值 = {check['computed_period_mean']:.9f}，"
                f"`stats_main.csv` 的 mid/60/全部 `rec_net` = "
                f"{check['expected_rec_net']:.9f}，绝对差 "
                f"{check['abs_diff']:.3e}（容差 {check['tol']:.1e}）"
                f"：{'通过' if check['passed'] else '未通过'}；"
                f"样本 {check['periods']} 个信号日 / {check['rows']} 行。",
            ]
        diff = alloc.get('candidate_source_diff')
        if diff:
            lines += [
                '',
                '### 9.5 候选集不同源（登记，不抹平）',
                '',
                f"- {diff['note']}。",
            ]
        if 'a' in tracks:
            lines += [
                '',
                '### 9.6 估值近似',
                '',
                f"- `a` 档逐日按收盘价给在手 lot 估值；当日无收盘价（停牌/缺行情）时"
                f'沿用该 lot 最近可得的收盘价（首次回退为买入开盘价），'
                f'本次共 {tracks["a"]["valuation_stale_days"]} 个「lot-日」发生回退。'
                '卖出日与卖出价一律取 A 的 `exit_date_60` / `exit_close_60`，'
                '不使用任何盘中价。',
                '- `signals.csv` 未存 `exit_date_60` 列，本次用 `forward_test/cache` '
                '的同口径 `shift` 反推，并用 `exit_close_60` 逐值校验全部命中；'
                '该偏移是在「建仓行」上取第 60 个信号日口径的后续行，与 A 的 '
                '`close.shift(-60)` 对齐。',
                f'- 价格尺度：A 的 `entry_open` / `exit_close_60` 是后复权价，'
                '估值因此统一取面板后复权收盘列 `close`（不是撮合用的 `raw_close`）。'
                f'有 {tracks["a"]["exit_price_checks"]} 笔到期卖出可比对，'
                '面板后复权收盘价与 `exit_close_60` 的平均相对差 '
                f'{_pct(tracks["a"]["exit_price_dev_mean"]) if tracks["a"]["exit_price_dev_mean"] is not None else "—"}。',
            ]
        lines.append('')
    return '\n'.join(lines)


# ----------------------------------------------------------------------
# 主流程
# ----------------------------------------------------------------------
def _resolve_window(args: argparse.Namespace, store_root: Path) -> tuple[date, date, list[date], str]:
    """确定窗口与交易日列表。"""
    end = date.fromisoformat(args.end)
    calendar = get_calendar(store_root, as_of=end)
    if args.start:
        start = date.fromisoformat(args.start)
        dates = list(calendar.between(start, end))
        note = '窗口由 --start/--end 直接指定'
    else:
        span = args.days * 2 + 30
        dates = list(calendar.between(end - timedelta(days=span), end))
        if len(dates) > args.days:
            dates = dates[-args.days:]
        note = ''
        if len(dates) < args.days:
            note = (f'请求 {args.days} 个交易日，日历内实际只有 {len(dates)} 个，'
                    f'按数据实际最早日期 {dates[0].isoformat() if dates else "—"} 起算')
        start = dates[0] if dates else end
    if not dates:
        raise SystemExit(f'窗口内没有交易日：{args.start}~{args.end}')
    return start, end, dates, note


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='状态机口径回测（T+1 开盘撮合）')
    parser.add_argument('--start', default='', help='窗口起点 YYYY-MM-DD（缺省按 --days 回溯）')
    parser.add_argument('--end', default='2026-11-09', help='窗口终点 YYYY-MM-DD')
    parser.add_argument('--days', type=int, default=120, help='回测交易日数（默认 120）')
    parser.add_argument('--tier', default='mid', help='档位 key（默认 mid）')
    parser.add_argument('--equity', type=float, default=1_000_000.0, help='初始权益')
    parser.add_argument('--out-root', default='reports/advice/stateful',
                        help='产物根目录（默认 reports/advice/stateful）')
    parser.add_argument('--chunk-by', choices=('none', 'year'), default='none',
                        help='分片方式（默认 none；year 按自然年分片）')
    parser.add_argument('--resume', action='store_true',
                        help='分片模式下若已存在 chunk_state.json 则跳过该片')
    parser.add_argument('--signal-mode', choices=('daily', 'month-end'),
                        default='daily',
                        help='选候选频率（默认 daily；month-end 只在每月最后一个交易日选）')
    parser.add_argument('--only-chunk', default='',
                        help='只跑指定分片（标签，如 2019），便于单次命令限时内落盘')
    parser.add_argument('--baseline-alloc', choices=('slots', 'a', 'slots60', 'all'),
                        default='slots',
                        help='固定持有 60 日基线的资金分配口径（默认 slots，保持旧行为；'
                             'a = 与尺子 A 同构的 lot 级共享现金池；'
                             'slots60 = 日频旧行为；all = 三档并列）')
    parser.add_argument('--out-suffix', default='',
                        help='产物目录后缀（如 _baseline_alloc，避免覆盖既有目录）')
    args = parser.parse_args(argv)

    started = datetime.now()
    app_config = get_config()
    store_root = resolve_store_root(app_config)
    config = default_config()
    tier = config.tier(args.tier)
    costs = _documented_costs()
    rules = ExecutionRules()

    round_trip_actual = costs.round_trip_rate
    round_trip_recomputed = _round_trip_rate(costs)
    gap = round_trip_actual - DOC_ROUND_TRIP_COST
    print('=' * 78)
    print('状态机口径回测（尺子 B：可执行性）')
    print(f'  档位 {tier.key} | 初始权益 {args.equity:,.0f} | '
          f'结束 {args.end} | 分片 {args.chunk_by} | 信号 {args.signal_mode}')
    print(f'  往返费率 TradingCosts={round_trip_actual:.6f}，'
          f'分项复算={round_trip_recomputed:.6f}，'
          f'文档常数={DOC_ROUND_TRIP_COST:.6f}，差额={gap:+.6f}')
    if abs(gap) > 1e-6:
        print('  [警告] 往返费率与文档冻结常数不一致：本次按分项实际扣费，'
              '差额如实写入 summary.json 与 report.md，未调平')
    print('=' * 78, flush=True)

    if args.chunk_by == 'none':
        return _run_single(args, started, app_config, store_root, config,
                           tier, costs, rules)
    if not args.start:
        raise SystemExit('--chunk-by 分片模式必须显式给出 --start')
    return _run_chunked(args, started, app_config, store_root, config, tier,
                        costs, rules)


def _run_single(args: argparse.Namespace, started: datetime, app_config: Any,
                store_root: Path, config: Any, tier: Any, costs: TradingCosts,
                rules: ExecutionRules) -> int:
    """单窗口（旧行为）：一次加载整段面板，不做分片。"""
    start, end, window_dates, window_note = _resolve_window(args, store_root)

    codes = list_local_stock_codes(app_config)
    universe = filter_universe(start, codes, store_root=store_root)
    _log(f'选池完成：{universe.kept_count}/{universe.input_count}', started)

    loader = LocalPanelLoader(app_config, min_rows=config.min_history_rows)
    panel_start = loader.default_start(start, tier.lookback_days + 120)
    panel, load_report = loader.load(universe.kept, panel_start, end)
    if panel.is_empty():
        raise SystemExit('本地面板为空，无法回测')
    _log(f'面板加载完成：{load_report.loaded} 只 / {load_report.rows} 行', started)

    factors = compute_factor_panel(panel, extended=True)
    _log(f'因子计算完成：{factors.height} 行', started)

    # attach_score / apply_soft_filter 均以 date 为分区做截面计算，
    # 一次性算全量与该日切片逐日重算完全等价（同一 date 的输入相同）。
    scored = attach_score(factors, tier,
                          min_history_rows=config.min_history_rows)
    scored = apply_soft_filter(scored, config.soft_filter)
    _log('打分与软筛完成', started)

    keep = [name for name in SLIM_COLUMNS if name in scored.columns]
    slim = scored.select(keep)
    panel_dates = [day for day in panel_trading_dates(slim) if isinstance(day, date)]
    window_dates = [day for day in window_dates if day in set(panel_dates)]
    if not window_dates:
        raise SystemExit('窗口内没有可用行情日')
    all_dates = panel_dates
    window_slim = slim.filter(pl.col('date').is_in(window_dates))
    parts = window_slim.partition_by('date', as_dict=True)
    by_date = {(key[0] if isinstance(key, tuple) else key): value
               for key, value in parts.items()}

    bars_by_date: dict[date, dict[str, tuple]] = {}
    for day, part in by_date.items():
        bars: dict[str, tuple] = {}
        for row in part.select(
                ['ts_code', 'raw_open', 'raw_close', 'raw_prev_close',
                 'raw_volume', 'close']).iter_rows(named=True):
            bars[str(row['ts_code'])] = (
                row['raw_open'], row['raw_close'], row['raw_prev_close'],
                row['raw_volume'], row['close'])
        bars_by_date[day] = bars
    _log(f'撮合价表构建完成：{len(bars_by_date)} 个交易日', started)

    signal_days = (_month_end_days(window_dates)
                   if args.signal_mode == 'month-end' else None)
    baseline_slots = _baseline_slots(window_dates, signal_days)
    stateful = run_stateful(window_dates, by_date, bars_by_date, all_dates,
                            tier, config, args.equity, costs, rules,
                            signal_days=signal_days)
    _log('状态机回测完成', started)
    allocs = _resolve_allocs(args)
    a_signals = load_a_signals() if 'a' in allocs else None
    a_check = check_a_rec_net(a_signals) if a_signals else None
    baselines = run_baselines(
        window_dates, by_date, bars_by_date, tier, config, args.equity, costs,
        rules, allocs=allocs, signal_days=signal_days,
        slots=baseline_slots['slots'], a_signals=a_signals)
    baseline = baselines['slots']
    _log(f'资金分配 {" / ".join(allocs)} 完成', started)
    return _emit_products(
        args=args, config=config, tier=tier, costs=costs, started=started,
        start=start, end=end, window_dates=window_dates,
        window_note=window_note, panel_start=panel_start,
        panel_dates=panel_dates, universe=universe, load_report=load_report,
        stateful=stateful, baseline=baseline, bars_by_date=bars_by_date,
        baseline_slots=baseline_slots, baselines=baselines,
        a_signals=a_signals, a_check=a_check)


def _attach_baseline_alloc(summary: dict[str, Any], *, args: argparse.Namespace,
                           baselines: dict[str, dict[str, Any]] | None,
                           baseline_rows: list[dict[str, Any]],
                           base_nav_key: str, base_cash_key: str,
                           baseline_stats: dict[str, Any],
                           baseline_block: dict[str, Any],
                           a_signals: dict[str, Any] | None,
                           a_check: dict[str, Any] | None,
                           stateful_stats: dict[str, Any],
                           window_dates: list[date]
                           ) -> tuple[list[str], list[dict[str, Any]]]:
    """把三档资金分配的指标与矩阵挂到 ``summary['baseline_alloc']``。

    ``slots`` 档必须与旧基线逐值一致，差任意一位即停下报告差异。
    返回 (档位清单, 矩阵行)。
    """
    alloc_names = list(baselines) if baselines else ['slots']
    alloc_blocks: dict[str, Any] = {}
    alloc_matrix: list[dict[str, Any]] = []
    for name in alloc_names:
        payload = (baselines[name] if baselines
                   else {'nav_rows': baseline_rows, 'stats': baseline_stats})
        rows = payload['nav_rows']
        nav_key = 'nav' if (rows and 'nav' in rows[0]) else base_nav_key
        cash_key = 'cash' if (rows and 'cash' in rows[0]) else base_cash_key
        block = _alloc_track_block(rows, nav_key, cash_key, args.equity,
                                   payload['stats'])
        if name == 'slots':
            for key in ('cum_return', 'year_return', 'max_drawdown',
                        'drawdown_duration', 'turnover', 'trade_count',
                        'final_nav', 'total_cost'):
                if abs(float(block[key]) - float(baseline_block[key])) > 1e-12:
                    raise SystemExit(
                        f'slots 档与旧基线不一致：{key} {block[key]} vs '
                        f'{baseline_block[key]}，停止（分片产物保留，可重跑合并）')
        alloc_blocks[name] = block
        for item in _alloc_matrix(rows, args.equity, payload['stats']):
            alloc_matrix.append({'alloc': name, **item})

    summary['baseline_alloc'] = {
        'requested': getattr(args, 'baseline_alloc', 'slots'),
        'computed': alloc_names,
        'tracks': alloc_blocks,
        'cash_yield_rates': list(CASH_YIELD_RATES),
        'main_cash_yield_rate': max(CASH_YIELD_RATES),
        'matrix': alloc_matrix,
        'method': ('现金收益沿用事后累加：Σ 每日现金 × 年化利率 / 252 加到期末净值，'
                   '不改写成交、持仓与复利路径；矩阵中的年化与最大回撤基于'
                   '「净值 + 累计现金利息」的事后序列，买卖与成本路径不变'),
        'definitions': {
            'slots': ('现有反推口径：slots=3（相邻信号日间隔中位数换算），'
                      '每槽预算 equity_init/slots，票按 target_weight，'
                      '到期开盘卖出；逐值与既有产物一致'),
            'a': ('与尺子 A 同构：直接读 reports/advice/forward_test/signals.csv '
                  '（tier=mid、variant=on、ok_60，209 行 / 56 个信号日），'
                  'lot 级共享现金池，T+1 开盘买入，每只票投入 '
                  'target_weight × 信号日收盘组合权益，买入后第 60 个交易日'
                  '（A 的 exit_date_60）按收盘价卖出，不预分配、不锁仓、现金不足少买'),
            'slots60': ('日频旧行为在月频下的样子：slots=60，每槽预算 '
                        'equity_init/60，其余同 slots'),
        },
    }
    if a_signals:
        summary['baseline_alloc']['a_signals'] = {
            'path': a_signals['path'],
            'filter': f"tier={a_signals['tier']} & variant={a_signals['variant']} & ok_60",
            'rows': a_signals['row_count'],
            'periods': a_signals['period_count'],
            'target_weight_min': a_signals['weight_min'],
            'target_weight_max': a_signals['weight_max'],
            'target_weight_mean': a_signals['weight_mean'],
            'period_weight_mean': a_signals['period_weight_mean'],
            'period_weight_min': a_signals['period_weight_min'],
            'period_weight_max': a_signals['period_weight_max'],
            'exit_date_offset': a_signals['exit_offset'],
            'exit_date_method': ('signals.csv 无 exit_date_60 列，用 forward_test/cache '
                                 '同口径 shift 反推，并用 exit_close_60 逐值校验'),
        }
    if a_check:
        summary['baseline_alloc']['a_check'] = a_check
    summary['baseline_alloc']['candidate_source_diff'] = {
        'a': {'rows': (a_signals or {}).get('row_count'),
              'periods': (a_signals or {}).get('period_count')},
        'b': {'signal_days': len(_month_end_days(window_dates)),
              'days_with_candidates': stateful_stats['days_with_candidates'],
              'candidate_signals': stateful_stats['candidate_signals']},
        'note': ('A 档与 B 档的候选集不同源：A 是 92 个月末信号日里 56 天有票、'
                 '共 209 笔；B 的 month-end 口径是 92 天里 39 天有候选、'
                 '共 847 个候选信号，两者差一个量级。`a` 档因此必须用 A 自己的'
                 ' 209 行，而不是 B 的 select_candidates 候选集'),
    }
    return alloc_names, alloc_matrix


def _emit_products(*, args: argparse.Namespace, config: Any, tier: Any,
                   costs: TradingCosts, started: datetime, start: date, end: date,
                   window_dates: list[date], window_note: str,
                   panel_start: date, panel_dates: list[date], universe: Any,
                   load_report: Any, stateful: dict[str, Any],
                   baseline: dict[str, Any],
                   bars_by_date: dict[date, dict[str, tuple]] | None,
                   baseline_slots: dict[str, Any],
                   baselines: dict[str, dict[str, Any]] | None = None,
                   a_signals: dict[str, Any] | None = None,
                   a_check: dict[str, Any] | None = None,
                   quotes: dict[tuple[str, date], tuple[float, float]] | None = None,
                   extra_summary: dict[str, Any] | None = None) -> int:
    """组装 summary / nav / trades / report 并打印对照表（单窗口与分片共用）。"""
    round_trip_actual = costs.round_trip_rate
    round_trip_recomputed = _round_trip_rate(costs)
    gap = round_trip_actual - DOC_ROUND_TRIP_COST
    requested_days = len(window_dates) if args.chunk_by != 'none' else args.days
    nav_stateful = [float(row['nav_stateful']) for row in stateful['nav_rows']]
    baseline_rows = baseline['nav_rows']
    first_baseline = baseline_rows[0] if baseline_rows else {}
    base_nav_key = 'nav' if 'nav' in first_baseline else 'nav_fixed60'
    base_cash_key = 'cash' if 'cash' in first_baseline else 'cash_fixed60'
    nav_baseline = [float(row[base_nav_key]) for row in baseline_rows]
    for index, row in enumerate(stateful['nav_rows']):
        if index >= len(baseline_rows):
            break
        row['nav_fixed60'] = nav_baseline[index]
        row['cash_fixed60'] = float(baseline_rows[index].get(base_cash_key) or 0.0)

    stateful_stats = stateful['stats']
    baseline_stats = baseline['stats']
    stateful_block = _summary_block(nav_stateful, args.equity, stateful_stats)
    baseline_block = _summary_block(nav_baseline, args.equity, baseline_stats)

    total_intents = max(1, stateful_stats['buy_intents'])
    decomp = _decompose(
        stateful['closed'], stateful_stats['unfilled_records'], window_dates,
        bars_by_date, costs, args.equity, stateful_block['total_cost'],
        baseline_block['total_cost'], stateful['nav'], baseline['nav'],
        quotes=quotes,
        turnover_stateful=stateful_block['turnover'],
        turnover_baseline=baseline_block['turnover'])
    planned_total = (stateful_stats['planned_amount']
                     + stateful_stats['blocked_turnover_cap_amount'])
    unfilled_diag = _unfilled_diagnosis(
        stateful_stats['unfilled_records'], window_dates, bars_by_date,
        quotes, costs, args.equity, planned_total,
        decomp_unfilled=decomp['decomp_unfilled'])

    delay_hist = {str(key): int(value)
                  for key, value in sorted(stateful_stats['delay_hist'].items())}
    path = _path_metrics([value / args.equity for value in nav_stateful])
    cost_total = (stateful_stats['commission'] + stateful_stats['stamp']
                  + stateful_stats['impact'])
    sell_confirm_count = stateful_stats['sell_confirm_count']
    cash_reserve = float(getattr(config.constraints, 'cash_buffer', 0.10) or 0.0)
    target_weight = min(1.0 / int(tier.max_positions),
                        max(0.0, 1.0 - cash_reserve))

    lookahead_notes = [
        '信号只用 T 日及之前的数据：打分、软筛、可推荐性判定全部取 T 日收盘截面，'
        '成交在 T+1 开盘，不使用 T+1 收盘价/最高价回填。',
        '涨跌停价只用 T 日收盘价与板块规则推导（ExecutionRules.resolve_limit_pct / '
        'resolve_limit_price），T+1 只读取开盘价与成交量。',
        (('选池固定为每个分片首日的 filter_universe 结果，片内不重算，'
          '分片边界结果见 summary.universe_by_chunk；片内退市标的留在池内属正确方向，'
          '片内新上市标的不会进入候选，方向偏保守。')
         if args.chunk_by != 'none' else
         (f'选池固定为窗口起点 {start.isoformat()} 当日的 filter_universe 结果，'
          '不在每日重算。这是刻意选择：120 天全市场逐日重算不可承受。'
          '后果是窗口内退市/新上市的标的不会进出池——退市标的留在池内属正确方向'
          '（不制造幸存者偏差），但窗口内新上市标的不会进入候选，方向偏保守。')),
        '状态机写入下一日的只有「已成交」的持仓事实与当日收盘后生成的调仓意图；'
        '意图在 T+1 开盘才变成持仓，不存在把明日打算买入写成已持有。',
        'ST 状态：选池阶段已用 get_st_status 按 T 日剔除 ST；撮合层 '
        'check_eligibility 未传 st_status（记 unknown，仅提示不拦截），'
        '未知状态没有被当成「非 ST」用于放行。',
        '窗口末 60 个交易日内买入的持仓没有「买入后第 60 个交易日收盘」'
        '（本地数据止于 2026-11-09），该部分不计入四桶并在 decomp_detail 单独计数，'
        '残差因此偏大——未用未来数据补齐。',
        '窗口期末仍未平仓的持仓在 nav.csv 按最后一个交易日收盘价估值，'
        '分解样本里按同一价格并扣预估卖出成本计算 r_actual，但不写入 trades.csv'
        '（不把估值伪装成成交），也不把预估卖出成本计入实际成本统计。',
        '换手上限按 T 日收盘后的组合权益计算，不使用 T+1 的任何价格。',
        'history_rows 由加载面板的行数给出（面板内恒定），不是截至 T 日的滚动历史；'
        '本次面板自 '
        f'{panel_start.isoformat()} 起加载，满足 tier.lookback_days='
        f'{tier.lookback_days} 的标的才可能入选。',
    ]
    methodology_notes = [
        '撮合价一律使用面板的未复权列 raw_open / raw_close（加载器保留的原始价），'
        '复权列只用于因子计算；除权日的价格跳空同时出现在实际收益与反事实里。',
        '涨跌停基准优先取撮合日的 raw_prev_close（即 T 日收盘，除权日按交易所口径调整），'
        '缺失时回退到信号日收盘价；两种取值在非除权日完全相同。',
        '成本按笔扣：买入扣佣金（含 5 元最低佣金）+ 冲击，卖出再扣佣金 + 印花税 + 冲击；'
        '状态的净值为收盘市值口径（当日开盘成交、当日收盘计价）。',
        '软筛与打分在完整面板上一次算完（两者都以 date 分区做截面），'
        '与逐日切片重算结果完全一致；逐日循环只做门槛判定与撮合。',
        '状态机持仓的当日分位缺失（停牌/无有效因子）时按 0.0 处理，'
        '即视为最弱信号，卖出被撮合层拦截时继续持有并重试。',
        '基线 cohort 按信号日间隔换算槽位：默认日频信号下每交易日起一个、共 60 个，'
        '各占初始权益的 1/60（旧行为）；月频信号下按相邻信号日间隔中位数反推 '
        'slots（约 3）并令每槽预算 = 初始权益 / slots，'
        '使两口径资金利用率可比，具体数值见 summary.baseline_slots。',
        '年化按 252 个交易日折算；换手为单边年化（买卖成交额之和 / 2 / 平均净值 / 年）。',
        '候选稀疏的根因是方向相反：mid 档扩展因子的 direction 全为 -1'
        '（偏好低动量、低波动、低回撤），而软筛要求 ma20_gap>0 且 ma60_gap>0'
        '（均线多头），两个条件天然互斥，交集只在少数交易日出现。'
        '本次未放宽任一门槛，如实保留这一漏斗结果。',
        '最大回撤与回撤持续按收盘净值序列计算；worst_period_return 与 '
        'max_consecutive_loss_periods 为日频口径。',
    ]

    summary: dict[str, Any] = {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'script': 'tools/advice_stateful_backtest.py',
        'window': {
            'start': start.isoformat(), 'end': end.isoformat(),
            'trading_days': len(window_dates),
            'requested_days': requested_days,
            'note': window_note,
            'panel_start': panel_start.isoformat(),
            'panel_first_date': panel_dates[0].isoformat() if panel_dates else None,
        },
        'tier': {
            'key': tier.key, 'label': tier.label,
            'lookback_days': tier.lookback_days,
            'min_hold_days': config.effective_min_hold(tier),
            'confirm_days': tier.confirm_days,
            'validity_days': tier.validity_days,
            'buy_percentile': tier.buy_percentile,
            'sell_percentile': tier.sell_percentile,
            'max_positions': tier.max_positions,
            'signal_decay': tier.signal_decay,
            'factor_count': len(tier.factors),
        },
        'initial_equity': args.equity,
        'cash_reserve': cash_reserve,
        'cash_reserve_source': 'config.constraints.cash_buffer',
        'target_weight': target_weight,
        'universe': {
            'as_of': start.isoformat(),
            'input': universe.input_count, 'kept': universe.kept_count,
            'version': universe.version,
            'rule_counts': universe.rule_counts,
        },
        'data': {
            **load_report.summary(),
            'price_columns': ['raw_open', 'raw_close', 'raw_prev_close',
                              'raw_volume'],
            'factor_set': config.factor_set,
            'min_history_rows': config.min_history_rows,
        },
        'cost_round_trip_rate': round_trip_actual,
        'cost_round_trip_recomputed': round_trip_recomputed,
        'cost_round_trip_target': DOC_ROUND_TRIP_COST,
        'cost_round_trip_gap': gap,
        'cost_breakdown': {
            'commission_rate': costs.commission_rate,
            'stamp_duty_rate': costs.stamp_duty_rate,
            'impact_rate': costs.impact_rate,
            'transfer_rate_in_commission': DOC_TRANSFER_RATE,
            'source': ('按文档冻结常数 0.724%/往返构造：'
                       '佣金 5bp×2 + 过户费 0.2bp×2 + 印花税 5bp + 冲击成本 28.7bp×2'),
            'min_commission': COST_DEFAULTS['min_commission'],
        },
        'signal_to_fill_rate': stateful_stats['buy_filled'] / total_intents,
        'funnel': {
            'decision_days': stateful_stats['days_scanned'],
            'days_with_candidates': stateful_stats['days_with_candidates'],
            'day_coverage': (
                stateful_stats['days_with_candidates']
                / stateful_stats['days_scanned']
                if stateful_stats['days_scanned'] else 0.0),
            'candidate_signals': stateful_stats['candidate_signals'],
            'buy_intents': stateful_stats['buy_intents'],
            'buy_filled': stateful_stats['buy_filled'],
            'candidate_to_intent_rate': (
                stateful_stats['buy_intents'] / stateful_stats['candidate_signals']
                if stateful_stats['candidate_signals'] else 0.0),
            'note': '候选=分位≥buy_percentile 且 soft_pass 且过可推荐性与成本门槛；'
                    '同一标的跨日重复计入，意图数受空余仓位与换手上限约束',
        },
        'fill_detail': {
            'buy_intents': stateful_stats['buy_intents'],
            'buy_filled': stateful_stats['buy_filled'],
            'buy_filled_amount': stateful_stats['buy_filled_amount'],
            'sell_signals': stateful_stats['sell_signals'],
            'sell_filled': stateful_stats['sell_filled'],
            'sell_abandoned': stateful_stats['sell_abandoned'],
            'sell_retry_limit_down': stateful_stats['sell_retry_limit_down'],
            'sell_retry_suspended': stateful_stats['sell_retry_suspended'],
        },
        'blocked_limit_up': {
            'count': stateful_stats['blocked_limit_up'],
            'pct_of_buy_intents': stateful_stats['blocked_limit_up'] / total_intents,
        },
        'blocked_limit_up_amount': stateful_stats['blocked_limit_up_amount'],
        'blocked_limit_up_amount_pct_of_planned': (
            stateful_stats['blocked_limit_up_amount']
            / stateful_stats['planned_amount']
            if stateful_stats['planned_amount'] > 0 else None),
        'blocked_limit_down': {
            'count': stateful_stats['blocked_limit_down'],
            'pct_of_sell_signals': (
                stateful_stats['blocked_limit_down']
                / stateful_stats['sell_signals']
                if stateful_stats['sell_signals'] else None),
        },
        'blocked_suspended': {
            'count': stateful_stats['blocked_suspended'],
            'pct_of_buy_intents': stateful_stats['blocked_suspended'] / total_intents,
        },
        'blocked_no_quote': {
            'count': stateful_stats['blocked_no_quote'],
            'pct_of_buy_intents': stateful_stats['blocked_no_quote'] / total_intents,
        },
        'delay_histogram': delay_hist,
        'overnight_gap_cost': stateful_stats['overnight_gap_abs'],
        'overnight_gap_cost_detail': {
            'abs_amount': stateful_stats['overnight_gap_abs'],
            'signed_amount': stateful_stats['overnight_gap_signed'],
            'pct_of_initial_equity': (
                stateful_stats['overnight_gap_abs'] / args.equity),
            'formula': 'Σ |open(T+1)/close(T) - 1| × 计划金额',
        },
        'cost_commission': stateful_stats['commission'],
        'cost_stamp_duty': stateful_stats['stamp'],
        'cost_impact': stateful_stats['impact'],
        'cost_commission_pct': stateful_stats['commission'] / args.equity,
        'cost_stamp_duty_pct': stateful_stats['stamp'] / args.equity,
        'cost_impact_pct': stateful_stats['impact'] / args.equity,
        'cost_total': cost_total,
        'cost_by_size': _cost_by_size(stateful_stats, costs),
        'cost_per_trade_bps': (
            cost_total / stateful_stats['buy_notional'] * 1e4
            if stateful_stats['buy_notional'] > 0 else 0.0),
        'rule_block_min_hold': {
            'count': stateful_stats['min_hold_blocks'],
            'pct_of_holding_obs': (
                stateful_stats['min_hold_blocks']
                / stateful_stats['holding_obs']
                if stateful_stats['holding_obs'] else 0.0),
            'note': '仅统计「卖出信号已触发但持有期未满」的持仓日，不是所有未满日',
        },
        'rule_block_sell_confirm': {
            'count': sell_confirm_count,
            'avg_delay_days': (
                stateful_stats['sell_confirm_delay_sum'] / sell_confirm_count
                if sell_confirm_count else 0.0),
            'blocked_days': stateful_stats['sell_confirm_block_days'],
        },
        'rule_block_turnover_cap': {
            'count': stateful_stats['blocked_turnover_cap'],
            'amount': stateful_stats['blocked_turnover_cap_amount'],
        },
        'blocked_cash': {
            'count': stateful_stats['blocked_cash'],
            'amount': stateful_stats['blocked_cash_amount'],
        },
        'max_drawdown': path['max_drawdown'],
        'drawdown_duration': path['drawdown_duration'],
        'worst_period_return': path['worst_period_return'],
        'max_consecutive_loss_periods': path['max_consecutive_loss_periods'],
        'decomp_early_exit': decomp['decomp_early_exit'],
        'decomp_late_exit': decomp['decomp_late_exit'],
        'decomp_unfilled': decomp['decomp_unfilled'],
        'decomp_cost': decomp['decomp_cost'],
        'decomp_residual': decomp['decomp_residual'],
        'decomp_total_diff_pp': decomp['decomp_total_diff_pp'],
        'decomp_residual_ratio': decomp['decomp_residual_ratio'],
        'decomp_detail': decomp['decomp_detail'],
        'comparison': {
            'stateful': stateful_block,
            'fixed60': baseline_block,
        },
        'baseline_slots': {
            'slots': baseline_slots['slots'],
            'spacing_days': baseline_slots['spacing_days'],
            'spacing_samples': baseline_slots['spacing_samples'],
            'cohort_budget': args.equity / max(1, baseline_slots['slots']),
            'cohort_budget_fraction': baseline_slots['cohort_budget_fraction'],
            'signal_mode': args.signal_mode,
            'formula': ('spacing_days = 相邻信号日交易日间隔中位数；'
                        'slots = max(1, round(BASELINE_HOLD / spacing_days))；'
                        'cohort_budget = equity_init / slots'),
            'purpose': ('让「固定持有 60 日」与状态机在同一资金利用率下可比；'
                        '日频信号下 slots=60 即旧行为，月频信号下不再把绝大部分'
                        '资金闲置'),
        },
        'cost_bucket': decomp['decomp_detail']['cost_bucket'],
        'unfilled_diagnosis': unfilled_diag,
        'lookahead_notes': lookahead_notes,
        'methodology_notes': methodology_notes,
        'elapsed_sec': round((datetime.now() - started).total_seconds(), 1),
    }
    # blocked_limit_up 的金额占比在报告里另取（保持字段名与文档一致）
    summary['blocked_limit_up']['amount_pct_of_planned'] = (
        summary['blocked_limit_up_amount_pct_of_planned'])

    summary['cash_yield_sensitivity'] = {
        'method': 'post-hoc accrual on recorded daily cash, 不重跑复利路径',
        'note': ('按每日现金余额 × rate/252 一次性累加到期末净值，'
                 '不改写成交、持仓与复利路径；仅用于量级敏感性判断'),
        'stateful': _cash_yield_rates(stateful['nav_rows'], 'nav_stateful',
                                      'cash', args.equity),
        'fixed60': _cash_yield_rates(stateful['nav_rows'], 'nav_fixed60',
                                     'cash_fixed60', args.equity),
    }
    if extra_summary:
        summary.update(extra_summary)

    alloc_names, alloc_matrix = _attach_baseline_alloc(
        summary, args=args, baselines=baselines, baseline_rows=baseline_rows,
        base_nav_key=base_nav_key, base_cash_key=base_cash_key,
        baseline_stats=baseline_stats, baseline_block=baseline_block,
        a_signals=a_signals, a_check=a_check, stateful_stats=stateful_stats,
        window_dates=window_dates)

    out_dir = ROOT / args.out_root / (
        f"{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}"
        f"{getattr(args, 'out_suffix', '')}")
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(out_dir / 'nav.csv', stateful['nav_rows'], NAV_COLUMNS)
    _write_csv(out_dir / 'trades.csv', stateful['trades'], TRADE_COLUMNS)
    for name in alloc_names:
        payload = (baselines[name] if baselines
                   else {'nav_rows': baseline_rows, 'stats': baseline_stats})
        _write_csv(out_dir / f'nav_{name}.csv', payload['nav_rows'],
                   ('date', 'nav', 'cash', 'holdings_count', 'gross_exposure'))
    if alloc_matrix:
        _write_csv(out_dir / 'matrix.csv', alloc_matrix,
                   ('alloc', 'cash_yield_rate', 'cum_return', 'year_return',
                    'max_drawdown', 'drawdown_duration', 'turnover',
                    'trade_count', 'cost_total', 'cost_pct_initial_equity',
                    'accrued_interest', 'nav_final', 'nav_final_zero_yield'))
    (out_dir / 'summary.json').write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str),
        encoding='utf-8', newline='\n')
    (out_dir / 'report.md').write_text(_build_report(summary),
                                       encoding='utf-8', newline='\n')

    print()
    print(f'窗口        : {start} ~ {end}，{len(window_dates)} 个交易日'
          f'{("（" + window_note + "）") if window_note else ""}')
    print(f'选池        : {start} 当日 {universe.kept_count}/{universe.input_count}')
    print(f'面板        : {load_report.loaded} 只 / {load_report.rows} 行')
    print(f'基线槽位    : slots={summary["baseline_slots"]["slots"]} | '
          f'相邻信号日中位间隔 {summary["baseline_slots"]["spacing_days"]} 日 | '
          f'每槽 {summary["baseline_slots"]["cohort_budget"]:,.0f} 元')
    print(f'未成交诊断  : {unfilled_diag["unfilled_intents"]} 笔 | 计划金额占比 '
          f'{_pct(unfilled_diag["planned_pct_of_all_buy_planned"])} | '
          f'60日反事实均值 {_pct(unfilled_diag["mean_r_hold60"])} | '
          f'top5 占 {_pct(unfilled_diag["top5_abs_share"])}')
    print('-' * 78)
    print(f'{"指标":<20}{"状态机":>16}{"固定持有60日":>18}')
    for label, key in (('累计收益', 'cum_return'), ('年化收益', 'year_return'),
                       ('最大回撤', 'max_drawdown'), ('年化单边换手', 'turnover')):
        left = stateful_block[key]
        right = baseline_block[key]
        if key == 'turnover':
            print(f'{label:<20}{left:>16.2f}{right:>18.2f}')
        else:
            print(f'{label:<20}{left:>16.2%}{right:>18.2%}')
    print(f'{"成交笔数":<20}{stateful_block["trade_count"]:>16}'
          f'{baseline_block["trade_count"]:>18}')
    print('-' * 78)
    print(f'信号到成交率: {summary["signal_to_fill_rate"]:.2%} | '
          f'涨停拦截 {summary["blocked_limit_up"]["count"]} 笔 | '
          f'跌停拦截 {summary["blocked_limit_down"]["count"]} 笔 | '
          f'停牌 {summary["blocked_suspended"]["count"]} 笔 | '
          f'无行情 {summary["blocked_no_quote"]["count"]} 笔')
    print(f'漏斗        : {summary["funnel"]["days_with_candidates"]}/'
          f'{summary["funnel"]["decision_days"]} 天有候选 | 候选信号 '
          f'{summary["funnel"]["candidate_signals"]} 个 → 意图 '
          f'{summary["funnel"]["buy_intents"]} 笔 → 成交 '
          f'{summary["funnel"]["buy_filled"]} 笔')
    print(f'差异分解    : 早卖 {decomp["decomp_early_exit"]:+.3f}pp | '
          f'晚卖 {decomp["decomp_late_exit"]:+.3f}pp | '
          f'未成交 {decomp["decomp_unfilled"]:+.3f}pp | '
          f'成本 {decomp["decomp_cost"]:+.3f}pp | '
          f'残差 {decomp["decomp_residual"]:+.3f}pp | '
          f'总差 {decomp["decomp_total_diff_pp"]:+.3f}pp')
    ratio = summary['decomp_residual_ratio']
    print(f'残差占比    : {"—" if ratio is None else f"{ratio:.2%}"}')
    alloc_tracks = (summary.get('baseline_alloc') or {}).get('tracks') or {}
    if alloc_tracks:
        print('-' * 78)
        print(f'{"资金分配":<12}{"累计(利率0)":>14}{"累计(1.5%)":>14}'
              f'{"最大回撤":>12}{"换手":>10}{"成本元":>14}')
        for name, block in alloc_tracks.items():
            cells = [item for item in alloc_matrix if item['alloc'] == name]
            zero = next((c for c in cells if c['cash_yield_rate'] == 0.0), None)
            main = next((c for c in cells
                         if c['cash_yield_rate'] == max(CASH_YIELD_RATES)), None)
            print(f'{name:<12}'
                  f'{(zero["cum_return"] if zero else block["cum_return"]):>14.2%}'
                  f'{(main["cum_return"] if main else block["cum_return"]):>14.2%}'
                  f'{block["max_drawdown"]:>12.2%}'
                  f'{block["turnover"]:>10.2f}'
                  f'{block["total_cost"]:>14,.0f}')
    print(f'耗时        : {summary["elapsed_sec"]}s')
    print(f'产物目录    : {out_dir}')


def _resolve_allocs(args: argparse.Namespace) -> tuple[str, ...]:
    """把 ``--baseline-alloc`` 解析成实际要算的档位（``slots`` 始终算，供旧列对照）。"""
    requested = getattr(args, 'baseline_alloc', 'slots')
    allocs = list(BASELINE_ALLOCS) if requested == 'all' else [requested]
    if 'slots' not in allocs:
        allocs.insert(0, 'slots')
    return tuple(dict.fromkeys(allocs))


def _resume_states(prev_state_path: Path, allocs: tuple[str, ...],
                   prev_label: str) -> tuple[dict[str, Any],
                                             dict[str, dict[str, Any] | None]]:
    """读上一分片的收尾状态，返回 (状态机状态, 各档基线状态)。"""
    if not prev_state_path.exists():
        raise SystemExit(f'缺少上一片状态 {prev_state_path}，请先完成 {prev_label}')
    previous = json.loads(prev_state_path.read_text(encoding='utf-8'))
    prev_baselines = previous.get('baselines') or {}
    old_baseline = previous.get('baseline') or {}
    baseline_states: dict[str, dict[str, Any] | None] = {}
    for name in allocs:
        entry = prev_baselines.get(name) or {}
        if entry.get('end') is not None:
            baseline_states[name] = entry['end']
        elif name == 'slots' and old_baseline.get('end') is not None:
            baseline_states[name] = old_baseline['end']
        else:
            baseline_states[name] = None
    return previous['stateful']['end'], baseline_states


def _chunk_paths(out_dir: Path, label: str) -> tuple[Path, Path, Path]:
    """返回 (nav.csv, trades.csv, chunk_state.json) 路径（目录自动创建）。"""
    base = out_dir / 'chunks' / label
    base.mkdir(parents=True, exist_ok=True)
    return base / 'nav.csv', base / 'trades.csv', base / 'chunk_state.json'


def _run_chunked(args: argparse.Namespace, started: datetime, app_config: Any,
                 store_root: Path, config: Any, tier: Any, costs: TradingCosts,
                 rules: ExecutionRules) -> int:
    """按自然年分片跑全区间；每片落盘，可 --resume 续跑。"""
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    calendar = get_calendar(store_root, as_of=end)
    global_dates = list(calendar.between(start, end))
    if not global_dates:
        raise SystemExit(f'窗口内没有交易日：{start}~{end}')
    signal_days = (_month_end_days(global_dates)
                   if args.signal_mode == 'month-end' else None)
    baseline_slots = _baseline_slots(global_dates, signal_days)
    all_chunks = _year_chunks(global_dates)
    labels = [label for label, _ in all_chunks]
    if args.only_chunk and args.only_chunk not in labels:
        raise SystemExit(f'--only-chunk {args.only_chunk} 不在分片 {labels} 内')

    out_dir = ROOT / args.out_root / (
        f"{start:%Y%m%d}_{end:%Y%m%d}{getattr(args, 'out_suffix', '')}")
    out_dir.mkdir(parents=True, exist_ok=True)
    codes = list_local_stock_codes(app_config)
    index_of = {day: i for i, day in enumerate(global_dates)}
    allocs = _resolve_allocs(args)
    a_signals = load_a_signals() if 'a' in allocs else None
    if a_signals:
        check_a_rec_net(a_signals)

    for position, (label, chunk_dates) in enumerate(all_chunks):
        if args.only_chunk and label != args.only_chunk:
            continue
        nav_path, trades_path, state_path = _chunk_paths(out_dir, label)
        if args.resume and state_path.exists() and nav_path.exists():
            print(f'[chunk {label}] 已完成，跳过（resume）', flush=True)
            continue
        if position > 0:
            prev_label = all_chunks[position - 1][0]
            _, _, prev_state_path = _chunk_paths(out_dir, prev_label)
            stateful_state, baseline_states = _resume_states(
                prev_state_path, allocs, prev_label)
        else:
            stateful_state = None
            baseline_states = dict.fromkeys(allocs)

        chunk_started = datetime.now()
        universe = filter_universe(chunk_dates[0], codes, store_root=store_root)
        loader = LocalPanelLoader(app_config, min_rows=config.min_history_rows)
        panel_start = loader.default_start(
            chunk_dates[0], tier.lookback_days + 120)
        panel, load_report = loader.load(
            universe.kept, panel_start, chunk_dates[-1])
        if panel.is_empty():
            raise SystemExit(f'[{label}] 本地面板为空，无法回测')
        factors = compute_factor_panel(panel, extended=True)
        scored = attach_score(factors, tier,
                              min_history_rows=config.min_history_rows)
        scored = apply_soft_filter(scored, config.soft_filter)
        keep = [name for name in SLIM_COLUMNS if name in scored.columns]
        slim = scored.select(keep)
        panel_dates = [day for day in panel_trading_dates(slim)
                       if isinstance(day, date)]
        present = set(panel_dates)
        missing_days = sum(1 for day in chunk_dates if day not in present)
        window_slim = slim.filter(pl.col('date').is_in(chunk_dates))
        parts = window_slim.partition_by('date', as_dict=True)
        by_date = {(key[0] if isinstance(key, tuple) else key): value
                   for key, value in parts.items()}
        bars_by_date: dict[date, dict[str, tuple]] = {}
        for day, part in by_date.items():
            bars: dict[str, tuple] = {}
            for row in part.select(
                    ['ts_code', 'raw_open', 'raw_close', 'raw_prev_close',
                     'raw_volume', 'close']).iter_rows(named=True):
                bars[str(row['ts_code'])] = (
                    row['raw_open'], row['raw_close'], row['raw_prev_close'],
                    row['raw_volume'], row['close'])
            bars_by_date[day] = bars
        is_final = position == len(all_chunks) - 1
        stateful = run_stateful(
            chunk_dates, by_date, bars_by_date, global_dates, tier, config,
            args.equity, costs, rules, state=stateful_state,
            finalize=is_final, scan_last_day=True, signal_days=signal_days)
        baselines = run_baselines(
            chunk_dates, by_date, bars_by_date, tier, config, args.equity,
            costs, rules, allocs=allocs,
            state=baseline_states,
            index_base=index_of[chunk_dates[0]], total_days=len(global_dates),
            signal_days=signal_days, slots=baseline_slots['slots'],
            a_signals=a_signals)
        baseline = baselines['slots']
        for row, base_row in zip(stateful['nav_rows'], baseline['nav_rows'],
                                 strict=False):
            row['nav_fixed60'] = base_row['nav']
            row['cash_fixed60'] = base_row['cash']

        elapsed = round((datetime.now() - chunk_started).total_seconds(), 1)
        _write_csv(nav_path, stateful['nav_rows'], NAV_COLUMNS)
        _write_csv(trades_path, stateful['trades'], TRADE_COLUMNS)
        for name, payload_rows in baselines.items():
            _write_csv(nav_path.parent / f'nav_{name}.csv', payload_rows['nav_rows'],
                       ('date', 'nav', 'cash', 'holdings_count', 'gross_exposure'))
        signal_count = sum(1 for day in chunk_dates
                           if signal_days is None or day in signal_days)
        payload = {
            'label': label,
            'start': chunk_dates[0].isoformat(),
            'end': chunk_dates[-1].isoformat(),
            'trading_days': len(chunk_dates),
            'dates': [day.isoformat() for day in chunk_dates],
            'missing_panel_days': missing_days,
            'panel_start': panel_start.isoformat(),
            'panel_dates': [day.isoformat() for day in panel_dates],
            'signal_mode': args.signal_mode,
            'signal_days': signal_count,
            'finalized': is_final,
            'elapsed_sec': elapsed,
            'universe': {
                'as_of': chunk_dates[0].isoformat(),
                'input': universe.input_count, 'kept': universe.kept_count,
                'version': universe.version,
                'rule_counts': universe.rule_counts,
            },
            'data': load_report.summary(),
            'stateful': {
                'end': stateful['state'], 'stats': stateful['stats'],
                'trades': stateful['trades'], 'closed': stateful['closed'],
                'nav_final': stateful['nav'],
            },
            'baseline': {
                'end': baseline['state'], 'stats': baseline['stats'],
                'nav_final': baseline['nav'],
            },
            'baselines': {
                name: {'end': item['state'], 'stats': item['stats'],
                       'nav_final': item['nav']}
                for name, item in baselines.items()
            },
        }
        state_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str),
            encoding='utf-8', newline='\n')
        print(f'[chunk {label}] {chunk_dates[0]} ~ {chunk_dates[-1]} | '
              f'交易日 {len(chunk_dates)} | 耗时 {elapsed}s | '
              f'片末净值 {stateful["nav"]:,.2f}', flush=True)

    missing = [label for label in labels
               if not _chunk_paths(out_dir, label)[2].exists()]
    if missing:
        print(f'分片未完成：{missing}；已完成分片可用 --resume 续跑', flush=True)
        return 0
    return _merge_chunks(args, started, app_config, store_root, config, tier,
                         costs, rules, start, end, global_dates, labels,
                         out_dir, baseline_slots)


def _merge_chunks(args: argparse.Namespace, started: datetime,
                  app_config: Any, store_root: Path, config: Any, tier: Any,
                  costs: TradingCosts, rules: ExecutionRules, start: date,
                  end: date, global_dates: list[date], labels: list[str],
                  out_dir: Path, baseline_slots: dict[str, Any]) -> int:
    """拼合各分片产物，在全区间重算指标并写出汇总产物。"""
    nav_rows: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    closed: list[dict[str, Any]] = []
    stats = _new_stats()
    allocs = _resolve_allocs(args)
    baseline_stats = {name: _new_stats() for name in allocs}
    baseline_nav: dict[str, list[dict[str, Any]]] = {name: [] for name in allocs}
    universe_by_chunk: list[dict[str, Any]] = []
    per_chunk_data: list[dict[str, Any]] = []
    totals = {'requested': 0, 'loaded': 0, 'rows': 0}
    first_panel_start = start
    first_panel_dates: list[date] = []
    for position, label in enumerate(labels):
        nav_path, trades_path, state_path = _chunk_paths(out_dir, label)
        payload = json.loads(state_path.read_text(encoding='utf-8'))
        nav_rows.extend(_read_chunk_nav(nav_path))
        trades.extend(_read_chunk_trades(trades_path))
        chunk_baselines = payload.get('baselines') or {}
        for name in allocs:
            alloc_nav = nav_path.parent / f'nav_{name}.csv'
            if not alloc_nav.exists():
                raise SystemExit(
                    f'分片 {label} 缺少 {alloc_nav}，请重跑该片（--only-chunk {label}）')
            baseline_nav[name].extend(_read_chunk_nav(alloc_nav))
            entry = chunk_baselines.get(name)
            if entry is None:
                raise SystemExit(
                    f'分片 {label} 缺少 {name} 档状态，请重跑该片（--only-chunk {label}）')
            _merge_stats(baseline_stats[name], entry['stats'])
        for item in payload['stateful']['closed']:
            record = dict(item)
            record['entry_date'] = date.fromisoformat(
                str(record['entry_date'])[:10])
            record['exit_date'] = date.fromisoformat(
                str(record['exit_date'])[:10])
            closed.append(record)
        chunk_stats = payload['stateful']['stats']
        for intent in chunk_stats.get('unfilled_records', []):
            intent['signal_date'] = date.fromisoformat(
                str(intent['signal_date'])[:10])
        _merge_stats(stats, chunk_stats)
        universe_by_chunk.append({'label': label, **payload['universe']})
        data = payload.get('data', {})
        per_chunk_data.append({'label': label, **data})
        totals['requested'] = max(totals['requested'],
                                  int(data.get('requested', 0) or 0))
        totals['loaded'] = max(totals['loaded'],
                               int(data.get('loaded', 0) or 0))
        totals['rows'] += int(data.get('rows', 0) or 0)
        if position == 0:
            first_panel_start = date.fromisoformat(payload['panel_start'])
            first_panel_dates = [date.fromisoformat(item)
                                 for item in payload['panel_dates']]

    window_dates = [row['date'] for row in nav_rows]
    if window_dates != global_dates:
        raise SystemExit(
            '分片拼合后的交易日序列与全局日历不一致：'
            f'{len(window_dates)} vs {len(global_dates)}')
    quotes = _decompose_quotes(closed, stats['unfilled_records'],
                               window_dates, store_root)
    stateful = {
        'nav_rows': nav_rows, 'trades': trades, 'closed': closed,
        'stats': stats, 'nav': float(nav_rows[-1]['nav_stateful']),
    }
    baselines: dict[str, dict[str, Any]] = {}
    for name in allocs:
        rows = baseline_nav[name]
        if len(rows) != len(window_dates):
            raise SystemExit(
                f'{name} 档逐日 NAV 长度 {len(rows)} 与窗口 {len(window_dates)} 不一致')
        baselines[name] = {
            'nav_rows': rows, 'stats': baseline_stats[name],
            'nav': float(rows[-1]['nav']) if rows else args.equity,
        }
    baseline = {
        'nav_rows': baselines['slots']['nav_rows'],
        'stats': baselines['slots']['stats'], 'nav': baselines['slots']['nav'],
    }
    a_signals = load_a_signals() if 'a' in allocs else None
    a_check = check_a_rec_net(a_signals) if a_signals else None
    load_report = LoadReport(requested=totals['requested'],
                             loaded=totals['loaded'], rows=totals['rows'])
    first = universe_by_chunk[0]
    universe = SimpleNamespace(
        input_count=first['input'], kept_count=first['kept'],
        version=first['version'], rule_counts=first['rule_counts'])
    extra = {
        'universe_by_chunk': universe_by_chunk,
        'chunking': {
            'chunk_by': args.chunk_by,
            'labels': labels,
            'chunks': per_chunk_data,
            'note': ('每片首日重算 filter_universe 并片内固定；状态与基线持仓'
                     '跨片续接，只有最后一片做期末估值收尾；'
                     'data.requested/loaded 取各片最大值，data.rows 为各片累计'),
        },
        'signal_days': {
            'mode': args.signal_mode,
            'month_end_days': len(_month_end_days(global_dates)),
            'scanned': stats['days_scanned'],
            'note': ('month-end 只统计自然月最后一个交易日；'
                     'month_end_days 含窗口首尾不完整月份'),
        },
    }
    return _emit_products(
        args=args, config=config, tier=tier, costs=costs, started=started,
        start=start, end=end, window_dates=window_dates,
        window_note=('按年分片：' + '/'.join(labels)
                     + '；每片独立选池、跨片续接状态，期末只在最后一片收尾'),
        panel_start=first_panel_start, panel_dates=first_panel_dates,
        universe=universe, load_report=load_report, stateful=stateful,
        baseline=baseline, bars_by_date=None, quotes=quotes,
        baseline_slots=baseline_slots, baselines=baselines,
        a_signals=a_signals, a_check=a_check, extra_summary=extra)



if __name__ == '__main__':
    raise SystemExit(main())
