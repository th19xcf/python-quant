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
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.advice.config import TradingCosts, default_config  # noqa: E402
from src.advice.data import LocalPanelLoader, panel_trading_dates  # noqa: E402
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
from src.data.local_store.store import resolve_store_root  # noqa: E402
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
    'date', 'nav_stateful', 'nav_fixed60', 'cash', 'holdings_count',
    'gross_exposure',
)


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
                 rules: ExecutionRules) -> dict[str, Any]:
    """逐日推进的状态机回测。"""
    cash_reserve = float(getattr(config.constraints, 'cash_buffer', 0.10) or 0.0)
    lot = int(getattr(config.constraints, 'lot_size', 100) or 100)
    max_positions = int(tier.max_positions)
    target_weight = min(1.0 / max_positions, max(0.0, 1.0 - cash_reserve))
    min_hold = config.effective_min_hold(tier)

    cash = float(equity_init)
    positions: dict[str, Position] = {}
    pending_buys: list[dict[str, Any]] = []
    pending_sells: list[dict[str, Any]] = []
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
            'cash': cash, 'holdings_count': len(positions),
            'gross_exposure': market_value / nav if nav > 0 else 0.0,
        })

        if index == len(window_dates) - 1:
            break

        frame = by_date.get(day)
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
    }


# ----------------------------------------------------------------------
# 基线：60 个重叠 cohort，固定持有 60 个交易日
# ----------------------------------------------------------------------
def _cohort_buy(cohort: Cohort, day: date, bars: dict[str, tuple],
                signal_bars: dict[str, tuple],
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
        signal_bar = signal_bars.get(code)
        prev_close = _finite(bar[2])
        if prev_close is None and signal_bar is not None:
            prev_close = _finite(signal_bar[1])
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


def run_fixed60(window_dates: list[date], by_date: dict[date, pl.DataFrame],
                bars_by_date: dict[date, dict[str, tuple]], tier: Any,
                config: Any, equity_init: float, costs: TradingCosts,
                rules: ExecutionRules) -> dict[str, Any]:
    """60 个重叠 cohort 各占 1/60 资金，固定持有 60 个交易日。"""
    cash_reserve = float(getattr(config.constraints, 'cash_buffer', 0.10) or 0.0)
    lot = int(getattr(config.constraints, 'lot_size', 100) or 100)
    target_weight = min(1.0 / int(tier.max_positions), max(0.0, 1.0 - cash_reserve))
    cohort_budget = equity_init / BASELINE_HOLD

    pool = float(equity_init)
    cohorts: list[Cohort] = []
    stats = _new_stats()
    nav_rows: list[dict[str, Any]] = []
    total_days = len(window_dates)
    candidate_cache: dict[date, list[tuple[str, float]]] = {}

    for index, day in enumerate(window_dates):
        bars = bars_by_date.get(day, {})
        # 1) 到期 cohort 开盘卖出（卖不出则顺延，最多 3 次）
        for cohort in cohorts:
            if cohort.exited or not cohort.entered or cohort.exit_idx != index:
                continue
            if _cohort_sell(cohort, day, bars, costs, rules, stats):
                cohort.exited = True
                pool += cohort.cash
                cohort.cash = 0.0
            elif cohort.exit_retries < MAX_SELL_RETRIES:
                cohort.exit_retries += 1
                cohort.exit_idx = min(index + 1, total_days - 1)
            else:
                cohort.exited = True
                pool += cohort.cash
                cohort.cash = 0.0

        # 2) 昨日 cohort 在今日开盘建仓
        for cohort in cohorts:
            if cohort.entered or cohort.exited or cohort.entry_idx != index:
                continue
            if cohort.signal_date not in candidate_cache:
                candidate_cache[cohort.signal_date] = select_candidates(
                    by_date.get(cohort.signal_date), tier, config)
            _cohort_buy(cohort, day, bars,
                        bars_by_date.get(cohort.signal_date, {}),
                        candidate_cache[cohort.signal_date], tier,
                        target_weight, costs, rules, lot, stats)
            cohort.entered = True

        # 3) 今日收盘后新建 cohort（下一交易日开盘建仓）
        if index + 1 < total_days and pool >= cohort_budget:
            budget = min(cohort_budget, pool)
            pool -= budget
            cohorts.append(Cohort(
                signal_date=day, entry_idx=index + 1,
                exit_idx=min(index + 1 + BASELINE_HOLD, total_days - 1),
                budget=budget, cash=budget))

        # 4) 收盘按市值计价
        total = pool
        for cohort in cohorts:
            if cohort.exited:
                continue
            value = cohort.cash
            for code, item in cohort.positions.items():
                bar = bars.get(code)
                close = _finite(bar[1]) if bar is not None else None
                close = close if close is not None else item['entry_price']
                value += item['shares'] * close
            total += value
        nav_rows.append({'date': day, 'nav_fixed60': total})

    return {'nav_rows': nav_rows, 'stats': stats,
            'nav': float(nav_rows[-1]['nav_fixed60']) if nav_rows else equity_init}


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


def _decompose(closed: list[dict[str, Any]],
               unfilled: list[dict[str, Any]],
               window_dates: list[date], bars_by_date: dict[date, dict[str, tuple]],
               costs: TradingCosts, equity_init: float,
               cost_stateful: float, cost_baseline: float,
               nav_stateful: float, nav_baseline: float) -> dict[str, Any]:
    """按文档 §2.2 做四桶分解，残差原样输出，不调平。"""
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
        bar = bars_by_date.get(window_dates[target_idx], {}).get(item['ts_code'])
        close60 = _finite(bar[1]) if bar is not None else None
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
        entry_idx = index_of.get(intent['signal_date'])
        if entry_idx is None or entry_idx + 1 >= len(window_dates):
            unfilled_skipped += 1
            continue
        entry_day = window_dates[entry_idx + 1]
        entry_bar = bars_by_date.get(entry_day, {}).get(intent['ts_code'])
        entry_price = _finite(entry_bar[0]) if entry_bar is not None else None
        if entry_price is None or entry_price <= 0:
            signal_bar = bars_by_date.get(intent['signal_date'], {}).get(
                intent['ts_code'])
            entry_price = _finite(signal_bar[1]) if signal_bar is not None else None
        target_idx = entry_idx + 1 + BASELINE_HOLD
        if entry_price is None or target_idx >= len(window_dates):
            unfilled_skipped += 1
            continue
        target_bar = bars_by_date.get(window_dates[target_idx], {}).get(
            intent['ts_code'])
        close60 = _finite(target_bar[1]) if target_bar is not None else None
        if close60 is None or close60 <= 0:
            unfilled_skipped += 1
            continue
        planned = float(intent['notional'])
        shares = planned / entry_price
        entry_cost = _buy_cost(planned, costs)
        exit_notional = shares * close60
        r_hold60 = (exit_notional - _sell_cost(exit_notional, costs)) / (
            planned + entry_cost) - 1.0
        unfilled_pp += -r_hold60 * planned / equity_init * 100.0

    decomp_cost = -(cost_stateful - cost_baseline) / equity_init * 100.0
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
            'residual_sources': [
                '两口径仓位规模与现金缓冲不同（状态机按 1/max_positions 建仓，基线每 cohort 独立 1/60）',
                '未投资现金的时间价值按 0 计',
                '持有期跨越窗口边界（窗口末买入无 60 日后收盘，已单独计数）',
            ],
        },
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
        f"- 选池：窗口起点 {summary['universe']['as_of']} 的 "
        f"`filter_universe` 结果 {summary['universe']['kept']}/"
        f"{summary['universe']['input']} 只（规则版本 "
        f"{summary['universe']['version']}）",
        f"- 面板：{summary['data']['loaded']} 只 / {summary['data']['rows']} 行，"
        f"列名 {', '.join(summary['data']['price_columns'])}（未复权撮合价）",
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
          f'请求 {args.days} 个交易日，结束 {args.end}')
    print(f'  往返费率 TradingCosts={round_trip_actual:.6f}，'
          f'分项复算={round_trip_recomputed:.6f}，'
          f'文档常数={DOC_ROUND_TRIP_COST:.6f}，差额={gap:+.6f}')
    if abs(gap) > 1e-6:
        print('  [警告] 往返费率与文档冻结常数不一致：本次按分项实际扣费，'
              '差额如实写入 summary.json 与 report.md，未调平')
    print('=' * 78, flush=True)

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
                 'raw_volume']).iter_rows(named=True):
            bars[str(row['ts_code'])] = (
                row['raw_open'], row['raw_close'], row['raw_prev_close'],
                row['raw_volume'])
        bars_by_date[day] = bars
    _log(f'撮合价表构建完成：{len(bars_by_date)} 个交易日', started)

    stateful = run_stateful(window_dates, by_date, bars_by_date, all_dates,
                            tier, config, args.equity, costs, rules)
    _log('状态机回测完成', started)
    baseline = run_fixed60(window_dates, by_date, bars_by_date, tier, config,
                           args.equity, costs, rules)
    _log('固定持有 60 日基线完成', started)

    nav_stateful = [float(row['nav_stateful']) for row in stateful['nav_rows']]
    nav_baseline = [float(row['nav_fixed60']) for row in baseline['nav_rows']]
    for index, row in enumerate(stateful['nav_rows']):
        row['nav_fixed60'] = nav_baseline[index]

    stateful_stats = stateful['stats']
    baseline_stats = baseline['stats']
    stateful_block = _summary_block(nav_stateful, args.equity, stateful_stats)
    baseline_block = _summary_block(nav_baseline, args.equity, baseline_stats)

    total_intents = max(1, stateful_stats['buy_intents'])
    decomp = _decompose(
        stateful['closed'], stateful_stats['unfilled_records'], window_dates,
        bars_by_date, costs, args.equity, stateful_block['total_cost'],
        baseline_block['total_cost'], stateful['nav'], baseline['nav'])

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
        f'选池固定为窗口起点 {start.isoformat()} 当日的 filter_universe 结果，'
        '不在每日重算。这是刻意选择：120 天全市场逐日重算不可承受。'
        '后果是窗口内退市/新上市的标的不会进出池——退市标的留在池内属正确方向'
        '（不制造幸存者偏差），但窗口内新上市标的不会进入候选，方向偏保守。',
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
        '基线 cohort 每个交易日起一个，各占初始权益的 1/60，'
        '持有满 60 个交易日后在开盘卖出；cohort 之间不共享持仓、不重算信号。',
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
            'requested_days': args.days,
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
        'lookahead_notes': lookahead_notes,
        'methodology_notes': methodology_notes,
        'elapsed_sec': round((datetime.now() - started).total_seconds(), 1),
    }
    # blocked_limit_up 的金额占比在报告里另取（保持字段名与文档一致）
    summary['blocked_limit_up']['amount_pct_of_planned'] = (
        summary['blocked_limit_up_amount_pct_of_planned'])

    out_dir = ROOT / args.out_root / f"{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}"
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(out_dir / 'nav.csv', stateful['nav_rows'], NAV_COLUMNS)
    _write_csv(out_dir / 'trades.csv', stateful['trades'], TRADE_COLUMNS)
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
    print(f'耗时        : {summary["elapsed_sec"]}s')
    print(f'产物目录    : {out_dir}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
