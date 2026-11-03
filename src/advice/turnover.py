#!/usr/bin/env python3

"""换手控制：最小持有期、卖出确认日、成本门槛与单次换手上限。

核心原则：日频输出 ≠ 日频调仓。信号每天更新，但真正改变仓位要同时满足
- 持有满最小天数；
- 卖出信号连续确认 N 日；
- 预期收益改善覆盖一次进出成本；
- 单次调仓不超过换手上限。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from src.advice.config import AdviceConfig, TierConfig, TradingCosts


def expected_edge(percentile: float, tier: TierConfig) -> float:
    """把截面百分位映射为持有期预期收益（-edge_scale ~ +edge_scale）。

    这是排序用的相对强弱代理，不追求精确收益预测；参数 ``edge_scale``
    按档位可调，仅用于成本门槛与换手比较。
    """
    return (float(percentile) - 0.5) * 2.0 * tier.edge_scale


def count_trading_days(entry: date, current: date,
                       trading_dates: list[date]) -> int:
    """计算两个交易日之间的交易日间隔（持有天数）。"""
    if entry >= current:
        return 0
    numeric = [item for item in trading_dates if entry <= item <= current]
    if numeric:
        return max(0, len(numeric) - 1)
    return max(0, (current - entry).days)


@dataclass
class HoldingDecision:
    """单笔持仓的当日决策。"""

    ts_code: str
    action: str          # HOLD / SELL
    smoothed_percentile: float
    sell_confirm: int
    days_held: int
    expected_edge: float
    reason: str = ''


def update_holding(prev_percentile: float, percentile: float,
                   sell_confirm: int, days_held: int, tier: TierConfig,
                   config: AdviceConfig, best_candidate_pct: float | None = None,
                   ts_code: str = '') -> HoldingDecision:
    """更新单笔持仓的信号平滑、确认计数与是否卖出。

    ``prev_percentile`` 为上一交易日的平滑百分位，``percentile`` 为当日
    原始百分位；``sell_confirm`` 为上一日已连续确认的卖出天数。
    """
    decay = max(0.0, min(1.0, tier.signal_decay))
    smoothed = decay * float(prev_percentile) + (1.0 - decay) * float(percentile)
    raw_sell = smoothed <= tier.sell_percentile
    confirm = int(sell_confirm) + 1 if raw_sell else 0
    edge = expected_edge(smoothed, tier)
    min_hold = config.effective_min_hold(tier)
    cost = config.costs.round_trip_rate * config.turnover.cost_threshold_multiple

    if days_held < min_hold:
        return HoldingDecision(
            ts_code=ts_code, action='HOLD', smoothed_percentile=smoothed,
            sell_confirm=confirm, days_held=days_held, expected_edge=edge,
            reason=f'最小持有期未满({days_held}/{min_hold})')

    if days_held >= tier.validity_days and percentile < tier.buy_percentile:
        return HoldingDecision(
            ts_code=ts_code, action='SELL', smoothed_percentile=smoothed,
            sell_confirm=confirm, days_held=days_held, expected_edge=edge,
            reason=f'推荐有效期到期({tier.validity_days}日)且未再确认')

    if confirm < tier.confirm_days:
        return HoldingDecision(
            ts_code=ts_code, action='HOLD', smoothed_percentile=smoothed,
            sell_confirm=confirm, days_held=days_held, expected_edge=edge,
            reason=f'卖出信号确认中({confirm}/{tier.confirm_days})')

    replacement = 0.0
    if best_candidate_pct is not None:
        replacement = expected_edge(best_candidate_pct, tier) - edge
    improvement = max(-edge, replacement)
    if improvement < cost:
        return HoldingDecision(
            ts_code=ts_code, action='HOLD', smoothed_percentile=smoothed,
            sell_confirm=confirm, days_held=days_held, expected_edge=edge,
            reason=f'预期改善{improvement:.2%}<成本门槛{cost:.2%}')

    return HoldingDecision(
        ts_code=ts_code, action='SELL', smoothed_percentile=smoothed,
        sell_confirm=confirm, days_held=days_held, expected_edge=edge,
        reason=f'卖出信号已确认{confirm}日且收益改善覆盖成本')


def entry_allowed(percentile: float, tier: TierConfig,
                  config: AdviceConfig) -> tuple[bool, str]:
    """买入成本门槛：预期收益必须覆盖一次完整进出的成本。"""
    project = expected_edge(percentile, tier)
    threshold = (config.costs.round_trip_rate
                 * config.turnover.min_entry_edge_multiple)
    if project < threshold:
        return False, f'预期收益{project:.2%}<成本门槛{threshold:.2%}'
    return True, ''


@dataclass
class TradeIntent:
    """调仓意图（用于换手上限分配）。"""

    ts_code: str
    side: str            # buy / sell
    notional: float
    priority: float      # 卖出用低分优先，买入用高分优先


@dataclass
class TurnoverPlan:
    """换手上限执行结果。"""

    admitted: list[TradeIntent]
    deferred: list[TradeIntent]
    gross_notional: float
    budget: float


def apply_turnover_cap(intents: list[TradeIntent], equity: float,
                       max_turnover: float) -> TurnoverPlan:
    """按换手上限裁剪调仓意图。

    口径：``gross_notional=(买入额+卖出额)``，单边换手 = gross/2；
    因此预算上限为 ``2 × max_turnover × equity``。卖出优先释放资金，
    买入按分数从高到低占用剩余预算。
    """
    budget = max(0.0, 2.0 * max_turnover * equity)
    sells = sorted([item for item in intents if item.side == 'sell'],
                   key=lambda item: item.priority)
    buys = sorted([item for item in intents if item.side == 'buy'],
                  key=lambda item: item.priority, reverse=True)
    admitted: list[TradeIntent] = []
    deferred: list[TradeIntent] = []
    gross = 0.0

    for intent in sells + buys:
        amount = max(0.0, float(intent.notional))
        if gross + amount <= budget + 1e-6:
            admitted.append(intent)
            gross += amount
        else:
            deferred.append(intent)

    return TurnoverPlan(admitted=admitted, deferred=deferred,
                        gross_notional=gross, budget=budget)


def load_state_payload(payload: dict[str, Any] | None,
                       costs: TradingCosts) -> dict[str, Any]:
    """兼容旧状态文件：缺失字段时补齐，避免跨版本读取崩溃。"""
    if not payload:
        return {'holdings': [], 'last_trade_date': None}
    payload.setdefault('holdings', [])
    payload.setdefault('last_trade_date', None)
    payload.setdefault('costs', costs.round_trip_rate)
    return payload
