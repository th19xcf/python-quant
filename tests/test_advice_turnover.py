#!/usr/bin/env python3

"""换手控制单元测试：最小持有期 / 确认日 / 成本门槛 / 换手上限。"""

from __future__ import annotations

from dataclasses import replace

import pytest

from src.advice.config import AdviceConfig, PortfolioConstraints, TierConfig, TradingCosts, TurnoverConfig
from src.advice.turnover import (
    TradeIntent, apply_turnover_cap, count_trading_days, entry_allowed,
    expected_edge, update_holding,
)


def _tier(**overrides) -> TierConfig:
    base = TierConfig(
        key='test', label='测试', lookback_days=30, min_hold_days=5,
        confirm_days=3, validity_days=60, buy_percentile=0.9,
        sell_percentile=0.3, max_positions=10, edge_scale=0.08,
        signal_decay=0.0, max_volatility=1.0, max_atr_pct=0.2,
        min_amount=1e7,
    )
    return replace(base, **overrides)


def _config(tier: TierConfig, costs: TradingCosts | None = None,
            turnover: TurnoverConfig | None = None) -> AdviceConfig:
    return AdviceConfig(
        tiers={'test': tier},
        constraints=PortfolioConstraints(),
        costs=costs or TradingCosts(),
        turnover=turnover or TurnoverConfig(),
    )


def test_min_hold_blocks_sell_even_if_confirmed():
    tier = _tier(min_hold_days=10)
    config = _config(tier)
    decision = update_holding(
        prev_percentile=0.2, percentile=0.1, sell_confirm=5,
        days_held=3, tier=tier, config=config)
    assert decision.action == 'HOLD'
    assert '最小持有期' in decision.reason


def test_sell_requires_consecutive_confirmations():
    tier = _tier(confirm_days=3, signal_decay=0.0)
    config = _config(tier)
    # 第 2 次确认：继续持有
    decision = update_holding(
        prev_percentile=0.1, percentile=0.1, sell_confirm=1,
        days_held=20, tier=tier, config=config, best_candidate_pct=0.95)
    assert decision.action == 'HOLD'
    assert decision.sell_confirm == 2
    # 第 3 次确认：卖出
    decision = update_holding(
        prev_percentile=0.1, percentile=0.1, sell_confirm=2,
        days_held=20, tier=tier, config=config, best_candidate_pct=0.95)
    assert decision.action == 'SELL'
    assert decision.sell_confirm == 3


def test_confirm_counter_resets_when_signal_recovers():
    tier = _tier(confirm_days=3, signal_decay=0.0)
    config = _config(tier)
    decision = update_holding(
        prev_percentile=0.1, percentile=0.6, sell_confirm=2,
        days_held=20, tier=tier, config=config)
    assert decision.action == 'HOLD'
    assert decision.sell_confirm == 0


def test_cost_gate_blocks_low_edge_sell():
    tier = _tier(edge_scale=0.001)
    config = _config(tier)
    decision = update_holding(
        prev_percentile=0.2, percentile=0.2, sell_confirm=2,
        days_held=20, tier=tier, config=config)
    # 预期改善 0.06% < 一次进出成本 0.36%
    assert decision.action == 'HOLD'
    assert '成本门槛' in decision.reason


def test_entry_cost_gate():
    tier = _tier(edge_scale=0.004)
    config = _config(tier)
    allowed, reason = entry_allowed(0.9, tier, config)
    assert not allowed and '成本门槛' in reason
    stronger = _tier(edge_scale=0.01)
    allowed, _ = entry_allowed(0.99, stronger, _config(stronger))
    assert allowed


def test_validity_expiry_forces_exit():
    tier = _tier(validity_days=20, confirm_days=3)
    config = _config(tier)
    decision = update_holding(
        prev_percentile=0.4, percentile=0.5, sell_confirm=0,
        days_held=20, tier=tier, config=config)
    assert decision.action == 'SELL'
    assert '有效期' in decision.reason


def test_turnover_cap_budgets_both_sides():
    tier = _tier()
    config = _config(tier, turnover=TurnoverConfig(max_turnover_per_rebalance=0.1))
    intents = [
        TradeIntent('A', 'sell', 50_000, 0.1),
        TradeIntent('B', 'buy', 90_000, 0.95),
        TradeIntent('C', 'buy', 80_000, 0.9),
    ]
    plan = apply_turnover_cap(intents, equity=1_000_000, max_turnover=0.1)
    # 单边上限 10% → 双边预算 20 万
    assert plan.budget == pytest.approx(200_000)
    assert plan.gross_notional <= 200_000 + 1e-6
    assert [item.ts_code for item in plan.admitted] == ['A', 'B']
    assert [item.ts_code for item in plan.deferred] == ['C']


def test_expected_edge_monotonic():
    tier = _tier(edge_scale=0.1)
    assert expected_edge(0.9, tier) > expected_edge(0.5, tier) > expected_edge(0.2, tier)
    assert expected_edge(0.5, tier) == pytest.approx(0.0)


def test_count_trading_days_uses_calendar():
    from datetime import date
    dates = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7),
             date(2026, 1, 8), date(2026, 1, 9)]
    assert count_trading_days(dates[0], dates[3], dates) == 3
    assert count_trading_days(dates[3], dates[3], dates) == 0
