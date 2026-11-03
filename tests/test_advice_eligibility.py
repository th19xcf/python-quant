#!/usr/bin/env python3

"""可交易性剔除：涨跌停、停牌、流动性与历史长度。"""

from __future__ import annotations

from src.advice.config import (
    AdviceConfig,
    PortfolioConstraints,
    TierConfig,
    TradingCosts,
    TurnoverConfig,
)
from src.advice.eligibility import check_eligibility


def _tier(**overrides) -> TierConfig:
    base = TierConfig(
        key='t', label='测试', lookback_days=60, min_hold_days=5,
        confirm_days=3, validity_days=60, buy_percentile=0.9,
        sell_percentile=0.3, max_positions=10, edge_scale=0.1,
        signal_decay=0.0, max_volatility=1.0, max_atr_pct=0.2,
        min_amount=1e7,
    )
    return TierConfig(**{**base.__dict__, **overrides})


def _config(tier: TierConfig) -> AdviceConfig:
    return AdviceConfig(tiers={'t': tier}, constraints=PortfolioConstraints(),
                        costs=TradingCosts(), turnover=TurnoverConfig())


def _row(**overrides) -> dict:
    base = {
        'ts_code': '600000.SH', 'date': None,
        'raw_prev_close': 10.0, 'raw_open': 10.0, 'raw_high': 10.2,
        'raw_low': 9.8, 'raw_close': 10.1, 'raw_volume': 100000,
        'amount_ma20': 5e8, 'volatility_20': 0.3, 'atr_pct': 0.03,
        'history_rows': 200,
    }
    base.update(overrides)
    return base


def test_normal_stock_eligible():
    tier = _tier()
    result = check_eligibility(_row(), tier, _config(tier), 'buy')
    assert result.ok
    assert any('ST状态未知' in note for note in result.notes)


def test_one_word_limit_up_blocks_buy():
    tier = _tier()
    row = _row(raw_prev_close=10.0, raw_open=11.0, raw_high=11.0,
               raw_low=11.0, raw_close=11.0)
    result = check_eligibility(row, tier, _config(tier), 'buy')
    assert not result.ok
    assert '涨停' in result.blocked_reason


def test_limit_down_blocks_sell():
    tier = _tier()
    row = _row(raw_prev_close=10.0, raw_open=9.0, raw_high=9.0,
               raw_low=9.0, raw_close=9.0)
    result = check_eligibility(row, tier, _config(tier), 'sell')
    assert not result.ok
    assert '跌停' in result.blocked_reason


def test_suspended_zero_volume_blocked():
    tier = _tier()
    result = check_eligibility(_row(raw_volume=0), tier, _config(tier), 'buy')
    assert not result.ok
    assert '停牌' in result.blocked_reason


def test_low_liquidity_blocked():
    tier = _tier(min_amount=2e7)
    result = check_eligibility(_row(amount_ma20=1e7), tier, _config(tier), 'buy')
    assert not result.ok
    assert '成交额' in result.blocked_reason


def test_short_history_blocked():
    tier = _tier(lookback_days=120)
    result = check_eligibility(_row(history_rows=80), tier, _config(tier), 'buy')
    assert not result.ok
    assert '历史不足' in result.blocked_reason


def test_high_volatility_blocked():
    tier = _tier(max_volatility=0.5)
    result = check_eligibility(_row(volatility_20=0.9), tier, _config(tier), 'buy')
    assert not result.ok
    assert '波动率' in result.blocked_reason
