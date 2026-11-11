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


def test_one_word_limit_up_kept_and_marked():
    """T 日一字涨停：条目保留（ok=True），只标注 limit_up_risk。"""
    tier = _tier()
    row = _row(raw_prev_close=10.0, raw_open=11.0, raw_high=11.0,
               raw_low=11.0, raw_close=11.0)
    result = check_eligibility(row, tier, _config(tier), 'buy')
    assert result.ok, result.blocked_reason
    assert result.limit_up_risk is True
    # 依据必须写明触发价与涨停价（主板 10% → 11.00）
    assert '涨停价 11.00' in result.limit_up_basis
    assert '一字板' in result.limit_up_basis
    assert any('次日可能买不进' in note for note in result.notes)


def test_intraday_limit_up_kept_and_marked():
    """盘中触板收盘涨停：同样保留并标注，形态写「盘中触板」。"""
    tier = _tier()
    row = _row(raw_prev_close=10.0, raw_open=10.4, raw_high=11.0,
               raw_low=10.3, raw_close=11.0)
    result = check_eligibility(row, tier, _config(tier), 'buy')
    assert result.ok, result.blocked_reason
    assert result.limit_up_risk is True
    assert '盘中触板' in result.limit_up_basis


def test_non_limit_up_not_marked():
    tier = _tier()
    result = check_eligibility(_row(raw_close=10.5, raw_high=10.6),
                               tier, _config(tier), 'buy')
    assert result.ok, result.blocked_reason
    assert result.limit_up_risk is False
    assert result.limit_up_basis == ''


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


# ----------------------------------------------------------------------
# 阈值边界：恰好等于阈值放行、差 1 单位拒绝。
# 现行口径是「上限含等号」（实现用严格的 ``<`` / ``>`` 比较），因此
# 「等于上限」必须放行；把 ``<`` 改成 ``<=``（或 ``>`` 改成 ``>=``）本组必红。
# 锚点来源：阈值本身取自 HTTP 层可读的 ``TierConfig`` / ``AdviceConfig`` 字段
# （min_amount=2e7、lookback_days=120、max_volatility=0.5、max_atr_pct=0.2、
# min_price=1.0），阻断原因里的数字由实现按实测输入格式化，非人工估算。
# ----------------------------------------------------------------------

def test_min_amount_at_threshold_passes():
    tier = _tier(min_amount=2e7)
    result = check_eligibility(_row(amount_ma20=2e7), tier, _config(tier), 'buy')
    assert result.ok, result.blocked_reason


def test_min_amount_one_yuan_below_threshold_blocked():
    tier = _tier(min_amount=2e7)
    result = check_eligibility(_row(amount_ma20=2e7 - 1), tier, _config(tier), 'buy')
    assert not result.ok
    assert result.blocked_reason == '20日均成交额不足(2000万)'


def test_history_rows_at_lookback_passes():
    tier = _tier(lookback_days=120)
    result = check_eligibility(_row(history_rows=120), tier, _config(tier), 'buy')
    assert result.ok, result.blocked_reason


def test_history_rows_one_below_lookback_blocked():
    tier = _tier(lookback_days=120)
    result = check_eligibility(_row(history_rows=119), tier, _config(tier), 'buy')
    assert not result.ok
    assert result.blocked_reason == '历史不足119日<120日'


def test_volatility_at_max_passes():
    tier = _tier(max_volatility=0.5)
    result = check_eligibility(_row(volatility_20=0.5), tier, _config(tier), 'buy')
    assert result.ok, result.blocked_reason


def test_volatility_above_max_blocked():
    tier = _tier(max_volatility=0.5)
    result = check_eligibility(_row(volatility_20=0.5001), tier, _config(tier), 'buy')
    assert not result.ok
    assert result.blocked_reason == '20日波动率50%超限'


def test_atr_pct_at_max_passes():
    tier = _tier(max_atr_pct=0.2)
    result = check_eligibility(_row(atr_pct=0.2), tier, _config(tier), 'buy')
    assert result.ok, result.blocked_reason


def test_atr_pct_above_max_blocked():
    tier = _tier(max_atr_pct=0.2)
    result = check_eligibility(_row(atr_pct=0.2001), tier, _config(tier), 'buy')
    assert not result.ok
    assert result.blocked_reason == 'ATR占比20.0%超限'


def test_min_price_at_threshold_passes():
    tier = _tier()
    result = check_eligibility(_row(raw_close=1.0), tier, _config(tier), 'buy')
    assert result.ok, result.blocked_reason


def test_min_price_below_threshold_blocked():
    tier = _tier()
    result = check_eligibility(_row(raw_close=0.99), tier, _config(tier), 'buy')
    assert not result.ok
    assert result.blocked_reason == '股价0.99<1.00'
