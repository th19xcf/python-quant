#!/usr/bin/env python3

"""回测可置信性补齐的四条硬约束单测。

覆盖：
1. 样本内/样本外 purge + embargo 边界（训练窗最后一行标签不得进入测试窗）；
2. 默认风控规则集完整性（9 条）与单票 profile 的可行域放宽；
3. ADV 容量上限（规则 fail-closed / 推荐目标权重裁剪）；
4. 成本单一真源（CostModel / TradingCosts / 因子评估）口径一致。
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import polars as pl
import pytest

from src.advice.config import PortfolioConstraints, TradingCosts
from src.advice.factor_eval import SimConfig
from src.advice.portfolio import Candidate, allocate
from src.backtest.costs.cost_model import (
    COST_DEFAULTS,
    TRANSFER_FEE_RATE_UNIFIED,
    CostModel,
)
from src.backtest.engine.holdout import holdout_bounds, split_frame_with_purge
from src.backtest.strategies.ma_strategy import MAStrategy
from src.core.usecases import run_backtest
from src.risk import (
    DEFAULT_RISK_LIMITS,
    MaxAdvParticipation,
    Order,
    Portfolio,
    default_risk_engine,
    single_asset_risk_engine,
)

# ---------------------------------------------------------------------------
# 1. purge / embargo
# ---------------------------------------------------------------------------

def test_purge_cuts_train_tail_when_embargo_shorter_than_horizon():
    """embargo < horizon+1 时，purge 必须把训练窗尾部切掉。"""
    bounds = holdout_bounds(100, 0.3, horizon=10, embargo=2)
    assert bounds is not None
    assert bounds.train_end == 70
    assert bounds.test_start == 72
    assert bounds.purged_train_end == 61

    # 训练窗最后一行索引 60，标签窗口 [61, 71]，必须整体早于 test_start=72
    last_train = bounds.purged_train_end - 1
    assert last_train + 1 + bounds.horizon < bounds.test_start
    assert bounds.purge_gap_days == 72 - 61


def test_embargo_alone_separates_when_long_enough():
    """embargo 足够长时 purge 不再额外裁剪，但仍保证标签不越界。"""
    bounds = holdout_bounds(160, 0.3, horizon=10, embargo=20)
    assert bounds is not None
    assert bounds.purged_train_end == bounds.train_end == 112
    assert bounds.test_start == 132
    last_train = bounds.purged_train_end - 1
    assert last_train + 1 + bounds.horizon < bounds.test_start


def test_split_frame_returns_disjoint_purged_windows():
    dates = pd.date_range('2023-01-02', periods=100, freq='B')
    frame = pl.DataFrame({'date': dates, 'close': np.arange(100, dtype=float)})
    split = split_frame_with_purge(frame, 0.3, horizon=10, embargo=2)
    assert split is not None
    assert split.is_df.height == split.bounds.purged_train_end
    assert split.oos_df.height == 100 - split.bounds.test_start
    # 训练窗最后一天与测试窗第一天之间至少隔 horizon+1 个交易日
    gap = (split.oos_df['date'][0] - split.is_df['date'][-1]).days
    assert gap > 10


@pytest.mark.parametrize('ratio', [0.0, 1.0, -0.1, 1.5])
def test_invalid_oos_ratio_raises(ratio):
    with pytest.raises(ValueError):
        holdout_bounds(100, ratio)


def test_short_sample_disables_oos():
    assert holdout_bounds(30, 0.3, horizon=10, embargo=20) is None


def test_run_backtest_returns_separate_in_and_out_of_sample_metrics():
    days = 160
    dates = pd.date_range('2023-01-02', periods=days, freq='B')
    close = 100.0 + 10.0 * np.sin(np.arange(days) / 10.0)
    data = pl.DataFrame({
        'date': dates,
        'open': close,
        'high': close * 1.01,
        'low': close * 0.99,
        'close': close,
        'volume': np.full(days, 1_000_000.0),
    })
    result = run_backtest(
        data=data, strategy=MAStrategy(short_window=5, long_window=20),
        ts_code='600000.SH', initial_capital=1_000_000.0, oos_ratio=0.3,
    )
    oos = result['oos_validation']
    assert oos['enabled'] is True
    assert oos['in_sample_days'] > 0 and oos['out_of_sample_days'] > 0
    assert set(oos['in_sample']) >= {'total_return', 'annual_return', 'max_drawdown'}
    assert set(oos['out_of_sample']) >= {'total_return', 'annual_return', 'max_drawdown'}
    # 不得只给一个合并数字：两段必须独立成块
    assert oos['in_sample'] != oos['out_of_sample']


# ---------------------------------------------------------------------------
# 2. 默认风控规则集
# ---------------------------------------------------------------------------

def test_default_risk_engine_is_complete():
    engine = default_risk_engine()
    assert {rule.name for rule in engine.rules} == {
        'price_limit_tradable', 'no_suspended_trade',
        'max_position_weight', 'max_industry_weight', 'max_industry_hhi',
        'max_drawdown_circuit', 'min_cash_reserve',
        'max_order_volume_ratio', 'max_adv_participation',
    }
    by_name = {rule.name: rule for rule in engine.rules}
    assert by_name['max_position_weight'].limit == DEFAULT_RISK_LIMITS['max_position_weight']
    assert by_name['max_industry_weight'].limit == DEFAULT_RISK_LIMITS['max_industry_weight']
    assert by_name['max_industry_hhi'].limit == DEFAULT_RISK_LIMITS['max_industry_hhi']
    assert by_name['max_adv_participation'].limit == DEFAULT_RISK_LIMITS['max_adv_participation']


def test_single_asset_profile_avoids_zero_trade_trap():
    """单票满仓策略不能被 10% 权重 / 30% 行业上限压成 0 成交。"""
    engine = single_asset_risk_engine()
    names = {rule.name for rule in engine.rules}
    assert 'max_position_weight' in names
    assert 'max_industry_weight' not in names
    assert 'max_industry_hhi' not in names
    by_name = {rule.name: rule for rule in engine.rules}
    assert by_name['max_position_weight'].limit == 1.0


# ---------------------------------------------------------------------------
# 3. ADV 容量
# ---------------------------------------------------------------------------

def _portfolio() -> Portfolio:
    return Portfolio(cash=1_000_000.0, last_prices={})


def test_adv_rule_boundary_and_fail_closed():
    portfolio = _portfolio()
    rule = MaxAdvParticipation(0.10)
    ok = Order(ts_code='600000.SH', side='buy', qty=1_000, price=10.0, adv_volume=100.0)
    assert rule.check(ok, portfolio).passed is True

    too_big = Order(ts_code='600000.SH', side='buy', qty=1_100, price=10.0, adv_volume=100.0)
    result = rule.check(too_big, portfolio)
    assert result.passed is False
    assert 'ADV' in result.reason

    missing = Order(ts_code='600000.SH', side='buy', qty=100, price=10.0)
    assert rule.check(missing, portfolio).passed is False


def test_allocation_caps_target_by_adv_notional():
    """单票目标资金不得超过 20 日均成交额 × 参与率。"""
    constraints = PortfolioConstraints(max_single_weight=0.08, adv_pct=0.10)
    candidate = Candidate(
        ts_code='600000.SH', score_percentile=0.9, price=10.0,
        industry='银行', industry_source='test', adv_amount=100_000.0,
    )
    allocations = allocate([candidate], equity=1_000_000.0, constraints=constraints)
    assert len(allocations) == 1
    # ADV 上限 10% × 10 万 = 1 万元 → 1000 股；若无 ADV 上限会是 8 万 → 8000 股
    assert allocations[0].shares == 1_000
    assert allocations[0].target_weight == pytest.approx(0.01)


def test_allocation_without_adv_keeps_weight_cap():
    constraints = PortfolioConstraints(max_single_weight=0.08, adv_pct=0.10)
    candidate = Candidate(
        ts_code='600000.SH', score_percentile=0.9, price=10.0,
        industry='银行', industry_source='test', adv_amount=0.0,
    )
    allocations = allocate([candidate], equity=1_000_000.0, constraints=constraints)
    assert allocations[0].shares == 8_000


# ---------------------------------------------------------------------------
# 4. 成本单一真源
# ---------------------------------------------------------------------------

def test_cost_model_defaults_match_single_source():
    model = CostModel()
    assert model.params == COST_DEFAULTS
    assert model.params['transfer_fee_rate'] == TRANSFER_FEE_RATE_UNIFIED == 0.00002
    # 不传 trade_date 也应扣满双边过户费，不能少扣一半。
    # 现行口径（佣金 0.05%、ETF 免印花税与过户费、ADV≥1 亿滑点 5bp）：
    # 50 + 0 + 0 + 50 = 100.0
    sell = model.calculate_sell_cost(10_000, 10.0, ts_code='510300.SH', adv20=2.0e8)
    assert sell == pytest.approx(100.0)


def test_advice_trading_costs_reference_backtest_defaults():
    costs = TradingCosts()
    assert costs.commission_rate == COST_DEFAULTS['commission_rate']
    assert costs.stamp_duty_rate == COST_DEFAULTS['stamp_tax_rate']
    assert costs.impact_rate == COST_DEFAULTS['slippage']
    assert costs.min_commission == COST_DEFAULTS['min_commission']
    assert costs.round_trip_rate == pytest.approx(
        2 * COST_DEFAULTS['commission_rate']
        + COST_DEFAULTS['stamp_tax_rate']
        + 2 * COST_DEFAULTS['slippage'])


def test_factor_eval_cost_model_matches_single_source():
    model = SimConfig().make_cost_model()
    assert model.params == COST_DEFAULTS
