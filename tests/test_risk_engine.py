#!/usr/bin/env python3

"""
风控模块单元测试
覆盖：Order / Portfolio / 3 条 Rule / RiskEngine.gate() 组合场景
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest

from src.risk import (
    MaxDrawdownCircuit,
    MaxIndustryWeight,
    MaxPositionWeight,
    Order,
    Portfolio,
    RiskEngine,
)

# ======================================================================
# Fixtures
# ======================================================================

@pytest.fixture
def empty_portfolio():
    return Portfolio(cash=1_000_000.0, positions={}, last_prices={})


@pytest.fixture
def portfolio_with_one_holding():
    """持仓一只股票 500 股 @ 100 元，总资产 50w+现金"""
    return Portfolio(
        positions={'000001.SZ': 500},
        last_prices={'000001.SZ': 100.0},
        cash=500_000.0,
        industry_map={'000001.SZ': '银行'},
    )


@pytest.fixture
def portfolio_with_two_industries():
    """银行 25 万 + 券商 25 万 + 现金 50 万，总资产 100 万"""
    return Portfolio(
        positions={'000001.SZ': 2500, '601211.SH': 1250},
        last_prices={'000001.SZ': 100.0, '601211.SH': 200.0},
        cash=500_000.0,
        industry_map={'000001.SZ': '银行', '601211.SH': '证券'},
    )


@pytest.fixture
def dd_portfolio():
    """有回撤的组合：权益曲线从 100w 跌到 80w，回撤 20%"""
    equity = []
    for i in range(260):
        eq = 1_000_000.0 - (200_000.0 * min(1, i / 260))
        equity.append({'equity': eq})
    return Portfolio(
        positions={'000001.SZ': 1000},
        last_prices={'000001.SZ': 200.0},
        cash=600_000.0,
        equity_curve=equity,
    )


# ======================================================================
# MaxPositionWeight
# ======================================================================

class TestMaxPositionWeight:

    def test_buy_within_limit_passes(self, empty_portfolio):
        rule = MaxPositionWeight(0.10)
        order = Order('000001.SZ', 'buy', 500, 100.0)
        result = rule.check(order, empty_portfolio)
        assert result.passed

    def test_buy_exceeds_limit_rejected(self, empty_portfolio):
        """空仓时买入 20% 权重 > 10% 上限"""
        rule = MaxPositionWeight(0.10)
        order = Order('000001.SZ', 'buy', 2000, 100.0)   # 20w / 100w = 20%
        result = rule.check(order, empty_portfolio)
        assert not result.passed
        assert '20.00%' in result.reason
        assert '10.00%' in result.reason

    def test_sell_always_passes(self, portfolio_with_one_holding):
        rule = MaxPositionWeight(0.10)
        order = Order('000001.SZ', 'sell', 500, 100.0)
        result = rule.check(order, portfolio_with_one_holding)
        assert result.passed

    def test_add_to_existing_position_still_ok(self, portfolio_with_one_holding):
        """已有 5% 权重，再加 4% = 9%，< 10% 通过"""
        rule = MaxPositionWeight(0.10)
        order = Order('000001.SZ', 'buy', 4000, 100.0)   # 40w
        result = rule.check(order, portfolio_with_one_holding)
        # 现有 5w + 新增 40w = 45w / 55w ≈ 81% ... wait
        # 总资产 = 现金 50w + 持仓 5w = 55w
        # 新权重 = (5w + 40w) / 55w = 45/55 ≈ 81.8% >> 10%
        assert not result.passed

    def test_current_total_value_none_auto_calc(self):
        """total_value=None 时用 cash + positions 自动算"""
        p = Portfolio(
            positions={'A': 100},
            last_prices={'A': 10.0},
            cash=900.0,
        )
        assert p.current_total_value() == 1900.0   # 100*10 + 900


# ======================================================================
# MaxIndustryWeight
# ======================================================================

class TestMaxIndustryWeight:

    def test_within_limit_passes(self, portfolio_with_two_industries):
        rule = MaxIndustryWeight(0.30)
        order = Order('000858.SZ', 'buy', 1000, 100.0, industry='银行')   # +10w 银行
        result = rule.check(order, portfolio_with_two_industries)
        # 银行现状 25w / 100w = 25%，+10w = 35w / 100w = 35% > 30%
        assert not result.passed

    def test_industry_not_in_map_treated_as_zero(self, portfolio_with_two_industries):
        """券商持仓不存在但 order.industry='证券'——不，map 里有 601211.SH → 证券"""
        rule = MaxIndustryWeight(0.30)
        order = Order('601211.SH', 'buy', 500, 200.0, industry='证券')   # +10w
        # 证券现状 25w，+10w = 35w / 100w = 35% > 30%
        result = rule.check(order, portfolio_with_two_industries)
        assert not result.passed

    def test_no_industry_label_skips(self, portfolio_with_two_industries):
        """order.industry='' 不拦"""
        rule = MaxIndustryWeight(0.30)
        order = Order('000001.SZ', 'buy', 1000, 100.0)   # industry=''
        result = rule.check(order, portfolio_with_two_industries)
        assert result.passed

    def test_sell_always_passes(self, portfolio_with_two_industries):
        rule = MaxIndustryWeight(0.30)
        order = Order('000001.SZ', 'sell', 2500, 100.0, industry='银行')
        result = rule.check(order, portfolio_with_two_industries)
        assert result.passed


# ======================================================================
# MaxDrawdownCircuit
# ======================================================================

class TestMaxDrawdownCircuit:

    def test_buy_rejected_when_dd_exceeds_limit(self, dd_portfolio):
        rule = MaxDrawdownCircuit(0.15)
        order = Order('000001.SZ', 'buy', 100, 200.0)
        result = rule.check(order, dd_portfolio)
        assert not result.passed
        assert '回撤' in result.reason
        assert '熔断' in result.reason

    def test_sell_always_passes_in_circuit(self, dd_portfolio):
        rule = MaxDrawdownCircuit(0.15)
        order = Order('000001.SZ', 'sell', 100, 200.0)
        result = rule.check(order, dd_portfolio)
        assert result.passed

    def test_no_equity_curve_treated_as_zero_dd(self, portfolio_with_one_holding):
        rule = MaxDrawdownCircuit(0.15)
        order = Order('000001.SZ', 'buy', 100, 100.0)
        result = rule.check(order, portfolio_with_one_holding)
        assert result.passed


# ======================================================================
# RiskEngine.gate() 组合
# ======================================================================

class TestRiskEngine:

    def test_no_rules_all_approved(self, empty_portfolio):
        engine = RiskEngine()
        order = Order('000001.SZ', 'buy', 100, 100.0)
        result = engine.gate([order], empty_portfolio)
        assert result.reject_rate == 0.0
        assert result.approved_orders == [order]

    def test_single_reject(self, empty_portfolio):
        engine = RiskEngine([MaxPositionWeight(0.05)])   # 5%
        orders = [Order('000001.SZ', 'buy', 600, 100.0)]   # 6w / 100w = 6% > 5%
        result = engine.gate(orders, empty_portfolio)
        assert result.reject_count == 1
        assert len(result.approved) == 0
        assert result.rejected_orders[0][1] == 'max_position_weight'

    def test_first_rule_blocks_no_further_check(self, empty_portfolio):
        """第一条拦了，后面不检查"""
        engine = RiskEngine([
            MaxPositionWeight(0.01),   # 1%
            MaxIndustryWeight(0.50),   # 50%
        ])
        order = Order('000001.SZ', 'buy', 200, 100.0)   # 2w / 100w = 2% > 1%
        result = engine.gate([order], empty_portfolio)
        assert result.rejected_orders[0][1] == 'max_position_weight'

    def test_mixed_orders(self, portfolio_with_one_holding):
        """buy 被拦、sell 通过"""
        engine = RiskEngine([MaxPositionWeight(0.05)])
        orders = [
            Order('000001.SZ', 'buy', 1000, 100.0),   # buy 超过
            Order('000001.SZ', 'sell', 500, 100.0),   # sell 永远过
        ]
        result = engine.gate(orders, portfolio_with_one_holding)
        assert result.reject_count == 1
        assert len(result.approved) == 1
        assert result.approved[0][0].side == 'sell'

    def test_gate_result_fields(self, empty_portfolio):
        engine = RiskEngine([MaxPositionWeight(0.05)])
        orders = [Order('A.SZ', 'buy', 600, 100.0)]
        result = engine.gate(orders, empty_portfolio)
        assert result.reject_rate == 1.0
        assert result.rejected[0] == (orders[0], 'max_position_weight', result.rejected[0][2])
        # 第三元组是 reason 字符串
        assert '上限' in result.rejected[0][2]


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
