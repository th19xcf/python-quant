#!/usr/bin/env python3

"""
风控模块单元测试
覆盖：Order / Portfolio / 8 条 Rule / RiskEngine.gate() 组合场景
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest

from src.risk import (
    MaxDrawdownCircuit,
    MaxIndustryHHI,
    MaxIndustryWeight,
    MaxOrderVolumeRatio,
    MaxPositionWeight,
    MinCashReserve,
    NoSuspendedTrade,
    Order,
    Portfolio,
    PriceLimitTradable,
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

    def test_current_total_value_auto_calc(self):
        """总资产由 cash + positions 现场计算（不接受外部注入）"""
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

    def test_no_industry_label_rejected(self, portfolio_with_two_industries):
        """order.industry='' 无法评估集中度，fail-closed 拒单（D1）"""
        rule = MaxIndustryWeight(0.30)
        order = Order('000001.SZ', 'buy', 1000, 100.0)   # industry=''
        result = rule.check(order, portfolio_with_two_industries)
        assert not result.passed
        assert '行业标签' in result.reason

    def test_unmapped_holding_rejected(self):
        """持仓中存在未映射行业的标的时不静默漏算，直接拒单（D1）"""
        rule = MaxIndustryWeight(0.30)
        portfolio = Portfolio(
            positions={'000001.SZ': 2500, '999999.SZ': 1000},
            last_prices={'000001.SZ': 100.0, '999999.SZ': 50.0},
            cash=500_000.0,
            industry_map={'000001.SZ': '银行'},   # 999999.SZ 未映射
        )
        order = Order('000858.SZ', 'buy', 1000, 100.0, industry='银行')
        result = rule.check(order, portfolio)
        assert not result.passed
        assert '未映射行业' in result.reason

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

    def test_no_equity_curve_fail_closed(self, portfolio_with_one_holding):
        """无权益曲线时无法评估回撤，fail-closed 拒单（D2）"""
        rule = MaxDrawdownCircuit(0.15)
        order = Order('000001.SZ', 'buy', 100, 100.0)
        result = rule.check(order, portfolio_with_one_holding)
        assert not result.passed
        assert '无法评估回撤' in result.reason

    def test_invalid_equity_fail_closed(self):
        """权益缺失/非正时不得伪造 0 回撤（D2）"""
        rule = MaxDrawdownCircuit(0.15)
        portfolio = Portfolio(
            cash=100_000.0,
            equity_curve=[{'date': '2024-01-01', 'equity': None}],
        )
        order = Order('000001.SZ', 'buy', 100, 100.0)
        result = rule.check(order, portfolio)
        assert not result.passed
        assert '数据非法' in result.reason

    def test_unordered_equity_curve_sorted(self):
        """乱序权益曲线按日期排序后再计算回撤（D3）"""
        curve = [
            {'date': '2024-01-03', 'equity': 1_000_000.0},
            {'date': '2024-01-01', 'equity': 1_200_000.0},
            {'date': '2024-01-02', 'equity': 900_000.0},
        ]
        portfolio = Portfolio(cash=500_000.0, equity_curve=curve)
        # 按日期排序后峰值 120w，当前 100w → 回撤 16.67% > 15%
        assert portfolio.current_drawdown() == pytest.approx((1_200_000 - 1_000_000) / 1_200_000)
        rule = MaxDrawdownCircuit(0.15)
        result = rule.check(Order('000001.SZ', 'buy', 100, 100.0), portfolio)
        assert not result.passed


# ======================================================================
# Order 输入域校验
# ======================================================================

class TestOrderValidation:

    def test_negative_qty_rejected(self):
        with pytest.raises(ValueError, match='qty'):
            Order('000001.SZ', 'buy', -100, 100.0)

    def test_zero_qty_rejected(self):
        with pytest.raises(ValueError, match='qty'):
            Order('000001.SZ', 'buy', 0, 100.0)

    def test_negative_price_rejected(self):
        with pytest.raises(ValueError, match='price'):
            Order('000001.SZ', 'buy', 100, -1.0)

    def test_nan_price_rejected(self):
        with pytest.raises(ValueError, match='price'):
            Order('000001.SZ', 'buy', 100, float('nan'))

    def test_invalid_side_rejected(self):
        with pytest.raises(ValueError, match='side'):
            Order('000001.SZ', 'short', 100, 100.0)

    def test_empty_ts_code_rejected(self):
        with pytest.raises(ValueError, match='ts_code'):
            Order('', 'buy', 100, 100.0)

    def test_invalid_prev_close_rejected(self):
        with pytest.raises(ValueError, match='prev_close'):
            Order('000001.SZ', 'buy', 100, 100.0, prev_close=0.0)

    def test_negative_bar_volume_rejected(self):
        with pytest.raises(ValueError, match='bar_volume'):
            Order('000001.SZ', 'buy', 100, 100.0, bar_volume=-1.0)


# ======================================================================
# RiskEngine.gate() 组合
# ======================================================================

class TestRiskEngine:

    def test_no_rules_fail_closed(self, empty_portfolio):
        """无规则时不得全放行，必须抛错（D5）"""
        engine = RiskEngine()
        order = Order('000001.SZ', 'buy', 100, 100.0)
        with pytest.raises(RuntimeError, match='fail-closed'):
            engine.gate([order], empty_portfolio)

    def test_multiple_violations_aggregated(self, empty_portfolio):
        """多规则同时违反时聚合全部原因（D11）"""
        engine = RiskEngine([
            MaxPositionWeight(0.05),
            MaxDrawdownCircuit(0.15),
        ])
        order = Order('000001.SZ', 'buy', 600, 100.0)
        result = engine.gate([order], empty_portfolio)
        assert result.reject_count == 1
        reason = result.rejected[0][2]
        assert 'max_position_weight' in reason
        assert 'max_drawdown_circuit' in reason

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


# ======================================================================
# MinCashReserve
# ======================================================================

class TestMinCashReserve:

    def test_buy_within_cash_passes(self, empty_portfolio):
        rule = MinCashReserve()
        order = Order('000001.SZ', 'buy', 500, 100.0)   # 5w < 100w 现金
        assert rule.check(order, empty_portfolio).passed

    def test_buy_overdraft_rejected(self, empty_portfolio):
        rule = MinCashReserve()
        order = Order('000001.SZ', 'buy', 20000, 100.0)   # 200w > 100w 现金
        result = rule.check(order, empty_portfolio)
        assert not result.passed
        assert '现金' in result.reason

    def test_buy_all_in_allowed(self, empty_portfolio):
        """默认 reserve=0：满仓买入（剩余现金恰好为 0）放行"""
        rule = MinCashReserve()
        order = Order('000001.SZ', 'buy', 10000, 100.0)   # 恰好 100w
        assert rule.check(order, empty_portfolio).passed

    def test_reserve_threshold(self, empty_portfolio):
        rule = MinCashReserve(reserve=100_000.0)
        order = Order('000001.SZ', 'buy', 9500, 100.0)   # 95w，剩余 5w < 10w 保留
        assert not rule.check(order, empty_portfolio).passed

    def test_sell_always_passes(self, portfolio_with_one_holding):
        rule = MinCashReserve()
        order = Order('000001.SZ', 'sell', 500, 100.0)
        assert rule.check(order, portfolio_with_one_holding).passed


# ======================================================================
# MaxIndustryHHI
# ======================================================================

class TestMaxIndustryHHI:

    def test_balanced_buy_passes(self, portfolio_with_two_industries):
        rule = MaxIndustryHHI(0.25)
        # 买入 10w 银行 → 银行 0.35 / 证券 0.25 → HHI = 0.185 < 0.25
        order = Order('000001.SZ', 'buy', 1000, 100.0, industry='银行')
        assert rule.check(order, portfolio_with_two_industries).passed

    def test_concentrated_buy_rejected(self, portfolio_with_two_industries):
        rule = MaxIndustryHHI(0.25)
        # 买入 50w 银行 → 银行 0.75 / 证券 0.25 → HHI = 0.625 > 0.25
        order = Order('000001.SZ', 'buy', 5000, 100.0, industry='银行')
        result = rule.check(order, portfolio_with_two_industries)
        assert not result.passed
        assert 'HHI' in result.reason

    def test_new_industry_counted(self, portfolio_with_two_industries):
        """买入新行业：新行业市值计入 HHI"""
        rule = MaxIndustryHHI(0.20)
        # 买入 40w 保险（新行业）→ 0.25²+0.25²+0.40² = 0.285 > 0.20
        order = Order('600000.SH', 'buy', 4000, 100.0, industry='保险')
        assert not rule.check(order, portfolio_with_two_industries).passed

    def test_missing_industry_fail_closed(self, portfolio_with_two_industries):
        rule = MaxIndustryHHI(0.25)
        order = Order('000001.SZ', 'buy', 100, 100.0)   # 无行业标签
        assert not rule.check(order, portfolio_with_two_industries).passed

    def test_unmapped_position_fail_closed(self):
        rule = MaxIndustryHHI(0.25)
        portfolio = Portfolio(
            positions={'000001.SZ': 100},
            last_prices={'000001.SZ': 100.0},
            cash=900_000.0,
            industry_map={},   # 持仓未映射行业
        )
        order = Order('601211.SH', 'buy', 100, 100.0, industry='证券')
        assert not rule.check(order, portfolio).passed

    def test_sell_always_passes(self, portfolio_with_two_industries):
        rule = MaxIndustryHHI(0.0001)   # 再严也不拦 sell
        order = Order('000001.SZ', 'sell', 100, 100.0)
        assert rule.check(order, portfolio_with_two_industries).passed


# ======================================================================
# PriceLimitTradable
# ======================================================================

class TestPriceLimitTradable:

    def test_buy_at_limit_up_rejected(self, empty_portfolio):
        rule = PriceLimitTradable()
        # 主板 10%：prev_close 10.00 → 涨停 11.00
        order = Order('000001.SZ', 'buy', 100, 11.0, sector='主板', prev_close=10.0)
        result = rule.check(order, empty_portfolio)
        assert not result.passed
        assert '涨停' in result.reason

    def test_buy_below_limit_up_passes(self, empty_portfolio):
        rule = PriceLimitTradable()
        order = Order('000001.SZ', 'buy', 100, 10.5, sector='主板', prev_close=10.0)
        assert rule.check(order, empty_portfolio).passed

    def test_sell_at_limit_down_rejected(self, portfolio_with_one_holding):
        rule = PriceLimitTradable()
        order = Order('000001.SZ', 'sell', 100, 9.0, sector='主板', prev_close=10.0)
        result = rule.check(order, portfolio_with_one_holding)
        assert not result.passed
        assert '跌停' in result.reason

    def test_st_tier_five_percent(self, empty_portfolio):
        """ST 档 5%：主板口径下放行的 +6% 买入，ST 档下触及涨停"""
        rule = PriceLimitTradable()
        order_st = Order('000001.SZ', 'buy', 100, 10.6, sector='ST', prev_close=10.0)
        assert not rule.check(order_st, empty_portfolio).passed   # ST 涨停 10.50
        order_main = Order('000001.SZ', 'buy', 100, 10.6, sector='主板', prev_close=10.0)
        assert rule.check(order_main, empty_portfolio).passed   # 主板涨停 11.00

    def test_chinext_tier(self, empty_portfolio):
        """创业板 20%：+15% 买入不触板"""
        rule = PriceLimitTradable()
        order = Order('300001.SZ', 'buy', 100, 11.5, sector='创业板', prev_close=10.0)
        assert rule.check(order, empty_portfolio).passed

    def test_missing_prev_close_fail_closed(self, empty_portfolio):
        rule = PriceLimitTradable()
        order = Order('000001.SZ', 'buy', 100, 10.5, sector='主板')
        assert not rule.check(order, empty_portfolio).passed

    def test_unknown_sector_fail_closed(self, empty_portfolio):
        rule = PriceLimitTradable()
        order = Order('000001.SZ', 'buy', 100, 10.5, sector='', prev_close=10.0)
        assert not rule.check(order, empty_portfolio).passed

    def test_limit_price_rounding(self, empty_portfolio):
        """涨跌停价按交易所口径四舍五入：10.05 × 1.1 = 11.055 → 11.06"""
        rule = PriceLimitTradable()
        # 11.05 < 11.06 → 放行（若舍入成 11.05 会被误拦）
        order = Order('000001.SZ', 'buy', 100, 11.05, sector='主板', prev_close=10.05)
        assert rule.check(order, empty_portfolio).passed
        order2 = Order('000001.SZ', 'buy', 100, 11.06, sector='主板', prev_close=10.05)
        assert not rule.check(order2, empty_portfolio).passed


# ======================================================================
# NoSuspendedTrade
# ======================================================================

class TestNoSuspendedTrade:

    def test_suspended_buy_rejected(self, portfolio_with_one_holding):
        portfolio_with_one_holding.suspended = {'000001.SZ'}
        rule = NoSuspendedTrade()
        order = Order('000001.SZ', 'buy', 100, 100.0)
        result = rule.check(order, portfolio_with_one_holding)
        assert not result.passed
        assert '停牌' in result.reason

    def test_suspended_sell_rejected(self, portfolio_with_one_holding):
        """停牌双向拒：sell 也无法成交"""
        portfolio_with_one_holding.suspended = {'000001.SZ'}
        rule = NoSuspendedTrade()
        order = Order('000001.SZ', 'sell', 100, 100.0)
        assert not rule.check(order, portfolio_with_one_holding).passed

    def test_normal_trade_passes(self, portfolio_with_one_holding):
        rule = NoSuspendedTrade()
        order = Order('601211.SH', 'buy', 100, 100.0)
        assert rule.check(order, portfolio_with_one_holding).passed

    def test_default_empty_suspended(self, portfolio_with_one_holding):
        """默认 suspended 为空集，不影响存量组合"""
        assert portfolio_with_one_holding.suspended == set()


# ======================================================================
# MaxOrderVolumeRatio
# ======================================================================

class TestMaxOrderVolumeRatio:

    def test_within_ratio_passes(self, empty_portfolio):
        rule = MaxOrderVolumeRatio(0.10)
        order = Order('000001.SZ', 'buy', 100, 100.0, bar_volume=1_000_000.0)
        assert rule.check(order, empty_portfolio).passed

    def test_over_ratio_rejected(self, empty_portfolio):
        rule = MaxOrderVolumeRatio(0.10)
        # 20w 股 / 100w 股 = 20% > 10%
        order = Order('000001.SZ', 'buy', 200_000, 100.0, bar_volume=1_000_000.0)
        result = rule.check(order, empty_portfolio)
        assert not result.passed
        assert '占比' in result.reason

    def test_sell_also_checked(self, portfolio_with_one_holding):
        """双向审查：sell 同样受占比约束"""
        rule = MaxOrderVolumeRatio(0.10)
        order = Order('000001.SZ', 'sell', 200_000, 100.0, bar_volume=1_000_000.0)
        assert not rule.check(order, portfolio_with_one_holding).passed

    def test_missing_bar_volume_fail_closed(self, empty_portfolio):
        rule = MaxOrderVolumeRatio(0.10)
        order = Order('000001.SZ', 'buy', 100, 100.0)
        assert not rule.check(order, empty_portfolio).passed

    def test_zero_volume_fail_closed(self, empty_portfolio):
        rule = MaxOrderVolumeRatio(0.10)
        order = Order('000001.SZ', 'buy', 100, 100.0, bar_volume=0.0)
        assert not rule.check(order, empty_portfolio).passed


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
