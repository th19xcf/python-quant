"""``CostModel`` 费率解析分支的直接单元测试。

``tests/test_cost_impact.py`` 锁的是冲击成本接线，``test_p0_hardening`` 与
``test_execution_core_consistency`` 是从引擎成交记录侧反向验证分档与历史费率；
本文件补的是「直接调用费率解析方法」这一层：印花税时间表边界、过户费
2015-08-01 切换与沪深差异、场内基金豁免、滑点分档的缺失输入兜底、
以及「费率 × 成交额」与「逐笔金额计算」必须相等这条不变量。

这些分支一旦错，回测结论会整体偏移却不会报错，所以逐条钉死具体数值。
"""

from datetime import date, datetime

import pytest

from src.backtest.costs.cost_model import (
    COST_DEFAULTS,
    ETF_SLIPPAGE_TIERS,
    STOCK_SLIPPAGE_TIERS_YI,
    TRANSFER_FEE_RATE_SH_LEGACY,
    TRANSFER_FEE_RATE_UNIFIED,
    CostModel,
)


@pytest.fixture()
def model() -> CostModel:
    """默认参数模型。冲击项不参与：本文件的调用都不传 adv_amount。"""
    return CostModel()


class TestParamsIsolation:
    """overrides 只能改实例，绝不能污染全局单一真源 COST_DEFAULTS"""

    def test_overrides_do_not_mutate_global_defaults(self):
        CostModel({'slippage': 0.9, 'commission_rate': 0.9})
        assert COST_DEFAULTS['slippage'] == 0.0002
        assert COST_DEFAULTS['commission_rate'] == 0.0005

    def test_set_params_updates_instance_only(self):
        model = CostModel()
        before = COST_DEFAULTS['min_commission']
        model.set_params({'min_commission': 1.0})
        assert model.params['min_commission'] == 1.0
        assert COST_DEFAULTS['min_commission'] == before


class TestIsShanghai:
    """沪市判定：后缀优先，无后缀时按 6/9 开头"""

    @pytest.mark.parametrize('ts_code,expected', [
        ('600000.SH', True),
        ('510300.SH', True),
        ('000001.SZ', False),
        ('159915.SZ', False),
        ('430047.BJ', False),
        ('600000', True),      # 无后缀，6 开头
        ('900901', True),      # 无后缀，9 开头（B 股）
        ('000001', False),     # 无后缀，0 开头
        ('', False),
    ])
    def test_matrix(self, ts_code, expected):
        assert CostModel._is_shanghai(ts_code) is expected


class TestStampTaxSchedule:
    """印花税时间表：按生效日倒序取第一条命中，方向不符即为 0"""

    @pytest.mark.parametrize('trade_date,side,expected', [
        (date(2023, 9, 1), 'sell', 0.0005),
        (date(2023, 9, 1), 'buy', 0.0),        # 2008-09-19 起卖方单边
        (date(2010, 6, 1), 'sell', 0.001),
        (date(2010, 6, 1), 'buy', 0.0),
        (date(2007, 6, 1), 'buy', 0.003),      # 2007-05-30 起 0.3% 双边
        (date(2007, 6, 1), 'sell', 0.003),
        (date(2005, 6, 1), 'sell', 0.001),     # 2005-01-24 起 0.1% 双边
        (date(2002, 6, 1), 'buy', 0.002),      # 2001-11-16 起 0.2% 双边
        (date(1999, 1, 1), 'sell', 0.004),     # 1998-06-12 起 0.4% 双边
        (date(1997, 1, 1), 'sell', 0.0),       # 早于时间表首条 → 0
    ])
    def test_schedule(self, model, trade_date, side, expected):
        assert model._stamp_tax_rate(trade_date, side, '600000.SH') == pytest.approx(expected)

    def test_fund_exempt_both_sides(self, model):
        assert model._stamp_tax_rate(date(2010, 6, 1), 'sell', '510300.SH') == 0.0
        assert model._stamp_tax_rate(date(2010, 6, 1), 'buy', '510300.SH') == 0.0

    def test_missing_date_falls_back_to_static_params(self, model):
        """不传日期时用现行静态口径：卖出取 stamp_tax_rate，买入为 0"""
        assert model._stamp_tax_rate(None, 'sell', '600000.SH') == pytest.approx(
            COST_DEFAULTS['stamp_tax_rate'])
        assert model._stamp_tax_rate(None, 'buy', '600000.SH') == 0.0

    def test_datetime_input_is_accepted(self, model):
        assert model._stamp_tax_rate(
            datetime(2010, 6, 1, 15, 0), 'sell', '600000.SH') == pytest.approx(0.001)


class TestTransferFeeSchedule:
    """过户费：2015-08-01 起沪深统一 0.002%，此前仅沪市 0.06%，基金豁免"""

    def test_unified_after_switch(self, model):
        for code in ('600000.SH', '000001.SZ'):
            assert model._transfer_fee_rate(date(2015, 8, 1), code) == pytest.approx(
                TRANSFER_FEE_RATE_UNIFIED)
            assert model._transfer_fee_rate(date(2023, 9, 1), code) == pytest.approx(
                TRANSFER_FEE_RATE_UNIFIED)

    def test_legacy_shanghai_only(self, model):
        assert model._transfer_fee_rate(date(2010, 6, 1), '600000.SH') == pytest.approx(
            TRANSFER_FEE_RATE_SH_LEGACY)
        assert model._transfer_fee_rate(date(2010, 6, 1), '000001.SZ') == 0.0

    def test_fund_exempt(self, model):
        assert model._transfer_fee_rate(date(2010, 6, 1), '510300.SH') == 0.0
        assert model._transfer_fee_rate(date(2023, 9, 1), '159915.SZ') == 0.0

    def test_missing_date_uses_static_params(self, model):
        assert model._transfer_fee_rate(None, '600000.SH') == pytest.approx(
            COST_DEFAULTS['transfer_fee_rate'])


class TestSlippageTiers:
    """滑点分档：ETF 按 ADV、个股按总市值，缺输入各自兜底"""

    def test_etf_missing_adv_falls_back_to_default_slippage(self, model):
        for adv20 in (None, 0.0, -1.0):
            assert model.slippage_rate('510300.SH', adv20=adv20) == pytest.approx(
                COST_DEFAULTS['slippage'])

    @pytest.mark.parametrize('adv20,expected', [
        (2.0e8, ETF_SLIPPAGE_TIERS[0][1]),
        (6.0e7, ETF_SLIPPAGE_TIERS[1][1]),
        (1.0e7, ETF_SLIPPAGE_TIERS[2][1]),
    ])
    def test_etf_tiers(self, model, adv20, expected):
        assert model.slippage_rate('510300.SH', adv20=adv20) == pytest.approx(expected)

    def test_etf_sector_bumps_one_tier_and_caps_at_last(self, model):
        """行业/主题在同一 ADV 档上取相邻更高一档；已是末档时保持末档"""
        assert model.slippage_rate('515000.SH', adv20=2.0e8, category='sector') == pytest.approx(
            ETF_SLIPPAGE_TIERS[1][1])
        assert model.slippage_rate('515000.SH', adv20=1.0e7, category='Themes') == pytest.approx(
            ETF_SLIPPAGE_TIERS[-1][1])

    @pytest.mark.parametrize('total_mv_yi,expected', [
        (600.0, STOCK_SLIPPAGE_TIERS_YI[0][1]),
        (200.0, STOCK_SLIPPAGE_TIERS_YI[1][1]),
        (50.0, STOCK_SLIPPAGE_TIERS_YI[2][1]),
        (None, STOCK_SLIPPAGE_TIERS_YI[-1][1]),
        (0.0, STOCK_SLIPPAGE_TIERS_YI[-1][1]),
    ])
    def test_stock_tiers(self, model, total_mv_yi, expected):
        assert model.slippage_rate('600000.SH', total_mv_yi=total_mv_yi) == pytest.approx(expected)


class TestImpactRate:
    """冲击项：系数或参与率缺失/非正时必须是精确 0.0，不抛错"""

    def test_zero_coefficient_is_exact_zero(self):
        assert CostModel({'impact_coef': 0.0}).impact_rate(0.5) == 0.0

    def test_missing_participation_returns_zero(self):
        assert CostModel({'impact_coef': 0.5}).impact_rate(None) == 0.0

    def test_non_positive_participation_returns_zero(self):
        assert CostModel({'impact_coef': 0.5}).impact_rate(0.0) == 0.0

    def test_proportional_to_sqrt_participation(self):
        assert CostModel({'impact_coef': 0.5}).impact_rate(4.0) == pytest.approx(1.0)

    def test_participation_requires_positive_adv(self):
        assert CostModel._participation(1000.0, None) is None
        assert CostModel._participation(1000.0, 0.0) is None
        assert CostModel._participation(1000.0, 2000.0) == pytest.approx(0.5)


class TestCostComposition:
    """成本构成与费率函数的互证：两套入口必须给出同一个数"""

    def test_calculate_cost_equals_buy_plus_sell(self, model):
        buy = model.calculate_buy_cost(10_000, 10.0, date(2023, 9, 1), '600000.SH',
                                       total_mv_yi=600.0)
        sell = model.calculate_sell_cost(10_000, 11.0, date(2023, 9, 1), '600000.SH',
                                         total_mv_yi=600.0)
        assert model.calculate_cost(10_000, 10.0, 11.0, date(2023, 9, 1), '600000.SH',
                                    total_mv_yi=600.0) == pytest.approx(buy + sell)

    def test_buy_cost_matches_rate_times_amount(self, model):
        """成交额大到佣金越过最低值后，逐笔计算应等于「费率 × 成交额」"""
        amount = 10_000 * 10.0
        rate = model.get_buy_cost_rate(date(2023, 9, 1), '600000.SH', total_mv_yi=600.0)
        assert model.calculate_buy_cost(10_000, 10.0, date(2023, 9, 1), '600000.SH',
                                        total_mv_yi=600.0) == pytest.approx(amount * rate)

    def test_sell_cost_matches_rate_times_amount(self, model):
        amount = 10_000 * 10.0
        rate = model.get_sell_cost_rate(date(2010, 6, 1), '600000.SH', total_mv_yi=600.0)
        assert model.calculate_sell_cost(10_000, 10.0, date(2010, 6, 1), '600000.SH',
                                         total_mv_yi=600.0) == pytest.approx(amount * rate)

    def test_min_commission_floor_applies_to_small_trades(self, model):
        """小额成交按最低佣金收取，此时「费率 × 成交额」必然偏低"""
        small = model.calculate_buy_cost(100, 10.0, date(2023, 9, 1), '600000.SH',
                                         total_mv_yi=600.0)
        assert small >= COST_DEFAULTS['min_commission']
