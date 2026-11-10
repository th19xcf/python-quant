#!/usr/bin/env python3
"""配置真源守护：同一语义只允许一处定义，其余位置必须引用它。

每条用例对应报告 5.x 的一处重复定义；断言写法是「改一处 → 这里变红」，
而不是在测试里再维护一份同样的常量。
"""

from __future__ import annotations

import importlib

import pytest

from src.advice import factor_eval, ic_monitor
from src.backtest.costs.cost_model import COST_DEFAULTS
from src.data.quality.tradable_universe import JUMP_THRESHOLD, REASON_TEXT
from src.risk import DEFAULT_RISK_LIMITS
from src.risk.rules import (
    MaxAdvParticipation,
    MaxDrawdownCircuit,
    MaxIndustryHHI,
    MaxIndustryWeight,
    MaxOrderVolumeRatio,
    MaxPositionWeight,
    MinCashReserve,
)

#: 评审过的风控口径锁定值（改它等于改风控结论，必须同步评审记录）。
#: 这是唯一一处口径快照；规则类的默认值不再各自写一份字面量。
AGREED_RISK_LIMITS = {
    'max_position_weight': 0.10,
    'max_industry_weight': 0.30,
    'max_industry_hhi': 0.25,
    'max_drawdown': 0.15,
    'drawdown_window': 252,
    'min_cash_reserve': 0.0,
    'max_order_volume_ratio': 0.10,
    'max_adv_participation': 0.10,
    'adv_window': 20,
}


def test_risk_limits_single_table() -> None:
    """5.2：风控阈值只有 DEFAULT_RISK_LIMITS 一处定义。"""
    assert DEFAULT_RISK_LIMITS == AGREED_RISK_LIMITS


def test_rule_constructors_default_to_the_table() -> None:
    """5.2：规则类不写第二份默认值，全部从同一张表取。"""
    assert MaxPositionWeight().limit == DEFAULT_RISK_LIMITS['max_position_weight']
    assert MaxIndustryWeight().limit == DEFAULT_RISK_LIMITS['max_industry_weight']
    assert MaxIndustryHHI().limit == DEFAULT_RISK_LIMITS['max_industry_hhi']
    assert MaxDrawdownCircuit().limit == DEFAULT_RISK_LIMITS['max_drawdown']
    assert MaxDrawdownCircuit().window == DEFAULT_RISK_LIMITS['drawdown_window']
    assert MinCashReserve().reserve == DEFAULT_RISK_LIMITS['min_cash_reserve']
    assert (MaxOrderVolumeRatio().limit
            == DEFAULT_RISK_LIMITS['max_order_volume_ratio'])
    assert (MaxAdvParticipation().limit
            == DEFAULT_RISK_LIMITS['max_adv_participation'])
    assert MaxAdvParticipation().window == DEFAULT_RISK_LIMITS['adv_window']


def test_rule_constructors_still_accept_explicit_values() -> None:
    """显式传参语义不变：装配层的必要放宽仍然有效。"""
    assert MaxPositionWeight(0.02).limit == 0.02
    assert MaxDrawdownCircuit(0.05, 60).limit == 0.05
    assert MaxDrawdownCircuit(0.05, 60).window == 60
    assert MaxAdvParticipation(0.3, 5).window == 5


def test_min_pairs_single_source() -> None:
    """5.4：单窗口最少有效样本对只在 factor_eval 定义一次。"""
    assert factor_eval.MIN_PAIRS == 30
    assert ic_monitor.MIN_PAIRS is factor_eval.MIN_PAIRS


def test_jump_threshold_texts_follow_the_constant() -> None:
    """5.5：跳变判据与其说明文案必须同源，改阈值文案跟着变。"""
    assert JUMP_THRESHOLD == 0.5
    assert f'{JUMP_THRESHOLD:.0%}' in REASON_TEXT['bad_price_jump']


def test_eval_daily_advice_help_uses_cost_defaults(capsys) -> None:
    """5.6：CLI 帮助里的佣金/滑点说明取自 COST_DEFAULTS，不再手写数字。"""
    module = importlib.import_module('tools.eval_daily_advice')
    with pytest.raises(SystemExit):
        module.main(['--help'])
    out = capsys.readouterr().out
    assert str(COST_DEFAULTS['commission_rate']) in out
    assert str(COST_DEFAULTS['slippage']) in out


def test_trust_benchmark_cost_text_uses_cost_defaults(capsys) -> None:
    """5.6：信任基准里「改后」往返成本必须等于按 COST_DEFAULTS 现算的值。"""
    module = importlib.import_module('tools.backtest_trust_benchmark')
    module.run_advice_cost()
    out = capsys.readouterr().out
    new_round_trip = (2 * COST_DEFAULTS['commission_rate']
                      + COST_DEFAULTS['stamp_tax_rate']
                      + 2 * COST_DEFAULTS['slippage'])
    assert f'= {new_round_trip:.2%}' in out
