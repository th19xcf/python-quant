"""规则 schema 负向用例：类型分派、未知类型、字段越界与示例文件可载入。

这些用例针对「按 ``rule_type`` 分派」的新校验：旧版校验没有类型分派，
未知 ``rule_type`` 会被静默当成选股规则或抛 KeyError，这里的断言在其上必红。
"""

from __future__ import annotations

import pytest

from src.quant.strategy_lab import rules as rules_module
from src.quant.strategy_lab.rules import RuleError, load_rule, validate_rule

SWITCH_BASE = {
    'rule_version': 'strategy_lab/v1',
    'rule_type': 'asset_switch',
    'name': '切换测试',
    'period': {'start': '2021-01-04', 'end': '2024-12-31'},
    'legs': [
        {'code': '510300.SH', 'label': '沪深300ETF'},
        {'code': '588000.SH', 'label': '科创50ETF'},
    ],
    'switch': {'numerator': '510300.SH', 'denominator': '588000.SH',
               'window': 30, 'entry': 0.03, 'exit': 0.03,
               'min_hold_days': 5, 'initial': 'numerator'},
    'positions': {'mode': 'all_in'},
    'execution': {'price': 'next_open', 'delay_days': 1},
}


def test_unknown_rule_type_rejected():
    with pytest.raises(RuleError, match='未知 rule_type'):
        validate_rule({**SWITCH_BASE, 'rule_type': 'rotation'})


def test_period_end_latest_accepted():
    rule = validate_rule({**SWITCH_BASE, 'period': {'start': '2021-01-04',
                                                   'end': rules_module.LATEST_END}})
    assert rule['period']['end'] == 'latest'
    # 原始表达要出现在规则摘要里，界面与 CLI 才看得懂
    assert '最近交易日' in rules_module.rule_text(rule)


def test_period_end_invalid_string_rejected():
    with pytest.raises(RuleError, match='period.end'):
        validate_rule({**SWITCH_BASE, 'period': {'start': '2021-01-04',
                                                 'end': 'tomorrow'}})


def test_missing_rule_type_rejected():
    raw = dict(SWITCH_BASE)
    raw.pop('rule_type')
    with pytest.raises(RuleError, match='缺少 rule_type'):
        validate_rule(raw)


def test_stock_select_only_key_rejected_for_switch():
    # top_n 只属于 stock_select；按类型的字段白名单必须拒绝它
    with pytest.raises(RuleError, match='未知字段'):
        validate_rule({**SWITCH_BASE, 'top_n': 10})


def test_switch_window_type_mismatch_rejected():
    switch = {**SWITCH_BASE['switch'], 'window': '30'}
    with pytest.raises(RuleError, match='switch.window'):
        validate_rule({**SWITCH_BASE, 'switch': switch})


def test_switch_missing_execution_rejected():
    raw = dict(SWITCH_BASE)
    raw.pop('execution')
    with pytest.raises(RuleError, match='缺少必填字段'):
        validate_rule(raw)


def test_switch_legs_code_must_be_referenced():
    switch = {**SWITCH_BASE['switch'], 'numerator': '159915.SZ'}
    with pytest.raises(RuleError, match='不在 legs'):
        validate_rule({**SWITCH_BASE, 'switch': switch})


def test_sample_rule_files_load():
    low_vol = load_rule(rules_module.RULES_DIR / '示例_低估值低波月频.json')
    assert low_vol['rule_type'] == 'stock_select'
    switch = load_rule(rules_module.RULES_DIR / '示例_强弱比双ETF切换.json')
    assert switch['rule_type'] == 'asset_switch'
    assert {leg['code'] for leg in switch['legs']} == {'510300.SH', '588000.SH'}
    assert switch['switch']['window'] == 30
    assert switch['execution'] == {'price': 'next_open', 'delay_days': 1}
