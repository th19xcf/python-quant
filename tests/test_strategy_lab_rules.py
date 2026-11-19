"""策略试算规则层：字段白名单、运算符、分位与序列化。"""

from __future__ import annotations

import pytest

from src.quant.strategy_lab.fields import FIELDS, field_catalog
from src.quant.strategy_lab.rules import (
    RuleError,
    dump_rule,
    load_rule,
    rule_text,
    save_rule,
    validate_rule,
)

BASE = {
    'rule_version': 'strategy_lab/v1',
    'rule_type': 'stock_select',
    'name': '测试规则',
    'universe': {'mode': 'all_market'},
    'conditions': [
        {'field': 'vol_60', 'op': '<=', 'operand': {'type': 'quantile', 'q': 0.5}},
    ],
    'sort': {'field': 'ep', 'direction': 'desc'},
    'top_n': 30,
    'rebalance': {'frequency': 'monthly'},
    'period': {'start': '2019-01-04', 'end': '2025-12-31'},
    'benchmark': '000300.SH',
}


def test_valid_rule_normalized():
    rule = validate_rule(BASE)
    assert rule['universe'] == {'mode': 'all_market'}
    assert rule['conditions'][0]['operand'] == {'type': 'quantile', 'q': 0.5}
    assert rule['cost']['slippage_bps'] == 10.0
    assert rule['portfolio']['initial_capital'] == 1e9


def test_unknown_field_rejected():
    raw = {**BASE, 'conditions': [{'field': 'future_eps', 'op': '>',
                                   'operand': {'type': 'value', 'value': 1}}]}
    with pytest.raises(RuleError, match='未开放'):
        validate_rule(raw)


def test_unknown_top_level_key_rejected():
    with pytest.raises(RuleError, match='未知字段'):
        validate_rule({**BASE, 'script': 'import os'})


def test_quantile_with_equality_rejected():
    raw = {**BASE, 'conditions': [{'field': 'ep', 'op': '==',
                                   'operand': {'type': 'quantile', 'q': 0.3}}]}
    with pytest.raises(RuleError, match='不能与分位组合'):
        validate_rule(raw)


def test_between_needs_ordered_bounds():
    raw = {**BASE, 'conditions': [{'field': 'pe', 'op': 'between',
                                   'operand': {'type': 'value', 'low': 50, 'high': 1}}]}
    with pytest.raises(RuleError, match='大于上限'):
        validate_rule(raw)


def test_non_monthly_rebalance_rejected():
    with pytest.raises(RuleError, match='月频'):
        validate_rule({**BASE, 'rebalance': {'frequency': 'weekly'}})


def test_semi_rebalance_accepted():
    rule = validate_rule({**BASE, 'rebalance': {'frequency': 'semi'}})
    assert rule['rebalance'] == {'frequency': 'semi'}
    assert '半年频' in rule_text(rule)


def test_quantile_group_industry_accepted():
    raw = {**BASE, 'conditions': [{'field': 'vq_value', 'op': '>=',
                                   'operand': {'type': 'quantile', 'q': 0.7,
                                               'group': 'industry'}}]}
    rule = validate_rule(raw)
    assert rule['conditions'][0]['operand'] == {'type': 'quantile', 'q': 0.7,
                                                'group': 'industry'}
    assert '行业内' in rule_text(rule)


def test_quantile_group_only_industry():
    raw = {**BASE, 'conditions': [{'field': 'vq_value', 'op': '>=',
                                   'operand': {'type': 'quantile', 'q': 0.7,
                                               'group': 'market'}}]}
    with pytest.raises(RuleError, match='只支持 .industry.'):
        validate_rule(raw)


def test_industry_field_cannot_be_condition_or_sort():
    cond = {**BASE, 'conditions': [{'field': 'industry', 'op': '==',
                                    'operand': {'type': 'value', 'value': 1}}]}
    with pytest.raises(RuleError, match='只用于分组'):
        validate_rule(cond)
    with pytest.raises(RuleError, match='只用于分组'):
        validate_rule({**BASE, 'sort': {'field': 'industry', 'direction': 'asc'}})


def test_sort_normalized_to_stages():
    single = validate_rule(BASE)['sort']
    assert single == [{'field': 'ep', 'direction': 'desc'}]
    raw = {**BASE, 'sort': [
        {'field': 'vq_value', 'direction': 'desc', 'keep_pct': 0.3},
        {'field': 'vq_score', 'direction': 'desc'},
    ]}
    rule = validate_rule(raw)
    assert rule['sort'][0]['keep_pct'] == 0.3
    assert rule['sort'][1] == {'field': 'vq_score', 'direction': 'desc'}
    assert '保留前 30%' in rule_text(rule)


def test_keep_pct_rejected_on_last_stage_and_bounds():
    with pytest.raises(RuleError, match='非末段'):
        validate_rule({**BASE, 'sort': [{'field': 'ep', 'direction': 'desc',
                                         'keep_pct': 0.3}]})
    with pytest.raises(RuleError, match=r'keep_pct'):
        validate_rule({**BASE, 'sort': [
            {'field': 'ep', 'direction': 'desc', 'keep_pct': 1.5},
            {'field': 'pb', 'direction': 'asc'}]})


def test_top_n_bounds():
    with pytest.raises(RuleError, match='top_n'):
        validate_rule({**BASE, 'top_n': 0})
    with pytest.raises(RuleError, match='top_n'):
        validate_rule({**BASE, 'top_n': 501})


def test_rule_text_mentions_labels():
    text = rule_text(validate_rule(BASE))
    assert '全市场洁净集' in text
    assert FIELDS['vol_60'].label in text
    assert FIELDS['ep'].label in text
    assert '月频' in text


def test_save_and_load_roundtrip(tmp_path):
    rule = validate_rule(BASE)
    path = save_rule(rule, tmp_path)
    assert path.name == '测试规则.json'
    assert path.read_bytes() == dump_rule(rule)
    assert load_rule(path) == rule


def test_field_catalog_covers_all_fields():
    groups = field_catalog()
    listed = {f['key'] for g in groups for f in g['fields']}
    assert listed == set(FIELDS)
    assert all(g['fields'] for g in groups)
