#!/usr/bin/env python3
"""生产新鲜度配置（``src/data/quality/freshness_config.json``）的守护用例。

本文件只读真实配置文件，不再把阈值硬编码一遍：断言的是「默认值生效、
覆盖值生效、覆盖必须写理由、不得把默认值抄进条目」这四条结构约束，
阈值本身从配置里读出来对比。
"""

from __future__ import annotations

import json
import shutil

import pytest

from src.data.quality.freshness import (
    _DEFAULT_CONFIG,
    load_config,
    parse_config,
)

#: 生产配置的条目快照（防止误删条目；新增数据集时同步更新本表）
EXPECTED_KEYS = (
    'quotes_stock', 'quotes_index', 'quotes_fund', 'valuation_daily',
    'financials_income', 'financials_balance', 'financials_cashflow',
    'events', 'names_json', 'stock_basic', 'stock_turnover',
    'stock_adj_factor', 'stock_dividend', 'fund_dividend',
    'fund_share_change', 'stock_shares', 'breadth_daily',
    'st_status', 'industry',
)

THRESHOLD_FIELDS = ('min_ratio', 'critical_ratio')

#: 生产门禁口径契约：默认要求 95% 覆盖、80% 熔断。
#: 这是评审过的口径锁定值（改它等于改门禁结论，必须同步评审记录），
#: 不是「把各条目里的阈值再抄一遍」——条目里已不再出现这两个数。
AGREED_DEFAULT = {'min_ratio': 0.95, 'critical_ratio': 0.80}


def _raw() -> dict:
    return load_config(_DEFAULT_CONFIG)


def _raw_datasets() -> dict[str, dict]:
    return {ds['key']: ds for ds in _raw()['datasets']}


def test_prod_config_keeps_all_datasets_and_declares_defaults() -> None:
    raw = _raw()
    assert tuple(ds['key'] for ds in raw['datasets']) == EXPECTED_KEYS
    coverage_defaults = raw['defaults']['coverage']
    for field in THRESHOLD_FIELDS:
        assert isinstance(coverage_defaults[field], (int, float)), field
    # 默认值是覆盖门禁的唯一真源，必须落在 0~1 且 critical 不高于 min
    assert 0 < coverage_defaults['critical_ratio'] <= coverage_defaults['min_ratio'] <= 1


def test_default_thresholds_match_agreed_contract() -> None:
    """默认阈值被无意改动时立刻变红（唯一一处口径锁定值）。"""
    defaults = _raw()['defaults']['coverage']
    assert {field: defaults[field] for field in THRESHOLD_FIELDS} == AGREED_DEFAULT


def test_observation_only_entries_are_not_gated() -> None:
    """显式 ``gate: false`` 的条目不能被默认阈值偷偷变成门禁（语义保持不变）。"""
    raw = _raw()
    specs = {spec.key: spec for spec in parse_config(raw)}
    observation_only = [
        ds['key'] for ds in raw['datasets']
        if (ds.get('coverage') or {}).get('gate') is False
    ]
    assert observation_only, '应存在只观测覆盖数、不设门禁的条目'
    for key in observation_only:
        coverage = specs[key].coverage
        assert 'min_ratio' not in coverage, key
        assert 'critical_ratio' not in coverage, key
        assert (coverage.get('why') or '').strip(), key


def test_every_coverage_entry_resolves_both_thresholds() -> None:
    """解析后所有启用覆盖门禁的条目都必须拿到两个阈值（来自条目或默认段）。"""
    specs = parse_config(_raw())
    assert specs
    for spec in specs:
        if not spec.coverage or spec.coverage.get('gate') is False:
            continue
        for field in THRESHOLD_FIELDS:
            assert spec.coverage.get(field) is not None, f'{spec.key} 缺 {field}'


def test_entries_without_thresholds_inherit_defaults() -> None:
    """条目没写阈值时必须原样取 defaults.coverage，而非代码里的第二份字面量。"""
    raw = _raw()
    default_cov = raw['defaults']['coverage']
    specs = {spec.key: spec for spec in parse_config(raw)}
    inherited = 0
    for ds in raw['datasets']:
        coverage = ds.get('coverage') or {}
        if not coverage or coverage.get('gate') is False:
            continue
        for field in THRESHOLD_FIELDS:
            if field in coverage:
                continue
            inherited += 1
            assert specs[ds['key']].coverage[field] == default_cov[field]
    assert inherited > 0, '生产配置里应存在「未写阈值、取默认」的条目'


def test_no_entry_copies_the_default_threshold() -> None:
    """条目不得把默认阈值抄一遍：改默认时这些副本不会跟着变，属静默漂移。"""
    raw = _raw()
    default_cov = raw['defaults']['coverage']
    for ds in raw['datasets']:
        coverage = ds.get('coverage') or {}
        for field in THRESHOLD_FIELDS:
            if field not in coverage:
                continue
            assert coverage[field] != default_cov[field], (
                f'{ds["key"]}.coverage.{field} 与默认值相同，应删除该字段改取默认')


def test_overrides_are_documented() -> None:
    """与默认不同的阈值必须在条目内写明 why 或 note。"""
    raw = _raw()
    default_cov = raw['defaults']['coverage']
    for ds in raw['datasets']:
        coverage = ds.get('coverage') or {}
        if not coverage:
            continue
        differs = any(
            field in coverage and coverage[field] != default_cov[field]
            for field in THRESHOLD_FIELDS
        )
        if differs:
            assert (coverage.get('why') or coverage.get('note') or '').strip(), (
                f'{ds["key"]} 使用了非默认阈值但未说明理由')


def test_defaults_change_propagates_to_entries(tmp_path) -> None:
    """把默认阈值改掉后，未写阈值的条目必须跟着变（单点生效的正面证明）。"""
    path = tmp_path / 'freshness_config.json'
    shutil.copy2(_DEFAULT_CONFIG, path)
    raw = json.loads(path.read_text(encoding='utf-8'))
    target = next(ds['key'] for ds in raw['datasets']
                  if (ds.get('coverage') or {}) and 'min_ratio' not in ds['coverage'])
    raw['defaults']['coverage']['min_ratio'] = 0.5
    path.write_text(json.dumps(raw, ensure_ascii=False), encoding='utf-8')

    specs = {spec.key: spec for spec in parse_config(load_config(path))}
    assert specs[target].coverage['min_ratio'] == 0.5


def test_override_without_reason_is_rejected(tmp_path) -> None:
    """非默认阈值且没写理由 → 解析直接报错，而不是静默放行。"""
    entry = {
        'key': 'demo',
        'label': 'demo',
        'category': '测试',
        'freshness_field': 'trade_date',
        'source': {'kind': 'parquet_max', 'path': 'x.parquet', 'column': 'trade_date'},
        'cadence': {
            'unit': 'trading_day', 'expected_interval': 1,
            'warn_multiple': 1, 'critical_multiple': 3,
        },
        'coverage': {
            'count': {'kind': 'parquet_distinct', 'path': 'x.parquet', 'column': 'ts_code'},
            'expected': {'kind': 'fixed', 'value': 100},
            'min_ratio': 0.5,
        },
    }
    config = {
        'defaults': {'holidays': [], 'coverage': {'min_ratio': 0.95, 'critical_ratio': 0.8}},
        'datasets': [entry],
    }
    with pytest.raises(ValueError, match='coverage.why'):
        parse_config(config)

    entry['coverage']['why'] = '演示条目：事件类数据天然稀疏'
    specs = parse_config(config)
    assert specs[0].coverage['min_ratio'] == 0.5
    assert specs[0].coverage['critical_ratio'] == 0.8
