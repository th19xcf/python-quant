#!/usr/bin/env python3

"""短线校准脚本的纯逻辑单测：参数组合、选择准则与邻域生成。"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    'calibrate_advice', ROOT / 'scripts' / 'calibrate_advice.py')
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules['calibrate_advice'] = _MODULE
_SPEC.loader.exec_module(_MODULE)

calib = _MODULE


def test_turnover_grid_covers_all_dimensions():
    combos = calib.turnover_combos()
    assert len(combos) == 4 * 3 * 4 * 3
    assert {item['min_hold_days'] for item in combos} == {10, 15, 20, 30}
    assert {item['confirm_days'] for item in combos} == {2, 3, 5}
    assert {item['max_turnover_per_rebalance'] for item in combos} == {
        0.05, 0.10, 0.15, 0.20}
    assert {item['cost_threshold_multiple'] for item in combos} == {1.0, 2.0, 3.0}


def test_apply_combo_overrides_turnover_only():
    short = calib.base_config().tier('short')
    tier, turnover = calib.apply_combo(short, {
        'min_hold_days': 15,
        'confirm_days': 3,
        'max_turnover_per_rebalance': 0.05,
        'cost_threshold_multiple': 2.0,
    })
    assert tier.key == 'short'
    assert tier.min_hold_days == 15
    assert tier.confirm_days == 3
    # 打分因子不受换手参数影响
    assert tier.factors == short.factors
    assert turnover.max_turnover_per_rebalance == 0.05
    # 成本门槛同时作用于卖出与买入侧
    assert turnover.cost_threshold_multiple == 2.0
    assert turnover.min_entry_edge_multiple == 2.0
    # 中线/长线预设必须保持原样
    from src.advice.config import TIER_PRESETS
    config = calib.base_config()
    assert config.tiers['mid'] == TIER_PRESETS['mid']
    assert config.tiers['long'] == TIER_PRESETS['long']


def test_factor_variants_keep_base_and_expose_reversal():
    short = calib.base_config().tier('short')
    variants = calib.factor_variants(short)
    assert variants['base'] == short
    momentum = {spec.column: spec.weight for spec in variants['momentum_strong'].factors}
    assert momentum['momentum_1m'] == 0.45
    assert momentum['rsi_14'] == 0.0
    reversal = {spec.column: spec.direction for spec in variants['reversal'].factors}
    assert reversal['momentum_1m'] == -1.0
    assert reversal['ma20_gap'] == -1.0


def test_neighbourhood_is_plus_minus_twenty_percent():
    params = {
        'min_hold_days': 20,
        'confirm_days': 4,
        'max_turnover_per_rebalance': 0.05,
        'cost_threshold_multiple': 2.0,
    }
    labels = dict(calib.neighbourhood_combos(params))
    assert len(labels) == 8
    assert labels['min_hold -20%']['min_hold_days'] == 16
    assert labels['min_hold +20%']['min_hold_days'] == 24
    assert labels['confirm -20%']['confirm_days'] == 3
    assert labels['confirm +20%']['confirm_days'] == 5
    assert labels['turnover cap -20%']['max_turnover_per_rebalance'] == pytest.approx(0.04)
    assert labels['turnover cap +20%']['max_turnover_per_rebalance'] == pytest.approx(0.06)
    assert labels['cost multiple -20%']['cost_threshold_multiple'] == pytest.approx(1.6)
    assert labels['cost multiple +20%']['cost_threshold_multiple'] == pytest.approx(2.4)


def test_neighbourhood_clamps_min_hold_at_one():
    labels = dict(calib.neighbourhood_combos({
        'min_hold_days': 1,
        'confirm_days': 1,
        'max_turnover_per_rebalance': 0.05,
        'cost_threshold_multiple': 1.0,
    }))
    assert labels['min_hold -20%']['min_hold_days'] == 1
    assert labels['confirm -20%']['confirm_days'] == 1


def test_selection_requires_turnover_and_return_floor():
    good = {'annual_turnover': 3.9, 'annualized_return': -0.04, 'max_drawdown': 0.2}
    too_fast = dict(good, annual_turnover=4.5)
    too_negative = dict(good, annualized_return=-0.06)
    assert calib._eligible(good)
    assert not calib._eligible(too_fast)
    assert not calib._eligible(too_negative)
    assert calib._rank(good) > calib._rank(too_negative)


def test_weight_scale_only_touches_target_factor():
    short = calib.base_config().tier('short')
    scaled = calib._weight_scale(short, 'momentum_1m', 0.8)
    weights = {spec.column: spec.weight for spec in scaled.factors}
    original = {spec.column: spec.weight for spec in short.factors}
    assert weights['momentum_1m'] == original['momentum_1m'] * 0.8
    for column in ('rsi_14', 'atr_pct', 'volume_momentum_20'):
        assert weights[column] == original[column]


def test_segment_and_cache_partition_by_date():
    frame = pl.DataFrame({
        'ts_code': ['A', 'B', 'A', 'B'],
        'date': [date(2025, 1, 2), date(2025, 1, 2),
                 date(2025, 1, 3), date(2025, 1, 3)],
        'score': [0.1, 0.2, 0.3, 0.4],
    })
    trimmed, dates = calib.segment(frame, (date(2025, 1, 3), date(2025, 1, 31)))
    assert trimmed.height == 2
    assert dates == [date(2025, 1, 3)]
    cache = calib.build_cache(frame)
    assert sorted(cache[date(2025, 1, 2)]) == ['A', 'B']
    assert cache[date(2025, 1, 3)]['A']['score'] == 0.3
