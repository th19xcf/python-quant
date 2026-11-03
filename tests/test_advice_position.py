#!/usr/bin/env python3

"""仓位分配：单票上限、行业上限、现金缓冲与整手股数。"""

from __future__ import annotations

import pytest

from src.advice.config import PortfolioConstraints
from src.advice.portfolio import Candidate, allocate


def _candidate(code: str, pct: float, price: float, industry: str) -> Candidate:
    return Candidate(code, pct, price, industry, 'code_prefix_board')


def test_single_and_industry_caps_normalized():
    constraints = PortfolioConstraints(
        max_single_weight=0.08, max_industry_weight=0.25,
        cash_buffer=0.10, lot_size=100)
    candidates = [
        _candidate('A', 0.99, 10.0, '沪主板'),
        _candidate('B', 0.95, 10.0, '沪主板'),
        _candidate('C', 0.93, 10.0, '沪主板'),
        _candidate('D', 0.91, 10.0, '创业板'),
        _candidate('E', 0.90, 10.0, '创业板'),
    ]
    allocations = allocate(candidates, 1_000_000, constraints, max_positions=5)
    total = sum(item.target_weight for item in allocations)
    assert total <= 1.0 - constraints.cash_buffer + 1e-6
    assert all(item.target_weight <= constraints.max_single_weight + 1e-6
               for item in allocations)
    for industry in ('沪主板', '创业板'):
        group = sum(item.target_weight for item in allocations
                    if item.industry == industry)
        assert group <= constraints.max_industry_weight + 1e-6
    assert all(item.shares % constraints.lot_size == 0 for item in allocations)
    assert all(item.shares > 0 for item in allocations)


def test_all_same_industry_respects_group_cap():
    constraints = PortfolioConstraints(
        max_single_weight=0.10, max_industry_weight=0.25, cash_buffer=0.05)
    candidates = [_candidate(str(index), 0.99 - index * 0.01, 10.0, '深主板')
                  for index in range(10)]
    allocations = allocate(candidates, 1_000_000, constraints, max_positions=10)
    total = sum(item.target_weight for item in allocations)
    assert total <= 0.25 + 1e-6


def test_max_positions_and_cash_buffer():
    constraints = PortfolioConstraints(
        max_single_weight=0.10, max_industry_weight=0.30, cash_buffer=0.20)
    candidates = [_candidate(f'C{index}', 0.99 - index * 0.005, 20.0,
                             '沪主板' if index % 2 else '深主板')
                  for index in range(20)]
    allocations = allocate(candidates, 1_000_000, constraints, max_positions=8)
    assert len(allocations) <= 8
    assert sum(item.target_weight for item in allocations) <= 0.80 + 1e-6
    assert all(item.target_weight > 0 for item in allocations)


def test_zero_max_positions_returns_empty():
    """持仓已满（slots=0）时必须返回空分配，不能退化成“不限制”。"""
    constraints = PortfolioConstraints()
    candidates = [_candidate('A', 0.99, 10.0, '沪主板')]
    assert allocate(candidates, 1_000_000, constraints, max_positions=0) == []


def test_zero_equity_returns_empty():
    constraints = PortfolioConstraints()
    assert allocate([_candidate('A', 0.99, 10.0, '沪主板')], 0.0, constraints) == []


def test_price_too_high_for_one_lot_is_dropped():
    constraints = PortfolioConstraints(
        max_single_weight=0.05, cash_buffer=0.10, lot_size=100)
    candidate = _candidate('A', 0.99, 10_000.0, '沪主板')
    assert allocate([candidate], 1_000_000, constraints) == []


def test_weights_are_after_lot_rounding():
    constraints = PortfolioConstraints(
        max_single_weight=0.08, cash_buffer=0.10, lot_size=100)
    candidate = _candidate('A', 0.99, 13.37, '沪主板')
    allocations = allocate([candidate], 1_000_000, constraints)
    assert len(allocations) == 1
    item = allocations[0]
    # 可投资金 90 万先被单票上限 8%（8 万）截断
    expected_shares = int(80_000 / 13.37 / 100) * 100
    assert item.shares == expected_shares
    assert item.target_weight == pytest.approx(expected_shares * 13.37 / 1_000_000)
