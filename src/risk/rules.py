#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
风控规则与引擎模块

关键设计：
    1. Order 是"候选下单请求"（不是已执行的订单），Rule 要审查的就是它
    2. Portfolio 是轻量 wrapper，包装引擎已有的 self.positions / 现金 / 行业映射，
       不重造组合管理
    3. Rule.check() 只返回 (bool, str)，不做任何下单/调仓副作用
    4. RiskEngine.gate() 输入 Order 列表，输出 (approved, rejected)；
       rejected 里每条带 reason，回测引擎把 rejected 记进日志
    5. 回测引擎和实盘用同一个 RiskEngine，保证闸门一致
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Order —— 待审查的候选下单请求
# ---------------------------------------------------------------------------

@dataclass
class Order:
    """
    风控层审查的候选下单请求（不是已执行的订单）。

    ts_code:   '000001.SZ'
    side:      'buy' | 'sell'
    qty:       股数（整数，A股按 100 股整手）
    price:     计划成交价（开盘价 / 当前价）
    sector:    板块标签（主板 / 创业板 / 科创板 / 北交所 / ST）
    industry:  行业标签（申万一级行业，如"银行"）
    trade_date: 交易日期（回测时的 Bar 日期）
    """
    ts_code: str
    side: str           # 'buy' | 'sell'
    qty: int
    price: float
    sector: str = ''
    industry: str = ''
    trade_date: Optional[date] = None

    @property
    def notional(self) -> float:
        return self.qty * self.price


# ---------------------------------------------------------------------------
# Portfolio —— 轻量组合状态快照
# ---------------------------------------------------------------------------

@dataclass
class Portfolio:
    """
    轻量组合状态，风控规则用它算权重/集中度/回撤。
    由回测引擎 / 实盘引擎在 gate() 调用前构造。

    positions:    {ts_code: qty}
    last_prices:  {ts_code: last_close}   — 用于盯市估值
    cash:         可用现金
    total_value:  总资产（现金 + 持仓盯市）。若 None 则由 positions + last_prices + cash 自动算
    industry_map: {ts_code: industry}     — 行业映射，算行业集中度时用
    equity_curve: [{date, equity}, ...]    — 算回撤用，取尾部 window
    """
    positions: Dict[str, int] = field(default_factory=dict)
    last_prices: Dict[str, float] = field(default_factory=dict)
    cash: float = 0.0
    total_value: Optional[float] = None
    industry_map: Dict[str, str] = field(default_factory=dict)
    equity_curve: List[Dict[str, Any]] = field(default_factory=list)

    # ------------------------------------------------------------------
    # 便捷方法
    # ------------------------------------------------------------------

    def value_of(self, ts_code: str) -> float:
        """某持仓的盯市价值"""
        qty = self.positions.get(ts_code, 0)
        px = self.last_prices.get(ts_code, 0.0)
        return qty * px

    def current_total_value(self) -> float:
        """总资产（自动算，如果没显式给 total_value）"""
        if self.total_value is not None:
            return self.total_value
        pos_value = sum(self.value_of(c) for c in self.positions)
        return self.cash + pos_value

    def industry_weight(self, industry: str) -> float:
        """某行业的权重 = 该行业所有持仓盯市价值 / 总资产"""
        tv = self.current_total_value()
        if tv <= 0:
            return 0.0
        industry_value = 0.0
        for code, ind in self.industry_map.items():
            if ind == industry and self.positions.get(code, 0) > 0:
                industry_value += self.value_of(code)
        return industry_value / tv

    def current_drawdown(self, window: int = 252) -> float:
        """
        当前回撤 = (window 内峰值 - 当前权益) / window 内峰值。
        equity_curve 为空时返回 0.0。
        """
        if not self.equity_curve:
            return 0.0
        tail = self.equity_curve[-window:]
        equities = [e.get('equity', 0) for e in tail]
        peak = max(equities)
        if peak <= 0:
            return 0.0
        current = equities[-1]
        return (peak - current) / peak


# ---------------------------------------------------------------------------
# Rule —— 风控规则基类
# ---------------------------------------------------------------------------

@dataclass
class RuleResult:
    """单条规则的审查结果"""
    passed: bool
    reason: str = ''


class Rule(ABC):
    """风控规则基类。子类实现 check(order, portfolio) -> RuleResult"""

    name: str = 'generic_rule'

    @abstractmethod
    def check(self, order: Order, portfolio: Portfolio) -> RuleResult: ...


# ---------------------------------------------------------------------------
# Concrete Rules
# ---------------------------------------------------------------------------

class MaxPositionWeight(Rule):
    """单票权重上限（默认 10%）。buy 时审查；sell 永远放行。"""

    name = 'max_position_weight'

    def __init__(self, limit: float = 0.10):
        self.limit = limit

    def check(self, order: Order, portfolio: Portfolio) -> RuleResult:
        if order.side != 'buy':
            return RuleResult(True)

        tv = portfolio.current_total_value()
        if tv <= 0:
            return RuleResult(False, '组合总资产为 0，无法买入')

        # 买入后的新权重 = (当前持仓市值 + 本次买入金额) / 总资产
        cur_value = portfolio.value_of(order.ts_code)
        new_weight = (cur_value + order.notional) / tv

        if new_weight > self.limit:
            return RuleResult(
                False,
                f"单票 {order.ts_code} 买入后权重 {new_weight:.2%} > 上限 {self.limit:.2%}"
            )
        return RuleResult(True)


class MaxIndustryWeight(Rule):
    """行业集中度上限（默认 30%）。buy 时审查。"""

    name = 'max_industry_weight'

    def __init__(self, limit: float = 0.30):
        self.limit = limit

    def check(self, order: Order, portfolio: Portfolio) -> RuleResult:
        if order.side != 'buy':
            return RuleResult(True)
        if not order.industry:
            return RuleResult(True)   # 没行业标签就不拦

        tv = portfolio.current_total_value()
        if tv <= 0:
            return RuleResult(False, '组合总资产为 0，无法买入')

        # 当前行业价值
        cur_ind_value = 0.0
        for code, ind in portfolio.industry_map.items():
            if ind == order.industry and portfolio.positions.get(code, 0) > 0:
                cur_ind_value += portfolio.value_of(code)

        new_weight = (cur_ind_value + order.notional) / tv

        if new_weight > self.limit:
            return RuleResult(
                False,
                f"行业 {order.industry} 买入后集中度 {new_weight:.2%} > 上限 {self.limit:.2%}"
            )
        return RuleResult(True)


class MaxDrawdownCircuit(Rule):
    """
    组合回撤熔断：当前回撤 > limit（默认 15%）时停止开新仓（buy 一律拒，sell 放行）。
    sell 放行是为了允许在熔断时主动减仓。
    """

    name = 'max_drawdown_circuit'

    def __init__(self, limit: float = 0.15, window: int = 252):
        self.limit = limit
        self.window = window

    def check(self, order: Order, portfolio: Portfolio) -> RuleResult:
        if order.side != 'buy':
            return RuleResult(True)

        dd = portfolio.current_drawdown(self.window)
        if dd > self.limit:
            return RuleResult(
                False,
                f"组合回撤 {dd:.2%} > 熔断上限 {self.limit:.2%}，停止开新仓"
            )
        return RuleResult(True)


# ---------------------------------------------------------------------------
# RiskEngine —— 规则组合 + 闸门 + rejected 日志
# ---------------------------------------------------------------------------

@dataclass
class GateResult:
    """RiskEngine.gate() 返回值"""
    approved: List[Tuple[Order, str]]          # (order, rule_name_that_approved)
    rejected: List[Tuple[Order, str, str]]     # (order, rule_name, reason)

    @property
    def approved_orders(self) -> List[Order]:
        return [o for o, _ in self.approved]

    @property
    def rejected_orders(self) -> List[Tuple[Order, str, str]]:
        return self.rejected

    @property
    def reject_count(self) -> int:
        return len(self.rejected)

    @property
    def reject_rate(self) -> float:
        total = len(self.approved) + len(self.rejected)
        return len(self.rejected) / total if total > 0 else 0.0


class RiskEngine:
    """
    规则组合引擎。按注册顺序依次审查，第一条不通过即拦截。

    Usage:
        engine = RiskEngine([
            MaxPositionWeight(0.10),
            MaxIndustryWeight(0.30),
            MaxDrawdownCircuit(0.15),
        ])

        result = engine.gate([order1, order2, ...], portfolio)
        for o, reason, rule_name in result.rejected:
            print(f"REJECTED [{rule_name}] {o.ts_code} {o.side}: {reason}")

    回测引擎集成：
        在 inline 下单前：
            order = Order(ts_code=..., side='buy', qty=shares, price=exec_price, ...)
            portfolio = Portfolio(..., equity_curve=equity_curve_so_far)
            gate_result = self.risk_engine.gate([order], portfolio)
            if gate_result.approved:
                # 原有下单逻辑
            else:
                self.rejected_orders.extend(gate_result.rejected)  # 记日志
    """

    def __init__(self, rules: Optional[List[Rule]] = None):
        self.rules: List[Rule] = list(rules or [])

    # ------------------------------------------------------------------
    # 规则管理
    # ------------------------------------------------------------------

    def add_rule(self, rule: Rule):
        self.rules.append(rule)

    def clear_rules(self):
        self.rules.clear()

    # ------------------------------------------------------------------
    # 闸门
    # ------------------------------------------------------------------

    def gate(self, orders: List[Order], portfolio: Portfolio) -> GateResult:
        """
        对一批候选订单逐个审查，返回 (approved, rejected)。

        每条 order 按 self.rules 顺序过筛，第一条不通过即拦截并记录规则名 + 原因。
        无规则时全部放行。
        """
        approved: List[Tuple[Order, str]] = []
        rejected: List[Tuple[Order, str, str]] = []

        for o in orders:
            passed = True
            rule_name = 'no_rules'
            reason = ''

            for rule in self.rules:
                result = rule.check(o, portfolio)
                if not result.passed:
                    passed = False
                    rule_name = rule.name
                    reason = result.reason
                    break
                rule_name = rule.name

            if passed:
                approved.append((o, rule_name))
            else:
                rejected.append((o, rule_name, reason))

        return GateResult(approved=approved, rejected=rejected)
