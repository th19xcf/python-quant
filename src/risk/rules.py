#!/usr/bin/env python3

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

import math
import numbers
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Tuple

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
    trade_date: date | None = None

    def __post_init__(self):
        """输入域校验：非法订单直接拒绝构造，避免绕过权重类规则。"""
        if not isinstance(self.ts_code, str) or not self.ts_code.strip():
            raise ValueError(f'Order.ts_code 必须为非空字符串，实际为 {self.ts_code!r}')
        if self.side not in ('buy', 'sell'):
            raise ValueError(f"Order.side 必须为 'buy' 或 'sell'，实际为 {self.side!r}")
        if (not isinstance(self.qty, numbers.Integral)
                or isinstance(self.qty, bool) or self.qty <= 0):
            raise ValueError(f'Order.qty 必须为正整数，实际为 {self.qty!r}')
        if not isinstance(self.price, numbers.Real) or isinstance(self.price, bool):
            raise TypeError(f'Order.price 必须为实数，实际为 {self.price!r}')
        price = float(self.price)
        if not math.isfinite(price) or price <= 0:
            raise ValueError(f'Order.price 必须为有限正数，实际为 {self.price!r}')

        self.qty = int(self.qty)
        self.price = price

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
    industry_map: {ts_code: industry}     — 行业映射，算行业集中度时用
    equity_curve: [{date, equity}, ...]    — 算回撤用，取尾部 window

    总资产一律由 positions + last_prices + cash 现场计算，不接受外部注入，
    避免过期快照把风控分母带偏。
    """
    positions: Dict[str, int] = field(default_factory=dict)
    last_prices: Dict[str, float] = field(default_factory=dict)
    cash: float = 0.0
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
        """总资产 = 现金 + 全部持仓盯市价值（唯一口径，不接受外部注入）"""
        pos_value = sum(self.value_of(c) for c in self.positions)
        return self.cash + pos_value

    def industry_value(self, industry: str) -> float:
        """某行业的持仓盯市价值。

        遍历 positions 而非 industry_map：认不出的持仓价值不会被静默跳过，
        而是由 unmapped_value() 单独暴露，供规则 fail-closed。
        """
        industry_value = 0.0
        for code, qty in self.positions.items():
            if qty <= 0:
                continue
            if self.industry_map.get(code, '') == industry:
                industry_value += self.value_of(code)
        return industry_value

    def industry_weight(self, industry: str) -> float:
        """某行业的权重 = 该行业持仓市值 / 总资产（分母为全组合总资产）"""
        tv = self.current_total_value()
        if tv <= 0:
            return 0.0
        return self.industry_value(industry) / tv

    def unmapped_value(self) -> float:
        """未映射行业（industry_map 缺失或空标签）的持仓市值。"""
        unmapped = 0.0
        for code, qty in self.positions.items():
            if qty <= 0:
                continue
            if not self.industry_map.get(code, ''):
                unmapped += self.value_of(code)
        return unmapped

    @staticmethod
    def _date_sort_key(entry: Dict[str, Any]) -> str:
        """equity_curve 排序键；date 缺失时统一为空串，保持原有相对顺序。"""
        value = entry.get('date')
        return '' if value is None else str(value)

    def current_drawdown(self, window: int = 252) -> float | None:
        """
        当前回撤 = (window 内峰值 - 当前权益) / window 内峰值。

        equity_curve 乱序时先按日期排序再截尾，避免把早期峰值误当近期峰值。
        无法计算（曲线为空、window 非法、权益缺失/非有限/非正）时返回 None，
        由调用方 fail-closed，不再伪造 0 回撤。
        """
        if window <= 0 or not self.equity_curve:
            return None

        curve = self.equity_curve
        keys = [self._date_sort_key(e) for e in curve]
        if any(keys[i] > keys[i + 1] for i in range(len(keys) - 1)):
            curve = [entry for _, entry in sorted(
                zip(keys, curve, strict=True), key=lambda pair: pair[0])]
        tail = curve[-window:]

        equities: List[float] = []
        for entry in tail:
            equity = entry.get('equity')
            if equity is None or isinstance(equity, bool):
                return None
            try:
                equity = float(equity)
            except (TypeError, ValueError):
                return None
            if not math.isfinite(equity) or equity <= 0:
                return None
            equities.append(equity)

        peak = max(equities)
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
            return RuleResult(
                False,
                f"单票 {order.ts_code} 缺少行业标签，无法评估行业集中度，拒绝开新仓"
            )

        tv = portfolio.current_total_value()
        if tv <= 0:
            return RuleResult(False, '组合总资产为 0，无法买入')

        # 未映射行业的持仓无法归入任何行业分子，若存在则集中度必然被低估
        unmapped = portfolio.unmapped_value()
        if unmapped > 0:
            return RuleResult(
                False,
                f"组合存在未映射行业的持仓（市值 {unmapped:.2f}），行业集中度无法评估，拒绝开新仓"
            )

        cur_ind_value = portfolio.industry_value(order.industry)
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
        if dd is None:
            return RuleResult(
                False,
                '权益曲线缺失或数据非法，无法评估回撤，停止开新仓'
            )
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
    规则组合引擎。按注册顺序依次审查，记录全部违规原因后拦截。

    Usage:
        engine = RiskEngine([
            MaxPositionWeight(0.10),
            MaxIndustryWeight(0.30),
            MaxDrawdownCircuit(0.15),
        ])

        result = engine.gate([order1, order2, ...], portfolio)
        for o, rule_name, reason in result.rejected:
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

    安全契约：未配置任何规则时 gate() 直接抛错（fail-closed），绝不放行。
    """

    def __init__(self, rules: List[Rule] | None = None):
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

        每条 order 按 self.rules 顺序过筛，记录全部违规规则的 name + reason
        （rejected 元组第二项为第一条失败规则名，第三项聚合所有失败原因）。
        无规则时抛 RuntimeError（fail-closed），不允许"无风控即全放行"。
        """
        if not self.rules:
            raise RuntimeError(
                'RiskEngine 未配置任何规则，拒绝在无风控规则时放行订单（fail-closed）'
            )

        approved: List[Tuple[Order, str]] = []
        rejected: List[Tuple[Order, str, str]] = []

        for o in orders:
            violations: List[Tuple[str, str]] = []

            for rule in self.rules:
                result = rule.check(o, portfolio)
                if not result.passed:
                    violations.append((rule.name, result.reason))

            if violations:
                reason = '；'.join(f'[{name}] {msg}' for name, msg in violations)
                rejected.append((o, violations[0][0], reason))
            else:
                approved.append((o, self.rules[-1].name))

        return GateResult(approved=approved, rejected=rejected)
