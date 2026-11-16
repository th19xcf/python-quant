"""组合仓位与风控：目标仓位映射、波动率目标、回撤制动、风险预算与约束校验。

口径见 ``docs/组合仓位风控与归因方法_2026-11-16.md``。本模块只做纯计算：
输入市场趋势总分、基准指数收盘序列、目标持仓权重，输出建议仓位、三个环节的
调整量、风险预算读数与约束校验结果；本地装配与 CLI 在
``portfolio_risk_local.py``。

统一约定：

- 仓位一律用「占可用资金的比例」表示，不做杠杆，缩放系数上限 1.00；
- 已实现波动率用 20 日对数收益标准差年化（252 交易日）；
- 回撤一律是「指数收盘相对近 250 个交易日高点」的跌幅，负值表示回撤；
- 任一输入缺失只影响对应环节，缺失环节不调整、不插值、不外推。
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from typing import Mapping, Sequence

#: 年化目标波动率（仓位缩放的分母）
TARGET_VOL = 0.15
#: 已实现波动率的滚动窗口（交易日）
VOL_WINDOW = 20
#: 交易日常数年化系数
TRADING_DAYS = 252
#: 回撤制动的观察窗口（交易日）：自窗口内高点起算
DRAWDOWN_WINDOW = 250
#: 回撤制动：自高点回撤达到该幅度开始减半
DRAWDOWN_HALF = -0.10
#: 回撤制动：自高点回撤达到该幅度清仓
DRAWDOWN_ZERO = -0.15
#: 约束上限
MAX_INDUSTRY_WEIGHT = 0.25
MAX_NAME_WEIGHT = 0.08
MAX_ONE_WAY_TURNOVER = 0.20


@dataclass(frozen=True)
class PositionDecision:
    """目标仓位的三段结果与触发依据。"""

    trend_score: float
    trend_label: str
    tier_position: float
    tier_label: str
    realized_vol: float | None
    vol_scale: float
    position_after_vol: float
    drawdown: float | None
    drawdown_factor: float
    target_position: float
    steps: tuple[str, ...]


@dataclass(frozen=True)
class RiskBudget:
    """风险预算读数。"""

    target_vol: float
    realized_vol: float | None
    vol_gap: float | None
    drawdown: float | None
    drawdown_distance: float | None
    risk_exposure: float | None


@dataclass(frozen=True)
class ConstraintItem:
    """一条约束的实测值与判定。"""

    name: str
    limit: float
    actual: float | None
    passed: bool
    detail: str


@dataclass(frozen=True)
class ConstraintReport:
    """约束校验汇总。"""

    asof: str | None
    source: str
    generated: bool
    items: tuple[ConstraintItem, ...]
    notes: tuple[str, ...]


@dataclass(frozen=True)
class PortfolioRiskState:
    """仓位与风控页面的完整结果。"""

    asof: date | None
    decision: PositionDecision
    budget: RiskBudget
    constraints: ConstraintReport
    notes: tuple[str, ...]


def baseline_tier(trend_score: float) -> tuple[float, str]:
    """市场趋势总分 → 基础仓位档位（0 / 30% / 60% / 100%）。"""
    if trend_score >= 0.40:
        return 1.00, '强多（≥+0.40）'
    if trend_score >= 0.15:
        return 0.60, '偏多（+0.15~+0.40）'
    if trend_score > -0.15:
        return 0.30, '中性（−0.15~+0.15）'
    return 0.00, '偏空（≤−0.15）'


def realized_volatility(closes: Sequence[float], window: int = VOL_WINDOW) -> float | None:
    """最近 ``window`` 个交易日的对数收益标准差年化；样本不足返回 None。"""
    values = [float(item) for item in closes if item is not None and item > 0]
    if len(values) < window + 1:
        return None
    recent = values[-(window + 1):]
    returns = [math.log(recent[i] / recent[i - 1]) for i in range(1, len(recent))]
    if len(returns) < 2:
        return None
    mean = sum(returns) / len(returns)
    variance = sum((item - mean) ** 2 for item in returns) / (len(returns) - 1)
    return math.sqrt(variance) * math.sqrt(TRADING_DAYS)


def drawdown_from_peak(closes: Sequence[float],
                       window: int = DRAWDOWN_WINDOW) -> float | None:
    """最新收盘相对近 ``window`` 个交易日高点的回撤（负值）；空序列返回 None。"""
    values = [float(item) for item in closes if item is not None and item > 0]
    if not values:
        return None
    recent = values[-window:] if window > 0 else values
    peak = max(recent)
    return values[-1] / peak - 1.0


def vol_scale(realized_vol: float | None, target_vol: float = TARGET_VOL) -> float:
    """波动率目标缩放：目标波动 / 已实现波动，上限 1.00（不加杠杆）。"""
    if realized_vol is None or realized_vol <= 0:
        return 1.0
    return min(1.0, target_vol / realized_vol)


def drawdown_factor(drawdown: float | None) -> float:
    """回撤制动系数：−10% 起线性降到 −15% 归零，之外保持 1.00。"""
    if drawdown is None:
        return 1.0
    if drawdown >= DRAWDOWN_HALF:
        return 1.0
    if drawdown <= DRAWDOWN_ZERO:
        return 0.0
    span = DRAWDOWN_HALF - DRAWDOWN_ZERO
    return (drawdown - DRAWDOWN_ZERO) / span


def evaluate_position(
    trend_score: float,
    trend_label: str,
    realized_vol: float | None,
    drawdown: float | None,
) -> PositionDecision:
    """三段式目标仓位：档位 → 波动率缩放 → 回撤制动。"""
    tier_position, tier_label = baseline_tier(trend_score)
    scale = vol_scale(realized_vol)
    after_vol = tier_position * scale
    factor = drawdown_factor(drawdown)
    target = after_vol * factor

    steps = [
        f'档位：趋势总分 {trend_score:+.2f}（{trend_label}）→ {tier_position:.0%}',
    ]
    if realized_vol is None:
        steps.append('波动率：样本不足 20 日，本环节不调整')
    else:
        steps.append(
            f'波动率：已实现 {realized_vol:.2%} / 目标 {TARGET_VOL:.0%} → 系数 {scale:.2f}'
            f'，仓位 {tier_position:.0%} → {after_vol:.0%}')
    if drawdown is None:
        steps.append('回撤：无指数序列，本环节不调整')
    else:
        steps.append(
            f'回撤：自高点 {drawdown:.2%} → 系数 {factor:.2f}'
            f'，仓位 {after_vol:.0%} → {target:.0%}')
    return PositionDecision(
        trend_score=trend_score,
        trend_label=trend_label,
        tier_position=tier_position,
        tier_label=tier_label,
        realized_vol=realized_vol,
        vol_scale=scale,
        position_after_vol=after_vol,
        drawdown=drawdown,
        drawdown_factor=factor,
        target_position=target,
        steps=tuple(steps),
    )


def evaluate_budget(
    realized_vol: float | None,
    drawdown: float | None,
    target_position: float,
    target_vol: float = TARGET_VOL,
) -> RiskBudget:
    """风险预算：波动缺口、距制动距离与年化风险敞口。"""
    gap = None if realized_vol is None else realized_vol - target_vol
    distance = None if drawdown is None else drawdown - DRAWDOWN_HALF
    exposure = None if realized_vol is None else target_position * realized_vol
    return RiskBudget(
        target_vol=target_vol,
        realized_vol=realized_vol,
        vol_gap=gap,
        drawdown=drawdown,
        drawdown_distance=distance,
        risk_exposure=exposure,
    )


def check_constraints(
    weights: Mapping[str, float] | None,
    industry_of: Mapping[str, str] | None,
    one_way_turnover: float | None,
    *,
    asof: str | None = None,
    source: str = '',
) -> ConstraintReport:
    """校验单行业 ≤25%、单标的 ≤8%、单边换手 ≤20%。"""
    if not weights:
        return ConstraintReport(
            asof=None, source=source, generated=False, items=(),
            notes=('未找到目标持仓文件（reports/target_holdings/），本次不做约束校验。',),
        )

    total = sum(abs(item) for item in weights.values())
    # 权重合计超过 1 才归一（清仓/部分快照保持原值，避免把半仓放大成全仓）
    normalized = (
        {code: abs(w) / total for code, w in weights.items()} if total > 1.0 else
        {code: abs(w) for code, w in weights.items()}
    )
    items = [
        _industry_item(normalized, industry_of or {}),
        _name_item(normalized),
        _turnover_item(one_way_turnover),
    ]
    notes = ['权重按持仓文件自身口径归一后校验。']
    if not industry_of:
        notes.append('行业归属缺失（申万 PIT 表未命中），单行业约束记为未校验。')
    return ConstraintReport(
        asof=asof, source=source, generated=True, items=tuple(items), notes=tuple(notes),
    )


def _industry_item(weights: Mapping[str, float], industry_of: Mapping[str, str]) -> ConstraintItem:
    if not industry_of:
        return ConstraintItem('单行业权重', MAX_INDUSTRY_WEIGHT, None, True,
                              '未校验：缺行业归属')
    buckets: dict[str, float] = {}
    unknown = 0.0
    for code, weight in weights.items():
        industry = industry_of.get(code)
        if not industry:
            unknown += weight
            continue
        buckets[industry] = buckets.get(industry, 0.0) + weight
    if not buckets:
        return ConstraintItem('单行业权重', MAX_INDUSTRY_WEIGHT, None, True,
                              '未校验：行业归属全部缺失')
    worst = max(buckets.items(), key=lambda item: item[1])
    detail = f'最高行业 {worst[0]} {worst[1]:.2%}'
    if unknown > 0:
        detail += f'｜未归类 {unknown:.2%}'
    return ConstraintItem('单行业权重', MAX_INDUSTRY_WEIGHT, worst[1],
                          worst[1] <= MAX_INDUSTRY_WEIGHT, detail)


def _name_item(weights: Mapping[str, float]) -> ConstraintItem:
    worst = max(weights.items(), key=lambda item: item[1])
    return ConstraintItem('单标的权重', MAX_NAME_WEIGHT, worst[1],
                          worst[1] <= MAX_NAME_WEIGHT,
                          f'最高标的 {worst[0]} {worst[1]:.2%}')


def _turnover_item(one_way_turnover: float | None) -> ConstraintItem:
    if one_way_turnover is None:
        return ConstraintItem('单边换手', MAX_ONE_WAY_TURNOVER, None, True,
                              '未校验：台账缺同交易日变动金额')
    return ConstraintItem('单边换手', MAX_ONE_WAY_TURNOVER, one_way_turnover,
                          one_way_turnover <= MAX_ONE_WAY_TURNOVER,
                          f'当日调仓单边换手 {one_way_turnover:.2%}')


def evaluate_portfolio_risk(
    trend_score: float,
    trend_label: str,
    index_closes: Sequence[float],
    *,
    asof: date | None = None,
    weights: Mapping[str, float] | None = None,
    industry_of: Mapping[str, str] | None = None,
    one_way_turnover: float | None = None,
    holdings_asof: str | None = None,
    holdings_source: str = '',
    notes: Sequence[str] = (),
) -> PortfolioRiskState:
    """一次算完仓位、风险预算与约束校验。"""
    vol = realized_volatility(index_closes)
    drawdown = drawdown_from_peak(index_closes)
    decision = evaluate_position(trend_score, trend_label, vol, drawdown)
    budget = evaluate_budget(vol, drawdown, decision.target_position)
    constraints = check_constraints(
        weights, industry_of, one_way_turnover,
        asof=holdings_asof, source=holdings_source,
    )
    return PortfolioRiskState(
        asof=asof,
        decision=decision,
        budget=budget,
        constraints=constraints,
        notes=tuple(notes),
    )
