#!/usr/bin/env python3

"""横截面打分、截面百分位与 top3 因子归因。"""

from __future__ import annotations

import polars as pl

from src.alpha.cross_section.cross_sectional import winsorize
from src.advice.config import FactorSpec, TierConfig

WINSOR_SUFFIX = '__win'
MIN_STD = 1e-9


def _z_expr(column: str) -> pl.Expr:
    """按交易日标准化的稳健 z 值（去极值后截断到 ±3）。"""
    mean = pl.col(column).mean().over('date')
    std = pl.col(column).std().over('date')
    z = pl.when(std.is_not_null() & (std > MIN_STD)).then(
        (pl.col(column) - mean) / std).otherwise(0.0)
    return z.clip(lower_bound=-3.0, upper_bound=3.0).fill_null(0.0)


def _winsor_columns(tier: TierConfig) -> list[str]:
    return [f'{spec.column}{WINSOR_SUFFIX}' for spec in tier.factors]


def attach_score(frame: pl.DataFrame, tier: TierConfig) -> pl.DataFrame:
    """给因子面板追加 ``score`` 与 ``score_percentile``。

    流程：按交易日去极值（1%/99%）→ 标准化 → 按档位权重/方向加权求和
    → 截面百分位（0~1，越高越看多）。不修改入参 DataFrame。
    """
    columns = [spec.column for spec in tier.factors]
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"档位 {tier.key} 缺少因子列: {missing}")

    scored = winsorize(frame, columns, by='date', suffix=WINSOR_SUFFIX)
    score_expr: pl.Expr = pl.lit(0.0)
    for spec in tier.factors:
        score_expr = score_expr + (
            spec.weight * spec.direction * _z_expr(f'{spec.column}{WINSOR_SUFFIX}'))

    scored = scored.with_columns(score_expr.alias('score'))
    scored = scored.with_columns(
        (pl.col('score').rank('average').over('date')
         / pl.len().over('date')).alias('score_percentile'))
    return scored


def factor_stats(scored: pl.DataFrame, tier: TierConfig) -> pl.DataFrame:
    """按交易日汇总去极值后因子的均值/标准差（供 top3 归因）。"""
    columns = _winsor_columns(tier)
    exprs: list[pl.Expr] = []
    for column in columns:
        exprs.append(pl.col(column).mean().alias(f'{column}__mean'))
        exprs.append(pl.col(column).std().alias(f'{column}__std'))
    return scored.group_by('date').agg(exprs).sort('date')


def explain_factors(row: dict, stats: dict | None, tier: TierConfig,
                    top_n: int = 3,
                    prefer_negative: bool = False) -> list[tuple[str, float]]:
    """返回对综合得分贡献最大的 top_n 因子（按贡献绝对值降序）。

    ``row`` 为单行 dict（含去极值后的因子列），``stats`` 为同日均值/标准差。
    贡献 = 权重 × 方向 × z 值；``prefer_negative=True`` 时优先返回负贡献
    （供 SELL/AVOID 解释看空理由）。
    """
    contributions: list[tuple[str, float]] = []
    for spec in tier.factors:
        value = row.get(f'{spec.column}{WINSOR_SUFFIX}')
        if value is None or stats is None:
            continue
        mean = stats.get(f'{spec.column}{WINSOR_SUFFIX}__mean')
        std = stats.get(f'{spec.column}{WINSOR_SUFFIX}__std')
        if mean is None or std is None or std <= MIN_STD:
            continue
        z = max(-3.0, min(3.0, (float(value) - float(mean)) / float(std)))
        contribution = spec.weight * spec.direction * z
        contributions.append((spec.label, contribution))
    contributions.sort(key=lambda item: abs(item[1]), reverse=True)
    if prefer_negative:
        negative = [item for item in contributions if item[1] < 0]
        if negative:
            return negative[:top_n]
    return contributions[:top_n]


def confidence_from_score(score: float, percentile: float) -> float:
    """由 z 强度与截面极端程度推导置信度（0.30~0.95）。

    用 3.5 倍标准差作为满分强度，避免选中标的普遍顶到 0.95 上限，
    让细进度条能反映真实的强弱差异。
    """
    strength = min(1.0, abs(float(score)) / 3.5)
    extreme = min(1.0, abs(float(percentile) - 0.5) * 2.0)
    return round(min(0.95, 0.30 + 0.40 * strength + 0.25 * extreme), 3)


def score_specs(tier: TierConfig) -> tuple[FactorSpec, ...]:
    """档位因子规格（对调用方只读暴露）。"""
    return tier.factors
