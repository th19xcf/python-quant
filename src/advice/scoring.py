#!/usr/bin/env python3

"""横截面打分、截面百分位与 top3 因子归因。"""

from __future__ import annotations

import polars as pl

from src.advice.config import TierConfig
from src.alpha.cross_section.cross_sectional import winsorize

WINSOR_SUFFIX = '__win'
MIN_STD = 1e-9


def _z_expr(column: str) -> pl.Expr:
    """按交易日标准化的稳健 z 值（去极值后截断到 ±3）。

    缺失保持 null，不填 0：填 0 会把「没有数据」变成截面里的极端值
    （例如波动率缺失填 0 后成为最低波动，direction=-1 反而拿到最高分）。
    """
    mean = pl.col(column).mean().over('date')
    std = pl.col(column).std().over('date')
    z = pl.when(std.is_not_null() & (std > MIN_STD)).then(
        (pl.col(column) - mean) / std).otherwise(None)
    return z.clip(lower_bound=-3.0, upper_bound=3.0)


def _winsor_columns(tier: TierConfig) -> list[str]:
    return [f'{spec.column}{WINSOR_SUFFIX}' for spec in tier.factors]


def attach_score(frame: pl.DataFrame, tier: TierConfig, *,
                 min_history_rows: int = 0) -> pl.DataFrame:
    """给因子面板追加 ``score`` 与 ``score_percentile``。

    流程：按交易日去极值（1%/99%）→ 标准化 → 按档位权重/方向加权求和
    → 截面百分位（0~1，越高越看多）。不修改入参 DataFrame。

    缺失一律走 reweight：
    - 缺失不填 0、不填均值；该票在该因子的权重从分母剔除，用剩余因子归一；
    - 历史不足 ``min_history_rows`` 或停牌（volume<=0）的票，因子先置空；
    - 全部因子缺失时 score 为 null，不参与候选。
    """
    columns = [spec.column for spec in tier.factors]
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"档位 {tier.key} 缺少因子列: {missing}")

    valid = pl.lit(True)
    if 'history_rows' in frame.columns and min_history_rows > 0:
        valid = valid & (pl.col('history_rows') >= min_history_rows)
    if 'volume' in frame.columns:
        valid = valid & (pl.col('volume') > 0)
    working = frame.with_columns([
        pl.when(valid).then(pl.col(column).cast(pl.Float64, strict=False))
        .otherwise(None).alias(column)
        for column in columns
    ])

    scored = winsorize(working, columns, by='date', suffix=WINSOR_SUFFIX)
    score_expr: pl.Expr = pl.lit(0.0)
    available_expr: pl.Expr = pl.lit(0.0)
    for spec in tier.factors:
        z = _z_expr(f'{spec.column}{WINSOR_SUFFIX}')
        contribution = pl.when(z.is_not_null()).then(
            spec.weight * spec.direction * z).otherwise(0.0)
        score_expr = score_expr + contribution
        available_expr = available_expr + pl.when(z.is_not_null()).then(
            abs(spec.weight)).otherwise(0.0)

    scored = scored.with_columns(
        pl.when(available_expr > 0).then(score_expr / available_expr)
        .otherwise(None).alias('score'))
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
