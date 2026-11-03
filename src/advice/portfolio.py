#!/usr/bin/env python3

"""仓位分配：横截面打分 → 单票/行业上限 → 现金缓冲 → 整手股数。

约束按「占总资金」解释：
- ``max_single_weight``：单票上限；
- ``max_industry_weight``：同行业（或代码前缀近似板块）合计上限；
- ``cash_buffer``：始终保留的现金比例。

分配采用迭代归一：先按分数种子分配，再裁剪越界仓位，剩余额度在未触顶
标的中按种子再分配，直至收敛或达到迭代上限；无法分配的部分留作现金。
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from loguru import logger

from src.advice.config import PortfolioConstraints


@dataclass
class Candidate:
    """待分配仓位的候选标的。"""

    ts_code: str
    score_percentile: float
    price: float
    industry: str
    industry_source: str


@dataclass
class Allocation:
    """单个标的的分配结果。"""

    ts_code: str
    target_weight: float
    shares: int
    price: float
    industry: str


def _seed(candidate: Candidate) -> float:
    """分数种子：百分位越极端，初始权重越大；下限避免除零。"""
    return max(1e-4, (float(candidate.score_percentile) - 0.5) ** 1.5)


def allocate(candidates: list[Candidate], equity: float,  # noqa: C901
             constraints: PortfolioConstraints,
             max_positions: int | None = None) -> list[Allocation]:
    """把候选标的分配为具体仓位与整手股数。"""
    if equity <= 0 or not candidates:
        return []

    pool = sorted(candidates, key=lambda item: item.score_percentile,
                  reverse=True)
    if max_positions is not None:
        # 0 表示没有空余仓位（例如持仓已满），必须返回空分配；
        # 旧实现把 0 当作“不限制”，会持续超配。
        pool = pool[:max(0, int(max_positions))]
    if not pool:
        return []

    investable = equity * max(0.0, 1.0 - constraints.cash_buffer)
    if investable <= 0:
        return []

    seeds = [(_seed(candidate), candidate) for candidate in pool]
    total_seed = sum(seed for seed, _ in seeds)
    targets = {candidate.ts_code: investable * seed / total_seed
               for seed, candidate in seeds}
    info = {candidate.ts_code: candidate for _, candidate in seeds}
    single_cap = constraints.max_single_weight * equity
    industry_cap = constraints.max_industry_weight * equity
    uncapped = set(constraints.uncapped_industry_buckets)

    for _ in range(max(1, constraints.normalize_iterations)):
        changed = False
        for code, weight in list(targets.items()):
            if weight > single_cap + 1e-9:
                targets[code] = single_cap
                changed = True
        groups: dict[str, float] = {}
        for code, weight in targets.items():
            groups[info[code].industry] = groups.get(info[code].industry, 0.0) + weight
        for industry, group_weight in groups.items():
            if industry in uncapped:
                # 未知行业是缺数据而非真实行业：单独成桶但不设上限，
                # 否则缺失数据会把整个组合压到行业上限以内。
                continue
            if group_weight <= industry_cap + 1e-9:
                continue
            scale = industry_cap / group_weight
            for code, weight in list(targets.items()):
                if info[code].industry == industry:
                    targets[code] = weight * scale
            changed = True

        free = investable - sum(targets.values())
        if free <= 1e-6:
            break
        eligible = [
            code for code, weight in targets.items()
            if weight < single_cap - 1e-9
            and (info[code].industry in uncapped
                 or groups.get(info[code].industry, 0.0) < industry_cap - 1e-9)
        ]
        if not eligible:
            break
        eligible_seed = sum(_seed(info[code]) for code in eligible)
        if eligible_seed <= 0:
            break
        for code in eligible:
            targets[code] += free * _seed(info[code]) / eligible_seed
        if not changed and free <= investable * 1e-6:
            break

    allocations: list[Allocation] = []
    lot = max(1, int(constraints.lot_size))
    for code, target in targets.items():
        candidate = info[code]
        if candidate.price <= 0:
            continue
        shares = int(math.floor(target / candidate.price / lot) * lot)
        if shares <= 0:
            continue
        actual_weight = (shares * candidate.price) / equity
        allocations.append(Allocation(
            ts_code=code,
            target_weight=actual_weight,
            shares=shares,
            price=candidate.price,
            industry=candidate.industry,
        ))

    allocated = sum(item.target_weight for item in allocations)
    if allocated > 1.0 - constraints.cash_buffer + 1e-6:
        logger.warning(
            f"[advice] 分配后仓位 {allocated:.2%} 超过可投资上限，"
            f"现金缓冲被压缩")
    return allocations
