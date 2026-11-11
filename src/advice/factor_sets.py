#!/usr/bin/env python3

"""扩展因子集配置（集中常量，单因子可关闭）。

原则：
- 只放经过 ``tools/eval_daily_advice.py`` 覆盖率/IC/IR/BH/共线性筛选的因子；
- 6 类风格组内**等权**（避免在 163 条上重复做 IC 加权造成二次过拟合），
  组间权重可配；legacy 12 因子保留 15%~35% 基线权重；
- 每因子 ``enabled`` 可单独关闭，不改代码路径；
- 权重按档位（short/mid/long）分别配置，三档合计 1.00。

本文件只描述配置，不包含计算逻辑（计算见 ``extended_factors.py``）。
"""

from __future__ import annotations

from dataclasses import dataclass, replace

from src.advice.config import FactorSpec, TierConfig


@dataclass(frozen=True)
class ExtendedFactor:
    """扩展因子配置项。"""

    column: str
    label: str
    style: str
    direction: float = 1.0
    enabled: bool = True
    note: str = ''


#: 6 类风格入选因子（12 个，全部通过 20 日全样本门槛；修复 purge/embargo 口径后的
#: 7 个 walk-forward 测试窗中入选 3~7 次，无因子触发连续失效判据）
#: 入选标准见 docs/每日推荐扩展因子评估.md；砍掉的因子及理由见评估报告。
STYLE_FACTORS: dict[str, tuple[ExtendedFactor, ...]] = {
    'trend': (
        ExtendedFactor('qlib_rsqr20', '20日趋势拟合度', 'trend', 1.0),
        ExtendedFactor('qlib_rsqr10', '10日趋势拟合度', 'trend', 1.0),
        # 方向 -1：与公开公式一致（实现无偏差），但 OOS IC 显著为负
        # （h=60 −0.0684），按实证方向取反，见 docs/因子方向冲突诊断_2026-11-11.md §10。
        ExtendedFactor('gtja_003', '方向动量(gtja003)', 'trend', -1.0),
    ),
    'momentum': (
        # 方向 -1：与公开公式一致（SMA 递推 α=M/N 已核对），OOS IC 显著为负
        # （h=60 −0.0929），按实证方向取反，见同上 §10。
        ExtendedFactor('gtja_024', '5日动量(gtja024)', 'momentum', -1.0),
        ExtendedFactor('a101_024', '100日动量阈值', 'momentum', 1.0),
    ),
    'volatility': (
        # 方向 -1：与公开公式一致，OOS IC 显著为负（h=60 −0.0868；改用原始价
        # 仍为 −0.0722），按实证方向取反，见同上 §10。
        ExtendedFactor('gtja_010', '下行波动(gtja010)', 'volatility', -1.0),
    ),
    'volume': (
        ExtendedFactor('a101_012', '量价反转', 'volume', 1.0),
        ExtendedFactor('ff_amihud_60', 'Amihud非流动性', 'volume', 1.0),
    ),
    'reversal': (
        ExtendedFactor('a101_020', '开盘跳空反转', 'reversal', 1.0),
    ),
    'cost': (
        ExtendedFactor('a101_042', 'VWAP折价', 'cost', 1.0),
        ExtendedFactor('gtja_012', 'VWAP偏离(gtja012)', 'cost', 1.0),
        ExtendedFactor('gtja_041', 'VWAP动量(gtja041)', 'cost', 1.0),
    ),
}

#: 组间权重（每档合计 + legacy 权重 = 1.00）
STYLE_WEIGHTS: dict[str, dict[str, float]] = {
    'short': {'trend': 0.15, 'momentum': 0.20, 'volatility': 0.15,
              'volume': 0.15, 'reversal': 0.10, 'cost': 0.05},
    'mid': {'trend': 0.15, 'momentum': 0.15, 'volatility': 0.15,
            'volume': 0.10, 'reversal': 0.10, 'cost': 0.05},
    'long': {'trend': 0.15, 'momentum': 0.20, 'volatility': 0.10,
             'volume': 0.05, 'reversal': 0.05, 'cost': 0.10},
}

#: 各档 legacy 基线权重（原 12 因子的相对权重保持）
LEGACY_WEIGHT: dict[str, float] = {'short': 0.20, 'mid': 0.30, 'long': 0.35}

#: 单因子开关：列名 -> False 即关闭（不参与打分），便于快速 A/B
DISABLED_FACTORS: frozenset[str] = frozenset()


def _direction_of(item: ExtendedFactor) -> float:
    if item.direction not in (0, 1.0, -1.0):
        return 1.0 if item.direction >= 0 else -1.0
    return item.direction


def build_extended_tier(tier: TierConfig, key: str,
                        style_weights: dict[str, float] | None = None,
                        legacy_weight: float | None = None) -> TierConfig:
    """在原档位基础上叠加 6 类扩展因子，返回新 TierConfig。"""
    weights = style_weights or STYLE_WEIGHTS[key]
    base_weight = LEGACY_WEIGHT[key] if legacy_weight is None else legacy_weight
    present = {
        style: [
            item for item in STYLE_FACTORS.get(style, ())
            if item.enabled and item.column not in DISABLED_FACTORS
        ]
        for style in weights
    }
    present = {style: items for style, items in present.items() if items}
    # 空风格（该类无入选因子）不占权重，剩余权重按比例回填，
    # 保证新因子总权重恒为 1-base_weight，不因缺少某类而稀释。
    scale = (1.0 - base_weight) / sum(weights[style] for style in present) \
        if present else 0.0
    factors: list[FactorSpec] = []
    for spec in tier.factors:
        factors.append(replace(spec, weight=spec.weight * base_weight))
    for style, items in present.items():
        per = weights[style] * scale / len(items)
        for item in items:
            factors.append(FactorSpec(
                column=item.column,
                label=item.label,
                weight=per,
                direction=_direction_of(item),
            ))
    return replace(tier, factors=tuple(factors),
                   note=(tier.note + '；已启用扩展因子集').strip('；'))


def build_extended_tiers(base: dict[str, TierConfig]) -> dict[str, TierConfig]:
    """为三档全部构建扩展因子集。"""
    return {key: build_extended_tier(tier, key) for key, tier in base.items()}


def enabled_extended_columns() -> tuple[str, ...]:
    """启用的扩展因子列（用于只计算需要的列）。"""
    return tuple(
        item.column
        for items in STYLE_FACTORS.values()
        for item in items
        if item.enabled and item.column not in DISABLED_FACTORS
    )


__all__ = [
    'DISABLED_FACTORS', 'ExtendedFactor', 'LEGACY_WEIGHT', 'STYLE_FACTORS',
    'STYLE_WEIGHTS', 'build_extended_tier', 'build_extended_tiers',
    'enabled_extended_columns',
]
