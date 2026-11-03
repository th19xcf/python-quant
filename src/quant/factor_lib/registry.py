#!/usr/bin/env python3

"""
因子登记簿 —— 每个因子的唯一元数据入口。

登记字段与用户口径一一对应：

    name          因子名（全局唯一，前缀标明来源库）
    category      因子大类（alpha101 / gtja191 / alpha158 / famafrench / technical）
    expression    公式原文（可直接对照论文/研报核验）
    dependencies  数据依赖（如 close/high/low/volume/amount/vwap）
    direction     方向：'+' 数值越大越看多，'-' 数值越大越看空，'?' 未定
    source        公开出处（论文编号 / 研报章节 / 开源实现）
    status        实现状态：verified 已验证 / approx 近似 / unavailable 不可用
    lookback      预热窗口（用于剔除前 N 行，保证样本可比）
    note          近似方式或不可用原因

状态口径：
    - verified：公式逐项对齐公开定义，且所需字段在本项目数据里真实存在；
    - approx：公式可算但有一项以上降级（日频近似 VWAP、行业中性退化为
      截面去均值、缺少市值/行业维表等），结论需打折使用；
    - unavailable：数据依赖缺失，只登记不计算（如 FF 规模/价值需要
      总市值与净资产，本项目本地库没有财务与股本数据）。
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Literal

import polars as pl

Status = Literal['verified', 'approx', 'unavailable']
Direction = Literal['+', '-', '?']

STATUS_LABELS: dict[str, str] = {
    'verified': '已验证',
    'approx': '近似',
    'unavailable': '不可用',
}

CATEGORY_LABELS: dict[str, str] = {
    'alpha101': 'WorldQuant Alpha101',
    'gtja191': '国泰君安 Alpha191',
    'alpha158': 'Qlib Alpha158',
    'famafrench': 'Fama-French 风格',
    'technical': '技术指标',
    'strategy': '公开策略',
}

#: 因子构建函数签名：入参为面板，返回等长 Series（推荐，可分阶段求值）
#: 或 Expr（仅适用于不嵌套窗口算子的简单公式）
Builder = Callable[[pl.DataFrame], 'pl.Series | pl.Expr']

_REGISTRY: dict[str, FactorSpec] = {}


@dataclass(frozen=True, slots=True)
class FactorSpec:
    """单个因子的登记信息 + 可执行 builder。"""

    name: str
    category: str
    expression: str
    dependencies: str
    direction: Direction
    source: str
    status: Status = 'verified'
    lookback: int = 1
    note: str = ''
    builder: Builder | None = field(default=None, repr=False, compare=False)

    @property
    def computable(self) -> bool:
        """是否可计算（不可用或缺少 builder 一律跳过）。"""
        return self.status != 'unavailable' and self.builder is not None

    @property
    def status_label(self) -> str:
        return STATUS_LABELS.get(self.status, self.status)

    @property
    def category_label(self) -> str:
        return CATEGORY_LABELS.get(self.category, self.category)


def factor(
    name: str,
    category: str,
    expression: str,
    dependencies: str,
    direction: Direction,
    source: str,
    lookback: int = 1,
    status: Status = 'verified',
    note: str = '',
) -> Callable[[Builder], Builder]:
    """装饰器：把 builder 连同元数据一起登记进全局登记簿。"""
    def decorator(builder: Builder) -> Builder:
        spec = FactorSpec(
            name=name,
            category=category,
            expression=expression,
            dependencies=dependencies,
            direction=direction,
            source=source,
            status=status,
            lookback=lookback,
            note=note,
            builder=builder,
        )
        register(spec)
        return builder

    return decorator


def register(spec: FactorSpec) -> None:
    """登记一条元数据；同名重复登记直接报错，防止静默覆盖。"""
    if spec.name in _REGISTRY:
        raise ValueError(f'因子重复登记: {spec.name}')
    _REGISTRY[spec.name] = spec


def register_unavailable(
    name: str,
    category: str,
    expression: str,
    dependencies: str,
    direction: Direction,
    source: str,
    note: str,
) -> None:
    """登记一个数据依赖缺失、只做台账不计算的因子。"""
    register(FactorSpec(
        name=name,
        category=category,
        expression=expression,
        dependencies=dependencies,
        direction=direction,
        source=source,
        status='unavailable',
        note=note,
    ))


def all_specs() -> list[FactorSpec]:
    """全部登记因子，按「大类 + 名称」稳定排序。"""
    return sorted(_REGISTRY.values(), key=lambda s: (s.category, s.name))


def computable_specs(categories: list[str] | None = None) -> list[FactorSpec]:
    """可计算因子，可按大类过滤。"""
    specs = [s for s in all_specs() if s.computable]
    if categories:
        specs = [s for s in specs if s.category in categories]
    return specs


def get_spec(name: str) -> FactorSpec:
    """按名称取登记项，缺失时给出明确错误。"""
    try:
        return _REGISTRY[name]
    except KeyError as exc:
        raise KeyError(f'未登记的因子: {name}') from exc


def clear() -> None:
    """清空登记簿（仅供测试重建使用）。"""
    _REGISTRY.clear()


def with_builder(spec: FactorSpec, builder: Builder) -> FactorSpec:
    """返回替换 builder 后的新登记项（避免修改 frozen dataclass）。"""
    return replace(spec, builder=builder)


__all__ = [
    'CATEGORY_LABELS',
    'STATUS_LABELS',
    'Builder',
    'Direction',
    'FactorSpec',
    'Status',
    'all_specs',
    'clear',
    'computable_specs',
    'factor',
    'get_spec',
    'register',
    'register_unavailable',
    'with_builder',
]
