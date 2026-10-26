#!/usr/bin/env python3

"""
Alpha 因子注册中心

与 tech_analysis/indicator_registry.py 保持同样的装饰器 + dict 模式。
使用方式：

    from src.alpha.factors.factor_registry import register_factor, FACTOR_REGISTRY

    @register_factor('price_momentum')
    class PriceMomentumFactors:
        def __init__(self, pl_df: pl.DataFrame): ...
        def calculate(self, **params) -> pl.DataFrame: ...

AlphaCalculator.calculate_factors(factor_types=['price_momentum'])
  → factor_cls = FACTOR_REGISTRY['price_momentum']
  → result = factor_cls(pl_df).calculate(**params)

新增因子只需两步：① 在新文件里定义因子类并加 @register_factor('xxx')；
                ② 在 src/alpha/factors/__init__.py 里 import 该文件
                   （触发装饰器注册）。
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Dict, Type


class _FactorRegistry:
    """因子类注册表：factor_name → 因子类"""

    def __init__(self):
        self._registry: Dict[str, Type] = {}

    def register(self, name: str) -> Callable[[Type], Type]:
        """装饰器：@register_factor('price_momentum')"""

        def decorator(cls: Type) -> Type:
            if name in self._registry:
                # 允许覆盖（调试时热替换），但给个 warning
                import warnings
                warnings.warn(
                    f"因子 '{name}' 已注册，{cls.__name__} 覆盖旧实现",
                    stacklevel=2,
                )
            self._registry[name] = cls
            cls.__factor_name__ = name  # 方便运行时自省
            return cls

        return decorator

    def get(self, name: str) -> Type | None:
        return self._registry.get(name)

    def __getitem__(self, name: str) -> Type:
        if name not in self._registry:
            available = ', '.join(sorted(self._registry)) or '(空)'
            raise KeyError(
                f"未注册的因子 '{name}'，可用: {available}")
        return self._registry[name]

    def __contains__(self, name: str) -> bool:
        return name in self._registry

    def __iter__(self):
        return iter(self._registry)

    def __len__(self) -> int:
        return len(self._registry)

    def names(self) -> list[str]:
        """返回已注册因子名列表（按字母序）"""
        return sorted(self._registry)


# 单例 + 便捷别名
FACTOR_REGISTRY = _FactorRegistry()
register_factor = FACTOR_REGISTRY.register
