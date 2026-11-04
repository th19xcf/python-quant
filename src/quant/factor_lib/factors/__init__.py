#!/usr/bin/env python3

"""因子实现模块集合 —— 导入即完成登记（含不可用因子的台账登记）。"""

from __future__ import annotations

from src.quant.factor_lib.factors import (
    alpha101,
    alpha158,
    famafrench,
    gtja191,
    technical,
)


def load_all() -> int:
    """导入全部因子实现并登记不可用因子，返回可计算因子数量。"""
    gtja191.register_unavailable()
    famafrench.register_unavailable()
    from src.quant.factor_lib.registry import computable_specs

    return len(computable_specs())


__all__ = [
    'alpha101',
    'alpha158',
    'famafrench',
    'gtja191',
    'load_all',
    'technical',
]
