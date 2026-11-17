"""策略试算：结构化条件 → 选股 → 复用既有回测引擎 → 指标与基准对比。

本包只做「规则解析 / PIT 字段面板 / 选股」三件事；净值、撮合与成本全部
复用 ``src.backtest`` 既有实现，不另写一套净值计算。
"""
from __future__ import annotations

from .fields import FIELDS, OPERATORS, field_catalog
from .rules import RuleError, load_rule, rule_text, save_rule, validate_rule

__all__ = [
    'FIELDS',
    'OPERATORS',
    'RuleError',
    'field_catalog',
    'load_rule',
    'rule_text',
    'save_rule',
    'validate_rule',
]
