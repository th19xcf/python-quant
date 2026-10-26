"""
风控模块 —— 回测引擎与实盘共用的交易闸门。

核心组件：
    - Order       : 待审查的候选下单请求（轻量 dataclass）
    - Portfolio   : 组合状态快照（轻量 wrapper，不重造组合管理）
    - Rule        : 风控规则基类（ABC）
    - RiskEngine  : 规则组合 + gate() 拦截 + rejected 日志

回测集成点：两个回测引擎在 inline 下单前调用 RiskEngine.gate()，
被拒的单子记进 rejected_orders 日志，让"策略以为在满仓，实际被风控砍"
的 gap 可见。
"""

from src.risk.rules import (
    GateResult,
    MaxDrawdownCircuit,
    MaxIndustryWeight,
    MaxPositionWeight,
    Order,
    Portfolio,
    RiskEngine,
    Rule,
    RuleResult,
)

__all__ = [
    'Order',
    'Portfolio',
    'Rule',
    'RuleResult',
    'GateResult',
    'MaxPositionWeight',
    'MaxIndustryWeight',
    'MaxDrawdownCircuit',
    'RiskEngine',
]
