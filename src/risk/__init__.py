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
    MaxIndustryHHI,
    MaxIndustryWeight,
    MaxOrderVolumeRatio,
    MaxPositionWeight,
    MinCashReserve,
    NoSuspendedTrade,
    Order,
    Portfolio,
    PriceLimitTradable,
    RiskEngine,
    Rule,
    RuleResult,
)


def default_risk_engine() -> RiskEngine:
    """
    默认安全规则集：涨跌停不可成交 + 停牌不可交易。

    只包含与撮合内核既有判定同源的两条规则（板块幅度、Decimal 取整口径一致），
    因此装配后不改变成交结果，只把"引擎内联判定"提升为可审计的风控闸门。
    权重/行业/回撤类规则需要调用方显式注入，避免默认收窄仓位语义。
    """
    return RiskEngine([PriceLimitTradable(), NoSuspendedTrade()])


__all__ = [
    'Order',
    'Portfolio',
    'Rule',
    'RuleResult',
    'GateResult',
    'MaxPositionWeight',
    'MaxIndustryWeight',
    'MaxDrawdownCircuit',
    'MinCashReserve',
    'MaxIndustryHHI',
    'PriceLimitTradable',
    'NoSuspendedTrade',
    'MaxOrderVolumeRatio',
    'RiskEngine',
    'default_risk_engine',
]
