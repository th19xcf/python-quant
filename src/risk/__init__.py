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

默认规则集分三种配置，避免把「组合约束」直接套到单票账户：

- ``default_risk_engine()``：组合完整集（9 条），供组合回测/实盘组合使用；
- ``single_asset_risk_engine()``：单票集（6 条），权重上限 100%、不启用
  行业/HHI（单票账户天然只有一个行业，10%/30% 上限只会把回测压成空跑）；
- ``default_risk_engine(include_industry=False)``：组合但行业映射缺失时使用。

所有默认阈值集中在 ``DEFAULT_RISK_LIMITS``，报告与结果里的
``risk_profile.effective_limits`` 会列出本次真正生效的参数。
"""

from src.risk.rules import (
    GateResult,
    MaxAdvParticipation,
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

#: 默认阈值（与任务约定一致；单票/小组合由装配层做必要放宽并记录）
DEFAULT_RISK_LIMITS = {
    'max_position_weight': 0.10,
    'max_industry_weight': 0.30,
    'max_industry_hhi': 0.25,
    'max_drawdown': 0.15,
    'drawdown_window': 252,
    'min_cash_reserve': 0.0,
    'max_order_volume_ratio': 0.10,
    'max_adv_participation': 0.10,
    'adv_window': 20,
}


def default_risk_engine(*, position_limit: float | None = None,
                        industry_limit: float | None = None,
                        hhi_limit: float | None = None,
                        include_industry: bool = True,
                        include_liquidity: bool = True) -> RiskEngine:
    """组合完整安全规则集。

    默认包含：涨跌停不可成交、停牌不可交易、单票 10%、行业 30%、
    行业 HHI 0.25、回撤 15%/252d、现金不低于 0、单日成交量 10%、
    20 日 ADV 参与率 10%。

    ``position_limit`` / ``industry_limit`` / ``hhi_limit`` 用于小组合的
    可行域放宽（见 ``src.core.usecases.run_backtest``）：例如 3 只等权组合
    的单票目标仓位天然 33%，若仍按 10% 上限会全部拒单；装配层会把上限
    放宽到 ``1/N + 1pp`` 并在 ``risk_profile`` 里记录实际生效值。
    """
    limits = DEFAULT_RISK_LIMITS
    rules = [
        PriceLimitTradable(),
        NoSuspendedTrade(),
        MaxPositionWeight(
            limits['max_position_weight'] if position_limit is None else position_limit),
        MaxDrawdownCircuit(
            limits['max_drawdown'], int(limits['drawdown_window'])),
        MinCashReserve(limits['min_cash_reserve']),
    ]
    if include_industry:
        rules.insert(2, MaxIndustryWeight(
            limits['max_industry_weight'] if industry_limit is None else industry_limit))
        rules.insert(3, MaxIndustryHHI(
            limits['max_industry_hhi'] if hhi_limit is None else hhi_limit))
    if include_liquidity:
        rules.insert(len(rules) - 2, MaxOrderVolumeRatio(
            limits['max_order_volume_ratio']))
        rules.insert(len(rules) - 2, MaxAdvParticipation(
            limits['max_adv_participation'], int(limits['adv_window'])))
    return RiskEngine(rules)


def single_asset_risk_engine() -> RiskEngine:
    """单票账户规则集：保留回撤/现金/流动性，关闭行业类约束。

    单票账户只有一个标的、一个行业，行业权重恒等于单票权重；行业上限
    对单票没有风险管理意义，只会让「满仓单票」策略无法成交。权重上限
    放宽到 100%，仍保留现金不透支约束。
    """
    return default_risk_engine(
        position_limit=1.0, include_industry=False, include_liquidity=True,
    )


__all__ = [
    'DEFAULT_RISK_LIMITS',
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
    'MaxAdvParticipation',
    'RiskEngine',
    'default_risk_engine',
    'single_asset_risk_engine',
]
