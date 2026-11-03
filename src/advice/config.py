#!/usr/bin/env python3

"""推荐清单参数：档位预设、换手控制、成本与仓位约束。

参数分三层：
1. ``TierConfig``：短线/中线/长线各自的持有期、确认日、信号衰减与风险阈值；
2. ``TurnoverConfig``：最小持有期覆盖、单次换手上限、成本门槛；
3. ``PortfolioConstraints``：单票/行业仓位上限与现金缓冲。

所有默认值都显式暴露为 dataclass 字段，方便 CLI 与测试逐项覆盖，
不做隐式全局常量。
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace


@dataclass(frozen=True)
class FactorSpec:
    """单个因子在综合打分中的权重与方向。

    ``direction=-1`` 表示该因子数值越大越差（如波动率、ATR、下行风险），
    在标准化后取负号参与加权。
    """

    column: str
    label: str
    weight: float
    direction: float = 1.0


@dataclass(frozen=True)
class TradingCosts:
    """交易成本：双边佣金 + 卖出印花税 + 双边冲击成本。"""

    commission_rate: float = 0.0003
    stamp_duty_rate: float = 0.001
    impact_rate: float = 0.001
    min_commission: float = 5.0

    @property
    def round_trip_rate(self) -> float:
        """一次买入+卖出（一进一出）的综合费率。"""
        return (2 * self.commission_rate
                + self.stamp_duty_rate
                + 2 * self.impact_rate)

    def one_side_rate(self, side: str) -> float:
        """单边费率：买入含佣金+冲击，卖出再叠加印花税。"""
        rate = self.commission_rate + self.impact_rate
        if side == 'sell':
            rate += self.stamp_duty_rate
        return rate

    def commission(self, notional: float) -> float:
        """按成交额计算佣金，遵守最低佣金。"""
        return max(self.min_commission, abs(notional) * self.commission_rate)


@dataclass(frozen=True)
class TurnoverConfig:
    """换手控制参数（日频输出 ≠ 日频调仓）。"""

    #: 全局最小持有期覆盖；None 表示使用档位自身的 min_hold_days
    min_hold_days_override: int | None = None
    #: 单次调仓的单边换手上限（占组合权益比例）
    max_turnover_per_rebalance: float = 0.20
    #: 成本门槛：预期收益改善低于一次进出成本时不调仓
    cost_threshold_multiple: float = 1.0
    #: 买入候选的预期收益至少为一次进出成本的多少倍
    min_entry_edge_multiple: float = 1.0


@dataclass(frozen=True)
class PortfolioConstraints:
    """仓位约束。

    ``max_single_weight`` 与 ``max_industry_weight`` 都是「占总资金」的比例，
    不是占可投资金；``cash_buffer`` 同理。行业缺失时由代码前缀近似并显式标注。
    """

    max_single_weight: float = 0.08
    max_industry_weight: float = 0.25
    cash_buffer: float = 0.10
    lot_size: int = 100
    #: 行业上限迭代归一的最大轮数
    normalize_iterations: int = 12


@dataclass(frozen=True)
class TierConfig:
    """单个持有期档位的完整参数。"""

    key: str
    label: str
    lookback_days: int
    min_hold_days: int
    confirm_days: int
    validity_days: int
    buy_percentile: float
    sell_percentile: float
    max_positions: int
    edge_scale: float
    signal_decay: float
    max_volatility: float
    max_atr_pct: float
    min_amount: float
    factors: tuple[FactorSpec, ...] = field(default_factory=tuple)
    #: 档位级风险提示（会写入 TierAdvice.notes 与 BUY 行 risk_notes）
    note: str = ''

    def with_overrides(self, **kwargs) -> TierConfig:
        """返回覆盖部分字段的新档位（不修改原对象）。"""
        return replace(self, **kwargs)


# ----------------------------------------------------------------------
# 三档预设
# 短线：5-20 交易日，动量+量能为主导，信号衰减快；
# 中线：1-3 月，动量+趋势+夏普，保留更多历史信号；
# 长线：6-12 月，长周期动量+回撤修复，信号最平滑。
# ----------------------------------------------------------------------
TIER_PRESETS: dict[str, TierConfig] = {
    'short': TierConfig(
        key='short',
        label='短线',
        lookback_days=120,
        min_hold_days=5,
        confirm_days=2,
        validity_days=20,
        buy_percentile=0.92,
        sell_percentile=0.35,
        max_positions=12,
        edge_scale=0.04,
        signal_decay=0.20,
        max_volatility=0.80,
        max_atr_pct=0.070,
        min_amount=2.0e7,
        factors=(
            FactorSpec('momentum_1m', '1月动量', 0.30),
            FactorSpec('volume_momentum_20', '20日量能', 0.20),
            FactorSpec('ma20_gap', '偏离20日线', 0.20),
            FactorSpec('rsi_14', 'RSI(14)', 0.10),
            FactorSpec('atr_pct', 'ATR占比', 0.20, direction=-1.0),
        ),
        note=('短线档位未通过样本外验证：日频信号在≤4x换手下扣费后仍为负收益，'
              '仅作观察，不建议据此实盘下单'),
    ),
    'mid': TierConfig(
        key='mid',
        label='中线',
        lookback_days=250,
        min_hold_days=20,
        confirm_days=3,
        validity_days=60,
        buy_percentile=0.90,
        sell_percentile=0.30,
        max_positions=15,
        edge_scale=0.08,
        signal_decay=0.50,
        max_volatility=0.60,
        max_atr_pct=0.050,
        min_amount=3.0e7,
        factors=(
            FactorSpec('momentum_3m', '3月动量', 0.30),
            FactorSpec('momentum_6m', '6月动量', 0.15),
            FactorSpec('trend_slope_60', '60日趋势', 0.20),
            FactorSpec('sharpe_ratio_20', '20日夏普', 0.15),
            FactorSpec('volatility_20', '20日波动率', 0.20, direction=-1.0),
        ),
    ),
    'long': TierConfig(
        key='long',
        label='长线',
        lookback_days=500,
        min_hold_days=60,
        confirm_days=5,
        validity_days=240,
        buy_percentile=0.88,
        sell_percentile=0.25,
        max_positions=15,
        edge_scale=0.15,
        signal_decay=0.80,
        max_volatility=0.50,
        max_atr_pct=0.040,
        min_amount=5.0e7,
        factors=(
            FactorSpec('momentum_6m', '6月动量', 0.25),
            FactorSpec('momentum_12m', '12月动量', 0.25),
            FactorSpec('trend_slope_60', '60日趋势', 0.20),
            FactorSpec('sharpe_ratio_20', '20日夏普', 0.20),
            FactorSpec('max_drawdown_20', '20日回撤', 0.10),
        ),
    ),
}


@dataclass(frozen=True)
class AdviceConfig:
    """一次推荐生成所需的全部参数。"""

    tiers: dict[str, TierConfig] = field(default_factory=lambda: dict(TIER_PRESETS))
    constraints: PortfolioConstraints = field(default_factory=PortfolioConstraints)
    costs: TradingCosts = field(default_factory=TradingCosts)
    turnover: TurnoverConfig = field(default_factory=TurnoverConfig)
    #: 单票最少历史行数（不足则 AVOID）
    min_history_rows: int = 60
    #: 单票最低参考价（过滤仙股）
    min_price: float = 1.0
    #: 每个档位最多输出多少条 AVOID 观察项
    max_avoid_rows: int = 10

    def tier(self, key: str) -> TierConfig:
        if key not in self.tiers:
            available = ', '.join(sorted(self.tiers))
            raise KeyError(f"未知档位 '{key}'，可用: {available}")
        return self.tiers[key]

    def effective_min_hold(self, tier: TierConfig) -> int:
        """换手控制的全局最小持有期覆盖。"""
        if self.turnover.min_hold_days_override is not None:
            return max(0, self.turnover.min_hold_days_override)
        return max(0, tier.min_hold_days)


def default_config() -> AdviceConfig:
    """返回一套默认参数（三档预设 + 默认约束/成本）。"""
    return AdviceConfig()
