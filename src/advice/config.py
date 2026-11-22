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

from src.backtest.costs.cost_model import COST_DEFAULTS
from src.core.tradability import TradabilityConfig
from src.quant.learning.config import MIN_HISTORY_ROWS


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
    """交易成本：双边佣金 + 卖出印花税 + 双边冲击成本。

    默认值直接引用回测成本单一真源 ``COST_DEFAULTS``，避免推荐、因子评估
    与回测三套口径漂移。语义映射：``impact_rate`` ↔ 回测 ``slippage``，
    ``stamp_duty_rate`` ↔ 回测 ``stamp_tax_rate``（均按一次完整往返计算）。
    """

    commission_rate: float = COST_DEFAULTS['commission_rate']
    stamp_duty_rate: float = COST_DEFAULTS['stamp_tax_rate']
    impact_rate: float = COST_DEFAULTS['slippage']
    min_commission: float = COST_DEFAULTS['min_commission']

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
    不是占可投资金；``cash_buffer`` 同理。

    ``uncapped_industry_buckets`` 是不受行业上限约束的桶。默认放行
    ``unknown``：未知行业是「缺数据」而不是一个真实行业，若给它 25% 上限，
    等于用缺失数据把整个组合压到 25%（基金/指数无行业口径时尤其明显）。
    """

    max_single_weight: float = 0.08
    max_industry_weight: float = 0.25
    cash_buffer: float = 0.10
    lot_size: int = 100
    #: 单票 ADV（20 日均成交额）参与率上限，默认 10%；与回测容量规则同口径
    adv_pct: float = 0.10
    #: 行业上限迭代归一的最大轮数
    normalize_iterations: int = 12
    #: 不参与行业上限的桶（unknown 永远单独成桶，但不默认设上限）
    uncapped_industry_buckets: tuple[str, ...] = ('unknown',)


@dataclass(frozen=True)
class RiskFilterConfig:
    """ST / 行业 / 公告事件三类风险过滤开关。

    默认值理由：

    - ``st_enabled=True``：当日推荐必须剔除 ST 风险警示标的；
    - ``st_unknown_policy='drop'``：ST 状态未知时 **fail-closed**，一律不进
      买入候选并单列 ``st_unknown_rejected``；只有明确 ``non_st`` 才放行。
      历史回放若遇到区间表/快照都覆盖不到的早期日期，会被如实记为 unknown
      并拒绝，不用「放行」掩盖数据缺口；
    - ``industry_enabled=True``：用通达信行业二级（约 56 组）替代代码前缀近似；
    - ``event_enabled=True``：只做三类风险冷却过滤；
    - ``event_unknown_policy='keep'``：事件表覆盖不到的日期返回 unknown，
      默认放行但显式登记，绝不当成「没有事件」。
    """

    st_enabled: bool = True
    st_unknown_policy: str = 'drop'  # 'keep' | 'drop'；默认 fail-closed
    industry_enabled: bool = True
    event_enabled: bool = True
    event_unknown_policy: str = 'keep'  # 'keep' | 'drop'
    #: 事件类型 → 冷却交易日数 N，区间 [T+1, T+N]
    event_cooldown: tuple[tuple[str, int], ...] = (
        ('并购重组', 20),
        ('重大合同/中标', 5),
        ('监管处罚', 5),
    )

    def cooldowns(self) -> dict[str, int]:
        return dict(self.event_cooldown)

    def validate(self) -> None:
        for name, value in (('st_unknown_policy', self.st_unknown_policy),
                            ('event_unknown_policy', self.event_unknown_policy)):
            if value not in ('keep', 'drop'):
                raise ValueError(f"{name} 只能是 'keep' 或 'drop'，实际 {value!r}")
        for event_type, days in self.event_cooldown:
            if days < 0:
                raise ValueError(f'事件 {event_type} 的冷却天数不能为负: {days}')


@dataclass(frozen=True)
class SoftFilterConfig:
    """扩展因子集的软筛选门槛（只在 extended 模式、只作用于新开仓）。

    阈值全部用当日截面分位数，不用绝对值拍数：
    - 均线多头：``ma20_gap > 0`` 且 ``ma60_gap > 0``；
    - 量能放大：``volume_momentum_20`` 不低于截面 ``volume_quantile``；
    - 波动区间：``atr_pct`` 不高于截面 ``atr_quantile``；
    - RSI 极值：剔除截面最低 ``rsi_low`` 与最高 ``rsi_high``。
    任一输入缺失时该票软筛选不通过（缺数据不放行），并记录原因。
    """

    enabled: bool = True
    ma_trend_required: bool = True
    #: 均线多头口径：``None`` = 现行「ma20_gap>0 且 ma60_gap>0」；
    #: 给数值（如 0.0 / -0.01 / -0.02）则改为「ma60_gap>0 或 ma20_gap>=该值」，
    #: 即方案 D 的「MA60 多头 或 MA20 偏离不小于 −ε」。默认 None 保持原行为。
    ma20_floor: float | None = None
    volume_quantile: float = 0.60
    atr_quantile: float = 0.80
    rsi_low: float = 0.10
    rsi_high: float = 0.90


@dataclass(frozen=True)
class TradabilityFilterConfig(TradabilityConfig):
    """实盘建议层的可交易性过滤：默认开启。

    与研究回测层分开配置：``backtest/runner`` 的 spec 缺省
    ``TradabilityConfig.research_default()``（关闭，保持历史结果可比）；
    实盘给出买入建议前先把「当天根本买不到」的票筛掉，因此这里默认开启。
    """

    enabled: bool = True


@dataclass(frozen=True)
class MarginFilterConfig:
    """融资余额 5 日增速分位剔除（选股侧可选闸门，默认关闭）。

    仅作用于新建仓候选：剔除当日横截面「融资余额 N 日增速」排名最高的
    ``exclude_top_pct`` 部分。依据 ``work/个股两融买点_2026-11-22/结论.md``
    ——该高增速档在 5/20 日均显著跑输且跨市值稳健，是本轮唯一的反向效应。

    ``enabled=False`` 时完全不读两融数据，选股行为与未接入本过滤器逐位一致。
    两融数据缺失或当日被 ``completeness.parquet`` 标记不完整时整体跳过过滤
    并记日志，不把「无数据」当成「未触发」去误剔除。
    """

    enabled: bool = False
    #: 增速回看交易日数（5 日增速 = 与第 5 个更早交易日比较）
    lookback_days: int = 5
    #: 剔除横截面最高比例（0.10 = 最高 10%）
    exclude_top_pct: float = 0.10
    #: 有效两融截面少于该只数时不计算分位（数据不完整），直接跳过
    min_universe: int = 100

    def validate(self) -> None:
        if self.lookback_days <= 0:
            raise ValueError(f'lookback_days 必须为正，实际 {self.lookback_days}')
        if not 0.0 < self.exclude_top_pct < 1.0:
            raise ValueError(
                f'exclude_top_pct 必须在 (0, 1) 内，实际 {self.exclude_top_pct}')
        if self.min_universe < 1:
            raise ValueError(f'min_universe 必须为正，实际 {self.min_universe}')


@dataclass(frozen=True)
class Rev5ExcludeConfig:
    """近 N 日复权收益最高分位剔除（排除层规则一，默认关闭）。

    口径沿用 ``work/REV5与异常换手_2026-11-22/factor_definition.md``：
    ``REV5[t] = -(close_hfq[t]/close_hfq[t-window] - 1)``，Q1（最低 REV5，
    即近 5 日涨幅最大）为被剔除组；等价地按近 5 日复权收益排名剔除最高
    ``exclude_top_pct``。只用当日及以前数据，停牌导致的窗口缺口置空。
    """

    enabled: bool = False
    #: 收益回看交易日数（位置差 = window）
    window: int = 5
    #: 剔除横截面最高比例（0.20 = 最高 20%）
    exclude_top_pct: float = 0.20
    #: 有效截面少于该只数时整体跳过（数据不完整）
    min_universe: int = 100

    def validate(self) -> None:
        if self.window <= 0:
            raise ValueError(f'REV5 window 必须为正，实际 {self.window}')
        if not 0.0 < self.exclude_top_pct < 1.0:
            raise ValueError(
                f'REV5 exclude_top_pct 必须在 (0, 1) 内，实际 {self.exclude_top_pct}')
        if self.min_universe < 1:
            raise ValueError(f'REV5 min_universe 必须为正，实际 {self.min_universe}')


@dataclass(frozen=True)
class AtoExcludeConfig:
    """异常换手最高分位剔除（排除层规则二，默认关闭）。

    口径沿用同一份研究：``ATO[t] = turn[t] / mean(turn[t-20..t-1])``，
    分母不含当日且要求过去 20 个该股交易日内至少 15 个非空观测；
    缺失不插值。剔除当日 ATO 最高的 ``exclude_top_pct``。
    """

    enabled: bool = False
    #: 均换手回看交易日数（不含当日）
    lookback_days: int = 20
    #: 分母窗口最少非空观测数
    min_obs: int = 15
    exclude_top_pct: float = 0.20
    min_universe: int = 100

    def validate(self) -> None:
        if self.lookback_days <= 0:
            raise ValueError(
                f'ATO lookback_days 必须为正，实际 {self.lookback_days}')
        if not 1 <= self.min_obs <= self.lookback_days:
            raise ValueError(
                f'ATO min_obs 必须在 [1, {self.lookback_days}] 内，'
                f'实际 {self.min_obs}')
        if not 0.0 < self.exclude_top_pct < 1.0:
            raise ValueError(
                f'ATO exclude_top_pct 必须在 (0, 1) 内，实际 {self.exclude_top_pct}')
        if self.min_universe < 1:
            raise ValueError(f'ATO min_universe 必须为正，实际 {self.min_universe}')


@dataclass(frozen=True)
class ExclusionLayerConfig:
    """统一排除层：三条独立开关的「新建仓拦截」规则。

    默认全部关闭；全部关闭时排除层不读取任何额外数据，输出与未接入时
    逐位一致。融资余额增速继续复用既有 ``MarginMomentumFilter`` 实现。
    """

    rev5: Rev5ExcludeConfig = field(default_factory=Rev5ExcludeConfig)
    ato: AtoExcludeConfig = field(default_factory=AtoExcludeConfig)
    margin: MarginFilterConfig = field(default_factory=MarginFilterConfig)

    def any_enabled(self) -> bool:
        return bool(self.rev5.enabled or self.ato.enabled or self.margin.enabled)

    def validate(self) -> None:
        self.rev5.validate()
        self.ato.validate()
        self.margin.validate()


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
# 短线：5-20 交易日，动量+量能为主导，信号衰减快；已降级为「仅观察」——
#   实测扣成本年化 -10.14%（加回撤熔断 -2.60%），未通过样本外，不在默认档位中；
# 中线：1-3 月，动量+趋势+夏普，保留更多历史信号；
# 长线：6-12 月，长周期动量+回撤修复，信号最平滑。
# ----------------------------------------------------------------------
#: 仅观察档位：不进入默认产出；显式请求时只输出观察态标记（不报错）
OBSERVE_ONLY_TIERS: frozenset[str] = frozenset({'short'})


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
            FactorSpec('momentum_1m', '1月动量', 0.30, direction=-1.0),
            FactorSpec('volume_momentum_20', '20日量能', 0.20, direction=-1.0),
            FactorSpec('ma20_gap', '偏离20日线', 0.20, direction=-1.0),
            FactorSpec('rsi_14', 'RSI(14)', 0.10, direction=-1.0),
            FactorSpec('atr_pct', 'ATR占比', 0.20, direction=-1.0),
        ),
        note=('短线档位未通过样本外验证：实测年化单边换手约 11.2x，'
              '未达 ≤4x 标定筛选门槛、未通过样本外，仅作观察，不建议据此实盘下单'),
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
            FactorSpec('momentum_3m', '3月动量', 0.30, direction=-1.0),
            FactorSpec('momentum_6m', '6月动量', 0.15, direction=-1.0),
            FactorSpec('trend_slope_60', '60日趋势', 0.20, direction=-1.0),
            FactorSpec('sharpe_ratio_20', '20日夏普', 0.15, direction=-1.0),
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
            FactorSpec('momentum_6m', '6月动量', 0.25, direction=-1.0),
            FactorSpec('momentum_12m', '12月动量', 0.25, direction=-1.0),
            FactorSpec('trend_slope_60', '60日趋势', 0.20, direction=-1.0),
            FactorSpec('sharpe_ratio_20', '20日夏普', 0.20, direction=-1.0),
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
    risk: RiskFilterConfig = field(default_factory=RiskFilterConfig)
    #: 单票最少历史行数（不足则 AVOID）
    min_history_rows: int = MIN_HISTORY_ROWS
    #: 单票最低参考价（过滤仙股）
    min_price: float = 1.0
    #: 每个档位最多输出多少条 AVOID 观察项
    max_avoid_rows: int = 10
    #: 因子集开关：legacy=原 12 因子；extended=legacy + 6 类扩展因子
    factor_set: str = 'legacy'
    #: 软筛选（仅 extended 生效）
    soft_filter: SoftFilterConfig = field(default_factory=SoftFilterConfig)
    #: 统一排除层（三条规则各自开关，默认全部关闭）
    exclusion: ExclusionLayerConfig = field(
        default_factory=ExclusionLayerConfig)
    #: 可交易性过滤（实盘建议层默认开启，研究回测层由 runner spec 默认关闭）
    tradability: TradabilityFilterConfig = field(
        default_factory=TradabilityFilterConfig)
    #: 归一化方式：'zscore'（默认）或 'percentile'
    normalization: str = 'zscore'

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


def default_output_tiers(
        tiers: dict[str, TierConfig] | None = None) -> list[str]:
    """默认产出的档位集合：排除 ``OBSERVE_ONLY_TIERS``。

    ``TIER_PRESETS`` 仍保留 short 配置，供历史回放与显式复核使用，
    但任何「未指定档位」的调用（CLI / 引擎 / UI）都不再默认产出 short。
    """
    source = TIER_PRESETS if tiers is None else tiers
    return [key for key in source if key not in OBSERVE_ONLY_TIERS]


def default_config(factor_set: str = 'extended',
                   soft_filter_enabled: bool = True,
                   margin_filter: MarginFilterConfig | None = None,
                   exclusion: ExclusionLayerConfig | None = None
                   ) -> AdviceConfig:
    """返回一套默认参数（默认启用扩展因子集，排除层三条规则默认全关）。

    ``factor_set='legacy'`` 时完全回到原 12 因子与无软筛选行为，
    用于 A/B 对照。``margin_filter`` 是旧入口，等价于只覆盖排除层的
    融资余额规则；``exclusion`` 给出完整排除层配置。两者同时给出时
    ``margin_filter`` 优先覆盖 ``exclusion.margin``。
    """
    if factor_set not in ('legacy', 'extended'):
        raise ValueError(f"factor_set 只能是 legacy/extended，实际 {factor_set!r}")
    layer = exclusion or ExclusionLayerConfig()
    if margin_filter is not None:
        layer = replace(layer, margin=margin_filter)
    layer.validate()
    tiers = dict(TIER_PRESETS)
    if factor_set == 'extended':
        from src.advice.factor_sets import build_extended_tiers

        tiers = build_extended_tiers(TIER_PRESETS)
    return AdviceConfig(
        tiers=tiers,
        factor_set=factor_set,
        soft_filter=SoftFilterConfig(enabled=soft_filter_enabled),
        exclusion=layer,
    )
