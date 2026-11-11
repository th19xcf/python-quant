#!/usr/bin/env python3

"""学习层配置与通用数据结构。

第一阶段「自学习」只做两级，不做深度强化学习：
- L1 参数自适应：策略参数按市场状态（regime）滚动选择；
- L2 因子权重学习：横截面打分权重由 IC/ICIR/半衰期衰减/滚动 Ridge 估计。

所有评估都必须满足样本外纪律：rolling/expanding 窗口、训练-测试之间留
embargo（默认 ≥ 一个持有期），训练样本还要按标签窗口做 purge。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

#: 「历史是否足够」的统一门槛。
#: 单票最少历史行数（推荐侧）与训练窗最少有效截面期数（学习侧）共用，
#: 避免同一个 60 在多个模块各写一遍后各自漂移。
MIN_HISTORY_ROWS = 60


@dataclass(frozen=True)
class WalkForwardConfig:
    """walk-forward 切分与评估配置。

    Args:
        horizon: 持有期（交易日），也是前瞻收益标签的跨度；T+1 开盘建仓，
            因此标签使用 ``open[t+1+H] / open[t+1] - 1``。
        train_size: 滚动训练窗长度（交易日数）。
        test_size: 测试窗长度（交易日数）。
        embargo: 训练窗与测试窗之间的隔离带长度（交易日数），
            必须 ≥ horizon，避免持有期收益跨越边界造成泄漏。
        expanding: True 为扩展窗（训练起点固定），False 为滚动窗。
        min_train_dates: 训练窗内最少有效截面期数，不足则跳过该窗口。
        min_test_dates: 测试窗内最少有效截面期数，不足则跳过该窗口。
        n_quantiles: 分层评估的分组数。
        top_fraction: 组合只做多时选取的头部比例。
        ridge_lambda: 滚动 Ridge 的正则强度。
        half_life: IC 加权时半衰期（交易日），用于衰减旧样本权重。
        annualization: 年化因子（日线 252）。
    """

    horizon: int = 10
    train_size: int = 504
    test_size: int = 126
    embargo: int = 20
    expanding: bool = False
    min_train_dates: int = MIN_HISTORY_ROWS
    min_test_dates: int = 20
    n_quantiles: int = 5
    top_fraction: float = 0.2
    ridge_lambda: float = 10.0
    half_life: int = 126
    annualization: int = 252

    def __post_init__(self) -> None:
        if self.horizon <= 0:
            raise ValueError(f'horizon 必须为正，实际 {self.horizon}')
        if self.train_size <= self.horizon + 1:
            raise ValueError('train_size 必须大于 horizon + 1')
        if self.test_size < 1:
            raise ValueError(f'test_size 必须为正，实际 {self.test_size}')
        if self.embargo < self.horizon:
            raise ValueError(
                f'embargo 必须 ≥ horizon（至少一个持有期），实际 '
                f'embargo={self.embargo}, horizon={self.horizon}')
        if self.expanding and self.train_size < self.min_train_dates:
            raise ValueError('train_size 小于 min_train_dates')
        if not 0 < self.top_fraction <= 1:
            raise ValueError(f'top_fraction 必须在 (0, 1]，实际 {self.top_fraction}')
        if self.n_quantiles < 2:
            raise ValueError('n_quantiles 至少为 2')


@dataclass(frozen=True)
class WalkForwardWindow:
    """全局交易日索引上的一个 walk-forward 窗口（半开区间）。"""

    index: int
    train_start: int
    train_end: int
    test_start: int
    test_end: int
    purged_train_end: int

    @property
    def train_days(self) -> int:
        return self.train_end - self.train_start

    @property
    def test_days(self) -> int:
        return self.test_end - self.test_start


@dataclass
class SchemeMetrics:
    """单个方案在样本外的一组指标。"""

    name: str
    ic_mean: float = 0.0
    ic_std: float = 0.0
    icir: float = 0.0
    ic_positive_ratio: float = 0.0
    ic_t_stat: float = 0.0
    quantile_returns: list[float] = field(default_factory=list)
    long_short_return: float = 0.0
    monotonicity: float = 0.0
    turnover: float = 0.0
    annual_return: float = 0.0
    max_drawdown: float = 0.0
    sharpe_ratio: float = 0.0
    total_return: float = 0.0
    n_periods: int = 0
    notes: str = ''


@dataclass
class LearningReport:
    """一次完整学习实验的输出（供报告渲染）。"""

    title: str
    config: WalkForwardConfig
    data_scope: dict[str, Any]
    weight_schemes: list[SchemeMetrics] = field(default_factory=list)
    factor_metrics: list[SchemeMetrics] = field(default_factory=list)
    param_schemes: list[SchemeMetrics] = field(default_factory=list)
    windows: int = 0
    weight_history: list[dict[str, Any]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
