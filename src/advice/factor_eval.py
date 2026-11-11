#!/usr/bin/env python3

"""因子有效性评估：覆盖率、RankIC、分组单调性、共线性、稳定性、BH 校正。

口径：
- 只用 t 日及以前数据计算因子；未来 h 日收益只做标签，不参与特征；
- 价格统一 hfq（``factor_lib.data.load_panel`` 已处理）；
- RankIC = 每日截面 Spearman(factor, forward_return)；
- IR = mean(IC) / std(IC)（不做年化，便于跨窗口比较）；
- 多重检验：对每个因子的 IC t 检验 p 值做 Benjamini-Hochberg 校正；
- 稳定性：因子截面前 20% 排名在 t 与 t-1 的截面相关（抽样日期计算）；
- 分组：每日按因子分 N 组（``EvalConfig.n_groups``，默认 5），统计各组未来收益均值
  与多空（G5-G1）；单调性 = 每日 N 点 Spearman 的跨日均值，门槛 ``min_monotonic``
  必须与 ``n_groups`` 成对给出（同一阈值在不同 N 下含义不同，量级依据见
  ``docs/因子单调性门槛配置_2026-11-11.md``）。

注意：本模块只做研究评估，不写入生产配置。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Iterable

import numpy as np
import polars as pl
from scipy import stats

from src.advice.extended_factors import (
    EXTRA_CANDIDATES,
    EXTRA_COLUMNS,
    FACTOR_LIB_CANDIDATES,
)
from src.advice.fundamental_factors import FUNDAMENTAL_CANDIDATES
from src.backtest.costs.cost_model import CostModel
from src.quant.factor_lib.registry import get_spec
from src.quant.learning.config import MIN_HISTORY_ROWS, WalkForwardConfig
from src.quant.learning.splits import make_walk_forward_windows

MIN_PAIRS = 30
MIN_COVERAGE = 0.80
MIN_ABS_IC = 0.010
MIN_IR = 0.15
#: 分组数：单调性是「N 个组收益」的 Spearman，门槛必须与它成对给出。
DEFAULT_N_GROUPS = 5
#: 单调性硬筛下限。当前为 0.0 即「不做单调性硬筛」，只要求方向为正；
#: 本口径（每日 N 点 Spearman 的跨日均值）实测上限很低，见
#: docs/因子单调性门槛配置_2026-11-11.md，收紧门槛前必须先看该文档的量级证据。
MIN_MONOTONIC = 0.0
MAX_OTHER = 12
CORR_LIMIT = 0.80
BH_ALPHA = 0.05

_TREND_COLUMNS = (
    'tech_macd_hist', 'tech_ema_bias_20', 'tech_ma_gap_5_20',
    'qlib_ma20', 'qlib_ma60', 'qlib_rsqr20',
)
STYLE_OF: dict[str, str] = {
    **dict.fromkeys(_TREND_COLUMNS, 'trend'),
    'tech_boll_pos_20': 'trend',
    'tech_mom_20': 'momentum',
    'qlib_roc20': 'momentum',
    'qlib_roc60': 'momentum',
    'tech_atr_14_pct': 'volatility',
    'qlib_std20': 'volatility',
    'tech_volume_ratio_5': 'volume',
    'qlib_vma20': 'volume',
    'qlib_vma60': 'volume',
    'tech_rsi_14': 'reversal',
    'tech_kdj_k': 'reversal',
    'tech_kdj_j': 'reversal',
    'tech_up_streak': 'reversal',
    **{item.column: item.style for item in EXTRA_CANDIDATES},
    **{item.column: item.style for item in FUNDAMENTAL_CANDIDATES},
}

#: 基本面候选的登记方向（其余因子按 factor_lib 登记或训练窗 IC 符号判定）
FUNDAMENTAL_DIRECTION: dict[str, float] = {
    item.column: item.direction for item in FUNDAMENTAL_CANDIDATES}


def style_of(name: str, expression: str = '') -> str:
    """按列名/公式关键词归类到 6 类；无法识别的归 ``other``（配额单列）。"""
    if name in STYLE_OF:
        return STYLE_OF[name]
    lowered = f'{name} {expression}'.lower()
    keyword_groups: tuple[tuple[str, tuple[str, ...]], ...] = (
        ('cost', ('mcst', 'cyc', 'cys', 'cost')),
        ('volume', ('volume', 'vwap', 'amount', 'adv', 'obv', 'amihud',
                    'illiq', 'vma', 'vr', 'psy', 'mfi', 'emv', 'asi')),
        ('volatility', ('stddev', 'std(', 'covariance', 'var(', 'beta',
                        'skew', 'kurt', 'amplitude')),
        ('reversal', ('ts_max', 'ts_min', 'hhv', 'llv', 'rsi', 'kdj', 'cci',
                      'wr', 'bias', 'streak', 'delta(', 'sign(')),
        ('momentum', ('roc', 'mom', 'mtm', 'ret_', 'rsqr')),
        ('trend', ('ma(', 'mean(', 'corr', 'slope', 'boll', 'trix', 'dmi',
                   'adx', 'sar', 'bbi', 'ema')),
    )
    for style, keywords in keyword_groups:
        if any(word in lowered for word in keywords):
            return style
    return 'other'

#: 现有 12 因子（legacy）
LEGACY_COLUMNS: tuple[str, ...] = (
    'momentum_1m', 'momentum_3m', 'momentum_6m', 'momentum_12m',
    'rsi_14', 'volume_momentum_20', 'volatility_20', 'atr_pct',
    'ma20_gap', 'trend_slope_60', 'sharpe_ratio_20', 'max_drawdown_20',
)


@dataclass
class EvalConfig:
    """评估参数（全部可配置，不写死在流程里）。"""

    start: str = '2020-01-01'
    end: str = '2026-11-01'
    universe_size: int = 800
    horizons: tuple[int, ...] = (5, 20)
    horizon: int = 20
    train_months: int = 24
    test_months: int = 6
    #: 训练/测试之间的隔离带（交易日），必须 ≥ horizon；复用小节 learning.splits 的口径
    embargo: int = 20
    min_coverage: float = MIN_COVERAGE
    min_abs_ic: float = MIN_ABS_IC
    min_ir: float = MIN_IR
    #: 单调性门槛；``n_groups`` 不同则同一数值含义不同，两者必须成对调整
    min_monotonic: float = MIN_MONOTONIC
    #: 每日分组数（单调性的样本点数），与 ``min_monotonic`` 绑定
    n_groups: int = DEFAULT_N_GROUPS
    corr_limit: float = CORR_LIMIT
    bh_alpha: float = BH_ALPHA
    max_per_style: int = 8
    min_per_style: int = 3
    max_total: int = 50
    max_other: int = MAX_OTHER
    top_k: int = 20
    hold_days: int = 20

    def __post_init__(self) -> None:
        if self.n_groups < 2:  # noqa: PLR2004
            raise ValueError(f'n_groups 必须 ≥ 2，当前 {self.n_groups}')
        if not -1.0 <= self.min_monotonic <= 1.0:
            raise ValueError(f'min_monotonic 必须落在 [-1, 1]，当前 {self.min_monotonic}')


@dataclass
class FactorMetrics:
    """单因子单窗口指标。"""

    factor: str
    style: str
    horizon: int
    coverage: float
    ic_mean: float
    ic_std: float
    ic_ir: float
    ic_positive: float
    t_stat: float
    p_value: float
    q_value: float = 1.0
    monotonic: float = 0.0
    long_short: float = 0.0
    stability: float = 0.0
    selected: bool = False
    reason: str = ''
    #: 未按登记方向对齐的原始 RankIC 均值（训练窗内）；权重符号只能由它决定
    ic_mean_raw: float = 0.0


@dataclass
class WindowResult:
    """一个 walk-forward 窗口的结果。"""

    train_start: date
    train_end: date
    test_start: date
    test_end: date
    selected: list[str] = field(default_factory=list)
    test_metrics: dict[str, Any] = field(default_factory=dict)
    weights: dict[str, float] = field(default_factory=dict)


def _as_pl_date(value: str | date) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value)[:10])


def add_forward_returns(frame: pl.DataFrame, horizons: Iterable[int]) -> pl.DataFrame:
    """追加未来 h 日 hfq 收益（只做标签）。"""
    out = frame.sort(['ts_code', 'date'])
    exprs = []
    for h in horizons:
        exprs.append(
            (pl.col('close').shift(-h).over('ts_code') / pl.col('close') - 1.0)
            .alias(f'fwd_{h}'))
    return out.with_columns(exprs)


def _rank_ic(frame: pl.DataFrame, factor: str, label: str) -> pl.DataFrame:
    """每日截面 RankIC（至少 MIN_PAIRS 个有效样本）。"""
    return (
        frame.filter(pl.col(factor).is_not_null() & pl.col(label).is_not_null())
        .group_by('date')
        .agg(
            pl.len().alias('pairs'),
            pl.corr(pl.col(factor).rank(), pl.col(label).rank()).alias('ic'),
        )
        .filter((pl.col('pairs') >= MIN_PAIRS) & pl.col('ic').is_not_null())
        .select('date', 'ic')
    )


def _ic_stats(ic: pl.DataFrame) -> tuple[float, float, float, float, float, float]:
    """mean / std / IR / 正比例 / t / p。"""
    values = ic['ic'].to_numpy()
    n = len(values)
    if n < 2:  # noqa: PLR2004
        return 0.0, 0.0, 0.0, 0.0, 0.0, 1.0
    mean = float(np.mean(values))
    std = float(np.std(values, ddof=1))
    ir = mean / std if std > 1e-12 else 0.0
    positive = float(np.mean(values > 0))
    if std <= 1e-12:
        return mean, std, ir, positive, 0.0, 1.0
    t_stat = mean / (std / np.sqrt(n))
    p_value = float(2.0 * stats.t.sf(abs(t_stat), df=n - 1))
    return mean, std, ir, positive, float(t_stat), p_value


def _as_float(value: Any) -> float:
    """Polars 标量（类型标注为宽 Union）安全转 float；空/NaN 返回 0.0。"""
    array = np.asarray(value, dtype=float).ravel()
    if array.size == 0:
        return 0.0
    result = float(array[0])
    return result if np.isfinite(result) else 0.0


def _coverage(frame: pl.DataFrame, factor: str) -> float:
    shares = frame.group_by('date').agg(
        pl.col(factor).is_not_null().mean().alias('cov'))
    return _as_float(shares['cov'].mean())


def _buckets(frame: pl.DataFrame, factor: str, label: str,
             n_groups: int = DEFAULT_N_GROUPS) -> pl.DataFrame:
    """每日按因子分 n 组，返回组号与未来收益（用于单调性）。"""
    valid = frame.filter(pl.col(factor).is_not_null() & pl.col(label).is_not_null())
    return (
        valid.with_columns(
            (((pl.col(factor).rank('ordinal').over('date') - 1) * n_groups)
             / pl.len().over('date')).floor().cast(pl.Int32).alias('bucket'))
        .group_by(['date', 'bucket'])
        .agg(pl.col(label).mean().alias('ret'), pl.len().alias('n'))
    )


def _monotonic_stats(frame: pl.DataFrame, factor: str, label: str,
                     n_groups: int = DEFAULT_N_GROUPS) -> tuple[float, float]:
    groups = _buckets(frame, factor, label, n_groups)
    per_date = groups.group_by('date').agg(
        pl.col('ret').sort_by('bucket').alias('rets'),
        pl.col('bucket').sort_by('bucket').alias('buckets'))
    arrays = [
        (row['rets'], row['buckets'])
        for row in per_date.to_dicts()
        if len(row['rets']) == n_groups
    ]
    if not arrays:
        return 0.0, 0.0
    monotonic_values: list[float] = []
    ls_values: list[float] = []
    for rets, _ in arrays:
        arr = np.asarray(rets, dtype=float)
        rho = stats.spearmanr(np.arange(n_groups), arr).statistic
        if np.isfinite(rho):
            monotonic_values.append(float(rho))
        ls_values.append(float(arr[-1] - arr[0]))
    return (float(np.mean(monotonic_values)) if monotonic_values else 0.0,
            float(np.mean(ls_values)) if ls_values else 0.0)


def _stability(frame: pl.DataFrame, factor: str, sample_dates: list[date]) -> float:
    """排名日间自相关：抽样日期上，t 与 t-1 的截面 rank 相关均值。"""
    sub = frame.filter(pl.col('date').is_in(sample_dates)).sort(['ts_code', 'date'])
    ranked = sub.with_columns(pl.col(factor).rank('average').over('date').alias('__r'))
    lag = ranked.with_columns(pl.col('__r').shift(1).over('ts_code').alias('__r_lag'))
    value = (
        lag.filter(pl.col('__r').is_not_null() & pl.col('__r_lag').is_not_null())
        .group_by('date')
        .agg(pl.corr(pl.col('__r'), pl.col('__r_lag')).alias('rho'))['rho'].mean()
    )
    return _as_float(value)


def bh_qvalues(p_values: list[float]) -> list[float]:
    """Benjamini-Hochberg 校正，返回 q 值（与输入同序）。

    非有限 p（因子全空/样本不足）直接记为 1.0，不参与排序，
    否则 NaN 会把排序打乱并给出错误的 q。
    """
    n = len(p_values)
    if n == 0:
        return []
    finite = [np.isfinite(p) for p in p_values]
    order = sorted((i for i in range(n) if finite[i]),
                   key=lambda i: p_values[i])
    q = [1.0] * n
    previous = 1.0
    for rank, index in enumerate(reversed(order), start=1):
        i = len(order) - rank + 1
        value = min(previous, p_values[index] * len(order) / i)
        q[index] = value
        previous = value
    return q


def evaluate_factor(frame: pl.DataFrame, factor: str, style: str, direction: float,
                    horizon: int, sample_dates: list[date],
                    label: str | None = None,
                    n_groups: int = DEFAULT_N_GROUPS) -> FactorMetrics:
    """单因子单窗口指标（IC 已按方向对齐；``n_groups`` 决定单调性的分组数）。"""
    label = label or f'fwd_{horizon}'
    ic = _rank_ic(frame, factor, label)
    mean, std, ir, positive, t_stat, p_value = _ic_stats(ic)
    signed_mean = direction * mean
    signed_ir = direction * ir
    monotonic, long_short = _monotonic_stats(frame, factor, label, n_groups)
    if direction < 0:
        monotonic = -monotonic
        long_short = -long_short
    return FactorMetrics(
        factor=factor,
        style=style,
        horizon=horizon,
        coverage=_coverage(frame, factor),
        ic_mean=signed_mean,
        ic_std=std,
        ic_ir=signed_ir,
        ic_positive=positive if direction > 0 else 1.0 - positive,
        t_stat=t_stat,
        p_value=p_value,
        monotonic=monotonic,
        long_short=long_short,
        stability=_stability(frame, factor, sample_dates),
        ic_mean_raw=mean,
    )


def _direction_of(factor: str, ic_mean: float) -> float:
    """方向优先取登记 direction；未定方向（'?'）再用训练窗 IC 符号。"""
    if factor in FUNDAMENTAL_DIRECTION:
        return FUNDAMENTAL_DIRECTION[factor]
    if factor in EXTRA_COLUMNS:
        item = next((x for x in EXTRA_CANDIDATES if x.column == factor), None)
        return item.direction if item else 1.0
    try:
        direction = get_spec(factor).direction
    except KeyError:
        return 1.0 if ic_mean >= 0 else -1.0
    if direction == '+':
        return 1.0
    if direction == '-':
        return -1.0
    return 1.0 if ic_mean >= 0 else -1.0


def evaluate_candidates(frame: pl.DataFrame, factors: list[str], horizon: int,
                        sample_dates: list[date],
                        n_groups: int = DEFAULT_N_GROUPS) -> list[FactorMetrics]:
    """批量评估并在同一批次内做 BH 校正。"""
    label = f'fwd_{horizon}'
    metrics: list[FactorMetrics] = []
    for factor in factors:
        ic = _rank_ic(frame, factor, label)
        raw_mean, _, _, _, _, _ = _ic_stats(ic)
        direction = _direction_of(factor, raw_mean)
        try:
            expression = get_spec(factor).expression
        except KeyError:
            expression = ''
        metrics.append(evaluate_factor(
            frame, factor, style_of(factor, expression), direction, horizon,
            sample_dates, label, n_groups))
    q_values = bh_qvalues([item.p_value for item in metrics])
    for item, q in zip(metrics, q_values, strict=True):
        item.q_value = q
    return metrics


def _eligibility_reason(item: FactorMetrics, config: EvalConfig) -> list[str]:
    """单因子入选门槛（顺序短路，返回空列表表示通过）。"""
    if not (np.isfinite(item.ic_mean) and np.isfinite(item.ic_ir)
            and np.isfinite(item.monotonic)):
        return ['IC/IR非有限值']
    if item.coverage < config.min_coverage:
        return [f'覆盖率{item.coverage:.1%}<{config.min_coverage:.0%}']
    if item.ic_mean < config.min_abs_ic:
        return [f'IC{item.ic_mean:.3f}<{config.min_abs_ic:.3f}']
    if item.ic_ir < config.min_ir:
        return [f'IR{item.ic_ir:.2f}<{config.min_ir:.2f}']
    if item.q_value >= config.bh_alpha:
        return [f'BH q={item.q_value:.3f}']
    if item.monotonic < config.min_monotonic:
        return [f'单调性{item.monotonic:.2f}']
    if item.long_short <= 0:
        return ['多空<=0']
    return []


def _prune_collinear(eligible: list[FactorMetrics], corr: pl.DataFrame,
                     config: EvalConfig) -> list[FactorMetrics]:
    """|corr|>阈值 成对合并：按 |IR| 从高到低保留，冲突者剔除。"""
    corr_rows = {row['factor']: row for row in corr.iter_rows(named=True)}
    keep: list[FactorMetrics] = []
    for item in sorted(eligible, key=lambda x: abs(x.ic_ir), reverse=True):
        row = corr_rows.get(item.factor, {})
        if any(
            abs(float(row.get(other.factor) or 0.0)) > config.corr_limit
            for other in keep
        ):
            item.selected = False
            item.reason = f'与已入选因子|corr|>{config.corr_limit:.0%}'
            continue
        keep.append(item)
    return keep


def _cap_by_style(keep: list[FactorMetrics], config: EvalConfig) -> list[FactorMetrics]:
    """风格配额 + 总量上限（``other`` 单列配额，避免无归类因子挤占）。"""
    by_style: dict[str, list[FactorMetrics]] = {}
    for item in keep:
        by_style.setdefault(item.style, []).append(item)
    capped: list[FactorMetrics] = []
    for style, items in by_style.items():
        cap = config.max_other if style == 'other' else config.max_per_style
        chosen = sorted(items, key=lambda x: abs(x.ic_ir), reverse=True)[:cap]
        for item in items:
            if item not in chosen:
                item.reason = f'风格{style}超过{cap}个上限'
        capped.extend(chosen)
    capped.sort(key=lambda x: abs(x.ic_ir), reverse=True)
    if len(capped) > config.max_total:
        for item in capped[config.max_total:]:
            item.reason = f'总量超过{config.max_total}上限'
        capped = capped[:config.max_total]
    return capped


def select_metrics(metrics: list[FactorMetrics], frame: pl.DataFrame,
                   corr: pl.DataFrame, config: EvalConfig) -> list[FactorMetrics]:
    """按覆盖率/IC/IR/BH/单调性/共线性/风格配额筛选。"""
    del frame  # 兼容旧签名，筛选本身只用 metrics/corr
    eligible: list[FactorMetrics] = []
    for item in metrics:
        reasons = _eligibility_reason(item, config)
        item.reason = '；'.join(reasons) if reasons else '通过'
        if not reasons:
            eligible.append(item)

    capped = _cap_by_style(_prune_collinear(eligible, corr, config), config)
    selected_set = {id(item) for item in capped}
    for item in metrics:
        item.selected = id(item) in selected_set
    return [item for item in metrics if item.selected]


def correlation_matrix(frame: pl.DataFrame, factors: list[str],
                       sample_dates: list[date]) -> pl.DataFrame:
    """抽样日期上的截面 rank 相关矩阵（近似平均 Spearman）。

    缺失值按当日中位秩填充（仅用于相关性诊断，不参与打分）；
    polars 的 ``DataFrame.corr()`` 遇到 null 会整列返回 NaN。
    """
    sub = frame.filter(pl.col('date').is_in(sample_dates))
    ranked = sub.select([
        pl.col('date'),
        *[pl.col(name).rank('average').over('date').alias(name) for name in factors],
    ])
    ranked = ranked.with_columns([
        pl.col(name).fill_null(pl.col(name).median().over('date')).alias(name)
        for name in factors
    ]).drop_nulls()
    corr = ranked.select(factors).corr()
    return corr.with_columns(pl.Series('factor', factors)).select(
        ['factor', *factors])


def selection_weights(selected: list[FactorMetrics], baseline: dict[str, float],
                      new_share: float = 0.70) -> dict[str, float]:
    """把训练窗入选因子转成打分权重。

    选 ``new_share`` 作为新因子总权重、组内**等权**（方向取训练窗**原始 IC** 符号，
    而不是已按登记方向对齐后恒为正的 ``ic_mean``），其余 ``1-new_share`` 留给
    legacy 基线。等权而非 IR 加权，避免在 163 条候选上做第二次拟合
    （IR 本身是训练窗内估计量，加权会放大过拟合）。
    """
    out = {name: weight * (1.0 - new_share) for name, weight in baseline.items()}
    if selected and new_share > 0:
        per = new_share / len(selected)
        for item in selected:
            direction = 1.0 if item.ic_mean_raw >= 0 else -1.0
            out[item.factor] = out.get(item.factor, 0.0) + direction * per
    return out


def _walk_forward_windows(dates: list[date],
                          config: EvalConfig) -> list[tuple[date, date, date, date]]:
    """复用 ``src.quant.learning.splits`` 的 purge/embargo 逻辑，返回日期窗口。

    交易日索引由本函数映射为日期；测试窗起点 = 训练窗终点 + embargo，
    训练窗在 ``purged_train_end`` 处截断，保证训练样本标签不跨入测试窗。
    """
    if not dates:
        return []
    step = WalkForwardConfig(
        horizon=config.horizon,
        train_size=max(int(config.train_months * 21), config.horizon + 1),
        test_size=max(int(config.test_months * 21), 1),
        embargo=config.embargo,
        min_train_dates=MIN_HISTORY_ROWS,
        min_test_dates=20,
    )
    bounds: list[tuple[date, date, date, date]] = []
    for window in make_walk_forward_windows(len(dates), step):
        train_dates = dates[window.train_start:window.purged_train_end]
        test_dates = dates[window.test_start:window.test_end]
        if not train_dates or not test_dates:
            continue
        bounds.append((train_dates[0], train_dates[-1],
                       test_dates[0], test_dates[-1]))
    return bounds


def walk_forward(frame: pl.DataFrame, factors: list[str], config: EvalConfig,
                 baseline_weights: dict[str, float] | None = None,
                 new_share: float = 0.70) -> list[WindowResult]:
    """训练窗选因子，测试窗只评估；返回每个窗口的独立结果。

    训练窗已按 ``purged_train_end`` 截断，训练标签不会使用测试窗行情。
    """
    results: list[WindowResult] = []
    scoped = frame.filter(
        (pl.col('date') >= _as_pl_date(config.start))
        & (pl.col('date') <= _as_pl_date(config.end)))
    sample_all = sorted(scoped['date'].unique().to_list())
    sample_dates = sample_all[::5]
    for train_start, train_end, test_start, test_end in _walk_forward_windows(
            sample_all, config):
        train = scoped.filter((pl.col('date') >= train_start)
                              & (pl.col('date') <= train_end))
        test = scoped.filter((pl.col('date') >= test_start)
                             & (pl.col('date') <= test_end))
        if train['date'].n_unique() < 60 or test['date'].n_unique() < 20:  # noqa: PLR2004
            continue
        corr = correlation_matrix(train, factors, sample_dates)
        train_metrics = evaluate_candidates(train, factors, config.horizon,
                                            sample_dates, config.n_groups)
        selected = select_metrics(train_metrics, train, corr, config)
        names = [item.factor for item in selected]
        weights = (selection_weights(selected, baseline_weights, new_share)
                   if baseline_weights else {})
        if not names:
            results.append(WindowResult(
                train_start, train_end, test_start, test_end, weights=weights))
            continue
        test_metrics = evaluate_candidates(test, names, config.horizon,
                                           sample_dates, config.n_groups)
        test_ics = [item.ic_mean for item in test_metrics]
        test_mons = [item.monotonic for item in test_metrics]
        test_ls = [item.long_short for item in test_metrics]
        result = WindowResult(
            train_start=train_start, train_end=train_end,
            test_start=test_start, test_end=test_end, selected=names,
            weights=weights,
            test_metrics={
                'ic_mean': float(np.mean(test_ics)) if test_ics else 0.0,
                'ic_ir': float(np.mean([item.ic_ir for item in test_metrics]))
                if test_metrics else 0.0,
                'ic_positive': float(np.mean([item.ic_positive for item in test_metrics]))
                if test_metrics else 0.0,
                'monotonic': float(np.mean(test_mons)) if test_mons else 0.0,
                'long_short': float(np.mean(test_ls)) if test_ls else 0.0,
                'n_factors': len(names),
                'failed_factors': [item.factor for item in test_metrics
                                   if item.ic_mean < 0 or item.ic_ir < 0],
                'per_factor': {item.factor: round(item.ic_mean, 4)
                               for item in test_metrics},
            })
        results.append(result)
    return results


def build_composite(frame: pl.DataFrame, weights: dict[str, float],
                    min_history: int = 60) -> pl.DataFrame:
    """按日截面 winsorize(1/99)+z-score 加权，缺失按可用权重归一。

    缺失策略：不填 0、不填均值；该票在该因子上缺值时，把它的权重从分母去掉，
    用剩余因子权重归一（分母为 0 才返回 null）。
    """
    out = frame
    cols = [name for name in weights if name in out.columns]
    exprs = []
    for name in cols:
        lo = pl.col(name).quantile(0.01, interpolation='linear').over('date')
        hi = pl.col(name).quantile(0.99, interpolation='linear').over('date')
        clipped = pl.col(name).clip(lo, hi)
        mean = clipped.mean().over('date')
        std = clipped.std().over('date')
        exprs.append(
            pl.when(std.is_not_null() & (std > 1e-9))
            .then((clipped - mean) / std)
            .otherwise(None)
            .clip(-3.0, 3.0)
            .alias(f'__z_{name}'))
    out = out.with_columns(exprs)

    weighted = pl.lit(0.0)
    available = pl.lit(0.0)
    for name, weight in weights.items():
        if name not in cols or weight == 0:
            continue
        weighted = weighted + weight * pl.col(f'__z_{name}')
        available = available + pl.when(pl.col(f'__z_{name}').is_not_null()).then(
            abs(weight)).otherwise(0.0)
    score = pl.when(available > 0).then(weighted / available).otherwise(None)
    return out.with_columns(score.alias('__score')).with_columns(
        (pl.col('history_rows') < min_history).alias('__short_history')
        if 'history_rows' in out.columns else pl.lit(False).alias('__short_history')
    )


@dataclass
class SimConfig:
    """组合模拟参数（净收益口径）。

    成本唯一来源为回测内核 ``src/backtest/costs/cost_model.py``：
    佣金 0.03%（单笔最低 5 元）、印花税按历史时间表（2023-08-28 起卖出 0.05%）、
    过户费 0.002%、滑点 0.02%。``cost_overrides`` 仅用于敏感性/压力对照。
    """

    top_k: int = 20
    hold_days: int = 20
    stress: bool = False
    cash_buffer: float = 0.05
    cost_overrides: dict[str, float] | None = None

    def make_cost_model(self) -> CostModel:
        """构造成本模型；``stress=True`` 时滑点翻倍（只在评估侧生效）。"""
        overrides = dict(self.cost_overrides or {})
        model = CostModel(overrides or None)
        if self.stress:
            overrides['slippage'] = float(model.params['slippage']) * 2.0
            model = CostModel(overrides)
        return model


def _curve_stats(curve: pl.DataFrame, initial: float, n_days: int) -> dict[str, Any]:
    """净值曲线 → 年化 / MDD / Calmar / Sharpe / 月胜率 / 季胜率。"""
    values = curve['equity'].to_numpy() if not curve.is_empty() else np.array([initial])
    final = float(values[-1]) if len(values) else initial
    peaks = np.maximum.accumulate(values)
    drawdowns = (values - peaks) / peaks
    mdd = float(drawdowns.min()) if len(drawdowns) else 0.0
    years = max(1e-9, n_days / 252.0)
    annual = (final / initial) ** (1.0 / years) - 1.0 if final > 0 else -1.0
    calmar = annual / abs(mdd) if mdd < 0 else 0.0
    daily = np.diff(values) / np.where(values[:-1] > 0, values[:-1], np.nan)
    daily = daily[np.isfinite(daily)]
    if len(daily) > 1:
        vol = float(np.std(daily, ddof=1)) * np.sqrt(252.0)
        sharpe = float(np.mean(daily)) / float(np.std(daily, ddof=1)) * np.sqrt(252.0) \
            if float(np.std(daily, ddof=1)) > 1e-12 else 0.0
    else:
        vol, sharpe = 0.0, 0.0
    dated = curve.with_columns(pl.col('date').cast(pl.Date))
    monthly = dated.group_by(pl.col('date').dt.strftime('%Y-%m')).agg(
        pl.col('equity').last()).sort('date')
    monthly_rets = monthly['equity'].pct_change().drop_nulls().to_numpy()
    quarterly = dated.group_by(pl.col('date').dt.strftime('%YQ%q')).agg(
        pl.col('equity').last()).sort('date')
    quarterly_rets = quarterly['equity'].pct_change().drop_nulls().to_numpy()
    return {
        'final': final,
        'net_return': final / initial - 1.0,
        'annualized': float(annual),
        'max_drawdown': mdd,
        'calmar': float(calmar),
        'sharpe': float(sharpe),
        'annual_volatility': vol,
        'monthly_win_rate': float(np.mean(monthly_rets > 0)) if len(monthly_rets) else 0.0,
        'quarterly_win_rate': float(np.mean(quarterly_rets > 0)) if len(quarterly_rets) else 0.0,
    }


def combine_curves(curves: list[pl.DataFrame], initial: float = 1_000_000.0,
                   n_days: int = 0) -> tuple[pl.DataFrame, dict[str, Any]]:
    """把各测试窗净值按复利拼接成一条连续曲线（相邻窗口重复日只保留一次）。"""
    rows: list[dict[str, Any]] = []
    level = initial
    seen: set[Any] = set()
    for curve in curves:
        if curve.is_empty():
            continue
        values = curve['equity'].to_numpy()
        base = float(values[0])
        for day, value in zip(curve['date'].to_list(), values, strict=True):
            if day in seen:
                continue
            seen.add(day)
            rows.append({'date': day, 'equity': level * float(value) / base})
        level = level * float(values[-1]) / base
    combined = pl.DataFrame(rows)
    return combined, _curve_stats(combined, initial, n_days or combined.height)


def simulate_fixed_hold(frame: pl.DataFrame, config: SimConfig) -> tuple[pl.DataFrame, dict[str, Any]]:
    """每日按分数买 Top-K、固定持有 H 日、T+1 开盘成交、含成本的简化组合。

    可成交性：次日开盘相对当日原始收盘的涨跌停以内且次日 volume>0；
    涨跌停幅度用 ``ExecutionRules.resolve_limit_pct``（ST 未知按板块常规幅度）。
    """
    from src.risk.execution_rules import ExecutionRules

    rules = ExecutionRules()
    cost_model = config.make_cost_model()
    dates = sorted(frame['date'].unique().to_list())
    by_date = {day: frame.filter(pl.col('date') == day).to_dicts() for day in dates}
    by_code_by_date = {
        day: {str(row['ts_code']): row for row in rows}
        for day, rows in by_date.items()
    }
    cash = 1_000_000.0
    positions: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []
    buy_notional = 0.0
    trades = 0
    skipped = 0
    initial = cash

    for index in range(1, len(dates)):
        day = dates[index]
        prev = dates[index - 1]
        rows = by_date[prev]          # t 日收盘后可得的分数
        today = by_code_by_date[day]
        prev_rows = by_code_by_date[prev]

        # 1) 卖出：持有满 H 个交易日，今日开盘卖出；跌停/停牌顺延
        still: list[dict[str, Any]] = []
        for pos in positions:
            pos['held'] += 1
            if pos['held'] < config.hold_days:
                still.append(pos)
                continue
            row = today.get(pos['ts_code'])
            prev_row = prev_rows.get(pos['ts_code'])
            if (row is None or not row.get('raw_open')
                    or row.get('volume', 0) <= 0 or prev_row is None):
                still.append(pos)
                continue
            limit = rules.resolve_limit_pct(pos['ts_code'], is_st=None, trade_date=day)
            move = float(row['raw_open']) / float(prev_row['raw_close']) - 1.0
            if move <= -limit + 0.004:
                skipped += 1
                still.append(pos)
                continue
            price = float(row['open'])
            proceeds = pos['shares'] * price
            # 成本唯一来源：回测内核 CostModel（含历史印花税/过户费/滑点）
            cost = cost_model.calculate_sell_cost(
                pos['shares'], price, trade_date=day, ts_code=pos['ts_code'])
            cash += proceeds - cost
            trades += 1
        positions = still

        # 2) 买入：用 t 日分数，t+1 开盘成交；涨停/停牌跳过
        ranked = [
            row for row in rows
            if row.get('__score') is not None
            and not row.get('__short_history')
            and row.get('raw_close')
            and row.get('volume', 0) > 0
        ]
        ranked.sort(key=lambda r: float(r['__score']), reverse=True)
        budget = max(0.0, cash - initial * config.cash_buffer)
        slots = max(0, config.top_k - len(positions))
        for row in ranked[:slots]:
            code = str(row['ts_code'])
            if any(pos['ts_code'] == code for pos in positions):
                continue
            nrow = today.get(code)
            if nrow is None or not nrow.get('raw_open') or nrow.get('volume', 0) <= 0:
                skipped += 1
                continue
            limit = rules.resolve_limit_pct(code, is_st=None, trade_date=day)
            move = abs(float(nrow['raw_open']) / float(row['raw_close']) - 1.0)
            if move >= limit - 0.004:
                skipped += 1
                continue
            weight = 1.0 / max(1, config.top_k)
            notional = min(budget, initial * weight)
            if notional < 1000:
                continue
            price = float(nrow['open'])
            shares = int(notional / price / 100) * 100
            if shares <= 0:
                continue
            cost = cost_model.calculate_buy_cost(
                shares, price, trade_date=day, ts_code=code)
            cash -= shares * price + cost
            budget -= shares * price
            buy_notional += shares * price
            trades += 1
            positions.append({
                'ts_code': code, 'shares': shares, 'price': price, 'held': 0,
            })

        # 3) 收盘估值（停牌按最近成交价）
        mark = cash
        for pos in positions:
            row = today.get(pos['ts_code'])
            mark += pos['shares'] * (
                float(row['close']) if row else pos['price'])
        equity_curve.append({'date': day, 'equity': mark})

    curve = pl.DataFrame(equity_curve)
    stats = _curve_stats(curve, initial, len(dates))
    years = max(1e-9, len(dates) / 252.0)
    average_equity = _as_float(curve['equity'].mean()) if not curve.is_empty() else initial
    return curve, {
        'initial': initial, 'final': stats['final'],
        'net_return': stats['net_return'],
        'annualized': stats['annualized'], 'max_drawdown': stats['max_drawdown'],
        'calmar': stats['calmar'], 'sharpe': stats['sharpe'],
        'annual_volatility': stats['annual_volatility'],
        'monthly_win_rate': stats['monthly_win_rate'],
        'quarterly_win_rate': stats['quarterly_win_rate'],
        'trades': trades, 'skipped': skipped,
        # 单边换手：买入成交额 / 平均权益 / 年（与 replay 的单边口径一致）
        'turnover_annual': float(buy_notional / max(1.0, average_equity) / years),
        'days': len(dates),
        'cost_params': dict(cost_model.params),
        'stress': config.stress,
    }


def ic_monitor(rolling_ic: pl.Series, threshold: float = 0.0,
               consecutive: int = 3) -> dict[str, Any]:
    """失效监控判据：滚动 IC 均值跌破阈值，或连续 N 个窗口为负。

    返回 ``failed`` 与原因。生产侧由 ``src.advice.ic_monitor`` 每日调用。
    """
    values = [float(v) for v in rolling_ic.drop_nulls().to_list()]
    if len(values) < consecutive:
        return {'failed': False, 'reason': '样本不足'}
    last = values[-consecutive:]
    if all(v < 0 for v in last):
        return {'failed': True, 'reason': f'连续{consecutive}个窗口IC为负：{last}'}
    if float(np.mean(last)) < threshold:
        return {'failed': True, 'reason': f'{consecutive}个窗口IC均值{np.mean(last):.4f}<{threshold}'}
    return {'failed': False, 'reason': '正常'}


__all__ = [
    'DEFAULT_N_GROUPS', 'EvalConfig', 'FactorMetrics', 'SimConfig', 'WindowResult',
    'add_forward_returns', 'bh_qvalues', 'build_composite',
    'correlation_matrix', 'evaluate_candidates', 'ic_monitor',
    'select_metrics', 'simulate_fixed_hold', 'walk_forward',
]
