#!/usr/bin/env python3

"""
因子评估 —— 同一时段、同一股票池、同一成本假设下的 5 分组分层回测。

统一口径（所有因子共用，不允许单因子特调）：
    - 信号在 t 日收盘产生，t+1 日开盘成交，持有 H 个交易日后在开盘卖出；
    - 前视偏差防护：t 日因子只用 t 日及之前数据，收益从 ``open.shift(-1)``
      开始，两者不重叠；
    - 调仓日取「非重叠」窗口（每 H 个交易日一次），避免重叠样本把
      IC 的 t 值放大；
    - 分层用横截面分位排名（1 分组最低、5 分组最高），因子先按登记方向
      统一取向（'-' 取反），因此 Q5 恒为「因子看多端」；
    - 成本用项目 ``CostModel``：买入侧佣金+过户费+滑点，卖出侧再加印花税；
      换手按「上期持仓中本期不再持有的比例」计，单边换手 100% 对应一次
      完整买卖。

输出指标：
    mean_ic / ic_std / icir / t_stat / ic_positive_rate
    q1_mean..q5_mean（各分组每期平均收益）
    q5_minus_q1（多空价差）
    monotonicity（分组序号与分组收益的 Spearman）
    turnover（单边换手率均值）
    ls_gross_cum / ls_net_cum / ls_net_sharpe（多空组合毛/净累计与夏普）
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import polars as pl
from loguru import logger

from src.backtest.costs.cost_model import CostModel
from src.quant.factor_lib.registry import FactorSpec, all_specs

#: 成本估算的名义成交金额（10 万元，避免最低佣金 5 元扭曲费率）
NOMINAL_AMOUNT = 100_000.0


@dataclass(frozen=True, slots=True)
class EvalConfig:
    """评估参数。"""

    horizon: int = 20
    n_groups: int = 5
    min_names: int = 15
    min_periods: int = 6


def orientation(spec: FactorSpec) -> float:
    """登记方向 → 取向系数：'+' 取 1，'-' 取 -1，'?' 默认 1。"""
    return -1.0 if spec.direction == '-' else 1.0


def forward_returns(panel: pl.DataFrame, horizon: int) -> pl.DataFrame:
    """T+1 开盘买入、持有 H 日后开盘卖出的远期收益（复权价空间）。"""
    frame = panel.select(['date', 'ts_code', 'open'])
    return frame.with_columns(
        (
            pl.col('open').shift(-(horizon + 1)) / pl.col('open').shift(-1) - 1.0
        ).over('ts_code').alias('fwd')
    ).select(['date', 'ts_code', 'fwd'])


def rebalance_dates(panel: pl.DataFrame, horizon: int) -> list:
    """非重叠调仓日：按交易日历等间隔取，最后一期留足未来收益。"""
    dates = panel['date'].unique().sort().to_list()
    if len(dates) <= horizon + 1:
        return []
    usable = dates[: -(horizon + 1)]
    return usable[::horizon]


def cost_rates() -> tuple[float, float]:
    """买入/卖出侧成本费率（10 万元名义成交额，避免最低佣金失真）。"""
    model = CostModel()
    price = 10.0
    shares = NOMINAL_AMOUNT / price
    buy = model.calculate_buy_cost(shares, price) / NOMINAL_AMOUNT
    sell = model.calculate_sell_cost(shares, price) / NOMINAL_AMOUNT
    return float(buy), float(sell)


def _ranked_scores(
    values: pl.DataFrame, factor: str, dates: list, config: EvalConfig, sign: float
) -> pl.DataFrame:
    """取调仓日样本，按登记方向取向后再做横截面排名并切分为 N 组。

    取向必须在分组之前：否则 '-' 方向的因子 Q5 会是「原始值最高」即看空端，
    分层单调性与 Q5-Q1 的符号会整体翻转，和登记方向对不上。
    """
    frame = values.filter(pl.col('date').is_in(dates)).select(
        ['date', 'ts_code', factor, 'next_tradable']
    ).drop_nulls(factor)
    # 剔除 T+1 开盘停牌/涨跌停不可成交的标的（口径见 data._with_derived）
    frame = frame.filter(pl.col('next_tradable').fill_null(False))
    frame = frame.with_columns(
        (pl.col(factor) * sign).alias('_oriented'),
    ).with_columns(
        pl.col('_oriented').rank().over('date').alias('_rank'),
        pl.len().over('date').alias('_count'),
    ).filter(pl.col('_count') >= config.min_names)
    return frame.with_columns(
        (
            ((pl.col('_rank') - 1) * config.n_groups / pl.col('_count'))
            .floor().cast(pl.Int8) + 1
        ).clip(1, config.n_groups).alias('_group')
    )


def _compound(returns: list[float]) -> float:
    """把逐期简单收益复合为累计收益（非有限值按 0 处理）。"""
    if not returns:
        return 0.0
    arr = np.asarray(returns, dtype=float)
    arr = np.where(np.isfinite(arr), arr, 0.0)
    return float(np.prod(1.0 + arr) - 1.0)


def _monotonicity(group_means: dict[int, float]) -> float:
    """分组序号与分组平均收益的 Spearman 相关系数。"""
    from scipy.stats import spearmanr

    groups = sorted(group_means)
    values = [group_means[g] for g in groups]
    if any(not np.isfinite(v) for v in values):
        return 0.0
    if len(set(values)) == 1:
        return 0.0
    corr, _p = spearmanr(groups, values)
    return float(corr) if np.isfinite(corr) else 0.0


def evaluate_factor(
    values: pl.DataFrame,
    fwd: pl.DataFrame,
    spec: FactorSpec,
    config: EvalConfig,
    dates: list,
    rates: tuple[float, float],
) -> dict[str, object]:
    """单因子分层评估。"""
    sign = orientation(spec)
    scores = _ranked_scores(values, spec.name, dates, config, sign)
    if scores.is_empty():
        return {'factor': spec.name, 'error': '调仓日样本不足'}

    merged = scores.join(fwd, on=['date', 'ts_code'], how='inner').drop_nulls('fwd')

    scored = merged.with_columns(
        pl.col('_oriented').rank().over('date').alias('_fr'),
        pl.col('fwd').rank().over('date').alias('_rr'),
    )
    # corr 在常数截面上会返回 NaN（不是 null），两者都要剔除，
    # 否则 nanmean/nanstd 会在空切片上报警并污染 ICIR。
    ic_frame = scored.group_by('date').agg(
        pl.corr('_fr', '_rr').alias('ic'),
    ).filter(pl.col('ic').is_not_null() & pl.col('ic').is_not_nan())
    if ic_frame.height < config.min_periods:
        return {'factor': spec.name, 'error': 'IC 有效样本不足'}

    ic = ic_frame['ic'].to_numpy()
    mean_ic = float(np.nanmean(ic))
    ic_std = float(np.nanstd(ic, ddof=1))
    icir = mean_ic / ic_std if ic_std > 0 else 0.0

    group_stats = scored.group_by(['date', '_group']).agg(
        pl.col('fwd').mean().alias('grp_ret'),
        pl.col('ts_code').alias('members'),
    ).sort(['date', '_group'])

    period_returns: dict[int, list[float]] = {g: [] for g in range(1, config.n_groups + 1)}
    period_members: dict[int, list[set[str]]] = {
        g: [] for g in range(1, config.n_groups + 1)
    }
    for row in group_stats.iter_rows(named=True):
        group = int(row['_group'])
        period_returns[group].append(float(row['grp_ret']))
        period_members[group].append(set(row['members']))

    buy_rate, sell_rate = rates
    round_trip = buy_rate + sell_rate

    group_means: dict[int, float] = {}
    group_cum: dict[int, float] = {}
    group_turnover: dict[int, float] = {}
    group_net_cum: dict[int, float] = {}
    for group, returns in period_returns.items():
        held = period_members[group]
        changes = [
            len(prev - cur) / max(len(prev), 1)
            for prev, cur in zip(held[:-1], held[1:], strict=False)
        ]
        turnover = float(np.mean(changes)) if changes else 0.0
        group_turnover[group] = turnover
        group_means[group] = float(np.mean(returns)) if returns else 0.0
        group_cum[group] = _compound(returns)
        group_net_cum[group] = _compound(
            [r - round_trip * turnover for r in returns if np.isfinite(r)]
        )

    top, bottom = config.n_groups, 1
    ls_returns = [
        (1.0 + high) / (1.0 + low) - 1.0
        for high, low in zip(period_returns[top], period_returns[bottom], strict=False)
        if np.isfinite(high) and np.isfinite(low)
    ]
    ls_turnover = (group_turnover[top] + group_turnover[bottom]) / 2.0
    ls_net_returns = [r - round_trip * ls_turnover for r in ls_returns]
    ls_std = float(np.std(ls_net_returns, ddof=1)) if len(ls_net_returns) > 1 else 0.0
    periods_per_year = 252.0 / config.horizon

    return {
        'factor': spec.name,
        'category': spec.category,
        'direction': spec.direction,
        'status': spec.status,
        'n_periods': ic_frame.height,
        'mean_ic': mean_ic,
        'ic_std': ic_std,
        'icir': icir,
        'icir_annual': icir * float(np.sqrt(periods_per_year)),
        't_stat': icir * float(np.sqrt(len(ic))),
        'ic_positive_rate': float(np.mean(ic > 0)),
        'q1_mean': group_means[1],
        'q5_mean': group_means[top],
        'q5_minus_q1': float(np.mean(ls_returns)) if ls_returns else 0.0,
        'monotonicity': _monotonicity(group_means),
        'turnover': float(np.mean(list(group_turnover.values()))),
        'ls_gross_cum': _compound(ls_returns),
        'ls_net_cum': _compound(ls_net_returns),
        'ls_net_sharpe': float(np.mean(ls_net_returns) / ls_std * np.sqrt(periods_per_year))
        if ls_std > 0 else 0.0,
        'q5_net_cum': group_net_cum[top],
        'q1_cum': group_cum[1],
        'q5_cum': group_cum[top],
    }


def evaluate_all(
    values: pl.DataFrame,
    panel: pl.DataFrame,
    config: EvalConfig | None = None,
    specs: list[FactorSpec] | None = None,
) -> pl.DataFrame:
    """批量评估：返回每个因子一行的指标表。"""
    config = config or EvalConfig()
    specs = specs or all_specs()
    specs = [s for s in specs if s.computable and s.name in values.columns]
    fwd = forward_returns(panel, config.horizon)
    dates = rebalance_dates(panel, config.horizon)
    if not dates:
        raise RuntimeError('可用调仓日不足，无法评估')

    rates = cost_rates()
    rows: list[dict[str, object]] = []
    for spec in specs:
        try:
            rows.append(evaluate_factor(values, fwd, spec, config, dates, rates))
        except (pl.exceptions.PolarsError, ValueError, TypeError, ArithmeticError) as exc:
            logger.warning(f'因子评估失败 {spec.name}: {exc}')
            rows.append({'factor': spec.name, 'error': f'{type(exc).__name__}: {exc}'})

    frame = pl.DataFrame(rows, infer_schema_length=None)
    logger.info(
        f'因子评估完成：{frame.height} 个因子，调仓日 {len(dates)} 个，'
        f'成本 买 {rates[0]:.5f} / 卖 {rates[1]:.5f}，持有 {config.horizon} 日'
    )
    return frame


__all__ = [
    'EvalConfig',
    'cost_rates',
    'evaluate_all',
    'evaluate_factor',
    'forward_returns',
    'orientation',
    'rebalance_dates',
]
