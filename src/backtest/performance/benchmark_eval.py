#!/usr/bin/env python3

"""评估侧基准与显著性工具。

补齐三件此前分散在临时脚本里的能力：

1. **等权全市场基准**——复用 ``src.quant.learning.regimes.market_series``
   的聚合逻辑（逐标的日收益按日等权平均），保证研究侧与评估侧口径一致；
2. **IR**——月度超额序列的年化超额 / 超额波动；
3. **α 与 Newey-West t 值**——对基准做月度回归，协方差矩阵用 HAC
   （默认滞后 3 期，可传 3~6），避免朴素 OLS 低估 t 值。
"""

from __future__ import annotations

import math
from typing import Iterable

import numpy as np
import polars as pl

#: 等权基准最少样本：少于 12 个月不报 IR / α，避免小样本结论。
MIN_MONTHS = 12


def information_ratio(excess_returns: Iterable[float], *,
                      periods_per_year: int = 12) -> float:
    """IR = 年化超额 / 超额波动（月度序列默认 12 期/年）。"""
    x = np.asarray(list(excess_returns), dtype=float)
    x = x[np.isfinite(x)]
    if x.size < 2:
        return float('nan')
    std = float(np.std(x, ddof=1))
    if std == 0:
        return float('nan')
    return float(np.mean(x) * periods_per_year / (std * math.sqrt(periods_per_year)))


def alpha_beta_hac(strategy_returns: Iterable[float],
                   benchmark_returns: Iterable[float], *,
                   periods_per_year: int = 12, maxlags: int = 3) -> dict:
    """对基准做月度回归，返回年化 α、Newey-West t 值、β 与 IR。

    策略与基准按位置对齐（调用方先做月度表 join），同一期内两个序列
    必须成对。样本少于 ``MIN_MONTHS`` 或滞后超过样本量时返回空字典。
    """
    y = np.asarray(list(strategy_returns), dtype=float)
    x = np.asarray(list(benchmark_returns), dtype=float)
    if y.shape != x.shape or y.size < MIN_MONTHS or y.size <= maxlags + 1:
        return {}

    mask = np.isfinite(y) & np.isfinite(x)
    y, x = y[mask], x[mask]
    if y.size < MIN_MONTHS or np.std(x) == 0:
        # 基准无波动时回归不可识别（HAC 会把常数项并掉），直接不报 α
        return {}

    import statsmodels.api as sm

    model = sm.OLS(y, sm.add_constant(x)).fit(
        cov_type='HAC', cov_kwds={'maxlags': int(maxlags)})
    alpha = float(model.params[0])
    beta = float(model.params[1])
    excess = y - x
    return {
        'alpha_ann': alpha * periods_per_year,
        'alpha_t': float(model.tvalues[0]),
        'beta': beta,
        'beta_t': float(model.tvalues[1]),
        'ir': information_ratio(excess, periods_per_year=periods_per_year),
        'excess_ann': float(np.mean(excess) * periods_per_year),
        'n_months': int(y.size),
        'maxlags': int(maxlags),
    }


#: 三段固定样本外切分（起止日，含端点）。
THREE_WAY_OOS_SPLITS: tuple[tuple[str, str], ...] = (
    ('2010-01-01', '2015-12-31'),
    ('2016-01-01', '2020-12-31'),
    ('2021-01-01', '2026-12-31'),
)

EULER_GAMMA = 0.5772156649015329


def deflated_sharpe_ratio(returns: Iterable[float], *, n_trials: int = 1,
                          periods_per_year: int = 12) -> float:
    """超额收益序列的 Deflated Sharpe Ratio（Bailey & López de Prado, 2014）。

    ``n_trials`` 为同一研究内的独立尝试次数；``n_trials=1`` 时退化为
    单次试验的 PSR（阈值取 0）。返回值是「真实夏普 > 0」的概率。
    """
    from scipy.stats import kurtosis, norm, skew

    x = np.asarray(list(returns), dtype=float)
    x = x[np.isfinite(x)]
    n = x.size
    if n < 3 or n_trials < 1:
        return float('nan')
    std = float(np.std(x, ddof=1))
    if std == 0:
        return float('nan')
    sr = float(np.mean(x) / std)
    g3 = float(skew(x, bias=False))
    g4 = float(kurtosis(x, fisher=False, bias=False))
    if not all(np.isfinite(v) for v in (g3, g4)):
        return float('nan')
    denom = 1.0 - g3 * sr + (g4 - 1.0) / 4.0 * sr * sr
    if denom <= 0:
        return float('nan')
    if n_trials <= 1:
        sr0 = 0.0
    else:
        sr0 = math.sqrt(denom / (n - 1)) * (
            (1.0 - EULER_GAMMA) * norm.ppf(1.0 - 1.0 / n_trials)
            + EULER_GAMMA * norm.ppf(1.0 - 1.0 / (n_trials * math.e)))
    z = (sr - sr0) * math.sqrt(n - 1) / math.sqrt(denom)
    return float(norm.cdf(z))


def excess_deflated_sharpe(strategy_returns: Iterable[float],
                           benchmark_returns: Iterable[float], *,
                           n_trials: int = 1,
                           periods_per_year: int = 12) -> float:
    """超额序列（策略 − 基准）的 DSR；两序列按位置对齐。"""
    y = np.asarray(list(strategy_returns), dtype=float)
    b = np.asarray(list(benchmark_returns), dtype=float)
    if y.shape != b.shape or y.size == 0:
        return float('nan')
    mask = np.isfinite(y) & np.isfinite(b)
    return deflated_sharpe_ratio(y[mask] - b[mask], n_trials=n_trials,
                                 periods_per_year=periods_per_year)
