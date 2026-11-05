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


def equal_weight_market_returns(
        panel: pl.DataFrame, *, close_col: str = 'close_hfq',
        ret_col: str | None = None) -> pl.DataFrame:
    """等权全市场日收益。

    Args:
        panel: 含 ``date / ts_code`` 的行情面板。
        close_col: 复权收盘价列名（``ret_col`` 为空时使用）。
        ret_col: 已算好的逐标的日收益列名；给出时直接按日取均值。

    Returns:
        DataFrame，列 ``date / ew_ret``（``ret_col`` 为空时另带 ``n_codes``）。
    """
    if ret_col is not None:
        return (
            panel.select(['date', 'ts_code', ret_col])
            .drop_nulls([ret_col])
            .group_by('date')
            .agg(pl.col(ret_col).mean().alias('ew_ret'), pl.len().alias('n_codes'))
            .sort('date')
        )

    # 与 regimes.market_series 完全同一套聚合（逐标的 shift 收益 → 按日均值）
    from src.quant.learning.regimes import market_series

    if close_col != 'close_hfq':
        panel = panel.rename({close_col: 'close_hfq'})
    return market_series(panel).select(['date', pl.col('mkt_ret').alias('ew_ret'), 'n_codes'])


def cap_weighted_market_returns(
        panel: pl.DataFrame, *, weight_col: str = 'total_mv',
        close_col: str = 'close_hfq') -> pl.DataFrame:
    """市值加权全市场日收益：用前一交易日权重，避免用当日市值引入未来信息。"""
    priced = (
        panel.select(['date', 'ts_code', close_col, weight_col])
        .sort(['ts_code', 'date'])
        .with_columns(
            (pl.col(close_col) / pl.col(close_col).shift(1).over('ts_code') - 1.0).alias('__ret'),
            pl.col(weight_col).shift(1).over('ts_code').alias('__w'),
        )
        .drop_nulls(['__ret', '__w'])
        .filter(pl.col('__w') > 0)
    )
    return (
        priced.group_by('date')
        .agg(
            ((pl.col('__ret') * pl.col('__w')).sum() / pl.col('__w').sum()).alias('mw_ret'),
            pl.len().alias('n_codes'),
        )
        .sort('date')
    )


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
