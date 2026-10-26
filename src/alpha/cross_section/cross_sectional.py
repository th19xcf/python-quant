#!/usr/bin/env python3

"""
横截面算子：截面排序、去极值、中性化、截面 IC

全部用 Polars 原生表达式实现，按 by（默认 date）在引擎内部分组，
调用方无需逐日 Python 循环，也不需要 DuckDB。

典型链路：
    df = winsorize(panel, ['momentum_1m'])
    df = neutralize(df, ['momentum_1m_win'], industry_col='industry')
    df = cross_sectional_rank(df, ['momentum_1m_win_neu'])
    ic = cross_sectional_ic(df, 'momentum_1m_win_neu_cs_rank', 'fwd_ret_5')
"""

from collections.abc import Iterable
from typing import Any, Dict, List, Union

import pandas as pd
import polars as pl

RANK_SUFFIX = '_cs_rank'
WINSOR_SUFFIX = '_win'
NEUTRAL_SUFFIX = '_neu'


def _as_polars(data: Union[pl.DataFrame, pd.DataFrame]) -> pl.DataFrame:
    return pl.from_pandas(data) if isinstance(data, pd.DataFrame) else data


def _check_cols(df: pl.DataFrame, cols: Iterable[str], what: str) -> List[str]:
    cols = list(cols)
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{what}列不存在: {missing}，可用列: {df.columns}")
    return cols


def cross_sectional_rank(data,
                         factor_cols: Iterable[str],
                         by: str = 'date',
                         method: str = 'average',
                         pct: bool = True,
                         suffix: str = RANK_SUFFIX) -> pl.DataFrame:
    """
    截面排序（截面百分位）

    Args:
        data: 面板数据
        factor_cols: 待排序的因子列
        by: 截面维度列，默认 'date'（同一交易日内比较）
        method: Polars rank 方法，average/min/max/dense/ordinal/random
        pct: True 输出 (0,1] 的百分位，False 输出整数排名
        suffix: 新增列后缀，产出 {col}{suffix}

    Returns:
        新增排名列的 DataFrame，行序不变
    """
    df = _as_polars(data)
    cols = _check_cols(df, factor_cols, '因子')
    if by not in df.columns:
        raise ValueError(f"截面维度列不存在: {by}")

    exprs = []
    for c in cols:
        rank = pl.col(c).rank(method).over(by)
        if pct:
            rank = rank / pl.col(c).count().over(by)
        exprs.append(rank.alias(f'{c}{suffix}'))
    return df.with_columns(exprs)


def winsorize(data,
              factor_cols: Iterable[str],
              by: str = 'date',
              lower: float = 0.01,
              upper: float = 0.99,
              suffix: str = WINSOR_SUFFIX) -> pl.DataFrame:
    """
    截面去极值：按 by 分组把因子截断到 [lower, upper] 分位

    Args:
        lower/upper: 分位点，默认 1% / 99%
        suffix: 新增列后缀，产出 {col}{suffix}

    Returns:
        新增去极值列的 DataFrame，行序不变

    说明：分位点用 linear 插值（Polars 默认是 nearest，会在小样本下退化为
    最大/最小值导致去极值失效）。
    """
    df = _as_polars(data)
    cols = _check_cols(df, factor_cols, '因子')
    if by not in df.columns:
        raise ValueError(f"截面维度列不存在: {by}")
    if not 0 <= lower < upper <= 1:
        raise ValueError(f"分位点非法: lower={lower}, upper={upper}")

    exprs = []
    for c in cols:
        lo = pl.col(c).quantile(lower, interpolation='linear').over(by)
        hi = pl.col(c).quantile(upper, interpolation='linear').over(by)
        exprs.append(pl.col(c).clip(lo, hi).alias(f'{c}{suffix}'))
    return df.with_columns(exprs)


def neutralize(data,
               factor_cols: Iterable[str],
               by: str = 'date',
               industry_col: str | None = None,
               exposure_col: str | None = None,
               suffix: str = NEUTRAL_SUFFIX) -> pl.DataFrame:
    """
    面板中性化

    两种模式，可同时使用（先行业内去均值，再对连续暴露回归取残差）：
      - 行业中性化：在 (by, industry_col) 分组内去均值
      - 暴露中性化：在每个 by 分组内对 exposure_col 做一元 OLS 取残差

    仅支持单个连续暴露的一元回归；多因子回归不在本函数范围内。

    Args:
        industry_col: 行业列名
        exposure_col: 连续暴露列名（如对数市值）
        suffix: 新增列后缀，产出 {col}{suffix}

    Returns:
        新增中性化列的 DataFrame，行序不变

    Raises:
        ValueError: industry_col 与 exposure_col 均未提供
    """
    df = _as_polars(data)
    cols = _check_cols(df, factor_cols, '因子')
    if by not in df.columns:
        raise ValueError(f"截面维度列不存在: {by}")
    if industry_col is None and exposure_col is None:
        raise ValueError("neutralize 需要 industry_col 或 exposure_col 之一")
    if industry_col is not None:
        _check_cols(df, [industry_col], '行业')
    if exposure_col is not None:
        _check_cols(df, [exposure_col], '暴露')

    exprs = []
    for c in cols:
        resid = pl.col(c)
        if industry_col is not None:
            resid = resid - resid.mean().over([by, industry_col])
        if exposure_col is not None:
            e = pl.col(exposure_col)
            mean_x = resid.mean().over(by)
            mean_e = e.mean().over(by)
            centered_e = e - mean_e
            cov = ((resid - mean_x) * centered_e).mean().over(by)
            var = (centered_e ** 2).mean().over(by)
            beta = (cov / var)
            resid = (resid - mean_x) - beta.fill_nan(0.0).fill_null(0.0) * centered_e
        exprs.append(resid.alias(f'{c}{suffix}'))
    return df.with_columns(exprs)


def cross_sectional_ic(data,
                       factor_col: str,
                       return_col: str,
                       by: str = 'date',
                       method: str = 'spearman') -> pl.DataFrame:
    """
    逐期截面 IC

    Args:
        factor_col: 因子列
        return_col: 未来收益列（调用方需已对齐，避免使用未来信息）
        by: 截面维度列，默认 'date'
        method: 'spearman'（Rank IC，默认）或 'pearson'

    Returns:
        DataFrame，列为 [by, 'ic']，按 by 升序
    """
    df = _as_polars(data)
    _check_cols(df, [factor_col, return_col], '因子/收益')
    if by not in df.columns:
        raise ValueError(f"截面维度列不存在: {by}")

    return (df.group_by(by)
              .agg(pl.corr(factor_col, return_col, method=method).alias('ic'))
              .sort(by))


def ic_summary(ic_frame: pl.DataFrame,
               ic_col: str = 'ic') -> Dict[str, Any]:
    """
    截面 IC 汇总指标

    Returns:
        dict: mean_ic（均值）、ic_std（标准差）、ic_ir（均值/标准差）、
              positive_ratio（IC > 0 占比）、n_periods（有效期数）
    """
    series = ic_frame[ic_col].drop_nulls()
    n = series.len()
    if n == 0:
        return {'mean_ic': 0.0, 'ic_std': 0.0, 'ic_ir': 0.0,
                'positive_ratio': 0.0, 'n_periods': 0}
    mean_ic = series.mean()
    std = series.std()
    return {
        'mean_ic': mean_ic,
        'ic_std': std if std is not None else 0.0,
        'ic_ir': (mean_ic / std) if std else 0.0,
        'positive_ratio': (series > 0).sum() / n,
        'n_periods': n,
    }
