#!/usr/bin/env python3

"""市场状态（regime）划分。

第一阶段只用两个可解释维度，避免为 regime 训练额外模型：
- 波动率分位：全市场等权收益的 20 日年化波动率，在过去 252 个交易日内的分位；
- 趋势强度：等权市场净值相对 60 日均线的偏离方向（上/下）。
两者组合成 3×2 = 6 个状态桶，供 L1 参数自适应按状态选参。

所有统计都只用当日及以前的数据（滚动/扩展计算），无未来函数。
"""

from __future__ import annotations

import numpy as np
import polars as pl
from loguru import logger

VOL_WINDOW = 20
PERCENTILE_WINDOW = 252
TREND_WINDOW = 60
TREND_THRESHOLD = 0.0


def market_series(panel: pl.DataFrame) -> pl.DataFrame:
    """由个股后复权收盘价聚合出全市场等权日收益序列。"""
    if panel.is_empty() or 'close_hfq' not in panel.columns:
        return pl.DataFrame({'date': [], 'mkt_ret': []})
    returns = (
        panel.sort(['ts_code', 'date'])
        .with_columns(
            (pl.col('close_hfq') / pl.col('close_hfq').shift(1).over('ts_code') - 1.0)
            .alias('__ret')
        )
        .group_by('date')
        .agg(
            pl.col('__ret').mean().alias('mkt_ret'),
            pl.col('__ret').count().alias('n_codes'),
        )
        .sort('date')
    )
    return returns


def _trailing_percentile(values: np.ndarray, window: int) -> np.ndarray:
    """逐点计算当前值在过去 window 个样本中的分位（含当前点）。"""
    out = np.zeros(len(values), dtype=float)
    for i, value in enumerate(values):
        if not np.isfinite(value):
            continue
        start = max(0, i - window + 1)
        history = values[start:i + 1]
        history = history[np.isfinite(history)]
        if len(history) == 0:
            continue
        out[i] = float((history <= value).sum()) / float(len(history))
    return out


def classify_regimes(panel: pl.DataFrame) -> pl.DataFrame:
    """输出每个交易日的 regime 标签。

    Returns:
        DataFrame，列为 ``date / vol_pct / trend_strength / regime_vol /
        regime_trend / regime``。
    """
    market = market_series(panel)
    if market.is_empty():
        logger.warning('regime 计算缺少行情面板，返回空表')
        return pl.DataFrame({
            'date': [], 'vol_pct': [], 'trend_strength': [],
            'regime_vol': [], 'regime_trend': [], 'regime': [],
        })

    ret = market['mkt_ret'].fill_null(0.0).to_numpy().astype(float)
    # 20 日滚动波动率（年化）
    vol = np.zeros(len(ret), dtype=float)
    for i in range(len(ret)):
        start = max(0, i - VOL_WINDOW + 1)
        window = ret[start:i + 1]
        if len(window) >= 2:
            vol[i] = float(np.std(window, ddof=0)) * np.sqrt(252.0)
    vol_pct = _trailing_percentile(
        np.where(np.isfinite(vol), vol, np.nan), PERCENTILE_WINDOW)

    # 等权净值相对 60 日均线的偏离
    level = np.cumprod(1.0 + ret)
    ma = np.zeros(len(level), dtype=float)
    for i in range(len(level)):
        start = max(0, i - TREND_WINDOW + 1)
        ma[i] = float(np.mean(level[start:i + 1]))
    trend_strength = np.divide(
        level - ma, ma, out=np.zeros_like(level), where=ma != 0)

    vol_bucket = ['low' if p < 1 / 3 else 'mid' if p < 2 / 3 else 'high'
                  for p in vol_pct]
    trend_bucket = ['up' if value > TREND_THRESHOLD else 'down'
                    for value in trend_strength]

    return market.select('date').with_columns([
        pl.Series('vol_pct', vol_pct, dtype=pl.Float64),
        pl.Series('trend_strength', trend_strength, dtype=pl.Float64),
        pl.Series('regime_vol', vol_bucket, dtype=pl.Utf8),
        pl.Series('regime_trend', trend_bucket, dtype=pl.Utf8),
        pl.Series('regime', [f'{v}_{t}' for v, t in zip(vol_bucket, trend_bucket, strict=True)],
                  dtype=pl.Utf8),
    ])
