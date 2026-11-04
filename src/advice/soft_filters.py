#!/usr/bin/env python3

"""推荐阶段的软筛选（只作用于新开仓，不改变打分）。

与硬性门槛（ST / 仙股 / 流动性 / 事件冷却）不同，软筛选使用**当日截面分位数**
而不是绝对值，避免不同年份波动中枢变化导致阈值失效。

所有条件只用当日及以前数据；缺失值按“不通过”处理并给出原因，绝不默认放行。
"""

from __future__ import annotations

import polars as pl

from src.advice.config import SoftFilterConfig


def _quantile(column: str, q: float) -> pl.Expr:
    return pl.col(column).quantile(q, interpolation='linear').over('date')


def apply_soft_filter(frame: pl.DataFrame,
                      config: SoftFilterConfig) -> pl.DataFrame:
    """给面板追加 ``soft_pass`` 与 ``soft_reason``。

    条件：
    - 均线多头：``ma20_gap > 0`` 且 ``ma60_gap > 0``；
    - 量能：``volume_momentum_20 >= 截面 q60``；
    - 波动：``atr_pct <= 截面 q80``；
    - RSI：``q10 <= rsi_14 <= q90``。
    """
    if frame.is_empty() or not config.enabled:
        return frame
    required = {'ma20_gap', 'ma60_gap', 'volume_momentum_20', 'atr_pct', 'rsi_14'}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f'软筛选缺少列: {sorted(missing)}')

    conditions: list[tuple[str, pl.Expr]] = []
    if config.ma_trend_required:
        conditions.append((
            '均线非多头',
            (pl.col('ma20_gap') > 0) & (pl.col('ma60_gap') > 0),
        ))
    conditions.append((
        '量能不足',
        pl.col('volume_momentum_20') >= _quantile('volume_momentum_20',
                                                  config.volume_quantile),
    ))
    conditions.append((
        '波动过高',
        pl.col('atr_pct') <= _quantile('atr_pct', config.atr_quantile),
    ))
    conditions.append((
        'RSI极值',
        (pl.col('rsi_14') >= _quantile('rsi_14', config.rsi_low))
        & (pl.col('rsi_14') <= _quantile('rsi_14', config.rsi_high)),
    ))

    passes = [expr.fill_null(False).alias(f'__soft_{index}')
              for index, (_, expr) in enumerate(conditions)]
    reasons = [
        pl.when(expr.fill_null(False)).then(None).otherwise(pl.lit(label))
        .alias(f'__reason_{index}')
        for index, (label, expr) in enumerate(conditions)
    ]
    soft_pass = pl.all_horizontal([pl.col(f'__soft_{index}')
                                   for index in range(len(conditions))])
    soft_reason = pl.concat_str(
        [pl.col(f'__reason_{index}') for index in range(len(conditions))],
        separator='；', ignore_nulls=True)
    return (
        frame.with_columns(passes + reasons)
        .with_columns([
            soft_pass.alias('soft_pass'),
            soft_reason.alias('soft_reason'),
        ])
        .drop([f'__soft_{index}' for index in range(len(conditions))]
              + [f'__reason_{index}' for index in range(len(conditions))])
    )


__all__ = ['apply_soft_filter']
