#!/usr/bin/env python3

"""因子面板：复用 ``src.alpha`` 因子注册表，追加推荐所需的衍生列。

不另起一套指标实现：动量/波动率/量能/风险全部走
``calculate_factors_cross_sectional``，与因子评估、回测使用完全同一套公式。
"""

from __future__ import annotations

import polars as pl
from loguru import logger

from src.advice.extended_factors import compute_extended_candidates
from src.alpha.calculator.alpha_calculator import calculate_factors_cross_sectional

#: 推荐打分使用的因子分组（technical 组与 price_momentum 组存在同名列，
#: 不得同时计算，否则后注册的列会被静默跳过）
FACTOR_GROUPS = ('price_momentum', 'volatility', 'volume', 'risk')

#: 因子库输出的打分列 + 本模块追加的衍生列
SCORE_COLUMNS = (
    'momentum_1m', 'momentum_3m', 'momentum_6m', 'momentum_12m',
    'rsi_14', 'macd_hist', 'kdj_k', 'kdj_d',
    'volatility_20', 'volatility_60', 'atr_14',
    'volume_momentum_20', 'volume_price_correlation',
    'sharpe_ratio_20', 'max_drawdown_20', 'downside_risk_20',
    'ma20_gap', 'ma60_gap', 'trend_slope_60', 'atr_pct', 'ret_20',
    'amount_ma20', 'liquidity_log', 'history_rows',
)


def compute_factor_panel(panel: pl.DataFrame, extended: bool = False) -> pl.DataFrame:
    """在本地行情面板上计算因子，并追加推荐层衍生列。

    ``extended=True`` 时额外计算 6 类扩展因子（只算启用的列），并保留缺失值
    供打分层按可用权重归一；``extended=False`` 保持原 12 因子填 0 行为。

    输入必须包含 ``date/ts_code/open/high/low/close/volume/amount``，
    其中价格已由加载器统一为后复权价。输出按 ``(ts_code, date)`` 升序。
    """
    if panel.is_empty():
        return panel
    required = {'date', 'ts_code', 'open', 'high', 'low', 'close', 'volume'}
    missing = required - set(panel.columns)
    if missing:
        raise ValueError(f"因子面板缺少列: {sorted(missing)}")

    frame = panel.sort(['ts_code', 'date'])
    frame = calculate_factors_cross_sectional(
        frame, list(FACTOR_GROUPS), code_col='ts_code')

    partition = 'ts_code'
    close = pl.col('close')
    amount = pl.col('raw_amount').cast(pl.Float64, strict=False)
    ma20 = close.rolling_mean(20).over(partition)
    ma60 = close.rolling_mean(60).over(partition)
    amount_ma20 = amount.rolling_mean(20).over(partition)

    frame = frame.with_columns([
        (close / ma20 - 1.0).alias('ma20_gap'),
        (close / ma60 - 1.0).alias('ma60_gap'),
        (ma20 / ma60 - 1.0).alias('trend_slope_60'),
        (pl.col('atr_14') / close).alias('atr_pct'),
        (close / close.shift(20).over(partition) - 1.0).alias('ret_20'),
        amount_ma20.alias('amount_ma20'),
        (amount_ma20 + 1.0).log().alias('liquidity_log'),
        pl.len().over(partition).cast(pl.Int64).alias('history_rows'),
    ])

    score_cols = [column for column in SCORE_COLUMNS if column in frame.columns]
    if extended:
        from src.advice.factor_sets import enabled_extended_columns

        compat = frame.with_columns([
            pl.col('raw_prev_close').alias('pre_close')
            if 'raw_prev_close' in frame.columns else pl.col('close').shift(1).over(
                'ts_code').alias('pre_close'),
        ])
        frame = compute_extended_candidates(
            compat, columns=enabled_extended_columns(), mask_invalid=True)
        frame = frame.drop('pre_close')
    else:
        frame = frame.with_columns([
            pl.col(column).cast(pl.Float64, strict=False).fill_null(0.0).fill_nan(0.0)
            for column in score_cols
        ])
    logger.info(
        f"[advice] 因子面板完成: {frame.height} 行 × "
        f"{frame[partition].n_unique()} 标的 × {len(score_cols)} 因子列"
        f"{'（extended）' if extended else ''}")
    return frame
