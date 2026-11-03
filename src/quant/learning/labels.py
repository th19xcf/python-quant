#!/usr/bin/env python3

"""标签构造与特征预处理。

标签严格按「T 日收盘出信号，T+1 开盘建仓，持有 H 个交易日」构造：
``label = open_hfq[t+1+H] / open_hfq[t+1] - 1``。
复权口径统一使用本地列存里的后复权因子（``hfq_factor``）实时乘出价格。
"""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl

LABEL_COL = 'fwd_ret'
DATE_IDX_COL = '__date_idx'


def add_date_index(panel: pl.DataFrame) -> pl.DataFrame:
    """为面板增加全局交易日索引列（按 panel 中出现过的交易日稠密编号）。"""
    return panel.with_columns(
        (pl.col('date').rank('dense').cast(pl.Int64) - 1).alias(DATE_IDX_COL)
    )


def add_adjusted_prices(panel: pl.DataFrame) -> pl.DataFrame:
    """按 ``hfq_factor`` 生成后复权 OHLC，保留原始价供涨跌停判定。

    成交量单位保持原始「手」，成交额保持原始「元」；这里不做任何单位换算。
    """
    factors = {
        'open': 'open_hfq',
        'high': 'high_hfq',
        'low': 'low_hfq',
        'close': 'close_hfq',
    }
    exprs = [
        (pl.col(src) * pl.col('hfq_factor')).alias(dst)
        for src, dst in factors.items()
    ]
    return panel.with_columns(exprs)


def attach_forward_labels(panel: pl.DataFrame, horizon: int,
                          price_col: str = 'open_hfq') -> pl.DataFrame:
    """按标的分区计算 T+1 开盘建仓、持有 H 日的前瞻收益标签。

    末尾 ``horizon + 1`` 行标签为空；调用方应将这些行从训练/测试中排除。
    """
    if horizon <= 0:
        raise ValueError(f'horizon 必须为正，实际 {horizon}')
    return panel.sort(['ts_code', 'date']).with_columns([
        pl.col(price_col).shift(-1).over('ts_code').alias('__entry_price'),
        pl.col(price_col).shift(-(horizon + 1)).over('ts_code').alias('__exit_price'),
    ]).with_columns([
        (pl.col('__exit_price') / pl.col('__entry_price') - 1.0).alias(LABEL_COL),
    ]).drop(['__entry_price', '__exit_price'])


def normalize_features(panel: pl.DataFrame, factor_cols: Sequence[str],
                       suffix: str = '__z') -> pl.DataFrame:
    """截面去极值 + 百分位排序 + 标准化。

    每一步都只用同一交易日的数据，因此可以在整个面板上一次性预计算，
    不会把未来信息带入训练窗。输出列名为 ``{factor}{suffix}``。
    """
    if not factor_cols:
        return panel
    exprs: list[pl.Expr] = []
    for col in factor_cols:
        low = pl.col(col).quantile(0.01, interpolation='linear').over('date')
        high = pl.col(col).quantile(0.99, interpolation='linear').over('date')
        clipped = pl.col(col).clip(low, high)
        rank = clipped.rank('average').over('date')
        count = clipped.count().over('date')
        pct = pl.when(count > 1).then((rank - 1) / (count - 1)).otherwise(0.5)
        mean = pct.mean().over('date')
        std = pct.std().over('date')
        z = pl.when(std > 0).then((pct - mean) / std).otherwise(0.0)
        exprs.append(z.fill_null(0.0).cast(pl.Float64).alias(f'{col}{suffix}'))
    return panel.with_columns(exprs)


def valid_label_filter(panel: pl.DataFrame) -> pl.DataFrame:
    """只保留有前瞻收益标签且标签有限的样本。"""
    return panel.filter(pl.col(LABEL_COL).is_finite())


def feature_columns(factor_cols: Sequence[str], suffix: str = '__z') -> list[str]:
    """返回标准化后的特征列名。"""
    return [f'{col}{suffix}' for col in factor_cols]
