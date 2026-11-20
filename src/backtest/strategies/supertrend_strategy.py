#!/usr/bin/env python3

"""
Supertrend 趋势策略：基于 Supertrend 方向翻转做买卖

BaseBacktestEngine 会在启动前通过 ``_precompute_indicators`` 无条件追加
supertrend / st_lower / st_upper / st_dir 四列，策略只读 ``st_dir``：

- st_dir 从 0/-1 翻到 1 → buy（多头确立）
- st_dir 从 1 翻到 -1   → sell（空头确立）
- 其它                  → hold

参数默认 ATR 10 / 倍数 3（TradingView ``ta.supertrend`` 标准口径）。
"""

from typing import Any, Dict

import polars as pl

from src.backtest.strategies.base_strategy import BaseStrategy


class SupertrendStrategy(BaseStrategy):
    """Supertrend 方向翻转策略。"""

    def __init__(self) -> None:
        super().__init__('Supertrend')
        self.params = {
            'st_period': 10,
            'st_multiplier': 3.0,
        }

    def generate_signal(self, row: Dict[str, Any], index: int) -> str:
        if index < 10:  # ATR 种子期
            return 'hold'
        curr = row.get('st_dir')
        prev = row.get('prev_st_dir')
        if curr is None or prev is None:
            return 'hold'
        if curr == 1 and prev != 1:
            return 'buy'
        if curr == -1 and prev == 1:
            return 'sell'
        return 'hold'

    def generate_signals_vectorized(self, df: pl.DataFrame) -> pl.Series:
        # st_dir 未被预计算时安全降级为全 hold，不抛错
        if 'st_dir' not in df.columns:
            return pl.Series('signal', ['hold'] * df.height, dtype=pl.Utf8)
        curr = pl.col('st_dir')
        prev = curr.shift(1).fill_null(0)
        warmup = pl.int_range(0, df.height) < self.params.get('st_period', 10)
        return df.select(
            pl.when(warmup | curr.is_null() | (curr == 0)).then(pl.lit('hold'))
              .when((curr == 1) & (prev != 1)).then(pl.lit('buy'))
              .when((curr == -1) & (prev == 1)).then(pl.lit('sell'))
              .otherwise(pl.lit('hold'))
              .alias('signal')
        ).to_series()
