#!/usr/bin/env python3

"""回测样本内/样本外切分：purge + embargo。

口径与 ``src.quant.learning.splits`` 完全一致，避免学习层与回测层出现两套
样本外纪律：

- 测试窗从 ``train_end + embargo`` 开始（半开区间）；
- 标签在 t 日使用 ``[t+1, t+1+horizon]``，因此训练样本最后一行必须满足
  ``t + 1 + horizon < test_start``，即
  ``purged_train_end = min(test_start - horizon - 1, train_end)``；
- 训练窗取 ``[0, purged_train_end)``，测试窗取 ``[test_start, test_end)``。

默认 ``horizon=10``、``embargo=20``：默认使用日频中线持有期（10 个交易日）
作为标签跨度，embargo 取两个持有期（20 个交易日），确保 embargo > horizon+1，
即使不做额外 purge 也不会让训练标签跨进测试窗；purge 公式仍保留，调用方
传入更短 embargo 时由它兜底。
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class HoldoutBounds:
    """按行索引描述的样本内/样本外边界（半开区间）。"""

    train_end: int
    purged_train_end: int
    test_start: int
    test_end: int
    horizon: int
    embargo: int

    @property
    def purge_gap_days(self) -> int:
        """训练窗最后一行与测试窗首行之间被隔离的交易日数。"""
        return self.test_start - self.purged_train_end

    @property
    def is_rows(self) -> range:
        return range(0, self.purged_train_end)

    @property
    def oos_rows(self) -> range:
        return range(self.test_start, self.test_end)


def holdout_bounds(n_rows: int, oos_ratio: float,
                   horizon: int = 10, embargo: int = 20,
                   min_rows: int = 20) -> HoldoutBounds | None:
    """计算带 purge/embargo 的单次切分边界；数据不足时返回 None。"""
    if not 0 < oos_ratio < 1:
        raise ValueError(f'oos_ratio 必须在 (0, 1) 内，实际 {oos_ratio}')
    if horizon < 1:
        raise ValueError(f'horizon 必须为正，实际 {horizon}')
    if embargo < 0:
        raise ValueError(f'embargo 必须为非负数，实际 {embargo}')
    if n_rows <= 0:
        return None

    train_end = int(n_rows * (1.0 - oos_ratio))
    test_start = train_end + embargo
    test_end = n_rows
    # 标签 purge：训练样本 t 的标签窗口 [t+1, t+1+horizon] 不得触碰 test_start
    purged_train_end = min(test_start - horizon - 1, train_end)
    if purged_train_end < min_rows or test_end - test_start < min_rows:
        return None
    return HoldoutBounds(
        train_end=train_end,
        purged_train_end=purged_train_end,
        test_start=test_start,
        test_end=test_end,
        horizon=horizon,
        embargo=embargo,
    )


@dataclass(frozen=True)
class FrameSplit:
    """带边界信息的样本内/样本外 DataFrame。"""

    is_df: pl.DataFrame
    oos_df: pl.DataFrame
    bounds: HoldoutBounds


def split_frame_with_purge(df: pl.DataFrame, oos_ratio: float,
                           horizon: int = 10, embargo: int = 20,
                           min_rows: int = 20) -> FrameSplit | None:
    """按行顺序切分 DataFrame；数据不足时返回 None。"""
    bounds = holdout_bounds(df.height, oos_ratio, horizon, embargo, min_rows)
    if bounds is None:
        return None
    return FrameSplit(
        is_df=df.head(bounds.purged_train_end),
        oos_df=df.slice(bounds.test_start, bounds.test_end - bounds.test_start),
        bounds=bounds,
    )
