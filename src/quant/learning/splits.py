#!/usr/bin/env python3

"""walk-forward 切分：滚动/扩展窗、embargo 与标签 purge。

口径：
- 测试窗从 ``train_end + embargo`` 开始，embargo ≥ 一个持有期；
- 标签在 t 日使用 ``[t+1, t+1+horizon]`` 的交易日区间，因此训练样本必须满足
  ``t + 1 + horizon < test_start``，否则标签会跨入 embargo / 测试窗。
"""

from __future__ import annotations

from collections.abc import Sequence

from src.quant.learning.config import WalkForwardConfig, WalkForwardWindow


def make_walk_forward_windows(n_dates: int,
                              config: WalkForwardConfig) -> list[WalkForwardWindow]:
    """按全局交易日索引生成 walk-forward 窗口。

    Args:
        n_dates: 全局交易日总数（升序）。
        config: 切分配置。

    Returns:
        list[WalkForwardWindow]: 依时间升序的窗口列表；数据不足时返回空列表。
    """
    if n_dates <= 0:
        return []
    windows: list[WalkForwardWindow] = []
    train_end = config.train_size
    index = 0
    while True:
        test_start = train_end + config.embargo
        test_end = min(test_start + config.test_size, n_dates)
        if test_end - test_start < config.min_test_dates:
            break
        # 标签 purge：训练样本的标签不得使用 test_start 及之后的行情
        purged_train_end = min(test_start - config.horizon - 1, train_end)
        train_start = 0 if config.expanding else train_end - config.train_size
        train_start = max(train_start, 0)
        if purged_train_end - train_start < config.min_train_dates:
            train_end += config.test_size
            continue
        windows.append(WalkForwardWindow(
            index=index,
            train_start=train_start,
            train_end=train_end,
            test_start=test_start,
            test_end=test_end,
            purged_train_end=purged_train_end,
        ))
        index += 1
        train_end += config.test_size
    return windows


def date_index_map(dates: Sequence[object]) -> dict[object, int]:
    """构造 交易日 → 全局索引 的映射。"""
    return {day: i for i, day in enumerate(dates)}


def indices_for_window(window: WalkForwardWindow) -> tuple[range, range]:
    """返回 (训练行索引范围, 测试行索引范围)，均已按 purge 截断。"""
    return range(window.train_start, window.purged_train_end), range(
        window.test_start, window.test_end)
