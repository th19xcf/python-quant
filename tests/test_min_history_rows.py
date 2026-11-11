#!/usr/bin/env python3

"""「最少历史行数」单一真源：四个调用点默认值与边界分支对照。

对照组口径：同一个 ``60`` 门槛下，59 行一律判不足、60 行一律判足够；
四个消费点（推荐配置、面板加载、打分、walk-forward 切分）在边界上
必须同向翻转。
"""

from __future__ import annotations

from datetime import date

import polars as pl

import src.advice.factor_eval as factor_eval
from src.advice.config import TIER_PRESETS, AdviceConfig
from src.advice.data import LocalPanelLoader
from src.advice.factor_eval import EvalConfig
from src.advice.scoring import attach_score
from src.quant.learning.config import MIN_HISTORY_ROWS, WalkForwardConfig
from src.quant.learning.splits import make_walk_forward_windows


def test_four_call_sites_share_the_same_default():
    assert MIN_HISTORY_ROWS == 60
    assert AdviceConfig().min_history_rows == MIN_HISTORY_ROWS
    assert LocalPanelLoader(object()).min_rows == MIN_HISTORY_ROWS
    assert WalkForwardConfig().min_train_dates == MIN_HISTORY_ROWS


def test_factor_eval_passes_shared_constant(monkeypatch):
    """factor_eval 里原来的硬编码 60 已改为引用常量。"""
    captured: dict[str, object] = {}

    class _Recorder(WalkForwardConfig):
        def __init__(self, **kwargs):
            captured.update(kwargs)
            super().__init__(**kwargs)

    monkeypatch.setattr(factor_eval, 'WalkForwardConfig', _Recorder)
    factor_eval._walk_forward_windows(
        [date(2020, 1, 1)] * 300, EvalConfig())
    assert captured['min_train_dates'] == MIN_HISTORY_ROWS


def _null_scores(history_rows: int) -> int:
    tier = TIER_PRESETS['short']
    columns = [spec.column for spec in tier.factors]
    frame = pl.DataFrame({
        'date': [date(2026, 1, 5)] * 4,
        'history_rows': [history_rows] * 4,
        'volume': [100.0] * 4,
        **{column: [1.0, 2.0, 3.0, 4.0] for column in columns},
    })
    out = attach_score(frame, tier, min_history_rows=MIN_HISTORY_ROWS)
    return out['score'].null_count()


def test_scoring_flips_at_the_same_boundary():
    assert _null_scores(MIN_HISTORY_ROWS - 1) == 4
    assert _null_scores(MIN_HISTORY_ROWS) == 0


def test_walk_forward_flips_at_the_same_boundary():
    """训练窗有效跨度 59 行被跳过，60 行保留。"""
    def windows(train_size: int):
        return make_walk_forward_windows(100, WalkForwardConfig(
            horizon=20, train_size=train_size, test_size=10, embargo=20,
            expanding=False, min_train_dates=MIN_HISTORY_ROWS,
            min_test_dates=1))

    # embargo=20 且 horizon=20 时，训练窗有效跨度 = train_size - 1
    assert windows(60) == []
    assert len(windows(61)) > 0
