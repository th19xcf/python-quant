#!/usr/bin/env python3
"""日期归一化契约测试（网页模型第二轮建议第 3 项·阶段 A）。

锁定两件事：
1. `src.utils.dates.as_date` 对全部日期/时间戳表示的行为契约；
2. 19 份存量 `_as_date` 副本已全部收敛到统一真源，此后**不允许出现任何
   新的解析差异**（含 P0-1 修过的 cost_model np.datetime64 静默失效）。
"""
from __future__ import annotations

import importlib
from datetime import date, datetime

import numpy as np
import pandas as pd
import pytest

from src.backtest.costs.cost_model import CostModel
from src.backtest.costs.cost_model import _as_date as cost_as_date
from src.utils.dates import as_date

EXPECTED = date(2026, 1, 5)

#: 契约输入：统一真源必须全部正确归一
CONTRACT_CASES = [
    ("date", EXPECTED),
    ("datetime", datetime(2026, 1, 5, 15, 30)),
    ("pd.Timestamp", pd.Timestamp("2026-01-05")),
    ("np.datetime64", np.datetime64("2026-01-05")),
    ("str_iso", "2026-01-05"),
    ("str_20260105", "20260105"),
]

INVALID_CASES = [("None", None), ("pd.NaT", pd.NaT),
                 ("np.datetime64_NaT", np.datetime64("NaT", "D")),
                 ("bad_str", "not-a-date")]

#: 2026-11-06 契约扫描发现的 5 处差异已全部收敛到 src.utils.dates.as_date：
#: cost_model(np.datetime64)、advice/pipeline 与 advice/regime(datetime/Timestamp)、
#: fund_factors 与 share_capital('YYYYMMDD')。此后**不允许再出现任何差异**。
KNOWN_DIVERGENCES: set[tuple[str, str]] = set()

#: 直接决定 PIT / ST / 存储口径的核心副本，不允许有任何已知差异
CORE_MODULES = (
    "src.backtest.costs.cost_model",
    "src.data.quality.tradable_universe",
    "src.data.local_store.store",
    "src.data.st_history",
)

ALL_COPIES = (
    "src.advice.data", "src.advice.pipeline", "src.advice.regime",
    "src.advice.risk_filters", "src.backtest.costs.cost_model",
    "src.backtest.engine.corporate_actions", "src.data.events",
    "src.data.industry", "src.data.local_store.fund_factors",
    "src.data.local_store.share_capital", "src.data.local_store.store",
    "src.data.quality.anomalies", "src.data.quality.clean_set",
    "src.data.quality.tradable_universe", "src.data.risk_flags",
    "src.data.services.breadth_service", "src.data.services.stock_daily_fill",
    "src.data.st_history", "src.data.st_name_history",
)


@pytest.mark.parametrize("label,value", CONTRACT_CASES)
def test_as_date_normalizes_every_supported_type(label, value):
    result = as_date(value)
    assert result == EXPECTED, f"{label}: {result!r}"
    assert type(result) is date, f"{label} 必须返回 date，不能是 datetime 子类"


@pytest.mark.parametrize("label,value", INVALID_CASES)
def test_as_date_returns_none_for_invalid(label, value):
    assert as_date(value) is None, label


def test_as_date_strict_mode_raises():
    with pytest.raises(ValueError, match="无法解析为日期"):
        as_date("not-a-date", strict=True)


def test_cost_model_delegates_to_single_source():
    """P0-1 残留缺口：np.datetime64 曾让历史费率表静默退回现行静态费率。"""
    for label, value in CONTRACT_CASES:
        assert cost_as_date(value) == as_date(value), label
    assert cost_as_date(np.datetime64("NaT", "D")) is None


def test_historical_fee_schedule_survives_numpy_timestamp():
    """2010 年印花税应为卖方单边 0.1%，不能因时间戳类型退回现行 0.05%。"""
    model = CostModel()
    assert model._stamp_tax_rate(np.datetime64("2010-01-04"), "sell",
                                 "600000.SH") == 0.001
    assert model._stamp_tax_rate(pd.Timestamp("2010-01-04"), "sell",
                                 "600000.SH") == 0.001


def _divergences() -> set[tuple[str, str]]:
    found: set[tuple[str, str]] = set()
    for mod in ALL_COPIES:
        fn = importlib.import_module(mod)._as_date
        for label, value in CONTRACT_CASES:
            try:
                out = fn(value)
            except (TypeError, ValueError):
                found.add((mod, label))
                continue
            if out != EXPECTED or isinstance(out, datetime):
                found.add((mod, label))
    return found


def test_no_new_date_parser_divergence():
    divergences = _divergences()
    new = divergences - KNOWN_DIVERGENCES
    assert not new, f"出现新的日期解析差异（应改用 src.utils.dates.as_date）: {sorted(new)}"
    assert ("src.backtest.costs.cost_model", "np.datetime64") not in divergences
    core_bad = {(m, k) for m, k in divergences if m in CORE_MODULES}
    assert not core_bad, f"PIT/ST/成本核心副本存在日期解析差异: {sorted(core_bad)}"
