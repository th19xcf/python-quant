#!/usr/bin/env python3

"""组合回测停更/退市标的过滤的回归测试。

对应 P0-1：``run_backtest`` 必须在组合分支按
``meta/universe/stale_status.parquet`` 剔除 ``action='exclude'`` 标的，
并同步裁剪 strategies，避免幸存者偏差与陈旧价格。
"""

from __future__ import annotations

import datetime as dt
import importlib
from unittest.mock import MagicMock

import polars as pl
import pytest

# 包级 __init__ 把 run_backtest 函数重新导出了，必须显式加载子模块
rb = importlib.import_module('src.core.usecases.run_backtest')


def _panel(code: str, n: int = 60) -> pl.DataFrame:
    dates = [dt.date(2024, 1, 1) + dt.timedelta(days=i) for i in range(n)]
    close = [10.0 + i * 0.05 for i in range(n)]
    return pl.DataFrame({
        'date': dates,
        'open': [c * 0.99 for c in close],
        'high': [c * 1.01 for c in close],
        'low': [c * 0.98 for c in close],
        'close': close,
        'volume': [1_000_000] * n,
        'pre_close': [10.0] + close[:-1],
        'pct_chg': [0.0] + [0.5] * (n - 1),
    })


@pytest.fixture
def panel() -> dict[str, pl.DataFrame]:
    return {
        '000001.SZ': _panel('000001.SZ'),
        '600000.SH': _panel('600000.SH'),
        '300750.SZ': _panel('300750.SZ'),
    }


def _patch_stale(monkeypatch, excluded: set[str] | None) -> None:
    from src.data.quality import stale_status

    if excluded is None:
        monkeypatch.setattr(stale_status, 'load_stale_status', lambda *a, **k: None)
    else:
        monkeypatch.setattr(
            stale_status, 'load_stale_status',
            lambda *a, **k: pl.DataFrame({'action': ['exclude']}),
        )
        monkeypatch.setattr(stale_status, 'stale_codes', lambda *a, **k: excluded)


def test_filter_removes_stale_and_clips_strategies(monkeypatch, panel) -> None:
    _patch_stale(monkeypatch, {'600000.SH'})
    strategies = {code: MagicMock() for code in panel}
    warnings: list[dict] = []

    data, kept_strategies, stats = rb._filter_stale_universe(
        panel, strategies, True, warnings,
    )

    assert set(data) == {'000001.SZ', '300750.SZ'}
    assert set(kept_strategies) == set(data)
    assert stats == {
        'enabled': True,
        'universe_before': 3,
        'universe_after': 2,
        'excluded_count': 1,
        'excluded_codes': ['600000.SH'],
    }
    assert warnings == []


def test_filter_disabled_keeps_everything(monkeypatch, panel) -> None:
    _patch_stale(monkeypatch, {'600000.SH'})
    strategies = {code: MagicMock() for code in panel}

    data, kept_strategies, stats = rb._filter_stale_universe(
        panel, strategies, False, [],
    )

    assert data is panel
    assert kept_strategies is strategies
    assert stats is None


def test_missing_status_table_is_degraded_not_silent(monkeypatch, panel) -> None:
    _patch_stale(monkeypatch, None)
    warnings: list[dict] = []

    data, _, stats = rb._filter_stale_universe(panel, {}, True, warnings)

    assert data is panel
    assert stats is None
    assert len(warnings) == 1
    assert warnings[0]['stage'] == 'stale_universe_filter'
    assert warnings[0]['severity'] == 'degraded'


def test_all_stale_raises_instead_of_empty_backtest(monkeypatch, panel) -> None:
    _patch_stale(monkeypatch, set(panel))

    with pytest.raises(ValueError, match='exclude_stale=False'):
        rb._filter_stale_universe(panel, {}, True, [])


def test_single_stock_dataframe_is_not_filtered(monkeypatch, panel) -> None:
    _patch_stale(monkeypatch, {'000001.SZ'})
    single = panel['000001.SZ']

    data, _, stats = rb._filter_stale_universe(single, None, True, [])

    assert data is single
    assert stats is None


def test_no_stale_hit_still_reports_zero_exclusion(monkeypatch, panel) -> None:
    _patch_stale(monkeypatch, set())

    _, _, stats = rb._filter_stale_universe(panel, {}, True, [])

    assert stats is not None
    assert stats['excluded_count'] == 0
    assert stats['universe_before'] == stats['universe_after'] == 3


def test_pit_filter_keeps_codes_that_delist_after_backtest(monkeypatch, panel) -> None:
    """PIT 口径：只用回测开始前的信息剔除，避免前视偏差。"""
    from src.data.quality import stale_status

    frame = pl.DataFrame({
        'ts_code': ['600000.SH', '300750.SZ', '600001.SH'],
        'action': ['exclude', 'exclude', 'exclude'],
        'last_trade_date': [
            dt.date(2020, 1, 1),   # 回测开始前已停更 → 剔除
            dt.date(2024, 2, 1),   # 回测区间内停更 → 保留并告警
            dt.date(2026, 1, 1),   # 回测结束后才停更 → 必须保留
        ],
    })
    monkeypatch.setattr(stale_status, 'load_stale_status', lambda *a, **k: frame)
    monkeypatch.setattr(
        stale_status, 'stale_codes',
        lambda *a, **k: {'600000.SH', '300750.SZ', '600001.SH'})

    data = dict(panel)
    data['600001.SH'] = _panel('600001.SH')
    warnings: list[dict] = []

    kept, _, stats = rb._filter_stale_universe(
        data, {}, True, warnings, start_date='2024-01-01', end_date='2024-03-01')

    assert '600000.SH' not in kept
    assert '300750.SZ' in kept and '600001.SH' in kept
    assert stats['mode'] == 'pit'
    assert stats['pit_cutoff'] == '2024-01-01'
    assert stats['excluded_codes'] == ['600000.SH']
    assert stats['late_stale_count'] == 1
    assert any(w['severity'] == 'warning' for w in warnings)


def test_run_backtest_reports_universe_filter(monkeypatch, panel) -> None:
    """端到端：结果 data_quality 里必须能看到过滤统计。"""
    from src.backtest.strategies.ma_strategy import MAStrategy

    _patch_stale(monkeypatch, {'600000.SH'})
    strategies = {code: MAStrategy(short_window=5, long_window=10) for code in panel}

    result = rb.run_backtest(
        data=panel,
        strategies=strategies,
        st_status={},
        oos_ratio=0,
        enable_default_risk=False,
        apply_dividends=False,
        exclude_stale=True,
    )

    stats = result['data_quality']['universe_filter']
    assert stats['excluded_count'] == 1
    assert stats['universe_before'] == 3
    assert stats['universe_after'] == 2
