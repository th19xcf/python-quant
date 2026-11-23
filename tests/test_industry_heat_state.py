"""行业热度判定：纯函数打分、缺失处理与无头界面渲染。"""

from __future__ import annotations

import math
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

from src.quant.analysis.industry_heat_state import (
    HEAT_LABELS,
    evaluate_industry_heat,
    format_heat_detail,
    format_industry_line,
)

_REQUIRED = {
    'rs20': 0.0, 'rs60': 0.0, 'mom60': 1.0, 'dev60': 0.0,
    'share20': 2.0, 'turnover20': 1.0, 'amount_confirm': 1.0,
    'conc': 0.0, 'premium': 0.0,
    'above60': 50.0, 'up_ratio': 50.0, 'highlow_net': 0.0,
    'pe_med': 20.0, 'pb_med': 2.0, 'rev_yoy_med': 10.0, 'np_yoy_med': 10.0,
}


def _panel(
    days: int = 300,
    trend: dict[str, float] | None = None,
    drop: set[str] | None = None,
    stale_codes: set[str] | None = None,
) -> dict[str, pl.DataFrame]:
    """构造小型行业面板：默认三条行业，可按代码给线性趋势与缺失列。"""
    trend = trend or {}
    drop = drop or set()
    stale_codes = stale_codes or set()
    start = date(2024, 1, 1)
    rows: list[dict] = []
    for code in ('A', 'B', 'C', 'D'):
        step = trend.get(code, 0.0)
        limit = 100 if code in stale_codes else days
        for index in range(limit):
            row = {
                'date': start + timedelta(days=index),
                'industry_code': code,
                'industry_name': f'行业{code}',
                'n_stocks': 30,
            }
            for column, base in _REQUIRED.items():
                row[column] = None if column in drop else base + step * index
            rows.append(row)
    return {'industry_daily': pl.DataFrame(rows).with_columns(pl.col('date').cast(pl.Date))}


def test_strength_ordering_and_labels():
    state = evaluate_industry_heat(_panel(trend={'A': 0.05, 'C': -0.05}))
    codes = [heat.code for heat in state.industries]
    assert codes.index('A') < codes.index('B') < codes.index('C')
    assert state.industries[0].label in ('偏热', '过热')
    assert state.industries[-1].label in ('偏冷', '冷清')
    assert all(heat.label in HEAT_LABELS for heat in state.industries)
    assert all(-1.0 <= heat.total_score <= 1.0 for heat in state.industries)


def test_drivers_and_line_format():
    state = evaluate_industry_heat(_panel(trend={'A': 0.05}))
    hottest = state.industries[0]
    assert hottest.drivers and not hottest.drags
    line = format_industry_line(hottest)
    assert hottest.name in line and hottest.label in line
    assert '拉动' in line
    coldest = evaluate_industry_heat(_panel(trend={'C': -0.05})).industries[-1]
    assert coldest.drags and not coldest.drivers
    assert '拖累' in format_industry_line(coldest)
    detail = format_heat_detail(state)
    assert '行业热度状态' in detail and 'shift(1)' in detail


def test_missing_indicator_renormalizes_without_zero_fill():
    partial = evaluate_industry_heat(_panel(trend={'A': 0.05}, drop={'pb_med'}))
    valuation = next(layer for layer in partial.industries[0].layers if layer.key == 'valuation')
    assert [item.key for item in valuation.indicators] == ['pe_q']
    assert valuation.weight == pytest.approx(0.10)

    dropped = evaluate_industry_heat(_panel(
        trend={'A': 0.05},
        drop={'pe_med', 'pb_med', 'rev_yoy_med', 'np_yoy_med'},
    ))
    heat = dropped.industries[0]
    missing = [layer for layer in heat.layers if layer.key in ('valuation', 'fundamental')]
    assert all(layer.score is None for layer in missing)
    # 整层缺失时不留 0 分占位，剩余层权重重新归一化到 1
    assert sum(layer.weight for layer in heat.layers if layer.score is not None) == pytest.approx(1.0)
    assert not math.isnan(heat.total_score)


def test_stale_industry_is_excluded_with_note():
    state = evaluate_industry_heat(_panel(stale_codes={'D'}))
    assert all(heat.code != 'D' for heat in state.industries)
    assert any('不参与判定' in note for note in state.notes)


def test_short_history_industry_is_excluded_not_extrapolated():
    """新行业历史不足 250 日时没有满窗分位，应排除并记 note，而不是外推。"""
    panel = _panel(days=300)
    rows = [
        {**row, 'industry_code': 'E', 'industry_name': '行业E'}
        for row in panel['industry_daily'].filter(pl.col('industry_code') == 'A').head(120).to_dicts()
    ]
    rows.append({**rows[-1], 'date': panel['industry_daily']['date'].max()})
    merged = pl.concat([panel['industry_daily'], pl.DataFrame(rows)])
    state = evaluate_industry_heat({'industry_daily': merged})
    assert all(heat.code != 'E' for heat in state.industries)
    assert any('历史不足 250 日' in note for note in state.notes)


def test_industry_with_too_few_stocks_is_skipped():
    panel = _panel()
    frame = panel['industry_daily'].with_columns(
        pl.when(pl.col('industry_code') == 'C').then(3).otherwise(pl.col('n_stocks')).alias('n_stocks')
    )
    state = evaluate_industry_heat({'industry_daily': frame})
    assert all(heat.code != 'C' for heat in state.industries)
    assert any('有效成分不足' in note for note in state.notes)


def test_rejects_missing_panel():
    with pytest.raises(ValueError):
        evaluate_industry_heat({'industry_daily': pl.DataFrame()})


@pytest.mark.skipif(
    not Path('data/local_store/meta/industry_pit/industry_pit_sw.parquet').exists(),
    reason='本地数据不可用',
)
def test_local_panel_end_to_end():
    from src.quant.analysis.industry_heat_local import load_local_panel

    state = evaluate_industry_heat(load_local_panel())
    assert len(state.industries) >= 25
    assert all(heat.code != '210000' for heat in state.industries), '采掘为 2021 前遗留在用行业'
    assert state.industries == tuple(
        sorted(state.industries, key=lambda heat: heat.total_score, reverse=True)
    )
    assert all(1 <= sum(1 for layer in heat.layers if layer.score is not None) <= 6
               for heat in state.industries)


def test_headless_page_renders_table(qapp):
    from src.ui.industry_heat import IndustryHeatPage

    state = evaluate_industry_heat(_panel(trend={'A': 0.05, 'C': -0.05}))
    page = IndustryHeatPage()
    try:
        page.render_state(state)
        assert page.heat_table.rowCount() == len(state.industries)
        assert page.heat_table.columnCount() == 5  # 第 0 列为「序号」列（SizedTableMixin 统一插入），数据 4 列 + 序号 1 列
        assert page.heat_table.showGrid()
        assert '最热' in page.hottest_label.text()
        assert '最冷' in page.coldest_label.text()
        assert '判定完成' in page.status_label.text()
        assert page.heat_table.item(0, 3).text() in HEAT_LABELS  # 序号列使数据列右移 1
    finally:
        page.deleteLater()
