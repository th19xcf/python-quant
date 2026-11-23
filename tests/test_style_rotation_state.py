"""风格轮动状态判定：打分、缺失重归一与界面渲染的最小用例。"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

from src.quant.analysis.style_rotation_state import (
    DIMENSIONS,
    evaluate_style_rotation,
    format_dimension_line,
    format_style_detail,
)

_BASE = {
    'ret_market': 0.0,
    'ret_small': 0.0, 'ret_mid': 0.0, 'ret_large': 0.0,
    'ret_value': 0.0, 'ret_growth': 0.0,
    'ret_highvol': 0.0, 'ret_lowvol': 0.0,
    'ret_large_value': 0.0, 'ret_large_growth': 0.0,
    'ret_small_value': 0.0, 'ret_small_growth': 0.0,
    'size_rs20': 0.0, 'size_rs60': 0.0, 'size_amount': 2.0, 'size_turn': 1.0,
    'vg_rs20': 0.0, 'vg_rs60': 0.0, 'vg_pb_gap': 2.0, 'vg_earn_gap': 0.0,
    'vol_rs20': 0.0, 'vol_rs60': 0.0, 'vol_disp': 2.0,
}


def _panel(
    days: int = 300,
    trend: dict[str, float] | None = None,
    drop: set[str] | None = None,
    overrides: dict[str, float] | None = None,
) -> dict[str, pl.DataFrame]:
    """构造小型风格面板：默认全列常数，可按列叠加线性趋势或整列删除。"""
    trend = trend or {}
    drop = drop or set()
    overrides = overrides or {}
    start = date(2024, 1, 1)
    rows: list[dict] = []
    for index in range(days):
        row: dict = {'date': start + timedelta(days=index)}
        for column, base in _BASE.items():
            if column in drop:
                continue
            row[column] = base + trend.get(column, 0.0) * index
        for column, value in overrides.items():
            if column not in drop:
                row[column] = value
        rows.append(row)
    return {'style_daily': pl.DataFrame(rows).with_columns(pl.col('date').cast(pl.Date))}


def test_size_trend_drives_direction():
    state = evaluate_style_rotation(_panel(trend={'size_rs20': 0.001, 'size_rs60': 0.001}))
    size = next(item for item in state.dimensions if item.key == 'size')
    assert size.score > 0.20
    assert size.label == '小盘占优'
    assert state.direction == '小盘占优'
    # 只有规模维度非中性：分化强度 = 0.35 × 维度得分
    assert state.total_score == pytest.approx(DIMENSIONS[0].weight * size.score, abs=1e-6)
    assert all(item.label == '均衡' for item in state.dimensions if item.key != 'size')


def test_missing_value_dimension_renormalizes_weights():
    drop = {'vg_rs20', 'vg_rs60', 'vg_pb_gap', 'vg_earn_gap'}
    state = evaluate_style_rotation(_panel(trend={'size_rs20': 0.001}, drop=drop))
    assert [item.key for item in state.dimensions] == ['size', 'vol']
    assert sum(item.weight for item in state.dimensions) == pytest.approx(1.0)
    assert any('价值成长' in note for note in state.notes)


def test_missing_indicator_within_dimension_is_skipped():
    drop = {'vg_pb_gap', 'vg_earn_gap'}
    state = evaluate_style_rotation(_panel(drop=drop))
    value = next(item for item in state.dimensions if item.key == 'value')
    assert [item.key for item in value.indicators] == ['vg_rs20', 'vg_rs60']
    assert [item.weight for item in value.indicators] == [0.35, 0.35]


def test_short_history_is_not_extrapolated():
    with pytest.raises(ValueError):
        evaluate_style_rotation(_panel(days=100))


def test_portfolio_returns_and_relative_excess():
    overrides = {'ret_large_value': 0.001, 'ret_market': 0.0005}
    state = evaluate_style_rotation(_panel(overrides=overrides))
    portfolio = next(item for item in state.portfolios if item.key == 'large_value')
    assert portfolio.ret20 is not None and portfolio.ret20 > 0.015
    assert portfolio.rel20 is not None and portfolio.rel20 > 0
    assert len(state.portfolios) == 5


def test_format_lines():
    state = evaluate_style_rotation(_panel(trend={'size_rs20': 0.001}))
    size = next(item for item in state.dimensions if item.key == 'size')
    assert '拉动' in format_dimension_line(size)
    detail = format_style_detail(state)
    assert '分化强度' in detail
    assert '大盘价值' in detail


@pytest.mark.skipif(
    not Path('data/local_store/meta/financials/valuation_daily.parquet').exists(),
    reason='本地数据不可用',
)
def test_local_panel_end_to_end():
    from src.quant.analysis.style_rotation_local import load_local_panel

    state = evaluate_style_rotation(load_local_panel())
    assert state.asof.year >= 2024
    assert [item.key for item in state.dimensions] == ['size', 'value', 'vol']
    assert sum(item.weight for item in state.dimensions) == pytest.approx(1.0)
    assert len(state.portfolios) == 5
    assert state.direction.endswith('占优') or state.direction == '风格均衡'
    assert 0.0 <= state.total_score <= 1.0


def test_headless_page_renders_tables(qapp):
    from src.ui.style_rotation import StyleRotationPage

    state = evaluate_style_rotation(_panel(trend={'size_rs20': 0.001}))
    page = StyleRotationPage()
    try:
        page.render_state(state)
        assert page.dimension_table.rowCount() == len(state.dimensions)
        assert page.dimension_table.columnCount() == 5  # 第 0 列为「序号」列（SizedTableMixin 统一插入），数据 4 列 + 序号 1 列
        assert page.dimension_table.showGrid()
        assert page.portfolio_table.rowCount() == len(state.portfolios)
        assert page.portfolio_table.columnCount() == 5  # 第 0 列为「序号」列（SizedTableMixin 统一插入），数据 4 列 + 序号 1 列
        assert page.portfolio_table.showGrid()
        assert '判定完成' in page.status_label.text()
        assert '占优' in page.direction_label.text() or '均衡' in page.direction_label.text()
        assert page.dimension_table.item(0, 3).text() in (  # 序号列使数据列右移 1
            '小盘占优', '大盘占优', '价值占优', '成长占优', '高波占优', '低波占优', '均衡')
    finally:
        page.deleteLater()
