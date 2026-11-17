"""ETF 仓位择时纯函数的单元测试：档位边界、滞回、单日一档与风格槽位。"""

from __future__ import annotations

import pytest

from src.quant.analysis.etf_timing_state import (
    CORE_ETF,
    HYSTERESIS,
    LEVEL_POSITIONS,
    allocation_rows,
    level_position,
    next_thresholds,
    raw_level,
    run_levels,
    sleeve_allocation,
    sleeve_slots,
    step_level,
)


@pytest.mark.parametrize(
    ('score', 'expected'),
    [
        (-1.00, 0),
        (-0.60, 0),
        (-0.599, 1),
        (-0.21, 1),
        (-0.20, 1),
        (-0.19, 2),
        (0.00, 2),
        (0.19, 2),
        (0.20, 3),
        (0.599, 3),
        (0.60, 4),
        (1.00, 4),
    ],
)
def test_raw_level_boundaries(score: float, expected: int) -> None:
    assert raw_level(score) == expected


def test_raw_level_rejects_nan() -> None:
    with pytest.raises(ValueError):
        raw_level(float('nan'))


def test_level_positions_are_five_steps() -> None:
    assert LEVEL_POSITIONS == (0.0, 0.25, 0.50, 0.75, 1.0)
    assert [level_position(level) for level in range(5)] == list(LEVEL_POSITIONS)


def test_hysteresis_blocks_whipsaw() -> None:
    scores = [0.21, 0.19] * 10
    levels = run_levels(scores, initial_level=2)
    assert set(levels) == {2}


def test_hysteresis_zero_reintroduces_whipsaw() -> None:
    """把滞回改成 0 后，同一振荡序列必然来回切换（对应断言会变红）。"""
    scores = [0.21, 0.19] * 10
    levels = run_levels(scores, hysteresis=0.0, initial_level=2)
    switches = sum(1 for a, b in zip(levels, levels[1:], strict=False) if a != b)
    assert switches == 19


def test_single_step_caps_one_level_per_day() -> None:
    levels = run_levels([0.90] * 5, initial_level=0)
    assert levels == [1, 2, 3, 4, 4]


def test_consecutive_band_jump_does_not_skip() -> None:
    """总分从 -0.9 直接跳到 +0.9，也只允许上调一档，不允许连跳。"""
    levels = run_levels([-0.90, -0.90, 0.90, 0.90], initial_level=0)
    assert levels == [0, 0, 1, 2]


def test_step_level_edges() -> None:
    assert step_level(0, -0.90) == 0
    assert step_level(4, 0.90) == 4
    assert step_level(2, 0.39) == 2
    assert step_level(2, 0.40) == 3
    assert step_level(2, -0.39) == 2
    assert step_level(2, -0.40) == 1


@pytest.mark.parametrize(
    ('level', 'up', 'down'),
    [
        (0, -0.60 + HYSTERESIS, None),
        (1, -0.20 + HYSTERESIS, -0.60 - HYSTERESIS),
        (2, 0.20 + HYSTERESIS, -0.20 - HYSTERESIS),
        (3, 0.60 + HYSTERESIS, 0.20 - HYSTERESIS),
        (4, None, 0.60 - HYSTERESIS),
    ],
)
def test_next_thresholds(level: int, up: float | None, down: float | None) -> None:
    assert next_thresholds(level) == (up, down)


def test_replay_is_deterministic_and_prefix_consistent() -> None:
    scores = [-0.70, -0.30, -0.10, 0.25, 0.45, 0.15, -0.51, -0.85, 0.05]
    whole = run_levels(scores)
    assert whole == run_levels(scores)
    for cut in range(1, len(scores) + 1):
        assert run_levels(scores[:cut])[-1] == whole[cut - 1]


def test_style_slots_thresholds() -> None:
    assert sleeve_slots(0.20, 0.20, -0.20) == {'size': '小盘', 'value': '价值', 'vol': '低波'}
    assert sleeve_slots(0.199, -0.199, 0.199) == {'size': '均衡', 'value': '均衡', 'vol': '均衡'}
    assert sleeve_slots(-0.20, -0.20, 0.20) == {'size': '大盘', 'value': '成长', 'vol': '高波'}
    assert sleeve_slots(None, None, None) == {'size': '均衡', 'value': '均衡', 'vol': '均衡'}


def test_sleeve_allocation_weights() -> None:
    slots = sleeve_slots(0.204, 0.287, -0.442)
    weights = sleeve_allocation(slots)
    assert weights == {'510300.SH': 0.30, '512100.SH': 0.25, '510880.SH': 0.25, '512890.SH': 0.20}
    assert abs(sum(weights.values()) - 1.0) < 1e-12

    balanced = sleeve_allocation(sleeve_slots(0.0, 0.0, 0.0))
    assert balanced == {CORE_ETF: 1.0}


def test_allocation_rows_collapses_balanced_slots() -> None:
    rows = allocation_rows(sleeve_slots(0.0, 0.0, 0.0))
    assert rows == [(CORE_ETF, '核心', '沪深300')]
    rows = allocation_rows(sleeve_slots(0.30, 0.30, -0.30))
    assert [code for code, _, _ in rows] == ['510300.SH', '512100.SH', '510880.SH', '512890.SH']


def _sample_state():
    from datetime import date

    from src.quant.analysis.etf_timing_local import EtfTimingHolding, EtfTimingState

    capital = 1_000_000.0
    position = 0.25
    rows = (
        ('510300.SH', '沪深300ETF', '核心·沪深300', 0.30),
        ('512100.SH', '中证1000ETF', '规模·小盘', 0.25),
        ('510880.SH', '红利ETF', '价值·价值', 0.25),
        ('512890.SH', '红利低波ETF', '低波·低波', 0.20),
    )
    holdings = tuple(
        EtfTimingHolding(code, name, role, sleeve, position * sleeve,
                         round(capital * position * sleeve, 2))
        for code, name, role, sleeve in rows)
    return EtfTimingState(
        asof=date(2026, 11, 13), score=-0.571, level=1, label='弱势下行',
        target_position=position, execution_date=date(2026, 11, 16),
        holdings=holdings, cash_weight=0.75, up_threshold=0.0,
        down_threshold=-0.80,
        style_text='规模 +0.204（小盘占优）；价值 +0.287（价值占优）；高波低波 -0.442（低波占优）',
        notes=('信号为 T 日收盘口径，交易按 T+1 开盘执行。',),
    )


def _truncation_count(table) -> int:
    """按字体实测内容宽与列宽比对：宽于列宽的列即被截断的列。"""
    from src.ui.table_sizing import measure_natural_widths

    natural = measure_natural_widths(table)
    return sum(1 for index, width in enumerate(natural)
               if width > table.columnWidth(index) + 1)


def _clipped_row_count(table) -> int:
    """按实测折行高度与行高比对：行高小于内容所需高度的行即被裁切。"""
    from PySide6.QtCore import QRect, Qt

    vertical = table.verticalHeader()
    metrics = table.fontMetrics()
    clipped = 0
    for row in range(table.rowCount()):
        needed = vertical.minimumSectionSize()
        for index in range(1, table.columnCount()):
            item = table.item(row, index)
            if item is None or not item.text():
                continue
            bounds = metrics.boundingRect(
                QRect(0, 0, table.columnWidth(index) - 26, 10000),
                int(Qt.TextFlag.TextWordWrap), item.text())
            needed = max(needed, bounds.height() + 14)
        if table.rowHeight(row) + 1 < needed:
            clipped += 1
    return clipped


def test_page_renders_without_truncation(qapp) -> None:
    """无头渲染：四只 ETF + 现金行，列宽与行高均不裁切（截断 0 条）。"""
    from src.ui.etf_timing import EtfTimingPage

    page = EtfTimingPage()
    try:
        page.render_state(_sample_state())
        table = page.holdings_table
        assert table.rowCount() == 5
        assert table.columnCount() == 7
        assert table.horizontalHeaderItem(0).text() == '序号'
        assert [table.item(row, 0).text() for row in range(5)] == ['1', '2', '3', '4', '5']
        assert table.item(4, 1).text() == 'CASH'
        assert page.position_label.text() == '25%'
        assert '弱势下行' in page.level_label.text()
        assert _truncation_count(table) == 0
        assert _clipped_row_count(table) == 0

        # 三态排序：金额列升序 → 降序 → 取消，回到原始行序
        page._on_header_clicked(table, 6)
        assert table.item(0, 1).text() == '512890.SH'
        page._on_header_clicked(table, 6)
        assert table.item(0, 1).text() == 'CASH'
        page._on_header_clicked(table, 6)
        assert table.item(0, 1).text() == '510300.SH'
        assert [table.item(row, 0).text() for row in range(5)] == ['1', '2', '3', '4', '5']
    finally:
        page.deleteLater()
        qapp.processEvents()
