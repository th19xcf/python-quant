"""五个分析页面表格口径：列宽可拖动、行高按内容自适应并夹在 [MIN, MAX]。"""

from __future__ import annotations

import pytest
from PySide6.QtWidgets import QHeaderView, QTableWidget, QTableWidgetItem

from src.ui.attribution.page import AttributionPage
from src.ui.industry_heat.page import IndustryHeatPage
from src.ui.market_trend.page import (
    HEADER_ROW_HEIGHT,
    MAX_ROW_HEIGHT,
    MIN_ROW_HEIGHT,
    MarketTrendPage,
)
from src.ui.portfolio_risk.page import PortfolioRiskPage
from src.ui.style_rotation.page import StyleRotationPage
from src.ui.table_sizing import MAX_COLUMN_WIDTH, MIN_COLUMN_WIDTH, apply_content_widths

PAGES = (
    ('market_trend', MarketTrendPage, ('layer_table', 'indicator_table')),
    ('style_rotation', StyleRotationPage, ('dimension_table', 'portfolio_table')),
    ('industry_heat', IndustryHeatPage, ('heat_table',)),
    ('portfolio_risk', PortfolioRiskPage,
     ('steps_table', 'budget_table', 'constraint_table')),
    ('attribution', AttributionPage,
     ('brinson_table', 'excess_table', 'industry_table', 'style_table')),
)

#: 用来验证换行的长文本（约 200 个汉字，必然超过单行宽度）
LONG_CELL_TEXT = '长文本说明用于验证换行后行高随内容增长' * 10


def _fill_sample_rows(table: QTableWidget) -> None:
    """灌入一行短文本 + 一行长文本，模拟真实数据行。"""
    table.setRowCount(2)
    for column in range(table.columnCount()):
        table.setItem(0, column, QTableWidgetItem('短'))
        table.setItem(1, column, QTableWidgetItem(LONG_CELL_TEXT))


@pytest.mark.parametrize(('name', 'page_cls', 'table_names'), PAGES,
                         ids=[item[0] for item in PAGES])
def test_all_tables_columns_are_interactive(qapp, name, page_cls, table_names):
    page = page_cls()
    page.resize(1280, 900)
    page.show()
    qapp.processEvents()
    try:
        for table_name in table_names:
            table = getattr(page, table_name)
            header = table.horizontalHeader()
            assert not header.stretchLastSection(), f'{name}.{table_name} 末列不应撑满'
            assert table.columnCount() > 0
            for index in range(table.columnCount()):
                assert (header.sectionResizeMode(index)
                        == QHeaderView.ResizeMode.Interactive), \
                    f'{name}.{table_name} 第 {index} 列不可拖动'
                assert table.columnWidth(index) > 0, \
                    f'{name}.{table_name} 第 {index} 列默认宽度为 0'
            last_width = table.columnWidth(table.columnCount() - 1)
            assert last_width < table.viewport().width() / 2, \
                f'{name}.{table_name} 末列 {last_width}px 被拉满'
    finally:
        page.close()


@pytest.mark.parametrize(('name', 'page_cls', 'table_names'), PAGES,
                         ids=[item[0] for item in PAGES])
def test_all_tables_rows_fit_wrapped_content(qapp, name, page_cls, table_names):
    """行高按换行后的内容自适应：≥ 下限、≤ 上限、长文本行高于短文本行。"""
    page = page_cls()
    page.resize(1280, 900)
    page.show()
    qapp.processEvents()
    try:
        for table_name in table_names:
            table = getattr(page, table_name)
            assert table.wordWrap(), f'{name}.{table_name} 未开启单元格换行'
            vertical = table.verticalHeader()
            assert vertical.minimumSectionSize() == MIN_ROW_HEIGHT, \
                f'{name}.{table_name} 最小行高不是 {MIN_ROW_HEIGHT}px'
            assert vertical.maximumSectionSize() == MAX_ROW_HEIGHT, \
                f'{name}.{table_name} 最大行高不是 {MAX_ROW_HEIGHT}px'
            assert table.horizontalHeader().height() == HEADER_ROW_HEIGHT, \
                f'{name}.{table_name} 表头行高未固定为 {HEADER_ROW_HEIGHT}px'

            _fill_sample_rows(table)
            assert (vertical.sectionResizeMode(0)
                    == QHeaderView.ResizeMode.ResizeToContents), \
                f'{name}.{table_name} 行高未按内容自适应'
            page._finalize_table(table)
            qapp.processEvents()

            heights = [table.rowHeight(row) for row in range(table.rowCount())]
            assert len(heights) == 2
            assert min(heights) >= MIN_ROW_HEIGHT, \
                f'{name}.{table_name} 存在行高 {min(heights)}px 小于下限'
            assert max(heights) <= MAX_ROW_HEIGHT, \
                f'{name}.{table_name} 存在行高 {max(heights)}px 超过上限'
            assert heights[1] > heights[0], \
                f'{name}.{table_name} 长文本行 {heights[1]}px 未高于短文本行 {heights[0]}px'
            assert table.horizontalHeader().height() == HEADER_ROW_HEIGHT, \
                f'{name}.{table_name} 表头行高被数据行带跑'
    finally:
        page.close()


def test_apply_content_widths_autosizes_and_keeps_interactive(qapp):
    table = QTableWidget(1, 3)
    table.setHorizontalHeaderLabels(['短列', '中等文本列', '长文本列'])
    table.setItem(0, 2, QTableWidgetItem('很长很长的说明文字' * 20))
    apply_content_widths(table)
    header = table.horizontalHeader()
    for index in range(3):
        assert header.sectionResizeMode(index) == QHeaderView.ResizeMode.Interactive
        assert table.columnWidth(index) > 0
    assert table.columnWidth(0) >= MIN_COLUMN_WIDTH
    assert table.columnWidth(2) <= MAX_COLUMN_WIDTH
    assert table.columnWidth(2) > table.columnWidth(0)
