"""五个分析页表格表头点击排序：数值按大小、档位按语义、空值沉底。"""

from __future__ import annotations

import pytest
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QTableWidget, QTableWidgetItem

from src.ui.attribution.page import AttributionPage
from src.ui.industry_heat.page import IndustryHeatPage
from src.ui.market_trend.page import MarketTrendPage
from src.ui.portfolio_risk.page import PortfolioRiskPage
from src.ui.style_rotation.page import StyleRotationPage

#: 页面 → 主表格 → 数值列下标 → 档位列下标（None 表示该表无档位列）
PAGES = (
    ('market_trend', MarketTrendPage, 'layer_table', 1, None),
    ('industry_heat', IndustryHeatPage, 'heat_table', 1, 2),
    ('style_rotation', StyleRotationPage, 'dimension_table', 1, 2),
    ('portfolio_risk', PortfolioRiskPage, 'budget_table', 1, None),
    ('attribution', AttributionPage, 'industry_table', 1, None),
)
IDS = [item[0] for item in PAGES]


def _open_page(qapp, page_cls):
    page = page_cls()
    page.resize(1280, 900)
    page.show()
    qapp.processEvents()
    return page


def _fill_column(table: QTableWidget, column: int, texts: list[str]) -> None:
    """按给定顺序灌入行；非被测列用行号占位，保证行内容非空。"""
    table.setRowCount(len(texts))
    for row, text in enumerate(texts):
        for index in range(table.columnCount()):
            item = QTableWidgetItem(text if index == column else f'行{row}')
            if index == column:
                item.setTextAlignment(Qt.AlignmentFlag.AlignRight
                                      | Qt.AlignmentFlag.AlignVCenter)
            table.setItem(row, index, item)


def _column_texts(table: QTableWidget, column: int) -> list[str]:
    return [table.item(row, column).text() for row in range(table.rowCount())]


@pytest.mark.parametrize(('name', 'page_cls', 'table_name', 'numeric_column', '_label'),
                         PAGES, ids=IDS)
def test_numeric_sort_three_state_cycle(
        qapp, name, page_cls, table_name, numeric_column, _label):
    """点击数值列：升序 → 降序 → 取消排序（回到初始行序），空值始终沉底。"""
    page = _open_page(qapp, page_cls)
    try:
        table = getattr(page, table_name)
        header = table.horizontalHeader()
        initial = ['-0.783', '0.843', '—']
        _fill_column(table, numeric_column, initial)
        page._finalize_table(table)
        assert not header.isSortIndicatorShown(), f'{name} 初始不应显示排序箭头'
        assert _column_texts(table, numeric_column) == initial, \
            f'{name} 默认顺序被改动'

        header.sectionClicked.emit(numeric_column)
        qapp.processEvents()
        assert _column_texts(table, numeric_column) == ['-0.783', '0.843', '—'], \
            f'{name} 升序未按数值排序或空值未沉底'
        assert header.isSortIndicatorShown()
        assert header.sortIndicatorOrder() == Qt.SortOrder.AscendingOrder

        header.sectionClicked.emit(numeric_column)
        qapp.processEvents()
        assert _column_texts(table, numeric_column) == ['0.843', '-0.783', '—'], \
            f'{name} 降序未按数值排序或空值未沉底'
        assert header.sortIndicatorOrder() == Qt.SortOrder.DescendingOrder

        header.sectionClicked.emit(numeric_column)
        qapp.processEvents()
        assert _column_texts(table, numeric_column) == initial, \
            f'{name} 第三态未恢复初始行序'
        assert not header.isSortIndicatorShown(), f'{name} 取消排序后箭头未清除'

        header.sectionClicked.emit(numeric_column)
        qapp.processEvents()
        assert header.sortIndicatorOrder() == Qt.SortOrder.AscendingOrder, \
            f'{name} 取消后再次点击未回到升序'
        assert _column_texts(table, numeric_column) == ['-0.783', '0.843', '—']

        item = table.item(1, numeric_column)
        assert (item.textAlignment() & Qt.AlignmentFlag.AlignRight), \
            f'{name} 排序后数值列右对齐丢失'
        heights = [table.rowHeight(row) for row in range(table.rowCount())]
        assert min(heights) >= 30, f'{name} 排序后行高塌陷：{heights}'
    finally:
        page.close()


def test_column_switch_starts_ascending_and_counts_independently(qapp):
    """换列第一击为升序；原列的点数保留，回点原列继续走它自己的三态。"""
    page = _open_page(qapp, IndustryHeatPage)
    try:
        table = page.heat_table
        header = table.horizontalHeader()
        heat_labels = ['冷清', '中性', '过热']
        _fill_column(table, 1, [' 0.2', '0.9', '0.5'])
        for row, label in enumerate(heat_labels):
            table.setItem(row, 2, QTableWidgetItem(label))
        page._finalize_table(table)
        initial_rows = [
            [table.item(row, index).text() for index in range(table.columnCount())]
            for row in range(table.rowCount())
        ]

        header.sectionClicked.emit(1)
        qapp.processEvents()
        assert header.sortIndicatorSection() == 1
        assert header.sortIndicatorOrder() == Qt.SortOrder.AscendingOrder
        header.sectionClicked.emit(1)
        qapp.processEvents()
        assert header.sortIndicatorOrder() == Qt.SortOrder.DescendingOrder

        header.sectionClicked.emit(2)
        qapp.processEvents()
        assert header.sortIndicatorSection() == 2, '换列后排序指示未切换'
        assert header.sortIndicatorOrder() == Qt.SortOrder.AscendingOrder, \
            '换列后第一击不是升序'
        assert _column_texts(table, 2) == ['过热', '中性', '冷清']

        header.sectionClicked.emit(1)
        qapp.processEvents()
        assert not header.isSortIndicatorShown(), '原列第三态未取消排序'
        current_rows = [
            [table.item(row, index).text() for index in range(table.columnCount())]
            for row in range(table.rowCount())
        ]
        assert current_rows == initial_rows, '取消排序后未回到初始行序'
    finally:
        page.close()


def test_heat_label_sort_is_semantic(qapp):
    """行业热度「档位」按 过热→偏热→中性→偏冷→冷清 排序，不按汉字编码。"""
    page = _open_page(qapp, IndustryHeatPage)
    try:
        table = page.heat_table
        _fill_column(table, 2, ['冷清', '中性', '过热', '偏冷', '偏热'])
        page._finalize_table(table)
        table.horizontalHeader().sectionClicked.emit(2)
        qapp.processEvents()
        assert _column_texts(table, 2) == ['过热', '偏热', '中性', '偏冷', '冷清']
    finally:
        page.close()


def test_style_label_sort_is_semantic(qapp):
    """风格轮动「档位」按 正向占优→均衡→反向占优 排序。"""
    page = _open_page(qapp, StyleRotationPage)
    try:
        table = page.dimension_table
        _fill_column(table, 2, ['大盘占优', '均衡', '小盘占优'])
        page._finalize_table(table)
        table.horizontalHeader().sectionClicked.emit(2)
        qapp.processEvents()
        assert _column_texts(table, 2) == ['小盘占优', '均衡', '大盘占优']
    finally:
        page.close()


@pytest.mark.parametrize('module', [
    'src.ui.market_trend.page',
    'src.ui.industry_heat.page',
    'src.ui.style_rotation.page',
    'src.ui.portfolio_risk.page',
    'src.ui.attribution.page',
])
def test_sort_key_parses_numbers_units_and_empty(module):
    """百分号、正负号、乘数、单位与空值文本都转成正确的排序键。"""
    sort_key = __import__(module, fromlist=['_sort_key'])._sort_key
    assert sort_key('-3.35%') == ('n', -3.35)
    assert sort_key('+1.92%') == ('n', 1.92)
    assert sort_key('×0.85') == ('n', 0.85)
    assert sort_key('1,234.5') == ('n', 1234.5)
    assert sort_key('12.34 亿') == ('n', 12.34)
    assert sort_key('-0.783') < sort_key('+0.843')
    assert sort_key('—') is None
    assert sort_key('') is None
    assert sort_key('主要依据：放量上行') == ('s', '主要依据：放量上行')
