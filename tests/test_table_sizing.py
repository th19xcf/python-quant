"""五个分析页面表格列宽口径：全部可拖动、默认宽度自适应、末列不撑满。"""

from __future__ import annotations

import pytest
from PySide6.QtWidgets import QHeaderView, QTableWidget, QTableWidgetItem

from src.ui.attribution.page import AttributionPage
from src.ui.industry_heat.page import IndustryHeatPage
from src.ui.market_trend.page import MarketTrendPage
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
