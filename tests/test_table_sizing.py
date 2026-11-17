"""分析页面表格口径：列宽可拖动、行高夹在 [MIN, MAX]、底色统一、三态排序。

覆盖「市场趋势」等五个新页面与「财务分析」页、行情表的共享实现。
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from PySide6.QtCore import QRect, Qt
from PySide6.QtGui import QFont, QFontMetrics, QPalette
from PySide6.QtWidgets import QHeaderView, QTableWidget, QTableWidgetItem

from src.ui.attribution.page import AttributionPage
from src.ui.finance.page import FinancePage
from src.ui.industry_heat.page import IndustryHeatPage
from src.ui.market_trend.page import (
    HEADER_ROW_HEIGHT,
    MAX_ROW_HEIGHT,
    MIN_ROW_HEIGHT,
    MarketTrendPage,
)
from src.ui.portfolio_risk.page import PortfolioRiskPage
from src.ui.style_rotation.page import StyleRotationPage
from src.ui.table_interaction_manager import TableInteractionManager
from src.ui.table_sizing import (
    CELL_PADDING_H,
    CELL_PADDING_V,
    FIXED_ROW_HEIGHT,
    HEADER_PADDING_H,
    MAX_COLUMN_WIDTH,
    SEQ_COLUMN_WIDTH,
    WrappedRowDelegate,
    _column_widths,
    apply_content_widths,
    apply_estimated_column_widths,
    apply_wrapped_row_heights,
    measure_natural_widths,
)

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


def _fill_two_short_rows(table: QTableWidget) -> None:
    """只灌两行短文本，保证列宽窄于视口、右侧留出可采样的空白区。"""
    table.setRowCount(2)
    for column in range(table.columnCount()):
        for row in range(2):
            table.setItem(row, column, QTableWidgetItem('短'))


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


def _show_ancestors(table: QTableWidget) -> None:
    """把被默认隐藏的卡片（如 brinson_card）显式打开，保证能取到渲染像素。"""
    parent = table.parentWidget()
    while parent is not None:
        parent.setVisible(True)
        parent = parent.parentWidget()


def _surface_pixel_colors(table: QTableWidget) -> dict[str, str]:
    """采样单元格、末列右侧留白、表头与表头空白区的实际渲染底色。"""
    header = table.horizontalHeader()
    image = table.grab().toImage()
    x_left = 4
    x_blank = table.viewport().width() - 5
    y_header = max(2, header.height() // 2)
    y_row0 = header.height() + table.rowViewportPosition(0) + table.rowHeight(0) // 2
    y_row1 = header.height() + table.rowViewportPosition(1) + table.rowHeight(1) // 2
    points = {
        'cell_row0': (x_left, y_row0),
        'cell_row1': (x_left, y_row1),
        'body_blank_row0': (x_blank, y_row0),
        'body_blank_row1': (x_blank, y_row1),
        'header_section': (x_left, y_header),
        'header_blank': (x_blank, y_header),
    }
    return {key: image.pixelColor(x, y).name() for key, (x, y) in points.items()}


@pytest.mark.parametrize(('name', 'page_cls', 'table_names'), PAGES,
                         ids=[item[0] for item in PAGES])
def test_all_tables_surfaces_share_one_color(qapp, name, page_cls, table_names):
    """单元格、末列右侧留白与表头空白区必须是同一 RGB，不能出现拼接色块。"""
    page = page_cls()
    page.resize(1280, 900)
    page.show()
    qapp.processEvents()
    try:
        for table_name in table_names:
            table = getattr(page, table_name)
            _show_ancestors(table)
            table.setMinimumHeight(200)
            _fill_two_short_rows(table)
            page._finalize_table(table)
            qapp.processEvents()

            total_width = sum(table.columnWidth(index)
                              for index in range(table.columnCount()))
            assert total_width + 20 < table.viewport().width(), \
                f'{name}.{table_name} 列宽已撑满，无法采样末列右侧留白'
            assert not table.alternatingRowColors(), \
                f'{name}.{table_name} 仍启用交替行色，会出现第三种色块'

            expected = table.palette().color(QPalette.ColorRole.Base).name()
            colors = _surface_pixel_colors(table)
            assert set(colors.values()) == {expected}, \
                f'{name}.{table_name} 底色不一致（期望 {expected}）：{colors}'
    finally:
        page.close()


def _shown_table(headers: list[str]) -> QTableWidget:
    """建一张已显示的窄表，保证 viewport 宽度可用。"""
    table = QTableWidget(0, len(headers))
    table.setHorizontalHeaderLabels(headers)
    table.resize(640, 240)
    table.show()
    return table


def test_apply_content_widths_uses_natural_width_when_it_fits(qapp):
    """自然宽度装得下时直接用自然宽度：不压缩、不折行。"""
    table = _shown_table(['短列', '中等文本列', '说明'])
    qapp.processEvents()
    table.setRowCount(1)
    for column, text in enumerate(('短', '中等文本内容', '说明文字')):
        table.setItem(0, column, QTableWidgetItem(text))
    natural = measure_natural_widths(table)
    assert sum(natural) < table.viewport().width()

    wrapped = apply_content_widths(table)
    header = table.horizontalHeader()
    assert not wrapped, '自然宽度装得下时不应标记为折行'
    for index in range(3):
        assert header.sectionResizeMode(index) == QHeaderView.ResizeMode.Interactive
        assert table.columnWidth(index) == natural[index], \
            f'第 {index} 列未使用自然宽度 {natural[index]}'
    qapp.processEvents()


def test_apply_content_widths_keeps_short_columns_and_feeds_long_text(qapp):
    """余量不足时：短列保持自然宽度，长文本列吃满可视区余量后仍标记折行。"""
    table = _shown_table(['短列', '数值', '长文本'])
    qapp.processEvents()
    table.setRowCount(1)
    table.setItem(0, 0, QTableWidgetItem('短'))
    table.setItem(0, 1, QTableWidgetItem('+1.23%'))
    table.setItem(0, 2, QTableWidgetItem('很长的说明文字' * 40))
    natural = measure_natural_widths(table)
    assert natural[2] > MAX_COLUMN_WIDTH

    wrapped = apply_content_widths(table)
    assert wrapped, '余量不足时长文本应标记为折行'
    assert table.columnWidth(0) == natural[0], '短列不应被压缩'
    assert table.columnWidth(1) == natural[1], '数值列不应被压缩'
    assert table.columnWidth(2) > MAX_COLUMN_WIDTH + CELL_PADDING_H, \
        '可视区余量应补给长文本列，不能停在上限'
    assert sum(table.columnWidth(i) for i in range(3)) <= table.viewport().width(), \
        '列宽总和不应超过可视宽度'
    qapp.processEvents()


def test_apply_content_widths_uses_spare_to_avoid_wrapping(qapp):
    """余量充足时：长文本列加宽到单行完整，不再折行。"""
    table = _shown_table(['短列', '说明'])
    table.resize(1400, 240)
    qapp.processEvents()
    table.setRowCount(1)
    table.setItem(0, 0, QTableWidgetItem('短'))
    table.setItem(0, 1, QTableWidgetItem('很长的说明文字' * 15))
    natural = measure_natural_widths(table)
    assert natural[1] > MAX_COLUMN_WIDTH + CELL_PADDING_H, '样本应触发上限截断'
    assert natural[0] + natural[1] < table.viewport().width(), '样本应留有余量'

    wrapped = apply_content_widths(table)
    assert not wrapped, '余量足够时长文本不应再折行'
    assert table.columnWidth(1) == natural[1], \
        f'折行列应加宽到自然宽度 {natural[1]}，实际 {table.columnWidth(1)}'
    qapp.processEvents()


def test_equal_length_text_uses_pixel_width_not_char_count():
    """等长文本字形宽度不同：'-0.990' 与 '+0.173' 字符数相同，列宽必须按像素取宽。

    离屏平台的字体是等宽回退（所有字形同宽），无法用真实 QFontMetrics 复现
    4px 差异，这里用可注入的度量器直接验证取最大像素宽的实现。
    """
    class _Metrics:
        def __init__(self, widths: dict[str, int]):
            self.widths = widths

        def horizontalAdvance(self, text: str) -> int:  # noqa: N802
            return self.widths[text]

    header = _Metrics({'得分': 10})
    cell = _Metrics({'-0.990': 20, '+0.173': 24})
    widths = _column_widths(['得分'], [['-0.990'], ['+0.173']], header, cell)
    assert widths[0] == 24 + CELL_PADDING_H, '等长文本应按像素取最宽的 +0.173'
    assert len('-0.990') == len('+0.173')


def test_wrapped_delegate_height_includes_cell_padding(qapp):
    """折行行高由委托按实测文本高度给出（含内边距），最后一行不被纵向裁切。"""
    table = _shown_table(['说明'])
    table.setColumnWidth(0, 160)
    table.setRowCount(1)
    text = '折行行高需要覆盖 QSS 内边距否则末行会被裁切' * 3
    table.setItem(0, 0, QTableWidgetItem(text))
    table.setItemDelegate(WrappedRowDelegate(table))
    table.resizeRowsToContents()
    qapp.processEvents()

    needed = table.fontMetrics().boundingRect(
        QRect(0, 0, 160 - CELL_PADDING_H, 10000),
        int(Qt.TextFlag.TextWordWrap), text).height() + CELL_PADDING_V
    assert table.rowHeight(0) >= needed, \
        f'行高 {table.rowHeight(0)}px 小于折行所需 {needed}px'
    qapp.processEvents()


def test_apply_wrapped_row_heights_follows_content(qapp):
    """折行表按内容实测行高：长文本行更高，且不突破上限。"""
    table = _shown_table(['列', '说明'])
    table.setColumnWidth(1, 200)
    table.setRowCount(2)
    table.setItem(0, 0, QTableWidgetItem('a'))
    table.setItem(0, 1, QTableWidgetItem('短'))
    table.setItem(1, 0, QTableWidgetItem('b'))
    table.setItem(1, 1, QTableWidgetItem('很长的说明文字' * 20))

    apply_wrapped_row_heights(table, MIN_ROW_HEIGHT, 120)
    assert (table.verticalHeader().sectionResizeMode(0)
            == QHeaderView.ResizeMode.Fixed), '折行大表应改用固定行高模式'
    assert table.rowHeight(0) == MIN_ROW_HEIGHT
    assert MIN_ROW_HEIGHT < table.rowHeight(1) <= 120
    qapp.processEvents()


# --------------------------------------------------------------- 财务分析页
HISTORY_ROWS = [
    {'label': 'C', 'values': ['5.00%', '4.80%']},
    {'label': 'A', 'values': ['10.50%', '9.80%']},
    {'label': 'B', 'values': ['2.00%', '—']},
    {'label': 'D', 'values': ['—', '—']},
]

NEWS_ITEMS = [
    {'time': '2026-02-01 09:00', 'type': '新闻', 'title': '后一条',
     'source': '来源乙', 'summary': '摘要乙'},
    {'time': '2026-01-01 09:00', 'type': '公告', 'title': '前一条',
     'source': '来源甲', 'summary': '摘要甲'},
]


def _finance_page(qapp) -> FinancePage:
    page = FinancePage(task_manager=object())
    page.resize(1280, 900)
    page.show()
    qapp.processEvents()
    return page


def _render_finance(page: FinancePage, history=None, news=None) -> None:
    page._render_history(history or {'periods': ['2026-06-30', '2026-09-30'],
                                     'rows': HISTORY_ROWS})
    page._render_news(NEWS_ITEMS if news is None else news)
    page.update()


def test_finance_tables_follow_shared_table_spec(qapp):
    """序号列在最左、宽 50、居中，关交替行色、开分隔线、列宽可拖动。"""
    page = _finance_page(qapp)
    try:
        _render_finance(page)
        qapp.processEvents()
        for table in (page.history_table, page.news_table):
            assert not table.alternatingRowColors(), '财务页仍在用交替行色'
            assert table.showGrid(), '财务页分隔线不可见'
            assert table.horizontalHeaderItem(0).text() == '序号'
            assert table.columnWidth(0) == SEQ_COLUMN_WIDTH
            seq = table.item(0, 0)
            assert seq.text() == '1'
            assert seq.textAlignment() & Qt.AlignmentFlag.AlignHCenter
            header = table.horizontalHeader()
            assert not header.stretchLastSection()
            for index in range(table.columnCount()):
                assert (header.sectionResizeMode(index)
                        == QHeaderView.ResizeMode.Interactive)
        _show_ancestors(page.history_table)
        page.history_table.setMinimumHeight(200)
        page._finalize_table(page.history_table)
        qapp.processEvents()
        expected = page.history_table.palette().color(
            QPalette.ColorRole.Base).name()
        assert set(_surface_pixel_colors(page.history_table).values()) == {expected}
    finally:
        page.close()


def test_finance_history_three_state_sort_uses_parsed_values(qapp):
    """升序 → 降序 → 取消，数值按解析值比较、空值沉底、取消还原默认行序。"""
    page = _finance_page(qapp)
    try:
        _render_finance(page)
        qapp.processEvents()
        table = page.history_table
        labels = lambda: [table.item(row, 1).text()  # noqa: E731
                          for row in range(table.rowCount())]
        assert labels() == ['C', 'A', 'B', 'D']

        page._on_header_clicked(table, 2)              # 升序
        assert labels() == ['B', 'C', 'A', 'D']
        assert [table.item(row, 0).text()
                for row in range(table.rowCount())] == ['1', '2', '3', '4']

        page._on_header_clicked(table, 2)              # 降序
        assert labels() == ['A', 'C', 'B', 'D']

        page._on_header_clicked(table, 2)              # 取消
        assert labels() == ['C', 'A', 'B', 'D']
        assert not table.horizontalHeader().isSortIndicatorShown()

        page._on_header_clicked(table, 0)              # 序号列不参与排序
        assert labels() == ['C', 'A', 'B', 'D']
    finally:
        page.close()


def test_finance_news_click_maps_to_row_after_sort(qapp):
    """排序后单击按单元格里的原始下标回查，不会读错条目。"""
    page = _finance_page(qapp)
    try:
        _render_finance(page)
        qapp.processEvents()
        page._on_header_clicked(page.news_table, 1)     # 时间升序
        assert page.news_table.item(0, 3).text() == '前一条'
        page._on_news_clicked(0, 3)
        assert '前一条' in page.news_detail.text()
    finally:
        page.close()


def test_finance_large_table_uses_fixed_rows_and_tooltips(qapp):
    """行数 >200 且文本折行时固定行高 32px，并给长文本补 tooltip。"""
    page = _finance_page(qapp)
    try:
        rows = [{'label': f'长文本指标名称用于验证固定行高与提示{index:03d}',
                 'values': ['12.34%']} for index in range(201)]
        page._render_history({'periods': ['2026-09-30'], 'rows': rows})
        qapp.processEvents()
        table = page.history_table
        assert table.rowCount() == 201
        assert table.rowHeight(0) == FIXED_ROW_HEIGHT
        assert table.item(0, 1).toolTip(), '大表长文本没有 tooltip'
    finally:
        page.close()


# ------------------------------------------------------------------- 行情表
class _FakeMarketWindow:
    """只提供 TableInteractionManager 需要的字段。"""

    def __init__(self, table: QTableWidget) -> None:
        self.stock_table = table
        self.column_sort_states: dict[int, int] = {}
        self.current_sorted_column = -1


def test_market_table_three_state_sort_is_numeric(qapp):
    """行情表按解析值排序：+10.50 不能排在 +9.80 前面；取消还原默认行序。"""
    table = _shown_table(['日期', '代码', '名称', '涨跌幅%'])
    fill = [('2026-11-14', '000001.SZ', '乙', '+9.80'),
            ('2026-11-14', '600000.SH', '甲', '+10.50'),
            ('2026-11-13', '000002.SZ', '丙', '-1.00')]
    table.setRowCount(len(fill))
    for row, values in enumerate(fill):
        for column, value in enumerate(values):
            table.setItem(row, column, QTableWidgetItem(value))
    qapp.processEvents()

    manager = TableInteractionManager(_FakeMarketWindow(table))
    manager.capture_default_order()
    names = lambda: [table.item(row, 2).text()  # noqa: E731
                     for row in range(table.rowCount())]

    manager.on_header_clicked(3)
    assert names() == ['丙', '乙', '甲']
    manager.on_header_clicked(3)
    assert names() == ['甲', '乙', '丙']
    manager.on_header_clicked(3)
    assert names() == ['乙', '甲', '丙'], '取消排序未还原默认行序'
    assert table.item(0, 3).text() == '+9.80'


def test_estimated_column_widths_use_longest_text_once(qapp):
    """按每列最长文本估宽：装得下最长内容，短列不再被 70px 下限撑宽。"""
    table = _shown_table(['代码', '名称'])
    rows = [['600519.SH', '贵州茅台'], ['000001.SZ', '中国石油天然气']]
    table.setRowCount(len(rows))
    for row, values in enumerate(rows):
        for column, value in enumerate(values):
            table.setItem(row, column, QTableWidgetItem(value))
    qapp.processEvents()
    metrics = table.fontMetrics()

    apply_estimated_column_widths(table, rows)

    assert table.columnWidth(0) >= metrics.horizontalAdvance('600519.SH')
    assert table.columnWidth(1) >= metrics.horizontalAdvance('中国石油天然气')
    assert table.columnWidth(1) < 70 + 100, '短列不应被旧的 70px 下限撑宽'


def test_estimated_column_widths_respect_long_header(qapp):
    """表头比内容长时按表头宽度估宽，表头不被截断。"""
    table = _shown_table(['涨跌幅百分比'])
    rows = [['+1.0'], ['-2.5']]
    table.setRowCount(len(rows))
    for row, values in enumerate(rows):
        table.setItem(row, 0, QTableWidgetItem(values[0]))
    qapp.processEvents()
    header_metrics = table.horizontalHeader().fontMetrics()

    apply_estimated_column_widths(table, rows)

    header_width = header_metrics.horizontalAdvance('涨跌幅百分比') + HEADER_PADDING_H
    assert table.columnWidth(0) >= header_width - 1, '表头比内容长时列宽按表头计算'


def test_content_widths_measured_with_polished_stylesheet_font(qapp):
    """QSS 字号在 polish 后才生效：填充后立即估宽也必须按最终字号，不能偏窄。"""
    table = QTableWidget(1, 1)
    table.setHorizontalHeaderLabels(['数值'])
    table.setItem(0, 0, QTableWidgetItem('1,234.56'))
    table.setStyleSheet('QTableWidget { font-size: 20px; }')

    apply_content_widths(table)

    table.resize(400, 200)
    table.show()
    qapp.processEvents()
    needed = table.fontMetrics().horizontalAdvance('1,234.56') + CELL_PADDING_H
    assert table.columnWidth(0) >= needed, '按 polish 前的默认字号估宽，短列被截断'
    table.close()


def test_header_width_uses_semibold_metrics(qapp):
    """表头 QSS 是 600 字重：列宽要按半粗度量，不能按常规字重偏窄。"""
    table = QTableWidget(1, 1)
    table.setHorizontalHeaderLabels(['涨跌幅百分比'])
    table.setItem(0, 0, QTableWidgetItem('1'))
    table.setStyleSheet('QHeaderView::section { font-weight: 600; }')

    apply_content_widths(table)

    header = table.horizontalHeader()
    bold = QFont(header.font())
    bold.setWeight(QFont.Weight.DemiBold)
    needed = QFontMetrics(bold).horizontalAdvance('涨跌幅百分比') + HEADER_PADDING_H
    assert table.columnWidth(0) >= needed, '表头按常规字重度量为偏窄'
    table.close()


#: 真实渲染实测的单元格左右内缩之和（独立于实现常量，实现改小即断言变红）
MEASURED_CELL_PADDING_H = 26


def test_rendered_page_columns_fit_longest_text_plus_padding(qapp):
    """真实页面渲染后逐列断言：列宽 ≥ 最长文本 advance + 实测内缩。

    样本同时包含「等长异宽」的 -0.990 / +0.990 与各列最宽的真实行；表头按
    QSS 的半粗字重单独度量，序号列按表头半粗字宽度量。
    """
    page = MarketTrendPage()
    page.resize(1280, 900)
    page.show()
    qapp.processEvents()
    try:
        table = page.layer_table
        page._prepare_table(table)
        rows = [
            ('量价同步', '-0.990', '+0.990', '12.5%', '价涨量增'),
            ('估值修复', '+0.990', '-0.990', '8.0%', '分位偏低'),
        ]
        table.setRowCount(len(rows))
        for row, values in enumerate(rows):
            for column, text in enumerate(values):
                table.setItem(row, column, QTableWidgetItem(text))
        page._finalize_table(table)
        table.repaint()
        qapp.processEvents()

        header = table.horizontalHeader()
        header_font = QFont(header.font())
        header_font.setWeight(QFont.Weight.DemiBold)
        header_metrics = QFontMetrics(header_font)
        cell_metrics = table.fontMetrics()
        for column in range(table.columnCount()):
            header_text = table.horizontalHeaderItem(column).text()
            header_needed = (header_metrics.horizontalAdvance(header_text)
                             + HEADER_PADDING_H)
            if column == 0:
                needed = max(SEQ_COLUMN_WIDTH, header_needed)
            else:
                cell_needed = max(
                    cell_metrics.horizontalAdvance(
                        table.item(row, column).text())
                    for row in range(table.rowCount())) + MEASURED_CELL_PADDING_H
                needed = max(header_needed, cell_needed)
            assert table.columnWidth(column) >= needed, (
                f'第 {column} 列「{header_text}」宽 {table.columnWidth(column)}px'
                f' < 所需 {needed}px（最长文本 + 两侧内缩）')
    finally:
        page.close()


def test_estimated_column_widths_caps_when_no_spare(qapp):
    """可视区没有余量时，超长文本仍按上限截断列宽，由横向滚动条承接。"""
    table = _shown_table(['备注'])
    table.resize(300, 240)
    long_text = '很长的说明文字' * 12
    table.setRowCount(1)
    table.setItem(0, 0, QTableWidgetItem(long_text))
    qapp.processEvents()

    apply_estimated_column_widths(table, [[long_text]])

    assert table.columnWidth(0) == MAX_COLUMN_WIDTH + CELL_PADDING_H
    assert table.columnWidth(0) > table.viewport().width()


# ----------------------------------------------- 列宽口径守门：页面不得自定列宽
UI_ROOT = Path(__file__).resolve().parents[1] / 'src' / 'ui'
#: 共享度量实现本身定义常量；页面一律不得写死列宽
GUARD_EXEMPT = frozenset({
    'table_sizing.py',
})
#: 写死像素宽（含数字字面量）、模块级/局部宽度表，都视为绕过统一口径
HARDCODED_WIDTH_PATTERNS = (
    re.compile(r'setColumnWidth\s*\(\s*[^,\n]+,\s*\d+\s*\)'),
    re.compile(r'^\s*[A-Za-z_][A-Za-z0-9_]*widths?\s*=\s*[\[\{]',
               re.IGNORECASE | re.MULTILINE),
)


def test_no_page_hardcodes_column_widths():
    """任何页面写死列宽（数字或宽度表）都必须变红，统一走 table_sizing 接口。"""
    offenders: list[str] = []
    for path in sorted(UI_ROOT.rglob('*.py')):
        relative = path.relative_to(UI_ROOT).as_posix()
        if relative in GUARD_EXEMPT:
            continue
        text = path.read_text(encoding='utf-8')
        for lineno, line in enumerate(text.splitlines(), 1):
            if any(pattern.search(line) for pattern in HARDCODED_WIDTH_PATTERNS):
                offenders.append(f'{relative}:{lineno}: {line.strip()}')
    assert not offenders, (
        '以下页面仍在写死列宽，必须改用 src/ui/table_sizing.py 的统一接口：\n'
        + '\n'.join(offenders))


