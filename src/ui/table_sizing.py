"""分析页面表格列宽口径：先按单行内容加宽，宽度不够才压缩与折行。

五个页面（市场趋势 / 风格轮动 / 行业热度 / 仓位与风控 / 收益归因）统一：

1. 所有列（含最后一列）的 resize mode 都是 ``Interactive``，表头分隔线可
   左右拖动，双击分隔线自适应内容；
2. 填充数据后按「表头 + 内容单行完整显示」测自然宽度（不按折行测量），
   下限 MIN_COLUMN_WIDTH；
3. 自然宽度之和不超过表格可视宽度时直接用自然宽度，不折行、不出横向滚动；
4. 超出时才压缩：优先压缩内容比表头宽的长文本列（二分出一个统一上限，
   越长的列压得越多），数值列与档位列的宽度不小于表头宽度；
5. 压缩后仍装不下、或长文本列被压到短于单行内容才折行，行高按内容实测，不截断。
"""

from __future__ import annotations

from PySide6.QtCore import QRect, Qt
from PySide6.QtWidgets import QHeaderView, QTableWidget

#: 列宽下限：保证表头文字与短数据不被截断
MIN_COLUMN_WIDTH = 70
#: 单元格左右内边距（QSS 6px 8px）+ 网格线余量
CELL_PADDING_H = 18
#: 表头左右内边距 + 加粗字宽余量（表头不允许被截断）
HEADER_PADDING_H = 26
#: 单元格上下内边距（QSS 6px 8px）
CELL_PADDING_V = 14
#: 序号列固定宽度与会话内不参与压缩
SEQ_COLUMN_TITLE = '序号'
SEQ_COLUMN_WIDTH = 50
#: 可视宽度小于该值视为尚未完成布局，按自然宽度处理不做压缩
MIN_LAYOUT_VIEWPORT = 200
#: 竖向滚动条预留宽度：压缩到正好贴合时避免横竖滚动条互相触发
SCROLLBAR_RESERVE = 18


def use_interactive_columns(table: QTableWidget) -> None:
    """建表时调用：全部列设为 Interactive，关闭末列撑满。"""
    header = table.horizontalHeader()
    header.setStretchLastSection(False)
    header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)


def measure_natural_widths(table: QTableWidget) -> list[int]:
    """按表头与单元格内容的「单行完整显示」测量每列自然宽度（不折行）。"""
    columns = table.columnCount()
    if columns == 0:
        return []
    header = table.horizontalHeader()
    header_metrics = header.fontMetrics()
    cell_metrics = table.fontMetrics()
    widths = [0] * columns
    for index in range(columns):
        item = table.horizontalHeaderItem(index)
        text = item.text() if item is not None else ''
        widths[index] = header_metrics.horizontalAdvance(text) + HEADER_PADDING_H
    for row in range(table.rowCount()):
        for index in range(columns):
            item = table.item(row, index)
            if item is None:
                continue
            advance = cell_metrics.horizontalAdvance(item.text()) + CELL_PADDING_H
            if advance > widths[index]:
                widths[index] = advance
    return [max(MIN_COLUMN_WIDTH, width) for width in widths]


def _header_floors(table: QTableWidget) -> list[int]:
    """每列的压缩下限：表头必须完整可见，且不低于 MIN_COLUMN_WIDTH。"""
    header = table.horizontalHeader()
    metrics = header.fontMetrics()
    floors: list[int] = []
    for index in range(table.columnCount()):
        item = table.horizontalHeaderItem(index)
        text = item.text() if item is not None else ''
        floors.append(max(MIN_COLUMN_WIDTH,
                          metrics.horizontalAdvance(text) + HEADER_PADDING_H))
    return floors


def _capped_total(natural: list[int], floors: list[int],
                  indexes: list[int], level: int) -> int:
    """给定压缩上限 level 时这些列的总宽度（每列不低于自身下限）。"""
    return sum(max(floors[index], min(natural[index], level)) for index in indexes)


def apply_content_widths(table: QTableWidget) -> bool:
    """填充数据后调用：自然宽度优先，装不下再压缩，仍装不下才允许折行。

    返回 True 表示至少一列被压到小于单行内容宽度（内容会折行）。
    """
    if table.columnCount() == 0:
        return False
    header = table.horizontalHeader()
    header.setStretchLastSection(False)
    natural = measure_natural_widths(table)
    floors = [min(floor, width)
              for floor, width in zip(_header_floors(table), natural, strict=False)]
    first_header = table.horizontalHeaderItem(0)
    seq_index = 0 if first_header is not None \
        and first_header.text() == SEQ_COLUMN_TITLE else None
    data_indexes = [index for index in range(table.columnCount()) if index != seq_index]
    seq_width = SEQ_COLUMN_WIDTH if seq_index is not None else 0

    viewport = table.viewport().width()
    if viewport < MIN_LAYOUT_VIEWPORT:
        viewport = table.width()
    fit_available = max(0, viewport - seq_width)
    budget = max(0, fit_available - SCROLLBAR_RESERVE)

    applied = list(natural)
    if sum(natural[index] for index in data_indexes) > fit_available \
            and viewport >= MIN_LAYOUT_VIEWPORT:
        shrinkable = [index for index in data_indexes
                      if natural[index] > floors[index]]
        if shrinkable:
            fixed_total = sum(natural[index] for index in data_indexes
                              if index not in shrinkable)
            low = min(floors[index] for index in shrinkable)
            high = max(natural[index] for index in shrinkable)
            if fixed_total + _capped_total(natural, floors, shrinkable, low) <= budget:
                # 二分出能装下的最大统一上限：越长的列被压得越多，短列保持自然宽度
                while low < high:
                    mid = (low + high + 1) // 2
                    if fixed_total + _capped_total(
                            natural, floors, shrinkable, mid) <= budget:
                        low = mid
                    else:
                        high = mid - 1
            for index in shrinkable:
                applied[index] = max(floors[index], min(natural[index], low))

    for index, width in enumerate(applied):
        table.setColumnWidth(index, width)
        header.setSectionResizeMode(index, QHeaderView.ResizeMode.Interactive)
    if seq_index is not None:
        table.setColumnWidth(seq_index, SEQ_COLUMN_WIDTH)
    return any(natural[index] > applied[index] for index in data_indexes)


def apply_wrapped_row_heights(table: QTableWidget, min_height: int,
                              max_height: int) -> None:
    """折行的大表一次性按内容实测行高：固定模式下写死每行，排序时不再重测。"""
    vertical = table.verticalHeader()
    vertical.setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
    vertical.setDefaultSectionSize(min_height)
    metrics = table.fontMetrics()
    for row in range(table.rowCount()):
        height = min_height
        for index in range(1, table.columnCount()):
            item = table.item(row, index)
            if item is None or not item.text():
                continue
            width = table.columnWidth(index) - CELL_PADDING_H
            if width <= 0:
                continue
            bounds = metrics.boundingRect(
                QRect(0, 0, width, 10000),
                int(Qt.TextFlag.TextWordWrap),
                item.text())
            height = max(height, bounds.height() + CELL_PADDING_V)
        table.setRowHeight(row, min(max_height, height))


def reorder_table_rows(table: QTableWidget, order: list[int]) -> None:
    """按 order（新行序 → 原行号）搬移单元格；行高不一致时一并搬移，避免错位。"""
    if not order:
        return
    heights = [table.rowHeight(row) for row in range(table.rowCount())]
    uniform = len(set(heights)) <= 1
    cells = [[table.takeItem(row, index) for index in range(table.columnCount())]
             for row in range(table.rowCount())]
    for target, source in enumerate(order):
        for index, cell in enumerate(cells[source]):
            if cell is not None:
                table.setItem(target, index, cell)
        if not uniform:
            table.setRowHeight(target, heights[source])
