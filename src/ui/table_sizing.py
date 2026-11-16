"""分析页面表格列宽口径：全部列可拖动、默认宽度按内容自适应。

五个页面（市场趋势 / 风格轮动 / 行业热度 / 仓位与风控 / 收益归因）统一：

1. 所有列（含最后一列）的 resize mode 都是 ``Interactive``，表头分隔线可
   左右拖动，双击分隔线自适应内容；
2. 填充数据后按「表头 + 内容」重算初值，并夹在 [MIN, MAX] 内，避免长文本
   把单列撑到几百像素；
3. 不使用 ``Fixed`` / ``Stretch`` / ``ResizeToContents`` 锁死，也不调用
   ``setStretchLastSection(True)``：末列不撑满，右侧留白由横向滚动承接。
"""

from __future__ import annotations

from PySide6.QtWidgets import QHeaderView, QTableWidget

#: 列宽下限：保证表头文字与短数据不被截断
MIN_COLUMN_WIDTH = 70
#: 列宽上限：长文本列不独占整行，超出部分靠 tooltip / 横向滚动查看
MAX_COLUMN_WIDTH = 360


def use_interactive_columns(table: QTableWidget) -> None:
    """建表时调用：全部列设为 Interactive，关闭末列撑满。"""
    header = table.horizontalHeader()
    header.setStretchLastSection(False)
    header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)


def apply_content_widths(table: QTableWidget) -> None:
    """填充数据后调用：按内容自适应列宽，再保持 Interactive 可拖动。"""
    if table.columnCount() == 0:
        return
    header = table.horizontalHeader()
    header.setStretchLastSection(False)
    # ResizeToContents 会同时计入表头文字与单元格内容，且不改变当前 resize mode
    header.resizeSections(QHeaderView.ResizeMode.ResizeToContents)
    for index in range(table.columnCount()):
        width = header.sectionSize(index)
        width = max(MIN_COLUMN_WIDTH, min(width, MAX_COLUMN_WIDTH))
        table.setColumnWidth(index, width)
        header.setSectionResizeMode(index, QHeaderView.ResizeMode.Interactive)
