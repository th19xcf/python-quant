"""分析页面表格列宽口径：先按单行内容加宽，宽度不够才压缩与折行。

所有页面的表格统一（行情表、财务分析、市场趋势、风格轮动、行业热度、
仓位与风控、收益归因、模型操作清单）：

1. 所有列（含最后一列）的 resize mode 都是 ``Interactive``，表头分隔线可
   左右拖动，双击分隔线自适应内容；
2. 列宽 = max(表头所需宽度, 该列所有内容单行所需宽度)，逐格按实际字体像
   素宽度取最大值（相同文本缓存结果），上限 MAX_COLUMN_WIDTH；
3. 不再压缩列宽换总宽：总宽超出可视区时出横向滚动条；
4. 只有超过 MAX_COLUMN_WIDTH 的内容才折行，行高按内容实测，不截断。
"""

from __future__ import annotations

from PySide6.QtCore import QRect, QSize, Qt
from PySide6.QtGui import QColor, QFont, QFontMetrics, QPalette
from PySide6.QtWidgets import (
    QAbstractItemView,
    QHeaderView,
    QStyledItemDelegate,
    QTableWidget,
    QTableWidgetItem,
)

from src.ui.advice.tokens import TOKENS

# ---------------------------------------------------------------- 度量口径
# 下表是全项目表格唯一的度量口径定义处：内缩、字号、表头字重、列宽上下限、
# 折行阈值都只在这里出现，页面只允许引用，不得再写第二份数字。

#: 单元格左右文本内缩之和（实测）：QSS ``padding: 6px 8px`` 之外样式还会再
#: 内缩，真实绘制可用文本宽 = 列宽 - 26；按 18 估宽会系统性偏窄、短列被省略
CELL_PADDING_H = 26
#: 表头左右内边距 + 加粗字宽余量（表头不允许被截断）
HEADER_PADDING_H = 26
#: 单元格上下内边距（QSS 6px 8px）
CELL_PADDING_V = 14
#: 表格正文字号（与页面 QSS 的 ``font-size`` 同一来源）
TABLE_FONT_SIZE_PX = TOKENS.font_body
#: 表头字重：QSS ``font-weight: 600``，度量必须用同一字重
HEADER_FONT_WEIGHT = QFont.Weight.DemiBold
#: 列宽上限，同时是折行阈值：文本宽度超过它的列才截断到上限并折行
MAX_COLUMN_WIDTH = 360
#: 序号列标题与最小宽度（实际宽度取 max(本值, 表头半粗字宽 + HEADER_PADDING_H)）
SEQ_COLUMN_TITLE = '序号'
SEQ_COLUMN_WIDTH = 50

# ---------------------------------------------------------------- 行高与提示
#: 数据行高下限：保证空行/单行不被压扁
MIN_ROW_HEIGHT = 30
#: 数据行高上限：超长文本换行后封顶，避免单行撑到半屏
MAX_ROW_HEIGHT = 120
#: 表头固定行高：不随数据行自适应
HEADER_ROW_HEIGHT = 34
#: 大表改用固定行高的行数阈值
FIXED_ROW_HEIGHT_THRESHOLD = 200
#: 大表固定行高：与自适应下限的观感一致
FIXED_ROW_HEIGHT = 32
#: 超过该字数的单元格补 tooltip，固定行高裁切后仍可查看全文
TOOLTIP_TEXT_MIN_LEN = 16
#: 空值文本：排序时统一沉底
EMPTY_TEXTS = frozenset({'', '—', '-', '--', 'nan', 'none'})


def use_interactive_columns(table: QTableWidget) -> None:
    """建表时调用：全部列设为 Interactive，关闭末列撑满。"""
    header = table.horizontalHeader()
    header.setStretchLastSection(False)
    header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)


class WrappedRowDelegate(QStyledItemDelegate):
    """按实际折行高度给出行高。

    Qt 默认 ``sizeHint`` 不含 QSS 单元格内边距，中文长文本被上限列宽折行后
    ``resizeRowsToContents()`` 会算少 9~15px，最后一行被纵向裁切；这里用与
    ``apply_wrapped_row_heights`` 相同的 ``boundingRect`` 口径补齐。
    """

    def sizeHint(self, option, index) -> QSize:  # noqa: N802
        size = super().sizeHint(option, index)
        widget = option.widget
        text = index.data(Qt.ItemDataRole.DisplayRole)
        if widget is None or text in (None, ''):
            return size
        width = widget.columnWidth(index.column()) - CELL_PADDING_H
        if width <= 0:
            return size
        bounds = option.fontMetrics.boundingRect(
            QRect(0, 0, width, 10000),
            int(Qt.TextFlag.TextWordWrap), str(text))
        return QSize(size.width(),
                     max(size.height(), bounds.height() + CELL_PADDING_V))


def _column_widths(header_texts: list[str], rows, header_metrics,
                   cell_metrics,
                   cell_padding: int = CELL_PADDING_H) -> list[int]:
    """逐格实测文本像素宽度，返回每列最宽值（含表头）。

    只比较字符数会在「等长但字形更宽」时漏判：``+0.173`` 比 ``-0.990`` 宽
    4px，``ST合力泰`` 比四字中文名宽，列宽会被低估而截断。这里对每个不同
    文本调用一次 ``horizontalAdvance``（相同文本走缓存），按像素取最大值。

    ``cell_padding`` 是单元格左右文本内缩之和。QSS 的 ``padding: 6px 8px``
    之外样式还会再内缩，实测总内缩大于 ``CELL_PADDING_H``，按页面传入实测值，
    否则列宽会系统性偏窄、文本被省略号截断。
    """
    widths = [header_metrics.horizontalAdvance(text) + HEADER_PADDING_H
              for text in header_texts]
    cache: dict[str, int] = {}
    for row in rows:
        for index, value in enumerate(row[:len(widths)]):
            text = '' if value is None else str(value)
            if not text:
                continue
            advance = cache.get(text)
            if advance is None:
                advance = cache[text] = cell_metrics.horizontalAdvance(text)
            width = advance + cell_padding
            if width > widths[index]:
                widths[index] = width
    return widths


def _header_metrics(header) -> QFontMetrics:
    """表头按 QSS 的半粗字重度量：``header.fontMetrics()`` 是常规字重，会偏窄。"""
    font = header.font()
    font.setWeight(HEADER_FONT_WEIGHT)
    return QFontMetrics(font)


def seq_column_width(table: QTableWidget) -> int:
    """序号列宽度：兼顾固定最小宽度与表头半粗字宽（各页字号不同，取实测）。"""
    header = table.horizontalHeader()
    header.ensurePolished()
    metrics = _header_metrics(header)
    return max(SEQ_COLUMN_WIDTH,
               metrics.horizontalAdvance(SEQ_COLUMN_TITLE) + HEADER_PADDING_H)


def _table_row_texts(table: QTableWidget, row: int) -> list[str]:
    """取整行文本（缺失单元格按空串处理），供按列挑最长内容使用。"""
    texts: list[str] = []
    for index in range(table.columnCount()):
        item = table.item(row, index)
        texts.append(item.text() if item is not None else '')
    return texts


def measure_natural_widths(table: QTableWidget,
                           cell_padding: int = CELL_PADDING_H) -> list[int]:
    """逐格按字体实测，返回每列单行完整显示所需宽度。

    先 ``ensurePolished()``：页面 QSS 的 font-size 要到 polish 后才生效，
    否则会用默认字号度量出偏窄的宽度（短列会被截断）。
    """
    columns = table.columnCount()
    if columns == 0:
        return []
    table.ensurePolished()
    header = table.horizontalHeader()
    header.ensurePolished()
    header_texts: list[str] = []
    for index in range(columns):
        item = table.horizontalHeaderItem(index)
        header_texts.append(item.text() if item is not None else '')
    return _column_widths(
        header_texts,
        (_table_row_texts(table, row) for row in range(table.rowCount())),
        _header_metrics(header), table.fontMetrics(), cell_padding)


def apply_content_widths(table: QTableWidget,
                         cell_padding: int = CELL_PADDING_H,
                         text_max: int = MAX_COLUMN_WIDTH) -> bool:
    """填充后调用：列宽 = max(表头宽, 最长内容宽)，超过上限才折行。

    不再压缩列宽换总宽：总宽超出可视区时由横向滚动条承接。
    ``text_max`` 是文本宽度上限，列宽上限 = ``text_max + cell_padding``，
    因此刚好卡在上限的列也不会因内缩而少一条边。
    返回 True 表示至少一列内容超过上限（该列截断到上限并折行）。
    """
    if table.columnCount() == 0:
        return False
    header = table.horizontalHeader()
    header.setStretchLastSection(False)
    natural = measure_natural_widths(table, cell_padding)
    limit = text_max + cell_padding
    first_header = table.horizontalHeaderItem(0)
    seq_index = 0 if first_header is not None \
        and first_header.text() == SEQ_COLUMN_TITLE else None
    for index, width in enumerate(natural):
        table.setColumnWidth(index, min(limit, width))
        header.setSectionResizeMode(index, QHeaderView.ResizeMode.Interactive)
    if seq_index is not None:
        table.setColumnWidth(seq_index, seq_column_width(table))
    return any(width > limit for index, width in enumerate(natural)
               if index != seq_index)


def apply_estimated_column_widths(table: QTableWidget, rows,
                                  text_max: int = MAX_COLUMN_WIDTH) -> None:
    """按原始行数据逐格实测列宽，供 5000+ 行的大表刷新使用。

    相同文本只度量一次（行情表大量重复的代码/名称/数字），不做字符数近似，
    因此「等长但更宽」的文本不会被漏判。
    """
    columns = table.columnCount()
    if columns == 0:
        return
    table.ensurePolished()
    header = table.horizontalHeader()
    header.ensurePolished()
    header.setStretchLastSection(False)
    header_texts: list[str] = []
    for index in range(columns):
        item = table.horizontalHeaderItem(index)
        header_texts.append(item.text() if item is not None else '')
    widths = _column_widths(header_texts, rows,
                            _header_metrics(header), table.fontMetrics())
    limit = text_max + CELL_PADDING_H
    for index, width in enumerate(widths):
        table.setColumnWidth(index, min(limit, width))
        header.setSectionResizeMode(index, QHeaderView.ResizeMode.Interactive)


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


def sort_key(text: str, label_ranks: dict[str, int] | None = None
             ) -> tuple[str, float | str] | None:
    """显示文本 → 排序键：空值 None、档位按语义、数字按解析值、其余按文本。"""
    value = str(text).strip()
    if value.lower() in EMPTY_TEXTS:
        return None
    if label_ranks and value in label_ranks:
        return ('n', float(label_ranks[value]))
    raw = (value.replace('−', '-').replace('＋', '+').replace(',', '')
           .replace(' ', ''))
    if raw[:1] in ('×', 'x', 'X'):
        raw = raw[1:]
    try:
        return ('n', float(raw.rstrip('%％倍亿万手股')))
    except ValueError:
        return ('s', value)


def row_signature(table: QTableWidget, row: int,
                  start_column: int = 1) -> tuple[str, ...]:
    """一行的文本指纹（默认跳过序号列），用来在重排后重新定位同一行。"""
    values: list[str] = []
    for index in range(start_column, table.columnCount()):
        item = table.item(row, index)
        values.append(item.text() if item is not None else '')
    return tuple(values)


def snapshot_row_order(table: QTableWidget,
                       start_column: int = 1) -> list[tuple[str, ...]]:
    """缓存当前行序快照，供「取消排序」按原样还原。"""
    return [row_signature(table, row, start_column)
            for row in range(table.rowCount())]


def restore_row_order(table: QTableWidget, snapshot: list[tuple[str, ...]],
                      start_column: int = 1) -> None:
    """按默认行序快照还原；快照之后才出现的行保持在后。"""
    if not snapshot:
        return
    current = [row_signature(table, row, start_column)
               for row in range(table.rowCount())]
    order: list[int] = []
    used: set[int] = set()
    for signature in snapshot:
        for index, row_signature_value in enumerate(current):
            if index not in used and row_signature_value == signature:
                used.add(index)
                order.append(index)
                break
    order.extend(index for index in range(len(current)) if index not in used)
    if order == list(range(len(order))):
        return
    reorder_table_rows(table, order)


def sort_rows_by_column(table: QTableWidget, column: int, descending: bool,
                        start_column: int = 1,
                        label_ranks: dict[str, int] | None = None) -> None:
    """按解析后的键重排行，空值沉底；文本、对齐、颜色、提示与行高原样保留。"""
    keyed: list[tuple[tuple[str, float | str], int]] = []
    empty: list[int] = []
    for row in range(table.rowCount()):
        cell = table.item(row, column)
        key = sort_key(cell.text(), label_ranks) if cell is not None else None
        if key is None:
            empty.append(row)
        else:
            keyed.append((key, row))
    keyed.sort(key=lambda pair: pair[0], reverse=descending)
    reorder_table_rows(table, [row for _, row in keyed] + empty)


class SizedTableMixin:
    """页面级表格规范：序号列、三态排序、统一底色与行高策略。

    使用方需提供 ``self._pal``（``src.ui.advice.tokens.Palette``）作为底色来源，
    并按页面布局选择 ``TABLE_FIXED_HEIGHT``：表内滚动为 True，外层拉伸为 False。
    """

    #: 数据行高下限：保证空行/单行不被压扁
    MIN_ROW_HEIGHT = MIN_ROW_HEIGHT
    #: 数据行高上限：超长文本换行后封顶，避免单行撑到半屏
    MAX_ROW_HEIGHT = MAX_ROW_HEIGHT
    #: 表头固定行高：不随数据行自适应
    HEADER_ROW_HEIGHT = HEADER_ROW_HEIGHT
    #: 收尾时是否把表钉到内容高度（False 交给外层布局拉伸）
    TABLE_FIXED_HEIGHT = True
    #: 档位列语义排序权重，无档位列的页面留空
    LABEL_SORT_RANK: dict[str, int] = {}
    #: 单元格左右文本内缩之和；与 ``apply_content_widths`` 的默认值一致
    CELL_PADDING_H = CELL_PADDING_H

    # ------------------------------------------------------------------ 建表
    def _configure_sized_table(self, table: QTableWidget,
                               selectable: bool = False) -> None:
        """建表统一配置：行高策略、无交替色、网格线、表头与列宽模式。"""
        vertical = table.verticalHeader()
        vertical.setVisible(False)
        vertical.setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        vertical.setMinimumSectionSize(self.MIN_ROW_HEIGHT)
        vertical.setMaximumSectionSize(self.MAX_ROW_HEIGHT)
        table.setAlternatingRowColors(False)
        table.setShowGrid(True)
        table.setWordWrap(True)
        table.setItemDelegate(WrappedRowDelegate(table))
        table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        if not selectable:
            table.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
            table.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        header = table.horizontalHeader()
        header.setHighlightSections(False)
        header.setFixedHeight(self.HEADER_ROW_HEIGHT)
        use_interactive_columns(table)
        self._enable_header_sorting(table)

    def _finalize_table(self, table: QTableWidget,
                        remember_order: bool = True,
                        remeasure: bool = True) -> None:
        """补序号列并统一底色；填充时量列宽行高，排序后只重编序号。"""
        self._ensure_seq_column(table)
        if remember_order:
            self._remember_row_order(table)
            self._table_sort_clicks().pop(table, None)
            table.horizontalHeader().setSortIndicatorShown(False)
        self._unify_table_surface(table)
        wrapped = apply_content_widths(table, self.CELL_PADDING_H) \
            if remeasure else False
        self._apply_row_height_mode(table, remeasure=remeasure, wrapped=wrapped)
        self._renumber_rows(table, remeasure=remeasure)
        if self.TABLE_FIXED_HEIGHT:
            table.setFixedHeight(self._table_height(table))

    def _ensure_seq_column(self, table: QTableWidget) -> None:
        """最左侧补「序号」列：只在首次收尾时插入，后续排序沿用。"""
        header_item = table.horizontalHeaderItem(0)
        if header_item is not None and header_item.text() == SEQ_COLUMN_TITLE:
            return
        table.insertColumn(0)
        table.setHorizontalHeaderItem(0, QTableWidgetItem(SEQ_COLUMN_TITLE))

    def _prepare_table(self, table: QTableWidget) -> None:
        """填数前移除旧序号列，保证数据列下标仍从 0 开始。"""
        header_item = table.horizontalHeaderItem(0)
        if header_item is not None and header_item.text() == SEQ_COLUMN_TITLE:
            table.removeColumn(0)

    def _apply_row_height_mode(self, table: QTableWidget,
                               remeasure: bool = True,
                               wrapped: bool = False) -> None:
        """未折行用统一行高；折行时按内容测高，大表一次性测完、排序时不重算。"""
        if not remeasure:
            return
        vertical = table.verticalHeader()
        if not wrapped:
            vertical.setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
            vertical.setDefaultSectionSize(FIXED_ROW_HEIGHT)
            for row in range(table.rowCount()):
                table.setRowHeight(row, FIXED_ROW_HEIGHT)
            return
        if table.rowCount() > FIXED_ROW_HEIGHT_THRESHOLD:
            apply_wrapped_row_heights(table, self.MIN_ROW_HEIGHT,
                                      self.MAX_ROW_HEIGHT)
            return
        vertical.setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        table.resizeRowsToContents()

    def _renumber_rows(self, table: QTableWidget,
                       remeasure: bool = True) -> None:
        """按当前显示行序重编 1..N；大表首次填充顺带补长文本 tooltip。"""
        tooltips = remeasure and table.rowCount() > FIXED_ROW_HEIGHT_THRESHOLD
        for row in range(table.rowCount()):
            cell = table.item(row, 0)
            if cell is None:
                cell = QTableWidgetItem('')
                cell.setFlags(Qt.ItemFlag.ItemIsEnabled)
                table.setItem(row, 0, cell)
            cell.setText(str(row + 1))
            cell.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            if not tooltips:
                continue
            for column in range(1, table.columnCount()):
                item = table.item(row, column)
                if item is not None and len(item.text()) >= TOOLTIP_TEXT_MIN_LEN:
                    item.setToolTip(item.text())

    def _unify_table_surface(self, table: QTableWidget) -> None:
        """单元格、末列右侧留白与表头空白区同色，避免出现拼接色块。"""
        color = QColor(self._pal.card)
        for widget in (table.viewport(), table.horizontalHeader()):
            palette = widget.palette()
            for role in (QPalette.ColorRole.Base, QPalette.ColorRole.Window,
                         QPalette.ColorRole.Button):
                palette.setColor(role, color)
            widget.setPalette(palette)
            widget.setAutoFillBackground(True)

    def _table_height(self, table: QTableWidget) -> int:
        header = table.horizontalHeader().height()
        rows = sum(table.rowHeight(row) for row in range(table.rowCount()))
        return header + rows + 8

    # ------------------------------------------------------------------ 排序
    def _table_sort_clicks(self) -> dict[QTableWidget, dict[int, int]]:
        """表头点击计数（按表、按列独立），首次访问时懒建。"""
        if not hasattr(self, '_sort_clicks'):
            self._sort_clicks: dict[QTableWidget, dict[int, int]] = {}
        return self._sort_clicks

    def _row_order_snapshots(self) -> dict[QTableWidget, list[tuple[str, ...]]]:
        """各表默认行序快照，首次访问时懒建。"""
        if not hasattr(self, '_row_orders'):
            self._row_orders: dict[QTableWidget, list[tuple[str, ...]]] = {}
        return self._row_orders

    def _enable_header_sorting(self, table: QTableWidget) -> None:
        """表头可点击排序；初始不显示箭头，保持各页默认语义顺序。"""
        header = table.horizontalHeader()
        header.setSortIndicatorShown(False)
        header.setSectionsClickable(True)
        header.sectionClicked.connect(
            lambda column: self._on_header_clicked(table, column))

    def _on_header_clicked(self, table: QTableWidget, column: int) -> None:
        """同一列三态循环：升序 → 降序 → 取消排序（还原默认行序）。"""
        if column == 0:
            return  # 「序号」列不参与排序，也不计入三态
        header = table.horizontalHeader()
        clicks = self._table_sort_clicks().setdefault(table, {})
        stage = clicks.get(column, 0) % 3
        clicks[column] = stage + 1
        if stage == 2:
            self._restore_row_order(table)
            header.setSortIndicator(-1, Qt.SortOrder.AscendingOrder)
            header.setSortIndicatorShown(False)
            self._finalize_table(table, remember_order=False, remeasure=False)
            return
        self._restore_row_order(table)
        order = (Qt.SortOrder.AscendingOrder if stage == 0
                 else Qt.SortOrder.DescendingOrder)
        self._sort_table_rows(table, column, order)
        header.setSortIndicator(column, order)
        header.setSortIndicatorShown(True)
        self._finalize_table(table, remember_order=False, remeasure=False)

    def _remember_row_order(self, table: QTableWidget) -> None:
        """缓存默认行序快照，供取消排序时按原样还原。"""
        self._row_order_snapshots()[table] = snapshot_row_order(table)

    def _restore_row_order(self, table: QTableWidget) -> None:
        restore_row_order(table, self._row_order_snapshots().get(table, []))

    def _sort_table_rows(self, table: QTableWidget, column: int,
                         order: Qt.SortOrder) -> None:
        sort_rows_by_column(table, column,
                            order == Qt.SortOrder.DescendingOrder,
                            label_ranks=self.LABEL_SORT_RANK)

