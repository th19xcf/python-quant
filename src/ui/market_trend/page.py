"""主界面「市场趋势」标签页：打开即计算并展示五层状态判定。"""

from __future__ import annotations

from typing import Any

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor, QPalette
from PySide6.QtWidgets import (
    QAbstractItemView,
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QPushButton,
    QSizePolicy,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from src.quant.analysis.market_trend_state import (
    TrendState,
    format_raw_value,
)
from src.ui.advice.tokens import TOKENS, palette_for
from src.ui.table_sizing import (
    apply_content_widths,
    apply_wrapped_row_heights,
    reorder_table_rows,
    use_interactive_columns,
)
from src.ui.theme_manager import is_dark_palette

#: 数据行高下限：保证空行/单行不被压扁
MIN_ROW_HEIGHT = 30
#: 数据行高上限：超长文本换行后封顶，避免单行撑到半屏
MAX_ROW_HEIGHT = 120
#: 表头固定行高：不随数据行自适应
HEADER_ROW_HEIGHT = 34
#: 档位列语义排序权重（本页无档位列，保留空表供排序键复用）
LABEL_SORT_RANK: dict[str, int] = {}
#: 最左侧「序号」列：宽 50px、居中、不参与排序
SEQ_COLUMN_TITLE = '序号'
SEQ_COLUMN_WIDTH = 50
#: 行数超过该阈值改用固定行高，避免大表排序时逐行测量换行文本
FIXED_ROW_HEIGHT_THRESHOLD = 200
#: 大表固定行高：与自适应下限 30px 的观感一致
FIXED_ROW_HEIGHT = 32
#: 超过该字数的单元格补 tooltip，固定行高裁切后仍可查看全文
TOOLTIP_TEXT_MIN_LEN = 16
#: 空值文本：排序时统一沉底
_EMPTY_TEXTS = frozenset({'', '—', '-', '--', 'nan', 'none'})


def _sort_key(text: str) -> tuple[str, float | str] | None:
    """显示文本 → 排序键：空值 None、档位按语义、数字按数值、其余按文本。"""
    value = str(text).strip()
    if value.lower() in _EMPTY_TEXTS:
        return None
    if value in LABEL_SORT_RANK:
        return ('n', float(LABEL_SORT_RANK[value]))
    raw = (value.replace('−', '-').replace('＋', '+').replace(',', '')
           .replace(' ', ''))
    if raw[:1] in ('×', 'x', 'X'):
        raw = raw[1:]
    try:
        return ('n', float(raw.rstrip('%％倍亿万手股')))
    except ValueError:
        return ('s', value)


def _fetch_state(token: int, asof: str | None = None,
                 task_id: str | None = None, refresh: bool = False,
                 signals: Any = None) -> dict[str, Any]:
    """后台任务：只读本地数据并计算状态（带版本落盘缓存）。"""
    from datetime import datetime

    from src.quant.analysis import analysis_cache
    from src.quant.analysis.market_trend_local import load_local_panel
    from src.quant.analysis.market_trend_state import evaluate_market_trend

    target = datetime.strptime(asof, '%Y-%m-%d').date() if asof else None

    def compute() -> Any:
        return evaluate_market_trend(load_local_panel(), asof=target)

    result = analysis_cache.cached(
        'market_trend', 'state', asof=asof,
        fingerprint={'panel_end': analysis_cache.panel_end()},
        compute=compute, refresh=refresh,
    )
    return {'token': token, 'state': result.value, 'cache': result.meta}


def _qss(theme: str) -> str:
    palette = palette_for(theme)
    tokens = TOKENS
    return f"""
QWidget#marketTrendPage {{
    background: {palette.bg};
    color: {palette.text};
    font-family: {tokens.font_family};
    font-size: {tokens.font_body}px;
}}
QLabel#trendTitle {{
    font-size: {tokens.font_title}px;
    font-weight: 700;
    color: {palette.text};
}}
QLabel#trendSubtitle {{
    font-size: {tokens.font_small}px;
    color: {palette.muted};
}}
QLabel#trendStatus {{
    font-size: {tokens.font_small}px;
    color: {palette.muted};
}}
QLabel#trendSectionTitle {{
    font-size: {tokens.font_section}px;
    font-weight: 700;
    color: {palette.text};
}}
QLabel#trendNotes {{
    font-size: {tokens.font_small}px;
    color: {palette.muted};
}}
QFrame#trendCard {{
    background: {palette.card};
    border: 1px solid {palette.border};
    border-radius: {tokens.radius}px;
}}
QPushButton {{
    background: {palette.accent_soft};
    color: {palette.accent};
    border: 1px solid {palette.accent};
    border-radius: {tokens.radius_sm}px;
    padding: 5px 16px;
}}
QPushButton:disabled {{
    color: {palette.faint};
    border-color: {palette.border};
    background: {palette.elevated};
}}
QTableWidget {{
    background: {palette.card};
    border: 1px solid {palette.border};
    border-radius: {tokens.radius_sm}px;
    gridline-color: {palette.faint};
    selection-background-color: {palette.selection};
}}
QHeaderView {{
    background: {palette.card};
}}
QHeaderView::section {{
    background: {palette.card};
    color: {palette.muted};
    border: none;
    border-bottom: 1px solid {palette.border};
    border-right: 1px solid {palette.border};
    padding: 6px 8px;
    font-weight: 600;
}}
QTableWidget::item {{
    background-color: {palette.card};
    padding: 6px 8px;
}}
"""


class MarketTrendPage(QWidget):
    """市场趋势状态页：结论卡 + 分层表 + 指标表。"""

    def __init__(self, host: Any = None, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName('marketTrendPage')
        self._host = host
        self.theme = 'dark' if is_dark_palette() else 'light'
        self._pal = palette_for(self.theme)
        #: 各表默认行序快照，供「取消排序」按原样还原
        self._row_orders: dict[QTableWidget, list[tuple[str, ...]]] = {}
        #: 表头点击计数（按列独立）：0 升序、1 降序、2 取消排序
        self._sort_clicks: dict[QTableWidget, dict[int, int]] = {}
        self._token = 0
        self._busy = False
        self._task_id: str | None = None
        self._state: TrendState | None = None
        self._build_ui()
        self.setStyleSheet(_qss(self.theme))
        self._connect_task_signals()

    # ------------------------------------------------------------------ UI
    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 14, 16, 14)
        root.setSpacing(12)

        header = QHBoxLayout()
        title_box = QVBoxLayout()
        title_box.setSpacing(2)
        title = QLabel('市场趋势状态')
        title.setObjectName('trendTitle')
        subtitle = QLabel('五层滚动分位打分：趋势 / 宽度 / 量价资金 / 波动情绪 / 估值宏观')
        subtitle.setObjectName('trendSubtitle')
        title_box.addWidget(title)
        title_box.addWidget(subtitle)
        header.addLayout(title_box)
        header.addStretch(1)

        self.status_label = QLabel('尚未计算')
        self.status_label.setObjectName('trendStatus')
        header.addWidget(self.status_label)

        self.refresh_button = QPushButton('重新计算')
        self.refresh_button.setCursor(Qt.CursorShape.PointingHandCursor)
        self.refresh_button.clicked.connect(lambda: self.load_async(refresh=True))
        header.addWidget(self.refresh_button)
        root.addLayout(header)

        root.addWidget(self._build_conclusion())
        root.addWidget(self._build_layers_card())
        root.addWidget(self._build_indicators_card())

        self.notes_label = QLabel('')
        self.notes_label.setObjectName('trendNotes')
        self.notes_label.setWordWrap(True)
        root.addWidget(self.notes_label)
        root.addStretch(1)

    def _build_conclusion(self) -> QFrame:
        card = QFrame()
        card.setObjectName('trendCard')
        layout = QHBoxLayout(card)
        layout.setContentsMargins(18, 14, 18, 14)
        layout.setSpacing(28)

        label_box = QVBoxLayout()
        label_box.setSpacing(2)
        caption = QLabel('当前状态')
        caption.setObjectName('trendSubtitle')
        self.conclusion_label = QLabel('—')
        font = self.conclusion_label.font()
        font.setPointSize(24)
        font.setBold(True)
        self.conclusion_label.setFont(font)
        label_box.addWidget(caption)
        label_box.addWidget(self.conclusion_label)
        layout.addLayout(label_box)

        score_box = QVBoxLayout()
        score_box.setSpacing(2)
        score_caption = QLabel('总分（-1 ~ +1）')
        score_caption.setObjectName('trendSubtitle')
        self.score_label = QLabel('—')
        score_font = self.score_label.font()
        score_font.setPointSize(18)
        score_font.setBold(True)
        self.score_label.setFont(score_font)
        self.asof_label = QLabel('数据截至 —')
        self.asof_label.setObjectName('trendSubtitle')
        score_box.addWidget(score_caption)
        score_box.addWidget(self.score_label)
        score_box.addWidget(self.asof_label)
        layout.addLayout(score_box)

        hint = QLabel(
            '标签阈值：+0.60 强趋势上行 / +0.20 弱上行 / ±0.20 震荡 / '
            '-0.60 强下行；阈值为滚动分位分档，不是上涨概率。'
        )
        hint.setObjectName('trendSubtitle')
        hint.setWordWrap(True)
        hint.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        layout.addWidget(hint, 1)
        return card

    def _build_layers_card(self) -> QFrame:
        card = QFrame()
        card.setObjectName('trendCard')
        layout = QVBoxLayout(card)
        layout.setContentsMargins(14, 12, 14, 12)
        layout.setSpacing(8)
        section = QLabel('分层得分与贡献')
        section.setObjectName('trendSectionTitle')
        layout.addWidget(section)

        self.layer_table = self._make_table(['层级', '得分', '贡献', '权重', '说明'])
        self.layer_table.setColumnWidth(0, 70)
        self.layer_table.setColumnWidth(1, 70)
        self.layer_table.setColumnWidth(2, 70)
        self.layer_table.setColumnWidth(3, 60)
        layout.addWidget(self.layer_table)
        return card

    def _build_indicators_card(self) -> QFrame:
        card = QFrame()
        card.setObjectName('trendCard')
        layout = QVBoxLayout(card)
        layout.setContentsMargins(14, 12, 14, 12)
        layout.setSpacing(8)
        section = QLabel('关键指标读数与判据')
        section.setObjectName('trendSectionTitle')
        layout.addWidget(section)

        self.indicator_table = self._make_table(
            ['层级', '指标', '读数', '滚动分位', '得分', '判据说明'])
        self.indicator_table.setColumnWidth(0, 66)
        self.indicator_table.setColumnWidth(1, 160)
        self.indicator_table.setColumnWidth(2, 110)
        self.indicator_table.setColumnWidth(3, 90)
        self.indicator_table.setColumnWidth(4, 64)
        layout.addWidget(self.indicator_table)
        return card

    def _make_table(self, headers: list[str]) -> QTableWidget:
        table = QTableWidget(0, len(headers))
        table.setHorizontalHeaderLabels(headers)
        vertical = table.verticalHeader()
        vertical.setVisible(False)
        vertical.setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        vertical.setMinimumSectionSize(MIN_ROW_HEIGHT)
        vertical.setMaximumSectionSize(MAX_ROW_HEIGHT)
        table.setAlternatingRowColors(False)
        table.setShowGrid(True)
        table.setWordWrap(True)
        table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        table.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        table.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        header = table.horizontalHeader()
        header.setHighlightSections(False)
        header.setFixedHeight(HEADER_ROW_HEIGHT)
        table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        use_interactive_columns(table)
        self._enable_header_sorting(table)
        return table

    def _finalize_table(self, table: QTableWidget,
                        remember_order: bool = True,
                        remeasure: bool = True) -> None:
        """补序号列并统一底色；填充时量列宽行高，排序后只重编序号。"""
        self._ensure_seq_column(table)
        if remember_order:
            self._remember_row_order(table)
            self._sort_clicks.pop(table, None)
            table.horizontalHeader().setSortIndicatorShown(False)
        self._unify_table_surface(table)
        wrapped = apply_content_widths(table) if remeasure else False
        self._apply_row_height_mode(table, remeasure=remeasure, wrapped=wrapped)
        if remeasure:
            table.setColumnWidth(0, SEQ_COLUMN_WIDTH)
        self._renumber_rows(table, remeasure=remeasure)
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
            apply_wrapped_row_heights(table, MIN_ROW_HEIGHT, MAX_ROW_HEIGHT)
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
        clicks = self._sort_clicks.setdefault(table, {})
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
        self._row_orders[table] = [
            self._row_signature(table, row) for row in range(table.rowCount())]

    def _row_signature(self, table: QTableWidget, row: int) -> tuple[str, ...]:
        """一行的文本指纹（跳过序号列），用来在重排后重新定位同一行。"""
        values: list[str] = []
        for index in range(1, table.columnCount()):
            item = table.item(row, index)
            values.append(item.text() if item is not None else '')
        return tuple(values)

    def _restore_row_order(self, table: QTableWidget) -> None:
        """按默认行序快照还原；快照之后才出现的行保持在后。"""
        snapshot = self._row_orders.get(table)
        if not snapshot:
            return
        current = [self._row_signature(table, row) for row in range(table.rowCount())]
        order: list[int] = []
        used: set[int] = set()
        for signature in snapshot:
            for index, row_signature in enumerate(current):
                if index not in used and row_signature == signature:
                    used.add(index)
                    order.append(index)
                    break
        order.extend(index for index in range(len(current)) if index not in used)
        if order == list(range(len(order))):
            return
        reorder_table_rows(table, order)

    def _sort_table_rows(self, table: QTableWidget, column: int,
                         order: Qt.SortOrder) -> None:
        """按解析后的键重排行，空值沉底；文本、对齐、颜色、提示与行高原样保留。"""
        keyed: list[tuple[tuple[str, float | str], int]] = []
        empty: list[int] = []
        for row in range(table.rowCount()):
            cell = table.item(row, column)
            key = _sort_key(cell.text()) if cell is not None else None
            if key is None:
                empty.append(row)
            else:
                keyed.append((key, row))
        keyed.sort(key=lambda pair: pair[0],
                   reverse=order == Qt.SortOrder.DescendingOrder)
        reorder_table_rows(table, [row for _, row in keyed] + empty)

    # -------------------------------------------------------------- loading
    def _connect_task_signals(self) -> None:
        from src.ui.task_manager import global_task_manager

        self._task_manager = global_task_manager
        global_task_manager.task_completed.connect(self._on_task_completed)
        global_task_manager.task_error.connect(self._on_task_error)

    def load_async(self, refresh: bool = False) -> None:
        """首次打开或点击重新计算时触发后台判定；``refresh`` 强制绕过缓存。"""
        if self._busy:
            return
        self._busy = True
        self._token += 1
        self.refresh_button.setEnabled(False)
        self.refresh_button.setText('计算中…')
        self.status_label.setText('正在读取本地数据并计算…')
        self._task_id = self._task_manager.create_task(
            f'市场趋势判定#{self._token}',
            _fetch_state,
            kwargs={'token': self._token, 'refresh': refresh},
            timeout_seconds=120,
        )

    def _on_task_completed(self, task_id: str, result: Any) -> None:
        if task_id != self._task_id:
            return
        self._finish_busy()
        state = result.get('state') if isinstance(result, dict) else None
        if state is None:
            self.status_label.setText('未返回结果')
            return
        self.render_state(state)
        from src.quant.analysis.analysis_cache import describe

        self.status_label.setText(
            self.status_label.text() + describe(result.get('cache')))

    def _on_task_error(self, task_id: str, error_message: str) -> None:
        if task_id != self._task_id:
            return
        self._finish_busy()
        self.status_label.setText(f'计算失败：{error_message}')

    def _finish_busy(self) -> None:
        self._busy = False
        self.refresh_button.setEnabled(True)
        self.refresh_button.setText('重新计算')

    # ------------------------------------------------------------ rendering
    def render_state(self, state: TrendState) -> None:
        """把判定结果灌入页面（截图与测试也走同一入口）。"""
        self._state = state
        self.conclusion_label.setText(state.label)
        self.conclusion_label.setStyleSheet(
            f'color: {self._label_color(state.label)};')
        self.score_label.setText(f'{state.total_score:+.3f}')
        self.score_label.setStyleSheet(
            f'color: {self._score_color(state.total_score)};')
        self.asof_label.setText(f'数据截至 {state.asof}')
        self.status_label.setText('判定完成')
        self._fill_layers(state)
        self._fill_indicators(state)
        self.notes_label.setText('\n'.join(state.notes))

    def _fill_layers(self, state: TrendState) -> None:
        table = self.layer_table
        self._prepare_table(table)
        table.setRowCount(len(state.layers))
        for row, layer in enumerate(state.layers):
            score_text = '—' if layer.score is None else f'{layer.score:+.3f}'
            contribution = (
                '—' if layer.contribution is None else f'{layer.contribution:+.3f}')
            values = [
                layer.name,
                score_text,
                contribution,
                f'{layer.weight:.2f}',
                layer.description,
            ]
            alignments = (
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
            )
            for column, text in enumerate(values):
                item = self._item(text, alignments[column])
                if column in (1, 2) and layer.score is not None:
                    item.setForeground(self._brush(self._score_color(layer.score)))
                table.setItem(row, column, item)
        self._finalize_table(table)

    def _fill_indicators(self, state: TrendState) -> None:
        table = self.indicator_table
        self._prepare_table(table)
        rows = [
            (layer.name, indicator)
            for layer in state.layers
            for indicator in layer.indicators
        ]
        table.setRowCount(len(rows))
        for row, (layer_name, indicator) in enumerate(rows):
            window = '1250 日' if indicator.mode == 'quantile' else '250 日'
            percentile = (
                '—' if indicator.percentile is None
                else f'{indicator.percentile:.1f}')
            score_text = (
                '—' if indicator.score is None else f'{indicator.score:+.2f}')
            values = [
                layer_name,
                indicator.name,
                format_raw_value(indicator.raw, indicator.raw_kind),
                percentile,
                score_text,
                indicator.judgment,
            ]
            alignments = (
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
            )
            for column, text in enumerate(values):
                item = self._item(text, alignments[column])
                if column == 2:
                    item.setToolTip(f'{indicator.formula}\n数据截至 {indicator.asof}')
                if column == 3:
                    item.setToolTip(f'{window}滚动分位（0~100）')
                if column == 4 and indicator.score is not None:
                    item.setForeground(self._brush(self._score_color(indicator.score)))
                table.setItem(row, column, item)
        self._finalize_table(table)

    def _table_height(self, table: QTableWidget) -> int:
        header = table.horizontalHeader().height()
        rows = sum(table.rowHeight(row) for row in range(table.rowCount()))
        return header + rows + 8

    @staticmethod
    def _item(
        text: str,
        alignment: Qt.AlignmentFlag = Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
    ) -> QTableWidgetItem:
        item = QTableWidgetItem(text)
        item.setFlags(Qt.ItemFlag.ItemIsEnabled)
        item.setTextAlignment(alignment)
        return item

    def _brush(self, color: str):
        from PySide6.QtGui import QBrush, QColor

        return QBrush(QColor(color))

    def _score_color(self, score: float) -> str:
        if score > 0.05:
            return self._pal.buy
        if score < -0.05:
            return self._pal.sell
        return self._pal.hold

    def _label_color(self, label: str) -> str:
        if '上行' in label:
            return self._pal.buy
        if '下行' in label:
            return self._pal.sell
        return self._pal.hold
