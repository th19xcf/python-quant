"""主界面「模型操作清单」标签页：按模型产出给出方向、股数与执行条件。"""

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

from src.quant.analysis.action_list_state import (
    DIRECTION_ORDER,
    ActionListState,
    ActionRow,
)
from src.ui.advice.tokens import TOKENS, palette_for
from src.ui.table_sizing import (
    WrappedRowDelegate,
    apply_content_widths,
    apply_wrapped_row_heights,
    use_interactive_columns,
)
from src.ui.theme_manager import is_dark_palette

#: 表头（第 0 列为序号）
COLUMNS = ['序号', '代码', '名称', '方向', '建议股数', '参考价', '目标权重',
           '执行时点', '触发/失效条件', '依据摘要']
#: 数据行高下限与固定行高
MIN_ROW_HEIGHT = 30
MAX_ROW_HEIGHT = 120
HEADER_ROW_HEIGHT = 34
FIXED_ROW_HEIGHT_THRESHOLD = 200
FIXED_ROW_HEIGHT = 32
#: 补 tooltip 的文本长度门槛
TOOLTIP_TEXT_MIN_LEN = 16
#: 序号列
SEQ_COLUMN_TITLE = '序号'
SEQ_COLUMN_WIDTH = 50


def _sort_value(row: ActionRow, column: int) -> tuple[int, float | str]:
    """排序键：方向按语义、股数/价格/权重按数值、其余按文本。"""
    if column == 3:
        return (0, float(DIRECTION_ORDER.get(row.direction, 9)))
    if column == 4:
        return (0, float(row.shares))
    if column == 5:
        return (0, float(row.price))
    if column == 6:
        return (0, float(row.target_weight))
    text = {
        1: row.code, 2: row.name, 7: row.exec_date,
        8: row.condition, 9: row.rationale,
    }.get(column, '')
    return (1, text)


def _fetch_state(token: int, capital: float = 1e9, task_id: str | None = None,
                 refresh: bool = False, signals: Any = None) -> dict[str, Any]:
    """后台任务：读取目标持仓台账与三层状态，产出模型侧操作清单（带缓存）。"""
    from src.quant.analysis import analysis_cache
    from src.quant.analysis.action_list_local import LEDGER_PATH, build_action_list

    dv = analysis_cache.data_version()

    def compute() -> Any:
        return build_action_list(capital=capital)

    result = analysis_cache.cached(
        'action_list', 'state', asof=None, dv=dv,
        fingerprint={'ledger': analysis_cache.file_stamp(LEDGER_PATH),
                     'capital': round(float(capital), 2),
                     # 口径版本：执行日改为按信号日推导后必须让旧缓存失效
                     'logic': 'action_list/v2'},
        compute=compute, refresh=refresh,
    )
    return {'token': token, 'state': result.value, 'cache': result.meta}


def _qss(theme: str) -> str:
    palette = palette_for(theme)
    tokens = TOKENS
    return f"""
QWidget#actionListPage {{
    background: {palette.bg};
    color: {palette.text};
    font-family: {tokens.font_family};
    font-size: {tokens.font_body}px;
}}
QLabel#actionTitle {{
    font-size: {tokens.font_title}px;
    font-weight: 700;
    color: {palette.text};
}}
QLabel#actionSubtitle {{
    font-size: {tokens.font_small}px;
    color: {palette.muted};
}}
QLabel#actionSectionTitle {{
    font-size: {tokens.font_section}px;
    font-weight: 700;
    color: {palette.text};
}}
QLabel#actionNotes {{
    font-size: {tokens.font_small}px;
    color: {palette.muted};
}}
QFrame#actionCard {{
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


class ActionListPage(QWidget):
    """模型操作清单页：结论卡 + 逐标的动作表。"""

    def __init__(self, host: Any = None, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName('actionListPage')
        self._host = host
        self.theme = 'dark' if is_dark_palette() else 'light'
        self._pal = palette_for(self.theme)
        self._token = 0
        self._busy = False
        self._task_id: str | None = None
        self._state: ActionListState | None = None
        self._rows: list[ActionRow] = []
        self._sort_col: int | None = None
        self._sort_stage = 0
        self.capital = 1e9
        self._build_ui()
        self.setStyleSheet(_qss(self.theme))
        self._connect_task_signals()

    # ------------------------------------------------------------------ UI
    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 14, 16, 14)
        root.setSpacing(12)
        root.addLayout(self._build_header())
        root.addWidget(self._build_conclusion())
        root.addWidget(self._build_table_card(), 1)
        self.notes_label = QLabel('')
        self.notes_label.setObjectName('actionNotes')
        self.notes_label.setWordWrap(True)
        root.addWidget(self.notes_label)

    def _build_header(self) -> QHBoxLayout:
        header = QHBoxLayout()
        title_box = QVBoxLayout()
        title_box.setSpacing(2)
        title = QLabel('模型操作清单')
        title.setObjectName('actionTitle')
        subtitle = QLabel(
            '模型侧该做什么：方向、整手股数、执行时点与失效条件（不含用户持仓）')
        subtitle.setObjectName('actionSubtitle')
        title_box.addWidget(title)
        title_box.addWidget(subtitle)
        header.addLayout(title_box)
        header.addStretch(1)
        self.status_label = QLabel('尚未计算')
        self.status_label.setObjectName('actionSubtitle')
        header.addWidget(self.status_label)
        self.refresh_button = QPushButton('重新计算')
        self.refresh_button.setCursor(Qt.CursorShape.PointingHandCursor)
        self.refresh_button.clicked.connect(lambda: self.load_async(refresh=True))
        header.addWidget(self.refresh_button)
        return header

    def _build_conclusion(self) -> QFrame:
        card = QFrame()
        card.setObjectName('actionCard')
        layout = QVBoxLayout(card)
        layout.setContentsMargins(18, 14, 18, 14)
        layout.setSpacing(6)
        section = QLabel('本期清单')
        section.setObjectName('actionSubtitle')
        layout.addWidget(section)
        self.period_label = QLabel('交易日 — ｜ 信号日 — ｜ 执行时点 —')
        self.period_label.setObjectName('actionSectionTitle')
        self.dist_label = QLabel('标的数 — ｜ 方向分布 —')
        self.dist_label.setObjectName('actionSubtitle')
        self.regime_label = QLabel('趋势 — ｜ 风格 —')
        self.regime_label.setObjectName('actionSubtitle')
        layout.addWidget(self.period_label)
        layout.addWidget(self.dist_label)
        layout.addWidget(self.regime_label)
        hint = QLabel(
            '执行口径：信号日 T 收盘 → T+1 开盘成交（执行日由信号日推导，不用台账交易日）；'
            '涨停/跌停不可成交、停牌阻断；方向按台账变动类型映射，股数按给定资金折算整手。')
        hint.setObjectName('actionSubtitle')
        hint.setWordWrap(True)
        hint.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        layout.addWidget(hint)
        return card

    def _build_table_card(self) -> QFrame:
        card = QFrame()
        card.setObjectName('actionCard')
        layout = QVBoxLayout(card)
        layout.setContentsMargins(14, 12, 14, 12)
        layout.setSpacing(8)
        section = QLabel('逐标的动作（默认先卖后买、持有沉底；表头可三态排序）')
        section.setObjectName('actionSectionTitle')
        layout.addWidget(section)
        self.table = QTableWidget(0, len(COLUMNS))
        self.table.setHorizontalHeaderLabels(COLUMNS)
        vertical = self.table.verticalHeader()
        vertical.setVisible(False)
        vertical.setMinimumSectionSize(MIN_ROW_HEIGHT)
        vertical.setMaximumSectionSize(MAX_ROW_HEIGHT)
        self.table.setAlternatingRowColors(False)
        self.table.setShowGrid(True)
        self.table.setWordWrap(True)
        self.table.setItemDelegate(WrappedRowDelegate(self.table))
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        self.table.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        header = self.table.horizontalHeader()
        header.setHighlightSections(False)
        header.setFixedHeight(HEADER_ROW_HEIGHT)
        header.setSectionsClickable(True)
        header.sectionClicked.connect(self._on_header_clicked)
        self.table.setSizePolicy(QSizePolicy.Policy.Expanding,
                                 QSizePolicy.Policy.Expanding)
        use_interactive_columns(self.table)
        layout.addWidget(self.table)
        return card

    # ------------------------------------------------------------ loading
    def _connect_task_signals(self) -> None:
        from src.ui.task_manager import global_task_manager

        self._task_manager = global_task_manager
        global_task_manager.task_completed.connect(self._on_task_completed)
        global_task_manager.task_error.connect(self._on_task_error)

    def load_async(self, refresh: bool = False) -> None:
        """首次打开或点击重新计算时触发后台计算；``refresh`` 强制绕过缓存。"""
        if self._busy:
            return
        self._busy = True
        self._token += 1
        self.refresh_button.setEnabled(False)
        self.refresh_button.setText('计算中…')
        self.status_label.setText('正在读取台账与三层状态…')
        self._task_id = self._task_manager.create_task(
            f'模型操作清单#{self._token}',
            _fetch_state,
            kwargs={'token': self._token, 'capital': self.capital,
                    'refresh': refresh},
            timeout_seconds=300,
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
    def render_state(self, state: ActionListState) -> None:
        """把结果灌入页面（测试与截图走同一入口）。"""
        self._state = state
        self._rows = list(state.rows)
        self._sort_col = None
        self._sort_stage = 0
        self.status_label.setText(f'计算完成（{state.trade_day or "—"}）')
        exec_state = '已过执行窗口' if state.exec_status == 'expired' else '待执行'
        self.period_label.setText(
            f'交易日 {state.trade_day or "—"} ｜ 信号日 {state.signal_day or "—"} ｜ '
            f'执行时点 {state.exec_date or "—"}（{exec_state}） ｜ '
            f'资金 {state.capital / 1e8:.2f} 亿')
        counts = state.direction_counts()
        dist = ' ｜ '.join(f'{key} {value}' for key, value in counts.items())
        self.dist_label.setText(f'标的数 {len(state.rows)} ｜ 方向分布 {dist or "—"}')
        self.regime_label.setText(
            f'趋势 {state.trend_label or "—"} ｜ 风格 {state.style_direction or "—"} ｜ '
            f'下一次再平衡 {state.next_signal or "—"} 收盘 → '
            f'{state.next_rebalance or "—"} 开盘')
        self.notes_label.setText('　'.join(state.notes))
        self._apply_state()

    def _on_header_clicked(self, column: int) -> None:
        if column == 0:
            return
        if column != self._sort_col:
            self._sort_col = column
            self._sort_stage = 1
        else:
            self._sort_stage = (self._sort_stage + 1) % 3
            if self._sort_stage == 0:
                self._sort_col = None
        header = self.table.horizontalHeader()
        if self._sort_col is None:
            header.setSortIndicatorShown(False)
            header.setSortIndicator(-1, Qt.SortOrder.AscendingOrder)
        else:
            order = (Qt.SortOrder.AscendingOrder if self._sort_stage == 1
                     else Qt.SortOrder.DescendingOrder)
            header.setSortIndicator(self._sort_col, order)
            header.setSortIndicatorShown(True)
        self._apply_state()

    def _apply_state(self) -> None:
        rows = list(self._rows)
        if self._sort_col is not None and self._sort_stage:
            rows.sort(key=lambda row: _sort_value(row, self._sort_col),
                      reverse=self._sort_stage == 2)
        self._populate(rows)

    def _populate(self, rows: list[ActionRow]) -> None:
        table = self.table
        table.setUpdatesEnabled(False)
        table.setRowCount(0)
        table.setRowCount(len(rows))
        for index, row in enumerate(rows):
            values = [
                str(index + 1), row.code, row.name, row.direction,
                f'{row.shares}', f'{row.price:.2f}', f'{row.target_weight:.4f}',
                row.exec_date, row.condition, row.rationale,
            ]
            for column, text in enumerate(values):
                item = QTableWidgetItem(text)
                if column == 0:
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                elif column in (4, 5, 6):
                    item.setTextAlignment(Qt.AlignmentFlag.AlignRight
                                          | Qt.AlignmentFlag.AlignVCenter)
                table.setItem(index, column, item)
        self._unify_table_surface(table)
        wrapped = apply_content_widths(table)
        table.setColumnWidth(0, SEQ_COLUMN_WIDTH)
        self._apply_row_height(len(rows), wrapped)
        table.setUpdatesEnabled(True)

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

    def _apply_row_height(self, count: int, wrapped: bool) -> None:
        """未折行用统一行高；折行时按内容测高，大表一次性测完、排序时不重算。"""
        table = self.table
        vertical = table.verticalHeader()
        if not wrapped:
            vertical.setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
            vertical.setDefaultSectionSize(FIXED_ROW_HEIGHT)
            for row in range(count):
                table.setRowHeight(row, FIXED_ROW_HEIGHT)
            return
        if count > FIXED_ROW_HEIGHT_THRESHOLD:
            apply_wrapped_row_heights(table, MIN_ROW_HEIGHT, MAX_ROW_HEIGHT)
            for row in range(count):
                for column in range(1, table.columnCount()):
                    item = table.item(row, column)
                    if item is not None and len(item.text()) >= TOOLTIP_TEXT_MIN_LEN:
                        item.setToolTip(item.text())
            return
        vertical.setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        table.resizeRowsToContents()
