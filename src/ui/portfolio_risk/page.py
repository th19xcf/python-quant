"""主界面「仓位与风控」标签页：目标仓位、风险预算、约束校验三块。"""

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

from src.quant.analysis.portfolio_risk_state import PortfolioRiskState
from src.ui.advice.tokens import TOKENS, palette_for
from src.ui.table_sizing import (
    FIXED_ROW_HEIGHT,
    FIXED_ROW_HEIGHT_THRESHOLD,
    HEADER_ROW_HEIGHT,
    MAX_ROW_HEIGHT,
    MIN_ROW_HEIGHT,
    SEQ_COLUMN_TITLE,
    TABLE_FONT_SIZE_PX,
    TOOLTIP_TEXT_MIN_LEN,
    WrappedRowDelegate,
    apply_content_widths,
    apply_wrapped_row_heights,
    reorder_table_rows,
    use_interactive_columns,
)
from src.ui.theme_manager import is_dark_palette

#: 环节列按业务语义排序：档位 → 波动率 → 回撤
LABEL_SORT_RANK: dict[str, int] = {'档位': 0, '波动率': 1, '回撤': 2}
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
    """后台任务：只读本地数据计算仓位、风险预算与约束校验（带版本落盘缓存）。"""
    from src.quant.analysis import analysis_cache
    from src.quant.analysis.portfolio_risk_local import (
        HOLDINGS_DIR,
        LEDGER_NAME,
        build_state,
    )

    ledger = analysis_cache.ROOT / HOLDINGS_DIR / LEDGER_NAME

    def compute() -> Any:
        return build_state(asof=asof)

    result = analysis_cache.cached(
        'portfolio_risk', 'state', asof=asof,
        fingerprint={'ledger': analysis_cache.file_stamp(ledger)},
        compute=compute, refresh=refresh,
    )
    return {'token': token, 'state': result.value, 'cache': result.meta}


def _qss(theme: str) -> str:
    palette = palette_for(theme)
    tokens = TOKENS
    return f"""
QWidget#portfolioRiskPage {{
    background: {palette.bg};
    color: {palette.text};
    font-family: {tokens.font_family};
    font-size: {TABLE_FONT_SIZE_PX}px;
}}
QLabel#riskTitle {{
    font-size: {tokens.font_title}px;
    font-weight: 700;
    color: {palette.text};
}}
QLabel#riskSubtitle {{
    font-size: {tokens.font_small}px;
    color: {palette.muted};
}}
QLabel#riskPosition {{
    font-size: {tokens.font_title}px;
    font-weight: 700;
    color: {palette.accent};
}}
QLabel#riskSectionTitle {{
    font-size: {tokens.font_section}px;
    font-weight: 700;
    color: {palette.text};
}}
QLabel#riskNotes {{
    font-size: {tokens.font_small}px;
    color: {palette.muted};
}}
QFrame#riskCard {{
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


class PortfolioRiskPage(QWidget):
    """仓位与风控页：结论卡 + 三段环节表 + 风险预算表 + 约束校验表。"""

    def __init__(self, host: Any = None, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName('portfolioRiskPage')
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
        self._state: PortfolioRiskState | None = None
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
        root.addWidget(self._build_steps_card())
        root.addWidget(self._build_budget_card())
        root.addWidget(self._build_constraints_card(), 1)

        self.notes_label = QLabel('')
        self.notes_label.setObjectName('riskNotes')
        self.notes_label.setWordWrap(True)
        root.addWidget(self.notes_label)

    def _build_header(self) -> QHBoxLayout:
        header = QHBoxLayout()
        title_box = QVBoxLayout()
        title_box.setSpacing(2)
        title = QLabel('仓位与风控')
        title.setObjectName('riskTitle')
        subtitle = QLabel(
            '趋势总分定档 → 波动率目标缩放 → 回撤制动，再叠加风险预算与持仓约束校验')
        subtitle.setObjectName('riskSubtitle')
        title_box.addWidget(title)
        title_box.addWidget(subtitle)
        header.addLayout(title_box)
        header.addStretch(1)

        self.status_label = QLabel('尚未计算')
        self.status_label.setObjectName('riskSubtitle')
        header.addWidget(self.status_label)

        self.refresh_button = QPushButton('重新计算')
        self.refresh_button.setCursor(Qt.CursorShape.PointingHandCursor)
        self.refresh_button.clicked.connect(lambda: self.load_async(refresh=True))
        header.addWidget(self.refresh_button)
        return header

    def _build_conclusion(self) -> QFrame:
        card = QFrame()
        card.setObjectName('riskCard')
        layout = QVBoxLayout(card)
        layout.setContentsMargins(18, 14, 18, 14)
        layout.setSpacing(6)

        section = QLabel('当前建议仓位')
        section.setObjectName('riskSubtitle')
        layout.addWidget(section)

        self.position_label = QLabel('—')
        self.position_label.setObjectName('riskPosition')
        self.tier_label = QLabel('趋势档位：—')
        self.tier_label.setObjectName('riskSubtitle')
        layout.addWidget(self.position_label)
        layout.addWidget(self.tier_label)

        hint = QLabel(
            '档位：总分 ≥+0.40 → 100%，+0.15~+0.40 → 60%，−0.15~+0.15 → 30%，≤−0.15 → 0%；'
            '波动率系数 = min(1, 15% / 20 日已实现波动)；'
            '回撤制动：近 250 日高点回撤 −10% 起线性减仓、−15% 清仓。'
            '三段相乘即为建议仓位，不加杠杆。')
        hint.setObjectName('riskSubtitle')
        hint.setWordWrap(True)
        hint.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        layout.addWidget(hint)
        return card

    def _build_steps_card(self) -> QFrame:
        card = QFrame()
        card.setObjectName('riskCard')
        layout = QVBoxLayout(card)
        layout.setContentsMargins(14, 12, 14, 12)
        layout.setSpacing(8)
        section = QLabel('三个环节的调整量与依据')
        section.setObjectName('riskSectionTitle')
        layout.addWidget(section)

        self.steps_table = self._make_table(['环节', '调整', '触发依据'])
        self.steps_table.setColumnWidth(0, 110)
        self.steps_table.setColumnWidth(1, 300)
        layout.addWidget(self.steps_table)
        return card

    def _build_budget_card(self) -> QFrame:
        card = QFrame()
        card.setObjectName('riskCard')
        layout = QVBoxLayout(card)
        layout.setContentsMargins(14, 12, 14, 12)
        layout.setSpacing(8)
        section = QLabel('风险预算')
        section.setObjectName('riskSectionTitle')
        layout.addWidget(section)

        self.budget_table = self._make_table(['指标', '读数', '说明'])
        self.budget_table.setColumnWidth(0, 180)
        self.budget_table.setColumnWidth(1, 120)
        layout.addWidget(self.budget_table)
        return card

    def _build_constraints_card(self) -> QFrame:
        card = QFrame()
        card.setObjectName('riskCard')
        layout = QVBoxLayout(card)
        layout.setContentsMargins(14, 12, 14, 12)
        layout.setSpacing(8)
        section = QLabel('约束校验（读 reports/target_holdings/ 最新持仓）')
        section.setObjectName('riskSectionTitle')
        layout.addWidget(section)

        self.constraint_table = self._make_table(['约束', '上限', '实际', '判定 / 明细'])
        self.constraint_table.setColumnWidth(0, 130)
        self.constraint_table.setColumnWidth(1, 90)
        self.constraint_table.setColumnWidth(2, 110)
        layout.addWidget(self.constraint_table)
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
        table.setItemDelegate(WrappedRowDelegate(table))
        table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        table.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        table.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        header = table.horizontalHeader()
        header.setHighlightSections(False)
        header.setFixedHeight(HEADER_ROW_HEIGHT)
        table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
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
        self._renumber_rows(table, remeasure=remeasure)

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
        """首次打开或点击重新计算时触发后台计算；``refresh`` 强制绕过缓存。"""
        if self._busy:
            return
        self._busy = True
        self._token += 1
        self.refresh_button.setEnabled(False)
        self.refresh_button.setText('计算中…')
        self.status_label.setText('正在读取趋势、基准与持仓…')
        self._task_id = self._task_manager.create_task(
            f'仓位风控#{self._token}',
            _fetch_state,
            kwargs={'token': self._token, 'refresh': refresh},
            timeout_seconds=180,
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
    def render_state(self, state: PortfolioRiskState) -> None:
        """把结果灌入页面（测试与截图走同一入口）。"""
        self._state = state
        self.status_label.setText(f'计算完成（{state.asof or "—"}）')
        decision = state.decision
        self.position_label.setText(f'建议仓位 {decision.target_position:.0%}')
        self.tier_label.setText(
            f'趋势档位：{decision.tier_label}｜波动率系数 {decision.vol_scale:.2f}'
            f'｜回撤系数 {decision.drawdown_factor:.2f}')
        self._fill_steps(state)
        self._fill_budget(state)
        self._fill_constraints(state)
        self.notes_label.setText('\n'.join(state.notes))

    def _fill_steps(self, state: PortfolioRiskState) -> None:
        decision = state.decision
        rows = [
            ('档位', f'{decision.tier_position:.0%}', decision.steps[0] if decision.steps else '—'),
            ('波动率', f'×{decision.vol_scale:.2f}',
             decision.steps[1] if len(decision.steps) > 1 else '—'),
            ('回撤', f'×{decision.drawdown_factor:.2f}',
             decision.steps[2] if len(decision.steps) > 2 else '—'),
        ]
        table = self.steps_table
        self._prepare_table(table)
        table.setRowCount(len(rows))
        for row, (name, adjust, basis) in enumerate(rows):
            values = [name, adjust, basis]
            alignments = (
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
            )
            for column, text in enumerate(values):
                table.setItem(row, column, self._item(text, alignments[column]))
        self._finalize_table(table)

    def _fill_budget(self, state: PortfolioRiskState) -> None:
        budget = state.budget
        rows = [
            ('已实现波动率（20 日年化）', self._pct(budget.realized_vol, sign=False),
             f'目标 {budget.target_vol:.0%}'),
            ('波动率缺口', self._pct(budget.vol_gap), '已实现 − 目标，正值表示超预算'),
            ('距回撤制动', self._pct(budget.drawdown_distance),
             '相对 −10% 首档制动线的距离，负值表示已进入减仓区'),
            ('年化风险敞口', self._pct(budget.risk_exposure, sign=False),
             '建议仓位 × 已实现波动率'),
        ]
        table = self.budget_table
        self._prepare_table(table)
        table.setRowCount(len(rows))
        for row, (name, value, note) in enumerate(rows):
            values = [name, value, note]
            alignments = (
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
            )
            for column, text in enumerate(values):
                table.setItem(row, column, self._item(text, alignments[column]))
        self._finalize_table(table)

    def _fill_constraints(self, state: PortfolioRiskState) -> None:
        report = state.constraints
        table = self.constraint_table
        self._prepare_table(table)
        table.setRowCount(len(report.items))
        for row, item in enumerate(report.items):
            values = [
                item.name,
                f'{item.limit:.0%}',
                '—' if item.actual is None else f'{item.actual:.2%}',
                f'{"通过" if item.passed else "违规"}｜{item.detail}',
            ]
            alignments = (
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
            )
            for column, text in enumerate(values):
                cell = self._item(text, alignments[column])
                if column == 3 and not item.passed:
                    cell.setForeground(self._brush(self._pal.sell))
                table.setItem(row, column, cell)
        self._finalize_table(table)

    @staticmethod
    def _pct(value: float | None, sign: bool = True) -> str:
        if value is None:
            return '—'
        return f'{value * 100:+.2f}%' if sign else f'{value * 100:.2f}%'

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
