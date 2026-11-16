"""主界面「收益归因」标签页：run 级超额 + 行业暴露 + 风格暴露（口径降级）。"""

from __future__ import annotations

from typing import Any

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor, QPalette
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
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

from src.quant.analysis.attribution_state import AttributionState
from src.ui.advice.tokens import TOKENS, palette_for
from src.ui.table_sizing import apply_content_widths, use_interactive_columns
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


def _fetch_state(token: int, asof: str | None = None, benchmark: str = 'equal_all',
                 task_id: str | None = None, signals: Any = None) -> dict[str, Any]:
    """后台任务：只读已有回测 run 与持仓做归因（有持仓表走完整 Brinson）。"""
    from src.quant.analysis.attribution_local import build_attribution

    return {'token': token, 'state': build_attribution(asof=asof, benchmark=benchmark)}


def _qss(theme: str) -> str:
    palette = palette_for(theme)
    tokens = TOKENS
    return f"""
QWidget#attributionPage {{
    background: {palette.bg};
    color: {palette.text};
    font-family: {tokens.font_family};
    font-size: {tokens.font_body}px;
}}
QLabel#attrTitle {{
    font-size: {tokens.font_title}px;
    font-weight: 700;
    color: {palette.text};
}}
QLabel#attrSubtitle {{
    font-size: {tokens.font_small}px;
    color: {palette.muted};
}}
QLabel#attrBanner {{
    font-size: {tokens.font_small}px;
    color: {palette.hold};
}}
QLabel#attrSectionTitle {{
    font-size: {tokens.font_section}px;
    font-weight: 700;
    color: {palette.text};
}}
QLabel#attrNotes {{
    font-size: {tokens.font_small}px;
    color: {palette.muted};
}}
QFrame#attrCard {{
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


class AttributionPage(QWidget):
    """收益归因页：降级说明 + run 超额 + 行业暴露 + 风格暴露。"""

    def __init__(self, host: Any = None, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName('attributionPage')
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
        self._state: AttributionState | None = None
        self._build_ui()
        self.setStyleSheet(_qss(self.theme))
        self._connect_task_signals()

    # ------------------------------------------------------------------ UI
    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 14, 16, 14)
        root.setSpacing(12)

        root.addLayout(self._build_header())
        root.addWidget(self._build_banner())
        root.addWidget(self._build_brinson_card())
        root.addWidget(self._build_excess_card())
        root.addWidget(self._build_industry_card())
        root.addWidget(self._build_style_card(), 1)

        self.notes_label = QLabel('')
        self.notes_label.setObjectName('attrNotes')
        self.notes_label.setWordWrap(True)
        root.addWidget(self.notes_label)

    def _build_header(self) -> QHBoxLayout:
        header = QHBoxLayout()
        title_box = QVBoxLayout()
        title_box.setSpacing(2)
        title = QLabel('收益归因')
        title.setObjectName('attrTitle')
        subtitle = QLabel(
            '复用已有回测 run：Brinson 两段分解 / run 级超额与暴露；测试 run 自动排除')
        subtitle.setObjectName('attrSubtitle')
        title_box.addWidget(title)
        title_box.addWidget(subtitle)
        header.addLayout(title_box)
        header.addStretch(1)

        self.status_label = QLabel('尚未计算')
        self.status_label.setObjectName('attrSubtitle')
        header.addWidget(self.status_label)

        self.benchmark_box = QComboBox()
        self.benchmark_box.addItem('全市场等权', 'equal_all')
        self.benchmark_box.addItem('中证全指', 'csi_all')
        self.benchmark_box.addItem('行业等权', 'equal_industry')
        self.benchmark_box.addItem('沪深 300（仅作参考，含大盘风格偏差）', 'csi300')
        self.benchmark_box.setToolTip('完整 Brinson 的基准权重口径；切换后自动重算')
        self.benchmark_box.currentIndexChanged.connect(self._on_benchmark_changed)
        header.addWidget(self.benchmark_box)

        self.refresh_button = QPushButton('重新计算')
        self.refresh_button.setCursor(Qt.CursorShape.PointingHandCursor)
        self.refresh_button.clicked.connect(self.load_async)
        header.addWidget(self.refresh_button)
        return header

    def _build_banner(self) -> QFrame:
        card = QFrame()
        card.setObjectName('attrCard')
        layout = QVBoxLayout(card)
        layout.setContentsMargins(18, 12, 18, 12)
        layout.setSpacing(4)
        section = QLabel('口径说明')
        section.setObjectName('attrSubtitle')
        layout.addWidget(section)

        self.banner_label = QLabel(
            '完整 Brinson 两段分解需要逐期持仓、逐笔成交与基准成分行业权重，'
            '现有 run 只保存净值序列，故本页为降级口径。')
        self.banner_label.setObjectName('attrBanner')
        self.banner_label.setWordWrap(True)
        layout.addWidget(self.banner_label)

        self.missing_label = QLabel('缺失：—')
        self.missing_label.setObjectName('attrSubtitle')
        self.missing_label.setWordWrap(True)
        layout.addWidget(self.missing_label)
        return card

    def _build_brinson_card(self) -> QFrame:
        card = QFrame()
        card.setObjectName('attrCard')
        layout = QVBoxLayout(card)
        layout.setContentsMargins(14, 12, 14, 12)
        layout.setSpacing(8)
        section = QLabel('完整 Brinson 两段分解（逐期）')
        section.setObjectName('attrSectionTitle')
        layout.addWidget(section)

        self.benchmark_table = self._make_table(
            ['基准', '行业配置', '个股选择', '交叉项', '累计超额', 'IR', 'α', 't'])
        self.benchmark_table.setToolTip(
            '双主基准并排：全市场等权 / 中证全指；中证全指无成分权重，三效应留空')
        layout.addWidget(self.benchmark_table)

        self.brinson_summary = QLabel('该 run 无 positions.parquet，未做分解')
        self.brinson_summary.setObjectName('attrSubtitle')
        self.brinson_summary.setWordWrap(True)
        layout.addWidget(self.brinson_summary)

        self.brinson_table = self._make_table(
            ['区间', '行业配置', '个股选择', '交叉项', '组合收益', '基准收益'])
        self.brinson_table.setColumnWidth(0, 200)
        for column in (1, 2, 3, 4, 5):
            self.brinson_table.setColumnWidth(column, 110)
        layout.addWidget(self.brinson_table)
        self.brinson_card = card
        card.setVisible(False)
        return card

    def _build_excess_card(self) -> QFrame:
        card = QFrame()
        card.setObjectName('attrCard')
        layout = QVBoxLayout(card)
        layout.setContentsMargins(14, 12, 14, 12)
        layout.setSpacing(8)
        section = QLabel('run 级超额（组合净值 vs 000300.SH）')
        section.setObjectName('attrSectionTitle')
        layout.addWidget(section)

        self.excess_table = self._make_table(['指标', '数值', '说明'])
        self.excess_table.setColumnWidth(0, 180)
        self.excess_table.setColumnWidth(1, 120)
        layout.addWidget(self.excess_table)
        return card

    def _build_industry_card(self) -> QFrame:
        card = QFrame()
        card.setObjectName('attrCard')
        layout = QVBoxLayout(card)
        layout.setContentsMargins(14, 12, 14, 12)
        layout.setSpacing(8)
        section = QLabel('行业暴露（申万一级，对照全 A 等权）')
        section.setObjectName('attrSectionTitle')
        layout.addWidget(section)

        self.industry_table = self._make_table(['行业', '组合权重', '基准占比', '偏离'])
        self.industry_table.setColumnWidth(0, 160)
        self.industry_table.setColumnWidth(1, 110)
        self.industry_table.setColumnWidth(2, 110)
        self.industry_table.setColumnWidth(3, 110)
        layout.addWidget(self.industry_table)
        return card

    def _build_style_card(self) -> QFrame:
        card = QFrame()
        card.setObjectName('attrCard')
        layout = QVBoxLayout(card)
        layout.setContentsMargins(14, 12, 14, 12)
        layout.setSpacing(8)
        section = QLabel('风格暴露（持仓加权分位，对照全市场中位 50）')
        section.setObjectName('attrSectionTitle')
        layout.addWidget(section)

        self.style_table = self._make_table(['维度', '组合分位', '基准', '偏离', '说明'])
        self.style_table.setColumnWidth(0, 130)
        self.style_table.setColumnWidth(1, 100)
        self.style_table.setColumnWidth(2, 90)
        self.style_table.setColumnWidth(3, 90)
        layout.addWidget(self.style_table)
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
        if remeasure:
            apply_content_widths(table)
        self._apply_row_height_mode(table, remeasure=remeasure)
        table.setColumnWidth(0, SEQ_COLUMN_WIDTH)
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
                               remeasure: bool = True) -> None:
        """大表用固定行高，避免排序时逐行测量换行文本；小表保留自适应。"""
        vertical = table.verticalHeader()
        if table.rowCount() > FIXED_ROW_HEIGHT_THRESHOLD:
            vertical.setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
            vertical.setDefaultSectionSize(FIXED_ROW_HEIGHT)
            for row in range(table.rowCount()):
                table.setRowHeight(row, FIXED_ROW_HEIGHT)
            return
        vertical.setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        if remeasure:
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
        cells = [[table.takeItem(row, index) for index in range(table.columnCount())]
                 for row in range(table.rowCount())]
        for row, source in enumerate(order):
            for index, cell in enumerate(cells[source]):
                if cell is not None:
                    table.setItem(row, index, cell)

    def _sort_table_rows(self, table: QTableWidget, column: int,
                         order: Qt.SortOrder) -> None:
        """按解析后的键重排行，空值沉底；文本、对齐、颜色与提示原样保留。"""
        cells: list[list[QTableWidgetItem | None]] = [
            [table.takeItem(row, index) for index in range(table.columnCount())]
            for row in range(table.rowCount())]
        if not cells:
            return
        keyed: list[tuple[tuple[str, float | str], list[QTableWidgetItem | None]]] = []
        empty: list[list[QTableWidgetItem | None]] = []
        for row_cells in cells:
            cell = row_cells[column]
            key = _sort_key(cell.text()) if cell is not None else None
            if key is None:
                empty.append(row_cells)
            else:
                keyed.append((key, row_cells))
        keyed.sort(key=lambda pair: pair[0],
                   reverse=order == Qt.SortOrder.DescendingOrder)
        for row, row_cells in enumerate([pair[1] for pair in keyed] + empty):
            for index, cell in enumerate(row_cells):
                if cell is not None:
                    table.setItem(row, index, cell)

    # -------------------------------------------------------------- loading
    def _connect_task_signals(self) -> None:
        from src.ui.task_manager import global_task_manager

        self._task_manager = global_task_manager
        global_task_manager.task_completed.connect(self._on_task_completed)
        global_task_manager.task_error.connect(self._on_task_error)

    def load_async(self) -> None:
        """首次打开或点击重新计算时触发后台归因。"""
        if self._busy:
            return
        self._busy = True
        self._token += 1
        self.refresh_button.setEnabled(False)
        self.refresh_button.setText('计算中…')
        self.status_label.setText('正在读取回测 run 与持仓…')
        self._task_id = self._task_manager.create_task(
            f'收益归因#{self._token}',
            _fetch_state,
            kwargs={'token': self._token, 'benchmark': self._benchmark_mode()},
            timeout_seconds=600,
        )

    def _benchmark_mode(self) -> str:
        data = self.benchmark_box.currentData()
        return str(data or 'equal_all')

    def _on_benchmark_changed(self, index: int) -> None:
        if self._state is None:
            return
        self.load_async()

    def _on_task_completed(self, task_id: str, result: Any) -> None:
        if task_id != self._task_id:
            return
        self._finish_busy()
        state = result.get('state') if isinstance(result, dict) else None
        if state is None:
            self.status_label.setText('未返回结果')
            return
        self.render_state(state)

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
    def render_state(self, state: AttributionState) -> None:
        """把归因结果灌入页面（测试与截图走同一入口）。"""
        self._state = state
        run_name = state.run.name if state.run else '无 run'
        self.status_label.setText(f'归因完成（{run_name}）')
        from src.quant.analysis.attribution_local import is_test_run

        if state.run is not None and is_test_run(state.run.name):
            # 跳转前自检：测试/冒烟 run 不静默出数
            self.status_label.setText('归因失败：选中 run 属测试/冒烟 run')
            self.banner_label.setText(
                f'错误：run {state.run.name} 属测试/冒烟样本，禁止进入归因。')
            self.missing_label.setText('缺失：—')
            self.brinson_card.setVisible(False)
            self.brinson_table.setRowCount(0)
            self._fill_excess(state)
            self._fill_industries(state)
            self._fill_styles(state)
            self.notes_label.setText('\n'.join(state.notes))
            return
        if state.run is None:
            self.status_label.setText('暂无可用正式回测')
            self.banner_label.setText('暂无可用正式回测，请先跑一次正式 spec。')
        elif state.degraded:
            self.banner_label.setText(
                '口径降级：该 run 无持仓明细（positions.parquet），'
                'Brinson 两段分解不可做，当前为 run 级超额 + 单期行业/风格暴露。')
        else:
            benchmark = state.brinson.benchmark if state.brinson else state.benchmark
            self.banner_label.setText(
                f'完整 Brinson 两段分解：基准 {benchmark}，'
                '行业归属用申万 PIT，逐期用区间期初权重与区间收益。')
        self.missing_label.setText('缺失：' + ('；'.join(state.missing) or '—'))
        if state.run is not None and state.run.experimental:
            self.banner_label.setText(
                '注意：该 run 为零成本实验，不得用于正式结论。'
                + self.banner_label.text())
        self._fill_brinson(state)
        self._fill_excess(state)
        self._fill_industries(state)
        self._fill_styles(state)
        self.notes_label.setText('\n'.join(state.notes))

    def _fill_benchmarks(self, benchmarks: tuple[Any, ...]) -> None:
        """双主基准并排：全市场等权 + 中证全指（后者三效应留空）。"""
        table = self.benchmark_table
        self._prepare_table(table)
        rows = list(benchmarks)
        if not rows:
            table.setRowCount(1)
            table.setItem(0, 0, self._item('双主基准', Qt.AlignmentFlag.AlignLeft))
            for column in range(1, 8):
                table.setItem(0, column,
                              self._item('—', Qt.AlignmentFlag.AlignRight))
            self._finalize_table(table)
            return
        table.setRowCount(len(rows))
        for row, item in enumerate(rows):
            values = [
                item.benchmark,
                self._pct(item.allocation),
                self._pct(item.selection),
                self._pct(item.interaction),
                f'{item.excess:+.2%}',
                self._num(item.information_ratio),
                self._pct(item.alpha),
                self._num(item.alpha_t),
            ]
            for column, text in enumerate(values):
                align = (Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter
                         if column == 0 else
                         Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                table.setItem(row, column, self._item(text, align))
        self._finalize_table(table)

    def _fill_brinson(self, state: AttributionState) -> None:
        item = state.brinson
        self._fill_benchmarks(state.benchmarks)
        if item is None:
            self.brinson_card.setVisible(False)
            self.brinson_table.setRowCount(0)
            return
        self.brinson_card.setVisible(True)
        extra = ''
        if item.allocation is None:
            extra = '｜该基准无成分权重，不计算三效应'
        self.brinson_summary.setText(
            f'基准 {item.benchmark}｜{len(item.periods)} 期｜累计超额 '
            f'{item.excess:+.2%}｜IR {self._num(item.information_ratio)}'
            f'｜α {self._pct(item.alpha)}｜t {self._num(item.alpha_t)}'
            f'｜配置 {self._pct(item.allocation)}｜选择 {self._pct(item.selection)}'
            f'｜交叉 {self._pct(item.interaction)}{extra}')
        table = self.brinson_table
        self._prepare_table(table)
        table.setRowCount(len(item.periods))
        for row, period in enumerate(item.periods):
            values = [
                f'{period.start} ~ {period.end}',
                self._pct(period.allocation),
                self._pct(period.selection),
                self._pct(period.interaction),
                self._pct(period.portfolio_return),
                self._pct(period.benchmark_return),
            ]
            alignments = (
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
            )
            for column, text in enumerate(values):
                table.setItem(row, column, self._item(text, alignments[column]))
        self._finalize_table(table)

    def _fill_excess(self, state: AttributionState) -> None:
        table = self.excess_table
        self._prepare_table(table)
        item = state.excess
        if item is None:
            table.setRowCount(1)
            table.setItem(0, 0, self._item('run 超额', Qt.AlignmentFlag.AlignLeft))
            table.setItem(0, 1, self._item('—', Qt.AlignmentFlag.AlignRight))
            table.setItem(0, 2, self._item('净值或基准序列不足', Qt.AlignmentFlag.AlignLeft))
            self._finalize_table(table)
            return
        rows = [
            ('组合年化收益', self._pct(item.annual_return), f'{item.days} 个交易日'),
            ('基准年化收益', self._pct(item.bench_annual), '000300.SH'),
            ('年化超额', self._pct(item.excess_annual), '组合 − 基准'),
            ('beta', self._num(item.beta), '日收益对基准回归'),
            ('跟踪误差', self._pct(item.tracking_error), '超额日收益标准差年化'),
            ('信息比率', self._num(item.information_ratio), '年化超额 / 跟踪误差'),
        ]
        table.setRowCount(len(rows))
        for row, (name, value, note) in enumerate(rows):
            alignments = (
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
            )
            for column, text in enumerate((name, value, note)):
                table.setItem(row, column, self._item(text, alignments[column]))
        self._finalize_table(table)

    def _fill_industries(self, state: AttributionState) -> None:
        table = self.industry_table
        self._prepare_table(table)
        table.setRowCount(len(state.industries))
        for row, item in enumerate(state.industries):
            values = [
                item.name,
                self._pct(item.weight, sign=False),
                '—' if item.baseline is None else f'{item.baseline:.2%}',
                self._pct(item.diff),
            ]
            alignments = (
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
            )
            for column, text in enumerate(values):
                cell = self._item(text, alignments[column])
                if column == 3 and item.diff is not None:
                    cell.setForeground(self._brush(self._score_color(item.diff)))
                table.setItem(row, column, cell)
        self._finalize_table(table)

    def _fill_styles(self, state: AttributionState) -> None:
        table = self.style_table
        self._prepare_table(table)
        table.setRowCount(len(state.styles))
        for row, item in enumerate(state.styles):
            values = [
                item.name,
                self._num(item.weight, nd=1),
                '—' if item.baseline is None else f'{item.baseline:.0f}',
                self._num(item.diff, nd=1),
                item.note,
            ]
            alignments = (
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
            )
            for column, text in enumerate(values):
                table.setItem(row, column, self._item(text, alignments[column]))
        self._finalize_table(table)

    @staticmethod
    def _pct(value: float | None, sign: bool = True) -> str:
        if value is None:
            return '—'
        return f'{value * 100:+.2f}%' if sign else f'{value * 100:.2f}%'

    @staticmethod
    def _num(value: float | None, nd: int = 2) -> str:
        return '—' if value is None else f'{value:+.{nd}f}'

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

    def _score_color(self, value: float) -> str:
        if value > 0.0005:
            return self._pal.buy
        if value < -0.0005:
            return self._pal.sell
        return self._pal.hold
