"""主界面「收益归因」标签页：run 级超额 + 行业暴露 + 风格暴露（口径降级）。"""

from __future__ import annotations

from typing import Any

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QFrame,
    QHBoxLayout,
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
    alternate-background-color: {palette.zebra};
    border: 1px solid {palette.border};
    border-radius: {tokens.radius_sm}px;
    gridline-color: {palette.faint};
    selection-background-color: {palette.selection};
}}
QHeaderView::section {{
    background: {palette.elevated};
    color: {palette.muted};
    border: none;
    border-bottom: 1px solid {palette.border};
    border-right: 1px solid {palette.border};
    padding: 6px 8px;
    font-weight: 600;
}}
QTableWidget::item {{
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
        title = QLabel('收益归因（口径降级）')
        title.setObjectName('attrTitle')
        subtitle = QLabel(
            '复用已有回测 run：run 级超额 + 单期行业暴露 + 单期风格暴露')
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
        self.benchmark_box.addItem('行业等权', 'equal_industry')
        self.benchmark_box.addItem('沪深 300（无成分权重）', 'csi300')
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
        table.verticalHeader().setVisible(False)
        table.verticalHeader().setMinimumSectionSize(30)
        table.setAlternatingRowColors(True)
        table.setShowGrid(True)
        table.setWordWrap(True)
        table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        table.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        table.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        table.horizontalHeader().setHighlightSections(False)
        table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        use_interactive_columns(table)
        return table

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
        if state.degraded:
            self.banner_label.setText(
                '口径降级：该 run 无持仓明细（positions.parquet），'
                'Brinson 两段分解不可做，当前为 run 级超额 + 单期行业/风格暴露。')
        else:
            benchmark = state.brinson.benchmark if state.brinson else state.benchmark
            self.banner_label.setText(
                f'完整 Brinson 两段分解：基准 {benchmark}，'
                '行业归属用申万 PIT，逐期用区间期初权重与区间收益。')
        self.missing_label.setText('缺失：' + ('；'.join(state.missing) or '—'))
        self._fill_brinson(state)
        self._fill_excess(state)
        self._fill_industries(state)
        self._fill_styles(state)
        self.notes_label.setText('\n'.join(state.notes))

    def _fill_brinson(self, state: AttributionState) -> None:
        item = state.brinson
        if item is None:
            self.brinson_card.setVisible(False)
            self.brinson_table.setRowCount(0)
            return
        self.brinson_card.setVisible(True)
        self.brinson_summary.setText(
            f'基准 {item.benchmark}｜{len(item.periods)} 期｜累计超额 '
            f'{item.excess:+.2%}｜IR {self._num(item.information_ratio)}'
            f'｜α {self._pct(item.alpha)}｜t {self._num(item.alpha_t)}'
            f'｜配置 {self._pct(item.allocation)}｜选择 {self._pct(item.selection)}'
            f'｜交叉 {self._pct(item.interaction)}')
        table = self.brinson_table
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
        table.resizeRowsToContents()
        apply_content_widths(table)

    def _fill_excess(self, state: AttributionState) -> None:
        table = self.excess_table
        item = state.excess
        if item is None:
            table.setRowCount(1)
            table.setItem(0, 0, self._item('run 超额', Qt.AlignmentFlag.AlignLeft))
            table.setItem(0, 1, self._item('—', Qt.AlignmentFlag.AlignRight))
            table.setItem(0, 2, self._item('净值或基准序列不足', Qt.AlignmentFlag.AlignLeft))
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
        table.resizeRowsToContents()
        apply_content_widths(table)

    def _fill_industries(self, state: AttributionState) -> None:
        table = self.industry_table
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
        table.resizeRowsToContents()
        apply_content_widths(table)

    def _fill_styles(self, state: AttributionState) -> None:
        table = self.style_table
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
        table.resizeRowsToContents()
        apply_content_widths(table)

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
