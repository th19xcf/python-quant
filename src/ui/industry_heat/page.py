"""主界面「行业热度」标签页：打开即后台计算并展示逐行业热度判定。"""

from __future__ import annotations

from typing import Any

from PySide6.QtCore import Qt
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

from src.quant.analysis.industry_heat_state import IndustryHeatState
from src.ui.advice.tokens import TOKENS, palette_for
from src.ui.theme_manager import is_dark_palette


def _fetch_state(token: int, asof: str | None = None,
                 task_id: str | None = None, signals: Any = None) -> dict[str, Any]:
    """后台任务：只读本地数据装配面板并计算行业热度。"""
    from datetime import datetime

    from src.quant.analysis.industry_heat_local import load_local_panel
    from src.quant.analysis.industry_heat_state import evaluate_industry_heat

    target = datetime.strptime(asof, '%Y-%m-%d').date() if asof else None
    state = evaluate_industry_heat(load_local_panel(), asof=target)
    return {'token': token, 'state': state}


def _qss(theme: str) -> str:
    palette = palette_for(theme)
    tokens = TOKENS
    return f"""
QWidget#industryHeatPage {{
    background: {palette.bg};
    color: {palette.text};
    font-family: {tokens.font_family};
    font-size: {tokens.font_body}px;
}}
QLabel#heatTitle {{
    font-size: {tokens.font_title}px;
    font-weight: 700;
    color: {palette.text};
}}
QLabel#heatSubtitle {{
    font-size: {tokens.font_small}px;
    color: {palette.muted};
}}
QLabel#heatStatus {{
    font-size: {tokens.font_small}px;
    color: {palette.muted};
}}
QLabel#heatSectionTitle {{
    font-size: {tokens.font_section}px;
    font-weight: 700;
    color: {palette.text};
}}
QLabel#heatNotes {{
    font-size: {tokens.font_small}px;
    color: {palette.muted};
}}
QFrame#heatCard {{
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


class IndustryHeatPage(QWidget):
    """行业热度页：结论卡 + 逐行业表。"""

    def __init__(self, host: Any = None, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName('industryHeatPage')
        self._host = host
        self.theme = 'dark' if is_dark_palette() else 'light'
        self._pal = palette_for(self.theme)
        self._token = 0
        self._busy = False
        self._task_id: str | None = None
        self._state: IndustryHeatState | None = None
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
        title = QLabel('行业热度状态')
        title.setObjectName('heatTitle')
        subtitle = QLabel(
            '六层滚动分位打分：相对强弱 / 量能 / 拥挤度 / 广度 / 估值 / 景气度（申万一级）')
        subtitle.setObjectName('heatSubtitle')
        title_box.addWidget(title)
        title_box.addWidget(subtitle)
        header.addLayout(title_box)
        header.addStretch(1)

        self.status_label = QLabel('尚未计算')
        self.status_label.setObjectName('heatStatus')
        header.addWidget(self.status_label)

        self.refresh_button = QPushButton('重新计算')
        self.refresh_button.setCursor(Qt.CursorShape.PointingHandCursor)
        self.refresh_button.clicked.connect(self.load_async)
        header.addWidget(self.refresh_button)
        root.addLayout(header)

        root.addWidget(self._build_conclusion())
        root.addWidget(self._build_table_card(), 1)

        self.notes_label = QLabel('')
        self.notes_label.setObjectName('heatNotes')
        self.notes_label.setWordWrap(True)
        root.addWidget(self.notes_label)

    def _build_conclusion(self) -> QFrame:
        card = QFrame()
        card.setObjectName('heatCard')
        layout = QVBoxLayout(card)
        layout.setContentsMargins(18, 14, 18, 14)
        layout.setSpacing(6)

        section = QLabel('当前结论')
        section.setObjectName('heatSubtitle')
        layout.addWidget(section)

        self.hottest_label = QLabel('最热：—')
        self.hottest_label.setObjectName('heatSectionTitle')
        self.coldest_label = QLabel('最冷：—')
        self.coldest_label.setObjectName('heatSectionTitle')
        layout.addWidget(self.hottest_label)
        layout.addWidget(self.coldest_label)

        hint = QLabel(
            '档位阈值：+0.60 过热 / +0.20 偏热 / ±0.20 中性 / -0.60 冷清；'
            '过热预警表示拥挤度指标处于极端分位，属热度状态描述，不是收益预测。')
        hint.setObjectName('heatSubtitle')
        hint.setWordWrap(True)
        hint.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        layout.addWidget(hint)
        return card

    def _build_table_card(self) -> QFrame:
        card = QFrame()
        card.setObjectName('heatCard')
        layout = QVBoxLayout(card)
        layout.setContentsMargins(14, 12, 14, 12)
        layout.setSpacing(8)
        section = QLabel('逐行业热度（按分数降序）')
        section.setObjectName('heatSectionTitle')
        layout.addWidget(section)

        self.heat_table = self._make_table(['行业', '热度分', '档位', '主要依据'])
        self.heat_table.setColumnWidth(0, 150)
        self.heat_table.setColumnWidth(1, 80)
        self.heat_table.setColumnWidth(2, 80)
        self.heat_table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeMode.Fixed)
        self.heat_table.horizontalHeader().setSectionResizeMode(
            2, QHeaderView.ResizeMode.Fixed)
        self.heat_table.horizontalHeader().setSectionResizeMode(
            3, QHeaderView.ResizeMode.Stretch)
        layout.addWidget(self.heat_table)
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
        return table

    # -------------------------------------------------------------- loading
    def _connect_task_signals(self) -> None:
        from src.ui.task_manager import global_task_manager

        self._task_manager = global_task_manager
        global_task_manager.task_completed.connect(self._on_task_completed)
        global_task_manager.task_error.connect(self._on_task_error)

    def load_async(self) -> None:
        """首次打开或点击重新计算时触发后台判定。"""
        if self._busy:
            return
        self._busy = True
        self._token += 1
        self.refresh_button.setEnabled(False)
        self.refresh_button.setText('计算中…')
        self.status_label.setText('正在聚合个股行情并计算…')
        self._task_id = self._task_manager.create_task(
            f'行业热度判定#{self._token}',
            _fetch_state,
            kwargs={'token': self._token},
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
    def render_state(self, state: IndustryHeatState) -> None:
        """把判定结果灌入页面（测试与截图走同一入口）。"""
        self._state = state
        self.status_label.setText(f'判定完成（{state.asof}）')
        self._fill_conclusion(state)
        self._fill_table(state)
        self.notes_label.setText('\n'.join(state.notes))

    def _fill_conclusion(self, state: IndustryHeatState) -> None:
        hottest = state.industries[:3]
        coldest = state.industries[-3:]
        self.hottest_label.setText('最热：' + '、'.join(
            f'{heat.name} {heat.total_score:+.2f} {heat.label}' for heat in hottest))
        self.coldest_label.setText('最冷：' + '、'.join(
            f'{heat.name} {heat.total_score:+.2f} {heat.label}'
            for heat in reversed(coldest)))
        self.hottest_label.setStyleSheet(f'color: {self._pal.buy};')
        self.coldest_label.setStyleSheet(f'color: {self._pal.sell};')

    def _fill_table(self, state: IndustryHeatState) -> None:
        table = self.heat_table
        table.setRowCount(len(state.industries))
        for row, heat in enumerate(state.industries):
            basis = []
            if heat.drivers:
                basis.append('拉动：' + '、'.join(heat.drivers))
            if heat.drags:
                basis.append('拖累：' + '、'.join(heat.drags))
            drivers = '｜'.join(basis) if basis else '—'
            if heat.overheat:
                drivers = f'{drivers}（过热预警：{"；".join(heat.overheat_reasons)}）'
            values = [heat.name, f'{heat.total_score:+.3f}', heat.label, drivers]
            alignments = (
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
            )
            for column, text in enumerate(values):
                item = self._item(text, alignments[column])
                if column == 1:
                    item.setForeground(self._brush(self._score_color(heat.total_score)))
                if column == 2:
                    item.setForeground(self._brush(self._label_color(heat.label)))
                if column == 3:
                    item.setToolTip(f'行业代码 {heat.code}｜数据截至 {heat.asof}')
                table.setItem(row, column, item)
        table.resizeRowsToContents()

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
        if label in ('过热', '偏热'):
            return self._pal.buy
        if label in ('偏冷', '冷清'):
            return self._pal.sell
        return self._pal.hold
