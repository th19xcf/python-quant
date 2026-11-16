"""主界面「风格轮动」标签页：打开即后台计算并展示风格维度与代表组合。"""

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

from src.quant.analysis.style_rotation_state import StyleRotationState
from src.ui.advice.tokens import TOKENS, palette_for
from src.ui.theme_manager import is_dark_palette


def _fetch_state(token: int, asof: str | None = None,
                 task_id: str | None = None, signals: Any = None) -> dict[str, Any]:
    """后台任务：只读本地数据装配面板并计算风格状态。"""
    from datetime import datetime

    from src.quant.analysis.style_rotation_local import load_local_panel
    from src.quant.analysis.style_rotation_state import evaluate_style_rotation

    target = datetime.strptime(asof, '%Y-%m-%d').date() if asof else None
    state = evaluate_style_rotation(load_local_panel(), asof=target)
    return {'token': token, 'state': state}


def _qss(theme: str) -> str:
    palette = palette_for(theme)
    tokens = TOKENS
    return f"""
QWidget#styleRotationPage {{
    background: {palette.bg};
    color: {palette.text};
    font-family: {tokens.font_family};
    font-size: {tokens.font_body}px;
}}
QLabel#styleTitle {{
    font-size: {tokens.font_title}px;
    font-weight: 700;
    color: {palette.text};
}}
QLabel#styleSubtitle {{
    font-size: {tokens.font_small}px;
    color: {palette.muted};
}}
QLabel#styleStatus {{
    font-size: {tokens.font_small}px;
    color: {palette.muted};
}}
QLabel#styleSectionTitle {{
    font-size: {tokens.font_section}px;
    font-weight: 700;
    color: {palette.text};
}}
QLabel#styleNotes {{
    font-size: {tokens.font_small}px;
    color: {palette.muted};
}}
QFrame#styleCard {{
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
    gridline-color: transparent;
    selection-background-color: {palette.selection};
}}
QHeaderView::section {{
    background: {palette.elevated};
    color: {palette.muted};
    border: none;
    border-bottom: 1px solid {palette.border};
    padding: 6px 8px;
    font-weight: 600;
}}
QTableWidget::item {{
    padding: 3px 6px;
}}
"""


class StyleRotationPage(QWidget):
    """风格轮动页：结论卡 + 维度表 + 代表组合表。"""

    def __init__(self, host: Any = None, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName('styleRotationPage')
        self._host = host
        self.theme = 'dark' if is_dark_palette() else 'light'
        self._pal = palette_for(self.theme)
        self._token = 0
        self._busy = False
        self._task_id: str | None = None
        self._state: StyleRotationState | None = None
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
        title = QLabel('风格轮动状态')
        title.setObjectName('styleTitle')
        subtitle = QLabel(
            '三组维度滚动分位打分：规模 / 价值成长 / 高波低波（全 A 等权三分组）')
        subtitle.setObjectName('styleSubtitle')
        title_box.addWidget(title)
        title_box.addWidget(subtitle)
        header.addLayout(title_box)
        header.addStretch(1)

        self.status_label = QLabel('尚未计算')
        self.status_label.setObjectName('styleStatus')
        header.addWidget(self.status_label)

        self.refresh_button = QPushButton('重新计算')
        self.refresh_button.setCursor(Qt.CursorShape.PointingHandCursor)
        self.refresh_button.clicked.connect(self.load_async)
        header.addWidget(self.refresh_button)
        root.addLayout(header)

        root.addWidget(self._build_conclusion())
        root.addWidget(self._build_dimension_card())
        root.addWidget(self._build_portfolio_card(), 1)

        self.notes_label = QLabel('')
        self.notes_label.setObjectName('styleNotes')
        self.notes_label.setWordWrap(True)
        root.addWidget(self.notes_label)

    def _build_conclusion(self) -> QFrame:
        card = QFrame()
        card.setObjectName('styleCard')
        layout = QVBoxLayout(card)
        layout.setContentsMargins(18, 14, 18, 14)
        layout.setSpacing(6)

        section = QLabel('当前结论')
        section.setObjectName('styleSubtitle')
        layout.addWidget(section)

        self.direction_label = QLabel('当前占优：—')
        self.direction_label.setObjectName('styleSectionTitle')
        self.strength_label = QLabel('分化强度：—')
        self.portfolio_label = QLabel('代表组合：—')
        self.portfolio_label.setObjectName('styleSubtitle')
        layout.addWidget(self.direction_label)
        layout.addWidget(self.strength_label)
        layout.addWidget(self.portfolio_label)

        hint = QLabel(
            '档位阈值：维度得分 ≥+0.20 正向占优 / ≤−0.20 反向占优 / 其余均衡；'
            '分化强度为各维度 |得分| 的加权平均，越高表示风格越极端，不表示多空方向。')
        hint.setObjectName('styleSubtitle')
        hint.setWordWrap(True)
        hint.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        layout.addWidget(hint)
        return card

    def _build_dimension_card(self) -> QFrame:
        card = QFrame()
        card.setObjectName('styleCard')
        layout = QVBoxLayout(card)
        layout.setContentsMargins(14, 12, 14, 12)
        layout.setSpacing(8)
        section = QLabel('维度得分与依据')
        section.setObjectName('styleSectionTitle')
        layout.addWidget(section)

        self.dimension_table = self._make_table(['维度', '得分', '档位', '主要依据'])
        self.dimension_table.setColumnWidth(0, 120)
        self.dimension_table.setColumnWidth(1, 80)
        self.dimension_table.setColumnWidth(2, 90)
        self.dimension_table.horizontalHeader().setSectionResizeMode(
            3, QHeaderView.ResizeMode.Stretch)
        layout.addWidget(self.dimension_table)
        return card

    def _build_portfolio_card(self) -> QFrame:
        card = QFrame()
        card.setObjectName('styleCard')
        layout = QVBoxLayout(card)
        layout.setContentsMargins(14, 12, 14, 12)
        layout.setSpacing(8)
        section = QLabel('代表风格组合近期强弱（等权）')
        section.setObjectName('styleSectionTitle')
        layout.addWidget(section)

        self.portfolio_table = self._make_table(
            ['组合', '20 日', '60 日', '相对全市场(20 日)'])
        self.portfolio_table.setColumnWidth(0, 120)
        self.portfolio_table.setColumnWidth(1, 100)
        self.portfolio_table.setColumnWidth(2, 100)
        self.portfolio_table.horizontalHeader().setSectionResizeMode(
            3, QHeaderView.ResizeMode.Stretch)
        layout.addWidget(self.portfolio_table)
        return card

    def _make_table(self, headers: list[str]) -> QTableWidget:
        table = QTableWidget(0, len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.verticalHeader().setVisible(False)
        table.setAlternatingRowColors(True)
        table.setShowGrid(False)
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
            f'风格轮动判定#{self._token}',
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
    def render_state(self, state: StyleRotationState) -> None:
        """把判定结果灌入页面（测试与截图走同一入口）。"""
        self._state = state
        self.status_label.setText(f'判定完成（{state.asof}）')
        self._fill_conclusion(state)
        self._fill_dimensions(state)
        self._fill_portfolios(state)
        self.notes_label.setText('\n'.join(state.notes))

    def _fill_conclusion(self, state: StyleRotationState) -> None:
        self.direction_label.setText(f'当前占优：{state.direction}')
        self.strength_label.setText(
            f'分化强度：{state.total_score:.2f}（0~1，越高越极端）')
        ranked = sorted(
            [item for item in state.portfolios if item.rel20 is not None],
            key=lambda item: item.rel20 or 0.0,
            reverse=True,
        )
        if ranked:
            top, bottom = ranked[0], ranked[-1]
            top_rel = top.rel20 or 0.0
            bottom_rel = bottom.rel20 or 0.0
            self.portfolio_label.setText(
                f'近期最强：{top.name}（20 日超额 {top_rel * 100:+.2f}%）｜'
                f'最弱：{bottom.name}（{bottom_rel * 100:+.2f}%）')
        else:
            self.portfolio_label.setText('代表组合：—')

    def _fill_dimensions(self, state: StyleRotationState) -> None:
        table = self.dimension_table
        table.setRowCount(len(state.dimensions))
        for row, dimension in enumerate(state.dimensions):
            basis = []
            if dimension.drivers:
                basis.append('拉动：' + '、'.join(dimension.drivers))
            if dimension.drags:
                basis.append('拖累：' + '、'.join(dimension.drags))
            values = [
                dimension.name,
                f'{dimension.score:+.3f}',
                dimension.label,
                '｜'.join(basis) if basis else '—',
            ]
            for column, text in enumerate(values):
                item = self._item(text)
                if column == 1:
                    item.setForeground(self._brush(self._score_color(dimension.score)))
                if column == 2:
                    item.setToolTip(f'实际权重 {dimension.weight:.0%}｜{dimension.description}')
                table.setItem(row, column, item)
        table.resizeRowsToContents()

    def _fill_portfolios(self, state: StyleRotationState) -> None:
        table = self.portfolio_table
        table.setRowCount(len(state.portfolios))
        for row, portfolio in enumerate(state.portfolios):
            values = [
                portfolio.name,
                self._pct(portfolio.ret20),
                self._pct(portfolio.ret60),
                self._pct(portfolio.rel20),
            ]
            for column, text in enumerate(values):
                item = self._item(text)
                if column in (1, 2, 3):
                    value = (portfolio.ret20, portfolio.ret60, portfolio.rel20)[column - 1]
                    if value is not None:
                        item.setForeground(self._brush(self._score_color(value)))
                table.setItem(row, column, item)
        table.resizeRowsToContents()

    @staticmethod
    def _pct(value: float | None) -> str:
        return '—' if value is None else f'{value * 100:+.2f}%'

    @staticmethod
    def _item(text: str) -> QTableWidgetItem:
        item = QTableWidgetItem(text)
        item.setFlags(Qt.ItemFlag.ItemIsEnabled)
        return item

    def _brush(self, color: str):
        from PySide6.QtGui import QBrush, QColor

        return QBrush(QColor(color))

    def _score_color(self, score: float) -> str:
        if score > 0.005:
            return self._pal.buy
        if score < -0.005:
            return self._pal.sell
        return self._pal.hold
