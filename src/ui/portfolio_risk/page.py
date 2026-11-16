"""主界面「仓位与风控」标签页：目标仓位、风险预算、约束校验三块。"""

from __future__ import annotations

from typing import Any

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
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

from src.quant.analysis.portfolio_risk_state import PortfolioRiskState
from src.ui.advice.tokens import TOKENS, palette_for
from src.ui.table_sizing import apply_content_widths, use_interactive_columns
from src.ui.theme_manager import is_dark_palette


def _fetch_state(token: int, asof: str | None = None,
                 task_id: str | None = None, signals: Any = None) -> dict[str, Any]:
    """后台任务：只读本地数据计算仓位、风险预算与约束校验。"""
    from src.quant.analysis.portfolio_risk_local import build_state

    return {'token': token, 'state': build_state(asof=asof)}


def _qss(theme: str) -> str:
    palette = palette_for(theme)
    tokens = TOKENS
    return f"""
QWidget#portfolioRiskPage {{
    background: {palette.bg};
    color: {palette.text};
    font-family: {tokens.font_family};
    font-size: {tokens.font_body}px;
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


class PortfolioRiskPage(QWidget):
    """仓位与风控页：结论卡 + 三段环节表 + 风险预算表 + 约束校验表。"""

    def __init__(self, host: Any = None, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName('portfolioRiskPage')
        self._host = host
        self.theme = 'dark' if is_dark_palette() else 'light'
        self._pal = palette_for(self.theme)
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
        self.refresh_button.clicked.connect(self.load_async)
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
        """首次打开或点击重新计算时触发后台计算。"""
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
        table.resizeRowsToContents()
        apply_content_widths(table)

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
        table.resizeRowsToContents()
        apply_content_widths(table)

    def _fill_constraints(self, state: PortfolioRiskState) -> None:
        report = state.constraints
        table = self.constraint_table
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
        table.resizeRowsToContents()
        apply_content_widths(table)

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
