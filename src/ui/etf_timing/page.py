"""主界面「ETF 仓位择时」标签页：目标仓位、ETF 权重与金额、下一档阈值。"""

from __future__ import annotations

from typing import Any

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
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

from src.ui.advice.tokens import TOKENS, palette_for
from src.ui.table_sizing import (
    TABLE_FONT_SIZE_PX,
    SizedTableMixin,
)
from src.ui.theme_manager import is_dark_palette


def _fetch_state(token: int, asof: str | None = None,
                 capital: float = 1_000_000.0, refresh: bool = False) -> dict[str, Any]:
    """后台任务：只读本地数据计算仓位档位与 ETF 分配。"""
    from datetime import datetime

    from src.quant.analysis.etf_timing_local import compute_state

    target = datetime.strptime(asof, '%Y-%m-%d').date() if asof else None
    state = compute_state(asof=target, capital=capital, refresh=refresh)
    return {'token': token, 'state': state}


def _qss(theme: str) -> str:
    palette = palette_for(theme)
    tokens = TOKENS
    return f"""
QWidget#etfTimingPage {{
    background: {palette.bg};
    color: {palette.text};
    font-family: {tokens.font_family};
    font-size: {TABLE_FONT_SIZE_PX}px;
}}
QLabel#etfTitle {{
    font-size: {tokens.font_title}px;
    font-weight: 700;
    color: {palette.text};
}}
QLabel#etfSubtitle, QLabel#etfStatus, QLabel#etfNotes {{
    font-size: {tokens.font_small}px;
    color: {palette.muted};
}}
QLabel#etfSectionTitle {{
    font-size: {tokens.font_section}px;
    font-weight: 700;
    color: {palette.text};
}}
QFrame#etfCard {{
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


class EtfTimingPage(QWidget, SizedTableMixin):
    """ETF 仓位择时页：结论卡 + ETF 明细表 + 阈值卡。"""

    def __init__(self, host: Any = None, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName('etfTimingPage')
        self._host = host
        self.theme = 'dark' if is_dark_palette() else 'light'
        self._pal = palette_for(self.theme)
        self._row_orders: dict[QTableWidget, list[tuple[str, ...]]] = {}
        self._sort_clicks: dict[QTableWidget, dict[int, int]] = {}
        self._token = 0
        self._busy = False
        self._task_id: str | None = None
        self._state: Any = None
        self._capital = 1_000_000.0
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
        title = QLabel('ETF 仓位择时')
        title.setObjectName('etfTitle')
        subtitle = QLabel('市场趋势总分定仓位档位，风格三维定权益内部槽位；T 日收盘出信号，T+1 开盘执行')
        subtitle.setObjectName('etfSubtitle')
        title_box.addWidget(title)
        title_box.addWidget(subtitle)
        header.addLayout(title_box)
        header.addStretch(1)
        self.status_label = QLabel('尚未计算')
        self.status_label.setObjectName('etfStatus')
        header.addWidget(self.status_label)
        self.refresh_button = QPushButton('重新计算')
        self.refresh_button.setCursor(Qt.CursorShape.PointingHandCursor)
        self.refresh_button.clicked.connect(lambda: self.load_async(refresh=True))
        header.addWidget(self.refresh_button)
        root.addLayout(header)

        root.addWidget(self._build_conclusion())
        root.addWidget(self._build_holdings_card())
        root.addWidget(self._build_threshold_card())

        self.notes_label = QLabel('')
        self.notes_label.setObjectName('etfNotes')
        self.notes_label.setWordWrap(True)
        root.addWidget(self.notes_label)
        root.addStretch(1)

    def _build_conclusion(self) -> QFrame:
        card = QFrame()
        card.setObjectName('etfCard')
        layout = QHBoxLayout(card)
        layout.setContentsMargins(18, 14, 18, 14)
        layout.setSpacing(28)

        position_box = QVBoxLayout()
        position_box.setSpacing(2)
        caption = QLabel('目标仓位')
        caption.setObjectName('etfSubtitle')
        self.position_label = QLabel('—')
        font = self.position_label.font()
        font.setPointSize(24)
        font.setBold(True)
        self.position_label.setFont(font)
        position_box.addWidget(caption)
        position_box.addWidget(self.position_label)
        layout.addLayout(position_box)

        detail_box = QVBoxLayout()
        detail_box.setSpacing(2)
        self.level_label = QLabel('—')
        self.level_label.setObjectName('etfSectionTitle')
        self.score_label = QLabel('总分 —')
        self.score_label.setObjectName('etfSubtitle')
        self.asof_label = QLabel('数据截至 —')
        self.asof_label.setObjectName('etfSubtitle')
        self.execution_label = QLabel('执行日 —')
        self.execution_label.setObjectName('etfSubtitle')
        detail_box.addWidget(self.level_label)
        detail_box.addWidget(self.score_label)
        detail_box.addWidget(self.asof_label)
        detail_box.addWidget(self.execution_label)
        layout.addLayout(detail_box)

        hint = QLabel(
            '档位：≥+0.60 满仓 / ≥+0.20 七成半 / ±0.20 半仓 / >-0.60 两成半 / ≤-0.60 空仓；'
            '滞回 0.20，单日最多调一档。'
        )
        hint.setObjectName('etfSubtitle')
        hint.setWordWrap(True)
        hint.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        layout.addWidget(hint, 1)
        return card

    def _build_holdings_card(self) -> QFrame:
        card = QFrame()
        card.setObjectName('etfCard')
        layout = QVBoxLayout(card)
        layout.setContentsMargins(14, 12, 14, 12)
        layout.setSpacing(8)
        section = QLabel('ETF 分配与金额')
        section.setObjectName('etfSectionTitle')
        layout.addWidget(section)
        self.holdings_table = self._make_table(
            ['代码', '名称', '角色', '权益内权重', '占本金', '金额'])
        self.holdings_table.setColumnWidth(0, 96)
        self.holdings_table.setColumnWidth(1, 130)
        self.holdings_table.setColumnWidth(2, 110)
        self.holdings_table.setColumnWidth(3, 100)
        self.holdings_table.setColumnWidth(4, 90)
        layout.addWidget(self.holdings_table)
        return card

    def _build_threshold_card(self) -> QFrame:
        card = QFrame()
        card.setObjectName('etfCard')
        layout = QVBoxLayout(card)
        layout.setContentsMargins(14, 12, 14, 12)
        layout.setSpacing(6)
        section = QLabel('下一档与风格依据')
        section.setObjectName('etfSectionTitle')
        layout.addWidget(section)
        self.next_level_label = QLabel('—')
        self.style_label = QLabel('—')
        for label in (self.next_level_label, self.style_label):
            label.setWordWrap(True)
            layout.addWidget(label)
        return card

    def _make_table(self, headers: list[str]) -> QTableWidget:
        table = QTableWidget(0, len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self._configure_sized_table(table)
        return table

    # -------------------------------------------------------------- loading
    def _connect_task_signals(self) -> None:
        from src.ui.task_manager import global_task_manager

        self._task_manager = global_task_manager
        global_task_manager.task_completed.connect(self._on_task_completed)
        global_task_manager.task_error.connect(self._on_task_error)

    def load_async(self, refresh: bool = False) -> None:
        if self._busy:
            return
        self._busy = True
        self._token += 1
        self.refresh_button.setEnabled(False)
        self.refresh_button.setText('计算中…')
        self.status_label.setText('正在读取本地数据并计算…')
        self._task_id = self._task_manager.create_task(
            f'ETF仓位择时#{self._token}',
            _fetch_state,
            kwargs={'token': self._token, 'capital': self._capital, 'refresh': refresh},
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
    def render_state(self, state: Any) -> None:
        """把状态灌入页面（截图与测试也走同一入口）。"""
        self._state = state
        self.position_label.setText(f'{state.target_position:.0%}')
        self.position_label.setStyleSheet(f'color: {self._level_color(state.level)};')
        self.level_label.setText(f'档位：{state.label}')
        self.score_label.setText(f'趋势总分 {state.score:+.3f}')
        self.asof_label.setText(f'信号日 {state.asof} 收盘')
        self.execution_label.setText(
            f'执行日 {state.execution_date} 开盘' if state.execution_date else '执行日：下一交易日待定')
        self._fill_holdings(state)
        self.next_level_label.setText(self._next_level_text(state))
        self.style_label.setText(f'风格依据：{state.style_text}')
        self.notes_label.setText('\n'.join(state.notes))
        self.status_label.setText('判定完成')

    def _fill_holdings(self, state: Any) -> None:
        table = self.holdings_table
        self._prepare_table(table)
        rows = list(state.holdings)
        table.setRowCount(len(rows) + 1)
        for row, holding in enumerate(rows):
            values = [
                holding.code,
                holding.name,
                holding.role,
                f'{holding.sleeve_weight:.0%}',
                f'{holding.position_weight:.2%}',
                f'{holding.amount:,.0f}',
            ]
            alignments = (
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
            )
            for column, text in enumerate(values):
                item = self._item(text, alignments[column])
                if column == 5:
                    item.setForeground(self._brush(self._pal.buy))
                table.setItem(row, column, item)
        cash_row = len(rows)
        cash_values = [
            'CASH', '现金', '—', '—',
            f'{state.cash_weight:.2%}',
            f'{state.cash_weight * self._capital:,.0f}',
        ]
        for column, text in enumerate(cash_values):
            item = self._item(text, Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            if column == 5:
                item.setForeground(self._brush(self._pal.muted))
            table.setItem(cash_row, column, item)
        self._finalize_table(table)

    @staticmethod
    def _next_level_text(state: Any) -> str:
        if state.up_threshold is None:
            up = '加仓：已满仓'
        else:
            up = f'加仓一档需总分 ≥ {state.up_threshold:+.2f}'
        if state.down_threshold is None:
            down = '减仓：已空仓'
        else:
            down = f'减仓一档需总分 ≤ {state.down_threshold:+.2f}'
        return f'{up}；{down}'

    def _level_color(self, level: int) -> str:
        if level >= 3:
            return self._pal.buy
        if level <= 1:
            return self._pal.sell
        return self._pal.hold

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
