"""副图区域右键菜单：副图指标子菜单（指标用法注释 / 调整指标参数）。

菜单层级照通达信截图，本轮仅实现「指标用法注释」「调整指标参数」两项，
其余项保持灰色占位，避免误触。
"""

from __future__ import annotations

from typing import Any

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QLabel,
    QLineEdit,
    QMenu,
    QMessageBox,
    QPushButton,
    QTextBrowser,
    QVBoxLayout,
    QWidget,
)

from config.indicator_notes import get_indicator_note
from src.ui.indicator_parameters import (
    PARAM_SPECS,
    apply_overrides,
    effective_params,
    parse_param_text,
    reset_overrides,
)

#: 副图指标子菜单的固定结构（text, 本轮是否可点击）
SUB_MENU_STRUCTURE: tuple[tuple[str, bool], ...] = (
    ('选择副图指标', False),
    ('临时叠加其它指标', False),
    ('指标用法注释', True),
    ('调整指标参数', True),
    ('修改当前指标公式', False),
    ('分享当前公式', False),
    ('分享到指标平台', False),
    ('删除当前指标', False),
)


def menu_entries(indicator: str | None) -> list[tuple[str, bool]]:
    """返回子菜单项及其启用状态；无副图指标时整组不可用。"""
    if not indicator:
        return [(text, False) for text, _ in SUB_MENU_STRUCTURE]
    return [(text, enabled) for text, enabled in SUB_MENU_STRUCTURE]


def build_subchart_menu(window: Any, window_num: int) -> QMenu | None:
    """构造副图右键菜单；仅窗口 2/3 返回菜单，主图（窗口 1）返回 None。"""
    if window_num not in (2, 3):
        return None

    indicator = (getattr(window, 'window_indicators', None) or {}).get(window_num)
    parent = window if isinstance(window, QWidget) else None
    menu = QMenu(parent)
    sub_menu = menu.addMenu('副图指标')
    sub_menu.setEnabled(bool(indicator))

    for text, enabled in menu_entries(indicator):
        action = sub_menu.addAction(text)
        action.setEnabled(enabled and bool(indicator))
        if not indicator or not enabled:
            continue
        if text == '指标用法注释':
            action.triggered.connect(
                lambda _checked=False, w=window, name=indicator:
                open_indicator_notes(w, name))
        elif text == '调整指标参数':
            action.triggered.connect(
                lambda _checked=False, w=window, name=indicator:
                open_indicator_params(w, name))

    return menu


def _note_html(name: str, note: dict[str, str]) -> str:
    sections = (
        ('含义', note.get('meaning', '')),
        ('参数含义', note.get('params', '')),
        ('常用判读', note.get('reading', '')),
        ('注意事项', note.get('caveat', '')),
    )
    body = ''.join(
        f"<h3 style='margin:10px 0 4px 0;'>{title}</h3>"
        f"<p style='margin:0 0 6px 0;'>{content}</p>"
        for title, content in sections
    )
    return (f"<h2 style='margin:0 0 8px 0;'>{name}</h2>{body}"
            "<p style='color:#808080;margin-top:12px;'>"
            "说明：以上内容按本项目公式口径整理，非通达信原文。</p>")


class IndicatorNotesDialog(QDialog):
    """只读的指标用法注释窗口。"""

    def __init__(self, indicator: str, parent: QWidget | None = None):
        super().__init__(parent)
        self.setWindowTitle(f'指标用法注释 - {indicator}')
        self.resize(460, 420)
        layout = QVBoxLayout(self)

        browser = QTextBrowser(self)
        browser.setReadOnly(True)
        note = get_indicator_note(indicator)
        if note:
            browser.setHtml(_note_html(indicator.upper(), note))
        else:
            browser.setPlainText(f'暂无 {indicator} 的用法注释。')
        layout.addWidget(browser)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close, parent=self)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)


class IndicatorParamsDialog(QDialog):
    """指标参数编辑窗口：数值型输入 + 范围校验 + 恢复默认。"""

    def __init__(self, indicator: str, parent: QWidget | None = None):
        super().__init__(parent)
        self.indicator = indicator.upper()
        self.setWindowTitle(f'调整指标参数 - {self.indicator}')
        self.resize(320, 220)
        layout = QVBoxLayout(self)
        self._edits: dict[str, QLineEdit] = {}

        specs = PARAM_SPECS.get(self.indicator, [])
        if not specs:
            layout.addWidget(QLabel('该指标无可调参数', self))
            buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close, parent=self)
            buttons.rejected.connect(self.reject)
            layout.addWidget(buttons)
            return

        form = QFormLayout()
        current = effective_params(self.indicator)
        for key, label, default, _low, _high in specs:
            edit = QLineEdit(str(current.get(key, default)), self)
            edit.setToolTip('支持整数/小数，空格与全角数字会自动转换')
            self._edits[key] = edit
            form.addRow(f'{label}：', edit)
        layout.addLayout(form)

        restore = QPushButton('恢复默认', self)
        restore.clicked.connect(self._restore_defaults)
        layout.addWidget(restore)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel,
            parent=self)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _restore_defaults(self) -> None:
        for key, _label, default, _low, _high in PARAM_SPECS.get(self.indicator, []):
            edit = self._edits.get(key)
            if edit is not None:
                edit.setText(str(default))

    def values(self) -> dict[str, float]:
        """解析输入框内容；非法输入抛出带可读消息的 ValueError。"""
        return {key: parse_param_text(self.indicator, key, edit.text())
                for key, edit in self._edits.items()}

    def accept(self) -> None:  # noqa: D401 - Qt 回调
        try:
            values = self.values()
        except ValueError as exc:
            QMessageBox.warning(self, '参数无效', str(exc))
            return
        try:
            apply_overrides(self.indicator, values)
        except ValueError as exc:
            QMessageBox.warning(self, '参数无效', str(exc))
            return
        super().accept()


def open_indicator_notes(window: Any, indicator: str) -> None:
    IndicatorNotesDialog(indicator, window).exec()


def open_indicator_params(window: Any, indicator: str) -> None:
    dialog = IndicatorParamsDialog(indicator, window)
    if dialog.exec() != QDialog.DialogCode.Accepted:
        return
    # 参数已保存：丢弃旧列并按新参数重算重绘（未改动的指标仍走默认值）
    plot = getattr(window, 'plot_k_line', None)
    data = getattr(window, 'current_stock_data', None)
    if callable(plot) and data is not None:
        plot(data, getattr(window, 'current_stock_name', ''), getattr(window, 'current_stock_code', ''))


def show_subchart_context_menu(window: Any, window_num: int, widget: QWidget, pos: Any) -> None:
    """在副图控件上弹出菜单（调用方负责只在窗口 2/3 接线）。"""
    menu = build_subchart_menu(window, window_num)
    if menu is None:
        return
    menu.exec(widget.mapToGlobal(pos))


def restore_indicator_defaults(window: Any, indicator: str) -> None:
    """供外部恢复默认参数并重绘。"""
    reset_overrides(indicator)
    plot = getattr(window, 'plot_k_line', None)
    data = getattr(window, 'current_stock_data', None)
    if callable(plot) and data is not None:
        plot(data, getattr(window, 'current_stock_name', ''), getattr(window, 'current_stock_code', ''))
