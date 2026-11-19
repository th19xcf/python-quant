#!/usr/bin/env python3

"""
主题管理模块：调色板切换 + 左侧导航栏（QTreeWidget）的配色与条目绘制。
"""

from typing import cast

from PySide6.QtCore import QRectF, QSize, Qt
from PySide6.QtGui import QColor, QFont, QPainter, QPalette
from PySide6.QtWidgets import (
    QApplication,
    QStyle,
    QStyledItemDelegate,
    QStyleOptionViewItem,
)

# 左侧导航栏两套主题的取色表：两端同时维护，避免只改深色导致浅色不一致
_SIDEBAR_COLORS = {
    "dark": {
        "panel_bg": QColor("#1A1C20"),
        "border": QColor("#2A2D33"),
        "text_primary": QColor("#E8EAED"),
        "text_secondary": QColor("#9BA1A8"),
        "hover_bg": QColor(255, 255, 255, 14),
        "selected_bg": QColor(0, 191, 255, 34),
        "tab_selected_bg": QColor(0, 191, 255, 60),
        "accent": QColor("#00BFFF"),
        "accent_hover": QColor(0, 191, 255, 110),
        "scroll_handle": QColor(255, 255, 255, 42),
        "scroll_handle_hover": QColor(255, 255, 255, 78),
    },
    "light": {
        "panel_bg": QColor("#F5F6F8"),
        "border": QColor("#E1E4E8"),
        "text_primary": QColor("#1F2329"),
        "text_secondary": QColor("#6B7280"),
        "hover_bg": QColor(0, 0, 0, 12),
        "selected_bg": QColor(0, 102, 204, 30),
        "tab_selected_bg": QColor(0, 102, 204, 48),
        "accent": QColor("#0066CC"),
        "accent_hover": QColor(0, 102, 204, 90),
        "scroll_handle": QColor(0, 0, 0, 38),
        "scroll_handle_hover": QColor(0, 0, 0, 66),
    },
}


def _rgba(color: QColor) -> str:
    """把 QColor 转成 QSS 的 rgba() 字符串，保留 alpha。"""
    return f"rgba({color.red()}, {color.green()}, {color.blue()}, {color.alpha()})"


def _colors(dark: bool) -> dict:
    return _SIDEBAR_COLORS["dark" if dark else "light"]


def is_dark_palette(app: QApplication | None = None) -> bool:
    """按当前应用调色板的窗口亮度判断深浅主题，供启动期挂样式使用。"""
    application = app if app is not None else cast(QApplication, QApplication.instance())
    if application is None:
        return True
    return application.palette().color(QPalette.ColorRole.Window).lightness() < 128


def sidebar_qss(dark: bool = True) -> str:
    """左侧导航面板样式：底色与无边框树，深浅两套同构。

    滚动条不在这里定义：统一由 :func:`scrollbar_qss` 提升到应用作用域，
    保证导航栏与其它页面用的是同一份样式。
    """
    c = _colors(dark)
    return f"""
    QWidget#navPanel {{
        background-color: {c['panel_bg'].name()};
        border-right: 1px solid {c['border'].name()};
    }}
    QTreeView#navTree {{
        background-color: transparent;
        border: none;
        outline: 0;
        padding: 2px 0px;
        show-decoration-selected: 0;
        font-family: "Microsoft YaHei";
    }}
    QTreeView#navTree::item {{
        border: none;
    }}
    """


def scrollbar_qss(dark: bool = True) -> str:
    """全局滚动条样式（唯一一处定义）：8px 细条 + 圆角 handle + 无页脚按钮。

    原样式只写在左侧导航面板上，其它页面各自回落 Fusion 默认外观；
    这里提升到 QApplication 作用域，竖向/横向、handle 悬停与 add/sub-line
    一并覆盖，深浅两套同构，仅换 handle 的 rgba。
    """
    c = _colors(dark)
    handle = _rgba(c['scroll_handle'])
    handle_hover = _rgba(c['scroll_handle_hover'])
    return f"""
    QScrollBar:vertical {{
        background: transparent;
        width: 8px;
        margin: 2px 0px 2px 0px;
        border: none;
    }}
    QScrollBar::handle:vertical {{
        background-color: {handle};
        min-height: 28px;
        border: none;
        border-radius: 4px;
    }}
    QScrollBar::handle:vertical:hover {{
        background-color: {handle_hover};
    }}
    QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
        height: 0px;
        width: 0px;
        background: none;
        border: none;
    }}
    QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {{
        background: none;
    }}
    QScrollBar:horizontal {{
        background: transparent;
        height: 8px;
        margin: 0px 2px 0px 2px;
        border: none;
    }}
    QScrollBar::handle:horizontal {{
        background-color: {handle};
        min-width: 28px;
        border: none;
        border-radius: 4px;
    }}
    QScrollBar::handle:horizontal:hover {{
        background-color: {handle_hover};
    }}
    QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{
        height: 0px;
        width: 0px;
        background: none;
        border: none;
    }}
    QScrollBar::add-page:horizontal, QScrollBar::sub-page:horizontal {{
        background: none;
    }}
    """


def tabbar_qss(dark: bool = True) -> str:
    """顶部标签栏选中态（唯一一处定义）：底色、文字色、上圆角拉开选中/未选中差距。

    只改颜色与圆角，不设 padding / margin / border / font-weight：
    这些属性会参与 QTabBar 的 sizeHint 计算，导致选中时标签宽度或行高跳变、
    甚至把标签文字挤到滚动按钮里。选中态与内容区之间不加下边框，
    用强调色底块收口，避免出现悬空感。
    """
    c = _colors(dark)
    return f"""
    QTabBar#mainTabBar {{
        background: transparent;
    }}
    QTabBar#mainTabBar::tab {{
        background: transparent;
        color: {c['text_secondary'].name()};
        border-top-left-radius: 4px;
        border-top-right-radius: 4px;
    }}
    QTabBar#mainTabBar::tab:hover {{
        background-color: {_rgba(c['hover_bg'])};
        color: {c['text_primary'].name()};
    }}
    QTabBar#mainTabBar::tab:selected {{
        background-color: {_rgba(c['tab_selected_bg'])};
        color: {c['accent'].name()};
    }}
    """


def app_qss(dark: bool = True) -> str:
    """应用级样式总入口：滚动条 + 顶部标签栏，供 set_*_theme 一次挂载。"""
    return scrollbar_qss(dark) + tabbar_qss(dark)


class SidebarItemDelegate(QStyledItemDelegate):
    """导航树条目绘制：圆角高亮块 + 左侧强调条 + 两级文字层级。

    仅接管绘制与行高，不改动条目文本、展开状态与点击命中逻辑。
    """

    GROUP_HEIGHT = 33
    ITEM_HEIGHT = 27
    TEXT_PADDING = 10

    def __init__(self, parent=None, dark: bool = True):
        super().__init__(parent)
        self._colors = _colors(dark)

    def sizeHint(self, option, index):
        base = super().sizeHint(option, index)
        is_group = not index.parent().isValid()
        height = self.GROUP_HEIGHT if is_group else self.ITEM_HEIGHT
        return QSize(base.width(), height)

    def paint(self, painter, option, index):
        painter.save()
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)

        opt = QStyleOptionViewItem(option)
        # 状态取自视口给的原始 option：initStyleOption 会重算 state，可能抹掉 MouseOver
        selected = bool(option.state & QStyle.StateFlag.State_Selected)
        hovered = bool(option.state & QStyle.StateFlag.State_MouseOver)
        is_group = not index.parent().isValid()

        self.initStyleOption(opt, index)
        # 背景由本委托负责，屏蔽原生的选中/悬停/焦点绘制，避免两层底色叠加
        opt.state &= ~(
            QStyle.StateFlag.State_Selected
            | QStyle.StateFlag.State_MouseOver
            | QStyle.StateFlag.State_HasFocus
        )

        if selected or hovered:
            block = QRectF(opt.rect).adjusted(4.0, 2.0, -6.0, -2.0)
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(self._colors["selected_bg"] if selected else self._colors["hover_bg"])
            painter.drawRoundedRect(block, 8.0, 8.0)

            # 左侧强调条：选中用主题强调色，悬停用半透明同色
            bar = QRectF(block.left(), block.top() + 5.0, 3.0, block.height() - 10.0)
            painter.setBrush(self._colors["accent"] if selected else self._colors["accent_hover"])
            painter.drawRoundedRect(bar, 1.5, 1.5)

        font = QFont(opt.font)
        if is_group:
            font.setBold(True)
            font.setPixelSize(13)
        else:
            font.setPixelSize(12)
        opt.font = font

        text_color = self._colors["text_primary"] if is_group else self._colors["text_secondary"]
        opt.palette.setColor(QPalette.ColorRole.Text, text_color)
        opt.palette.setColor(QPalette.ColorRole.WindowText, text_color)

        # 文字与图标统一左内边距，垂直方向由样式居中，保证基线一致
        opt.rect = opt.rect.adjusted(self.TEXT_PADDING, 0, -6, 0)

        style = option.widget.style() if option.widget is not None else QApplication.style()
        style.drawControl(QStyle.ControlElement.CE_ItemViewItem, opt, painter, option.widget)
        painter.restore()


class ThemeManager:
    """
    主题管理器，负责管理应用的主题切换
    """

    @staticmethod
    def set_light_theme(app: QApplication):
        """
        设置浅色主题

        Args:
            app: QApplication实例
        """
        palette = QPalette()

        # 背景色
        palette.setColor(QPalette.ColorRole.Window, QColor(240, 240, 240))
        palette.setColor(QPalette.ColorRole.WindowText, QColor(0, 0, 0))
        palette.setColor(QPalette.ColorRole.Base, QColor(255, 255, 255))
        palette.setColor(QPalette.ColorRole.AlternateBase, QColor(245, 245, 245))

        # 文本颜色
        palette.setColor(QPalette.ColorRole.Text, QColor(0, 0, 0))
        palette.setColor(QPalette.ColorRole.ButtonText, QColor(0, 0, 0))
        palette.setColor(QPalette.ColorRole.BrightText, QColor(255, 0, 0))

        # 按钮颜色
        palette.setColor(QPalette.ColorRole.Button, QColor(240, 240, 240))
        palette.setColor(QPalette.ColorRole.Highlight, QColor(65, 105, 225))
        palette.setColor(QPalette.ColorRole.HighlightedText, QColor(255, 255, 255))

        # 禁用状态
        palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.Text, QColor(160, 160, 160))
        palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.ButtonText, QColor(160, 160, 160))

        app.setPalette(palette)
        app.setStyle("Fusion")
        # 全局样式：滚动条 + 顶部标签栏，深浅主题各一份
        app.setStyleSheet(app_qss(False))

    @staticmethod
    def set_dark_theme(app: QApplication):
        """
        设置深色主题

        Args:
            app: QApplication实例
        """
        palette = QPalette()

        # 背景色
        palette.setColor(QPalette.ColorRole.Window, QColor(53, 53, 53))
        palette.setColor(QPalette.ColorRole.WindowText, QColor(255, 255, 255))
        palette.setColor(QPalette.ColorRole.Base, QColor(25, 25, 25))
        palette.setColor(QPalette.ColorRole.AlternateBase, QColor(53, 53, 53))

        # 文本颜色
        palette.setColor(QPalette.ColorRole.Text, QColor(255, 255, 255))
        palette.setColor(QPalette.ColorRole.ButtonText, QColor(255, 255, 255))
        palette.setColor(QPalette.ColorRole.BrightText, QColor(255, 0, 0))

        # 按钮颜色
        palette.setColor(QPalette.ColorRole.Button, QColor(53, 53, 53))
        palette.setColor(QPalette.ColorRole.Highlight, QColor(65, 105, 225))
        palette.setColor(QPalette.ColorRole.HighlightedText, QColor(255, 255, 255))

        # 禁用状态
        palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.Text, QColor(127, 127, 127))
        palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.ButtonText, QColor(127, 127, 127))

        app.setPalette(palette)
        app.setStyle("Fusion")
        # 全局样式：滚动条 + 顶部标签栏，深浅主题各一份
        app.setStyleSheet(app_qss(True))

    @staticmethod
    def toggle_theme(app: QApplication, is_dark: bool):
        """
        切换主题

        Args:
            app: QApplication实例
            is_dark: 是否切换到深色主题
        """
        if is_dark:
            ThemeManager.set_dark_theme(app)
        else:
            ThemeManager.set_light_theme(app)
