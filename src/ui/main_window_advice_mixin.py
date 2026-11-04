#!/usr/bin/env python3

"""主窗口「每日推荐」标签页 Mixin。

只负责把 ``AdvicePage`` 挂到中央标签页，并做首次显示时的懒加载；
页面内部的数据/线程/渲染逻辑全部收敛在 ``src/ui/advice`` 包内。
"""

from typing import Any

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QFrame, QScrollArea, QVBoxLayout, QWidget


class MainWindowAdviceMixin:
    """中央标签页的「每日推荐」入口。"""

    #: 由 MainWindow 提供的中央标签容器
    tab_widget: Any
    advice_tab: QWidget
    advice_page: Any

    def create_advice_tab(self):
        """创建每日推荐标签页（由 create_center_area 在末尾追加）。"""
        layout = QVBoxLayout(self.advice_tab)
        layout.setContentsMargins(0, 0, 0, 0)

        from src.ui.advice import AdvicePage
        self.advice_page = AdvicePage(host=self)

        # 整页套竖向滚动容器：该页自身最小高度约 795px，不套滚动会把整个主窗口
        # 的最小高度撑到 929px，导致技术分析页在 1024×600 下被窗口底边裁掉。
        # 页面内部布局不变，窗口变矮时由该页自身滚动。
        self.advice_scroll_area = QScrollArea()
        self.advice_scroll_area.setWidgetResizable(True)
        self.advice_scroll_area.setFrameShape(QFrame.NoFrame)
        self.advice_scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.advice_scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.advice_scroll_area.setWidget(self.advice_page)
        layout.addWidget(self.advice_scroll_area)

        self._advice_loaded = False
        self.tab_widget.currentChanged.connect(self._on_center_tab_changed)

    def _on_center_tab_changed(self, index: int):
        """首次切到每日推荐页时才触发后台加载，避免拖慢启动。"""
        if not hasattr(self, 'advice_tab') or not hasattr(self, 'advice_page'):
            return
        if self.tab_widget.widget(index) is not self.advice_tab:
            return
        if getattr(self, '_advice_loaded', False):
            return
        self._advice_loaded = True
        self.advice_page.load_async()
