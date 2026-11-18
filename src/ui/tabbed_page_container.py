#!/usr/bin/env python3

"""
主窗口中央页面容器：QStackedWidget 承载页面，QTabBar 独立放到工具栏行。

保留 ``tab_widget`` 这一兼容对象名，对外暴露与 QTabWidget 一致的方法
（addTab / count / widget / tabText / setCurrentWidget / currentChanged 等），
既有调用方与测试无需改动。标签栏由主窗口工具栏持有，容器只负责两者索引同步。
"""

from PySide6.QtWidgets import QStackedWidget, QTabBar, QWidget


class TabbedPageContainer(QStackedWidget):
    """页面容器 + 外部 QTabBar 的双向索引同步。"""

    def __init__(self, tab_bar: QTabBar, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._tab_bar = tab_bar
        self._tab_bar.currentChanged.connect(self._on_tab_bar_changed)
        self.currentChanged.connect(self._on_stack_changed)

    # -- 标签栏 ------------------------------------------------------------
    def tabBar(self) -> QTabBar:
        return self._tab_bar

    def _on_tab_bar_changed(self, index: int) -> None:
        if index != self.currentIndex():
            self.setCurrentIndex(index)

    def _on_stack_changed(self, index: int) -> None:
        if index != self._tab_bar.currentIndex():
            self._tab_bar.setCurrentIndex(index)

    # -- QTabWidget 兼容方法 ----------------------------------------------
    def addTab(self, widget: QWidget, label: str) -> int:
        index = self.addWidget(widget)
        self._tab_bar.addTab(label)
        return index

    def tabText(self, index: int) -> str:
        return self._tab_bar.tabText(index)
