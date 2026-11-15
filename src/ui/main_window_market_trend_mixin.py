#!/usr/bin/env python3

"""主窗口「市场趋势」标签页 Mixin。

只负责把 ``MarketTrendPage`` 挂到中央标签页，并在首次显示时触发后台计算；
计算与渲染逻辑全部收敛在 ``src/ui/market_trend`` 包内。
"""

from typing import Any

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QFrame, QScrollArea, QVBoxLayout, QWidget


class MainWindowMarketTrendMixin:
    """中央标签页的「市场趋势」「行业热度」入口。"""

    #: 由 MainWindow 提供的中央标签容器
    tab_widget: Any
    market_trend_tab: QWidget
    market_trend_page: Any
    industry_heat_tab: QWidget
    industry_heat_page: Any

    def create_market_trend_tab(self):
        """创建市场趋势标签页（由 create_center_area 在每日推荐之后追加）。"""
        layout = QVBoxLayout(self.market_trend_tab)
        layout.setContentsMargins(0, 0, 0, 0)

        from src.ui.market_trend import MarketTrendPage
        self.market_trend_page = MarketTrendPage(host=self)

        # 指标表较长，套竖向滚动容器，避免把主窗口最小高度撑大。
        self.market_trend_scroll_area = QScrollArea()
        self.market_trend_scroll_area.setWidgetResizable(True)
        self.market_trend_scroll_area.setFrameShape(QFrame.NoFrame)
        self.market_trend_scroll_area.setHorizontalScrollBarPolicy(
            Qt.ScrollBarAsNeeded)
        self.market_trend_scroll_area.setVerticalScrollBarPolicy(
            Qt.ScrollBarAsNeeded)
        self.market_trend_scroll_area.setWidget(self.market_trend_page)
        layout.addWidget(self.market_trend_scroll_area)

        self._market_trend_loaded = False
        self.tab_widget.currentChanged.connect(self._on_market_trend_tab_changed)

    def _on_market_trend_tab_changed(self, index: int):
        """首次切到市场趋势页时才计算，避免拖慢启动。"""
        if not hasattr(self, 'market_trend_tab'):
            return
        if not hasattr(self, 'market_trend_page'):
            return
        if self.tab_widget.widget(index) is not self.market_trend_tab:
            return
        if getattr(self, '_market_trend_loaded', False):
            return
        self._market_trend_loaded = True
        self.market_trend_page.load_async()

    def create_industry_heat_tab(self):
        """创建行业热度标签页（由 create_center_area 在市场趋势之后追加）。"""
        layout = QVBoxLayout(self.industry_heat_tab)
        layout.setContentsMargins(0, 0, 0, 0)

        from src.ui.industry_heat import IndustryHeatPage
        self.industry_heat_page = IndustryHeatPage(host=self)

        self.industry_heat_scroll_area = QScrollArea()
        self.industry_heat_scroll_area.setWidgetResizable(True)
        self.industry_heat_scroll_area.setFrameShape(QFrame.NoFrame)
        self.industry_heat_scroll_area.setHorizontalScrollBarPolicy(
            Qt.ScrollBarAsNeeded)
        self.industry_heat_scroll_area.setVerticalScrollBarPolicy(
            Qt.ScrollBarAsNeeded)
        self.industry_heat_scroll_area.setWidget(self.industry_heat_page)
        layout.addWidget(self.industry_heat_scroll_area)

        self._industry_heat_loaded = False
        self.tab_widget.currentChanged.connect(self._on_industry_heat_tab_changed)

    def _on_industry_heat_tab_changed(self, index: int):
        """首次切到行业热度页时才计算，避免拖慢启动。"""
        if not hasattr(self, 'industry_heat_tab'):
            return
        if not hasattr(self, 'industry_heat_page'):
            return
        if self.tab_widget.widget(index) is not self.industry_heat_tab:
            return
        if getattr(self, '_industry_heat_loaded', False):
            return
        self._industry_heat_loaded = True
        self.industry_heat_page.load_async()
