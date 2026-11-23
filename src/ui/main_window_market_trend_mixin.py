#!/usr/bin/env python3

"""主窗口「市场趋势」标签页 Mixin。

只负责把 ``MarketTrendPage`` 挂到中央标签页，并在首次显示时触发后台计算；
计算与渲染逻辑全部收敛在 ``src/ui/market_trend`` 包内。
"""

from typing import Any

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QFrame, QScrollArea, QVBoxLayout, QWidget


class MainWindowMarketTrendMixin:
    """中央标签页的「市场趋势」「行业热度」「风格轮动」入口。"""

    # 由宿主 MainWindow（QMainWindow + 其余 mixin）提供；仅供类型检查，不在此创建
    action_list_tab: Any
    attribution_tab: Any
    portfolio_risk_tab: Any

    #: 由 MainWindow 提供的中央标签容器
    tab_widget: Any
    market_trend_tab: QWidget
    market_trend_page: Any
    industry_heat_tab: QWidget
    industry_heat_page: Any
    style_rotation_tab: QWidget
    style_rotation_page: Any
    etf_timing_tab: QWidget
    etf_timing_page: Any

    def create_market_trend_tab(self) -> None:
        """创建市场趋势标签页（由 create_center_area 在每日推荐之后追加）。"""
        layout = QVBoxLayout(self.market_trend_tab)
        layout.setContentsMargins(0, 0, 0, 0)

        from src.ui.market_trend import MarketTrendPage
        self.market_trend_page = MarketTrendPage(host=self)

        # 指标表较长，套竖向滚动容器，避免把主窗口最小高度撑大。
        self.market_trend_scroll_area = QScrollArea()
        self.market_trend_scroll_area.setWidgetResizable(True)
        self.market_trend_scroll_area.setFrameShape(QFrame.Shape.NoFrame)
        self.market_trend_scroll_area.setHorizontalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.market_trend_scroll_area.setVerticalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAsNeeded)
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

    def create_industry_heat_tab(self) -> None:
        """创建行业热度标签页（由 create_center_area 在风格轮动之后追加）。"""
        layout = QVBoxLayout(self.industry_heat_tab)
        layout.setContentsMargins(0, 0, 0, 0)

        from src.ui.industry_heat import IndustryHeatPage
        self.industry_heat_page = IndustryHeatPage(host=self)

        self.industry_heat_scroll_area = QScrollArea()
        self.industry_heat_scroll_area.setWidgetResizable(True)
        self.industry_heat_scroll_area.setFrameShape(QFrame.Shape.NoFrame)
        self.industry_heat_scroll_area.setHorizontalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.industry_heat_scroll_area.setVerticalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAsNeeded)
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

    def create_style_rotation_tab(self) -> None:
        """创建风格轮动标签页（由 create_center_area 在市场趋势之后追加）。"""
        layout = QVBoxLayout(self.style_rotation_tab)
        layout.setContentsMargins(0, 0, 0, 0)

        from src.ui.style_rotation import StyleRotationPage
        self.style_rotation_page = StyleRotationPage(host=self)

        self.style_rotation_scroll_area = QScrollArea()
        self.style_rotation_scroll_area.setWidgetResizable(True)
        self.style_rotation_scroll_area.setFrameShape(QFrame.Shape.NoFrame)
        self.style_rotation_scroll_area.setHorizontalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.style_rotation_scroll_area.setVerticalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.style_rotation_scroll_area.setWidget(self.style_rotation_page)
        layout.addWidget(self.style_rotation_scroll_area)

        self._style_rotation_loaded = False
        self.tab_widget.currentChanged.connect(self._on_style_rotation_tab_changed)

    def _on_style_rotation_tab_changed(self, index: int):
        """首次切到风格轮动页时才计算，避免拖慢启动。"""
        if not hasattr(self, 'style_rotation_tab'):
            return
        if not hasattr(self, 'style_rotation_page'):
            return
        if self.tab_widget.widget(index) is not self.style_rotation_tab:
            return
        if getattr(self, '_style_rotation_loaded', False):
            return
        self._style_rotation_loaded = True
        self.style_rotation_page.load_async()

    def create_portfolio_risk_tab(self) -> None:
        """创建仓位与风控标签页（由 create_center_area 在行业热度之后追加）。"""
        layout = QVBoxLayout(self.portfolio_risk_tab)
        layout.setContentsMargins(0, 0, 0, 0)

        from src.ui.portfolio_risk import PortfolioRiskPage
        self.portfolio_risk_page = PortfolioRiskPage(host=self)

        self.portfolio_risk_scroll_area = QScrollArea()
        self.portfolio_risk_scroll_area.setWidgetResizable(True)
        self.portfolio_risk_scroll_area.setFrameShape(QFrame.Shape.NoFrame)
        self.portfolio_risk_scroll_area.setHorizontalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.portfolio_risk_scroll_area.setVerticalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.portfolio_risk_scroll_area.setWidget(self.portfolio_risk_page)
        layout.addWidget(self.portfolio_risk_scroll_area)

        self._portfolio_risk_loaded = False
        self.tab_widget.currentChanged.connect(self._on_portfolio_risk_tab_changed)

    def _on_portfolio_risk_tab_changed(self, index: int):
        """首次切到仓位与风控页时才计算，避免拖慢启动。"""
        if not hasattr(self, 'portfolio_risk_tab'):
            return
        if not hasattr(self, 'portfolio_risk_page'):
            return
        if self.tab_widget.widget(index) is not self.portfolio_risk_tab:
            return
        if getattr(self, '_portfolio_risk_loaded', False):
            return
        self._portfolio_risk_loaded = True
        self.portfolio_risk_page.load_async()

    def create_attribution_tab(self) -> None:
        """创建收益归因标签页（由 create_center_area 在仓位与风控之后追加）。"""
        layout = QVBoxLayout(self.attribution_tab)
        layout.setContentsMargins(0, 0, 0, 0)

        from src.ui.attribution import AttributionPage
        self.attribution_page = AttributionPage(host=self)

        self.attribution_scroll_area = QScrollArea()
        self.attribution_scroll_area.setWidgetResizable(True)
        self.attribution_scroll_area.setFrameShape(QFrame.Shape.NoFrame)
        self.attribution_scroll_area.setHorizontalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.attribution_scroll_area.setVerticalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.attribution_scroll_area.setWidget(self.attribution_page)
        layout.addWidget(self.attribution_scroll_area)

        self._attribution_loaded = False
        self.tab_widget.currentChanged.connect(self._on_attribution_tab_changed)

    def _on_attribution_tab_changed(self, index: int):
        """首次切到收益归因页时才计算，避免拖慢启动。"""
        if not hasattr(self, 'attribution_tab'):
            return
        if not hasattr(self, 'attribution_page'):
            return
        if self.tab_widget.widget(index) is not self.attribution_tab:
            return
        if getattr(self, '_attribution_loaded', False):
            return
        self._attribution_loaded = True
        self.attribution_page.load_async()

    def create_action_list_tab(self) -> None:
        """创建模型操作清单标签页（由 create_center_area 在收益归因之后追加）。"""
        layout = QVBoxLayout(self.action_list_tab)
        layout.setContentsMargins(0, 0, 0, 0)

        from src.ui.action_list import ActionListPage
        self.action_list_page = ActionListPage(host=self)

        self.action_list_scroll_area = QScrollArea()
        self.action_list_scroll_area.setWidgetResizable(True)
        self.action_list_scroll_area.setFrameShape(QFrame.Shape.NoFrame)
        self.action_list_scroll_area.setHorizontalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.action_list_scroll_area.setVerticalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.action_list_scroll_area.setWidget(self.action_list_page)
        layout.addWidget(self.action_list_scroll_area)

        self._action_list_loaded = False
        self.tab_widget.currentChanged.connect(self._on_action_list_tab_changed)

    def _on_action_list_tab_changed(self, index: int):
        """首次切到模型操作清单页时才计算，避免拖慢启动。"""
        if not hasattr(self, 'action_list_tab'):
            return
        if not hasattr(self, 'action_list_page'):
            return
        if self.tab_widget.widget(index) is not self.action_list_tab:
            return
        if getattr(self, '_action_list_loaded', False):
            return
        self._action_list_loaded = True
        self.action_list_page.load_async()

    def create_etf_timing_tab(self) -> None:
        """创建 ETF 仓位择时标签页（由 create_center_area 在模型操作清单之后追加）。"""
        layout = QVBoxLayout(self.etf_timing_tab)
        layout.setContentsMargins(0, 0, 0, 0)

        from src.ui.etf_timing import EtfTimingPage
        self.etf_timing_page = EtfTimingPage(host=self)

        self.etf_timing_scroll_area = QScrollArea()
        self.etf_timing_scroll_area.setWidgetResizable(True)
        self.etf_timing_scroll_area.setFrameShape(QFrame.Shape.NoFrame)
        self.etf_timing_scroll_area.setHorizontalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.etf_timing_scroll_area.setVerticalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.etf_timing_scroll_area.setWidget(self.etf_timing_page)
        layout.addWidget(self.etf_timing_scroll_area)

        self._etf_timing_loaded = False
        self.tab_widget.currentChanged.connect(self._on_etf_timing_tab_changed)

    def _on_etf_timing_tab_changed(self, index: int):
        """首次切到 ETF 仓位择时页时才计算，避免拖慢启动。"""
        if not hasattr(self, 'etf_timing_tab'):
            return
        if not hasattr(self, 'etf_timing_page'):
            return
        if self.tab_widget.widget(index) is not self.etf_timing_tab:
            return
        if getattr(self, '_etf_timing_loaded', False):
            return
        self._etf_timing_loaded = True
        self.etf_timing_page.load_async()
