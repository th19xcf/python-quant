#!/usr/bin/env python3

"""
主窗口整体布局 Mixin：UI 初始化编排、左右分割布局、行情页、财务页与状态栏。
"""

from typing import Any, cast
import contextlib

from PySide6.QtCore import QSize, Qt
from PySide6.QtGui import QAction, QActionGroup
from PySide6.QtWidgets import (
    QAbstractItemView,
    QFrame,
    QHeaderView,
    QLabel,
    QMenu,
    QProgressBar,
    QScrollArea,
    QSizePolicy,
    QSplitter,
    QStatusBar,
    QTableWidget,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from src.ui.advice.tokens import TOKENS, palette_for
from src.ui.tabbed_page_container import TabbedPageContainer
from src.ui.table_sizing import FIXED_ROW_HEIGHT, SEQ_COLUMN_WIDTH
from src.ui.theme_manager import SidebarItemDelegate, is_dark_palette, sidebar_qss
from src.utils.logger import logger


class MainWindowLayoutMixin:
    """主窗口骨架布局：导航栏、中央标签页、右侧信息栏与状态栏。"""

    # 由宿主 MainWindow（QMainWindow + 其余 mixin）提供；仅供类型检查，不在此创建
    create_indicator_menu: Any
    create_menu_bar: Any
    create_tool_bar: Any
    indicator_interaction_manager: Any
    setCentralWidget: Any
    setStatusBar: Any
    table_interaction_manager: Any

    def init_ui(self):
        """
        初始化UI组件（委托给UI管理器）
        """
        # 如果有ui_manager，则使用它，否则直接调用impl
        if hasattr(self, 'ui_manager'):
            return self.ui_manager.init_ui()
        return self._init_ui_impl()

    def _init_ui_impl(self) -> None:
        """
        初始化UI组件
        """
        # 初始化缺失的属性
        self.current_stock_data = None

        # 初始化窗口菜单
        self.current_window_count = 3
        host = cast(QWidget, self)  # 运行时宿主是 QMainWindow；mixin 自身不是 QObject
        self.window_menu = QMenu(host)
        self.window_actions = []

        # 创建QActionGroup，确保单选效果
        self.window_action_group = QActionGroup(host)
        self.window_action_group.setExclusive(True)

        for i in range(1, 10):  # 最大选择9个窗口
            action = QAction(f'{i}个窗口', host)
            action.setCheckable(True)
            action.setActionGroup(self.window_action_group)
            if i == self.current_window_count:  # 使用当前窗口数量作为默认选择
                action.setChecked(True)
            action.triggered.connect(lambda checked, w=i: self.indicator_interaction_manager.on_window_count_changed(w, checked))
            self.window_menu.addAction(action)
            self.window_actions.append(action)

        self.current_selected_window = 1
        # 保存每个窗口当前显示的指标
        self.window_indicators = {
            1: "MA",  # K线图默认显示MA指标
            2: "VOL",  # 成交量图默认显示成交量
            3: "KDJ"  # 第三个窗口默认显示KDJ指标
        }
        # 初始化指标菜单
        self.create_indicator_menu()

        # 创建菜单栏
        self.create_menu_bar()

        # 创建工具栏
        self.create_tool_bar()

        # 创建中央部件
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        # 创建主布局
        main_layout = QVBoxLayout(central_widget)

        # 创建主分割器（左右布局）
        main_splitter = QSplitter(Qt.Orientation.Horizontal)
        main_layout.addWidget(main_splitter)

        # 创建左侧导航栏
        self.create_left_navigation(main_splitter)

        # 创建中间和右侧的分割器
        center_right_splitter = QSplitter(Qt.Orientation.Horizontal)
        main_splitter.addWidget(center_right_splitter)

        # 设置分割器比例
        main_splitter.setSizes([205, 1195])

        # 创建中间主区域
        self.create_center_area(center_right_splitter)

        # 创建右侧信息栏
        self.create_right_area(center_right_splitter)

        # 设置中间和右侧分割器比例，初始最小化右侧区域
        center_right_splitter.setSizes([1200, 0])

        # 创建状态栏
        self.create_status_bar()

    def create_left_navigation(self, parent):
        """
        创建左侧导航栏

        Args:
            parent: 父部件
        """
        left_widget = QWidget()
        left_widget.setObjectName("navPanel")
        left_layout = QVBoxLayout(left_widget)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(0)

        # 创建导航树：样式与绘制委托统一在 theme_manager 里，深浅主题共用一套结构
        self.nav_tree = QTreeWidget()
        self.nav_tree.setObjectName("navTree")
        self.nav_tree.setHeaderHidden(True)
        self.nav_tree.setIndentation(12)
        self.nav_tree.setIconSize(QSize(16, 16))
        self.nav_tree.setFrameShape(QFrame.NoFrame)
        # 视口开启鼠标跟踪与 hover 事件，条目悬停高亮才会被委托收到
        self.nav_tree.viewport().setMouseTracking(True)
        self.nav_tree.viewport().setAttribute(Qt.WidgetAttribute.WA_Hover, True)
        self.nav_tree.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        # 列宽撑满视口，悬停/选中高亮块才能铺满整行
        self.nav_tree.header().setStretchLastSection(True)
        self.nav_tree.header().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)

        dark = is_dark_palette()
        left_widget.setStyleSheet(sidebar_qss(dark))
        self.nav_tree.setItemDelegate(SidebarItemDelegate(self.nav_tree, dark=dark))

        # 添加导航项
        self.add_nav_item(self.nav_tree, "自选股", ["我的自选", "自定义板块1", "自定义板块2"])
        self.add_nav_item(self.nav_tree, "全景图", ["大盘指数", "行业板块", "概念板块"])
        self.add_nav_item(self.nav_tree, "沪深京指数", ["沪市指数", "深市指数", "京市指数", "创业板指", "科创板指"])
        self.add_nav_item(self.nav_tree, "沪深京板块", ["行业板块", "概念板块", "地域板块"])
        self.add_nav_item(self.nav_tree, "沪深京个股", ["全部A股", "上证A股", "深证A股", "京市个股", "创业板", "科创板"])
        self.add_nav_item(self.nav_tree, "热点主题", ["热门概念", "资金流入", "涨跌幅榜"])
        self.add_nav_item(self.nav_tree, "新股", ["新股申购", "新股上市", "次新股"])
        self.add_nav_item(self.nav_tree, "港股", ["港股通", "恒生指数"])
        self.add_nav_item(self.nav_tree, "期权", ["上证50ETF期权", "沪深300ETF期权"])
        self.add_nav_item(self.nav_tree, "ETF", ["ETF基金"])
        self.add_nav_item(self.nav_tree, "基金", ["LOF", "分级"])
        self.add_nav_item(self.nav_tree, "债券", ["国债", "企业债", "可转债"])
        self.add_nav_item(self.nav_tree, "新三板", ["精选层", "创新层", "基础层"])
        self.add_nav_item(self.nav_tree, "发现", ["研报中心", "财务分析", "估值分析"])

        # 连接导航项点击信号
        self.nav_tree.itemClicked.connect(self.data_view_manager.handle_nav_item_clicked)

        left_layout.addWidget(self.nav_tree)
        parent.addWidget(left_widget)

    def add_nav_item(self, parent, title, children):
        """
        添加导航项

        Args:
            parent: 父部件
            title: 导航项标题
            children: 子项列表
        """
        item = QTreeWidgetItem(parent)
        item.setText(0, title)

        for child in children:
            child_item = QTreeWidgetItem(item)
            child_item.setText(0, child)

        # 展开第一项
        if title == "自选股":
            item.setExpanded(True)

    def create_center_area(self, parent):
        """
        创建中间主区域

        Args:
            parent: 父部件
        """
        center_widget = QWidget()
        center_layout = QVBoxLayout(center_widget)
        # 上下外边距归零：页面容器与左侧导航面板同顶同底，
        # 左右保留原边距，不动横向版式。
        margins = center_layout.contentsMargins()
        center_layout.setContentsMargins(margins.left(), 0, margins.right(), 0)

        # 页面容器只放页面内容；标签栏已由 create_tool_bar 放进工具栏行，
        # 这里不再保留 QTabWidget 顶部标签条，页面区因此多出约一条标签栏高度。
        self.tab_widget = TabbedPageContainer(self.tab_bar)
        center_layout.addWidget(self.tab_widget)

        # 添加行情标签页
        self.market_tab = QWidget()
        self.tab_widget.addTab(self.market_tab, "行情")
        self.create_market_tab()

        # 添加技术分析标签页
        self.tech_tab = QWidget()
        self.tab_widget.addTab(self.tech_tab, "技术分析")
        self.create_tech_tab()

        # 添加财务分析标签页
        self.finance_tab = QWidget()
        self.tab_widget.addTab(self.finance_tab, "财务分析")
        self.create_finance_tab()

        # 添加每日推荐标签页（追加在末尾，保持技术分析页 index=1）
        self.advice_tab = QWidget()
        self.tab_widget.addTab(self.advice_tab, "每日推荐")
        self.create_advice_tab()

        # 添加市场趋势标签页（继续追加在末尾，不改变既有页 index）
        self.market_trend_tab = QWidget()
        self.tab_widget.addTab(self.market_trend_tab, "市场趋势")
        self.create_market_trend_tab()

        # 添加风格轮动标签页（紧随市场趋势之后：大势 → 风格 → 行业）
        self.style_rotation_tab = QWidget()
        self.tab_widget.addTab(self.style_rotation_tab, "风格轮动")
        self.create_style_rotation_tab()

        # 添加行业热度标签页（紧随风格轮动之后）
        self.industry_heat_tab = QWidget()
        self.tab_widget.addTab(self.industry_heat_tab, "行业热度")
        self.create_industry_heat_tab()

        # 添加仓位与风控标签页（紧随行业热度之后）
        self.portfolio_risk_tab = QWidget()
        self.tab_widget.addTab(self.portfolio_risk_tab, "仓位与风控")
        self.create_portfolio_risk_tab()

        # 添加收益归因标签页（紧随仓位与风控之后）
        self.attribution_tab = QWidget()
        self.tab_widget.addTab(self.attribution_tab, "收益归因")
        self.create_attribution_tab()

        # 添加模型操作清单标签页（紧随收益归因之后）
        self.action_list_tab = QWidget()
        self.tab_widget.addTab(self.action_list_tab, "模型操作清单")
        self.create_action_list_tab()

        # 添加 ETF 仓位择时标签页（紧随模型操作清单之后）
        self.etf_timing_tab = QWidget()
        self.tab_widget.addTab(self.etf_timing_tab, "ETF仓位择时")
        self.create_etf_timing_tab()

        parent.addWidget(center_widget)

    def create_market_tab(self) -> None:
        """
        创建行情标签页
        """
        market_layout = QVBoxLayout(self.market_tab)
        # 与左侧导航面板同顶同底：上下外边距归零，左右保留原边距
        margins = market_layout.contentsMargins()
        market_layout.setContentsMargins(margins.left(), 0, margins.right(), 0)

        # 创建进度条
        self.market_progress_bar = QProgressBar()
        self.market_progress_bar.setRange(0, 100)
        self.market_progress_bar.setValue(0)
        self.market_progress_bar.setVisible(False)  # 初始隐藏
        self.market_progress_bar.setStyleSheet("QProgressBar { height: 8px; background-color: #222; border: 1px solid #444; border-radius: 4px; text-align: left; text-indent: 10px; } QProgressBar::chunk { background-color: #00BFFF; border-radius: 0; margin: 0; }")
        market_layout.addWidget(self.market_progress_bar)
        # 设置进度条的大小策略，确保它在水平方向上拉伸
        self.market_progress_bar.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)

        # 创建股票列表
        self.stock_table = QTableWidget()
        self.stock_table.setColumnCount(13)  # 调整为通达信日线数据实际包含的字段
        self.stock_table.setHorizontalHeaderLabels([
            "日期", "代码", "名称", "涨跌幅%", "现价", "涨跌", "总量", "成交额", "今开", "最高", "最低", "昨收", "振幅%"
        ])

        # 列宽由 data_market_renderer 按每列最长内容实测（见 table_sizing 统一口径）

        # 设置表格为只读模式
        self.stock_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)

        # 初始化排序状态跟踪
        self.column_sort_states: dict[int, Any] = {}  # 保存每列的排序状态：0=未排序，1=升序，2=降序
        self.current_sorted_column = -1  # 当前排序的列

        self.stock_table.horizontalHeader().sectionClicked.connect(self.table_interaction_manager.on_header_clicked)
        self.stock_table.cellDoubleClicked.connect(self.table_interaction_manager.on_stock_double_clicked)

        # 初始状态禁用默认排序
        self.stock_table.setSortingEnabled(False)

        # 表头作为序号列：宽 50、居中、按显示行序自动重编；大表固定行高 32px
        vertical = self.stock_table.verticalHeader()
        vertical.setVisible(True)
        vertical.setFixedWidth(SEQ_COLUMN_WIDTH)
        vertical.setDefaultAlignment(Qt.AlignmentFlag.AlignCenter)
        vertical.setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
        vertical.setDefaultSectionSize(FIXED_ROW_HEIGHT)
        self.stock_table.setCornerButtonEnabled(False)
        self.stock_table.setWordWrap(False)
        self.stock_table.setAlternatingRowColors(False)
        self.stock_table.setShowGrid(True)

        # 底色与表头同色，与「市场趋势」等新页面的表格口径一致
        pal = palette_for('dark' if is_dark_palette() else 'light')
        self.stock_table.setStyleSheet(f"""
            QTableWidget {{
                background-color: {pal.card};
                color: {pal.text};
                gridline-color: {pal.faint};
                font-family: {TOKENS.font_family};
                font-size: {TOKENS.font_body}px;
            }}
            QTableWidget::item {{
                padding: 4px 6px;
                background-color: {pal.card};
            }}
            QHeaderView {{
                background-color: {pal.card};
            }}
            QHeaderView::section {{
                background-color: {pal.card};
                color: {pal.muted};
                border: none;
                border-bottom: 1px solid {pal.border};
                border-right: 1px solid {pal.border};
                font-weight: 600;
                padding: 6px 8px;
            }}
            QTableCornerButton::section {{
                background-color: {pal.card};
                border: none;
            }}
            QTableWidget::item:selected {{
                background-color: {pal.selection};
                color: {pal.text};
            }}
        """)


        market_layout.addWidget(self.stock_table)

    def create_finance_tab(self) -> None:
        """
        创建财务分析标签页：财务指标 + 近期资讯，按需后台取数。
        """
        from src.ui.finance import FinancePage

        finance_layout = QVBoxLayout(self.finance_tab)
        finance_layout.setContentsMargins(0, 0, 0, 0)
        self.finance_page = FinancePage(host=self, parent=self.finance_tab)

        # 整页套竖向滚动容器：该页最小高度约 570px，不套滚动会成为「每日推荐」
        # 之后新的窗口最小高度瓶颈，技术分析页仍无法在 1024×600 下完整显示。
        self.finance_scroll_area = QScrollArea()
        self.finance_scroll_area.setWidgetResizable(True)
        self.finance_scroll_area.setFrameShape(QFrame.Shape.NoFrame)
        self.finance_scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.finance_scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.finance_scroll_area.setWidget(self.finance_page)
        finance_layout.addWidget(self.finance_scroll_area)

        # 页签切到财务分析时才触发加载，避免后台无谓请求
        with contextlib.suppress(RuntimeError, TypeError):
            self.tab_widget.currentChanged.connect(self._on_finance_tab_changed)

    def _on_finance_tab_changed(self, index: int) -> None:
        """仅在当前页签就是财务分析时加载，避免其他页签切换误触。"""
        page = getattr(self, 'finance_page', None)
        if page is None:
            return
        if self.tab_widget.widget(index) is not self.finance_tab:
            return
        try:
            page.refresh_if_needed()
        except RuntimeError as exc:
            logger.error(f"财务分析页刷新失败: {exc}")

    def show_finance_tab(self) -> None:
        """统一入口：菜单/导航需要打开财务分析窗口时调用。"""
        page = getattr(self, 'finance_page', None)
        if page is None:
            logger.warning("财务分析页尚未初始化")
            return
        self.tab_widget.setCurrentWidget(self.finance_tab)
        try:
            page.refresh_if_needed()
        except RuntimeError as exc:
            logger.error(f"财务分析页刷新失败: {exc}")

    def create_right_area(self, parent):
        """
        创建右侧信息栏

        Args:
            parent: 父部件
        """
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)

        # 创建K线图区域
        kline_widget = QWidget()
        kline_layout = QVBoxLayout(kline_widget)

        kline_label = QLabel("K线图区域")
        kline_label.setAlignment(Qt.AlignCenter)
        kline_label.setStyleSheet("font-size: 16px; color: #666; background-color: #f0f0f0;")
        kline_layout.addWidget(kline_label)

        right_layout.addWidget(kline_widget)

        # 创建成交量图区域
        volume_widget = QWidget()
        volume_layout = QVBoxLayout(volume_widget)

        volume_label = QLabel("成交量图区域")
        volume_label.setAlignment(Qt.AlignCenter)
        volume_label.setStyleSheet("font-size: 16px; color: #666; background-color: #f5f5f5;")
        volume_layout.addWidget(volume_label)

        right_layout.addWidget(volume_widget)

        parent.addWidget(right_widget)

    def create_status_bar(self) -> None:
        """
        创建状态栏
        """
        status_bar = QStatusBar()
        self.setStatusBar(status_bar)

        # 添加状态栏信息
        self.market_info_label = QLabel("上证指数: 3864.18 -0.15% | 深证成指: 12087.13 +0.22% | 创业板指: 2911.00 -0.54%")
        status_bar.addWidget(self.market_info_label)

        # 添加分隔符（使用QLabel作为分隔符）
        separator1 = QLabel(" | ")
        status_bar.addWidget(separator1)

        # 添加时间信息
        self.time_label = QLabel("16:45:00")
        status_bar.addWidget(self.time_label)

        # 添加分隔符（使用QLabel作为分隔符）
        separator2 = QLabel(" | ")
        status_bar.addWidget(separator2)

        # 添加连接状态
        self.connection_label = QLabel("已连接")
        status_bar.addWidget(self.connection_label)

        # 添加分隔符（使用QLabel作为分隔符）
        separator3 = QLabel(" | ")
        status_bar.addWidget(separator3)

        # 添加进度条
        self.progress_bar = QProgressBar()
        self.progress_bar.setMaximumWidth(200)
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(100)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(False)  # 初始隐藏
        status_bar.addWidget(self.progress_bar)
