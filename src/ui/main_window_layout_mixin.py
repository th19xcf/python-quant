#!/usr/bin/env python3

"""
主窗口整体布局 Mixin：UI 初始化编排、左右分割布局、行情页、财务页与状态栏。
"""

from PySide6.QtCore import Qt
from PySide6.QtGui import QAction, QActionGroup
from PySide6.QtWidgets import (
    QAbstractItemView,
    QLabel,
    QMenu,
    QProgressBar,
    QSizePolicy,
    QSplitter,
    QStatusBar,
    QTableWidget,
    QTabWidget,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)


class MainWindowLayoutMixin:
    """主窗口骨架布局：导航栏、中央标签页、右侧信息栏与状态栏。"""

    def init_ui(self):
        """
        初始化UI组件（委托给UI管理器）
        """
        # 如果有ui_manager，则使用它，否则直接调用impl
        if hasattr(self, 'ui_manager'):
            return self.ui_manager.init_ui()
        return self._init_ui_impl()

    def _init_ui_impl(self):
        """
        初始化UI组件
        """
        # 初始化缺失的属性
        self.current_stock_data = None

        # 初始化窗口菜单
        self.current_window_count = 3
        self.window_menu = QMenu(self)
        self.window_actions = []

        # 创建QActionGroup，确保单选效果
        self.window_action_group = QActionGroup(self)
        self.window_action_group.setExclusive(True)

        for i in range(1, 10):  # 最大选择9个窗口
            action = QAction(f'{i}个窗口', self)
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
        main_splitter = QSplitter(Qt.Horizontal)
        main_layout.addWidget(main_splitter)

        # 创建左侧导航栏
        self.create_left_navigation(main_splitter)

        # 创建中间和右侧的分割器
        center_right_splitter = QSplitter(Qt.Horizontal)
        main_splitter.addWidget(center_right_splitter)

        # 设置分割器比例
        main_splitter.setSizes([200, 1200])

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
        left_layout = QVBoxLayout(left_widget)

        # 创建导航树
        self.nav_tree = QTreeWidget()
        self.nav_tree.setHeaderHidden(True)

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
        self.add_nav_item(self.nav_tree, "基金", ["开放式基金", "封闭式基金"])
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

        # 创建标签页
        self.tab_widget = QTabWidget()
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

        parent.addWidget(center_widget)

    def create_market_tab(self):
        """
        创建行情标签页
        """
        market_layout = QVBoxLayout(self.market_tab)

        # 创建进度条
        self.market_progress_bar = QProgressBar()
        self.market_progress_bar.setRange(0, 100)
        self.market_progress_bar.setValue(0)
        self.market_progress_bar.setVisible(False)  # 初始隐藏
        self.market_progress_bar.setStyleSheet("QProgressBar { height: 8px; background-color: #222; border: 1px solid #444; border-radius: 4px; text-align: left; text-indent: 10px; } QProgressBar::chunk { background-color: #00BFFF; border-radius: 0; margin: 0; }")
        market_layout.addWidget(self.market_progress_bar)
        # 设置进度条的大小策略，确保它在水平方向上拉伸
        self.market_progress_bar.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

        # 创建股票列表
        self.stock_table = QTableWidget()
        self.stock_table.setColumnCount(13)  # 调整为通达信日线数据实际包含的字段
        self.stock_table.setHorizontalHeaderLabels([
            "日期", "代码", "名称", "涨跌幅%", "现价", "涨跌", "总量", "成交额", "今开", "最高", "最低", "昨收", "振幅%"
        ])

        # 设置列宽
        column_widths = [100, 80, 100, 80, 80, 80, 100, 120, 80, 80, 80, 80, 80]  # 调整为适合通达信数据的列宽
        for i, width in enumerate(column_widths):
            self.stock_table.setColumnWidth(i, width)

        # 设置表格为只读模式
        self.stock_table.setEditTriggers(QAbstractItemView.NoEditTriggers)

        # 初始化排序状态跟踪
        self.column_sort_states = {}  # 保存每列的排序状态：0=未排序，1=升序，2=降序
        self.current_sorted_column = -1  # 当前排序的列

        self.stock_table.horizontalHeader().sectionClicked.connect(self.table_interaction_manager.on_header_clicked)
        self.stock_table.cellDoubleClicked.connect(self.table_interaction_manager.on_stock_double_clicked)

        # 初始状态禁用默认排序
        self.stock_table.setSortingEnabled(False)

        # 设置通达信风格的表格样式
        self.stock_table.setStyleSheet("""
            QTableWidget {
                background-color: #000000;
                color: #C0C0C0;
                gridline-color: #333333;
                font-family: "Microsoft YaHei";
                font-size: 12px;
            }
            QTableWidget::item {
                padding: 2px;
            }
            QHeaderView::section {
                background-color: #222222;
                color: #C0C0C0;
                border: 1px solid #333333;
                font-weight: bold;
                padding: 5px;
            }
            QTableWidget::item:selected {
                background-color: #0066CC;
                color: #FFFFFF;
            }
        """)


        market_layout.addWidget(self.stock_table)

    def create_finance_tab(self):
        """
        创建财务分析标签页
        """
        finance_layout = QVBoxLayout(self.finance_tab)

        # 添加财务分析占位符
        finance_label = QLabel("财务分析区域")
        finance_label.setAlignment(Qt.AlignCenter)
        finance_label.setStyleSheet("font-size: 16px; color: #666;")
        finance_layout.addWidget(finance_label)

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

    def create_status_bar(self):
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
