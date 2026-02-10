#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
主窗口UI混入类，负责界面构建
"""

import warnings
import math
import datetime
from typing import TYPE_CHECKING

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QSplitter,
    QTreeWidget, QTreeWidgetItem, QTableWidget, QTableWidgetItem,
    QTabWidget, QPushButton, QLineEdit, QLabel, QStatusBar,
    QMenuBar, QMenu, QDialog, QCheckBox, QDateEdit, QComboBox,
    QProgressBar, QAbstractItemView, QToolButton, QScrollArea
)
from PySide6.QtCore import Qt, QSize, QPoint
from PySide6.QtGui import QAction, QActionGroup, QIcon, QFont, QColor

import pyqtgraph as pg

from src.utils.logger import logger

if TYPE_CHECKING:
    from src.ui.main_window import MainWindow

class MainWindowUiMixin:
    """
    主窗口UI混入类
    包含所有UI创建相关的逻辑
    """
    
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
    
    def create_menu_bar(self):
        """
        创建菜单栏
        """
        menu_bar = self.menuBar()
        
        # 行情菜单
        market_menu = menu_bar.addMenu("行情")
        self.add_menu_actions(market_menu, [
            ("自选股", self.action_manager.on_self_selected),
            ("全景图", self.action_manager.on_panorama),
            ("沪深京指数", self.data_view_manager.show_index_overview),
            ("沪深京板块", self.action_manager.on_sector),
            ("沪深京个股", self.action_manager.on_stock),
        ])
        
        # 交易菜单
        trade_menu = menu_bar.addMenu("交易")
        self.add_menu_actions(trade_menu, [
            ("传统交易", self.action_manager.on_traditional_trade),
            ("条件单", self.action_manager.on_conditional_order),
            ("新股申购", self.action_manager.on_new_stock_subscribe),
        ])
        
        # 发现菜单
        discovery_menu = menu_bar.addMenu("发现")
        self.add_menu_actions(discovery_menu, [
            ("热点主题", self.action_manager.on_hot_topics),
            ("资金流向", self.action_manager.on_capital_flow),
            ("研报中心", self.action_manager.on_research_report),
        ])
        
        # 资讯菜单
        info_menu = menu_bar.addMenu("资讯")
        self.add_menu_actions(info_menu, [
            ("财经新闻", self.action_manager.on_financial_news),
            ("公司公告", self.action_manager.on_company_announcement),
            ("行业资讯", self.action_manager.on_industry_info),
        ])
        
        # 数据菜单
        data_menu = menu_bar.addMenu("数据")
        self.add_menu_actions(data_menu, [
            ("宏观数据", self.action_manager.on_macro_data),
            ("财务数据", self.action_manager.on_financial_data),
            ("技术指标", self.action_manager.on_technical_indicator),
            ("盘后数据下载", self.action_manager.on_download_data),
        ])
        
        # 特色功能菜单
        feature_menu = menu_bar.addMenu("特色功能")
        self.add_menu_actions(feature_menu, [
            ("量化回测", self.action_manager.on_quant_backtest),
            ("股票推荐", self.action_manager.on_stock_recommendation),
            ("自动交易", self.action_manager.on_auto_trade),
        ])
        
        # 系统菜单
        system_menu = menu_bar.addMenu("系统")
        self.add_menu_actions(system_menu, [
            ("设置", self.action_manager.on_settings),
            ("关于", self.action_manager.on_about),
            ("退出", self.action_manager.on_exit),
        ])
        
        # 窗口菜单已在__init__方法中创建，此处不再重新创建
    
    def add_menu_actions(self, menu, actions):
        """
        向菜单添加动作
        
        Args:
            menu: 菜单对象
            actions: 动作列表，每个动作是一个元组(名称, 回调函数)
        """
        for name, callback in actions:
            action = QAction(name, self)
            action.triggered.connect(callback)
            menu.addAction(action)
    
    def create_tool_bar(self):
        """
        创建工具栏
        """
        tool_bar = self.addToolBar("工具栏")
        tool_bar.setIconSize(QSize(16, 16))
        
        # 添加分隔符
        tool_bar.addSeparator()
        
        # 添加自选股按钮
        self.selected_btn = QPushButton("自选股")
        self.selected_btn.clicked.connect(self.action_manager.on_self_selected)
        tool_bar.addWidget(self.selected_btn)
        
        # 添加行情按钮
        self.market_btn = QPushButton("行情")
        self.market_btn.clicked.connect(self.action_manager.on_market)
        tool_bar.addWidget(self.market_btn)
        
        # 添加技术分析按钮
        self.tech_btn = QPushButton("技术分析")
        self.tech_btn.clicked.connect(self.action_manager.on_technical_analysis)
        tool_bar.addWidget(self.tech_btn)
        
        # 添加分隔符
        tool_bar.addSeparator()
        
        # 添加刷新按钮
        refresh_btn = QPushButton("刷新")
        refresh_btn.clicked.connect(self.action_manager.on_refresh)
        tool_bar.addWidget(refresh_btn)
        
        # 添加设置按钮
        settings_btn = QPushButton("设置")
        settings_btn.clicked.connect(self.action_manager.on_settings)
        tool_bar.addWidget(settings_btn)
    
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
        self.add_nav_item(self.nav_tree, "沪深京指数", ["上证指数", "深证成指", "创业板指", "科创板指"])
        self.add_nav_item(self.nav_tree, "沪深京板块", ["行业板块", "概念板块", "地域板块"])
        self.add_nav_item(self.nav_tree, "沪深京个股", ["全部A股", "上证A股", "深证A股", "创业板", "科创板"])
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
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(False)  # 初始隐藏
        self.progress_bar.setStyleSheet("QProgressBar { height: 5px; background-color: #333; border: none; } QProgressBar::chunk { background-color: #00BFFF; }")
        market_layout.addWidget(self.progress_bar)
        
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
        
        # 添加示例数据
        self.add_sample_stock_data()
        
        market_layout.addWidget(self.stock_table)
    
    def add_sample_stock_data(self):
        """
        添加示例股票数据
        """
        # 获取当前日期
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        
        sample_data = [
            [current_date, "999999", "上证指数", "-0.15", "3864.18", "-5.84", "458亿", "8960亿", "3867.43", "3879.92", "3861.18", "3870.02", "0.48"],
            [current_date, "600030", "中信证券", "+0.47", "27.60", "+0.13", "681695", "1.88亿", "27.52", "27.72", "27.44", "27.47", "1.02"],
            [current_date, "000001", "平安银行", "+1.23", "18.95", "+0.23", "1234567", "2.34亿", "18.75", "19.02", "18.70", "18.72", "1.71"],
            [current_date, "000858", "五粮液", "-0.89", "178.50", "-1.60", "345678", "6.18亿", "179.80", "180.20", "178.00", "180.10", "1.22"],
            [current_date, "600519", "贵州茅台", "+0.56", "1890.00", "+10.50", "12345", "2.33亿", "1880.00", "1895.00", "1875.00", "1879.50", "1.06"],
        ]
        
        self.stock_table.setRowCount(len(sample_data))
        
        for row, data in enumerate(sample_data):
            for col, value in enumerate(data):
                item = QTableWidgetItem(value)
                
                # 设置对齐方式
                if col in [3, 4, 5, 6, 7, 8, 9, 10, 11, 12]:
                    item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                
                # 设置通达信风格的颜色
                if col == 3:  # 涨跌幅%
                    # 涨跌幅颜色
                    if value.startswith("+") or (value.replace(".", "").isdigit() and float(value) > 0):
                        item.setForeground(QColor(255, 0, 0))  # 红色上涨
                    elif value.startswith("-"):
                        item.setForeground(QColor(0, 255, 0))  # 绿色下跌
                    else:
                        item.setForeground(QColor(204, 204, 204))  # 灰色平盘
                elif col == 5:  # 涨跌
                    # 涨跌额颜色
                    if value.startswith("+") or (value.replace(".", "").isdigit() and float(value) > 0):
                        item.setForeground(QColor(255, 0, 0))  # 红色上涨
                    elif value.startswith("-"):
                        item.setForeground(QColor(0, 255, 0))  # 绿色下跌
                    else:
                        item.setForeground(QColor(204, 204, 204))  # 灰色平盘
                # 获取昨收价用于比较
                preclose = float(data[11]) if len(data) > 11 and data[11] != "-" else 0.0
                if col == 4:  # 现价
                    current_price = float(value) if value != "-" else 0.0
                    if current_price > preclose:
                        item.setForeground(QColor(255, 0, 0))  # 红色高于昨收
                    elif current_price < preclose:
                        item.setForeground(QColor(0, 255, 0))  # 绿色低于昨收
                    else:
                        item.setForeground(QColor(204, 204, 204))  # 灰色等于昨收
                elif col == 8:  # 今开
                    # 今开与昨收比较
                    open_price = float(value) if value != "-" else 0.0
                    if open_price > preclose:
                        item.setForeground(QColor(255, 0, 0))  # 红色高于昨收
                    elif open_price < preclose:
                        item.setForeground(QColor(0, 255, 0))  # 绿色低于昨收
                    else:
                        item.setForeground(QColor(204, 204, 204))  # 灰色等于昨收
                elif col == 9:  # 最高
                    # 最高与昨收比较
                    high_price = float(value) if value != "-" else 0.0
                    if high_price > preclose:
                        item.setForeground(QColor(255, 0, 0))  # 红色高于昨收
                    elif high_price < preclose:
                        item.setForeground(QColor(0, 255, 0))  # 绿色低于昨收
                    else:
                        item.setForeground(QColor(204, 204, 204))  # 灰色等于昨收
                elif col == 10:  # 最低
                    # 最低与昨收比较
                    low_price = float(value) if value != "-" else 0.0
                    if low_price > preclose:
                        item.setForeground(QColor(255, 0, 0))  # 红色高于昨收
                    elif low_price < preclose:
                        item.setForeground(QColor(0, 255, 0))  # 绿色低于昨收
                    else:
                        item.setForeground(QColor(204, 204, 204))  # 灰色等于昨收
                
                self.stock_table.setItem(row, col, item)
    
    def create_tech_tab(self):
        """
        创建技术分析标签页
        """
        tech_layout = QVBoxLayout(self.tech_tab)
        
        # 导入pyqtgraph
        import pyqtgraph as pg
        from PySide6.QtCore import Qt
        
        # 创建工具栏
        toolbar = QWidget()
        toolbar_layout = QHBoxLayout(toolbar)
        toolbar.setStyleSheet("background-color: #222222;")       

        # 添加周期按钮
        self.period_buttons = {
            '日线': QPushButton('日线'),
            '周线': QPushButton('周线'),
            '月线': QPushButton('月线')
        }
        
        # 设置按钮样式
        button_style = """
        QPushButton {
            background-color: #333333;
            color: #C0C0C0;
            border: 1px solid #444444;
            padding: 6px 12px;
            border-radius: 4px;
            font-family: 'Microsoft YaHei';
            font-size: 12px;
        }
        QPushButton:hover {
            background-color: #444444;
        }
        QPushButton:checked {
            background-color: #555555;
            border: 1px solid #666666;
        }
        """
        
        # 添加按钮到工具栏
        for name, button in self.period_buttons.items():
            button.setCheckable(True)
            button.setStyleSheet(button_style)
            button.toggled.connect(lambda checked, p=name: self.indicator_interaction_manager.on_period_changed(p, checked))
            toolbar_layout.addWidget(button)
        
        # 当前窗口数量已在__init__方法中初始化
        
        # 默认选中日线
        self.period_buttons['日线'].setChecked(True)
        self.current_period = '日线'
        
        # 添加显示柱体数量的输入框
        bar_count_label = QLabel("柱体数:")
        bar_count_label.setStyleSheet("color: #C0C0C0; font-family: 'Microsoft YaHei'; font-size: 12px;")
        toolbar_layout.addWidget(bar_count_label)
        
        self.bar_count_input = QLineEdit()
        self.bar_count_input.setText("100")  # 默认显示100个柱体
        self.bar_count_input.setStyleSheet("""
        QLineEdit {
            background-color: #333333;
            color: #C0C0C0;
            border: 1px solid #444444;
            padding: 4px 6px;
            border-radius: 4px;
            font-family: 'Microsoft YaHei';
            font-size: 12px;
            width: 60px;
        }
        QLineEdit:focus {
            border: 1px solid #666666;
            background-color: #444444;
        }
        """)
        # 连接输入框事件
        self.bar_count_input.editingFinished.connect(self.indicator_interaction_manager.on_bar_count_changed)
        toolbar_layout.addWidget(self.bar_count_input)
        
        # 添加分隔符
        toolbar_layout.addSpacing(10)
        
        # 添加复权按钮
        self.adjustment_btn = QPushButton("前复权")  # 默认显示前复权
        self.adjustment_btn.setStyleSheet(button_style)
        self.adjustment_btn.setCheckable(True)
        self.adjustment_btn.setChecked(True)  # 默认选中前复权
        self.adjustment_btn.clicked.connect(self.indicator_interaction_manager.on_adjustment_clicked)
        toolbar_layout.addWidget(self.adjustment_btn)
        
        # 添加工具栏到布局
        tech_layout.addWidget(toolbar)
        
        # 创建图表容器，用于放置标签和分割器
        self.chart_container = QWidget()
        self.chart_layout = QVBoxLayout(self.chart_container)
        self.chart_layout.setSpacing(0)
        self.chart_layout.setContentsMargins(0, 0, 0, 0)
        
        # 创建垂直分割器，用于放置K线图和成交量图
        self.chart_splitter = QSplitter(Qt.Vertical)
        self.chart_splitter.setStyleSheet("QSplitter::handle:vertical { background-color: #444444; height: 2px; border: 0px; }")
        self.chart_splitter.setHandleWidth(2)
        self.chart_splitter.setOpaqueResize(True)
        
        # 创建K线图
        self.tech_plot_widget = pg.PlotWidget()
        self.tech_plot_widget.setBackground('#000000')
        self.tech_plot_widget.setLabel('left', '价格', color='#C0C0C0')
        # 不显示X轴标签
        self.tech_plot_widget.setLabel('bottom', '')
        self.tech_plot_widget.getAxis('left').setPen(pg.mkPen('#C0C0C0'))
        self.tech_plot_widget.getAxis('bottom').setPen(pg.mkPen('#C0C0C0'))
        self.tech_plot_widget.getAxis('left').setTextPen(pg.mkPen('#C0C0C0'))
        self.tech_plot_widget.getAxis('bottom').setTextPen(pg.mkPen('#C0C0C0'))
        self.tech_plot_widget.showGrid(x=True, y=True, alpha=0.3)
        
        # 创建成交量图
        self.volume_plot_widget = pg.PlotWidget()
        self.volume_plot_widget.setBackground('#000000')
        self.volume_plot_widget.setLabel('left', '成交量', color='#C0C0C0')
        self.volume_plot_widget.setLabel('bottom', '', color='#C0C0C0')
        self.volume_plot_widget.getAxis('left').setPen(pg.mkPen('#C0C0C0'))
        self.volume_plot_widget.getAxis('bottom').setPen(pg.mkPen('#C0C0C0'))
        self.volume_plot_widget.getAxis('left').setTextPen(pg.mkPen('#C0C0C0'))
        self.volume_plot_widget.getAxis('bottom').setTextPen(pg.mkPen('#C0C0C0'))
        self.volume_plot_widget.showGrid(x=True, y=True, alpha=0.3)
        
        # 确保两个图的Y轴宽度一致，实现竖轴对齐
        # 设置Y轴宽度为50像素
        self.tech_plot_widget.getAxis('left').setWidth(50)
        self.volume_plot_widget.getAxis('left').setWidth(50)
        
        # 设置X轴宽度，确保两个图的X轴对齐
        self.tech_plot_widget.getAxis('bottom').setHeight(20)
        self.volume_plot_widget.getAxis('bottom').setHeight(20)
        
        # 创建KDJ指标图
        self.kdj_plot_widget = pg.PlotWidget()
        self.kdj_plot_widget.setBackground('#000000')
        self.kdj_plot_widget.setLabel('left', 'KDJ', color='#C0C0C0')
        self.kdj_plot_widget.setLabel('bottom', '', color='#C0C0C0')
        self.kdj_plot_widget.getAxis('left').setPen(pg.mkPen('#C0C0C0'))
        self.kdj_plot_widget.getAxis('bottom').setPen(pg.mkPen('#C0C0C0'))
        self.kdj_plot_widget.getAxis('left').setTextPen(pg.mkPen('#C0C0C0'))
        self.kdj_plot_widget.getAxis('bottom').setTextPen(pg.mkPen('#C0C0C0'))
        self.kdj_plot_widget.showGrid(x=True, y=True, alpha=0.3)
        # 设置KDJ指标图的Y轴范围，考虑到KDJ可能超出0-100范围，特别是J值
        self.kdj_plot_widget.setYRange(-50, 150)
        # 确保KDJ图的Y轴宽度与其他图一致
        self.kdj_plot_widget.getAxis('left').setWidth(50)
        self.kdj_plot_widget.getAxis('bottom').setHeight(20)
        
        # 为K线图创建容器，只包含K线图
        self.tech_container = QWidget()
        self.tech_container_layout = QVBoxLayout(self.tech_container)
        self.tech_container_layout.setSpacing(0)
        self.tech_container_layout.setContentsMargins(0, 0, 0, 0)
        
        self.tech_container_layout.addWidget(self.tech_plot_widget)
        
        # 添加点击事件，用于选中窗口
        self.tech_container.mousePressEvent = lambda event: self.indicator_interaction_manager.on_window_clicked(1)
        
        # 为成交量图创建容器，只包含成交量图
        self.volume_container = QWidget()
        self.volume_container_layout = QVBoxLayout(self.volume_container)
        self.volume_container_layout.setSpacing(0)
        self.volume_container_layout.setContentsMargins(0, 0, 0, 0)
        
        self.volume_container_layout.addWidget(self.volume_plot_widget)
        
        # 添加点击事件，用于选中窗口
        self.volume_container.mousePressEvent = lambda event: self.indicator_interaction_manager.on_window_clicked(2)
        
        # 为KDJ指标图创建容器，只包含KDJ指标图
        self.kdj_container = QWidget()
        self.kdj_container_layout = QVBoxLayout(self.kdj_container)
        self.kdj_container_layout.setSpacing(0)
        self.kdj_container_layout.setContentsMargins(0, 0, 0, 0)
        
        self.kdj_container_layout.addWidget(self.kdj_plot_widget)
        
        # 添加点击事件，用于选中窗口
        self.kdj_container.mousePressEvent = lambda event: self.indicator_interaction_manager.on_window_clicked(3)
        
        # 添加图表到分割器
        self.chart_splitter.addWidget(self.tech_container)
        self.chart_splitter.addWidget(self.volume_container)
        self.chart_splitter.addWidget(self.kdj_container)
        
        # 设置分割比例（K线图占50%，成交量图占25%，KDJ图占25%）
        # 先使用setStretchFactor设置相对比例
        self.chart_splitter.setStretchFactor(0, 2)  # 第一个组件（K线图）占2份
        self.chart_splitter.setStretchFactor(1, 1)  # 第二个组件（成交量图）占1份
        self.chart_splitter.setStretchFactor(2, 1)  # 第三个组件（KDJ图）占1份
        
        # 再使用setSizes设置初始尺寸，确保比例正确
        self.chart_splitter.setSizes([2000, 1000, 1000])
        
        # 添加分割器到容器布局
        self.chart_layout.addWidget(self.chart_splitter, 1)  # 1表示垂直方向拉伸
        
        # 添加容器到主布局
        tech_layout.addWidget(self.chart_container, 1)  # 1表示垂直方向拉伸
        
        # 添加指标选择窗口
        self.create_indicator_selection()
        tech_layout.addWidget(self.indicator_widget)
        
        # 保存k线图数据项
        self.candle_plot_item = None
        self.volume_bar_item = None
        self.volume_ma_item = None
        
        # 初始化完成后，设置为3个窗口模式
        self.indicator_interaction_manager.on_window_count_changed(3, True)
    
    def _create_indicator_button(self, indicator_name, style_sheet, checkable=True, fixed_width=None, clicked_handler=None):
        """
        创建指标按钮的通用方法
        
        Args:
            indicator_name: 指标名称
            style_sheet: 按钮样式
            checkable: 是否可勾选
            fixed_width: 固定宽度，None表示自适应
            clicked_handler: 点击事件处理函数
            
        Returns:
            QPushButton: 创建的按钮实例
        """
        btn = QPushButton(indicator_name)
        btn.setStyleSheet(style_sheet)
        btn.setCheckable(checkable)
        
        if fixed_width is not None:
            btn.setFixedWidth(fixed_width)
        
        if clicked_handler is not None:
            btn.clicked.connect(clicked_handler)
        else:
            btn.clicked.connect(lambda checked, ind=indicator_name: self.indicator_interaction_manager.on_indicator_clicked(ind, checked))
        
        return btn
    
    def _create_separator(self):
        """
        创建分隔符的通用方法
        
        Returns:
            QLabel: 创建的分隔符实例
        """
        separator = QLabel("|")
        separator.setStyleSheet("color: #666666; font-size: 12px;")
        return separator
    
    def create_indicator_selection(self):
        """
        创建指标选择窗口，类似通达信的指标选择栏
        """
        # 创建指标选择容器
        self.indicator_widget = QWidget()
        self.indicator_widget.setStyleSheet("background-color: #222222;")
        indicator_layout = QHBoxLayout(self.indicator_widget)
        indicator_layout.setContentsMargins(5, 3, 5, 3)
        indicator_layout.setSpacing(2)
        
        # 指标列表
        indicators = [
            "窗口", "指标A", "<", "VOL", "MACD", "KDJ", "DMI", "DMA", "FSL", "TRIX", "BRAR", "CR", 
            "VR", "OBV", "ASI", "EMV", "VOL-TDX", "RSI", "WR", "SAR",  
            "CCI", "ROC", "MTM", "BOLL", "PSY", "MCST", ">" ]
        
        # 创建指标按钮样式
        indicator_button_style = """
        QPushButton {
            background-color: #333333;
            color: #C0C0C0;
            border: 1px solid #444444;
            padding: 3px 8px;
            border-radius: 3px;
            font-family: 'Microsoft YaHei';
            font-size: 11px;
        }
        QPushButton:hover {
            background-color: #444444;
        }
        QPushButton:checked {
            background-color: #555555;
            border: 1px solid #666666;
            color: #FFFFFF;
        }
        """
        
        # 创建水平滚动区域
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll_area.setStyleSheet("QScrollArea { background-color: transparent; border: none; }")
        self.indicator_interaction_manager.set_scroll_area(scroll_area)
        
        # 创建滚动内容容器
        scroll_content = QWidget()
        scroll_layout = QHBoxLayout(scroll_content)
        scroll_layout.setContentsMargins(0, 0, 0, 0)
        scroll_layout.setSpacing(2)
        
        # 保存指标按钮
        self.indicator_buttons = {}
        
        # 创建指标按钮
        for indicator in indicators:
            if indicator == "窗口":
                # 特殊处理窗口按钮，使其具有与上方工具栏相同的功能
                btn = self._create_indicator_button(
                    indicator, 
                    indicator_button_style, 
                    checkable=False
                )
                # 使用已经创建好的顶部菜单，共享相同的actions和状态管理
                # 这样可以确保单选功能正常工作
                btn.clicked.connect(lambda checked, b=btn, m=self.window_menu: 
                                   m.popup(b.mapToGlobal(QPoint(0, -m.sizeHint().height()))))
                
                scroll_layout.addWidget(btn)
                self.indicator_buttons[indicator] = btn
            elif indicator == "指标A":
                # 特殊按钮样式
                btn = self._create_indicator_button(indicator, indicator_button_style)
                scroll_layout.addWidget(btn)
                self.indicator_buttons[indicator] = btn
                
                # 添加左箭头滚动按钮
                left_arrow_btn = self._create_indicator_button(
                    "<", 
                    indicator_button_style, 
                    checkable=False,
                    fixed_width=20,
                    clicked_handler=self.indicator_interaction_manager.on_left_arrow_clicked
                )
                scroll_layout.addWidget(left_arrow_btn)
                self.indicator_buttons["<"] = left_arrow_btn
                
                # 添加分隔符
                scroll_layout.addWidget(self._create_separator())
            elif indicator == "指标B" or indicator == "模板":
                # 特殊按钮样式
                btn = QPushButton(indicator)
                btn.setCheckable(True)
                btn.setStyleSheet(indicator_button_style)
                btn.clicked.connect(lambda checked, ind=indicator: self.indicator_interaction_manager.on_indicator_clicked(ind, checked))
                scroll_layout.addWidget(btn)
                self.indicator_buttons[indicator] = btn
                
                # 添加分隔符
                if indicator in ["指标A", "指标B"]:
                    separator = QLabel("|")
                    separator.setStyleSheet("color: #666666; font-size: 12px;")
                    scroll_layout.addWidget(separator)
            else:
                # 已实现的指标列表
                implemented_indicators = [
                    'VOL', 'MACD', 'KDJ', 'DMI', 'DMA', 'FSL', 'TRIX', 'BRAR', 'CR',
                    'VR', 'OBV', 'ASI', 'EMV', 'VOL-TDX', 'RSI', 'WR', 'SAR',
                    'CCI', 'ROC', 'MTM', 'BOLL', 'PSY', 'MCST'
                ]
                # 未实现指标的样式（灰色文字）
                disabled_indicator_style = indicator_button_style + "QPushButton { color: #666666; }"

                # 普通指标按钮
                # 检查指标是否已实现（箭头按钮特殊处理）
                is_arrow = indicator in ['<', '>']
                if indicator in implemented_indicators or is_arrow:
                    # 已实现的指标或箭头按钮，使用默认样式
                    btn = self._create_indicator_button(indicator, indicator_button_style)
                else:
                    # 未实现的指标，使用灰色文字样式
                    btn = self._create_indicator_button(indicator, disabled_indicator_style, checkable=False)
                    btn.setDisabled(True)
                
                scroll_layout.addWidget(btn)
                self.indicator_buttons[indicator] = btn
        
        # 添加指标B和模板按钮
        scroll_layout.addWidget(self._create_separator())
        
        btn = self._create_indicator_button("指标B", indicator_button_style)
        scroll_layout.addWidget(btn)
        self.indicator_buttons["指标B"] = btn
        
        scroll_layout.addWidget(self._create_separator())
        
        btn = self._create_indicator_button("模板", indicator_button_style)
        scroll_layout.addWidget(btn)
        self.indicator_buttons["模板"] = btn
        
        # 设置滚动区域内容
        scroll_area.setWidget(scroll_content)
        
        # 添加滚动区域到主布局
        indicator_layout.addWidget(scroll_area)
        
        # 设置滚动区域高度
        scroll_area.setMaximumHeight(30)
        
        # 添加+、-按钮用于调整柱体数量
        # 添加分隔符
        indicator_layout.addWidget(self._create_separator())
        
        # 添加+按钮
        plus_btn = self._create_indicator_button(
            "+", 
            indicator_button_style, 
            checkable=False,
            fixed_width=20,
            clicked_handler=self.indicator_interaction_manager.on_plus_btn_clicked
        )
        indicator_layout.addWidget(plus_btn)
        
        # 添加-按钮
        minus_btn = self._create_indicator_button(
            "-", 
            indicator_button_style, 
            checkable=False,
            fixed_width=20,
            clicked_handler=self.indicator_interaction_manager.on_minus_btn_clicked
        )
        indicator_layout.addWidget(minus_btn)
    
    def create_indicator_menu(self):
        """
        创建指标功能菜单
        """
        # 创建主菜单
        self.indicator_menu = QMenu()
        
        # 选择副图指标
        submenu = QMenu("选择副图指标", self.indicator_menu)
        self.indicator_menu.addMenu(submenu)
        
        # 指标用法注释
        action = QAction("指标用法注释", self.indicator_menu)
        action.triggered.connect(self.indicator_interaction_manager.on_indicator_usage)
        self.indicator_menu.addAction(action)
        
        # 调整指标参数
        action = QAction("调整指标参数", self.indicator_menu)
        action.triggered.connect(self.indicator_interaction_manager.on_indicator_params)
        self.indicator_menu.addAction(action)
        
        # 修改当前指标公式
        action = QAction("修改当前指标公式", self.indicator_menu)
        action.triggered.connect(self.indicator_interaction_manager.on_indicator_formula)
        self.indicator_menu.addAction(action)
    
    def create_indicator_menu_button(self, window_num):
        """
        创建功能菜单按钮
        
        Args:
            window_num: 窗口编号
            
        Returns:
            QPushButton: 功能菜单按钮
        """
        # 创建功能菜单按钮
        btn = QPushButton("≡")
        # 设置按钮背景色与第1窗口标签栏一致
        btn.setStyleSheet("background-color: transparent; color: #C0C0C0; border: none; font-size: 12px; padding: 0 5px; height: 20px;")
        btn.setFixedSize(20, 20)
        btn.setToolTip("功能菜单")
        # 连接点击事件
        btn.clicked.connect(lambda: self.show_indicator_menu(btn, window_num))
        
        return btn
    
    def show_indicator_menu(self, btn, window_num):
        """
        显示指标功能菜单
        
        Args:
            btn: 触发菜单的按钮
            window_num: 窗口编号
        """
        # 显示菜单
        self.indicator_menu.exec_(btn.mapToGlobal(btn.rect().bottomLeft()))
        logger.info(f"显示窗口{window_num}的指标功能菜单")

    def _on_window_clicked_impl(self, window_num):
        """
        窗口点击事件处理
        """
        if 1 <= window_num <= 3:
            self.current_selected_window = window_num
            logger.info(f"选中了窗口: {window_num}")
            
            # 更新窗口标题，显示当前选中的窗口
            for container in [self.tech_container, self.volume_container, self.kdj_container]:
                container.setStyleSheet("background-color: transparent;")
            
            # 如果存在标签栏容器，移除其边框
            if hasattr(self, 'title_ma_container'):
                self.title_ma_container.setStyleSheet("background-color: #222222;")
            
            # 为选中的窗口添加边框，显示选中状态
            if window_num == 1:
                # 为标签栏容器添加左、右、上边框，移除下边框，确保两边有竖线，中间没有竖分割线
                if hasattr(self, 'title_ma_container'):
                    self.title_ma_container.setStyleSheet("background-color: #222222; border-top: 1px solid #00BFFF; border-left: 1px solid #00BFFF; border-right: 1px solid #00BFFF; border-bottom: none;")
                # 为K线图容器添加完整的边框，使其包含整个K线图区域
                self.tech_container.setStyleSheet("background-color: transparent; border: 1px solid #00BFFF;")
            elif window_num == 2:
                self.volume_container.setStyleSheet("background-color: transparent; border: 1px solid #00BFFF;")
            elif window_num == 3:
                self.kdj_container.setStyleSheet("background-color: transparent; border: 1px solid #00BFFF;")
    

    def _on_indicator_clicked_impl(self, indicator, checked):
        """
        指标按钮点击事件处理
        """
        if checked:
            # 取消其他同类型指标的选中状态
            if indicator in ["指标A", "指标B"]:
                # 指标A/B是互斥的
                for name, button in self.indicator_buttons.items():
                    if name in ["指标A", "指标B"] and name != indicator:
                        button.setChecked(False)
            elif indicator != "模板" and indicator != "窗口":
                # 普通指标是互斥的
                for name, button in self.indicator_buttons.items():
                    if name not in ["指标A", "指标B", "模板", "窗口"] and name != indicator:
                        button.setChecked(False)
            
            # 根据当前选中的窗口设置指标
            if indicator not in ["指标A", "指标B", "模板", "窗口"]:
                # 保存当前选中窗口的指标
                self.window_indicators[self.current_selected_window] = indicator
                logger.info(f"为窗口 {self.current_selected_window} 设置了指标: {indicator}")
                
                # 重新绘制K线图，应用新的指标
                if hasattr(self, 'current_stock_data') and self.current_stock_data is not None:
                    self.plot_k_line(self.current_stock_data, self.current_stock_name, self.current_stock_code)
        
        logger.info(f"选择了指标: {indicator}, 状态: {'选中' if checked else '取消'}")
    
    
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
