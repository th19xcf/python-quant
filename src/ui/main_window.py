#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
主窗口类，参考通达信软件界面设计
"""

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QSplitter,
    QTreeWidget, QTreeWidgetItem, QTableWidget, QTableWidgetItem,
    QTabWidget, QPushButton, QLineEdit, QLabel, QStatusBar,
    QMenuBar, QMenu, QDialog, QCheckBox, QDateEdit, QComboBox,
    QProgressBar, QAbstractItemView, QToolButton, QScrollArea
)
from PySide6.QtCore import Qt, QSize, QPoint
from PySide6.QtGui import QAction, QActionGroup, QIcon, QFont, QColor
import warnings
from functools import wraps

# 忽略PySide6信号断开警告
warnings.simplefilter("ignore", RuntimeWarning)

from src.utils.logger import logger
from src.utils.event_bus import EventBus
from src.ui.chart_items import CandleStickItem


def event_handler(event_type):
    """
    事件处理装饰器，统一记录日志和处理事件
    
    Args:
        event_type: 事件类型，用于日志记录
        
    Returns:
        decorator: 装饰器函数
    """
    def decorator(func):
        @wraps(func)
        def wrapper(self, sender, *args, **kwargs):
            # 记录事件接收
            logger.info(f"收到{event_type}事件: {args} {kwargs}")
            try:
                # 调用实际的事件处理函数
                return func(self, sender, *args, **kwargs)
            except Exception as e:
                # 记录异常
                logger.exception(f"处理{event_type}事件时发生错误: {e}")
        return wrapper
    return decorator


class MainWindow(QMainWindow):
    """
    主窗口类，参考通达信软件界面设计
    """
    
    def __init__(self, config, data_manager, plugin_manager=None):
        """
        初始化主窗口
        
        Args:
            config: 配置对象
            data_manager: 数据管理器实例
            plugin_manager: 插件管理器实例
        """
        super().__init__()
        self.config = config
        self.data_manager = data_manager
        self.plugin_manager = plugin_manager
        self.setWindowTitle("中国股市量化分析系统")
        self.setGeometry(100, 100, 1400, 900)
        
        # 订阅事件
        self._subscribe_events()
        
        # 初始化K线图相关属性
        self.vline = None
        self.hline = None
        self.info_text = None
        self.info_timer = None
        self.current_mouse_pos = None
        self.current_kline_index = -1
        
        # 初始化其他UI组件
        self.init_ui()
        self.current_kline_data = None
        self.displayed_bar_count = 100  # 默认显示100个柱体
        self.crosshair_enabled = False  # 十字线和信息框显示状态，False=隐藏，True=显示
    
    def _subscribe_events(self):
        """
        订阅事件
        """
        # 订阅数据更新事件
        EventBus.subscribe('data_updated', self._handle_data_updated)
        EventBus.subscribe('data_error', self._handle_data_error)
        
        # 订阅技术指标计算完成事件
        EventBus.subscribe('indicator_calculated', self._handle_indicator_calculated)
        EventBus.subscribe('indicator_error', self._handle_indicator_error)
        
        # 订阅系统事件
        EventBus.subscribe('system_shutdown', self._handle_system_shutdown)
        
        logger.info("主窗口事件订阅完成")
    
    @event_handler("数据更新")
    def _handle_data_updated(self, sender, data_type, ts_code, message=""):
        """
        处理数据更新事件
        
        Args:
            data_type: 数据类型
            ts_code: 股票代码或指数代码
            message: 附加消息
        """
        # 根据数据类型更新UI
        if data_type in ['stock_daily', 'index_daily']:
            # 如果当前显示的是该股票或指数，刷新K线图
            if hasattr(self, 'current_stock_code') and self.current_stock_code == ts_code:
                self.refresh_kline_chart()
        elif data_type == 'stock_basic':
            # 更新股票列表
            self.update_stock_list()
        elif data_type == 'index_basic':
            # 更新指数列表
            self.update_index_list()
    
    @event_handler("数据错误")
    def _handle_data_error(self, sender, data_type, ts_code, message=""):
        """
        处理数据错误事件
        
        Args:
            data_type: 数据类型
            ts_code: 股票代码或指数代码
            message: 错误消息
        """
        # 可以在这里显示错误提示
    
    @event_handler("指标计算完成")
    def _handle_indicator_calculated(self, sender, indicator_name, ts_code, result=None):
        """
        处理技术指标计算完成事件
        
        Args:
            indicator_name: 指标名称
            ts_code: 股票代码
            result: 计算结果
        """
        # 更新指标显示
        if hasattr(self, 'current_stock_code') and self.current_stock_code == ts_code:
            self.refresh_indicator_charts()
    
    @event_handler("指标计算错误")
    def _handle_indicator_error(self, sender, indicator_name, ts_code, error=""):
        """
        处理技术指标计算错误事件
        
        Args:
            indicator_name: 指标名称
            ts_code: 股票代码
            error: 错误信息
        """
        pass
    
    @event_handler("系统关闭")
    def _handle_system_shutdown(self, sender):
        """
        处理系统关闭事件
        """
        # 可以在这里保存用户设置等
        self.close()
    
    def refresh_kline_chart(self):
        """
        刷新K线图
        """
        # 实现K线图刷新逻辑
        logger.info("刷新K线图")
    
    def refresh_indicator_charts(self):
        """
        刷新指标图表
        """
        # 实现指标图表刷新逻辑
        logger.info("刷新指标图表")
    
    def update_stock_list(self):
        """
        更新股票列表
        """
        # 实现股票列表更新逻辑
        logger.info("更新股票列表")
    
    def update_index_list(self):
        """
        更新指数列表
        """
        # 实现指数列表更新逻辑
        logger.info("更新指数列表")
        
        # 保存当前显示的个股信息
        self.current_stock_data = None
        self.current_stock_name = ""
        self.current_stock_code = ""
        
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
            action.triggered.connect(lambda checked, w=i: self.on_window_count_changed(w, checked))
            self.window_menu.addAction(action)
            self.window_actions.append(action)
        
        # 当前选中的窗口（1: K线图, 2: 成交量, 3: KDJ/其他指标）
        self.current_selected_window = 1
        # 保存每个窗口当前显示的指标
        self.window_indicators = {
            1: "MA",  # K线图默认显示MA指标
            2: "VOL",  # 成交量图默认显示成交量
            3: "KDJ"  # 第三个窗口默认显示KDJ指标
        }
        
        # 创建指标功能菜单
        self.create_indicator_menu()
        
        # 初始化UI组件
        self.init_ui()
        logger.info("主窗口初始化成功")
        
        # 初始化后获取实时数据
        self.refresh_stock_data()
        self.refresh_market_info()
    
    def init_ui(self):
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
            action.triggered.connect(lambda checked, w=i: self.on_window_count_changed(w, checked))
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
            ("自选股", self.on_self_selected),
            ("全景图", self.on_panorama),
            ("沪深京指数", self.on_index),
            ("沪深京板块", self.on_sector),
            ("沪深京个股", self.on_stock),
        ])
        
        # 交易菜单
        trade_menu = menu_bar.addMenu("交易")
        self.add_menu_actions(trade_menu, [
            ("传统交易", self.on_traditional_trade),
            ("条件单", self.on_conditional_order),
            ("新股申购", self.on_new_stock_subscribe),
        ])
        
        # 发现菜单
        discovery_menu = menu_bar.addMenu("发现")
        self.add_menu_actions(discovery_menu, [
            ("热点主题", self.on_hot_topics),
            ("资金流向", self.on_capital_flow),
            ("研报中心", self.on_research_report),
        ])
        
        # 资讯菜单
        info_menu = menu_bar.addMenu("资讯")
        self.add_menu_actions(info_menu, [
            ("财经新闻", self.on_financial_news),
            ("公司公告", self.on_company_announcement),
            ("行业资讯", self.on_industry_info),
        ])
        
        # 数据菜单
        data_menu = menu_bar.addMenu("数据")
        self.add_menu_actions(data_menu, [
            ("宏观数据", self.on_macro_data),
            ("财务数据", self.on_financial_data),
            ("技术指标", self.on_technical_indicator),
            ("盘后数据下载", self.on_download_data),
        ])
        
        # 特色功能菜单
        feature_menu = menu_bar.addMenu("特色功能")
        self.add_menu_actions(feature_menu, [
            ("量化回测", self.on_quant_backtest),
            ("股票推荐", self.on_stock_recommendation),
            ("自动交易", self.on_auto_trade),
        ])
        
        # 系统菜单
        system_menu = menu_bar.addMenu("系统")
        self.add_menu_actions(system_menu, [
            ("设置", self.on_settings),
            ("关于", self.on_about),
            ("退出", self.on_exit),
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
        
        # 添加搜索框
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("输入股票代码/名称")
        self.search_edit.setFixedWidth(200)
        tool_bar.addWidget(self.search_edit)
        
        # 添加搜索按钮
        search_btn = QPushButton("搜索")
        search_btn.clicked.connect(self.on_search)
        tool_bar.addWidget(search_btn)
        
        # 添加分隔符
        tool_bar.addSeparator()
        
        # 添加自选股按钮
        self.selected_btn = QPushButton("自选股")
        self.selected_btn.clicked.connect(self.on_self_selected)
        tool_bar.addWidget(self.selected_btn)
        
        # 添加行情按钮
        self.market_btn = QPushButton("行情")
        self.market_btn.clicked.connect(self.on_market)
        tool_bar.addWidget(self.market_btn)
        
        # 添加技术分析按钮
        self.tech_btn = QPushButton("技术分析")
        self.tech_btn.clicked.connect(self.on_technical_analysis)
        tool_bar.addWidget(self.tech_btn)
        
        # 添加分隔符
        tool_bar.addSeparator()
        
        # 添加刷新按钮
        refresh_btn = QPushButton("刷新")
        refresh_btn.clicked.connect(self.on_refresh)
        tool_bar.addWidget(refresh_btn)
        
        # 添加设置按钮
        settings_btn = QPushButton("设置")
        settings_btn.clicked.connect(self.on_settings)
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
        self.nav_tree.itemClicked.connect(self.on_nav_item_clicked)
        
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
        
        # 连接表头点击信号到自定义槽函数
        self.stock_table.horizontalHeader().sectionClicked.connect(self.on_header_clicked)
        
        # 连接表格双击信号到自定义槽函数
        self.stock_table.cellDoubleClicked.connect(self.on_stock_double_clicked)
        
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
        import datetime
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
                    # 现价与昨收比较
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
            button.toggled.connect(lambda checked, p=name: self.on_period_changed(p, checked))
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
        self.bar_count_input.editingFinished.connect(self.on_bar_count_changed)
        toolbar_layout.addWidget(self.bar_count_input)
        
        # 添加分隔符
        toolbar_layout.addSpacing(10)
        
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
        self.tech_container.mousePressEvent = lambda event: self.on_window_clicked(1)
        
        # 为成交量图创建容器，只包含成交量图
        self.volume_container = QWidget()
        self.volume_container_layout = QVBoxLayout(self.volume_container)
        self.volume_container_layout.setSpacing(0)
        self.volume_container_layout.setContentsMargins(0, 0, 0, 0)
        
        self.volume_container_layout.addWidget(self.volume_plot_widget)
        
        # 添加点击事件，用于选中窗口
        self.volume_container.mousePressEvent = lambda event: self.on_window_clicked(2)
        
        # 为KDJ指标图创建容器，只包含KDJ指标图
        self.kdj_container = QWidget()
        self.kdj_container_layout = QVBoxLayout(self.kdj_container)
        self.kdj_container_layout.setSpacing(0)
        self.kdj_container_layout.setContentsMargins(0, 0, 0, 0)
        
        self.kdj_container_layout.addWidget(self.kdj_plot_widget)
        
        # 添加点击事件，用于选中窗口
        self.kdj_container.mousePressEvent = lambda event: self.on_window_clicked(3)
        
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
        self.on_window_count_changed(3, True)
    
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
            btn.clicked.connect(lambda checked, ind=indicator_name: self.on_indicator_clicked(ind, checked))
        
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
                    clicked_handler=self.on_left_arrow_clicked
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
                btn.clicked.connect(lambda checked, ind=indicator: self.on_indicator_clicked(ind, checked))
                scroll_layout.addWidget(btn)
                self.indicator_buttons[indicator] = btn
                
                # 添加分隔符
                if indicator in ["指标A", "指标B"]:
                    separator = QLabel("|")
                    separator.setStyleSheet("color: #666666; font-size: 12px;")
                    scroll_layout.addWidget(separator)
            else:
                # 普通指标按钮
                btn = self._create_indicator_button(indicator, indicator_button_style)
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
            clicked_handler=self.on_plus_btn_clicked
        )
        indicator_layout.addWidget(plus_btn)
        
        # 添加-按钮
        minus_btn = self._create_indicator_button(
            "-", 
            indicator_button_style, 
            checkable=False,
            fixed_width=20,
            clicked_handler=self.on_minus_btn_clicked
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
        action.triggered.connect(self.on_indicator_usage)
        self.indicator_menu.addAction(action)
        
        # 调整指标参数
        action = QAction("调整指标参数", self.indicator_menu)
        action.triggered.connect(self.on_indicator_params)
        self.indicator_menu.addAction(action)
        
        # 修改当前指标公式
        action = QAction("修改当前指标公式", self.indicator_menu)
        action.triggered.connect(self.on_indicator_formula)
        self.indicator_menu.addAction(action)
    
    def on_indicator_usage(self):
        """
        指标用法注释
        """
        logger.info("显示指标用法注释")
        # 这里可以添加显示指标用法的逻辑
    
    def on_indicator_params(self):
        """
        调整指标参数
        """
        logger.info("调整指标参数")
        # 这里可以添加调整指标参数的逻辑
    
    def on_indicator_formula(self):
        """
        修改当前指标公式
        """
        logger.info("修改当前指标公式")
        # 这里可以添加修改指标公式的逻辑
    
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
    
    def on_window_clicked(self, window_num):
        """
        窗口点击事件处理
        
        Args:
            window_num: 窗口编号（1: K线图, 2: 成交量, 3: KDJ/其他指标）
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
    
    def on_indicator_clicked(self, indicator, checked):
        """
        指标按钮点击事件处理
        
        Args:
            indicator: 指标名称
            checked: 是否被选中
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
                if self.current_stock_data is not None:
                    self.plot_k_line(self.current_stock_data, self.current_stock_name, self.current_stock_code)
        
        logger.info(f"选择了指标: {indicator}, 状态: {'选中' if checked else '取消'}")
    
    def on_plus_btn_clicked(self):
        """
        点击+按钮，增加1倍的柱体数
        """
        try:
            # 增加1倍柱体数
            new_count = self.displayed_bar_count * 2
            
            # 更新输入框
            self.bar_count_input.setText(str(new_count))
            
            # 更新显示的柱体数量
            self.displayed_bar_count = new_count
            
            logger.info(f"柱体数增加1倍，当前柱体数: {new_count}")
            
            # 如果当前有显示的个股数据，重新绘制K线图
            if self.current_stock_data is not None:
                self.plot_k_line(self.current_stock_data, self.current_stock_name, self.current_stock_code)
        except Exception as e:
            logger.exception(f"增加柱体数失败: {e}")
    
    def on_minus_btn_clicked(self):
        """
        点击-按钮，减少1倍的柱体数
        """
        try:
            # 减少1倍柱体数，最小为50
            new_count = max(50, self.displayed_bar_count // 2)
            
            # 更新输入框
            self.bar_count_input.setText(str(new_count))
            
            # 更新显示的柱体数量
            self.displayed_bar_count = new_count
            
            logger.info(f"柱体数减少1倍，当前柱体数: {new_count}")
            
            # 如果当前有显示的个股数据，重新绘制K线图
            if self.current_stock_data is not None:
                self.plot_k_line(self.current_stock_data, self.current_stock_name, self.current_stock_code)
        except Exception as e:
            logger.exception(f"减少柱体数失败: {e}")
    
    def on_left_arrow_clicked(self):
        """
        点击左箭头按钮，指标选择栏向左滚动
        """
        try:
            # 查找指标选择栏的滚动区域
            scroll_area = None
            for child in self.indicator_widget.children():
                if isinstance(child, QScrollArea):
                    scroll_area = child
                    break
            
            if scroll_area:
                # 获取当前滚动位置
                current_pos = scroll_area.horizontalScrollBar().value()
                # 计算新的滚动位置（向左滚动100像素）
                new_pos = max(0, current_pos - 100)
                # 设置新的滚动位置
                scroll_area.horizontalScrollBar().setValue(new_pos)
                logger.info(f"指标选择栏向左滚动，当前位置: {new_pos}")
        except Exception as e:
            logger.exception(f"指标选择栏向左滚动失败: {e}")
    
    def on_period_changed(self, period, checked):
        """
        周期按钮点击事件处理
        
        Args:
            period: 周期类型（日线、周线、月线）
            checked: 是否被选中
        """
        if checked:
            # 取消其他按钮的选中状态
            for name, button in self.period_buttons.items():
                if name != period:
                    button.setChecked(False)
            
            # 更新当前周期
            self.current_period = period
            
            # 如果是日线，且有当前个股数据，按照柱体数重新绘制K线
            if period == "日线" and self.current_stock_data is not None:
                print(f"切换到日线，按照柱体数 {self.displayed_bar_count} 重新绘制K线")
                # 重新绘制K线图
                self.plot_k_line(self.current_stock_data, self.current_stock_name, self.current_stock_code)
            else:
                # TODO: 其他周期的K线图更新逻辑
                print(f"切换到{period}")
    
    def on_window_count_changed(self, window_count, checked):
        """
        窗口数量变化事件处理
        
        Args:
        
            window_count: 窗口数量
            checked: 是否被选中
        """
        if checked:
            # 取消其他窗口数量选项的选中状态
            for action in self.window_actions:
                if action.text() != f'{window_count}个窗口':
                    action.setChecked(False)
            
            # 更新当前窗口数量
            self.current_window_count = window_count
            
            # 实现根据窗口数量重新布局图表的逻辑
            if hasattr(self, 'tech_plot_widget') and hasattr(self, 'volume_plot_widget') and hasattr(self, 'kdj_plot_widget') and hasattr(self, 'chart_splitter'):
                if window_count == 1:
                    # 1个窗口模式：只显示K线图，充满整个区域
                    self.volume_plot_widget.hide()
                    self.kdj_plot_widget.hide()
                    # 隐藏成交量标签栏
                    if hasattr(self, 'volume_values_label'):
                        self.volume_values_label.hide()
                    # 隐藏成交量容器和KDJ容器
                    if hasattr(self, 'volume_container'):
                        self.volume_container.hide()
                    if hasattr(self, 'kdj_container'):
                        self.kdj_container.hide()
                    # 调整分割器，让K线图充满整个区域
                    self.chart_splitter.setSizes([1, 0, 0])
                    logger.info("切换到1个窗口：只显示K线图，隐藏成交量图、KDJ图和标签栏")
                elif window_count == 2:
                    # 2个窗口模式：显示K线图和成交量图，隐藏KDJ图
                    self.volume_plot_widget.show()
                    self.kdj_plot_widget.hide()
                    # 显示成交量标签栏
                    if hasattr(self, 'volume_values_label'):
                        self.volume_values_label.show()
                    # 显示成交量容器，隐藏KDJ容器
                    if hasattr(self, 'volume_container'):
                        self.volume_container.show()
                    if hasattr(self, 'kdj_container'):
                        self.kdj_container.hide()
                    # 调整分割器比例，让K线图和成交量图都显示
                    # 先使用setStretchFactor设置相对比例
                    self.chart_splitter.setStretchFactor(0, 2)  # K线图占66.6%
                    self.chart_splitter.setStretchFactor(1, 1)  # 成交量图占33.3%
                    # 再使用setSizes设置初始尺寸，确保比例正确
                    self.chart_splitter.setSizes([2000, 1000, 0])
                    logger.info(f"切换到{window_count}个窗口：显示K线图和成交量图，隐藏KDJ图")
                else:  # window_count == 3
                    # 3个窗口模式：显示K线图、成交量图和KDJ图
                    self.volume_plot_widget.show()
                    self.kdj_plot_widget.show()
                    # 显示成交量标签栏
                    if hasattr(self, 'volume_values_label'):
                        self.volume_values_label.show()
                    # 显示成交量容器和KDJ容器
                    if hasattr(self, 'volume_container'):
                        self.volume_container.show()
                    if hasattr(self, 'kdj_container'):
                        self.kdj_container.show()
                    # 调整分割器比例，让三个图都显示
                    # 先使用setStretchFactor设置相对比例
                    self.chart_splitter.setStretchFactor(0, 2)  # K线图占50%
                    self.chart_splitter.setStretchFactor(1, 1)  # 成交量图占25%
                    self.chart_splitter.setStretchFactor(2, 1)  # KDJ图占25%
                    # 再使用setSizes设置初始尺寸，确保比例正确
                    self.chart_splitter.setSizes([2000, 1000, 1000])
                    logger.info(f"切换到{window_count}个窗口：显示K线图、成交量图和KDJ图")
            
            print(f"切换到{window_count}个窗口")
    
    def on_bar_count_changed(self):
        """
        显示柱体数量输入框事件处理
        """
        try:
            # 获取输入的数值
            bar_count_text = self.bar_count_input.text()
            bar_count = int(bar_count_text)
            
            # 验证数值有效性
            if bar_count > 0:
                # 更新显示的柱体数量
                self.displayed_bar_count = bar_count
                print(f"显示柱体数量更新为: {bar_count}")
                
                # 如果当前有显示的个股数据，重新绘制K线图
                if self.current_stock_data is not None:
                    print(f"按照新的柱体数 {bar_count} 重新绘制K线图")
                    self.plot_k_line(self.current_stock_data, self.current_stock_name, self.current_stock_code)
            else:
                # 输入无效，恢复默认值
                self.bar_count_input.setText("100")
                self.displayed_bar_count = 100
                print(f"无效的柱体数量，已恢复为默认值: 100")
        except ValueError:
            # 输入不是整数，恢复默认值
            self.bar_count_input.setText("100")
            self.displayed_bar_count = 100
            print(f"无效的输入，已恢复为默认值: 100")
    
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
    
    def on_self_selected(self):
        logger.info("点击了自选股")
    
    def on_panorama(self):
        logger.info("点击了全景图")
    
    def on_index(self):
        """
        点击了沪深京指数，从通达信指数文件获取数据并更新表格
        """
        try:
            logger.info("开始从通达信获取沪深京指数数据")
            
            # 主要指数代码映射
            index_map = {
                "sh000001": "上证指数",
                "sh000016": "上证50",
                "sh000300": "沪深300",
                "sh000905": "中证500",
                "sh000852": "中证1000",
                "sh000688": "科创板指",
                "sz399001": "深证成指",
                "sz399006": "创业板指"
            }
            
            # 清空现有数据
            self.stock_table.setSortingEnabled(False)
            self.stock_table.setRowCount(0)
            
            # 设置表头 - 确保与数据行匹配
            headers = ["日期", "代码", "名称", "涨跌幅", "现价", "涨跌额", "总量", "成交额", "开盘价", "最高价", "最低价", "昨收价", "振幅%"]
            self.stock_table.setColumnCount(len(headers))
            self.stock_table.setHorizontalHeaderLabels(headers)
            
            # 构建通达信指数数据目录路径
            from pathlib import Path
            tdx_data_path = Path(self.data_manager.config.data.tdx_data_path)
            
            # 获取所有指数文件
            sh_index_files = list(Path(tdx_data_path / 'sh' / 'lday').glob('sh*.day')) if (tdx_data_path / 'sh' / 'lday').exists() else []
            sz_index_files = list(Path(tdx_data_path / 'sz' / 'lday').glob('sz*.day')) if (tdx_data_path / 'sz' / 'lday').exists() else []
            all_index_files = sh_index_files + sz_index_files
            
            logger.info(f"找到{len(all_index_files)}个通达信指数数据文件")
            
            import struct
            from datetime import datetime
            
            # 解析指数文件
            for index_file in all_index_files:
                try:
                    file_name = index_file.stem
                    if file_name not in index_map:
                        continue
                    
                    index_name = index_map[file_name]
                    
                    with open(index_file, 'rb') as f:
                        # 获取文件大小
                        f.seek(0, 2)
                        file_size = f.tell()
                        
                        # 计算数据条数
                        record_count = file_size // 32
                        if record_count == 0:
                            continue
                        
                        # 读取最新两条记录，用于计算涨跌幅和涨跌
                        # 先读取最新一条记录（当天数据）
                        f.seek((record_count - 1) * 32)
                        latest_record = f.read(32)
                        
                        # 如果有至少两条记录，读取前一天的记录（用于计算涨跌）
                        if record_count >= 2:
                            f.seek((record_count - 2) * 32)
                            prev_record = f.read(32)
                        else:
                            prev_record = None
                        
                        # 解析最新一条记录
                        date_int = struct.unpack('I', latest_record[0:4])[0]  # 日期，格式：YYYYMMDD
                        open_val = struct.unpack('I', latest_record[4:8])[0] / 100  # 开盘价，转换为元
                        high_val = struct.unpack('I', latest_record[8:12])[0] / 100  # 最高价，转换为元
                        low_val = struct.unpack('I', latest_record[12:16])[0] / 100  # 最低价，转换为元
                        close_val = struct.unpack('I', latest_record[16:20])[0] / 100  # 收盘价，转换为元
                        volume = struct.unpack('I', latest_record[20:24])[0]  # 成交量，单位：手
                        amount = struct.unpack('I', latest_record[24:28])[0] / 100  # 成交额，转换为元
                        
                        # 转换日期格式
                        date_str = str(date_int)
                        date = datetime.strptime(date_str, '%Y%m%d').date()
                        
                        # 计算涨跌额和涨跌幅
                        if prev_record:
                            # 解析前一天数据
                            prev_date_int = struct.unpack('I', prev_record[0:4])[0]  # 前一天日期
                            prev_close_val = struct.unpack('I', prev_record[16:20])[0] / 100  # 前一天收盘价
                            
                            # 计算涨跌额和涨跌幅
                            preclose = prev_close_val  # 昨收价
                            change = close_val - preclose  # 涨跌额
                            pct_chg = (change / preclose) * 100 if preclose != 0 else 0.0  # 涨跌幅
                        else:
                            # 只有一条记录，无法计算涨跌额和涨跌幅，设为0
                            preclose = close_val  # 没有前一天数据，使用收盘价作为昨收价
                            change = 0.0
                            pct_chg = 0.0
                        
                        # 计算振幅
                        if preclose > 0:
                            amplitude = ((high_val - low_val) / preclose) * 100
                        else:
                            amplitude = 0.0
                        
                        # 构建数据行，适配新的列结构
                        data_row = [
                            date.strftime('%Y-%m-%d'),  # 日期
                            file_name,  # 代码
                            index_name,  # 名称
                            f"{pct_chg:.2f}",  # 涨跌幅
                            f"{close_val:.2f}",  # 现价
                            f"{change:.2f}",  # 涨跌
                            f"{volume:,}",  # 总量
                            f"{amount:,}",  # 成交额
                            f"{open_val:.2f}",  # 今开
                            f"{high_val:.2f}",  # 最高
                            f"{low_val:.2f}",  # 最低
                            f"{preclose:.2f}",  # 昨收
                            f"{amplitude:.2f}%"  # 振幅%
                        ]
                        
                        # 添加行
                        row_pos = self.stock_table.rowCount()
                        self.stock_table.insertRow(row_pos)
                        
                        # 设置数据
                        for col, value in enumerate(data_row):
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
                            preclose_val = float(data_row[11]) if len(data_row) > 11 and data_row[11] != "-" else 0.0
                            if col == 4:  # 现价
                                # 现价与昨收比较
                                current_price = float(value) if value != "-" else 0.0
                                if current_price > preclose_val:
                                    item.setForeground(QColor(255, 0, 0))  # 红色高于昨收
                                elif current_price < preclose_val:
                                    item.setForeground(QColor(0, 255, 0))  # 绿色低于昨收
                                else:
                                    item.setForeground(QColor(204, 204, 204))  # 灰色等于昨收
                            elif col == 8:  # 今开
                                # 今开与昨收比较
                                open_price = float(value) if value != "-" else 0.0
                                if open_price > preclose_val:
                                    item.setForeground(QColor(255, 0, 0))  # 红色高于昨收
                                elif open_price < preclose_val:
                                    item.setForeground(QColor(0, 255, 0))  # 绿色低于昨收
                                else:
                                    item.setForeground(QColor(204, 204, 204))  # 灰色等于昨收
                            elif col == 9:  # 最高
                                # 最高与昨收比较
                                high_price = float(value) if value != "-" else 0.0
                                if high_price > preclose_val:
                                    item.setForeground(QColor(255, 0, 0))  # 红色高于昨收
                                elif high_price < preclose_val:
                                    item.setForeground(QColor(0, 255, 0))  # 绿色低于昨收
                                else:
                                    item.setForeground(QColor(204, 204, 204))  # 灰色等于昨收
                            elif col == 10:  # 最低
                                # 最低与昨收比较
                                low_price = float(value) if value != "-" else 0.0
                                if low_price > preclose_val:
                                    item.setForeground(QColor(255, 0, 0))  # 红色高于昨收
                                elif low_price < preclose_val:
                                    item.setForeground(QColor(0, 255, 0))  # 绿色低于昨收
                                else:
                                    item.setForeground(QColor(204, 204, 204))  # 灰色等于昨收
                            
                            self.stock_table.setItem(row_pos, col, item)
                    
                except Exception as e:
                    logger.exception(f"解析指数文件{index_file}失败: {e}")
                    continue
            
            # 数据添加完成后重新启用排序
            self.stock_table.setSortingEnabled(True)
            
            # 设置表格为只读模式
            self.stock_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
            
            logger.info("沪深京指数数据更新完成")
            self.statusBar().showMessage(f"成功显示{self.stock_table.rowCount()}个指数的最新交易日数据", 3000)
            
        except Exception as e:
            logger.exception(f"获取沪深京指数数据失败: {e}")
            self.statusBar().showMessage(f"获取沪深京指数数据失败: {str(e)[:50]}...", 5000)
    
    def on_sector(self):
        logger.info("点击了沪深京板块")
    
    def on_stock(self):
        logger.info("点击了沪深京个股")
    
    def on_traditional_trade(self):
        logger.info("点击了传统交易")
    
    def on_conditional_order(self):
        logger.info("点击了条件单")
    
    def on_new_stock_subscribe(self):
        logger.info("点击了新股申购")
    
    def on_hot_topics(self):
        logger.info("点击了热点主题")
    
    def on_capital_flow(self):
        logger.info("点击了资金流向")
    
    def on_research_report(self):
        logger.info("点击了研报中心")
    
    def on_financial_news(self):
        logger.info("点击了财经新闻")
    
    def on_company_announcement(self):
        logger.info("点击了公司公告")
    
    def on_industry_info(self):
        logger.info("点击了行业资讯")
    
    def on_macro_data(self):
        logger.info("点击了宏观数据")
    
    def on_financial_data(self):
        logger.info("点击了财务数据")
    
    def on_technical_indicator(self):
        logger.info("点击了技术指标")
    
    def on_download_data(self):
        logger.info("点击了盘后数据下载")
    
    def on_quant_backtest(self):
        logger.info("点击了量化回测")
    
    def on_stock_recommendation(self):
        logger.info("点击了股票推荐")
    
    def on_auto_trade(self):
        logger.info("点击了自动交易")
    
    def on_settings(self):
        logger.info("点击了设置")
    
    def on_about(self):
        logger.info("点击了关于")
    
    def on_exit(self):
        logger.info("点击了退出")
        self.close()
    
    def on_search(self):
        search_text = self.search_edit.text()
        logger.info(f"搜索: {search_text}")
    
    def on_market(self):
        logger.info("点击了行情按钮")
    
    def on_technical_analysis(self):
        logger.info("点击了技术分析按钮")
        self.tab_widget.setCurrentIndex(1)
    
    def on_refresh(self):
        logger.info("点击了刷新按钮")
    
    def on_header_clicked(self, logical_index):
        """
        自定义表头点击事件处理，实现点击第3次取消排序的功能
        
        Args:
            logical_index: 点击的列索引
        """
        try:
            # 获取当前列的排序状态，默认为未排序(0)
            current_state = self.column_sort_states.get(logical_index, 0)
            
            # 计算下一个状态：0→1→2→0
            next_state = (current_state + 1) % 3
            
            # 更新排序状态
            self.column_sort_states[logical_index] = next_state
            
            # 应用排序
            if next_state == 1:
                # 升序排序
                # 确保排序功能是启用的
                self.stock_table.setSortingEnabled(True)
                # 执行排序
                self.stock_table.sortItems(logical_index, Qt.AscendingOrder)
                # 更新当前排序列
                self.current_sorted_column = logical_index
            elif next_state == 2:
                # 降序排序
                # 确保排序功能是启用的
                self.stock_table.setSortingEnabled(True)
                # 执行排序
                self.stock_table.sortItems(logical_index, Qt.DescendingOrder)
                # 更新当前排序列
                self.current_sorted_column = logical_index
            else:
                # 取消排序，恢复原始顺序
                # 直接清空并重新添加数据，保持原始顺序
                self.stock_table.setSortingEnabled(False)
                
                # 保存当前显示的所有数据
                current_data = []
                row_count = self.stock_table.rowCount()
                for row in range(row_count):
                    row_data = []
                    for col in range(self.stock_table.columnCount()):
                        item = self.stock_table.item(row, col)
                        row_data.append(item.text() if item else "")
                    current_data.append(row_data)
                
                # 清空表格
                self.stock_table.setRowCount(0)
                
                # 重新添加数据，保持原始顺序
                for row_data in current_data:
                    row_pos = self.stock_table.rowCount()
                    self.stock_table.insertRow(row_pos)
                    for col, value in enumerate(row_data):
                        item = QTableWidgetItem(value)
                        # 恢复对齐方式
                        if col in [3, 4, 5, 6, 7, 8, 9, 10, 11, 12]:
                            item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                        # 恢复颜色编码
                        if col == 3:  # 涨跌幅%
                            if value.startswith("+") or (value.replace(".", "").isdigit() and float(value) > 0):
                                item.setForeground(QColor(255, 0, 0))
                            elif value.startswith("-"):
                                item.setForeground(QColor(0, 255, 0))
                            else:
                                item.setForeground(QColor(204, 204, 204))
                        elif col == 5:  # 涨跌
                            if value.startswith("+") or (value.replace(".", "").isdigit() and float(value) > 0):
                                item.setForeground(QColor(255, 0, 0))
                            elif value.startswith("-"):
                                item.setForeground(QColor(0, 255, 0))
                            else:
                                item.setForeground(QColor(204, 204, 204))
                        elif col == 4:  # 现价
                            preclose = float(row_data[11]) if len(row_data) > 11 and row_data[11] != "-" else 0.0
                            current_price = float(value) if value != "-" else 0.0
                            if current_price > preclose:
                                item.setForeground(QColor(255, 0, 0))
                            elif current_price < preclose:
                                item.setForeground(QColor(0, 255, 0))
                            else:
                                item.setForeground(QColor(204, 204, 204))
                        elif col == 8:  # 今开
                            preclose = float(row_data[11]) if len(row_data) > 11 and row_data[11] != "-" else 0.0
                            open_price = float(value) if value != "-" else 0.0
                            if open_price > preclose:
                                item.setForeground(QColor(255, 0, 0))
                            elif open_price < preclose:
                                item.setForeground(QColor(0, 255, 0))
                            else:
                                item.setForeground(QColor(204, 204, 204))
                        elif col == 9:  # 最高
                            preclose = float(row_data[11]) if len(row_data) > 11 and row_data[11] != "-" else 0.0
                            high_price = float(value) if value != "-" else 0.0
                            if high_price > preclose:
                                item.setForeground(QColor(255, 0, 0))
                            elif high_price < preclose:
                                item.setForeground(QColor(0, 255, 0))
                            else:
                                item.setForeground(QColor(204, 204, 204))
                        elif col == 10:  # 最低
                            preclose = float(row_data[11]) if len(row_data) > 11 and row_data[11] != "-" else 0.0
                            low_price = float(value) if value != "-" else 0.0
                            if low_price > preclose:
                                item.setForeground(QColor(255, 0, 0))
                            elif low_price < preclose:
                                item.setForeground(QColor(0, 255, 0))
                            else:
                                item.setForeground(QColor(204, 204, 204))
                        # 设置单元格
                        self.stock_table.setItem(row_pos, col, item)
                
                # 重新启用排序功能，以便下次点击时显示排序指示器
                self.stock_table.setSortingEnabled(True)
                # 更新当前排序列为未排序
                self.current_sorted_column = -1
        except Exception as e:
            logger.exception(f"处理表头点击事件失败: {e}")
    
    def on_stock_double_clicked(self, row, column):
        """
        处理股票表格双击事件，显示该股票的技术分析图表
        
        Args:
            row: 双击的行索引
            column: 双击的列索引
        """
        try:
            # 获取股票代码和名称
            code_item = self.stock_table.item(row, 1)
            name_item = self.stock_table.item(row, 2)
            if not code_item or not name_item:
                return
            
            code = code_item.text()
            name = name_item.text()
            logger.info(f"双击了股票: {name}({code})")
            
            # 解析股票代码，确定市场类型
            if code.startswith('sh'):
                # 指数代码，如sh000001
                market = 'sh'
                tdx_code = code
                ts_code = f'{code[2:]}.SH'
            elif code.startswith('sz'):
                # 指数代码，如sz399001
                market = 'sz'
                tdx_code = code
                ts_code = f'{code[2:]}.SZ'
            elif code.startswith('6'):
                # 上海股票
                market = 'sh'
                tdx_code = f'sh{code}'
                ts_code = f'{code}.SH'
            else:
                # 深圳股票
                market = 'sz'
                tdx_code = f'sz{code}'
                ts_code = f'{code}.SZ'
            
            # 从通达信数据文件读取历史数据
            from pathlib import Path
            import struct
            from datetime import datetime
            import polars as pl
            import numpy as np
            import pyqtgraph as pg
            
            # 构建通达信日线数据文件路径
            tdx_data_path = Path(self.data_manager.config.data.tdx_data_path)
            tdx_file_path = tdx_data_path / market / 'lday' / f'{tdx_code}.day'
            
            if not tdx_file_path.exists():
                logger.warning(f"找不到股票数据文件: {tdx_file_path}")
                self.statusBar().showMessage(f"找不到股票数据文件: {tdx_file_path}", 5000)
                return
            
            logger.info(f"正在读取股票数据文件: {tdx_file_path}")
            
            # 读取并解析通达信日线数据文件
            data = []
            with open(tdx_file_path, 'rb') as f:
                # 获取文件大小
                f.seek(0, 2)
                file_size = f.tell()
                f.seek(0)
                
                # 计算数据条数
                record_count = file_size // 32
                if record_count == 0:
                    logger.warning(f"股票数据文件为空: {tdx_file_path}")
                    self.statusBar().showMessage(f"股票数据文件为空: {tdx_file_path}", 5000)
                    return
                
                # 读取所有记录
                for i in range(record_count):
                    record = f.read(32)
                    if len(record) < 32:
                        break
                    
                    # 解析记录
                    date_int = struct.unpack('I', record[0:4])[0]  # 日期，格式：YYYYMMDD
                    open_val = struct.unpack('I', record[4:8])[0] / 100  # 开盘价，转换为元
                    high_val = struct.unpack('I', record[8:12])[0] / 100  # 最高价，转换为元
                    low_val = struct.unpack('I', record[12:16])[0] / 100  # 最低价，转换为元
                    close_val = struct.unpack('I', record[16:20])[0] / 100  # 收盘价，转换为元
                    volume = struct.unpack('I', record[20:24])[0]  # 成交量，单位：手
                    amount = struct.unpack('I', record[24:28])[0] / 100  # 成交额，转换为元
                    
                    # 转换日期格式
                    date_str = str(date_int)
                    date = datetime.strptime(date_str, '%Y%m%d').date()
                    
                    # 添加到数据列表
                    data.append({
                        'date': date,
                        'open': open_val,
                        'high': high_val,
                        'low': low_val,
                        'close': close_val,
                        'volume': volume,
                        'amount': amount
                    })
            
            # 将数据转换为Polars DataFrame
            df = pl.DataFrame(data)
            logger.info(f"读取到{len(df)}条历史数据")
            
            # 切换到技术分析标签页
            self.tab_widget.setCurrentIndex(1)
            
            # 绘制K线图
            self.plot_k_line(df, name, code)
            
        except Exception as e:
            logger.exception(f"处理股票双击事件失败: {e}")
            self.statusBar().showMessage(f"处理股票双击事件失败: {str(e)[:50]}...", 5000)
    
    def _setup_chart_labels(self, stock_name, stock_code):
        """
        设置图表标签
        
        Args:
            stock_name: 股票名称
            stock_code: 股票代码
        """
        from PySide6.QtWidgets import QLabel, QHBoxLayout, QWidget
        
        # 检查是否已经存在标题标签，如果存在则移除
        if hasattr(self, 'chart_title_label'):
            try:
                self.chart_title_label.deleteLater()
            except Exception as e:
                logger.warning(f"移除旧标题标签时发生错误: {e}")
        
        # 检查是否已经存在MA标签，如果存在则移除
        if hasattr(self, 'ma_values_label'):
            try:
                self.ma_values_label.deleteLater()
            except Exception as e:
                logger.warning(f"移除旧MA标签时发生错误: {e}")
        
        # 创建图表标题标签，放置在左上角
        self.chart_title_label = QLabel()
        self.chart_title_label.setStyleSheet("font-family: Consolas, monospace; padding: 5px; color: #C0C0C0; background-color: transparent; border: none;")
        # 获取当前周期，如果没有设置则默认为日线
        current_period = getattr(self, 'current_period', '日线')
        self.chart_title_label.setText(f"{stock_name}({stock_code}) {current_period}")
        self.chart_title_label.setWordWrap(False)
        
        # 创建MA值显示标签
        self.ma_values_label = QLabel()
        self.ma_values_label.setStyleSheet("font-family: Consolas, monospace; padding: 5px; color: #C0C0C0; background-color: transparent; border: none;")
        # 使用HTML设置初始文本和颜色，添加日期显示
        self.ma_values_label.setText("<font color='#C0C0C0'>日期: --</font>  <font color='white'>MA5: --</font>  <font color='cyan'>MA10: --</font>  <font color='red'>MA20: --</font>  <font color='#00FF00'>MA60: --</font>")
        # 确保不换行
        self.ma_values_label.setWordWrap(False)
        
        # 检查是否已经创建了图表容器
        if hasattr(self, 'chart_container') and hasattr(self, 'chart_layout'):
            # 移除旧的标题和MA标签相关组件
            for i in range(self.chart_layout.count()):
                item = self.chart_layout.itemAt(i)
                if isinstance(item, QHBoxLayout):
                    # 移除旧的水平布局
                    self.chart_layout.removeItem(item)
                    break
                elif hasattr(self, 'title_ma_container') and item.widget() == self.title_ma_container:
                    # 移除旧的容器
                    self.chart_layout.removeWidget(self.title_ma_container)
                    break
            
            # 创建一个统一的背景容器
            self.title_ma_container = QWidget()
            self.title_ma_container.setStyleSheet("background-color: #222222;")
            
            # 创建水平布局
            title_ma_layout = QHBoxLayout(self.title_ma_container)
            title_ma_layout.setSpacing(0)
            title_ma_layout.setContentsMargins(0, 0, 0, 0)
            
            # 添加功能菜单按钮到布局最左端
            self.menu_btn = self.create_indicator_menu_button(self.current_selected_window)
            title_ma_layout.addWidget(self.menu_btn)
            
            # 添加窗口标题标签
            window_title_label = QLabel("K线")
            window_title_label.setStyleSheet("background-color: transparent; color: #C0C0C0; font-size: 12px; padding: 0 5px;")
            title_ma_layout.addWidget(window_title_label)
            
            # 添加标签到布局
            title_ma_layout.addWidget(self.chart_title_label)
            title_ma_layout.addWidget(self.ma_values_label)
            title_ma_layout.addStretch(1)
            
            # 将容器添加到图表布局顶部
            self.chart_layout.insertWidget(0, self.title_ma_container)
            logger.info("已添加标题标签和MA值显示标签，在同一行显示")
    
    def _prepare_chart_data(self, df):
        """
        准备图表数据
        
        Args:
            df: 股票历史数据DataFrame
        
        Returns:
            tuple: (dates, opens, highs, lows, closes, x, ohlc_list)
        """
        import numpy as np
        
        # 准备K线图数据
        dates = df['date'].to_list()
        opens = df['open'].to_list()
        highs = df['high'].to_list()
        lows = df['low'].to_list()
        closes = df['close'].to_list()
        
        # 只显示指定数量的柱体
        bar_count = getattr(self, 'displayed_bar_count', 100)
        if len(dates) > bar_count:
            dates = dates[-bar_count:]
            opens = opens[-bar_count:]
            highs = highs[-bar_count:]
            lows = lows[-bar_count:]
            closes = closes[-bar_count:]
        
        # 创建x轴坐标（使用索引）
        x = np.arange(len(dates))
        
        # 创建K线图数据
        # K线图由OHLC数据组成：(x, open, high, low, close)
        ohlc = np.column_stack((x, opens, highs, lows, closes))
        
        # 转换为列表格式，适合自定义CandleStickItem
        ohlc_list = [tuple(row) for row in ohlc]
        
        return dates, opens, highs, lows, closes, x, ohlc_list
    
    def _setup_chart_axes(self, plot_widget, dates, highs, lows):
        """
        设置图表坐标轴
        
        Args:
            plot_widget: 图表组件
            dates: 日期列表
            highs: 最高价列表
            lows: 最低价列表
        """
        import numpy as np
        
        # 设置x轴刻度标签（显示日期）
        ax = plot_widget.getAxis('bottom')
        ax.setTicks([[(i, dates[i].strftime('%Y-%m-%d')) for i in range(0, len(dates), 10)]])
        
        # 设置Y轴范围，留出一定的边距
        y_min = np.min(lows) * 0.99
        y_max = np.max(highs) * 1.01
        plot_widget.setYRange(y_min, y_max)
    
    def _draw_indicator_curve(self, plot_widget, x, data, color, width=1, name=None):
        """
        绘制指标曲线的通用方法
        
        Args:
            plot_widget: 图表组件
            x: x轴数据
            data: 指标数据
            color: 曲线颜色
            width: 曲线宽度
            name: 曲线名称
        """
        import pyqtgraph as pg
        plot_widget.plot(x, data, pen=pg.mkPen(color, width=width), name=name)
    
    def _draw_indicator_histogram(self, plot_widget, x, data, positive_color='r', negative_color='g', width=0.35):
        """
        绘制指标柱状图的通用方法
        
        Args:
            plot_widget: 图表组件
            x: x轴数据
            data: 指标数据
            positive_color: 正值柱状图颜色
            negative_color: 负值柱状图颜色
            width: 柱状图宽度，默认与K线图柱体宽度保持一致
        """
        import pyqtgraph as pg
        import numpy as np
        
        # 创建颜色数组
        colors = []
        for val in data:
            if val >= 0:
                colors.append(positive_color)
            else:
                colors.append(negative_color)
        
        # 使用一个BarGraphItem绘制所有柱状图，而不是为每个数据点创建一个
        # 这样可以更好地控制宽度和间距
        bar_item = pg.BarGraphItem(
            x=np.array(x),
            height=np.array(data),
            width=width,
            brush=colors
        )
        plot_widget.addItem(bar_item)
    
    def _update_indicator_values(self, label_widget, indicator_name, data_df):
        """
        更新指标数值显示的通用方法
        
        Args:
            label_widget: 显示指标数值的标签组件
            indicator_name: 指标名称
            data_df: 包含指标数据的DataFrame
        """
        # 根据不同指标更新数值显示
        if indicator_name == "KDJ":
            # KDJ指标数值显示
            latest_k = data_df['k'].iloc[-1]
            latest_d = data_df['d'].iloc[-1]
            latest_j = data_df['j'].iloc[-1]
            text = f"<font color='white'>K: {latest_k:.2f}</font>  <font color='yellow'>D: {latest_d:.2f}</font>  <font color='magenta'>J: {latest_j:.2f}</font>"
        elif indicator_name == "MACD":
            # MACD指标数值显示
            latest_macd = data_df['macd'].iloc[-1]
            latest_macd_signal = data_df['macd_signal'].iloc[-1]
            latest_macd_hist = data_df['macd_hist'].iloc[-1]
            text = f"<font color='white'>DIF: {latest_macd:.2f}</font>  <font color='yellow'>DEA: {latest_macd_signal:.2f}</font>  <font color='magenta'>MACD: {latest_macd_hist:.2f}</font>"
        elif indicator_name == "MA":
            # MA指标数值显示
            latest_ma5 = data_df['ma5'].iloc[-1]
            latest_ma10 = data_df['ma10'].iloc[-1]
            latest_ma20 = data_df['ma20'].iloc[-1]
            latest_ma60 = data_df['ma60'].iloc[-1]
            text = f"<font color='white'>MA5: {latest_ma5:.2f}</font>  <font color='cyan'>MA10: {latest_ma10:.2f}</font>  <font color='red'>MA20: {latest_ma20:.2f}</font>  <font color='#00FF00'>MA60: {latest_ma60:.2f}</font>"
        else:
            # 默认情况，不显示指标数值
            text = ""
        
        # 更新标签文本
        if label_widget:
            label_widget.setText(text)
    
    def _setup_indicator_axis(self, plot_widget, x_data, y_data):
        """
        设置指标图表坐标轴的通用方法
        
        Args:
            plot_widget: 图表组件
            x_data: x轴数据
            y_data: y轴数据
        """
        import numpy as np
        
        # 设置X轴范围
        plot_widget.setXRange(0, len(x_data) - 1)
        
        # 设置Y轴范围，留出一定的边距
        y_min = np.min(y_data) * 1.2 if np.min(y_data) < 0 else 0
        y_max = np.max(y_data) * 1.2
        plot_widget.setYRange(y_min, y_max)
    
    def _draw_technical_indicator(self, plot_widget, indicator_name, x, data_df):
        """
        绘制技术指标的通用方法
        
        Args:
            plot_widget: 图表组件
            indicator_name: 指标名称
            x: x轴数据
            data_df: 包含指标数据的DataFrame（支持pandas和Polars）
        """
        import pyqtgraph as pg
        
        logger.info(f"绘制{indicator_name}指标")
        
        # 辅助函数：获取列的numpy数组，支持pandas和Polars DataFrame
        def get_col_values(df, col_name):
            if hasattr(df, 'to_pandas'):
                # Polars DataFrame
                return df[col_name].to_numpy()
            else:
                # pandas DataFrame
                return df[col_name].values
        
        # 辅助函数：获取所有数值列，支持pandas和Polars DataFrame
        def get_all_values(df):
            if hasattr(df, 'to_pandas'):
                # Polars DataFrame
                return df.to_numpy()
            else:
                # pandas DataFrame
                return df.values
        
        # 根据不同指标调用相应的绘制逻辑
        if indicator_name == "KDJ":
            # 绘制KDJ指标
            k_values = get_col_values(data_df, 'k')
            d_values = get_col_values(data_df, 'd')
            j_values = get_col_values(data_df, 'j')
            
            self._draw_indicator_curve(plot_widget, x, k_values, 'white', 1, 'K')
            self._draw_indicator_curve(plot_widget, x, d_values, 'yellow', 1, 'D')
            self._draw_indicator_curve(plot_widget, x, j_values, 'magenta', 1, 'J')
            self._setup_indicator_axis(plot_widget, x, np.column_stack((k_values, d_values, j_values)).flatten())
        elif indicator_name == "MACD":
            # 绘制MACD指标
            macd_values = get_col_values(data_df, 'macd')
            macd_signal_values = get_col_values(data_df, 'macd_signal')
            macd_hist_values = get_col_values(data_df, 'macd_hist')
            
            self._draw_indicator_curve(plot_widget, x, macd_values, 'blue', 1, 'DIF')
            self._draw_indicator_curve(plot_widget, x, macd_signal_values, 'red', 1, 'DEA')
            # MACD柱状图使用默认宽度，与K线图柱体宽度保持一致
            self._draw_indicator_histogram(plot_widget, x, macd_hist_values, 'r', 'g')
            self._setup_indicator_axis(plot_widget, x, np.column_stack((macd_values, macd_signal_values, macd_hist_values)).flatten())
        elif indicator_name == "RSI":
            # 绘制RSI指标
            rsi_values = get_col_values(data_df, 'rsi')
            self._draw_indicator_curve(plot_widget, x, rsi_values, 'white', 1, 'RSI')
            self._setup_indicator_axis(plot_widget, x, rsi_values)
        elif indicator_name == "VOL":
            # 绘制VOL指标
            volume_values = get_col_values(data_df, 'volume')
            # 成交量柱状图使用更窄的宽度，确保比K线图柱体窄
            self._draw_indicator_histogram(plot_widget, x, volume_values, 'r', 'g', width=0.35)
            self._setup_indicator_axis(plot_widget, x, volume_values)
        elif indicator_name == "MA":
            # 绘制MA指标
            ma5_values = get_col_values(data_df, 'ma5')
            ma10_values = get_col_values(data_df, 'ma10')
            ma20_values = get_col_values(data_df, 'ma20')
            ma60_values = get_col_values(data_df, 'ma60')
            
            self._draw_indicator_curve(plot_widget, x, ma5_values, 'white', 1, 'MA5')
            self._draw_indicator_curve(plot_widget, x, ma10_values, 'cyan', 1, 'MA10')
            self._draw_indicator_curve(plot_widget, x, ma20_values, 'red', 1, 'MA20')
            self._draw_indicator_curve(plot_widget, x, ma60_values, '#00FF00', 1, 'MA60')
            self._setup_indicator_axis(plot_widget, x, np.column_stack((ma5_values, ma10_values, ma20_values, ma60_values)).flatten())
        else:
            # 默认情况，尝试绘制所有数值列
            for column in data_df.columns:
                if column not in ['date', 'open', 'high', 'low', 'close', 'volume']:
                    self._draw_indicator_curve(plot_widget, x, get_col_values(data_df, column), 'white', 1, column)
            self._setup_indicator_axis(plot_widget, x, get_all_values(data_df).flatten())
    
    def plot_k_line(self, df, stock_name, stock_code):
        """
        使用pyqtgraph绘制K线图
        
        Args:
            df: 股票历史数据DataFrame
            stock_name: 股票名称
            stock_code: 股票代码
        """
        # 初始化变量，避免UnboundLocalError
        df_pd = None
        x = None
        
        try:
            import pyqtgraph as pg
            
            # 修复：在绘制K线图开始时重置十字线状态
            self.crosshair_enabled = False
            self.current_kline_index = -1
            import numpy as np

            from pyqtgraph import Point
            
            # 使用导入的CandleStickItem类
            
            # 清空图表
            self.tech_plot_widget.clear()
            self.volume_plot_widget.clear()
            
            # 创建标签，使用HTML格式设置不同颜色
            from PySide6.QtWidgets import QLabel, QHBoxLayout, QWidget
            
            # 检查是否已经存在标题标签，如果存在则移除
            if hasattr(self, 'chart_title_label'):
                try:
                    self.chart_title_label.deleteLater()
                except Exception as e:
                    logger.warning(f"移除旧标题标签时发生错误: {e}")
            
            # 检查是否已经存在MA标签，如果存在则移除
            if hasattr(self, 'ma_values_label'):
                try:
                    self.ma_values_label.deleteLater()
                except Exception as e:
                    logger.warning(f"移除旧MA标签时发生错误: {e}")
            
            # 创建图表标题标签，放置在左上角
            self.chart_title_label = QLabel()
            self.chart_title_label.setStyleSheet("font-family: Consolas, monospace; padding: 5px; color: #C0C0C0; background-color: transparent; border: none;")
            # 获取当前周期，如果没有设置则默认为日线
            current_period = getattr(self, 'current_period', '日线')
            self.chart_title_label.setText(f"{stock_name}({stock_code}) {current_period}")
            self.chart_title_label.setWordWrap(False)
            
            # 创建MA值显示标签
            self.ma_values_label = QLabel()
            self.ma_values_label.setStyleSheet("font-family: Consolas, monospace; padding: 5px; color: #C0C0C0; background-color: transparent; border: none;")
            # 使用HTML设置初始文本和颜色，添加日期显示
            self.ma_values_label.setText("<font color='#C0C0C0'>日期: --</font>  <font color='white'>MA5: --</font>  <font color='cyan'>MA10: --</font>  <font color='red'>MA20: --</font>  <font color='#00FF00'>MA60: --</font>")
            # 确保不换行
            self.ma_values_label.setWordWrap(False)
            
            # 检查是否已经创建了图表容器
            if hasattr(self, 'chart_container') and hasattr(self, 'chart_layout'):
                # 移除旧的标题和MA标签相关组件
                for i in range(self.chart_layout.count()):
                    item = self.chart_layout.itemAt(i)
                    if isinstance(item, QHBoxLayout):
                        # 移除旧的水平布局
                        self.chart_layout.removeItem(item)
                        break
                    elif hasattr(self, 'title_ma_container') and item.widget() == self.title_ma_container:
                        # 移除旧的容器
                        self.chart_layout.removeWidget(self.title_ma_container)
                        break
                
                # 创建一个统一的背景容器
                self.title_ma_container = QWidget()
                self.title_ma_container.setStyleSheet("background-color: #222222;")
                
                # 创建水平布局
                title_ma_layout = QHBoxLayout(self.title_ma_container)
                title_ma_layout.setSpacing(0)
                title_ma_layout.setContentsMargins(0, 0, 0, 0)
                
                # 添加功能菜单按钮到布局最左端
                self.menu_btn = self.create_indicator_menu_button(self.current_selected_window)
                title_ma_layout.addWidget(self.menu_btn)
                
                # 添加窗口标题标签
                window_title_label = QLabel("K线")
                window_title_label.setStyleSheet("background-color: transparent; color: #C0C0C0; font-size: 12px; padding: 0 5px;")
                title_ma_layout.addWidget(window_title_label)
                
                # 添加标签到布局
                title_ma_layout.addWidget(self.chart_title_label)
                title_ma_layout.addWidget(self.ma_values_label)
                title_ma_layout.addStretch(1)
                
                # 将容器添加到图表布局顶部
                self.chart_layout.insertWidget(0, self.title_ma_container)
                logger.info("已添加标题标签和MA值显示标签，在同一行显示")
            
            # 准备K线图数据
            dates = df['date'].to_list()
            opens = df['open'].to_list()
            highs = df['high'].to_list()
            lows = df['low'].to_list()
            closes = df['close'].to_list()
            
            # 只显示指定数量的柱体
            bar_count = getattr(self, 'displayed_bar_count', 100)
            if len(dates) > bar_count:
                dates = dates[-bar_count:]
                opens = opens[-bar_count:]
                highs = highs[-bar_count:]
                lows = lows[-bar_count:]
                closes = closes[-bar_count:]
            
            # 创建x轴坐标（使用索引）
            x = np.arange(len(dates))
            
            # 创建K线图数据
            # K线图由OHLC数据组成：(x, open, high, low, close)
            ohlc = np.column_stack((x, opens, highs, lows, closes))
            
            # 转换为列表格式，适合自定义CandleStickItem
            ohlc_list = [tuple(row) for row in ohlc]
            
            # 创建K线图项
            self.candle_plot_item = CandleStickItem(ohlc_list)
            
            # 添加K线图到图表
            self.tech_plot_widget.addItem(self.candle_plot_item)
            
            # 设置x轴刻度标签（显示日期）
            ax = self.tech_plot_widget.getAxis('bottom')
            ax.setTicks([[(i, dates[i].strftime('%Y-%m-%d')) for i in range(0, len(dates), 10)]])
            
            # 设置Y轴范围，留出一定的边距
            y_min = np.min(lows) * 0.99
            y_max = np.max(highs) * 1.01
            self.tech_plot_widget.setYRange(y_min, y_max)
            
            # 计算当前显示区域的最高、最低点及其位置
            current_high = np.max(highs)
            current_low = np.min(lows)
            high_index = np.argmax(highs)
            low_index = np.argmin(lows)
            
            # 清除之前的最高、最低点标注
            if hasattr(self, 'high_text_item'):
                self.tech_plot_widget.removeItem(self.high_text_item)
            if hasattr(self, 'low_text_item'):
                self.tech_plot_widget.removeItem(self.low_text_item)
            if hasattr(self, 'high_arrow_item'):
                self.tech_plot_widget.removeItem(self.high_arrow_item)
            if hasattr(self, 'low_arrow_item'):
                self.tech_plot_widget.removeItem(self.low_arrow_item)
            
            # 创建最高点标注，加上日期
            high_date = dates[high_index].strftime('%Y-%m-%d')
            self.high_text_item = pg.TextItem(f" {high_date} {current_high:.2f} ", color='w')
            self.high_text_item.setHtml(f'<div style="background-color: rgba(0, 0, 0, 0.8); padding: 3px; border: 1px solid #666; font-family: monospace; font-size: 10px;">{high_date} {current_high:.2f}</div>')
            self.tech_plot_widget.addItem(self.high_text_item)
            
            # 创建最高点箭头 - 简单三角形箭头
            self.high_arrow_item = pg.ArrowItem(
                pos=(high_index, current_high), 
                angle=-45, 
                brush=pg.mkBrush('w'), 
                pen=pg.mkPen('w', width=1), 
                tipAngle=30, 
                headLen=8, 
                headWidth=6,
                tailLen=0,  # 没有尾巴
                tailWidth=1
            )
            self.tech_plot_widget.addItem(self.high_arrow_item)
            
            # 创建最低点标注，加上日期
            low_date = dates[low_index].strftime('%Y-%m-%d')
            self.low_text_item = pg.TextItem(f" {low_date} {current_low:.2f} ", color='w')
            self.low_text_item.setHtml(f'<div style="background-color: rgba(0, 0, 0, 0.8); padding: 3px; border: 1px solid #666; font-family: monospace; font-size: 10px;">{low_date} {current_low:.2f}</div>')
            self.tech_plot_widget.addItem(self.low_text_item)
            
            # 创建最低点箭头 - 简单三角形箭头
            self.low_arrow_item = pg.ArrowItem(
                pos=(low_index, current_low), 
                angle=45, 
                brush=pg.mkBrush('w'), 
                pen=pg.mkPen('w', width=1), 
                tipAngle=30, 
                headLen=8, 
                headWidth=6,
                tailLen=0,  # 没有尾巴
                tailWidth=1
            )
            self.tech_plot_widget.addItem(self.low_arrow_item)
            
            # 定位标注位置（在对应柱图旁边）
            self.high_text_item.setPos(high_index + 0.5, current_high + (y_max - y_min) * 0.02)
            self.low_text_item.setPos(low_index + 0.5, current_low - (y_max - y_min) * 0.02)
            
            # 设置X轴范围，不使用autoRange，确保与成交量图一致
            self.tech_plot_widget.setXRange(0, len(dates) - 1)
            
            # 创建定时器，用于实现停留显示信息框
            self.info_timer = pg.QtCore.QTimer()
            self.info_timer.setSingleShot(True)
            self.info_timer.setInterval(200)  # 200毫秒
            self.info_timer.timeout.connect(self.show_info_box)
            
            # 禁用pyqtgraph的默认右键菜单
            logger.info("禁用pyqtgraph默认右键菜单")
            
            # 方法1: 禁用viewBox的右键菜单
            if hasattr(self.tech_plot_widget, 'getViewBox') and hasattr(self.volume_plot_widget, 'getViewBox'):
                # 获取K线图viewBox
                view_box = self.tech_plot_widget.getViewBox()
                view_box.setMenuEnabled(False)
                logger.info("已禁用K线图viewBox的右键菜单")
                
                # 获取成交量图viewBox
                volume_view_box = self.volume_plot_widget.getViewBox()
                volume_view_box.setMenuEnabled(False)
                logger.info("已禁用成交量图viewBox的右键菜单")
                
                # 连接K线图viewBox范围变化事件，将X轴范围同步到成交量图和KDJ图
                def on_kline_range_changed(view_range):
                    # 获取新的X轴范围
                    x_min, x_max = view_box.viewRange()[0]
                    
                    # 将X轴范围应用到成交量图
                    volume_view_box.setXRange(x_min, x_max, padding=0)
                    
                    # 将X轴范围应用到KDJ图
                    kdj_view_box = self.kdj_plot_widget.getViewBox()
                    kdj_view_box.setXRange(x_min, x_max, padding=0)
                    
                    logger.debug(f"从K线图同步X轴范围到成交量图和KDJ图: {x_min:.2f} - {x_max:.2f}")
                
                # 连接成交量图viewBox范围变化事件，将X轴范围同步到K线图和KDJ图
                def on_volume_range_changed(view_range):
                    # 获取新的X轴范围
                    x_min, x_max = volume_view_box.viewRange()[0]
                    
                    # 将X轴范围应用到K线图
                    view_box.setXRange(x_min, x_max, padding=0)
                    
                    # 将X轴范围应用到KDJ图
                    kdj_view_box = self.kdj_plot_widget.getViewBox()
                    kdj_view_box.setXRange(x_min, x_max, padding=0)
                    
                    logger.debug(f"从成交量图同步X轴范围到K线图和KDJ图: {x_min:.2f} - {x_max:.2f}")
                
                # 连接KDJ图viewBox范围变化事件，将X轴范围同步到K线图和成交量图
                def on_kdj_range_changed(view_range):
                    # 获取新的X轴范围
                    x_min, x_max = self.kdj_plot_widget.getViewBox().viewRange()[0]
                    
                    # 将X轴范围应用到K线图
                    view_box.setXRange(x_min, x_max, padding=0)
                    
                    # 将X轴范围应用到成交量图
                    volume_view_box.setXRange(x_min, x_max, padding=0)
                    
                    logger.debug(f"从KDJ图同步X轴范围到K线图和成交量图: {x_min:.2f} - {x_max:.2f}")
                
                # 连接三个viewBox的范围变化事件
                view_box.sigRangeChanged.connect(on_kline_range_changed)
                volume_view_box.sigRangeChanged.connect(on_volume_range_changed)
                self.kdj_plot_widget.getViewBox().sigRangeChanged.connect(on_kdj_range_changed)
                logger.info("已连接viewBox范围变化事件，确保三个图表双向同步缩放")
            
            # 方法2: 禁用所有子项的右键菜单
            for item in self.tech_plot_widget.items():
                if hasattr(item, 'setMenuEnabled'):
                    item.setMenuEnabled(False)
                    logger.info(f"已禁用{item}的右键菜单")
            
            # 方法3: 完全替换右键菜单事件处理
            def custom_context_menu(event):
                logger.info(f"自定义右键菜单被调用")
                
                # 创建自定义菜单
                menu = QMenu(self.tech_plot_widget)
                
                # 如果有选中的均线，添加修改指标参数选项
                if hasattr(self, 'selected_ma') and self.selected_ma:
                    modify_action = QAction(f"修改{self.selected_ma}指标参数", self)
                    modify_action.triggered.connect(lambda: self.on_modify_indicator(self.selected_ma))
                    menu.addAction(modify_action)
                else:
                    # 如果没有选中均线，添加提示信息
                    no_select_action = QAction("未选中均线，请先点击选中均线", self)
                    no_select_action.setEnabled(False)  # 禁用选项
                    menu.addAction(no_select_action)
                
                # 在鼠标位置显示菜单，确保使用QPoint类型
                qpoint = event.globalPos().toPoint()
                logger.info(f"在位置 {qpoint} 显示自定义菜单")
                menu.exec(qpoint)
                
                # 阻止事件传播，防止显示默认菜单
                event.accept()
            
            # 设置自定义右键菜单
            self.tech_plot_widget.contextMenuEvent = custom_context_menu
            logger.info("已设置自定义右键菜单")
            
            # 方法4: 连接全局上下文菜单事件
            self.tech_plot_widget.setContextMenuPolicy(Qt.CustomContextMenu)
            self.tech_plot_widget.customContextMenuRequested.connect(lambda pos: self.on_custom_context_menu(pos))
            logger.info("已连接customContextMenuRequested信号")
            
            # 方法5: 扩展已有的on_kline_clicked方法，添加取消均线选中状态功能
            # 不再创建新的on_plot_clicked函数，避免覆盖之前的连接
            logger.info("已使用现有的on_kline_clicked方法处理点击事件，包括取消均线选中状态")
            
            # 计算并绘制技术指标
            try:
                import pandas as pd
                import numpy as np
                import ta
                
                logger.info("开始计算技术指标")
                
                # 使用TechnicalAnalyzer类计算技术指标
                from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
                
                # 只取显示数量的数据
                if hasattr(df, 'tail'):
                    df_display = df.tail(bar_count)
                else:
                    df_display = df
                
                # 转换为Pandas DataFrame，确保使用正确的方法
                # 创建TechnicalAnalyzer实例，自动处理数据转换和类型检查
                logger.info("创建TechnicalAnalyzer实例")
                analyzer = TechnicalAnalyzer(df_display)
                
                # 一次性计算所有指标，利用并行计算提高效率
                logger.info("计算所有技术指标")
                analyzer.calculate_all_indicators(parallel=True)
                

                df_pd = analyzer.get_data()
                logger.info(f"使用TechnicalAnalyzer计算指标完成，DataFrame形状: {df_pd.shape}")
                
                # 确保数据索引正确
                
                # 确保数据索引正确
                x = np.arange(len(df_pd))
                
                # 绘制K线图
                logger.info("绘制K线图")
                self.draw_k_line_indicator(self.tech_plot_widget, df, dates, opens, highs, lows, closes, df_pd)
                
                # 绘制KDJ指标
                current_indicator = self.window_indicators[3]
                logger.info(f"绘制{current_indicator}指标")
                self.draw_indicator(self.kdj_plot_widget, current_indicator, x, df_pd)
                
                # 仅在第3窗口显示VOL指标时，进行额外的特殊处理
                if current_indicator == "VOL":
                    # 设置成交量图的x轴与K线图一致，实现柱体对齐
                    self.kdj_plot_widget.setXRange(0, len(df_pd) - 1)
                    
                    # 设置成交量图的X轴刻度标签，与K线图保持一致
                    kdj_ax = self.kdj_plot_widget.getAxis('bottom')
                    kdj_ax.setTicks([[(i, dates[i].strftime('%Y-%m-%d')) for i in range(0, len(dates), 10)]])
                    
                    # 仅在当前指标是VOL时设置成交量相关的Y轴范围和样式
                    # 获取成交量数据
                    volume_data = df_pd['volume'].values
                    volume_min = volume_data.min()
                    volume_max = volume_data.max()
                    
                    # 重置对数模式，默认使用线性刻度
                    self.kdj_plot_widget.setLogMode(y=False)
                    
                    # 计算成交量的统计信息
                    volume_mean = volume_data.mean()
                    volume_std = volume_data.std()
                    
                    # 计算合理的Y轴范围，进一步增加顶部空间
                    if volume_max > 0:
                        # 如果数据差异不大，使用基于均值和标准差的范围
                        if volume_std / volume_mean < 0.1:  # 标准差小于均值的10%，数据比较集中
                            # 扩大Y轴范围，显示更多细节，特别是顶部留出更多空间
                            y_min = max(0, volume_mean - volume_std * 2)
                            y_max = volume_mean + volume_std * 3.5  # 进一步增加顶部空间
                            self.kdj_plot_widget.setYRange(y_min, y_max)
                        else:
                            # 数据有一定差异，使用基于最小值和最大值的范围
                            y_range = volume_max - volume_min
                            y_min = max(0, volume_min - y_range * 0.1)
                            y_max = volume_max + y_range * 0.1  # 进一步增加顶部空间，从20%调整为30%
                            self.kdj_plot_widget.setYRange(y_min, y_max)
                    else:
                        # 成交量都是0，使用默认范围
                        self.kdj_plot_widget.setYRange(0, 100)
                    
                    # 禁用科学计数法，使用正常的数值显示
                    y_axis = self.kdj_plot_widget.getAxis('left')
                    y_axis.enableAutoSIPrefix(False)
                    y_axis.setStyle(tickTextOffset=20)
                    
                    # 设置X轴范围
                    self.kdj_plot_widget.setXRange(0, len(df_pd) - 1)
                # 重新添加十字线到KDJ图中
                if hasattr(self, 'kdj_vline') and hasattr(self, 'kdj_hline'):
                    self.kdj_plot_widget.addItem(self.kdj_vline, ignoreBounds=True)
                    self.kdj_plot_widget.addItem(self.kdj_hline, ignoreBounds=True)
                    # 恢复十字线的显示状态
                    if self.crosshair_enabled:
                        self.kdj_vline.show()
                        self.kdj_hline.show()
                    else:
                        self.kdj_vline.hide()
                        self.kdj_hline.hide()
                
                # 根据当前指标更新标签栏数值
                current_indicator = self.window_indicators[3]
                
                # 创建标签栏容器
                from PySide6.QtWidgets import QLabel, QHBoxLayout, QWidget
                
                # 检查是否已经存在标签，如果存在则移除
                if hasattr(self, 'kdj_values_label'):
                    try:
                        self.kdj_values_label.deleteLater()
                    except Exception as e:
                        logger.warning(f"移除旧KDJ标签时发生错误: {e}")
                
                # 检查是否已经存在标签栏容器，如果存在则先移除
                if hasattr(self, 'kdj_label_container'):
                    try:
                        # 移除旧的标签栏容器
                        if hasattr(self, 'kdj_container') and hasattr(self, 'kdj_container_layout'):
                            self.kdj_container_layout.removeWidget(self.kdj_label_container)
                        self.kdj_label_container.deleteLater()
                        delattr(self, 'kdj_label_container')
                    except Exception as e:
                        logger.warning(f"移除旧KDJ标签栏容器时发生错误: {e}")
                
                # 检查是否已经存在指标数值标签，如果存在则移除
                if hasattr(self, 'kdj_values_label'):
                    try:
                        self.kdj_values_label.deleteLater()
                        delattr(self, 'kdj_values_label')
                    except Exception as e:
                        logger.warning(f"移除旧KDJ数值标签时发生错误: {e}")
                
                # 创建标签栏容器
                self.kdj_label_container = QWidget()
                # 设置标签栏容器背景色与第1窗口一致
                self.kdj_label_container.setStyleSheet("background-color: #222222;")
                self.kdj_label_layout = QHBoxLayout(self.kdj_label_container)
                self.kdj_label_layout.setSpacing(0)
                self.kdj_label_layout.setContentsMargins(0, 0, 0, 0)
                
                # 添加功能菜单按钮到标签栏最左端
                self.kdj_menu_btn = self.create_indicator_menu_button(3)
                self.kdj_label_layout.addWidget(self.kdj_menu_btn)
                
                # 创建KDJ值显示标签
                self.kdj_values_label = QLabel()
                # 设置标签样式与第1窗口一致
                self.kdj_values_label.setStyleSheet("font-family: Consolas, monospace; padding: 5px; color: #C0C0C0; background-color: transparent; border: none;")
                # 确保不换行
                self.kdj_values_label.setWordWrap(False)
                
                # 根据当前指标更新标签文本
                if current_indicator == "KDJ":
                    # 获取最新的KDJ值
                    latest_k = df_pd['k'].iloc[-1]
                    latest_d = df_pd['d'].iloc[-1]
                    latest_j = df_pd['j'].iloc[-1]
                    # 更新标签文本
                    kdj_text = f"<font color='white'>K: {latest_k:.2f}</font>  <font color='yellow'>D: {latest_d:.2f}</font>  <font color='magenta'>J: {latest_j:.2f}</font>"
                    self.kdj_values_label.setText(kdj_text)
                elif current_indicator == "RSI":
                    # 获取最新的RSI值
                    latest_rsi = df_pd['rsi14'].iloc[-1]
                    # 更新标签文本
                    rsi_text = f"<font color='blue'>RSI14: {latest_rsi:.2f}</font>"
                    self.kdj_values_label.setText(rsi_text)
                elif current_indicator == "MACD":
                    # 获取最新的MACD值
                    latest_macd = df_pd['macd'].iloc[-1]
                    latest_macd_signal = df_pd['macd_signal'].iloc[-1]
                    latest_macd_hist = df_pd['macd_hist'].iloc[-1]
                    # 更新标签文本
                    macd_text = f"<font color='blue'>MACD: {latest_macd:.2f}</font>  <font color='red'>SIGNAL: {latest_macd_signal:.2f}</font>  <font color='#C0C0C0'>HIST: {latest_macd_hist:.2f}</font>"
                    self.kdj_values_label.setText(macd_text)
                elif current_indicator == "VOL":
                    # 获取最新的成交量值
                    latest_volume = df_pd['volume'].iloc[-1]
                    latest_vol_ma5 = df_pd['vol_ma5'].iloc[-1]
                    latest_vol_ma10 = df_pd['vol_ma10'].iloc[-1]
                    # 更新标签文本
                    vol_text = f"<font color='#C0C0C0'>VOLUME: {int(latest_volume):,}</font>  <font color='white'>MA5: {int(latest_vol_ma5):,}</font>  <font color='cyan'>MA10: {int(latest_vol_ma10):,}</font>"
                    self.kdj_values_label.setText(vol_text)
                else:
                    # 默认绘制KDJ指标，显示KDJ数值
                    latest_k = df_pd['k'].iloc[-1]
                    latest_d = df_pd['d'].iloc[-1]
                    latest_j = df_pd['j'].iloc[-1]
                    # 更新标签文本
                    kdj_text = f"<font color='white'>K: {latest_k:.2f}</font>  <font color='yellow'>D: {latest_d:.2f}</font>  <font color='magenta'>J: {latest_j:.2f}</font>"
                    self.kdj_values_label.setText(kdj_text)
                
                # 检查是否已经创建了KDJ容器和布局
                if hasattr(self, 'kdj_container') and hasattr(self, 'kdj_container_layout'):
                    # 首先移除可能存在的旧标签容器和标签，然后添加新标签
                    try:
                        self.kdj_container_layout.removeWidget(self.kdj_label_container)
                    except Exception:
                        pass
                    
                    try:
                        self.kdj_container_layout.removeWidget(self.kdj_values_label)
                    except Exception:
                        pass
                    
                    # 首先确保KDJ图已经在布局中，如果不在则添加
                    try:
                        self.kdj_container_layout.removeWidget(self.kdj_plot_widget)
                    except Exception:
                        pass
                    
                    # 将标签添加到水平布局中
                    self.kdj_label_layout.addWidget(self.kdj_values_label)
                    # 添加KDJ标签栏容器到容器布局顶部
                    self.kdj_container_layout.addWidget(self.kdj_label_container)
                    # 添加KDJ图到容器布局
                    self.kdj_container_layout.addWidget(self.kdj_plot_widget)
                    
                    logger.info("已添加KDJ值显示标签")
                
                # 保存指标数据，用于鼠标移动时更新指标数值
                self.current_kdj_data = {
                    'k': df_pd['k'].values.tolist(),
                    'd': df_pd['d'].values.tolist(),
                    'j': df_pd['j'].values.tolist()
                }
                self.current_rsi_data = {
                    'rsi': df_pd['rsi14'].values.tolist()
                }
                self.current_macd_data = {
                    'macd': df_pd['macd'].values.tolist(),
                    'macd_signal': df_pd['macd_signal'].values.tolist(),
                    'macd_hist': df_pd['macd_hist'].values.tolist()
                }
                # 保存成交量数据
                self.current_volume_data = {
                    'volume': df_pd['volume'].values.tolist(),
                    'vol_ma5': df_pd['vol_ma5'].values.tolist(),
                    'vol_ma10': df_pd['vol_ma10'].values.tolist()
                }
                
                # 仅在第3窗口显示VOL指标时，应用与第2窗口相同的绘制逻辑
                if current_indicator == "VOL":
                    # 设置X轴刻度标签，与K线图保持一致
                    kdj_ax = self.kdj_plot_widget.getAxis('bottom')
                    kdj_ax.setTicks([[(i, dates[i].strftime('%Y-%m-%d')) for i in range(0, len(dates), 10)]])
                    
                    # 设置X轴范围，与K线图一致
                    self.kdj_plot_widget.setXRange(0, len(dates) - 1)
                    
                    # 设置Y轴范围，与第2窗口的处理逻辑完全相同
                    # 确保使用真实的成交量数值，而不是相对比例
                    volumes = df_pd['volume'].values
                    if len(volumes) > 0:
                        # 计算合理的Y轴范围，进一步增加顶部空间
                        volume_mean = volumes.mean()
                        volume_std = volumes.std()
                        if volume_std / volume_mean < 0.1:  # 标准差小于均值的10%，数据比较集中
                            # 扩大Y轴范围，显示更多细节，特别是顶部留出更多空间
                            y_min = max(0, volume_mean - volume_std * 2)
                            y_max = volume_mean + volume_std * 3.5  # 进一步增加顶部空间
                            self.kdj_plot_widget.setYRange(y_min, y_max)
                        else:
                            # 数据有一定差异，使用基于最小值和最大值的范围
                            y_range = volumes.max() - volumes.min()
                            y_min = max(0, volumes.min() - y_range * 0.1)
                            y_max = volumes.max() + y_range * 0.1  # 进一步增加顶部空间，从20%调整为30%
                            self.kdj_plot_widget.setYRange(y_min, y_max)
                    else:
                        # 成交量都是0，使用默认范围
                        self.kdj_plot_widget.setYRange(0, 100)
                    
                    # 禁用科学计数法，使用正常的数值显示
                    y_axis = self.kdj_plot_widget.getAxis('left')
                    y_axis.enableAutoSIPrefix(False)
                    y_axis.setStyle(tickTextOffset=20)
                    
                    # 重置缩放比例，确保显示真实数值
                    y_axis.setScale(1.0)
                    
                    # 确保X轴范围和刻度与K线图完全一致，实现柱体对齐
                    self.tech_plot_widget.setXRange(0, len(dates) - 1)
                    self.kdj_plot_widget.setXRange(0, len(dates) - 1)
                
            except Exception as e:
                logger.exception(f"计算或绘制技术指标时发生错误: {e}")
            
            # 保存当前鼠标位置和K线索引
            self.current_mouse_pos = None
            self.current_kline_index = -1
            # 绘制第二个窗口的指标图
            try:
                # 检查df_pd是否已经定义，避免UnboundLocalError
                if df_pd is None:
                    logger.warning("指标数据未计算，跳过第二个窗口指标绘制")
                    return
                    
                # 确保x变量已经定义
                if x is None:
                    x = np.arange(len(df_pd))
                    logger.info(f"重新计算x轴坐标，长度: {len(x)}")
                    
                # 获取当前窗口指标
                current_indicator = self.window_indicators[2]
                logger.info(f"绘制{current_indicator}指标")
                
                # 直接使用之前已经计算好的数据，不需要重复处理
                # 只取显示数量的数据
                # if hasattr(df, 'tail'):
                #     df_display = df.tail(bar_count)
                # else:
                #     df_display = df
                # 
                # 转换为Pandas DataFrame
                # df_pd = None
                # if hasattr(df_display, 'to_pandas'):
                #     df_pd = df_display.to_pandas()
                # elif isinstance(df_display, pd.DataFrame):
                #     df_pd = df_display
                # else:
                #     df_pd = pd.DataFrame(df_display)
                # 
                # 确保volume列存在且为数值类型
                # df_pd['volume'] = pd.to_numeric(df_pd['volume'], errors='coerce')
                # df_pd['close'] = pd.to_numeric(df_pd['close'], errors='coerce')
                # df_pd['open'] = pd.to_numeric(df_pd['open'], errors='coerce')
                # 
                # 准备x轴坐标
                # x = np.arange(len(df_pd))
                
                # 直接使用之前已经计算好的指标数据，不需要重复计算
                # 计算成交量5日均线和10日均线（无论什么指标，都先计算好）
                # df_pd['vol_ma5'] = ta.trend.sma_indicator(df_pd['volume'], window=5, fillna=True)
                # df_pd['vol_ma10'] = ta.trend.sma_indicator(df_pd['volume'], window=10, fillna=True)
                
                # 计算所有可能的技术指标，确保切换指标时不会出错
                # if 'close' in df_pd.columns:
                #     # 计算MACD指标
                #     df_pd['macd'] = ta.trend.macd(df_pd['close'], fillna=True)
                #     df_pd['macd_signal'] = ta.trend.macd_signal(df_pd['close'], fillna=True)
                #     df_pd['macd_hist'] = ta.trend.macd_diff(df_pd['close'], fillna=True)
                #     
                #     # 计算RSI指标
                #     df_pd['rsi14'] = ta.momentum.rsi(df_pd['close'], window=14, fillna=True)
                #     
                #     # 计算KDJ指标
                #     if 'high' in df_pd.columns and 'low' in df_pd.columns:
                #         df_pd['k'] = ta.momentum.stoch(df_pd['high'], df_pd['low'], df_pd['close'], window=14, fillna=True)
                #         df_pd['d'] = ta.momentum.stoch_signal(df_pd['high'], df_pd['low'], df_pd['close'], window=14, fillna=True)
                #         df_pd['j'] = 3 * df_pd['k'] - 2 * df_pd['d']
                
                # 绘制指标
                self.draw_indicator(self.volume_plot_widget, current_indicator, x, df_pd)

                # 添加数值显示
                if len(df_pd) > 0:
                    if current_indicator == "VOL":
                        # 获取最新的成交量数据
                        latest_volume = df_pd.iloc[-1]['volume']
                        latest_vol_ma5 = df_pd.iloc[-1]['vol_ma5']
                        latest_vol_ma10 = df_pd.iloc[-1]['vol_ma10']
                        
                        # 保存成交量数据和成交量均线数据，用于鼠标移动时更新标题
                        self.current_volume_data = {
                            'volume': df_pd['volume'].values.tolist(),
                            'vol_ma5': df_pd['vol_ma5'].values.tolist(),
                            'vol_ma10': df_pd['vol_ma10'].values.tolist()
                        }
                    
                    # 检查是否已经存在标签，如果存在则移除
                    if hasattr(self, 'volume_values_label'):
                        try:
                            self.volume_values_label.deleteLater()
                        except Exception as e:
                            logger.warning(f"移除旧标签时发生错误: {e}")
                    
                    # 检查是否已经存在标签栏容器，如果存在则先移除
                    if hasattr(self, 'volume_label_container'):
                        try:
                            # 移除旧的标签栏容器
                            if hasattr(self, 'volume_container') and hasattr(self, 'volume_container_layout'):
                                self.volume_container_layout.removeWidget(self.volume_label_container)
                            self.volume_label_container.deleteLater()
                            delattr(self, 'volume_label_container')
                        except Exception as e:
                            logger.warning(f"移除旧成交量标签栏容器时发生错误: {e}")
                    
                    # 检查是否已经存在指标数值标签，如果存在则移除
                    if hasattr(self, 'volume_values_label'):
                        try:
                            self.volume_values_label.deleteLater()
                            delattr(self, 'volume_values_label')
                        except Exception as e:
                            logger.warning(f"移除旧成交量数值标签时发生错误: {e}")
                    
                    # 创建标签栏容器
                    from PySide6.QtWidgets import QLabel, QHBoxLayout, QWidget
                    self.volume_label_container = QWidget()
                    # 设置标签栏容器背景色与第1窗口一致
                    self.volume_label_container.setStyleSheet("background-color: #222222;")
                    self.volume_label_layout = QHBoxLayout(self.volume_label_container)
                    self.volume_label_layout.setSpacing(0)
                    self.volume_label_layout.setContentsMargins(0, 0, 0, 0)
                    
                    # 添加功能菜单按钮到标签栏最左端
                    self.volume_menu_btn = self.create_indicator_menu_button(2)
                    self.volume_label_layout.addWidget(self.volume_menu_btn)
                    
                    # 创建标签，使用与K线图均线标签相同的样式
                    self.volume_values_label = QLabel()
                    # 设置标签样式与第1窗口一致
                    self.volume_values_label.setStyleSheet("font-family: Consolas, monospace; padding: 5px; color: #C0C0C0; background-color: transparent; border: none;")
                    # 确保不换行
                    self.volume_values_label.setWordWrap(False)
                    
                    # 根据不同指标设置标签文本
                    if current_indicator == "VOL":
                        # 使用HTML设置初始文本和颜色，与K线图均线标签样式一致
                        self.volume_values_label.setText(f"<font color='#C0C0C0'>VOLUME: {int(latest_volume):,}</font>  <font color='white'>MA5: {int(latest_vol_ma5):,}</font>  <font color='cyan'>MA10: {int(latest_vol_ma10):,}</font>")
                    elif current_indicator == "MACD":
                        # 添加MACD数值显示
                        latest_macd = df_pd['macd'].iloc[-1]
                        latest_macd_signal = df_pd['macd_signal'].iloc[-1]
                        latest_macd_hist = df_pd['macd_hist'].iloc[-1]
                        
                        self.volume_values_label.setText(f"<font color='blue'>MACD: {latest_macd:.2f}</font>  <font color='red'>SIGNAL: {latest_macd_signal:.2f}</font>  <font color='#C0C0C0'>HIST: {latest_macd_hist:.2f}</font>")
                    elif current_indicator == "RSI":
                        # 添加RSI数值显示
                        latest_rsi = df_pd['rsi14'].iloc[-1]
                        
                        self.volume_values_label.setText(f"<font color='blue'>RSI14: {latest_rsi:.2f}</font>")
                    elif current_indicator == "KDJ":
                        # 添加KDJ数值显示
                        latest_k = df_pd['k'].iloc[-1]
                        latest_d = df_pd['d'].iloc[-1]
                        latest_j = df_pd['j'].iloc[-1]
                        
                        self.volume_values_label.setText(f"<font color='white'>K: {latest_k:.2f}</font>  <font color='yellow'>D: {latest_d:.2f}</font>  <font color='magenta'>J: {latest_j:.2f}</font>")
                    
                    # 检查是否已经创建了成交量容器和布局
                    if hasattr(self, 'volume_container') and hasattr(self, 'volume_container_layout'):
                        # 首先移除可能存在的旧标签容器和标签，然后添加新标签
                        try:
                            self.volume_container_layout.removeWidget(self.volume_label_container)
                        except Exception:
                            pass
                        
                        try:
                            self.volume_container_layout.removeWidget(self.volume_values_label)
                        except Exception:
                            pass
                        
                        # 将成交量图添加到容器布局中
                        # 首先移除可能存在的旧成交量图，然后添加新的成交量图
                        try:
                            self.volume_container_layout.removeWidget(self.volume_plot_widget)
                        except Exception:
                            pass
                        
                        # 将标签添加到水平布局中
                        self.volume_label_layout.addWidget(self.volume_values_label)
                        # 在成交量图上方添加成交量标签容器
                        self.volume_container_layout.addWidget(self.volume_label_container)
                        self.volume_container_layout.addWidget(self.volume_plot_widget)
                    
                    # 检查窗口数量，如果是1个窗口模式，隐藏成交量标签
                    if hasattr(self, 'current_window_count') and self.current_window_count == 1:
                        self.volume_values_label.hide()
                        self.volume_plot_widget.hide()
                    else:
                        self.volume_values_label.show()
                        self.volume_plot_widget.show()
                        
                        logger.info("已添加成交量值显示标签")
                
                # 设置成交量图的x轴与K线图一致，实现柱体对齐
                self.volume_plot_widget.setXRange(0, len(df_pd) - 1)
                
                # 设置成交量图的X轴刻度标签，与K线图保持一致
                volume_ax = self.volume_plot_widget.getAxis('bottom')
                volume_ax.setTicks([[(i, dates[i].strftime('%Y-%m-%d')) for i in range(0, len(dates), 10)]])
                
                # 确保两个图的X轴范围和刻度完全一致，实现柱体对齐
                self.tech_plot_widget.setXRange(0, len(dates) - 1)
                self.volume_plot_widget.setXRange(0, len(dates) - 1)
                
                # 仅在当前指标是VOL时设置成交量相关的Y轴范围和样式
                if current_indicator == "VOL":
                    # 获取成交量数据
                    volume_data = df_pd['volume'].values
                    volume_min = volume_data.min()
                    volume_max = volume_data.max()
                    
                    # 重置对数模式，默认使用线性刻度
                    self.volume_plot_widget.setLogMode(y=False)
                    
                    # 计算成交量的统计信息
                    volume_mean = volume_data.mean()
                    volume_std = volume_data.std()
                    
                    # 计算合理的Y轴范围，进一步增加顶部空间
                    if volume_max > 0:
                        # 如果数据差异不大，使用基于均值和标准差的范围
                        if volume_std / volume_mean < 0.1:  # 标准差小于均值的10%，数据比较集中
                            # 扩大Y轴范围，显示更多细节，特别是顶部留出更多空间
                            y_min = max(0, volume_mean - volume_std * 2)
                            y_max = volume_mean + volume_std * 3.5  # 进一步增加顶部空间
                            self.volume_plot_widget.setYRange(y_min, y_max)
                        else:
                            # 数据有一定差异，使用基于最小值和最大值的范围
                            y_range = volume_max - volume_min
                            y_min = max(0, volume_min - y_range * 0.1)
                            y_max = volume_max + y_range * 0.1  # 进一步增加顶部空间，从20%调整为30%
                            self.volume_plot_widget.setYRange(y_min, y_max)
                    else:
                        # 成交量都是0，使用默认范围
                        self.volume_plot_widget.setYRange(0, 100)
                
                # 禁用科学计数法，使用正常的数值显示
                y_axis = self.volume_plot_widget.getAxis('left')
                y_axis.enableAutoSIPrefix(False)
                y_axis.setStyle(tickTextOffset=20)
                
                # 设置X轴范围
                self.volume_plot_widget.setXRange(0, len(df_pd) - 1)
                
                logger.info(f"成功绘制{stock_name}({stock_code})的{current_indicator}图")
                
            except Exception as e:
                logger.exception(f"绘制{current_indicator}图失败: {e}")
            
            # 保存当前显示的个股信息
            self.current_stock_data = df
            self.current_stock_name = stock_name
            self.current_stock_code = stock_code
            
            # 移除旧的十字线对象，避免多个十字线对象导致的问题
            try:
                # 移除K线图中的旧十字线
                if hasattr(self, 'vline'):
                    self.tech_plot_widget.removeItem(self.vline)
                if hasattr(self, 'hline'):
                    self.tech_plot_widget.removeItem(self.hline)
                # 移除成交量图中的旧十字线
                if hasattr(self, 'volume_vline'):
                    self.volume_plot_widget.removeItem(self.volume_vline)
                if hasattr(self, 'volume_hline'):
                    self.volume_plot_widget.removeItem(self.volume_hline)
                # 移除KDJ图中的旧十字线
                if hasattr(self, 'kdj_vline'):
                    self.kdj_plot_widget.removeItem(self.kdj_vline)
                if hasattr(self, 'kdj_hline'):
                    self.kdj_plot_widget.removeItem(self.kdj_hline)
                logger.info("已移除旧的十字线对象")
            except Exception as e:
                logger.debug(f"移除旧的十字线对象时发生错误: {e}")
            
            # 添加十字线
            self.vline = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen('w', width=1, style=Qt.DotLine))
            self.hline = pg.InfiniteLine(angle=0, movable=False, pen=pg.mkPen('w', width=1, style=Qt.DotLine))
            self.volume_vline = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen('w', width=1, style=Qt.DotLine))
            self.volume_hline = pg.InfiniteLine(angle=0, movable=False, pen=pg.mkPen('w', width=1, style=Qt.DotLine))
            # 为KDJ图添加十字线
            self.kdj_vline = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen('w', width=1, style=Qt.DotLine))
            self.kdj_hline = pg.InfiniteLine(angle=0, movable=False, pen=pg.mkPen('w', width=1, style=Qt.DotLine))
            
            # 添加十字线到K线图
            self.tech_plot_widget.addItem(self.vline, ignoreBounds=True)
            self.tech_plot_widget.addItem(self.hline, ignoreBounds=True)
            
            # 添加十字线到成交量图
            self.volume_plot_widget.addItem(self.volume_vline, ignoreBounds=True)
            self.volume_plot_widget.addItem(self.volume_hline, ignoreBounds=True)
            
            # 添加十字线到KDJ图
            self.kdj_plot_widget.addItem(self.kdj_vline, ignoreBounds=True)
            self.kdj_plot_widget.addItem(self.kdj_hline, ignoreBounds=True)
            
            logger.info("已添加新的十字线对象")
            
            # 初始隐藏十字线
            self.vline.hide()
            self.hline.hide()
            self.volume_vline.hide()
            self.volume_hline.hide()
            # 初始隐藏KDJ图十字线
            self.kdj_vline.hide()
            self.kdj_hline.hide()
            
            # 创建信息文本项
            self.info_text = pg.TextItem(anchor=(0, 1))  # 锚点在左下角，确保信息框左下角在指定位置
            self.info_text.setColor(pg.mkColor('w'))
            self.info_text.setHtml('<div style="background-color: rgba(0, 0, 0, 0.8); padding: 5px; border: 1px solid #666; font-family: monospace;"></div>')
            self.tech_plot_widget.addItem(self.info_text)
            self.info_text.hide()
            
            # 保存当前K线数据，用于双击显示信息
            self.current_kline_data = {
                'dates': dates,
                'opens': opens,
                'highs': highs,
                'lows': lows,
                'closes': closes,
                'ohlc_list': ohlc_list
            }
            
            # 断开之前的所有事件连接，避免多次连接导致的问题
            try:
                # 断开鼠标移动事件的所有连接
                # 使用disconnect()不带参数会断开所有连接，但在PyQt/PySide中如果没有连接会警告
                # 这里使用try-except来捕获可能的警告
                if hasattr(self.tech_plot_widget.scene().sigMouseMoved, 'disconnect'):
                    self.tech_plot_widget.scene().sigMouseMoved.disconnect()
            except Exception:
                pass
            
            try:
                if hasattr(self.volume_plot_widget.scene().sigMouseMoved, 'disconnect'):
                    self.volume_plot_widget.scene().sigMouseMoved.disconnect()
            except Exception:
                pass
            
            try:
                if hasattr(self.kdj_plot_widget.scene().sigMouseMoved, 'disconnect'):
                    self.kdj_plot_widget.scene().sigMouseMoved.disconnect()
                logger.info("已断开鼠标移动事件的所有连接")
            except Exception as e:
                logger.debug(f"断开鼠标移动事件连接时发生错误: {e}")
            
            try:
                # 断开鼠标点击事件的所有连接
                if hasattr(self.tech_plot_widget.scene().sigMouseClicked, 'disconnect'):
                    self.tech_plot_widget.scene().sigMouseClicked.disconnect()
                logger.info("已断开鼠标点击事件的所有连接")
            except Exception as e:
                logger.debug(f"断开鼠标点击事件连接时发生错误: {e}")
            
            # 连接鼠标移动事件，实现十字线跟随和指标值更新
            self.tech_plot_widget.scene().sigMouseMoved.connect(lambda pos: self.on_kline_mouse_moved(pos, dates, opens, highs, lows, closes))
            self.volume_plot_widget.scene().sigMouseMoved.connect(lambda pos: self.on_kline_mouse_moved(pos, dates, opens, highs, lows, closes))
            self.kdj_plot_widget.scene().sigMouseMoved.connect(lambda pos: self.on_kline_mouse_moved(pos, dates, opens, highs, lows, closes))
            
            # 连接鼠标点击事件，处理左键和右键点击
            self.tech_plot_widget.scene().sigMouseClicked.connect(lambda event: self.on_kline_clicked(event, dates, opens, highs, lows, closes))
            # 连接成交量图鼠标点击事件
            self.volume_plot_widget.scene().sigMouseClicked.connect(lambda event: self.on_kline_clicked(event, dates, opens, highs, lows, closes))
            # 连接KDJ图鼠标点击事件
            self.kdj_plot_widget.scene().sigMouseClicked.connect(lambda event: self.on_kline_clicked(event, dates, opens, highs, lows, closes))
            
            # 连接鼠标离开视图事件，通过监控鼠标位置实现
            self.tech_plot_widget.viewport().setMouseTracking(True)
            
            logger.info(f"成功绘制{stock_name}({stock_code})的K线图")
            self.statusBar().showMessage(f"成功绘制{stock_name}({stock_code})的K线图", 3000)
            
            # 使用当前的窗口数量，不强制重置为3个
            self.on_window_count_changed(self.current_window_count, True)
            
        except Exception as e:
            logger.exception(f"绘制K线图失败: {e}")
            self.statusBar().showMessage(f"绘制K线图失败: {str(e)[:50]}...", 5000)
    
    def on_kline_double_clicked(self, event, dates, opens, highs, lows, closes):
        """
        处理K线图双击事件，切换十字线和信息框显示状态
        
        Args:
            event: 鼠标点击事件
            dates: 日期列表
            opens: 开盘价列表
            highs: 最高价列表
            lows: 最低价列表
            closes: 收盘价列表
        """
        # 检查是否是双击事件
        if event.double():  # 检查是否是双击
            # 切换十字线和信息框显示状态
            self.crosshair_enabled = not self.crosshair_enabled
            
            if self.crosshair_enabled:
                logger.info("双击K线图，启用十字线和信息框")
                # 如果当前有K线数据，显示十字线
                if self.current_kline_data:
                    # 获取当前鼠标位置对应的K线索引
                    pos = event.pos()
                    view_box = self.tech_plot_widget.getViewBox()
                    view_pos = view_box.mapSceneToView(pos)
                    x_val = view_pos.x()
                    index = int(round(x_val))
                    
                    # 确保索引在有效范围内
                    index = max(0, min(len(dates) - 1, index))
                    
                    # 保存当前索引和鼠标位置
                    self.current_kline_index = index
                    self.current_mouse_pos = pos
                    
                    # 保存当前K线数据
                    self.current_kline_data = {
                        'dates': dates,
                        'opens': opens,
                        'highs': highs,
                        'lows': lows,
                        'closes': closes,
                        'index': index
                    }
                    
                    # 显示K线图十字线
                    self.vline.setValue(index)
                    self.hline.setValue(view_pos.y())
                    self.vline.show()
                    self.hline.show()
                    
                    # 显示成交量图十字线
                    self.volume_vline.setValue(index)
                    volume_view_box = self.volume_plot_widget.getViewBox()
                    volume_pos = volume_view_box.mapSceneToView(pos)
                    self.volume_hline.setValue(volume_pos.y())
                    self.volume_vline.show()
                    self.volume_hline.show()
                    
                    # 显示KDJ图十字线
                    self.kdj_vline.setValue(index)
                    kdj_view_box = self.kdj_plot_widget.getViewBox()
                    kdj_pos = kdj_view_box.mapSceneToView(pos)
                    self.kdj_hline.setValue(kdj_pos.y())
                    self.kdj_vline.show()
                    self.kdj_hline.show()
                    
                    # 直接显示信息框，不需要等待鼠标移动
                    self.show_info_box()
            else:
                logger.info("双击K线图，禁用十字线和信息框")
                # 隐藏K线图十字线
                self.vline.hide()
                self.hline.hide()
                
                # 隐藏成交量图十字线
                self.volume_vline.hide()
                self.volume_hline.hide()
                
                # 隐藏KDJ图十字线
                self.kdj_vline.hide()
                self.kdj_hline.hide()
                
                # 隐藏信息框
                if self.info_text is not None:
                    self.info_text.hide()
    
    def on_kline_clicked(self, event, dates, opens, highs, lows, closes):
        """
        处理K线图点击事件，区分左键和右键点击
        
        Args:
            event: 鼠标点击事件
            dates: 日期列表
            opens: 开盘价列表
            highs: 最高价列表
            lows: 最低价列表
            closes: 收盘价列表
        """
        # 检查是否是双击事件
        if event.double():  # 检查是否是双击
            self.on_kline_double_clicked(event, dates, opens, highs, lows, closes)
        else:
            # 单击事件，调用均线点击处理函数
            self.on_ma_clicked(event)
    
    def on_ma_clicked(self, event):
        """
        处理均线点击事件，在选中的均线上显示白点标注
        
        Args:
            event: 鼠标点击事件
        """
        try:
            import pyqtgraph as pg
            import pandas as pd
            import numpy as np
            from pyqtgraph import Point
            
            # 获取点击位置
            pos = event.scenePos()
            view_box = self.tech_plot_widget.getViewBox()
            view_pos = view_box.mapSceneToView(pos)
            x_val = view_pos.x()
            y_val = view_pos.y()
            
            # 找到最接近的K线索引
            index = int(round(x_val))
            
            # 检测点击位置是否在某个均线上
            clicked_ma = None
            min_distance = float('inf')
            
            # 定义点击容忍度（Y轴方向的容忍度）
            tolerance = 0.02  # 2%的价格容忍度
            
            # 获取当前价格范围，用于计算相对容忍度
            y_range = self.tech_plot_widget.viewRange()[1]
            y_min, y_max = y_range
            price_tolerance = (y_max - y_min) * tolerance
            
            # 确保moving_averages属性存在
            if hasattr(self, 'moving_averages'):
                # 遍历所有均线，检查点击位置是否在均线上
                for ma_name, ma_info in self.moving_averages.items():
                    x_data, y_data = ma_info['data']
                    if 0 <= index < len(x_data):
                        # 获取该位置的均线值
                        ma_value = y_data[index]
                        
                        # 计算点击位置与均线的距离
                        distance = abs(y_val - ma_value)
                        
                        # 如果距离小于容忍度，认为点击了该均线
                        if distance < price_tolerance and distance < min_distance:
                            min_distance = distance
                            clicked_ma = ma_name
            
            # 如果点击了均线
            if clicked_ma:
                logger.info(f"点击了{clicked_ma}")
                
                # 确保ma_points属性存在
                if not hasattr(self, 'ma_points'):
                    self.ma_points = []
                
                # 清除之前的标注点
                for point_item in self.ma_points:
                    self.tech_plot_widget.removeItem(point_item)
                self.ma_points.clear()
                
                # 确保moving_averages属性存在
                if hasattr(self, 'moving_averages'):
                    # 绘制新的标注点
                    ma_info = self.moving_averages.get(clicked_ma)
                    if ma_info:
                        x_data, y_data = ma_info['data']
                        
                        # 在均线上每隔几个点绘制一个白点
                        step = max(1, len(x_data) // 20)  # 最多绘制20个点
                        for i in range(0, len(x_data), step):
                            if not pd.isna(y_data[i]):
                                # 创建白点标注
                                point = pg.ScatterPlotItem([x_data[i]], [y_data[i]], size=6, pen=pg.mkPen('w', width=1), brush=pg.mkBrush('w'))
                                self.tech_plot_widget.addItem(point)
                                self.ma_points.append(point)
                
                # 更新选中的均线
                self.selected_ma = clicked_ma
            else:
                # 点击位置不在均线上，取消选中状态
                logger.info(f"点击位置不在均线上，取消选中状态")
                
                # 确保ma_points属性存在
                if hasattr(self, 'ma_points'):
                    # 清除之前的标注点
                    for point_item in self.ma_points:
                        self.tech_plot_widget.removeItem(point_item)
                    self.ma_points.clear()
                
                # 重置选中状态
                self.selected_ma = None
                logger.info(f"已取消均线选中状态")
                
                # 检查是否是右键点击
                logger.info(f"点击按钮: {event.button()}, Qt.RightButton: {Qt.RightButton}")
                if event.button() == Qt.RightButton:
                    logger.info(f"检测到右键点击，创建自定义菜单")
                    
                    # 创建右键菜单
                    menu = QMenu(self)
                    
                    # 如果点击了均线，添加修改指标参数选项
                    if clicked_ma:
                        modify_action = QAction(f"修改{clicked_ma}指标参数", self)
                        modify_action.triggered.connect(lambda: self.on_modify_indicator(clicked_ma))
                        menu.addAction(modify_action)
                    else:
                        # 如果没有点击在均线上，添加提示信息
                        no_select_action = QAction("未选中均线，请先点击选中均线", self)
                        no_select_action.setEnabled(False)  # 禁用选项
                        menu.addAction(no_select_action)
                    
                    # 使用event的pos方法获取场景位置，然后转换为屏幕位置
                    scene_pos = event.pos()
                    logger.info(f"场景位置: {scene_pos}")
                    
                    # 获取tech_plot_widget在屏幕上的位置
                    widget_pos = self.tech_plot_widget.pos()
                    logger.info(f"部件位置: {widget_pos}")
                    
                    # 转换为屏幕坐标，确保是QPoint类型
                    screen_pos = self.tech_plot_widget.mapToGlobal(scene_pos)
                    # 将QPointF转换为QPoint类型
                    qpoint = screen_pos.toPoint()
                    logger.info(f"屏幕位置: {screen_pos}, QPoint: {qpoint}")
                    
                    # 显示菜单
                    logger.info(f"在QPoint位置 {qpoint} 显示菜单")
                    menu.exec(qpoint)
                    logger.info(f"菜单已显示")
                    
                    # 阻止事件传播，防止显示默认菜单
                    event.accept()
        except Exception as e:
            logger.exception(f"处理均线点击事件时发生错误: {e}")
    
    def on_custom_context_menu(self, pos):
        """
        处理customContextMenuRequested信号，显示自定义右键菜单
        
        Args:
            pos: 鼠标位置，相对于widget的坐标
        """
        logger.info(f"customContextMenuRequested信号被调用，位置: {pos}")
        
        # 创建自定义菜单
        menu = QMenu(self.tech_plot_widget)
        
        # 如果有选中的均线，添加修改指标参数选项
        if hasattr(self, 'selected_ma') and self.selected_ma:
            modify_action = QAction(f"修改{self.selected_ma}指标参数", self)
            modify_action.triggered.connect(lambda: self.on_modify_indicator(self.selected_ma))
            menu.addAction(modify_action)
        else:
            # 如果没有选中均线，添加提示信息
            no_select_action = QAction("未选中均线，请先点击选中均线", self)
            no_select_action.setEnabled(False)  # 禁用选项
            menu.addAction(no_select_action)
        
        # 转换为全局坐标
        global_pos = self.tech_plot_widget.mapToGlobal(pos)
        logger.info(f"转换后的全局坐标: {global_pos}")
        
        # 显示菜单
        menu.exec(global_pos)
        logger.info("自定义菜单已显示")
    
    def on_modify_indicator(self, ma_name):
        """
        处理修改指标参数的菜单动作，显示修改指标参数的对话框
        
        Args:
            ma_name: 选中的均线名称，如"MA5", "MA10"等
        """
        logger.info(f"修改指标参数: {ma_name}")
        
        # 创建修改指标参数的对话框
        dialog = QDialog(self)
        dialog.setWindowTitle(f"修改{ma_name}指标参数")
        dialog.setGeometry(300, 300, 300, 200)
        
        # 创建布局
        layout = QVBoxLayout(dialog)
        
        # 获取当前的窗口参数
        current_window = int(ma_name.replace("MA", ""))
        
        # 创建标签和输入框
        window_label = QLabel("周期:", dialog)
        layout.addWidget(window_label)
        
        window_input = QLineEdit(dialog)
        window_input.setText(str(current_window))
        layout.addWidget(window_input)
        
        # 创建按钮布局
        button_layout = QHBoxLayout()
        
        ok_button = QPushButton("确定", dialog)
        cancel_button = QPushButton("取消", dialog)
        
        button_layout.addWidget(ok_button)
        button_layout.addWidget(cancel_button)
        
        layout.addLayout(button_layout)
        
        # 连接按钮信号
        def on_ok():
            try:
                # 获取新的窗口参数
                new_window = int(window_input.text())
                if new_window <= 0:
                    raise ValueError("周期必须大于0")
                
                # 保存新的参数
                logger.info(f"修改{ma_name}周期为: {new_window}")
                
                # TODO: 实现更新均线的逻辑
                # 这里需要重新计算均线并更新绘制
                
                dialog.accept()
            except ValueError as e:
                # 显示错误信息
                logger.error(f"周期输入错误: {e}")
                # 可以添加一个错误提示对话框
        
        ok_button.clicked.connect(on_ok)
        cancel_button.clicked.connect(dialog.reject)
        
        # 显示对话框
        dialog.exec()
    
    def keyPressEvent(self, event):
        """
        处理键盘事件，实现按ESC键从技术分析窗口返回行情窗口
        
        Args:
            event: 键盘事件
        """
        # 检查是否按下了ESC键
        if event.key() == Qt.Key_Escape:
            # 检查当前是否在技术分析窗口
            if self.tab_widget.currentWidget() == self.tech_tab:
                logger.info("按ESC键，从技术分析窗口返回行情窗口")
                # 切换到行情窗口
                self.tab_widget.setCurrentWidget(self.market_tab)
        
        # 调用父类方法处理其他键盘事件
        super().keyPressEvent(event)
    
    def on_kline_mouse_moved(self, pos, dates, opens, highs, lows, closes):
        """
        处理K线图、成交量图和KDJ图鼠标移动事件，实现十字线跟随和指标值更新
        
        Args:
            pos: 鼠标位置
            dates: 日期列表
            opens: 开盘价列表
            highs: 最高价列表
            lows: 最低价列表
            closes: 收盘价列表
        """
        try:
            # 获取三个图表的视图框
            tech_view_box = self.tech_plot_widget.getViewBox()
            volume_view_box = self.volume_plot_widget.getViewBox()
            kdj_view_box = self.kdj_plot_widget.getViewBox()
            
            # 获取三个图表在场景中的区域
            tech_scene_rect = tech_view_box.viewRect()
            volume_scene_rect = volume_view_box.viewRect()
            kdj_scene_rect = kdj_view_box.viewRect()
            
            # 初始化x_val为无效值
            x_val = -1
            
            # 检查鼠标在哪个图表上，并获取对应的x_val
            # 检查K线图
            tech_pos = tech_view_box.mapSceneToView(pos)
            if 0 <= tech_pos.x() < len(dates):
                x_val = tech_pos.x()
            # 检查成交量图
            else:
                volume_pos = volume_view_box.mapSceneToView(pos)
                if 0 <= volume_pos.x() < len(dates):
                    x_val = volume_pos.x()
                # 检查KDJ图
                else:
                    kdj_pos = kdj_view_box.mapSceneToView(pos)
                    if 0 <= kdj_pos.x() < len(dates):
                        x_val = kdj_pos.x()
            
            # 找到最接近的K线索引
            index = int(round(x_val))
            if 0 <= index < len(dates):
                # 保存当前鼠标位置和K线索引
                self.current_mouse_pos = pos
                self.current_kline_index = index
                
                # 保存当前K线数据
                self.current_kline_data = {
                    'dates': dates,
                    'opens': opens,
                    'highs': highs,
                    'lows': lows,
                    'closes': closes,
                    'index': index
                }
                
                # 更新顶部均线值显示
                self.update_ma_values_display(index, dates, opens, highs, lows, closes)
                
                # 更新第二个窗口标签，根据当前指标类型显示不同内容
                if hasattr(self, 'volume_values_label'):
                    # 获取当前窗口指标
                    current_indicator = self.window_indicators[2]
                    
                    if current_indicator == "VOL" and hasattr(self, 'current_volume_data') and 0 <= index < len(self.current_volume_data['volume']):
                        # 更新成交量标签
                        current_volume = self.current_volume_data['volume'][index]
                        current_vol_ma5 = self.current_volume_data['vol_ma5'][index]
                        current_vol_ma10 = self.current_volume_data['vol_ma10'][index]
                        self.volume_values_label.setText(f"<font color='#C0C0C0'>VOLUME: {int(current_volume):,}</font>  <font color='white'>MA5: {int(current_vol_ma5):,}</font>  <font color='cyan'>MA10: {int(current_vol_ma10):,}</font>")
                
                # 更新第3窗口标签，根据当前指标类型显示不同内容
                if hasattr(self, 'kdj_values_label'):
                    # 获取当前第3窗口指标
                    current_indicator = self.window_indicators[3]
                    
                    if current_indicator == "KDJ" and hasattr(self, 'current_kdj_data') and 0 <= index < len(self.current_kdj_data['k']):
                        # 更新KDJ标签
                        current_k = self.current_kdj_data['k'][index]
                        current_d = self.current_kdj_data['d'][index]
                        current_j = self.current_kdj_data['j'][index]
                        self.kdj_values_label.setText(f"<font color='white'>K: {current_k:.2f}</font>  <font color='yellow'>D: {current_d:.2f}</font>  <font color='magenta'>J: {current_j:.2f}</font>")
                    elif current_indicator == "RSI" and hasattr(self, 'current_rsi_data') and 0 <= index < len(self.current_rsi_data['rsi']):
                        # 更新RSI标签
                        current_rsi = self.current_rsi_data['rsi'][index]
                        self.kdj_values_label.setText(f"<font color='blue'>RSI14: {current_rsi:.2f}</font>")
                    elif current_indicator == "MACD" and hasattr(self, 'current_macd_data') and 0 <= index < len(self.current_macd_data['macd']):
                        # 更新MACD标签
                        current_macd = self.current_macd_data['macd'][index]
                        current_macd_signal = self.current_macd_data['macd_signal'][index]
                        current_macd_hist = self.current_macd_data['macd_hist'][index]
                        self.kdj_values_label.setText(f"<font color='blue'>MACD: {current_macd:.2f}</font>  <font color='red'>SIGNAL: {current_macd_signal:.2f}</font>  <font color='#C0C0C0'>HIST: {current_macd_hist:.2f}</font>")
                
                # 更新第二个窗口标签（重复检查，确保所有情况下都正确显示）
                if hasattr(self, 'volume_values_label'):
                    # 获取当前窗口指标
                    current_indicator = self.window_indicators[2]
                    
                    if current_indicator == "VOL" and hasattr(self, 'current_volume_data') and 0 <= index < len(self.current_volume_data['volume']):
                        # 更新成交量标签
                        current_volume = self.current_volume_data['volume'][index]
                        current_vol_ma5 = self.current_volume_data['vol_ma5'][index]
                        current_vol_ma10 = self.current_volume_data['vol_ma10'][index]
                        self.volume_values_label.setText(f"<font color='#C0C0C0'>VOLUME: {int(current_volume):,}</font>  <font color='white'>MA5: {int(current_vol_ma5):,}</font>  <font color='cyan'>MA10: {int(current_vol_ma10):,}</font>")
                    elif current_indicator == "MACD" and hasattr(self, 'current_macd_data') and 0 <= index < len(self.current_macd_data['macd']):
                        # 更新MACD标签
                        current_macd = self.current_macd_data['macd'][index]
                        current_macd_signal = self.current_macd_data['macd_signal'][index]
                        current_macd_hist = self.current_macd_data['macd_hist'][index]
                        self.volume_values_label.setText(f"<font color='blue'>MACD: {current_macd:.2f}</font>  <font color='red'>SIGNAL: {current_macd_signal:.2f}</font>  <font color='#C0C0C0'>HIST: {current_macd_hist:.2f}</font>")
                    elif current_indicator == "RSI" and hasattr(self, 'current_rsi_data') and 0 <= index < len(self.current_rsi_data['rsi']):
                        # 更新RSI标签
                        current_rsi = self.current_rsi_data['rsi'][index]
                        self.volume_values_label.setText(f"<font color='blue'>RSI14: {current_rsi:.2f}</font>")
                    elif current_indicator == "KDJ" and hasattr(self, 'current_kdj_data') and 0 <= index < len(self.current_kdj_data['k']):
                        # 更新KDJ标签
                        current_k = self.current_kdj_data['k'][index]
                        current_d = self.current_kdj_data['d'][index]
                        current_j = self.current_kdj_data['j'][index]
                        self.volume_values_label.setText(f"<font color='white'>K: {current_k:.2f}</font>  <font color='yellow'>D: {current_d:.2f}</font>  <font color='magenta'>J: {current_j:.2f}</font>")
                
            # 如果十字线功能启用，更新十字线位置和信息框
            if self.crosshair_enabled and 0 <= index < len(dates):
                # 检查十字线是否已经初始化
                if hasattr(self, 'vline') and hasattr(self, 'hline') and hasattr(self, 'volume_vline') and hasattr(self, 'volume_hline') and hasattr(self, 'kdj_vline') and hasattr(self, 'kdj_hline'):
                    # 更新K线图十字线
                    self.vline.setValue(index)
                    self.vline.show()
                    
                    # 更新K线图水平线
                    self.hline.setValue(tech_view_box.mapSceneToView(pos).y())
                    self.hline.show()
                    
                    # 更新成交量图十字线
                    self.volume_vline.setValue(index)
                    self.volume_vline.show()
                    
                    # 获取成交量图的鼠标位置
                    volume_pos = volume_view_box.mapSceneToView(pos)
                    volume_y_val = volume_pos.y()
                    self.volume_hline.setValue(volume_y_val)
                    self.volume_hline.show()
                    
                    # 更新KDJ图十字线
                    self.kdj_vline.setValue(index)
                    self.kdj_vline.show()
                    
                    # 获取KDJ图的鼠标位置
                    kdj_pos = kdj_view_box.mapSceneToView(pos)
                    kdj_y_val = kdj_pos.y()
                    self.kdj_hline.setValue(kdj_y_val)
                    self.kdj_hline.show()
                
                # 检查info_timer和info_text是否已经初始化
                if hasattr(self, 'info_timer') and hasattr(self, 'info_text'):
                    # 直接显示信息框，不再等待定时器
                    self.show_info_box()
            else:
                # 十字线功能禁用或索引无效，隐藏十字线
                if hasattr(self, 'vline') and hasattr(self, 'hline') and hasattr(self, 'volume_vline') and hasattr(self, 'volume_hline') and hasattr(self, 'kdj_vline') and hasattr(self, 'kdj_hline'):
                    self.vline.hide()
                    self.hline.hide()
                    self.volume_vline.hide()
                    self.volume_hline.hide()
                    self.kdj_vline.hide()
                    self.kdj_hline.hide()
                if hasattr(self, 'info_text'):
                    self.info_text.hide()
        except Exception as e:
            logger.exception(f"处理K线图鼠标移动事件失败: {e}")
    
    def update_ma_values_display(self, index, dates, opens, highs, lows, closes):
        """
        更新顶部均线值显示
        
        Args:
            index: 当前K线索引
            dates: 日期列表
            opens: 开盘价列表
            highs: 最高价列表
            lows: 最低价列表
            closes: 收盘价列表
        """
        try:
            if not hasattr(self, 'ma_values_label'):
                return
            
            # 确保索引有效
            if index < 0 or index >= len(dates):
                return
            
            # 获取当前日期
            current_date = dates[index].strftime('%Y-%m-%d')
            
            # 获取当前的MA值
            ma_values = {}
            
            # 检查是否有保存的MA数据
            if hasattr(self, 'ma_data'):
                # 使用保存的MA值，确保与绘制的MA线一致
                if 0 <= index < len(self.ma_data['MA5']):
                    ma5 = self.ma_data['MA5'][index]
                    if ma5 != '' and ma5 is not None and str(ma5) != 'nan':
                        ma_values['MA5'] = f"{ma5:.2f}"
                    else:
                        ma_values['MA5'] = "--"
                else:
                    ma_values['MA5'] = "--"
                
                if 0 <= index < len(self.ma_data['MA10']):
                    ma10 = self.ma_data['MA10'][index]
                    if ma10 != '' and ma10 is not None and str(ma10) != 'nan':
                        ma_values['MA10'] = f"{ma10:.2f}"
                    else:
                        ma_values['MA10'] = "--"
                else:
                    ma_values['MA10'] = "--"
                
                if 0 <= index < len(self.ma_data['MA20']):
                    ma20 = self.ma_data['MA20'][index]
                    if ma20 != '' and ma20 is not None and str(ma20) != 'nan':
                        ma_values['MA20'] = f"{ma20:.2f}"
                    else:
                        ma_values['MA20'] = "--"
                else:
                    ma_values['MA20'] = "--"
                
                if 0 <= index < len(self.ma_data['MA60']):
                    ma60 = self.ma_data['MA60'][index]
                    if ma60 != '' and ma60 is not None and str(ma60) != 'nan':
                        ma_values['MA60'] = f"{ma60:.2f}"
                    else:
                        ma_values['MA60'] = "--"
                else:
                    ma_values['MA60'] = "--"
            else:
                # 如果没有保存的MA数据，使用默认值
                ma_values['MA5'] = "--"
                ma_values['MA10'] = "--"
                ma_values['MA20'] = "--"
                ma_values['MA60'] = "--"
            
            # 获取MA线的颜色，默认使用当前设置的颜色
            if hasattr(self, 'ma_colors'):
                ma5_color = self.ma_colors.get('MA5', 'white')
                ma10_color = self.ma_colors.get('MA10', 'cyan')
                ma20_color = self.ma_colors.get('MA20', 'red')
                ma60_color = self.ma_colors.get('MA60', 'green')
            else:
                # 默认颜色设置
                ma5_color = 'white'
                ma10_color = 'cyan'
                ma20_color = 'red'
                ma60_color = 'green'
            
            # 更新标签文本，使用HTML格式设置不同颜色，添加日期显示
            ma_text = f"<font color='#C0C0C0'>日期: {current_date}</font>  <font color='{ma5_color}'>MA5: {ma_values['MA5']}</font>  <font color='{ma10_color}'>MA10: {ma_values['MA10']}</font>  <font color='{ma20_color}'>MA20: {ma_values['MA20']}</font>  <font color='{ma60_color}'>MA60: {ma_values['MA60']}</font>"
            self.ma_values_label.setText(ma_text)
            logger.debug(f"更新MA值显示: {ma_text}")
        except Exception as e:
            logger.exception(f"更新MA值显示时发生错误: {e}")
    
    def show_info_box(self):
        """
        显示信息框
        """
        try:
            if self.current_kline_index >= 0 and self.current_kline_data:
                dates = self.current_kline_data['dates']
                opens = self.current_kline_data['opens']
                highs = self.current_kline_data['highs']
                lows = self.current_kline_data['lows']
                closes = self.current_kline_data['closes']
                index = self.current_kline_index
                
                # 确保索引在有效范围内
                if 0 <= index < len(dates):
                    # 计算前一天的收盘价，用于计算涨跌幅
                    pre_close = closes[index-1] if index > 0 else closes[index]
                    
                    # 计算涨跌幅和涨跌额
                    change = closes[index] - pre_close
                    pct_change = (change / pre_close) * 100 if pre_close != 0 else 0
                    
                    # 获取星期几，0=周一，1=周二，2=周三，3=周四，4=周五，5=周六，6=周日
                    weekday = dates[index].weekday()
                    # 转换为中文星期
                    weekday_map = {0: '一', 1: '二', 2: '三', 3: '四', 4: '五', 5: '六', 6: '日'}
                    weekday_str = weekday_map.get(weekday, '')
                    
                    # 生成信息文本
                    info_html = f"""
                    <div style="background-color: rgba(0, 0, 0, 0.8); padding: 8px; border: 1px solid #666; color: white; font-family: monospace;">
                    <div style="font-weight: bold;">{dates[index].strftime('%Y-%m-%d')}/{weekday_str}</div>
                    <div>开盘: {opens[index]:.2f}</div>
                    <div>最高: {highs[index]:.2f}</div>
                    <div>最低: {lows[index]:.2f}</div>
                    <div>收盘: {closes[index]:.2f}</div>
                    <div>涨跌: {change:.2f}</div>
                    <div>涨幅: {pct_change:.2f}%</div>
                    </div>
                    """
                    
                    # 更新信息文本
                    if self.info_text is not None:
                        self.info_text.setHtml(info_html)
                        # 设置信息文本位置，跟随鼠标显示
                        if self.current_mouse_pos is not None:
                            # 将场景坐标转换为视图坐标
                            view_box = self.tech_plot_widget.getViewBox()
                            view_pos = view_box.mapSceneToView(self.current_mouse_pos)
                            
                            # 获取当前视图范围
                            x_min, x_max = view_box.viewRange()[0]
                            y_min, y_max = view_box.viewRange()[1]
                            
                            # 估计信息框的尺寸（以K线数量为单位）
                            info_width_kline = 8  # 信息框宽度约占8个K线
                            info_height_kline = 15  # 信息框高度约占15个K线（7行文本+边距）
                            
                            # 使用K线位置作为信息框的基准位置
                            kline_x = self.current_kline_index
                            kline_y = lows[index]
                            
                            # 获取当前视图范围
                            x_min, x_max = view_box.viewRange()[0]
                            y_min, y_max = view_box.viewRange()[1]
                            
                            # 定义信息框尺寸和边距
                            info_box_height = 15  # 信息框高度（7行文本+边距）
                            info_box_width = 8  # 信息框宽度
                            margin = 2  # 边距
                            
                            # 检测K线是否靠近窗口顶部
                            # 如果K线位于视图上半部分，信息框显示在下方
                            # 这样可以确保信息框不会超出顶部边界
                            # 在pyqtgraph中，y值越大，位置越靠上
                            view_height = y_max - y_min
                            view_width = x_max - x_min
                            
                            # 计算信息框显示在K线上方时的底部位置
                            # 信息框显示在上方时，底部对齐K线顶部
                            info_box_bottom_when_top = kline_y + margin + info_box_height
                            
                            # 定义顶部安全距离，考虑工具栏等UI元素的影响
                            # 当K线距离顶部的距离小于安全距离时，信息框显示在下方
                            top_safety_distance = info_box_height + margin + 10  # 额外增加10单位安全距离
                            #is_near_top = (y_max - kline_y) < top_safety_distance
                            is_near_top = (y_max - kline_y) < view_height * 0.3
                            # 如果K线位于下30%区域，信息框显示在上方
                            is_near_bottom = kline_y < y_min + view_height * 0.3
                            # 如果K线位于左10%区域，信息框显示在右侧
                            is_near_left = kline_x < x_min + view_width * 0.1
                            
                            # 根据K线位置智能选择信息框显示位置
                            # 锚点是(0, 1)，所以pos_x和pos_y定义了信息框的左下角位置
                            # 在pyqtgraph中，y值越大位置越靠上，y值越小位置越靠下
                            # 根据K线位置智能选择信息框显示位置
                            # 锚点是(0, 1)，所以pos_x和pos_y定义了信息框的左下角位置
                            # 在pyqtgraph中，y值越大位置越靠上，y值越小位置越靠下
                            
                            # 计算信息框的尺寸（基于视图范围的百分比）
                            view_height = y_max - y_min
                            view_width = x_max - x_min
                            
                            # 使用实际像素或视图坐标的百分比来确定信息框尺寸
                            info_box_height = view_height * 0.2  # 信息框高度为视图高度的20%
                            info_box_width = view_width * 0.25  # 信息框宽度为视图宽度的25%
                            margin = view_height * 0.02  # 边距为视图高度的2%
                            
                            # 计算K线在视图中的相对位置
                            kline_relative_y = (kline_y - y_min) / view_height
                            
                            # 始终将信息框显示在K线右侧
                            pos_x = kline_x + view_width * 0.01  # K线右侧1%视图宽度
                            
                            if kline_relative_y > 0.7:  # K线位于视图上70%
                                # 信息框显示在K线下方
                                pos_y = kline_y - info_box_height - margin
                            else:  # K线位于视图下30%
                                # 信息框显示在K线上方
                                pos_y = kline_y + margin
                            
                            # 确保信息框完全在视图范围内
                            # 水平方向：确保信息框不会超出左右边界
                            if pos_x < x_min + margin:
                                pos_x = x_min + margin
                            if pos_x + info_box_width > x_max - margin:
                                pos_x = x_max - info_box_width - margin
                            
                            # 垂直方向：确保信息框不会超出上下边界
                            # 在pyqtgraph中，y值越大位置越靠上
                            # 检查信息框底部是否超出视图底部
                            if pos_y < y_min + margin:
                                pos_y = y_min + margin
                            # 检查信息框顶部是否超出视图顶部
                            elif pos_y + info_box_height > y_max - margin:
                                pos_y = y_max - info_box_height - margin
                            
                            # 输出最终位置
                            # 最终边界检查，确保信息框完全显示
                            pos_x = max(pos_x, x_min + margin)
                            pos_x = min(pos_x, x_max - margin - info_box_width)
                            pos_y = max(pos_y, y_min + margin)
                            pos_y = min(pos_y, y_max - margin - info_box_height)
                            
                            self.info_text.setPos(pos_x, pos_y)
                            self.info_text.show()
        except Exception as e:
            logger.exception(f"显示信息框失败: {e}")

    def on_nav_item_clicked(self, item, column):
        """
        处理导航项点击事件，显示对应的行情数据
        """
        text = item.text(column)
        logger.info(f"点击了导航项: {text}")
        
        # 切换到行情标签页
        self.tab_widget.setCurrentIndex(0)
        
        try:
            # 处理指数相关导航项
            if text in ["上证指数", "深证成指", "创业板指", "科创板指"]:
                self.show_index_data(text)
            # 处理沪深京A股导航项
            elif text == "沪深京A股":
                self.show_hs_aj_stock_data()
            # 处理个股分类导航项
            elif text in ["全部A股", "上证A股", "深证A股", "创业板", "科创板"]:
                self.show_stock_data_by_type(text)
        except Exception as e:
            logger.exception(f"处理导航项点击事件失败: {e}")
    
    def show_stock_data_by_type(self, stock_type):
        """
        根据股票类型显示数据，从通达信日线文件读取最新交易日的对应股票数据
        
        Args:
            stock_type: 股票类型，如"全部A股"、"上证A股"、"深证A股"、"创业板"、"科创板"
        """
        try:
            logger.info(f"开始显示{stock_type}数据")
            
            # 显示进度条
            self.progress_bar.setVisible(True)
            self.progress_bar.setValue(0)
            
            # 导入所需模块
            import polars as pl
            from src.data.tdx_handler import TdxHandler
            
            # 创建通达信数据处理器
            tdx_handler = TdxHandler(self.data_manager.config, self.data_manager.db_manager)
            
            # 更新进度
            self.progress_bar.setValue(10)
            
            # 构建通达信日线数据目录路径
            from pathlib import Path
            tdx_data_path = Path(self.data_manager.config.data.tdx_data_path)
            
            # 获取所有日线数据文件
            sh_stock_files = list(Path(tdx_data_path / 'sh' / 'lday').glob('sh*.day')) if (tdx_data_path / 'sh' / 'lday').exists() else []
            sz_stock_files = list(Path(tdx_data_path / 'sz' / 'lday').glob('sz*.day')) if (tdx_data_path / 'sz' / 'lday').exists() else []
            
            # 根据股票类型过滤文件
            filtered_files = []
            if stock_type == "全部A股":
                # 显示所有A股
                filtered_files = sh_stock_files + sz_stock_files
            elif stock_type == "上证A股":
                # 只显示上证A股（上海市场，代码以6开头）
                filtered_files = sh_stock_files
            elif stock_type == "深证A股":
                # 只显示深证A股（深圳市场，代码以0开头，不含创业板）
                filtered_files = [f for f in sz_stock_files if f.stem[2:3] == "0"]
            elif stock_type == "创业板":
                # 只显示创业板股票（代码以300开头）
                filtered_files = [f for f in sz_stock_files if f.stem[2:5] == "300"]
            elif stock_type == "科创板":
                # 只显示科创板股票（代码以688开头）
                filtered_files = [f for f in sh_stock_files if f.stem[2:5] == "688"]
            
            logger.info(f"找到{len(filtered_files)}个符合条件的通达信股票数据文件")
            
            # 更新进度
            self.progress_bar.setValue(20)
            
            if not filtered_files:
                logger.warning(f"没有找到{stock_type}的通达信股票数据文件")
                self.statusBar().showMessage(f"没有找到{stock_type}的通达信股票数据文件，请检查路径是否正确", 5000)
                self.progress_bar.setVisible(False)
                return
            
            # 获取最新交易日
            latest_date = None
            all_stock_data = []
            
            # 获取股票基本信息映射
            stock_name_map = self.data_manager.get_stock_basic()
            
            # 更新进度
            self.progress_bar.setValue(30)
            
            # 解析所有股票文件，获取最新交易日的数据
            total_files = len(filtered_files)
            for i, file_path in enumerate(filtered_files):
                try:
                    # 批量更新进度，减少UI重绘
                    update_interval = max(1, total_files // 10)  # 最多更新10次
                    if i % update_interval == 0:
                        progress = 30 + int((i / total_files) * 50)
                        self.progress_bar.setValue(progress)
                    
                    # 解析文件，获取所有数据
                    # 只在每100个文件记录一次日志，减少IO开销
                    if i % 100 == 0:
                        logger.info(f"正在解析文件: {file_path} ({i+1}/{total_files})")
                    
                    # 直接解析文件，获取所有数据
                    data = []
                    with open(file_path, 'rb') as f:
                        # 获取文件大小
                        f.seek(0, 2)
                        file_size = f.tell()
                        
                        # 计算数据条数
                        record_count = file_size // 32
                        if record_count == 0:
                            continue
                        
                        # 读取最新两条记录，用于计算涨跌幅和涨跌
                        # 先读取最新一条记录（当天数据）
                        f.seek((record_count - 1) * 32)
                        latest_record = f.read(32)
                        
                        # 如果有至少两条记录，读取前一天的记录（用于计算涨跌）
                        if record_count >= 2:
                            f.seek((record_count - 2) * 32)
                            prev_record = f.read(32)
                        else:
                            prev_record = None
                        
                        # 解析最新一条记录
                        import struct
                        from datetime import datetime
                        
                        # 解析当天数据
                        date_int = struct.unpack('I', latest_record[0:4])[0]  # 日期，格式：YYYYMMDD
                        open_val = struct.unpack('I', latest_record[4:8])[0] / 100  # 开盘价，转换为元
                        high_val = struct.unpack('I', latest_record[8:12])[0] / 100  # 最高价，转换为元
                        low_val = struct.unpack('I', latest_record[12:16])[0] / 100  # 最低价，转换为元
                        close_val = struct.unpack('I', latest_record[16:20])[0] / 100  # 收盘价，转换为元
                        volume = struct.unpack('I', latest_record[20:24])[0]  # 成交量，单位：手
                        amount = struct.unpack('I', latest_record[24:28])[0] / 100  # 成交额，转换为元
                        
                        # 转换日期格式
                        date_str = str(date_int)
                        date = datetime.strptime(date_str, '%Y%m%d').date()
                        
                        # 更新最新日期
                        if latest_date is None or date > latest_date:
                            latest_date = date
                        
                        # 提取股票代码
                        file_name = file_path.stem
                        if file_name.startswith('sh'):
                            code = file_name[2:]
                            market = "SH"
                            ts_code = f"{code}.{market}"
                            # 尝试不同的ts_code格式
                            ts_code_formats = [
                                f"{code}.{market}",
                                f"{code}.{market.lower()}",
                                f"{market}{code}",
                                f"{market.lower()}{code}"
                            ]
                            
                            # 从stock_basic获取真实股票名称
                            stock_name = f"{code}（股票）"  # 默认名称
                            for ts_format in ts_code_formats:
                                if ts_format in stock_name_map:
                                    stock_name = stock_name_map[ts_format]
                                    break
                        elif file_name.startswith('sz'):
                            code = file_name[2:]
                            market = "SZ"
                            ts_code = f"{code}.{market}"
                            # 尝试不同的ts_code格式
                            ts_code_formats = [
                                f"{code}.{market}",
                                f"{code}.{market.lower()}",
                                f"{market}{code}",
                                f"{market.lower()}{code}"
                            ]
                            
                            # 从stock_basic获取真实股票名称
                            stock_name = f"{code}（股票）"  # 默认名称
                            for ts_format in ts_code_formats:
                                if ts_format in stock_name_map:
                                    stock_name = stock_name_map[ts_format]
                                    break
                        else:
                            continue
                        
                        # 计算涨跌额和涨跌幅
                        if prev_record:
                            # 解析前一天数据
                            prev_date_int = struct.unpack('I', prev_record[0:4])[0]  # 前一天日期
                            prev_close_val = struct.unpack('I', prev_record[16:20])[0] / 100  # 前一天收盘价
                            
                            # 计算涨跌额和涨跌幅
                            preclose = prev_close_val  # 昨收价
                            change = close_val - preclose  # 涨跌额
                            pct_chg = (change / preclose) * 100 if preclose != 0 else 0.0  # 涨跌幅
                        else:
                            # 只有一条记录，无法计算涨跌额和涨跌幅，设为0
                            preclose = close_val  # 没有前一天数据，使用收盘价作为昨收价
                            change = 0.0
                            pct_chg = 0.0
                        
                        # 添加到数据列表
                        data.append({
                            'date': date.strftime('%Y-%m-%d'),
                            'code': code,
                            'name': stock_name,
                            'pct_chg': pct_chg,
                            'close': close_val,
                            'change': change,
                            'open': open_val,
                            'high': high_val,
                            'low': low_val,
                            'volume': volume,
                            'amount': amount,
                            'preclose': preclose
                        })
                    
                    # 添加到所有股票数据列表
                    all_stock_data.extend(data)
                    
                except Exception as e:
                    logger.exception(f"解析文件{file_path}失败: {e}")
                    continue
            
            # 更新进度
            self.progress_bar.setValue(80)
            
            if not all_stock_data:
                logger.warning(f"没有解析到任何{stock_type}数据")
                self.statusBar().showMessage(f"没有解析到任何{stock_type}数据，请检查文件格式是否正确", 5000)
                self.progress_bar.setVisible(False)
                return
            
            # 过滤出最新交易日的数据
            if latest_date:
                latest_date_str = latest_date.strftime('%Y-%m-%d')
                all_stock_data = [item for item in all_stock_data if item['date'] == latest_date_str]
                logger.info(f"最新交易日: {latest_date_str}，共{len(all_stock_data)}只{stock_type}股票有数据")
            
            # 清空现有数据前先关闭排序
            self.stock_table.setSortingEnabled(False)
            
            # 清空现有数据
            self.stock_table.setRowCount(0)
            
            # 更新进度
            self.progress_bar.setValue(90)
            
            # 添加数据到表格
            for row_data in all_stock_data:
                # 计算振幅
                if row_data['preclose'] > 0:
                    amplitude = ((row_data['high'] - row_data['low']) / row_data['preclose']) * 100
                else:
                    amplitude = 0.0
                
                # 构建数据行，适配新的列结构
                data_row = [
                    row_data['date'],  # 日期
                    row_data['code'],  # 代码
                    row_data['name'],  # 名称
                    f"{row_data['pct_chg']:.2f}",  # 涨跌幅
                    f"{row_data['close']:.2f}",  # 现价
                    f"{row_data['change']:.2f}",  # 涨跌
                    f"{row_data['volume']:,}",  # 总量
                    f"{row_data['amount']:,}",  # 成交额
                    f"{row_data['open']:.2f}",  # 今开
                    f"{row_data['high']:.2f}",  # 最高
                    f"{row_data['low']:.2f}",  # 最低
                    f"{row_data['preclose']:.2f}",  # 昨收
                    f"{amplitude:.2f}%"  # 振幅%
                ]
                
                # 添加行
                row_pos = self.stock_table.rowCount()
                self.stock_table.insertRow(row_pos)
                
                # 设置数据
                for col, value in enumerate(data_row):
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
                    preclose = float(data_row[11]) if len(data_row) > 11 and data_row[11] != "-" else 0.0
                    if col == 4:  # 现价
                        # 现价与昨收比较
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
                    
                    self.stock_table.setItem(row_pos, col, item)
            
            # 数据添加完成后重新启用排序
            self.stock_table.setSortingEnabled(True)
            
            # 更新进度
            self.progress_bar.setValue(100)
            
            logger.info(f"{stock_type}数据显示完成")
            self.statusBar().showMessage(f"成功显示{len(all_stock_data)}只{stock_type}股票的最新交易日数据", 3000)
            
            # 隐藏进度条
            self.progress_bar.setVisible(False)
            
        except Exception as e:
            logger.exception(f"显示{stock_type}数据失败: {e}")
            self.statusBar().showMessage(f"显示{stock_type}数据失败: {str(e)[:50]}...", 5000)
            # 隐藏进度条
            self.progress_bar.setVisible(False)
    
    def show_hs_aj_stock_data(self):
        """
        显示沪深京A股数据，从通达信日线文件读取最新交易日的全部股票数据
        """
        try:
            logger.info("开始显示沪深京A股数据")
            
            # 显示进度条
            self.progress_bar.setVisible(True)
            self.progress_bar.setValue(0)
            
            # 更新配置中的通达信数据路径为用户指定的路径
            import polars as pl
            from src.data.tdx_handler import TdxHandler
            
            # 创建通达信数据处理器
            tdx_handler = TdxHandler(self.data_manager.config, self.data_manager.db_manager)
            
            # 更新进度
            self.progress_bar.setValue(10)
            
            # 构建通达信日线数据目录路径
            from pathlib import Path
            tdx_data_path = Path(self.data_manager.config.data.tdx_data_path)
            
            # 获取所有日线数据文件
            sh_stock_files = list(Path(tdx_data_path / 'sh' / 'lday').glob('sh*.day')) if (tdx_data_path / 'sh' / 'lday').exists() else []
            sz_stock_files = list(Path(tdx_data_path / 'sz' / 'lday').glob('sz*.day')) if (tdx_data_path / 'sz' / 'lday').exists() else []
            all_stock_files = sh_stock_files + sz_stock_files
            
            logger.info(f"找到{len(all_stock_files)}个通达信股票数据文件")
            
            # 更新进度
            self.progress_bar.setValue(20)
            
            if not all_stock_files:
                logger.warning("没有找到通达信股票数据文件")
                self.statusBar().showMessage("没有找到通达信股票数据文件，请检查路径是否正确", 5000)
                self.progress_bar.setVisible(False)
                return
            
            # 获取最新交易日
            latest_date = None
            all_stock_data = []
            
            # 获取股票基本信息映射
            stock_name_map = self.data_manager.get_stock_basic()
            
            # 更新进度
            self.progress_bar.setValue(30)
            
            # 解析所有股票文件，获取最新交易日的数据
            total_files = len(all_stock_files)
            for i, file_path in enumerate(all_stock_files):
                try:
                    # 更新进度，减少UI更新频率
                    if i % max(1, total_files // 10) == 0:  # 最多更新10次
                        progress = 30 + int((i / total_files) * 50)
                        self.progress_bar.setValue(progress)
                    
                    # 解析文件，获取所有数据
                    logger.info(f"正在解析文件: {file_path}")
                    
                    # 直接解析文件，获取所有数据
                    data = []
                    with open(file_path, 'rb') as f:
                        # 获取文件大小
                        f.seek(0, 2)
                        file_size = f.tell()
                        
                        # 计算数据条数
                        record_count = file_size // 32
                        if record_count == 0:
                            continue
                        
                        # 读取最新两条记录，用于计算涨跌幅和涨跌
                        # 先读取最新一条记录（当天数据）
                        f.seek((record_count - 1) * 32)
                        latest_record = f.read(32)
                        
                        # 如果有至少两条记录，读取前一天的记录（用于计算涨跌）
                        if record_count >= 2:
                            f.seek((record_count - 2) * 32)
                            prev_record = f.read(32)
                        else:
                            prev_record = None
                        
                        # 解析最新一条记录
                        import struct
                        from datetime import datetime
                        
                        # 解析当天数据
                        date_int = struct.unpack('I', latest_record[0:4])[0]  # 日期，格式：YYYYMMDD
                        open_val = struct.unpack('I', latest_record[4:8])[0] / 100  # 开盘价，转换为元
                        high_val = struct.unpack('I', latest_record[8:12])[0] / 100  # 最高价，转换为元
                        low_val = struct.unpack('I', latest_record[12:16])[0] / 100  # 最低价，转换为元
                        close_val = struct.unpack('I', latest_record[16:20])[0] / 100  # 收盘价，转换为元
                        volume = struct.unpack('I', latest_record[20:24])[0]  # 成交量，单位：手
                        amount = struct.unpack('I', latest_record[24:28])[0] / 100  # 成交额，转换为元
                        
                        # 转换日期格式
                        date_str = str(date_int)
                        date = datetime.strptime(date_str, '%Y%m%d').date()
                        
                        # 更新最新日期
                        if latest_date is None or date > latest_date:
                            latest_date = date
                        
                        # 提取股票代码
                        file_name = file_path.stem
                        if file_name.startswith('sh'):
                            code = file_name[2:]
                            market = "SH"
                            ts_code = f"{code}.{market}"
                            # 从stock_basic获取真实股票名称
                            stock_name = stock_name_map.get(ts_code, f"{code}（股票）")
                        elif file_name.startswith('sz'):
                            code = file_name[2:]
                            market = "SZ"
                            ts_code = f"{code}.{market}"
                            # 从stock_basic获取真实股票名称
                            stock_name = stock_name_map.get(ts_code, f"{code}（股票）")
                        else:
                            continue
                        
                        # 计算涨跌额和涨跌幅
                        if prev_record:
                            # 解析前一天数据
                            prev_date_int = struct.unpack('I', prev_record[0:4])[0]  # 前一天日期
                            prev_close_val = struct.unpack('I', prev_record[16:20])[0] / 100  # 前一天收盘价
                            
                            # 计算涨跌额和涨跌幅
                            preclose = prev_close_val  # 昨收价
                            change = close_val - preclose  # 涨跌额
                            pct_chg = (change / preclose) * 100 if preclose != 0 else 0.0  # 涨跌幅
                        else:
                            # 只有一条记录，无法计算涨跌额和涨跌幅，设为0
                            preclose = close_val  # 没有前一天数据，使用收盘价作为昨收价
                            change = 0.0
                            pct_chg = 0.0
                        
                        # 添加到数据列表
                        data.append({
                            'date': date.strftime('%Y-%m-%d'),
                            'code': code,
                            'name': stock_name,
                            'pct_chg': pct_chg,
                            'close': close_val,
                            'change': change,
                            'open': open_val,
                            'high': high_val,
                            'low': low_val,
                            'volume': volume,
                            'amount': amount,
                            'preclose': preclose
                        })
                    
                    # 添加到所有股票数据列表
                    all_stock_data.extend(data)
                    
                except Exception as e:
                    logger.exception(f"解析文件{file_path}失败: {e}")
                    continue
            
            # 更新进度
            self.progress_bar.setValue(80)
            
            if not all_stock_data:
                logger.warning("没有解析到任何股票数据")
                self.statusBar().showMessage("没有解析到任何股票数据，请检查文件格式是否正确", 5000)
                self.progress_bar.setVisible(False)
                return
            
            # 过滤出最新交易日的数据
            if latest_date:
                latest_date_str = latest_date.strftime('%Y-%m-%d')
                all_stock_data = [item for item in all_stock_data if item['date'] == latest_date_str]
                logger.info(f"最新交易日: {latest_date_str}，共{len(all_stock_data)}只股票有数据")
            
            # 清空现有数据
            self.stock_table.setRowCount(0)
            
            # 更新进度
            self.progress_bar.setValue(90)
            
            # 添加数据到表格
            for row_data in all_stock_data:
                # 计算振幅
                if row_data['preclose'] > 0:
                    amplitude = ((row_data['high'] - row_data['low']) / row_data['preclose']) * 100
                else:
                    amplitude = 0.0
                
                # 构建数据行，适配新的列结构
                data_row = [
                    row_data['date'],  # 日期
                    row_data['code'],  # 代码
                    row_data['name'],  # 名称
                    f"{row_data['pct_chg']:.2f}",  # 涨跌幅
                    f"{row_data['close']:.2f}",  # 现价
                    f"{row_data['change']:.2f}",  # 涨跌
                    f"{row_data['volume']:,}",  # 总量
                    f"{row_data['amount']:,}",  # 成交额
                    f"{row_data['open']:.2f}",  # 今开
                    f"{row_data['high']:.2f}",  # 最高
                    f"{row_data['low']:.2f}",  # 最低
                    f"{row_data['preclose']:.2f}",  # 昨收
                    f"{amplitude:.2f}%"  # 振幅%
                ]
                
                # 添加行
                row_pos = self.stock_table.rowCount()
                self.stock_table.insertRow(row_pos)
                
                # 设置数据
                for col, value in enumerate(data_row):
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
                    preclose_val = float(data_row[11]) if len(data_row) > 11 and data_row[11] != "-" else 0.0
                    if col == 4:  # 现价
                        # 现价与昨收比较
                        current_price = float(value) if value != "-" else 0.0
                        if current_price > preclose_val:
                            item.setForeground(QColor(255, 0, 0))  # 红色高于昨收
                        elif current_price < preclose_val:
                            item.setForeground(QColor(0, 255, 0))  # 绿色低于昨收
                        else:
                            item.setForeground(QColor(204, 204, 204))  # 灰色等于昨收
                    elif col == 8:  # 今开
                        # 今开与昨收比较
                        open_price = float(value) if value != "-" else 0.0
                        if open_price > preclose_val:
                            item.setForeground(QColor(255, 0, 0))  # 红色高于昨收
                        elif open_price < preclose_val:
                            item.setForeground(QColor(0, 255, 0))  # 绿色低于昨收
                        else:
                            item.setForeground(QColor(204, 204, 204))  # 灰色等于昨收
                    elif col == 9:  # 最高
                        # 最高与昨收比较
                        high_price = float(value) if value != "-" else 0.0
                        if high_price > preclose_val:
                            item.setForeground(QColor(255, 0, 0))  # 红色高于昨收
                        elif high_price < preclose_val:
                            item.setForeground(QColor(0, 255, 0))  # 绿色低于昨收
                        else:
                            item.setForeground(QColor(204, 204, 204))  # 灰色等于昨收
                    elif col == 10:  # 最低
                        # 最低与昨收比较
                        low_price = float(value) if value != "-" else 0.0
                        if low_price > preclose_val:
                            item.setForeground(QColor(255, 0, 0))  # 红色高于昨收
                        elif low_price < preclose_val:
                            item.setForeground(QColor(0, 255, 0))  # 绿色低于昨收
                        else:
                            item.setForeground(QColor(204, 204, 204))  # 灰色等于昨收
                    
                    self.stock_table.setItem(row_pos, col, item)
            
            # 数据添加完成后重新启用排序
            self.stock_table.setSortingEnabled(True)
            
            # 更新进度
            self.progress_bar.setValue(100)
            
            logger.info("沪深京A股数据显示完成")
            self.statusBar().showMessage(f"成功显示{len(all_stock_data)}只股票的最新交易日数据", 3000)
            
            # 隐藏进度条
            self.progress_bar.setVisible(False)
            
        except Exception as e:
            logger.exception(f"显示沪深京A股数据失败: {e}")
            self.statusBar().showMessage(f"显示沪深京A股数据失败: {str(e)[:50]}...", 5000)
            # 隐藏进度条
            self.progress_bar.setVisible(False)
    
    def show_index_data(self, index_name):
        """
        显示指定指数的数据
        
        Args:
            index_name: 指数名称，如"上证指数"、"深证成指"等
        """
        try:
            logger.info(f"开始显示{index_name}的数据")
            
            # 指数代码映射
            index_code_map = {
                "上证指数": "sh000001",
                "深证成指": "sz399001",
                "创业板指": "sz399006",
                "科创板指": "sh000688"
            }
            
            if index_name not in index_code_map:
                logger.warning(f"不支持的指数名称: {index_name}")
                return
            
            index_code = index_code_map[index_name]
            
            # 构建通达信日线数据目录路径
            from pathlib import Path
            tdx_data_path = Path(self.data_manager.config.data.tdx_data_path)
            
            # 确定指数文件路径
            market = "sh" if index_code.startswith("sh") else "sz"
            index_file = Path(tdx_data_path / market / 'lday' / f"{index_code}.day")
            
            if not index_file.exists():
                logger.warning(f"未找到{index_name}的通达信指数文件: {index_file}")
                self.statusBar().showMessage(f"未找到{index_name}的通达信指数文件，请检查路径是否正确", 5000)
                return
            
            # 清空现有数据
            self.stock_table.setSortingEnabled(False)
            self.stock_table.setRowCount(0)
            
            import struct
            from datetime import datetime
            
            # 解析指数文件
            with open(index_file, 'rb') as f:
                # 获取文件大小
                f.seek(0, 2)
                file_size = f.tell()
                
                # 计算数据条数
                record_count = file_size // 32
                if record_count == 0:
                    logger.warning(f"{index_name}的指数文件为空")
                    return
                
                # 读取最新30天的数据
                start_record = max(0, record_count - 30)
                f.seek(start_record * 32)
                
                # 读取所有记录
                records = []
                for _ in range(record_count - start_record):
                    record = f.read(32)
                    if len(record) != 32:
                        break
                    records.append(record)
            
            # 解析记录并添加到表格
            prev_close = None
            for record in records:
                # 解析记录
                date_int = struct.unpack('I', record[0:4])[0]  # 日期，格式：YYYYMMDD
                open_val = struct.unpack('I', record[4:8])[0] / 100  # 开盘价，转换为元
                high_val = struct.unpack('I', record[8:12])[0] / 100  # 最高价，转换为元
                low_val = struct.unpack('I', record[12:16])[0] / 100  # 最低价，转换为元
                close_val = struct.unpack('I', record[16:20])[0] / 100  # 收盘价，转换为元
                volume = struct.unpack('I', record[20:24])[0]  # 成交量，单位：手
                amount = struct.unpack('I', record[24:28])[0] / 100  # 成交额，转换为元
                
                # 转换日期格式
                date_str = str(date_int)
                date = datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
                
                # 计算涨跌额和涨跌幅
                if prev_close is not None:
                    change = close_val - prev_close
                    pct_chg = (change / prev_close) * 100 if prev_close != 0 else 0.0
                else:
                    # 第一条记录，无法计算涨跌额和涨跌幅
                    change = 0.0
                    pct_chg = 0.0
                
                # 计算振幅
                if prev_close is not None and prev_close > 0:
                    amplitude = ((high_val - low_val) / prev_close) * 100
                else:
                    amplitude = 0.0
                
                # 构建数据行
                data_row = [
                    date,  # 日期
                    index_code,  # 代码
                    index_name,  # 名称
                    f"{pct_chg:.2f}",  # 涨跌幅
                    f"{close_val:.2f}",  # 现价
                    f"{change:.2f}",  # 涨跌
                    f"{volume:,}",  # 总量
                    f"{amount:,}",  # 成交额
                    f"{open_val:.2f}",  # 今开
                    f"{high_val:.2f}",  # 最高
                    f"{low_val:.2f}",  # 最低
                    f"{prev_close:.2f}" if prev_close is not None else f"{close_val:.2f}",  # 昨收
                    f"{amplitude:.2f}%"  # 振幅%
                ]
                
                # 添加行
                row_pos = self.stock_table.rowCount()
                self.stock_table.insertRow(row_pos)
                
                # 设置数据
                for col, value in enumerate(data_row):
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
                    preclose_val = float(data_row[11]) if len(data_row) > 11 and data_row[11] != "-" else 0.0
                    if col == 4:  # 现价
                        # 现价与昨收比较
                        current_price = float(value) if value != "-" else 0.0
                        if current_price > preclose_val:
                            item.setForeground(QColor(255, 0, 0))  # 红色高于昨收
                        elif current_price < preclose_val:
                            item.setForeground(QColor(0, 255, 0))  # 绿色低于昨收
                        else:
                            item.setForeground(QColor(204, 204, 204))  # 灰色等于昨收
                    elif col == 8:  # 今开
                        # 今开与昨收比较
                        open_price = float(value) if value != "-" else 0.0
                        if open_price > preclose_val:
                            item.setForeground(QColor(255, 0, 0))  # 红色高于昨收
                        elif open_price < preclose_val:
                            item.setForeground(QColor(0, 255, 0))  # 绿色低于昨收
                        else:
                            item.setForeground(QColor(204, 204, 204))  # 灰色等于昨收
                    elif col == 9:  # 最高
                        # 最高与昨收比较
                        high_price = float(value) if value != "-" else 0.0
                        if high_price > preclose_val:
                            item.setForeground(QColor(255, 0, 0))  # 红色高于昨收
                        elif high_price < preclose_val:
                            item.setForeground(QColor(0, 255, 0))  # 绿色低于昨收
                        else:
                            item.setForeground(QColor(204, 204, 204))  # 灰色等于昨收
                    elif col == 10:  # 最低
                        # 最低与昨收比较
                        low_price = float(value) if value != "-" else 0.0
                        if low_price > preclose_val:
                            item.setForeground(QColor(255, 0, 0))  # 红色高于昨收
                        elif low_price < preclose_val:
                            item.setForeground(QColor(0, 255, 0))  # 绿色低于昨收
                        else:
                            item.setForeground(QColor(204, 204, 204))  # 灰色等于昨收
                    
                    self.stock_table.setItem(row_pos, col, item)
                
                # 更新前收盘价，用于计算下一条记录的涨跌幅
                prev_close = close_val
            
            # 数据添加完成后重新启用排序
            self.stock_table.setSortingEnabled(True)
            
            logger.info(f"{index_name}数据显示完成")
            
        except Exception as e:
            logger.exception(f"显示{index_name}数据失败: {e}")
    
    def refresh_stock_data(self):
        """
        从通达信获取股票数据并更新表格
        """
        try:
            logger.info("开始从通达信获取股票数据")
            
            # 默认显示上证指数数据
            self.on_index()
            
        except Exception as e:
            logger.exception(f"获取股票数据失败: {e}")
    
    def show_latest_5days_data(self):
        """
        显示最新5个交易日的上证指数和深证成指数据
        """
        try:
            logger.info("开始显示最新5个交易日的上证指数和深证成指数据")
            
            # 指数代码映射
            index_map = {
                "sh000001": "上证指数",
                "sz399001": "深证成指"
            }
            
            # 清空现有数据
            self.stock_table.setSortingEnabled(False)
            self.stock_table.setRowCount(0)
            
            # 设置表头 - 确保与行情窗口字段一致
            headers = ["日期", "代码", "名称", "涨跌幅", "涨跌额", "最高价", "最低价", "收盘价", "开盘价", "成交量"]
            self.stock_table.setColumnCount(len(headers))
            self.stock_table.setHorizontalHeaderLabels(headers)
            
            # 构建通达信指数数据目录路径
            from pathlib import Path
            tdx_data_path = Path(r"H:\zxzq\vipdoc")
            
            import struct
            from datetime import datetime
            
            # 存储所有数据，用于后续排序和显示
            all_data = []
            
            for index_code, index_name in index_map.items():
                try:
                    market = "sh" if index_code.startswith("sh") else "sz"
                    index_file = Path(tdx_data_path / market / 'lday' / f"{index_code}.day")
                    
                    if not index_file.exists():
                        logger.warning(f"未找到{index_name}的通达信指数文件: {index_file}")
                        continue
                    
                    with open(index_file, 'rb') as f:
                        # 获取文件大小
                        f.seek(0, 2)
                        file_size = f.tell()
                        
                        # 计算数据条数
                        record_count = file_size // 32
                        if record_count == 0:
                            continue
                        
                        # 确定要读取的记录数量（最多5条）
                        read_count = min(5, record_count)
                        
                        # 从最新记录开始读取
                        for i in range(read_count):
                            # 计算记录位置
                            record_pos = (record_count - 1 - i) * 32
                            f.seek(record_pos)
                            record = f.read(32)
                            
                            # 解析记录
                            date_int = struct.unpack('I', record[0:4])[0]  # 日期，格式：YYYYMMDD
                            open_val = struct.unpack('I', record[4:8])[0] / 100  # 开盘价
                            high_val = struct.unpack('I', record[8:12])[0] / 100  # 最高价
                            low_val = struct.unpack('I', record[12:16])[0] / 100  # 最低价
                            close_val = struct.unpack('I', record[16:20])[0] / 100  # 收盘价
                            volume = struct.unpack('I', record[20:24])[0]  # 成交量
                            amount = struct.unpack('I', record[24:28])[0] / 100  # 成交额
                            
                            # 转换日期格式
                            date_str = str(date_int)
                            date = datetime.strptime(date_str, '%Y%m%d').date()
                            date_display = date.strftime('%Y-%m-%d')
                            
                            # 计算涨跌额和涨跌幅
                            # 获取前一天的收盘价作为昨收
                            prev_close = 0.0
                            try:
                                if i < read_count - 1:
                                    # 如果不是最后一条记录（最旧的），使用下一条记录的收盘价作为昨收
                                    prev_pos = record_pos + 32
                                    f.seek(prev_pos)
                                    prev_record = f.read(32)
                                    if len(prev_record) == 32:
                                        prev_close = struct.unpack('I', prev_record[16:20])[0] / 100
                                    else:
                                        prev_close = close_val
                                elif record_count > read_count:
                                    # 如果还有更早的记录，使用更早的记录
                                    prev_pos = (record_count - 1 - read_count) * 32
                                    f.seek(prev_pos)
                                    prev_record = f.read(32)
                                    if len(prev_record) == 32:
                                        prev_close = struct.unpack('I', prev_record[16:20])[0] / 100
                                    else:
                                        prev_close = close_val
                                else:
                                    # 没有前一天数据，使用当前收盘价
                                    prev_close = close_val
                            except Exception as e:
                                # 任何错误都使用当前收盘价作为昨收
                                prev_close = close_val
                            
                            # 计算涨跌额和涨跌幅
                            change = close_val - prev_close
                            pct_chg = (change / prev_close) * 100 if prev_close != 0 else 0.0
                            
                            # 添加到数据列表
                            all_data.append({
                                "index_name": index_name,
                                "date": date,
                                "date_display": date_display,
                                "open": open_val,
                                "high": high_val,
                                "low": low_val,
                                "close": close_val,
                                "change": change,
                                "pct_chg": pct_chg
                            })
                except Exception as e:
                    logger.exception(f"解析{index_name}文件失败: {e}")
                    continue
            
            # 按日期排序（最新日期在前）
            all_data.sort(key=lambda x: x["date"], reverse=True)
            
            # 添加到表格
            self.stock_table.setRowCount(len(all_data))
            
            for row_idx, data in enumerate(all_data):
                # 指数名称
                item = QTableWidgetItem(data["index_name"])
                item.setTextAlignment(Qt.AlignCenter)
                self.stock_table.setItem(row_idx, 0, item)
                
                # 日期
                item = QTableWidgetItem(data["date_display"])
                item.setTextAlignment(Qt.AlignCenter)
                self.stock_table.setItem(row_idx, 1, item)
                
                # 开盘价
                item = QTableWidgetItem(f"{data['open']:.2f}")
                item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                self.stock_table.setItem(row_idx, 2, item)
                
                # 最高价
                item = QTableWidgetItem(f"{data['high']:.2f}")
                item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                self.stock_table.setItem(row_idx, 3, item)
                
                # 最低价
                item = QTableWidgetItem(f"{data['low']:.2f}")
                item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                self.stock_table.setItem(row_idx, 4, item)
                
                # 收盘价
                item = QTableWidgetItem(f"{data['close']:.2f}")
                item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                self.stock_table.setItem(row_idx, 5, item)
                
                # 涨跌额
                item = QTableWidgetItem(f"{data['change']:+.2f}")
                item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                # 设置颜色：上涨红色，下跌绿色
                if data['change'] > 0:
                    item.setForeground(QColor(255, 0, 0))
                elif data['change'] < 0:
                    item.setForeground(QColor(0, 200, 0))
                self.stock_table.setItem(row_idx, 6, item)
                
                # 涨跌幅
                item = QTableWidgetItem(f"{data['pct_chg']:+.2f}")
                item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                # 设置颜色：上涨红色，下跌绿色
                if data['pct_chg'] > 0:
                    item.setForeground(QColor(255, 0, 0))
                elif data['pct_chg'] < 0:
                    item.setForeground(QColor(0, 200, 0))
                self.stock_table.setItem(row_idx, 7, item)
            
            # 调整列宽
            for col in range(8):
                self.stock_table.resizeColumnToContents(col)
            
            # 启用排序
            self.stock_table.setSortingEnabled(True)
            
            logger.info(f"最新5个交易日的数据显示完成，共显示{len(all_data)}条记录")
            
        except Exception as e:
            logger.exception(f"显示最新5个交易日数据失败: {e}")
    
    def refresh_market_info(self):
        """
        从通达信获取市场指数信息并更新状态栏
        """
        try:
            logger.info("开始从通达信获取市场指数信息")
            
            # 主要指数代码映射
            index_map = {
                "sh000001": "上证指数",
                "sz399001": "深证成指",
                "sz399006": "创业板指"
            }
            
            # 构建通达信指数数据目录路径
            from pathlib import Path
            tdx_data_path = Path(r"H:\zxzq\vipdoc")
            
            # 解析主要指数文件，获取最新数据
            index_info = []
            import struct
            from datetime import datetime
            
            for index_code, index_name in index_map.items():
                try:
                    market = "sh" if index_code.startswith("sh") else "sz"
                    index_file = Path(tdx_data_path / market / 'lday' / f"{index_code}.day")
                    
                    if not index_file.exists():
                        continue
                    
                    with open(index_file, 'rb') as f:
                        # 获取文件大小
                        f.seek(0, 2)
                        file_size = f.tell()
                        
                        # 计算数据条数
                        record_count = file_size // 32
                        if record_count < 2:
                            continue
                        
                        # 读取最新两条记录
                        f.seek((record_count - 2) * 32)
                        prev_record = f.read(32)
                        f.seek((record_count - 1) * 32)
                        latest_record = f.read(32)
                        
                        # 解析记录
                        latest_close = struct.unpack('I', latest_record[16:20])[0] / 100  # 最新收盘价
                        prev_close = struct.unpack('I', prev_record[16:20])[0] / 100  # 前一天收盘价
                        
                        # 计算涨跌幅
                        change = latest_close - prev_close
                        pct_chg = (change / prev_close) * 100 if prev_close != 0 else 0.0
                        
                        # 格式化显示
                        pct_chg_str = f"{pct_chg:+.2f}%" if pct_chg != 0 else "0.00%"
                        index_info.append(f"{index_name}: {latest_close:.2f} {pct_chg_str}")
                except Exception as e:
                    logger.exception(f"解析{index_name}文件失败: {e}")
                    continue
            
            # 更新状态栏
            if index_info:
                self.market_info_label.setText(" | ".join(index_info))
            
            logger.info("市场指数信息更新完成")
            
        except Exception as e:
            logger.exception(f"获取市场指数信息失败: {e}")
    
    def draw_kdj_indicator(self, plot_widget, x, df_pd):
        """
        绘制KDJ指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pd: pandas DataFrame，包含kdj数据
        """
        # 导入pyqtgraph
        import pyqtgraph as pg
        from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
        
        # 确保KDJ相关列存在
        if 'k' not in df_pd.columns or 'd' not in df_pd.columns or 'j' not in df_pd.columns:
            # 使用TechnicalAnalyzer计算KDJ指标
            analyzer = TechnicalAnalyzer(df_pd)
            analyzer.calculate_kdj(14)
            df_pd = analyzer.get_data()
        
        # 设置KDJ指标图的Y轴范围，考虑到KDJ可能超出0-100范围，特别是J值
        plot_widget.setYRange(-50, 150)
        # 绘制K线（白色）
        plot_widget.plot(x, df_pd['k'].values, pen=pg.mkPen('w', width=1), name='K')
        # 绘制D线（黄色）
        plot_widget.plot(x, df_pd['d'].values, pen=pg.mkPen('y', width=1), name='D')
        # 绘制J线（紫色）
        plot_widget.plot(x, df_pd['j'].values, pen=pg.mkPen('m', width=1), name='J')
        # 绘制超买超卖线
        plot_widget.addItem(pg.InfiniteLine(pos=20, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine)))
        plot_widget.addItem(pg.InfiniteLine(pos=80, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine)))
    
    def draw_rsi_indicator(self, plot_widget, x, df_pd):
        """
        绘制RSI指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pd: pandas DataFrame，包含rsi数据
        """
        # 导入pyqtgraph
        import pyqtgraph as pg
        from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
        
        # 确保RSI相关列存在
        if 'rsi14' not in df_pd.columns:
            # 使用TechnicalAnalyzer计算RSI指标
            analyzer = TechnicalAnalyzer(df_pd)
            analyzer.calculate_rsi(14)
            df_pd = analyzer.get_data()
        
        # 绘制RSI指标
        plot_widget.setYRange(0, 100)
        # 绘制RSI线（蓝色）
        plot_widget.plot(x, df_pd['rsi14'].values, pen=pg.mkPen('b', width=1), name='RSI14')
        # 绘制超买超卖线
        plot_widget.addItem(pg.InfiniteLine(pos=30, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine)))
        plot_widget.addItem(pg.InfiniteLine(pos=70, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine)))
    
    def draw_macd_indicator(self, plot_widget, x, df_pd):
        """
        绘制MACD指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pd: pandas DataFrame，包含macd数据
        """
        # 导入pyqtgraph
        import pyqtgraph as pg
        from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
        
        # 确保MACD相关列存在
        if 'macd' not in df_pd.columns or 'macd_signal' not in df_pd.columns or 'macd_hist' not in df_pd.columns:
            # 使用TechnicalAnalyzer计算MACD指标
            analyzer = TechnicalAnalyzer(df_pd)
            analyzer.calculate_macd()
            df_pd = analyzer.get_data()
        
        # 绘制MACD指标
        plot_widget.setYRange(min(df_pd['macd_hist'].min(), df_pd['macd'].min(), df_pd['macd_signal'].min()) * 1.2, 
                             max(df_pd['macd_hist'].max(), df_pd['macd'].max(), df_pd['macd_signal'].max()) * 1.2)
        # 绘制MACD线（蓝色）
        plot_widget.plot(x, df_pd['macd'].values, pen=pg.mkPen('b', width=1), name='MACD')
        # 绘制信号线（红色）
        plot_widget.plot(x, df_pd['macd_signal'].values, pen=pg.mkPen('r', width=1), name='Signal')
        # 绘制柱状图
        for i in range(len(x)):
            if df_pd['macd_hist'].values[i] >= 0:
                color = 'r'  # 上涨为红色
            else:
                color = 'g'  # 下跌为绿色
            plot_widget.plot([x[i], x[i]], [0, df_pd['macd_hist'].values[i]], pen=pg.mkPen(color, width=2))
    
    def draw_vol_indicator(self, plot_widget, x, df_pd):
        """
        绘制VOL指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pd: pandas DataFrame，包含成交量数据
        """
        # 导入pyqtgraph
        import pyqtgraph as pg
        import numpy as np
        
        # 设置Y轴范围，确保从0开始
        volumes = df_pd['volume'].values
        if len(volumes) > 0:
            y_max = volumes.max() * 1.3  # 顶部留出30%空间
            plot_widget.setYRange(0, y_max)
        else:
            plot_widget.setYRange(0, 100)
        
        # 禁用科学计数法，使用正常数值显示
        y_axis = plot_widget.getAxis('left')
        y_axis.enableAutoSIPrefix(False)
        y_axis.setStyle(tickTextOffset=20)  # 设置刻度文本偏移，确保显示完整
        
        # 重置对数模式，默认使用线性刻度
        plot_widget.setLogMode(y=False)
        
        # 绘制成交量柱状图
        for i in range(len(x)):
            if i == 0:
                color = 'r'  # 默认第一个为红色
            else:
                if df_pd['close'].values[i] >= df_pd['close'].values[i-1]:
                    color = 'r'  # 上涨为红色
                else:
                    color = 'g'  # 下跌为绿色
            # 使用更窄的柱体，比K线图柱体窄
            bar_item = pg.BarGraphItem(x=[i], height=[volumes[i]], width=0.6, brush=pg.mkBrush(color))
            plot_widget.addItem(bar_item)
        
        # 绘制成交量5日均线（白色，与K线图MA5颜色一致）
        if 'vol_ma5' in df_pd.columns:
            vol_ma5_item = plot_widget.plot(x, df_pd['vol_ma5'].values, pen=pg.mkPen('w', width=1), name='VOL_MA5')
        
        # 绘制成交量10日均线（青色，与K线图MA10颜色一致）
        if 'vol_ma10' in df_pd.columns:
            vol_ma10_item = plot_widget.plot(x, df_pd['vol_ma10'].values, pen=pg.mkPen('c', width=1), name='VOL_MA10')
        
        # 绘制完成后，计算并强制设置正确的Y轴范围，使用真实的成交量数值
        if len(volumes) > 0:
            # 获取所有相关数据的最大值，包括成交量和均线
            max_value = volumes.max()
            if 'vol_ma5' in df_pd.columns:
                max_value = max(max_value, df_pd['vol_ma5'].max())
            if 'vol_ma10' in df_pd.columns:
                max_value = max(max_value, df_pd['vol_ma10'].max())
            
            y_min = 0  # 成交量不能为负
            y_max = max_value * 1.3  # 顶部留出30%空间
            
            # 强制设置Y轴范围，禁用padding
            plot_widget.setYRange(y_min, y_max, padding=0)
            
            # 直接设置Y轴刻度，确保显示真实的成交量数值
            y_axis = plot_widget.getAxis('left')
            y_axis.setScale(1.0)  # 重置缩放比例
            y_axis.setRange(y_min, y_max)
            
            # 设置Y轴刻度的格式，确保显示完整的数值
            y_axis.setTicks([[(0, '0'), (max_value * 0.5, f'{int(max_value * 0.5):,}'), (max_value, f'{int(max_value):,}')]])
        else:
            plot_widget.setYRange(0, 100, padding=0)
        
        # 获取ViewBox并禁用自动Y轴范围调整
        view_box = plot_widget.getViewBox()
        view_box.enableAutoRange(axis=pg.ViewBox.YAxis, enable=False)
        
        # 再次禁用科学计数法，确保Y轴显示正常数值
        y_axis = plot_widget.getAxis('left')
        y_axis.enableAutoSIPrefix(False)
        y_axis.setStyle(tickTextOffset=20)  # 设置刻度文本偏移，确保显示完整
        
        # 确保Y轴显示的是真实的成交量数值，而不是相对比例
        y_axis.setScale(1.0)  # 重置缩放比例
        y_axis.setRange(y_min, y_max)  # 重新设置Y轴范围
    
    def draw_k_line_indicator(self, plot_widget, df, dates, opens, highs, lows, closes, df_pd):
        """
        绘制K线图
        
        Args:
            plot_widget: 绘图控件
            df: 原始数据
            dates: 日期列表
            opens: 开盘价列表
            highs: 最高价列表
            lows: 最低价列表
            closes: 收盘价列表
            df_pd: pandas DataFrame，包含均线数据
        """
        # 导入pyqtgraph
        import pyqtgraph as pg
        import numpy as np

        from pyqtgraph import Point
        
        # 使用导入的CandleStickItem类
        
        # 创建x轴坐标（使用索引）
        x = np.arange(len(dates))
        
        # 创建K线图数据
        # K线图由OHLC数据组成：(x, open, high, low, close)
        ohlc = np.column_stack((x, opens, highs, lows, closes))
        
        # 转换为列表格式，适合自定义CandleStickItem
        ohlc_list = [tuple(row) for row in ohlc]
        
        # 创建K线图项
        candle_plot_item = CandleStickItem(ohlc_list)
        
        # 添加K线图到图表
        plot_widget.addItem(candle_plot_item)
        
        # 设置x轴刻度标签（显示日期）
        ax = plot_widget.getAxis('bottom')
        ax.setTicks([[(i, dates[i].strftime('%Y-%m-%d')) for i in range(0, len(dates), 10)]])
        
        # 设置Y轴范围，留出一定的边距
        y_min = np.min(lows) * 0.99
        y_max = np.max(highs) * 1.01
        plot_widget.setYRange(y_min, y_max)
        
        # 计算当前显示区域的最高、最低点及其位置
        current_high = np.max(highs)
        current_low = np.min(lows)
        high_index = np.argmax(highs)
        low_index = np.argmin(lows)
        
        # 清除之前的最高、最低点标注
        if hasattr(self, 'high_text_item'):
            plot_widget.removeItem(self.high_text_item)
        if hasattr(self, 'low_text_item'):
            plot_widget.removeItem(self.low_text_item)
        if hasattr(self, 'high_arrow_item'):
            plot_widget.removeItem(self.high_arrow_item)
        if hasattr(self, 'low_arrow_item'):
            plot_widget.removeItem(self.low_arrow_item)
        
        # 创建最高点标注，加上日期
        high_date = dates[high_index].strftime('%Y-%m-%d')
        self.high_text_item = pg.TextItem(f" {high_date} {current_high:.2f} ", color='w')
        self.high_text_item.setHtml(f'<div style="background-color: rgba(0, 0, 0, 0.8); padding: 3px; border: 1px solid #666; font-family: monospace; font-size: 10px;">{high_date} {current_high:.2f}</div>')
        plot_widget.addItem(self.high_text_item)
        
        # 创建最高点箭头 - 简单三角形箭头
        self.high_arrow_item = pg.ArrowItem(
            pos=(high_index, current_high), 
            angle=-45, 
            brush=pg.mkBrush('w'), 
            pen=pg.mkPen('w', width=1), 
            tipAngle=30, 
            headLen=8, 
            headWidth=6,
            tailLen=0,  # 没有尾巴
            tailWidth=1
        )
        plot_widget.addItem(self.high_arrow_item)
        
        # 创建最低点标注，加上日期
        low_date = dates[low_index].strftime('%Y-%m-%d')
        self.low_text_item = pg.TextItem(f" {low_date} {current_low:.2f} ", color='w')
        self.low_text_item.setHtml(f'<div style="background-color: rgba(0, 0, 0, 0.8); padding: 3px; border: 1px solid #666; font-family: monospace; font-size: 10px;">{low_date} {current_low:.2f}</div>')
        plot_widget.addItem(self.low_text_item)
        
        # 创建最低点箭头 - 简单三角形箭头
        self.low_arrow_item = pg.ArrowItem(
            pos=(low_index, current_low), 
            angle=45, 
            brush=pg.mkBrush('w'), 
            pen=pg.mkPen('w', width=1), 
            tipAngle=30, 
            headLen=8, 
            headWidth=6,
            tailLen=0,  # 没有尾巴
            tailWidth=1
        )
        plot_widget.addItem(self.low_arrow_item)
        
        # 定位标注位置（在对应柱图旁边）
        self.high_text_item.setPos(high_index + 0.5, current_high + (y_max - y_min) * 0.02)
        self.low_text_item.setPos(low_index + 0.5, current_low - (y_max - y_min) * 0.02)
        
        # 设置X轴范围，不使用autoRange，确保与成交量图一致
        plot_widget.setXRange(0, len(dates) - 1)
        
        # 初始化均线相关属性
        if not hasattr(self, 'moving_averages'):
            self.moving_averages = {}
        if not hasattr(self, 'selected_ma'):
            self.selected_ma = None
        if not hasattr(self, 'ma_points'):
            self.ma_points = []
        
        # 清除之前的标注点
        for point_item in self.ma_points:
            plot_widget.removeItem(point_item)
        self.ma_points.clear()
        
        # 绘制5日均线（白色）
        ma5_item = plot_widget.plot(x, df_pd['ma5'].values, pen=pg.mkPen('w', width=1), name='MA5')
        self.moving_averages['MA5'] = {'item': ma5_item, 'data': (x, df_pd['ma5'].values), 'color': 'w'}
        
        # 绘制10日均线（青色）
        ma10_item = plot_widget.plot(x, df_pd['ma10'].values, pen=pg.mkPen('c', width=1), name='MA10')
        self.moving_averages['MA10'] = {'item': ma10_item, 'data': (x, df_pd['ma10'].values), 'color': 'c'}
        
        # 绘制20日均线（红色）
        ma20_item = plot_widget.plot(x, df_pd['ma20'].values, pen=pg.mkPen('r', width=1), name='MA20')
        self.moving_averages['MA20'] = {'item': ma20_item, 'data': (x, df_pd['ma20'].values), 'color': 'r'}
        
        # 绘制60日均线（绿色）
        ma60_item = plot_widget.plot(x, df_pd['ma60'].values, pen=pg.mkPen('g', width=1), name='MA60')
        self.moving_averages['MA60'] = {'item': ma60_item, 'data': (x, df_pd['ma60'].values), 'color': 'g'}
        
        # 保存当前鼠标位置和K线索引
        self.current_kline_data = {
            'dates': dates,
            'opens': opens,
            'highs': highs,
            'lows': lows,
            'closes': closes
        }
        
        # 保存计算好的MA值和颜色，用于鼠标移动时更新显示
        self.ma_data = {
            'MA5': df_pd['ma5'].values.tolist(),
            'MA10': df_pd['ma10'].values.tolist(),
            'MA20': df_pd['ma20'].values.tolist(),
            'MA60': df_pd['ma60'].values.tolist()
        }
        
        # 保存MA线的颜色映射，使用与绘制线条一致的颜色值
        self.ma_colors = {
            'MA5': 'white',
            'MA10': 'cyan',
            'MA20': 'red',
            'MA60': '#00FF00'  # 使用亮绿色，与pyqtgraph的'g'颜色一致
        }
        
        # 保存K线图数据项
        self.candle_plot_item = candle_plot_item
    
    def draw_indicator(self, plot_widget, indicator_name, x, df_pd):
        """
        根据指标名称绘制相应的指标
        
        Args:
            plot_widget: 绘图控件
            indicator_name: 指标名称
            x: x轴数据
            df_pd: pandas DataFrame，包含指标数据
        """
        # 导入pyqtgraph
        import pyqtgraph as pg
        
        # 清除之前的内容
        plot_widget.clear()
        # 设置图的样式
        plot_widget.setBackground('#000000')
        plot_widget.setLabel('left', indicator_name, color='#C0C0C0')
        plot_widget.setLabel('bottom', '', color='#C0C0C0')
        plot_widget.getAxis('left').setPen(pg.mkPen('#C0C0C0'))
        plot_widget.getAxis('bottom').setPen(pg.mkPen('#C0C0C0'))
        plot_widget.getAxis('left').setTextPen(pg.mkPen('#C0C0C0'))
        plot_widget.getAxis('bottom').setTextPen(pg.mkPen('#C0C0C0'))
        plot_widget.showGrid(x=True, y=True, alpha=0.3)
        
        # 使用字典映射替代条件判断，设置初始Y轴范围
        indicator_y_ranges = {
            "VOL": (0, 1000000000),  # 成交量指标范围
            "MACD": (-5, 5),  # MACD指标范围
            "KDJ": (-50, 150),  # KDJ指标范围
            "RSI": (-50, 150)  # RSI指标范围
        }
        
        if indicator_name in indicator_y_ranges:
            y_min, y_max = indicator_y_ranges[indicator_name]
            plot_widget.setYRange(y_min, y_max)
            
            # 特殊处理VOL指标的对数模式
            if indicator_name == "VOL":
                plot_widget.setLogMode(y=False)
        
        # 使用字典映射替代条件判断，调用相应的绘制函数
        indicator_drawers = {
            "KDJ": self.draw_kdj_indicator,
            "RSI": self.draw_rsi_indicator,
            "MACD": self.draw_macd_indicator,
            "VOL": self.draw_vol_indicator
        }
        
        # 获取绘制函数，如果没有匹配到，使用默认绘制函数
        drawer = indicator_drawers.get(indicator_name, self.draw_kdj_indicator)
        drawer(plot_widget, x, df_pd)
    
    def update_indicator_values_label(self, indicator_name, df_pd):
        """
        更新指标值显示标签
        
        Args:
            indicator_name: 指标名称
            df_pd: pandas DataFrame，包含指标数据
        """
        # 创建指标值显示标签
        if not hasattr(self, 'kdj_values_label'):
            # 创建新的标签
            self.kdj_values_label = QLabel()
            self.kdj_values_label.setStyleSheet("font-family: Consolas, monospace; background-color: rgba(0, 0, 0, 0.5); padding: 5px; color: #C0C0C0;")
            # 确保不换行
            self.kdj_values_label.setWordWrap(False)
        
        # 根据当前指标更新标签文本
        if indicator_name == "KDJ":
            # 获取最新的KDJ值
            latest_k = df_pd['k'].iloc[-1]
            latest_d = df_pd['d'].iloc[-1]
            latest_j = df_pd['j'].iloc[-1]
            # 更新标签文本
            kdj_text = f"<font color='white'>K: {latest_k:.2f}</font>  <font color='yellow'>D: {latest_d:.2f}</font>  <font color='magenta'>J: {latest_j:.2f}</font>"
            self.kdj_values_label.setText(kdj_text)
        elif indicator_name == "RSI":
            # 获取最新的RSI值
            latest_rsi = df_pd['rsi14'].iloc[-1]
            # 更新标签文本
            rsi_text = f"<font color='blue'>RSI14: {latest_rsi:.2f}</font>"
            self.kdj_values_label.setText(rsi_text)
        elif indicator_name == "MACD":
            # 获取最新的MACD值
            latest_macd = df_pd['macd'].iloc[-1]
            latest_macd_signal = df_pd['macd_signal'].iloc[-1]
            latest_macd_hist = df_pd['macd_hist'].iloc[-1]
            # 更新标签文本
            macd_text = f"<font color='blue'>MACD: {latest_macd:.2f}</font>  <font color='red'>SIGNAL: {latest_macd_signal:.2f}</font>  <font color='#C0C0C0'>HIST: {latest_macd_hist:.2f}</font>"
            self.kdj_values_label.setText(macd_text)
        else:
            # 默认绘制KDJ指标，显示KDJ数值
            latest_k = df_pd['k'].iloc[-1]
            latest_d = df_pd['d'].iloc[-1]
            latest_j = df_pd['j'].iloc[-1]
            # 更新标签文本
            kdj_text = f"<font color='white'>K: {latest_k:.2f}</font>  <font color='yellow'>D: {latest_d:.2f}</font>  <font color='magenta'>J: {latest_j:.2f}</font>"
            self.kdj_values_label.setText(kdj_text)
    
    def save_indicator_data(self, df_pd):
        """
        保存指标数据，用于鼠标移动时更新指标数值
        
        Args:
            df_pd: pandas DataFrame，包含指标数据
        """
        self.current_kdj_data = {
            'k': df_pd['k'].values.tolist(),
            'd': df_pd['d'].values.tolist(),
            'j': df_pd['j'].values.tolist()
        }
        self.current_rsi_data = {
            'rsi': df_pd['rsi14'].values.tolist()
        }
        self.current_macd_data = {
            'macd': df_pd['macd'].values.tolist(),
            'macd_signal': df_pd['macd_signal'].values.tolist(),
            'macd_hist': df_pd['macd_hist'].values.tolist()
        }
           