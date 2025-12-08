#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
主窗口类，参考通达信软件界面设计
"""

from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QSplitter,
    QTreeWidget, QTreeWidgetItem, QTableWidget, QTableWidgetItem,
    QTabWidget, QPushButton, QLineEdit, QLabel, QStatusBar,
    QMenuBar, QMenu, QDialog, QCheckBox, QDateEdit, QComboBox,
    QProgressBar, QAbstractItemView
)
from PySide6.QtCore import Qt, QSize
from PySide6.QtGui import QAction, QIcon, QFont, QColor

from src.utils.logger import logger


class MainWindow(QMainWindow):
    """
    主窗口类，参考通达信软件界面设计
    """
    
    def __init__(self, config, data_manager):
        """
        初始化主窗口
        
        Args:
            config: 配置对象
            data_manager: 数据管理器实例
        """
        super().__init__()
        self.config = config
        self.data_manager = data_manager
        self.setWindowTitle("中国股市量化分析系统")
        self.setGeometry(100, 100, 1400, 900)
        
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
        
        # 默认选中日线
        self.period_buttons['日线'].setChecked(True)
        self.current_period = '日线'
        
        # 添加分隔符
        toolbar_layout.addStretch()
        
        # 添加工具栏到布局
        tech_layout.addWidget(toolbar)
        
        # 创建pyqtgraph图表
        self.tech_plot_widget = pg.PlotWidget()
        self.tech_plot_widget.setBackground('#000000')
        self.tech_plot_widget.setLabel('left', '价格', color='#C0C0C0')
        self.tech_plot_widget.setLabel('bottom', '日期', color='#C0C0C0')
        self.tech_plot_widget.getAxis('left').setPen(pg.mkPen('#C0C0C0'))
        self.tech_plot_widget.getAxis('bottom').setPen(pg.mkPen('#C0C0C0'))
        self.tech_plot_widget.getAxis('left').setTextPen(pg.mkPen('#C0C0C0'))
        self.tech_plot_widget.getAxis('bottom').setTextPen(pg.mkPen('#C0C0C0'))
        self.tech_plot_widget.showGrid(x=True, y=True, alpha=0.3)
        
        # 保存k线图数据项
        self.candle_plot_item = None
        
        tech_layout.addWidget(self.tech_plot_widget)
    
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
            # TODO: 根据周期更新K线图数据
            print(f"切换到{period}")
    
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
    
    def plot_k_line(self, df, stock_name, stock_code):
        """
        使用pyqtgraph绘制K线图
        
        Args:
            df: 股票历史数据DataFrame
            stock_name: 股票名称
            stock_code: 股票代码
        """
        try:
            import pyqtgraph as pg
            import numpy as np
            from pyqtgraph import GraphicsObject
            from pyqtgraph import Point
            
            # 自定义K线图项类
            class CandleStickItem(GraphicsObject):
                def __init__(self, data):
                    GraphicsObject.__init__(self)
                    self.data = data  # data must be a list of tuples (x, open, high, low, close)
                    self.generatePicture()
                
                def generatePicture(self):
                    self.picture = pg.QtGui.QPicture()
                    p = pg.QtGui.QPainter(self.picture)
                    for (t, open_val, high_val, low_val, close_val) in self.data:
                        if close_val >= open_val:
                            # 上涨，红色
                            color = 'r'
                        else:
                            # 下跌，绿色
                            color = 'g'
                        
                        # 绘制实体部分，不显示边框
                        p.setPen(pg.mkPen(color, width=0))  # 设置宽度为0，不绘制边框
                        p.setBrush(pg.mkBrush(color))
                        p.drawRect(pg.QtCore.QRectF(t-0.3, open_val, 0.6, close_val-open_val))
                        
                        # 绘制上下影线，使用与实体相同的颜色
                        p.setPen(pg.mkPen(color, width=1))  # 使用1像素宽度的线条
                        p.setBrush(pg.mkBrush(color))
                        p.drawLine(pg.QtCore.QPointF(t, high_val), pg.QtCore.QPointF(t, low_val))
                    p.end()
                
                def paint(self, p, *args):
                    p.drawPicture(0, 0, self.picture)
                
                def boundingRect(self):
                    # 边界矩形
                    return pg.QtCore.QRectF(self.picture.boundingRect())
            
            # 清空图表
            self.tech_plot_widget.clear()
            
            # 设置图表标题
            self.tech_plot_widget.setTitle(f"{stock_name}({stock_code}) K线图", color='#C0C0C0', size='14pt')
            
            # 准备K线图数据
            dates = df['date'].to_list()
            opens = df['open'].to_list()
            highs = df['high'].to_list()
            lows = df['low'].to_list()
            closes = df['close'].to_list()
            
            # 只显示最近100个交易日的数据
            if len(dates) > 100:
                dates = dates[-100:]
                opens = opens[-100:]
                highs = highs[-100:]
                lows = lows[-100:]
                closes = closes[-100:]
            
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
            
            # 设置x轴标签（显示日期）
            ax = self.tech_plot_widget.getAxis('bottom')
            ax.setTicks([[(i, dates[i].strftime('%Y-%m-%d')) for i in range(0, len(dates), 10)]])
            
            # 设置Y轴范围，留出一定的边距
            y_min = np.min(lows) * 0.99
            y_max = np.max(highs) * 1.01
            self.tech_plot_widget.setYRange(y_min, y_max)
            
            logger.info(f"成功绘制{stock_name}({stock_code})的K线图")
            self.statusBar().showMessage(f"成功绘制{stock_name}({stock_code})的K线图", 3000)
            
        except Exception as e:
            logger.exception(f"绘制K线图失败: {e}")
            self.statusBar().showMessage(f"绘制K线图失败: {str(e)[:50]}...", 5000)
    
    def on_nav_item_clicked(self, item, column):
        """
        处理导航项点击事件，显示对应的行情数据
        """
        text = item.text(column)
        logger.info(f"点击了导航项: {text}")
        
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
            sh_stock_files = list(Path(tdx_data_path / 'vipdoc' / 'sh' / 'lday').glob('sh*.day')) if (tdx_data_path / 'vipdoc' / 'sh' / 'lday').exists() else []
            sz_stock_files = list(Path(tdx_data_path / 'vipdoc' / 'sz' / 'lday').glob('sz*.day')) if (tdx_data_path / 'vipdoc' / 'sz' / 'lday').exists() else []
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
            index_file = Path(tdx_data_path / 'vipdoc' / market / 'lday' / f"{index_code}.day")
            
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
           