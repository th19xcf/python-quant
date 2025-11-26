#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
主窗口类，参考通达信软件界面设计
"""

from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QSplitter,
    QTreeWidget, QTreeWidgetItem, QTableWidget, QTableWidgetItem,
    QTabWidget, QPushButton, QLineEdit, QLabel, QStatusBar,
    QMenuBar, QMenu
)
from PySide6.QtCore import Qt, QSize
from PySide6.QtGui import QAction, QIcon, QFont

import pandas as pd
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
        
        # 设置中间和右侧分割器比例
        center_right_splitter.setSizes([800, 400])
        
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
        
        # 创建股票列表
        self.stock_table = QTableWidget()
        self.stock_table.setColumnCount(16)
        self.stock_table.setHorizontalHeaderLabels([
            "代码", "名称", "涨跌幅%", "现价", "涨跌", "买价", "卖价", "总量",
            "现量", "涨速%", "换手率%", "今开", "最高", "最低", "昨收", "市值"
        ])
        
        # 设置列宽
        column_widths = [80, 100, 80, 80, 80, 80, 80, 100, 80, 80, 80, 80, 80, 80, 80, 100]
        for i, width in enumerate(column_widths):
            self.stock_table.setColumnWidth(i, width)
        
        # 添加示例数据
        self.add_sample_stock_data()
        
        market_layout.addWidget(self.stock_table)
    
    def add_sample_stock_data(self):
        """
        添加示例股票数据
        """
        sample_data = [
            ["999999", "上证指数", "-0.15", "3864.18", "-5.84", "3864.18", "3864.19", "458亿", "1648", "0.03", "0.96", "3867.43", "3879.92", "3861.18", "3870.02", "7010亿"],
            ["600030", "中信证券", "+0.47", "27.60", "+0.13", "27.60", "27.61", "681695", "8464", "-0.03", "0.56", "27.52", "27.72", "27.44", "27.47", "1325亿"],
            ["000001", "平安银行", "+1.23", "18.95", "+0.23", "18.95", "18.96", "1234567", "15678", "+0.10", "0.89", "18.75", "19.02", "18.70", "18.72", "2840亿"],
            ["000858", "五粮液", "-0.89", "178.50", "-1.60", "178.50", "178.51", "345678", "4567", "-0.23", "0.45", "179.80", "180.20", "178.00", "180.10", "7890亿"],
            ["600519", "贵州茅台", "+0.56", "1890.00", "+10.50", "1890.00", "1890.01", "12345", "1234", "+0.05", "0.08", "1880.00", "1895.00", "1875.00", "1879.50", "23700亿"],
        ]
        
        self.stock_table.setRowCount(len(sample_data))
        
        for row, data in enumerate(sample_data):
            for col, value in enumerate(data):
                item = QTableWidgetItem(value)
                
                # 设置对齐方式
                if col in [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]:
                    item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                
                # 设置涨跌幅颜色
                if col == 2:
                    if value.startswith("+"):
                        item.setForeground(Qt.red)
                    elif value.startswith("-"):
                        item.setForeground(Qt.green)
                
                self.stock_table.setItem(row, col, item)
    
    def create_tech_tab(self):
        """
        创建技术分析标签页
        """
        tech_layout = QVBoxLayout(self.tech_tab)
        
        # 添加技术分析图表占位符
        tech_label = QLabel("技术分析图表区域")
        tech_label.setAlignment(Qt.AlignCenter)
        tech_label.setStyleSheet("font-size: 16px; color: #666;")
        tech_layout.addWidget(tech_label)
    
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
    
    # 菜单和按钮事件处理
    def on_self_selected(self):
        logger.info("点击了自选股")
    
    def on_panorama(self):
        logger.info("点击了全景图")
    
    def on_index(self):
        """
        点击了沪深京指数，从AkShare获取指数数据并更新表格
        """
        try:
            logger.info("开始从AkShare获取沪深京指数数据")
            
            # 使用AkShare获取指数实时行情
            import akshare as ak
            import requests
            
            # 获取主要指数代码列表
            index_codes = ["000001", "399001", "399006", "000688", "000016", "000300", "000905", "000852"]
            
            # 使用历史数据作为替代
            import datetime as dt
            end_date = dt.datetime.now().strftime("%Y%m%d")
            start_date = (dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y%m%d")
            
            # 清空现有数据
            self.stock_table.setRowCount(0)
            
            # 获取每个指数的数据
            for code in index_codes:
                try:
                    # 获取指数历史数据
                    try:
                        index_daily = ak.index_zh_a_hist(
                            symbol=code,
                            period="daily",
                            start_date=start_date,
                            end_date=end_date
                        )
                    except requests.exceptions.ConnectionError as conn_e:
                        logger.warning(f"网络连接失败，无法获取{code}的数据: {conn_e}")
                        continue
                    except Exception as e:
                        logger.exception(f"获取{code}数据失败: {e}")
                        continue
                    
                    if index_daily.empty:
                        logger.warning(f"没有找到{code}的历史数据")
                        continue
                    
                    # 获取最新一条数据
                    row = index_daily.iloc[0]
                    
                    # 指数名称映射
                    index_name_map = {
                        "000001": "上证指数",
                        "399001": "深证成指",
                        "399006": "创业板指",
                        "000688": "科创板指",
                        "000016": "上证50",
                        "000300": "沪深300",
                        "000905": "中证500",
                        "000852": "中证1000"
                    }
                    
                    name = index_name_map.get(code, code)
                    
                    # 构建数据行
                    data_row = [
                        code,
                        name,
                        f"{row['涨跌幅']:.2f}%" if not pd.isna(row['涨跌幅']) else "0.00%",
                        f"{row['收盘']:.2f}" if not pd.isna(row['收盘']) else "0.00",
                        f"{row['涨跌额']:.2f}" if not pd.isna(row['涨跌额']) else "0.00",
                        "-",  # 买一价
                        "-",  # 卖一价
                        f"{row['成交量']:,}" if not pd.isna(row['成交量']) else "0",
                        "-",  # 现量
                        "-",  # 涨速
                        "-",  # 换手率
                        f"{row['开盘']:.2f}" if not pd.isna(row['开盘']) else "0.00",
                        f"{row['最高']:.2f}" if not pd.isna(row['最高']) else "0.00",
                        f"{row['最低']:.2f}" if not pd.isna(row['最低']) else "0.00",
                        f"{row['昨收']:.2f}" if not pd.isna(row['昨收']) else "0.00",
                        "-"  # 总市值
                    ]
                    
                    # 添加行
                    row_pos = self.stock_table.rowCount()
                    self.stock_table.insertRow(row_pos)
                    
                    # 设置数据
                    for col, value in enumerate(data_row):
                        item = QTableWidgetItem(value)
                        
                        # 设置对齐方式
                        if col in [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]:
                            item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                        
                        # 设置涨跌幅颜色
                        if col == 2:
                            if value.startswith("+") or (value.replace("%", "").replace(".", "").isdigit() and float(value.replace("%", "")) > 0):
                                item.setForeground(Qt.red)
                            elif value.startswith("-"):
                                item.setForeground(Qt.green)
                        
                        self.stock_table.setItem(row_pos, col, item)
                    
                except Exception as e:
                    logger.exception(f"获取{code}的指数数据失败: {e}")
                    continue
            
            logger.info("沪深京指数数据更新完成")
            
        except Exception as e:
            logger.exception(f"获取沪深京指数数据失败: {e}")
    
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
        except Exception as e:
            logger.exception(f"处理导航项点击事件失败: {e}")
    
    def show_index_data(self, index_name):
        """
        显示指定指数的数据
        
        Args:
            index_name: 指数名称，如"上证指数"、"深证成指"等
        """
        try:
            logger.info(f"开始显示{index_name}的数据")
            
            # 使用AkShare获取指数数据
            import akshare as ak
            import requests
            
            # 指数代码映射
            index_code_map = {
                "上证指数": "000001",
                "深证成指": "399001",
                "创业板指": "399006",
                "科创板指": "000688"
            }
            
            if index_name not in index_code_map:
                logger.warning(f"不支持的指数名称: {index_name}")
                return
            
            index_code = index_code_map[index_name]
            
            # 获取指数历史日线数据
            import datetime as dt
            end_date = dt.datetime.now().strftime("%Y%m%d")
            start_date = (dt.datetime.now() - dt.timedelta(days=30)).strftime("%Y%m%d")
            
            try:
                index_daily = ak.index_zh_a_hist(
                    symbol=index_code,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date
                )
            except requests.exceptions.ConnectionError as conn_e:
                logger.warning(f"网络连接失败，无法获取{index_name}的数据: {conn_e}")
                return
            except Exception as e:
                logger.exception(f"获取{index_name}数据失败: {e}")
                return
            
            if index_daily.empty:
                logger.warning(f"没有找到{index_name}的历史数据")
                return
            
            logger.info(f"获取到{index_name}的{len(index_daily)}条历史数据")
            
            # 清空现有数据
            self.stock_table.setRowCount(0)
            
            # 添加指数历史数据
            for index, row in index_daily.iterrows():
                # 构建数据行
                data_row = [
                    index_code,  # 代码
                    index_name,  # 名称
                    f"{row['涨跌幅']:.2f}%" if not pd.isna(row['涨跌幅']) else "0.00%",
                    f"{row['收盘']:.2f}" if not pd.isna(row['收盘']) else "0.00",
                    f"{row['涨跌额']:.2f}" if not pd.isna(row['涨跌额']) else "0.00",
                    "-",  # 买一价
                    "-",  # 卖一价
                    f"{row['成交量']:,}" if not pd.isna(row['成交量']) else "0",
                    "-",  # 现量
                    "-",  # 涨速
                    "-",  # 换手率
                    f"{row['开盘']:.2f}" if not pd.isna(row['开盘']) else "0.00",
                    f"{row['最高']:.2f}" if not pd.isna(row['最高']) else "0.00",
                    f"{row['最低']:.2f}" if not pd.isna(row['最低']) else "0.00",
                    f"{row['昨收']:.2f}" if not pd.isna(row['昨收']) else "0.00",
                    "-"  # 总市值
                ]
                
                # 添加行
                row_pos = self.stock_table.rowCount()
                self.stock_table.insertRow(row_pos)
                
                # 设置数据
                for col, value in enumerate(data_row):
                    item = QTableWidgetItem(value)
                    
                    # 设置对齐方式
                    if col in [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]:
                        item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                    
                    # 设置涨跌幅颜色
                    if col == 2:
                        if value.startswith("+") or (value.replace("%", "").replace(".", "").isdigit() and float(value.replace("%", "")) > 0):
                            item.setForeground(Qt.red)
                        elif value.startswith("-"):
                            item.setForeground(Qt.green)
                    
                    self.stock_table.setItem(row_pos, col, item)
            
            logger.info(f"{index_name}数据显示完成")
            
        except Exception as e:
            logger.exception(f"显示{index_name}数据失败: {e}")
    
    def refresh_stock_data(self):
        """
        从AkShare获取实时股票数据并更新表格
        """
        try:
            logger.info("开始从AkShare获取实时股票数据")
            
            # 使用AkShare获取实时股票数据
            import akshare as ak
            import requests
            
            # 获取沪深京A股实时行情
            try:
                realtime_data = ak.stock_zh_a_spot()
            except requests.exceptions.ConnectionError as conn_e:
                logger.warning(f"网络连接失败，无法获取实时股票数据: {conn_e}")
                return
            except Exception as e:
                logger.exception(f"获取实时股票数据失败: {e}")
                return
            
            if realtime_data.empty:
                logger.warning("从AkShare获取的实时股票数据为空")
                return
            
            logger.info(f"从AkShare获取到{len(realtime_data)}条实时股票数据")
            
            # 清空现有数据
            self.stock_table.setRowCount(0)
            
            # 添加实时数据
            for index, row in realtime_data.iterrows():
                # 只显示部分数据，最多显示100条
                if index >= 100:
                    break
                
                # 构建数据行
                data_row = [
                    row['代码'],
                    row['名称'],
                    f"{row['涨跌幅']:.2f}%" if not pd.isna(row['涨跌幅']) else "0.00%",
                    f"{row['最新价']:.2f}" if not pd.isna(row['最新价']) else "0.00",
                    f"{row['涨跌额']:.2f}" if not pd.isna(row['涨跌额']) else "0.00",
                    f"{row['买入']:.2f}" if not pd.isna(row['买入']) else "0.00",
                    f"{row['卖出']:.2f}" if not pd.isna(row['卖出']) else "0.00",
                    f"{row['成交量']:,}" if not pd.isna(row['成交量']) else "0",
                    "-",  # 现量
                    "-",  # 涨速
                    "-",  # 换手率
                    f"{row['今开']:.2f}" if not pd.isna(row['今开']) else "0.00",
                    f"{row['最高']:.2f}" if not pd.isna(row['最高']) else "0.00",
                    f"{row['最低']:.2f}" if not pd.isna(row['最低']) else "0.00",
                    f"{row['昨收']:.2f}" if not pd.isna(row['昨收']) else "0.00",
                    "-"  # 总市值
                ]
                
                # 添加行
                row_pos = self.stock_table.rowCount()
                self.stock_table.insertRow(row_pos)
                
                # 设置数据
                for col, value in enumerate(data_row):
                    item = QTableWidgetItem(value)
                    
                    # 设置对齐方式
                    if col in [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]:
                        item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                    
                    # 设置涨跌幅颜色
                    if col == 2:
                        if value.startswith("+") or (value.replace("%", "").replace(".", "").isdigit() and float(value.replace("%", "")) > 0):
                            item.setForeground(Qt.red)
                        elif value.startswith("-"):
                            item.setForeground(Qt.green)
                    
                    self.stock_table.setItem(row_pos, col, item)
            
            logger.info("实时股票数据更新完成")
            
        except Exception as e:
            logger.exception(f"获取实时股票数据失败: {e}")
    
    def refresh_market_info(self):
        """
        从AkShare获取市场指数信息并更新状态栏
        """
        try:
            logger.info("开始从AkShare获取市场指数信息")
            
            # 使用AkShare获取市场指数实时行情
            import akshare as ak
            import requests
            
            # 获取主要指数数据
            try:
                # 尝试使用正确的方法名获取指数数据
                try:
                    index_data = ak.index_zh_a_spot()
                except requests.exceptions.ConnectionError as conn_e:
                    logger.warning(f"网络连接失败，尝试使用替代方法: {conn_e}")
                    # 使用历史数据作为替代
                    import datetime as dt
                    end_date = dt.datetime.now().strftime("%Y%m%d")
                    start_date = (dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y%m%d")
                    
                    # 获取上证指数
                    try:
                        sh_data = ak.index_zh_a_hist(symbol="000001", period="daily", start_date=start_date, end_date=end_date)
                        sz_data = ak.index_zh_a_hist(symbol="399001", period="daily", start_date=start_date, end_date=end_date)
                        cy_data = ak.index_zh_a_hist(symbol="399006", period="daily", start_date=start_date, end_date=end_date)
                    except requests.exceptions.ConnectionError as conn_e2:
                        logger.warning(f"网络连接失败，无法获取指数历史数据: {conn_e2}")
                        return
                    except Exception as e:
                        logger.exception(f"获取指数历史数据失败: {e}")
                        return
                    
                    # 构建状态栏信息
                    status_info = []
                    
                    # 上证指数
                    if not sh_data.empty:
                        sh_row = sh_data.iloc[0]
                        change_pct = sh_row['涨跌幅']
                        change_pct_str = f"{change_pct:.2f}%" if not pd.isna(change_pct) else "0.00%"
                        status_info.append(f"上证指数: {sh_row['收盘']:.2f} {change_pct_str}")
                    
                    # 深证成指
                    if not sz_data.empty:
                        sz_row = sz_data.iloc[0]
                        change_pct = sz_row['涨跌幅']
                        change_pct_str = f"{change_pct:.2f}%" if not pd.isna(change_pct) else "0.00%"
                        status_info.append(f"深证成指: {sz_row['收盘']:.2f} {change_pct_str}")
                    
                    # 创业板指
                    if not cy_data.empty:
                        cy_row = cy_data.iloc[0]
                        change_pct = cy_row['涨跌幅']
                        change_pct_str = f"{change_pct:.2f}%" if not pd.isna(change_pct) else "0.00%"
                        status_info.append(f"创业板指: {cy_row['收盘']:.2f} {change_pct_str}")
                    
                    # 更新状态栏
                    if status_info:
                        self.market_info_label.setText(" | ".join(status_info))
                    
                    logger.info("市场指数信息更新完成")
                    return
            except AttributeError:
                # 如果方法不存在，使用替代方法
                logger.warning("stock_zh_index_spot方法不存在，尝试使用替代方法")
                # 使用历史数据作为替代
                import datetime as dt
                end_date = dt.datetime.now().strftime("%Y%m%d")
                start_date = (dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y%m%d")
                
                # 获取上证指数
                try:
                    sh_data = ak.index_zh_a_hist(symbol="000001", period="daily", start_date=start_date, end_date=end_date)
                    sz_data = ak.index_zh_a_hist(symbol="399001", period="daily", start_date=start_date, end_date=end_date)
                    cy_data = ak.index_zh_a_hist(symbol="399006", period="daily", start_date=start_date, end_date=end_date)
                except requests.exceptions.ConnectionError as conn_e:
                    logger.warning(f"网络连接失败，无法获取指数历史数据: {conn_e}")
                    return
                except Exception as e:
                    logger.exception(f"获取指数历史数据失败: {e}")
                    return
                
                # 构建状态栏信息
                status_info = []
                
                # 上证指数
                if not sh_data.empty:
                    sh_row = sh_data.iloc[0]
                    change_pct = sh_row['涨跌幅']
                    change_pct_str = f"{change_pct:.2f}%" if not pd.isna(change_pct) else "0.00%"
                    status_info.append(f"上证指数: {sh_row['收盘']:.2f} {change_pct_str}")
                
                # 深证成指
                if not sz_data.empty:
                    sz_row = sz_data.iloc[0]
                    change_pct = sz_row['涨跌幅']
                    change_pct_str = f"{change_pct:.2f}%" if not pd.isna(change_pct) else "0.00%"
                    status_info.append(f"深证成指: {sz_row['收盘']:.2f} {change_pct_str}")
                
                # 创业板指
                if not cy_data.empty:
                    cy_row = cy_data.iloc[0]
                    change_pct = cy_row['涨跌幅']
                    change_pct_str = f"{change_pct:.2f}%" if not pd.isna(change_pct) else "0.00%"
                    status_info.append(f"创业板指: {cy_row['收盘']:.2f} {change_pct_str}")
                
                # 更新状态栏
                if status_info:
                    self.market_info_label.setText(" | ".join(status_info))
                
                logger.info("市场指数信息更新完成")
                return
            
            if index_data.empty:
                logger.warning("从AkShare获取的指数数据为空")
                return
            
            # 构建状态栏信息
            status_info = []
            
            # 上证指数
            sh_data = index_data[index_data['代码'] == '000001']
            if not sh_data.empty:
                sh_row = sh_data.iloc[0]
                change_pct = sh_row['涨跌幅']
                change_pct_str = f"{change_pct:.2f}%" if not pd.isna(change_pct) else "0.00%"
                status_info.append(f"上证指数: {sh_row['最新价']:.2f} {change_pct_str}")
            
            # 深证成指
            sz_data = index_data[index_data['代码'] == '399001']
            if not sz_data.empty:
                sz_row = sz_data.iloc[0]
                change_pct = sz_row['涨跌幅']
                change_pct_str = f"{change_pct:.2f}%" if not pd.isna(change_pct) else "0.00%"
                status_info.append(f"深证成指: {sz_row['最新价']:.2f} {change_pct_str}")
            
            # 创业板指
            cy_data = index_data[index_data['代码'] == '399006']
            if not cy_data.empty:
                cy_row = cy_data.iloc[0]
                change_pct = cy_row['涨跌幅']
                change_pct_str = f"{change_pct:.2f}%" if not pd.isna(change_pct) else "0.00%"
                status_info.append(f"创业板指: {cy_row['最新价']:.2f} {change_pct_str}")
            
            # 更新状态栏
            if status_info:
                self.market_info_label.setText(" | ".join(status_info))
            
            logger.info("市场指数信息更新完成")
            
        except Exception as e:
            logger.exception(f"获取市场指数信息失败: {e}")
    
    def on_refresh(self):
        """
        刷新数据
        """
        logger.info("开始刷新数据")
        self.refresh_stock_data()
        self.refresh_market_info()
