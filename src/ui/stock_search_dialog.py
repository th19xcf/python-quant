#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
股票搜索对话框
实现类似通达信的股票代码/名称模糊搜索功能
"""

from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLineEdit, QTableWidget, 
    QTableWidgetItem, QPushButton, QLabel, QHeaderView, QAbstractItemView
)
from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QKeyEvent
from loguru import logger


class StockSearchDialog(QDialog):
    """
    股票搜索对话框
    
    功能：
    1. 支持股票代码/名称模糊搜索
    2. 实时显示搜索结果
    3. 支持键盘上下键选择
    4. 回车键确认选择
    """
    
    def __init__(self, parent=None, db_manager=None, initial_text=""):
        """
        初始化搜索对话框
        
        Args:
            parent: 父窗口
            db_manager: 数据库管理器
            initial_text: 初始搜索文本
        """
        super().__init__(parent)
        self.db_manager = db_manager
        self.selected_stock = None
        self.all_stocks = []  # 缓存所有股票列表
        self.initial_text = initial_text
        
        self.init_ui()
        self.load_all_stocks()  # 预加载股票列表
        
        # 如果有初始文本，设置并触发搜索
        if initial_text:
            self.search_edit.setText(initial_text)
            self.perform_search()
        
    def init_ui(self):
        """初始化UI"""
        self.setWindowTitle("股票搜索")
        self.setFixedSize(500, 400)
        
        # 设置窗口标志，使其更像一个下拉框
        self.setWindowFlags(
            Qt.Dialog |
            Qt.FramelessWindowHint |
            Qt.WindowStaysOnTopHint
        )
        
        layout = QVBoxLayout(self)
        
        # 搜索输入框
        search_layout = QHBoxLayout()
        search_label = QLabel("代码/名称:")
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("输入股票代码或名称进行搜索...")
        self.search_edit.textChanged.connect(self.on_search_text_changed)
        
        search_layout.addWidget(search_label)
        search_layout.addWidget(self.search_edit)
        layout.addLayout(search_layout)
        
        # 提示标签
        self.tip_label = QLabel("请输入股票代码或名称，支持模糊搜索")
        self.tip_label.setStyleSheet("color: gray; font-size: 12px;")
        layout.addWidget(self.tip_label)
        
        # 搜索结果表格
        self.result_table = QTableWidget()
        self.result_table.setColumnCount(4)
        self.result_table.setHorizontalHeaderLabels(["代码", "名称", "市场", "行业"])
        self.result_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.result_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Fixed)
        self.result_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Fixed)
        self.result_table.setColumnWidth(0, 100)
        self.result_table.setColumnWidth(1, 120)
        self.result_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.result_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.result_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.result_table.itemDoubleClicked.connect(self.on_item_double_clicked)
        
        layout.addWidget(self.result_table)
        
        # 按钮区域
        btn_layout = QHBoxLayout()
        
        self.ok_btn = QPushButton("确定")
        self.ok_btn.clicked.connect(self.on_ok_clicked)
        self.ok_btn.setEnabled(False)
        
        self.cancel_btn = QPushButton("取消")
        self.cancel_btn.clicked.connect(self.reject)
        
        btn_layout.addStretch()
        btn_layout.addWidget(self.ok_btn)
        btn_layout.addWidget(self.cancel_btn)
        
        layout.addLayout(btn_layout)
        
        # 设置焦点到搜索框
        self.search_edit.setFocus()
        
        # 延迟搜索定时器
        self.search_timer = QTimer(self)
        self.search_timer.setSingleShot(True)
        self.search_timer.timeout.connect(self.perform_search)
        
    def load_all_stocks(self):
        """预加载所有股票列表到内存"""
        try:
            if not self.db_manager:
                logger.warning("数据库管理器未初始化")
                return
                
            session = self.db_manager.get_session()
            if not session:
                logger.warning("无法获取数据库会话")
                return
            
            # 从StockBasic表获取所有股票
            from src.database.models.stock import StockBasic
            
            # 先尝试查询所有股票，不限制状态
            stocks = session.query(StockBasic).all()
            
            self.all_stocks = []
            for stock in stocks:
                self.all_stocks.append({
                    'ts_code': stock.ts_code or '',
                    'symbol': stock.symbol or '',
                    'name': stock.name or '',
                    'market': stock.market or '-',
                    'industry': stock.industry or '-'
                })
            
            logger.info(f"预加载了 {len(self.all_stocks)} 只股票")
            
            # 如果没有数据，记录警告
            if len(self.all_stocks) == 0:
                logger.warning("数据库中没有股票数据，请检查StockBasic表")
            
        except Exception as e:
            logger.exception(f"加载股票列表失败: {e}")
    
    def on_search_text_changed(self, text):
        """搜索文本变化时触发"""
        # 延迟搜索，避免频繁查询
        self.search_timer.stop()
        if text.strip():
            self.search_timer.start(300)  # 300ms后执行搜索
        else:
            self.clear_results()
    
    def perform_search(self):
        """执行搜索"""
        search_text = self.search_edit.text().strip()
        if not search_text:
            self.clear_results()
            return
        
        try:
            # 转换为大写进行比较
            search_text_upper = search_text.upper()
            
            # 在内存中过滤股票
            filtered_stocks = []
            for stock in self.all_stocks:
                # 支持代码或名称模糊匹配（不区分大小写）
                ts_code = stock['ts_code'].upper() if stock['ts_code'] else ''
                symbol = stock['symbol'].upper() if stock['symbol'] else ''
                name = stock['name'] if stock['name'] else ''
                
                if (search_text_upper in ts_code or 
                    search_text_upper in symbol or
                    search_text in name):  # 名称保持原样匹配
                    filtered_stocks.append(stock)
            
            # 限制结果数量
            filtered_stocks = filtered_stocks[:50]
            
            logger.debug(f"搜索 '{search_text}' 找到 {len(filtered_stocks)} 条结果")
            self.update_results(filtered_stocks)
            
        except Exception as e:
            logger.exception(f"搜索股票失败: {e}")
    
    def update_results(self, stocks):
        """更新搜索结果表格"""
        self.result_table.setRowCount(len(stocks))
        
        for i, stock in enumerate(stocks):
            # 代码
            code_item = QTableWidgetItem(stock['ts_code'])
            code_item.setData(Qt.UserRole, stock)  # 存储完整数据
            self.result_table.setItem(i, 0, code_item)
            
            # 名称
            name_item = QTableWidgetItem(stock['name'])
            self.result_table.setItem(i, 1, name_item)
            
            # 市场
            market_item = QTableWidgetItem(stock['market'])
            self.result_table.setItem(i, 2, market_item)
            
            # 行业
            industry_item = QTableWidgetItem(stock['industry'])
            self.result_table.setItem(i, 3, industry_item)
        
        # 更新提示
        if stocks:
            self.tip_label.setText(f"找到 {len(stocks)} 条结果 (显示前50条)")
            self.tip_label.setStyleSheet("color: green; font-size: 12px;")
            self.ok_btn.setEnabled(True)
            # 默认选中第一行
            self.result_table.selectRow(0)
        else:
            self.tip_label.setText("未找到匹配的股票")
            self.tip_label.setStyleSheet("color: red; font-size: 12px;")
            self.ok_btn.setEnabled(False)
    
    def clear_results(self):
        """清空搜索结果"""
        self.result_table.setRowCount(0)
        self.tip_label.setText("请输入股票代码或名称，支持模糊搜索")
        self.tip_label.setStyleSheet("color: gray; font-size: 12px;")
        self.ok_btn.setEnabled(False)
    
    def on_item_double_clicked(self, item):
        """双击选中"""
        self.on_ok_clicked()
    
    def on_ok_clicked(self):
        """确定按钮点击"""
        current_row = self.result_table.currentRow()
        if current_row >= 0:
            item = self.result_table.item(current_row, 0)
            if item:
                self.selected_stock = item.data(Qt.UserRole)
                self.accept()
    
    def keyPressEvent(self, event: QKeyEvent):
        """键盘事件处理"""
        if event.key() == Qt.Key_Up:
            # 向上选择
            current_row = self.result_table.currentRow()
            if current_row > 0:
                self.result_table.selectRow(current_row - 1)
        elif event.key() == Qt.Key_Down:
            # 向下选择
            current_row = self.result_table.currentRow()
            if current_row < self.result_table.rowCount() - 1:
                self.result_table.selectRow(current_row + 1)
        elif event.key() == Qt.Key_Return or event.key() == Qt.Key_Enter:
            # 回车确认
            self.on_ok_clicked()
        elif event.key() == Qt.Key_Escape:
            # ESC取消
            self.reject()
        else:
            super().keyPressEvent(event)
    
    def get_selected_stock(self):
        """
        获取选中的股票
        
        Returns:
            dict: 选中的股票信息，如果没有选中则返回None
        """
        return self.selected_stock
    
    def move_to_position(self, x, y):
        """
        移动对话框到指定位置
        
        Args:
            x: x坐标
            y: y坐标
        """
        self.move(int(x), int(y))
    
    def showEvent(self, event):
        """
        对话框显示事件
        在显示后设置光标位置，避免文本被选中
        """
        super().showEvent(event)
        # 延迟设置光标位置，确保对话框已完全显示
        from PySide6.QtCore import QTimer
        QTimer.singleShot(10, self._set_cursor_position)
    
    def _set_cursor_position(self):
        """设置光标到文本末尾"""
        text = self.search_edit.text()
        if text:
            self.search_edit.setCursorPosition(len(text))
