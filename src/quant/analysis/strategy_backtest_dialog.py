#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
策略回测对话框
"""

from PySide6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QGroupBox, 
                             QLabel, QLineEdit, QComboBox, QDateEdit, 
                             QPushButton, QTextEdit, QProgressBar, QGridLayout)
from PySide6.QtCore import Qt, QDate
from PySide6.QtGui import QFont

from src.utils.logger import logger
from src.quant.analysis.backtest_engine import BacktestEngine


class StrategyBacktestDialog(QDialog):
    """
    策略回测对话框
    """
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("策略回测")
        self.setGeometry(100, 100, 800, 600)
        self.setModal(True)
        
        self.init_ui()
        
    def init_ui(self):
        """
        初始化UI
        """
        main_layout = QVBoxLayout(self)
        
        # 基本设置分组
        basic_group = QGroupBox("基本设置")
        basic_layout = QGridLayout()
        
        # 策略选择
        basic_layout.addWidget(QLabel("策略类型:"), 0, 0)
        self.strategy_combo = QComboBox()
        self.strategy_combo.addItems(["趋势跟踪策略", "均值回归策略", "多因子策略"])
        basic_layout.addWidget(self.strategy_combo, 0, 1)
        
        # 股票代码
        basic_layout.addWidget(QLabel("股票代码:"), 1, 0)
        self.stock_code_edit = QLineEdit("600519.SH")
        basic_layout.addWidget(self.stock_code_edit, 1, 1)
        
        # 时间范围
        basic_layout.addWidget(QLabel("开始日期:"), 2, 0)
        self.start_date_edit = QDateEdit(QDate.currentDate().addYears(-3))
        self.start_date_edit.setCalendarPopup(True)
        basic_layout.addWidget(self.start_date_edit, 2, 1)
        
        basic_layout.addWidget(QLabel("结束日期:"), 3, 0)
        self.end_date_edit = QDateEdit(QDate.currentDate())
        self.end_date_edit.setCalendarPopup(True)
        basic_layout.addWidget(self.end_date_edit, 3, 1)
        
        # 初始资金
        basic_layout.addWidget(QLabel("初始资金:"), 4, 0)
        self.initial_capital_edit = QLineEdit("1000000")
        basic_layout.addWidget(self.initial_capital_edit, 4, 1)
        
        basic_group.setLayout(basic_layout)
        main_layout.addWidget(basic_group)
        
        # 策略参数分组
        param_group = QGroupBox("策略参数")
        param_layout = QGridLayout()
        
        # 移动平均线参数
        param_layout.addWidget(QLabel("短期均线:"), 0, 0)
        self.short_ma_edit = QLineEdit("5")
        param_layout.addWidget(self.short_ma_edit, 0, 1)
        
        param_layout.addWidget(QLabel("长期均线:"), 1, 0)
        self.long_ma_edit = QLineEdit("20")
        param_layout.addWidget(self.long_ma_edit, 1, 1)
        
        # RSI参数
        param_layout.addWidget(QLabel("RSI周期:"), 2, 0)
        self.rsi_period_edit = QLineEdit("14")
        param_layout.addWidget(self.rsi_period_edit, 2, 1)
        
        param_layout.addWidget(QLabel("RSI超买阈值:"), 3, 0)
        self.rsi_overbought_edit = QLineEdit("70")
        param_layout.addWidget(self.rsi_overbought_edit, 3, 1)
        
        param_layout.addWidget(QLabel("RSI超卖阈值:"), 4, 0)
        self.rsi_oversold_edit = QLineEdit("30")
        param_layout.addWidget(self.rsi_oversold_edit, 4, 1)
        
        param_group.setLayout(param_layout)
        main_layout.addWidget(param_group)
        
        # 回测结果
        result_group = QGroupBox("回测结果")
        result_layout = QVBoxLayout()
        
        self.result_text = QTextEdit()
        self.result_text.setReadOnly(True)
        self.result_text.setFont(QFont("Courier New", 10))
        result_layout.addWidget(self.result_text)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        result_layout.addWidget(self.progress_bar)
        
        result_group.setLayout(result_layout)
        main_layout.addWidget(result_group, 1)
        
        # 按钮布局
        button_layout = QHBoxLayout()
        
        self.run_button = QPushButton("开始回测")
        self.run_button.clicked.connect(self.run_backtest)
        button_layout.addWidget(self.run_button)
        
        self.export_button = QPushButton("导出报告")
        self.export_button.clicked.connect(self.export_report)
        self.export_button.setEnabled(False)
        button_layout.addWidget(self.export_button)
        
        self.close_button = QPushButton("关闭")
        self.close_button.clicked.connect(self.close)
        button_layout.addWidget(self.close_button)
        
        main_layout.addLayout(button_layout)
        
    def run_backtest(self):
        """
        运行策略回测
        """
        try:
            # 获取参数
            strategy_type = self.strategy_combo.currentText()
            stock_code = self.stock_code_edit.text()
            start_date = self.start_date_edit.date().toString("yyyy-MM-dd")
            end_date = self.end_date_edit.date().toString("yyyy-MM-dd")
            initial_capital = float(self.initial_capital_edit.text())
            
            # 获取策略参数
            short_ma = int(self.short_ma_edit.text())
            long_ma = int(self.long_ma_edit.text())
            rsi_period = int(self.rsi_period_edit.text())
            rsi_overbought = float(self.rsi_overbought_edit.text())
            rsi_oversold = float(self.rsi_oversold_edit.text())
            
            # 显示进度条
            self.progress_bar.setVisible(True)
            self.progress_bar.setValue(0)
            
            # 创建回测引擎
            engine = BacktestEngine()
            
            # 运行回测
            result = engine.run_backtest(
                strategy_type=strategy_type,
                stock_code=stock_code,
                start_date=start_date,
                end_date=end_date,
                initial_capital=initial_capital,
                short_ma=short_ma,
                long_ma=long_ma,
                rsi_period=rsi_period,
                rsi_overbought=rsi_overbought,
                rsi_oversold=rsi_oversold
            )
            
            # 显示结果
            self.display_result(result)
            
            # 启用导出按钮
            self.export_button.setEnabled(True)
            
        except Exception as e:
            logger.error(f"回测失败: {e}")
            self.result_text.setText(f"回测失败: {str(e)}")
        finally:
            self.progress_bar.setVisible(False)
            self.progress_bar.setValue(0)
    
    def display_result(self, result):
        """
        显示回测结果
        """
        if not result:
            self.result_text.setText("回测结果为空")
            return
        
        text = "回测结果:\n"
        text += "-" * 50 + "\n"
        text += f"策略类型: {result.get('strategy_type', '未知')}\n"
        text += f"股票代码: {result.get('stock_code', '未知')}\n"
        text += f"回测周期: {result.get('start_date', '未知')} 至 {result.get('end_date', '未知')}\n"
        text += f"初始资金: {result.get('initial_capital', 0):,.2f} 元\n"
        text += f"最终资金: {result.get('final_capital', 0):,.2f} 元\n"
        text += f"总收益率: {result.get('total_return', 0):.2f}%\n"
        text += f"年化收益率: {result.get('annual_return', 0):.2f}%\n"
        text += f"夏普比率: {result.get('sharpe_ratio', 0):.2f}\n"
        text += f"最大回撤: {result.get('max_drawdown', 0):.2f}%\n"
        text += f"交易次数: {result.get('trade_count', 0)}\n"
        text += f"胜率: {result.get('win_rate', 0):.2f}%\n"
        text += "-" * 50 + "\n"
        
        self.result_text.setText(text)
    
    def export_report(self):
        """
        导出回测报告
        """
        try:
            # 这里可以实现报告导出逻辑
            from src.quant.reports.report_generator import ReportGenerator
            
            # 获取回测结果（这里需要从result_text中解析，实际应该从回测引擎获取）
            report_generator = ReportGenerator()
            report_generator.generate_backtest_report({})
            
            self.result_text.append("\n报告导出成功！")
        except Exception as e:
            logger.error(f"导出报告失败: {e}")
            self.result_text.append(f"\n导出报告失败: {str(e)}")
