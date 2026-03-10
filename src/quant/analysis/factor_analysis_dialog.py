#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
因子分析对话框
"""

from PySide6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QGroupBox, 
                             QLabel, QLineEdit, QComboBox, QDateEdit, 
                             QPushButton, QTextEdit, QProgressBar, QGridLayout,
                             QCheckBox)
from PySide6.QtCore import Qt, QDate
from PySide6.QtGui import QFont

from src.utils.logger import logger
from src.quant.analysis.factor_analyzer import FactorAnalyzer


class FactorAnalysisDialog(QDialog):
    """
    因子分析对话框
    """
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("因子分析")
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
        
        # 股票代码
        basic_layout.addWidget(QLabel("股票代码:"), 0, 0)
        self.stock_code_edit = QLineEdit("600519.SH")
        basic_layout.addWidget(self.stock_code_edit, 0, 1)
        
        # 时间范围
        basic_layout.addWidget(QLabel("开始日期:"), 1, 0)
        self.start_date_edit = QDateEdit(QDate.currentDate().addYears(-2))
        self.start_date_edit.setCalendarPopup(True)
        basic_layout.addWidget(self.start_date_edit, 1, 1)
        
        basic_layout.addWidget(QLabel("结束日期:"), 2, 0)
        self.end_date_edit = QDateEdit(QDate.currentDate())
        self.end_date_edit.setCalendarPopup(True)
        basic_layout.addWidget(self.end_date_edit, 2, 1)
        
        basic_group.setLayout(basic_layout)
        main_layout.addWidget(basic_group)
        
        # 因子选择分组
        factor_group = QGroupBox("因子选择")
        factor_layout = QGridLayout()
        
        # 技术指标因子
        factor_layout.addWidget(QLabel("技术指标因子:"), 0, 0)
        
        # 技术指标复选框
        self.tech_factors = {
            'MACD': QCheckBox("MACD"),
            'RSI': QCheckBox("RSI"),
            'BOLL': QCheckBox("布林带"),
            'KDJ': QCheckBox("KDJ"),
            'MA': QCheckBox("移动平均线"),
            'ATR': QCheckBox("ATR"),
            'WR': QCheckBox("威廉指标"),
            'CCI': QCheckBox("CCI"),
        }
        
        # 添加技术指标复选框
        row = 1
        col = 0
        for name, checkbox in self.tech_factors.items():
            factor_layout.addWidget(checkbox, row, col)
            col += 1
            if col >= 4:
                col = 0
                row += 1
        
        # 基本面因子
        factor_layout.addWidget(QLabel("基本面因子:"), row, 0)
        row += 1
        
        self.fundamental_factors = {
            'PE': QCheckBox("市盈率"),
            'PB': QCheckBox("市净率"),
            'ROE': QCheckBox("净资产收益率"),
            'EPS': QCheckBox("每股收益"),
        }
        
        # 添加基本面因子复选框
        col = 0
        for name, checkbox in self.fundamental_factors.items():
            factor_layout.addWidget(checkbox, row, col)
            col += 1
            if col >= 4:
                col = 0
                row += 1
        
        # 动量因子
        factor_layout.addWidget(QLabel("动量因子:"), row, 0)
        row += 1
        
        self.momentum_factors = {
            'MOM1M': QCheckBox("1个月动量"),
            'MOM3M': QCheckBox("3个月动量"),
            'MOM6M': QCheckBox("6个月动量"),
            'MOM12M': QCheckBox("12个月动量"),
        }
        
        # 添加动量因子复选框
        col = 0
        for name, checkbox in self.momentum_factors.items():
            factor_layout.addWidget(checkbox, row, col)
            col += 1
            if col >= 4:
                col = 0
                row += 1
        
        factor_group.setLayout(factor_layout)
        main_layout.addWidget(factor_group)
        
        # 分析设置分组
        analysis_group = QGroupBox("分析设置")
        analysis_layout = QGridLayout()
        
        # 分析类型
        analysis_layout.addWidget(QLabel("分析类型:"), 0, 0)
        self.analysis_combo = QComboBox()
        self.analysis_combo.addItems(["因子有效性检验", "因子相关性分析", "因子组合分析"])
        analysis_layout.addWidget(self.analysis_combo, 0, 1)
        
        # 滚动窗口
        analysis_layout.addWidget(QLabel("滚动窗口:"), 1, 0)
        self.window_edit = QLineEdit("20")
        analysis_layout.addWidget(self.window_edit, 1, 1)
        
        analysis_group.setLayout(analysis_layout)
        main_layout.addWidget(analysis_group)
        
        # 分析结果
        result_group = QGroupBox("分析结果")
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
        
        self.run_button = QPushButton("开始分析")
        self.run_button.clicked.connect(self.run_analysis)
        button_layout.addWidget(self.run_button)
        
        self.export_button = QPushButton("导出报告")
        self.export_button.clicked.connect(self.export_report)
        self.export_button.setEnabled(False)
        button_layout.addWidget(self.export_button)
        
        self.close_button = QPushButton("关闭")
        self.close_button.clicked.connect(self.close)
        button_layout.addWidget(self.close_button)
        
        main_layout.addLayout(button_layout)
        
    def run_analysis(self):
        """
        运行因子分析
        """
        try:
            # 获取参数
            stock_code = self.stock_code_edit.text()
            start_date = self.start_date_edit.date().toString("yyyy-MM-dd")
            end_date = self.end_date_edit.date().toString("yyyy-MM-dd")
            analysis_type = self.analysis_combo.currentText()
            window = int(self.window_edit.text())
            
            # 获取选中的因子
            selected_factors = []
            for name, checkbox in self.tech_factors.items():
                if checkbox.isChecked():
                    selected_factors.append(name)
            for name, checkbox in self.fundamental_factors.items():
                if checkbox.isChecked():
                    selected_factors.append(name)
            for name, checkbox in self.momentum_factors.items():
                if checkbox.isChecked():
                    selected_factors.append(name)
            
            if not selected_factors:
                self.result_text.setText("请至少选择一个因子")
                return
            
            # 显示进度条
            self.progress_bar.setVisible(True)
            self.progress_bar.setValue(0)
            
            # 创建因子分析器
            analyzer = FactorAnalyzer()
            
            # 运行分析
            result = analyzer.run_analysis(
                stock_code=stock_code,
                start_date=start_date,
                end_date=end_date,
                factors=selected_factors,
                analysis_type=analysis_type,
                window=window
            )
            
            # 显示结果
            self.display_result(result)
            
            # 启用导出按钮
            self.export_button.setEnabled(True)
            
        except Exception as e:
            logger.error(f"分析失败: {e}")
            self.result_text.setText(f"分析失败: {str(e)}")
        finally:
            self.progress_bar.setVisible(False)
            self.progress_bar.setValue(0)
    
    def display_result(self, result):
        """
        显示分析结果
        """
        if not result:
            self.result_text.setText("分析结果为空")
            return
        
        text = "因子分析结果:\n"
        text += "-" * 50 + "\n"
        
        if 'analysis_type' in result:
            text += f"分析类型: {result['analysis_type']}\n"
        if 'stock_code' in result:
            text += f"股票代码: {result['stock_code']}\n"
        if 'start_date' in result and 'end_date' in result:
            text += f"分析周期: {result['start_date']} 至 {result['end_date']}\n"
        
        text += "-" * 50 + "\n"
        
        if 'factor_results' in result:
            for factor, factor_result in result['factor_results'].items():
                text += f"\n{factor} 因子分析:\n"
                text += "-" * 30 + "\n"
                for key, value in factor_result.items():
                    if isinstance(value, float):
                        text += f"{key}: {value:.4f}\n"
                    else:
                        text += f"{key}: {value}\n"
        
        if 'correlation_matrix' in result:
            text += "\n因子相关性矩阵:\n"
            text += "-" * 30 + "\n"
            matrix = result['correlation_matrix']
            # 简单显示相关性矩阵
            for row in matrix:
                text += "\t".join([f"{v:.2f}" for v in row]) + "\n"
        
        text += "-" * 50 + "\n"
        
        self.result_text.setText(text)
    
    def export_report(self):
        """
        导出分析报告
        """
        try:
            # 这里可以实现报告导出逻辑
            from src.quant.reports.report_generator import ReportGenerator
            
            # 获取分析结果（这里需要从result_text中解析，实际应该从分析器获取）
            report_generator = ReportGenerator()
            report_generator.generate_factor_analysis_report({})
            
            self.result_text.append("\n报告导出成功！")
        except Exception as e:
            logger.error(f"导出报告失败: {e}")
            self.result_text.append(f"\n导出报告失败: {str(e)}")
