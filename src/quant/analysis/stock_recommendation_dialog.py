#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
股票推荐对话框
"""

from PySide6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QGroupBox, 
                             QLabel, QLineEdit, QComboBox, QDateEdit, 
                             QPushButton, QTableWidget, QTableWidgetItem, 
                             QProgressBar, QGridLayout, QHeaderView)
from PySide6.QtCore import Qt, QDate
from PySide6.QtGui import QFont, QColor

from src.utils.logger import logger
from src.utils.config import get_config
from src.database.db_manager import DatabaseManager
from src.data.data_manager import DataManager
from src.recommendation.stock_recommender import StockRecommender
import polars as pl


class StockRecommendationDialog(QDialog):
    """
    股票推荐对话框
    """
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("股票推荐")
        self.setGeometry(100, 100, 900, 600)
        self.setModal(True)
        
        self.init_ui()
        
    def init_ui(self):
        """
        初始化UI
        """
        main_layout = QVBoxLayout(self)
        
        # 基本设置分组
        basic_group = QGroupBox("推荐设置")
        basic_layout = QGridLayout()
        
        # 推荐算法
        basic_layout.addWidget(QLabel("推荐算法:"), 0, 0)
        self.algorithm_combo = QComboBox()
        self.algorithm_combo.addItems(["多因子选股", "行业轮动", "动量策略"])
        basic_layout.addWidget(self.algorithm_combo, 0, 1)
        
        # 股票数量
        basic_layout.addWidget(QLabel("推荐股票数量:"), 1, 0)
        self.stock_count_edit = QLineEdit("10")
        basic_layout.addWidget(self.stock_count_edit, 1, 1)
        
        # 时间范围
        basic_layout.addWidget(QLabel("分析周期:"), 2, 0)
        self.period_combo = QComboBox()
        self.period_combo.addItems(["1个月", "3个月", "6个月", "12个月"])
        basic_layout.addWidget(self.period_combo, 2, 1)
        
        # 行业过滤
        basic_layout.addWidget(QLabel("行业过滤:"), 3, 0)
        self.industry_combo = QComboBox()
        self.industry_combo.addItems(["全部行业", "金融", "科技", "消费", "医药", "能源"])
        basic_layout.addWidget(self.industry_combo, 3, 1)
        
        basic_group.setLayout(basic_layout)
        main_layout.addWidget(basic_group)
        
        # 因子权重设置
        factor_group = QGroupBox("因子权重设置")
        factor_layout = QGridLayout()
        
        # 动量因子
        factor_layout.addWidget(QLabel("动量因子:"), 0, 0)
        self.momentum_weight_edit = QLineEdit("0.2")
        factor_layout.addWidget(self.momentum_weight_edit, 0, 1)
        
        # 价值因子
        factor_layout.addWidget(QLabel("价值因子:"), 1, 0)
        self.value_weight_edit = QLineEdit("0.2")
        factor_layout.addWidget(self.value_weight_edit, 1, 1)
        
        # 成长因子
        factor_layout.addWidget(QLabel("成长因子:"), 2, 0)
        self.growth_weight_edit = QLineEdit("0.2")
        factor_layout.addWidget(self.growth_weight_edit, 2, 1)
        
        # 质量因子
        factor_layout.addWidget(QLabel("质量因子:"), 3, 0)
        self.quality_weight_edit = QLineEdit("0.2")
        factor_layout.addWidget(self.quality_weight_edit, 3, 1)
        
        # 波动率因子
        factor_layout.addWidget(QLabel("波动率因子:"), 4, 0)
        self.volatility_weight_edit = QLineEdit("0.2")
        factor_layout.addWidget(self.volatility_weight_edit, 4, 1)
        
        factor_group.setLayout(factor_layout)
        main_layout.addWidget(factor_group)
        
        # 推荐结果
        result_group = QGroupBox("推荐结果")
        result_layout = QVBoxLayout()
        
        self.result_table = QTableWidget()
        self.result_table.setColumnCount(6)
        self.result_table.setHorizontalHeaderLabels(["排名", "股票代码", "股票名称", "综合评分", "预期收益", "风险评级"])
        
        # 设置列宽
        self.result_table.setColumnWidth(0, 60)
        self.result_table.setColumnWidth(1, 100)
        self.result_table.setColumnWidth(2, 120)
        self.result_table.setColumnWidth(3, 100)
        self.result_table.setColumnWidth(4, 100)
        self.result_table.setColumnWidth(5, 100)
        
        # 设置表格样式
        self.result_table.setStyleSheet("""
            QTableWidget {
                background-color: #000000;
                color: #C0C0C0;
                gridline-color: #333333;
                font-family: "Microsoft YaHei";
                font-size: 12px;
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
        
        result_layout.addWidget(self.result_table)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        result_layout.addWidget(self.progress_bar)
        
        result_group.setLayout(result_layout)
        main_layout.addWidget(result_group, 1)
        
        # 按钮布局
        button_layout = QHBoxLayout()
        
        self.run_button = QPushButton("生成推荐")
        self.run_button.clicked.connect(self.generate_recommendation)
        button_layout.addWidget(self.run_button)
        
        self.export_button = QPushButton("导出报告")
        self.export_button.clicked.connect(self.export_report)
        self.export_button.setEnabled(False)
        button_layout.addWidget(self.export_button)
        
        self.close_button = QPushButton("关闭")
        self.close_button.clicked.connect(self.close)
        button_layout.addWidget(self.close_button)
        
        main_layout.addLayout(button_layout)
        
    def generate_recommendation(self):
        """
        生成股票推荐
        """
        try:
            # 获取参数
            algorithm = self.algorithm_combo.currentText()
            stock_count = int(self.stock_count_edit.text())
            period = self.period_combo.currentText()
            industry = self.industry_combo.currentText()
            
            # 获取因子权重
            weights = {
                'momentum': float(self.momentum_weight_edit.text()),
                'value': float(self.value_weight_edit.text()),
                'growth': float(self.growth_weight_edit.text()),
                'quality': float(self.quality_weight_edit.text()),
                'volatility': float(self.volatility_weight_edit.text())
            }
            
            # 显示进度条
            self.progress_bar.setVisible(True)
            self.progress_bar.setValue(0)
            
            # 从DataManager获取真实股票数据
            config = get_config()
            db_manager = DatabaseManager(config)
            data_manager = DataManager(config, db_manager)
            
            self.progress_bar.setValue(10)
            
            # 获取股票基本信息
            stock_basic = data_manager.get_stock_basic()
            if stock_basic.is_empty():
                self.result_table.setRowCount(1)
                self.result_table.setItem(0, 0, QTableWidgetItem("无法获取股票列表"))
                return
            
            stock_codes = stock_basic['ts_code'].to_list()[:stock_count * 3]
            
            self.progress_bar.setValue(20)
            
            # 获取多只股票的日线数据
            stocks_data_list = []
            for i, stock_code in enumerate(stock_codes):
                try:
                    df = data_manager.get_stock_data(stock_code, "2024-01-01", "2024-12-31")
                    if not df.is_empty() and len(df) >= 20:
                        stock_info = stock_basic.filter(pl.col('ts_code') == stock_code)
                        stock_name = stock_info['name'].to_list()[0] if not stock_info.is_empty() else f'股票{stock_code}'
                        industry_name = stock_info.get_column('industry').to_list()[0] if 'industry' in stock_info.columns else '未知'
                        df = df.with_columns([
                            pl.lit(stock_code).alias('stock_code'),
                            pl.lit(stock_name).alias('stock_name'),
                            pl.lit(industry_name).alias('industry')
                        ])
                        stocks_data_list.append(df)
                except Exception as e:
                    logger.warning(f"获取股票 {stock_code} 数据失败: {e}")
                
                progress = 20 + int((i + 1) / len(stock_codes) * 40)
                self.progress_bar.setValue(progress)
            
            if not stocks_data_list:
                self.result_table.setRowCount(1)
                self.result_table.setItem(0, 0, QTableWidgetItem("无法获取任何股票数据"))
                return
            
            stocks_data = pl.concat(stocks_data_list)
            
            self.progress_bar.setValue(70)
            
            # 创建推荐引擎
            recommender = StockRecommender()
            
            # 生成推荐（适配新接口）
            recommendations = recommender.recommend_stocks(
                stocks_data, 
                top_n=stock_count,
                algorithm=algorithm,
                industry=industry,
                weights=weights
            )
            
            self.progress_bar.setValue(90)
            
            # 适配展示字段
            for rec in recommendations:
                rec.setdefault('code', rec.get('stock_code', ''))
                rec.setdefault('name', rec.get('stock_name', ''))
                rec.setdefault('expected_return', rec.get('expected_return', rec.get('score', 0) * 100))
                rec.setdefault('risk', rec.get('risk_level', '中'))
            
            # 显示结果
            self.display_result(recommendations)
            
            # 启用导出按钮
            self.export_button.setEnabled(True)
            
        except Exception as e:
            logger.error(f"生成推荐失败: {e}")
            # 清空表格并显示错误信息
            # 手动释放QTableWidgetItem对象
            for row in range(self.result_table.rowCount()):
                for col in range(self.result_table.columnCount()):
                    item = self.result_table.takeItem(row, col)
                    if item:
                        del item
            self.result_table.setRowCount(1)
            self.result_table.setItem(0, 0, QTableWidgetItem("错误"))
            self.result_table.setItem(0, 1, QTableWidgetItem(str(e)))
        finally:
            self.progress_bar.setVisible(False)
            self.progress_bar.setValue(0)
    
    def display_result(self, recommendations):
        """
        显示推荐结果
        """
        # 手动释放QTableWidgetItem对象
        for row in range(self.result_table.rowCount()):
            for col in range(self.result_table.columnCount()):
                item = self.result_table.takeItem(row, col)
                if item:
                    del item
        
        if not recommendations:
            self.result_table.setRowCount(1)
            self.result_table.setItem(0, 0, QTableWidgetItem("无推荐结果"))
            return
        
        self.result_table.setRowCount(len(recommendations))
        
        for i, stock in enumerate(recommendations):
            # 排名
            self.result_table.setItem(i, 0, QTableWidgetItem(str(i + 1)))
            # 股票代码
            self.result_table.setItem(i, 1, QTableWidgetItem(stock.get('code', '')))
            # 股票名称
            self.result_table.setItem(i, 2, QTableWidgetItem(stock.get('name', '')))
            # 综合评分
            score = stock.get('score', 0)
            score_item = QTableWidgetItem(f"{score:.2f}")
            score_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            self.result_table.setItem(i, 3, score_item)
            # 预期收益
            expected_return = stock.get('expected_return', 0)
            return_item = QTableWidgetItem(f"{expected_return:.2f}%")
            return_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            if expected_return > 0:
                return_item.setForeground(QColor(255, 0, 0))
            else:
                return_item.setForeground(QColor(0, 255, 0))
            self.result_table.setItem(i, 4, return_item)
            # 风险评级
            risk = stock.get('risk', '中')
            risk_item = QTableWidgetItem(risk)
            if risk == '低':
                risk_item.setForeground(QColor(0, 255, 0))
            elif risk == '中':
                risk_item.setForeground(QColor(255, 255, 0))
            else:
                risk_item.setForeground(QColor(255, 0, 0))
            self.result_table.setItem(i, 5, risk_item)
    
    def export_report(self):
        """
        导出推荐报告
        """
        try:
            # 这里可以实现报告导出逻辑
            from src.quant.reports.report_generator import ReportGenerator
            
            # 获取推荐结果（这里需要从表格中解析，实际应该从推荐引擎获取）
            report_generator = ReportGenerator()
            report_generator.generate_recommendation_report({})
            
            # 显示导出成功信息
            self.result_table.setRowCount(self.result_table.rowCount() + 1)
            self.result_table.setItem(self.result_table.rowCount() - 1, 0, QTableWidgetItem("报告导出成功！"))
        except Exception as e:
            logger.error(f"导出报告失败: {e}")
            # 显示错误信息
            self.result_table.setRowCount(self.result_table.rowCount() + 1)
            self.result_table.setItem(self.result_table.rowCount() - 1, 0, QTableWidgetItem(f"导出报告失败: {str(e)}"))
