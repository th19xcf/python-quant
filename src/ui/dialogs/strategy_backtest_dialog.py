#!/usr/bin/env python3

"""
策略回测对话框（UI 层只调 usecases，不直接碰 DatabaseManager）
"""

from PySide6.QtCore import QDate
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QComboBox,
    QDateEdit,
    QDialog,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QProgressBar,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
)

from src.backtest.strategies.mean_reversion_strategy import MeanReversionStrategy
from src.backtest.strategies.multi_factor_strategy import MultiFactorStrategy
from src.backtest.strategies.trend_following_strategy import TrendFollowingStrategy
from src.core.usecases import run_backtest
from src.utils.logger import logger


class StrategyBacktestDialog(QDialog):
    """策略回测对话框"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("策略回测")
        self.setGeometry(100, 100, 800, 600)
        self.setModal(True)

        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)

        basic_group = QGroupBox("基本设置")
        basic_layout = QGridLayout()

        basic_layout.addWidget(QLabel("策略类型:"), 0, 0)
        self.strategy_combo = QComboBox()
        self.strategy_combo.addItems(["趋势跟踪策略", "均值回归策略", "多因子策略"])
        basic_layout.addWidget(self.strategy_combo, 0, 1)

        basic_layout.addWidget(QLabel("股票代码:"), 1, 0)
        self.stock_code_edit = QLineEdit("600519.SH")
        basic_layout.addWidget(self.stock_code_edit, 1, 1)

        basic_layout.addWidget(QLabel("开始日期:"), 2, 0)
        self.start_date_edit = QDateEdit(QDate.currentDate().addYears(-3))
        self.start_date_edit.setCalendarPopup(True)
        basic_layout.addWidget(self.start_date_edit, 2, 1)

        basic_layout.addWidget(QLabel("结束日期:"), 3, 0)
        self.end_date_edit = QDateEdit(QDate.currentDate())
        self.end_date_edit.setCalendarPopup(True)
        basic_layout.addWidget(self.end_date_edit, 3, 1)

        basic_layout.addWidget(QLabel("初始资金:"), 4, 0)
        self.initial_capital_edit = QLineEdit("1000000")
        basic_layout.addWidget(self.initial_capital_edit, 4, 1)

        basic_group.setLayout(basic_layout)
        main_layout.addWidget(basic_group)

        param_group = QGroupBox("策略参数")
        param_layout = QGridLayout()

        param_layout.addWidget(QLabel("短期均线:"), 0, 0)
        self.short_ma_edit = QLineEdit("5")
        param_layout.addWidget(self.short_ma_edit, 0, 1)

        param_layout.addWidget(QLabel("长期均线:"), 1, 0)
        self.long_ma_edit = QLineEdit("20")
        param_layout.addWidget(self.long_ma_edit, 1, 1)

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
        """运行策略回测（走 usecases 层）"""
        try:
            strategy_type = self.strategy_combo.currentText()
            stock_code = self.stock_code_edit.text()
            start_date = self.start_date_edit.date().toString("yyyy-MM-dd")
            end_date = self.end_date_edit.date().toString("yyyy-MM-dd")
            initial_capital = float(self.initial_capital_edit.text())

            short_ma = int(self.short_ma_edit.text())
            long_ma = int(self.long_ma_edit.text())
            rsi_period = int(self.rsi_period_edit.text())
            rsi_overbought = float(self.rsi_overbought_edit.text())
            rsi_oversold = float(self.rsi_oversold_edit.text())

            self.progress_bar.setVisible(True)
            self.progress_bar.setValue(10)

            strategy_map = {
                "趋势跟踪策略": TrendFollowingStrategy,
                "均值回归策略": MeanReversionStrategy,
                "多因子策略": MultiFactorStrategy
            }
            strategy_cls = strategy_map.get(strategy_type, TrendFollowingStrategy)
            strategy = strategy_cls()
            if strategy_type == "趋势跟踪策略":
                strategy.params['short_window'] = short_ma
                strategy.params['long_window'] = long_ma
            elif strategy_type == "均值回归策略":
                strategy.params['rsi_period'] = rsi_period
                strategy.params['overbought'] = rsi_overbought
                strategy.params['oversold'] = rsi_oversold

            self.progress_bar.setValue(30)

            result = run_backtest(
                ts_code=stock_code,
                start_date=start_date,
                end_date=end_date,
                strategy=strategy,
                initial_capital=initial_capital,
            )

            self.progress_bar.setValue(80)

            if result:
                result['strategy_type'] = strategy_type
                result['stock_code'] = stock_code
                result['start_date'] = start_date
                result['end_date'] = end_date
                performance = result.get('performance', {})
                result.setdefault('annual_return', performance.get('annual_return', 0))
                result.setdefault('sharpe_ratio', performance.get('sharpe_ratio', 0))
                result.setdefault('max_drawdown', performance.get('max_drawdown', 0))
                result.setdefault('trade_count', len(result.get('trades', [])) // 2)
                result.setdefault('win_rate', performance.get('winning_rate', 0))
                result.setdefault('final_capital', result.get('final_equity', 0))

            self.display_result(result)
            self.export_button.setEnabled(True)

        except Exception as e:
            logger.error(f"回测失败: {e}")
            self.result_text.setText(f"回测失败: {str(e)}")
        finally:
            self.progress_bar.setVisible(False)
            self.progress_bar.setValue(0)

    def display_result(self, result):
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

        # 风控摘要
        risk_summary = result.get('risk_summary')
        if risk_summary:
            text += f"\n【风控层】规则: {', '.join(risk_summary['rules'])}\n"
            text += f"       拒单 {risk_summary['total_rejected']} 笔\n"
            for line in risk_summary['reject_detail'][:5]:
                text += f"       {line}\n"
            if risk_summary['total_rejected'] > 5:
                text += f"       ... 还有 {risk_summary['total_rejected'] - 5} 笔\n"

        self.result_text.setText(text)

    def export_report(self):
        try:
            from src.quant.reports.report_generator import ReportGenerator
            report_generator = ReportGenerator()
            report_generator.generate_backtest_report({})
            self.result_text.append("\n报告导出成功！")
        except Exception as e:
            logger.error(f"导出报告失败: {e}")
            self.result_text.append(f"\n导出报告失败: {str(e)}")
