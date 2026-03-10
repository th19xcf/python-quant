#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
动作/菜单处理管理器
"""

from src.utils.logger import logger


class ActionManager:
    """菜单与工具栏动作处理"""

    def __init__(self, window):
        self.window = window

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

    def on_strategy_backtest(self):
        logger.info("点击了策略回测")
        # 导入策略回测对话框
        from src.quant.analysis.strategy_backtest_dialog import StrategyBacktestDialog
        dialog = StrategyBacktestDialog(self.window)
        dialog.exec_()

    def on_stock_recommendation(self):
        logger.info("点击了股票推荐")
        # 导入股票推荐对话框
        from src.quant.analysis.stock_recommendation_dialog import StockRecommendationDialog
        dialog = StockRecommendationDialog(self.window)
        dialog.exec_()

    def on_factor_analysis(self):
        logger.info("点击了因子分析")
        # 导入因子分析对话框
        from src.quant.analysis.factor_analysis_dialog import FactorAnalysisDialog
        dialog = FactorAnalysisDialog(self.window)
        dialog.exec_()

    def on_settings(self):
        logger.info("点击了设置")

    def on_about(self):
        logger.info("点击了关于")

    def on_exit(self):
        logger.info("点击了退出")
        self.window.close()

    def on_market(self):
        logger.info("点击了行情按钮")

    def on_technical_analysis(self):
        logger.info("点击了技术分析按钮")
        self.window.tab_widget.setCurrentIndex(1)

    def on_refresh(self):
        logger.info("点击了刷新按钮")

    def on_self_selected(self):
        logger.info("点击了自选股")

    def on_panorama(self):
        logger.info("点击了全景图")

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

    def _load_stock_chart(self, ts_code: str, stock_name: str):
        """
        加载股票K线图

        Args:
            ts_code: 股票代码 (如: 600519.SH)
            stock_name: 股票名称
        """
        try:
            window = self.window
            
            # 使用 process_stock_data 方法加载数据，确保应用正确的复权类型
            window.process_stock_data(ts_code, stock_name)

        except (OSError, RuntimeError) as e:
            logger.exception(f"加载股票图表失败: {e}")
            self.window.statusBar().showMessage(f"加载股票图表失败: {str(e)[:50]}...", 5000)

    def _on_data_read_completed(self, df, name, code):
        """数据读取完成"""
        try:
            self.window.statusBar().showMessage(f"正在计算 {name}({code}) 技术指标...", 0)

            if hasattr(self.window, 'progress_bar'):
                self.window.progress_bar.setValue(50)

            from src.ui.async_processing import IndicatorCalculateThread
            self.window.indicator_calculate_thread = IndicatorCalculateThread(df)

            self.window.indicator_calculate_thread.indicator_calculated.connect(
                lambda result_df: self._on_indicator_calculated(result_df, name, code)
            )
            self.window.indicator_calculate_thread.indicator_calculate_error.connect(self._on_indicator_calculate_error)
            self.window.indicator_calculate_thread.indicator_calculate_progress.connect(self._on_indicator_calculate_progress)

            self.window.indicator_calculate_thread.start()
        except (OSError, RuntimeError) as e:
            logger.exception(f"处理数据读取完成信号失败: {e}")

    def _on_data_read_error(self, error_msg):
        """数据读取错误"""
        self.window.statusBar().showMessage(error_msg, 5000)
        logger.error(f"数据读取错误: {error_msg}")
        if hasattr(self.window, 'progress_bar'):
            self.window.progress_bar.setVisible(False)

    def _on_data_read_progress(self, progress, total):
        """数据读取进度"""
        if hasattr(self.window, 'progress_bar'):
            self.window.progress_bar.setValue(progress // 2)

    def _on_indicator_calculated(self, df, name, code):
        """指标计算完成"""
        try:
            self.window.statusBar().showMessage(f"正在绘制 {name}({code}) 图表...", 0)

            if hasattr(self.window, 'progress_bar'):
                self.window.progress_bar.setValue(90)

            self.window.plot_k_line(df, name, code)
            self.window.statusBar().showMessage(f"{name}({code}) 图表绘制完成", 5000)

            if hasattr(self.window, 'progress_bar'):
                self.window.progress_bar.setValue(100)
                self.window.progress_bar.setVisible(False)
        except (OSError, RuntimeError) as e:
            logger.exception(f"处理指标计算完成信号失败: {e}")
            if hasattr(self.window, 'progress_bar'):
                self.window.progress_bar.setVisible(False)

    def _on_indicator_calculate_error(self, error_msg):
        """指标计算错误"""
        logger.error(f"指标计算错误: {error_msg}")
        self.window.statusBar().showMessage(error_msg, 5000)

    def _on_indicator_calculate_progress(self, progress, total):
        """指标计算进度"""
        if hasattr(self.window, 'progress_bar'):
            self.window.progress_bar.setValue(50 + progress // 2)
