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

    def on_settings(self):
        logger.info("点击了设置")

    def on_about(self):
        logger.info("点击了关于")

    def on_exit(self):
        logger.info("点击了退出")
        self.window.close()

    def on_search(self):
        search_text = self.window.search_edit.text()
        logger.info(f"搜索: {search_text}")

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
