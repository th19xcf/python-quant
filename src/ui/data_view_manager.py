#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据视图管理器，负责行情数据刷新与展示逻辑
"""

from src.utils.logger import logger


class DataViewManager:
    """数据视图管理器"""

    def __init__(self, window):
        self.window = window

    def refresh_stock_data(self):
        """刷新股票数据"""
        logger.info("刷新股票数据")
        return self.window._refresh_stock_data_impl()

    def refresh_market_info(self):
        """刷新市场指数信息"""
        logger.info("刷新市场指数信息")
        return self.window._refresh_market_info_impl()

    def show_stock_data_by_type(self, stock_type):
        """按类型展示股票数据"""
        logger.info(f"显示{stock_type}数据")
        return self.window._show_stock_data_by_type_impl(stock_type)

    def show_hs_aj_stock_data(self):
        """显示沪深京A股数据"""
        logger.info("显示沪深京A股数据")
        return self.window._show_hs_aj_stock_data_impl()

    def show_index_data(self, index_name):
        """显示指数数据"""
        logger.info(f"显示指数数据: {index_name}")
        return self.window._show_index_data_impl(index_name)

    def handle_nav_item_clicked(self, item, column):
        """处理导航点击"""
        return self.window._on_nav_item_clicked_impl(item, column)

    def show_index_overview(self):
        """显示沪深京指数总览"""
        logger.info("显示沪深京指数总览")
        return self.window._on_index_impl()
