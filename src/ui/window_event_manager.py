#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
主窗口事件处理管理器
"""

from src.utils.logger import logger
from src.utils.event_bus import subscribe


class WindowEventManager:
    """主窗口事件处理管理器"""

    def __init__(self, window):
        self.window = window

    def subscribe_events(self):
        """订阅事件"""
        subscribe('data_updated', self.window._handle_data_updated)
        subscribe('data_error', self.window._handle_data_error)
        subscribe('indicator_calculated', self.window._handle_indicator_calculated)
        subscribe('indicator_error', self.window._handle_indicator_error)
        subscribe('system_shutdown', self.window._handle_system_shutdown)
        logger.info("主窗口事件订阅完成")

    def handle_data_updated(self, sender, data_type, ts_code, message="", **kwargs):
        """处理数据更新事件"""
        if data_type in ['stock_daily', 'index_daily']:
            if hasattr(self.window, 'current_stock_code') and self.window.current_stock_code == ts_code:
                self.window.refresh_kline_chart()
        elif data_type == 'stock_basic':
            self.window.update_stock_list()
        elif data_type == 'index_basic':
            self.window.update_index_list()

    def handle_data_error(self, sender, data_type, ts_code, message="", **kwargs):
        """处理数据错误事件"""
        logger.warning(f"数据错误: {data_type} {ts_code} {message}")

    def handle_indicator_calculated(self, sender, data_type=None, indicators=None, calculated_indicators=None,
                                   success=True, error=None, indicator_name=None, ts_code=None, result=None, **kwargs):
        """处理技术指标计算完成事件"""
        logger.info(f"技术指标计算完成: {indicators}")

    def handle_indicator_error(self, sender, data_type=None, indicators=None, error="", indicator_name=None, ts_code=None, **kwargs):
        """处理技术指标计算错误事件"""
        logger.error(f"指标计算错误: {error}")

    def handle_system_shutdown(self, sender):
        """处理系统关闭事件"""
        self.window.close()
