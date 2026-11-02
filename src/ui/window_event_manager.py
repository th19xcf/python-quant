#!/usr/bin/env python3

"""
主窗口事件处理管理器
"""

from src.utils.event_bus import subscribe
from src.utils.logger import logger


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
                # 明确能力边界：自动重绘K线尚未接入。历史实现调用的是 MainWindow 上
                # 不存在的 refresh_kline_chart，事件触发时会抛 AttributeError；
                # 这里改为显式告警，避免用异常表达"未实现"。
                logger.warning(
                    f"数据已更新但K线自动刷新未实现，当前图表保持旧数据: {ts_code}"
                )
        elif data_type == 'stock_basic':
            self.window.update_stock_list()
        elif data_type == 'index_basic':
            self.window.update_index_list()

    def handle_data_error(self, sender, data_type, ts_code, message="", **kwargs):
        """处理数据错误事件"""
        logger.warning(f"数据错误: {data_type} {ts_code} {message}")

    def handle_indicator_calculated(self, sender, data_type=None, indicators=None, calculated_indicators=None,
                                   success=True, error=None, indicator_name=None, ts_code=None, result=None,
                                   data=None, **kwargs):
        """处理技术指标计算完成事件

        该事件有两个发布点：parallel_ops 携带 indicators，IndicatorCalculateThread
        在计算结束后再发布一次仅携带 data 的完成事件。后者没有 indicators 字段，
        不能把“未携带指标列表”误报成“指标为空（None）”。
        """
        if indicators is not None:
            logger.info(f"技术指标计算完成: {indicators}")
        elif data is not None:
            shape = getattr(data, 'shape', None)
            logger.info(f"技术指标计算完成，结果数据: {shape if shape is not None else type(data).__name__}")
        else:
            logger.info("技术指标计算完成（事件未携带指标列表或结果数据）")

    def handle_indicator_error(self, sender, data_type=None, indicators=None, error="", indicator_name=None, ts_code=None, **kwargs):
        """处理技术指标计算错误事件"""
        logger.error(f"指标计算错误: {error}")

    def handle_system_shutdown(self, sender):
        """处理系统关闭事件"""
        self.window.close()
