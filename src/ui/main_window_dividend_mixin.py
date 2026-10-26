#!/usr/bin/env python3

"""
主窗口除权标记 Mixin：分红标记加载展示与悬浮提示。
"""

from src.ui.dividend_marker import DividendMarkerManager
from src.utils.logger import logger


class MainWindowDividendMixin:
    def _load_dividend_markers(self, stock_code: str, dates):
        """
        加载分红标记

        Args:
            stock_code: 股票代码
            dates: 日期数组
        """
        self._load_and_show_dividend_markers(stock_code, dates)

    def _load_and_show_dividend_markers(self, stock_code, dates):
        """加载并显示分红配股标记"""
        try:
            # 初始化分红标记管理器
            if not hasattr(self, 'dividend_marker_manager'):
                self.dividend_marker_manager = DividendMarkerManager(self.tech_plot_widget)
            else:
                self.dividend_marker_manager.clear_markers()

            # 通过 DividendService 获取并过滤分红数据
            if len(dates) > 0:
                filtered_dividends = self.dividend_service.get_dividends_in_range(stock_code, dates)
                if filtered_dividends:
                    self.dividend_marker_manager.set_dividend_data(filtered_dividends, dates)

        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"加载分红标记失败: {e}")

    def show_dividend_tooltip(self, index, pos):
        """显示分红信息提示框"""
        try:
            if hasattr(self, 'dividend_marker_manager') and self.dividend_marker_manager:
                if self.dividend_marker_manager.has_dividend_at_index(index):
                    view_box = self.tech_plot_widget.getViewBox()
                    view_pos = view_box.mapSceneToView(pos)
                    self.dividend_marker_manager.show_tooltip(index, view_pos)
                else:
                    self.dividend_marker_manager.hide_tooltip()
        except (OSError, RuntimeError, ValueError) as e:
            logger.debug(f"显示分红提示框失败: {e}")

    def hide_dividend_tooltip(self):
        """隐藏分红信息提示框"""
        try:
            if hasattr(self, 'dividend_marker_manager') and self.dividend_marker_manager:
                self.dividend_marker_manager.hide_tooltip()
        except (OSError, RuntimeError, ValueError) as e:
            logger.debug(f"隐藏分红提示框失败: {e}")
