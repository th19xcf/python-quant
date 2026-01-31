#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
指标交互管理器
"""

from src.utils.logger import logger


class IndicatorInteractionManager:
    """指标相关交互处理"""

    def __init__(self, window):
        self.window = window
        self._scroll_area = None

    def set_scroll_area(self, scroll_area):
        """缓存指标滚动区域"""
        self._scroll_area = scroll_area

    def on_indicator_usage(self):
        logger.info("显示指标用法注释")

    def on_indicator_params(self):
        logger.info("调整指标参数")

    def on_indicator_formula(self):
        logger.info("修改当前指标公式")

    def on_window_clicked(self, window_num):
        return self.window._on_window_clicked_impl(window_num)

    def on_indicator_clicked(self, indicator, checked):
        return self.window._on_indicator_clicked_impl(indicator, checked)

    def on_plus_btn_clicked(self):
        window = self.window
        try:
            new_count = window.displayed_bar_count * 2
            window.bar_count_input.setText(str(new_count))
            window.displayed_bar_count = new_count
            logger.info(f"柱体数增加1倍，当前柱体数: {new_count}")

            if window.current_stock_data is not None:
                window.plot_k_line(window.current_stock_data, window.current_stock_name, window.current_stock_code)
        except Exception as e:
            logger.exception(f"增加柱体数失败: {e}")

    def on_minus_btn_clicked(self):
        window = self.window
        try:
            new_count = max(50, window.displayed_bar_count // 2)
            window.bar_count_input.setText(str(new_count))
            window.displayed_bar_count = new_count
            logger.info(f"柱体数减少1倍，当前柱体数: {new_count}")

            if window.current_stock_data is not None:
                window.plot_k_line(window.current_stock_data, window.current_stock_name, window.current_stock_code)
        except Exception as e:
            logger.exception(f"减少柱体数失败: {e}")

    def on_left_arrow_clicked(self):
        try:
            scroll_area = self._scroll_area
            if scroll_area:
                current_pos = scroll_area.horizontalScrollBar().value()
                new_pos = max(0, current_pos - 100)
                scroll_area.horizontalScrollBar().setValue(new_pos)
                logger.info(f"指标选择栏向左滚动，当前位置: {new_pos}")
        except Exception as e:
            logger.exception(f"指标选择栏向左滚动失败: {e}")

    def on_period_changed(self, period, checked):
        window = self.window
        if checked:
            for name, button in window.period_buttons.items():
                if name != period:
                    button.setChecked(False)

            window.current_period = period

            if period == "日线" and window.current_stock_data is not None:
                print(f"切换到日线，按照柱体数 {window.displayed_bar_count} 重新绘制K线")
                window.plot_k_line(window.current_stock_data, window.current_stock_name, window.current_stock_code)
            else:
                print(f"切换到{period}")

    def on_window_count_changed(self, window_count, checked):
        window = self.window
        if checked:
            for action in window.window_actions:
                if action.text() != f'{window_count}个窗口':
                    action.setChecked(False)

            window.current_window_count = window_count

            if hasattr(window, 'tech_plot_widget') and hasattr(window, 'volume_plot_widget') and hasattr(window, 'kdj_plot_widget') and hasattr(window, 'chart_splitter'):
                if window_count == 1:
                    window.volume_plot_widget.hide()
                    window.kdj_plot_widget.hide()
                    if hasattr(window, 'volume_values_label'):
                        window.volume_values_label.hide()
                    if hasattr(window, 'volume_container'):
                        window.volume_container.hide()
                    if hasattr(window, 'kdj_container'):
                        window.kdj_container.hide()
                    window.chart_splitter.setSizes([1, 0, 0])
                    logger.info("切换到1个窗口：只显示K线图，隐藏成交量图、KDJ图和标签栏")
                elif window_count == 2:
                    window.volume_plot_widget.show()
                    window.kdj_plot_widget.hide()
                    if hasattr(window, 'volume_values_label'):
                        window.volume_values_label.show()
                    if hasattr(window, 'volume_container'):
                        window.volume_container.show()
                    if hasattr(window, 'kdj_container'):
                        window.kdj_container.hide()
                    window.chart_splitter.setStretchFactor(0, 2)
                    window.chart_splitter.setStretchFactor(1, 1)
                    window.chart_splitter.setSizes([2000, 1000, 0])
                    logger.info(f"切换到{window_count}个窗口：显示K线图和成交量图，隐藏KDJ图")
                else:
                    window.volume_plot_widget.show()
                    window.kdj_plot_widget.show()
                    if hasattr(window, 'volume_values_label'):
                        window.volume_values_label.show()
                    if hasattr(window, 'volume_container'):
                        window.volume_container.show()
                    if hasattr(window, 'kdj_container'):
                        window.kdj_container.show()
                    window.chart_splitter.setStretchFactor(0, 2)
                    window.chart_splitter.setStretchFactor(1, 1)
                    window.chart_splitter.setStretchFactor(2, 1)
                    window.chart_splitter.setSizes([2000, 1000, 1000])
                    logger.info(f"切换到{window_count}个窗口：显示K线图、成交量图和KDJ图")

            print(f"切换到{window_count}个窗口")

    def on_bar_count_changed(self):
        window = self.window
        try:
            bar_count_text = window.bar_count_input.text()
            bar_count = int(bar_count_text)

            if bar_count > 0:
                window.displayed_bar_count = bar_count
                print(f"显示柱体数量更新为: {bar_count}")

                if window.current_stock_data is not None:
                    print(f"按照新的柱体数 {bar_count} 重新绘制K线图")
                    window.plot_k_line(window.current_stock_data, window.current_stock_name, window.current_stock_code)
            else:
                window.bar_count_input.setText("100")
                window.displayed_bar_count = 100
                print("无效的柱体数量，已恢复为默认值: 100")
        except ValueError:
            window.bar_count_input.setText("100")
            window.displayed_bar_count = 100
            print("无效的输入，已恢复为默认值: 100")
