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
                data_len = len(window.current_stock_data)
                actual_count = min(new_count, data_len)
                logger.info(f"重新绘制K线图，数据长度: {data_len}, 请求柱体数: {new_count}, 实际显示: {actual_count}")
                window.plot_k_line(window.current_stock_data, window.current_stock_name, window.current_stock_code)
            else:
                logger.warning("current_stock_data 为 None，无法重新绘制K线图")
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
                data_len = len(window.current_stock_data)
                actual_count = min(new_count, data_len)
                logger.info(f"重新绘制K线图，数据长度: {data_len}, 请求柱体数: {new_count}, 实际显示: {actual_count}")
                window.plot_k_line(window.current_stock_data, window.current_stock_name, window.current_stock_code)
            else:
                logger.warning("current_stock_data 为 None，无法重新绘制K线图")
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

            # 切换周期时，如果有当前股票数据，重新获取数据并绘制
            if hasattr(window, 'current_stock_code') and hasattr(window, 'current_stock_name'):
                stock_code = window.current_stock_code
                stock_name = window.current_stock_name
                if stock_code and stock_name:
                    print(f"切换到{period}，按照柱体数 {window.displayed_bar_count} 重新获取数据")
                    window.process_stock_data(stock_code, stock_name)

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

    def on_adjustment_clicked(self, checked):
        window = self.window
        try:
            # 始终显示复权选择菜单，不检查 checked 状态
            menu = self._create_adjustment_menu()
            menu.exec_(window.adjustment_btn.mapToGlobal(window.adjustment_btn.rect().bottomLeft()))
            # 恢复按钮状态
            window.adjustment_btn.setChecked(True)
        except Exception as e:
            logger.exception(f"处理复权按钮点击失败: {e}")

    def _create_adjustment_menu(self):
        """创建复权选择菜单"""
        from PySide6.QtWidgets import QMenu
        from PySide6.QtGui import QAction
        from PySide6.QtCore import Qt
        
        window = self.window
        menu = QMenu(window)
        menu.setStyleSheet("QMenu { background-color: #333333; color: #C0C0C0; } QMenu::item { padding: 8px 20px; } QMenu::item:selected { background-color: #555555; }")
        
        # 定义复权类型
        adjustment_types = [
            ('不复权', 'none'),
            ('前复权', 'qfq'),
            ('后复权', 'hfq')
        ]
        
        # 创建菜单项
        for name, value in adjustment_types:
            action = QAction(name, menu)
            action.setCheckable(True)
            action.setChecked(window.adjustment_type == value)
            action.triggered.connect(lambda checked, v=value, n=name: self._on_adjustment_type_changed(v, n))
            menu.addAction(action)
        
        return menu

    def _on_adjustment_type_changed(self, value, name):
        """复权类型改变处理"""
        window = self.window
        window.adjustment_type = value
        logger.info(f"复权类型更改为: {name} ({value})")
        
        # 更新按钮文本显示当前复权类型
        if hasattr(window, 'adjustment_btn'):
            window.adjustment_btn.setText(name)
        
        # 如果有当前股票数据，重新获取数据并绘制
        if hasattr(window, 'current_stock_code') and hasattr(window, 'current_stock_name'):
            stock_code = window.current_stock_code
            stock_name = window.current_stock_name
            if stock_code and stock_name:
                window.process_stock_data(stock_code, stock_name)
