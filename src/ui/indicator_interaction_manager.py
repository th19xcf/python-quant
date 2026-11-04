#!/usr/bin/env python3

"""
指标交互管理器
"""

from src.ui.data_stock_controller import DEFAULT_BAR_COUNT, MIN_BAR_COUNT
from src.utils.logger import logger


class IndicatorInteractionManager:
    """指标相关交互处理"""

    def __init__(self, window):
        self.window = window
        self._scroll_area = None
        # 加号翻倍后若超过标的历史，等取数完成再收敛到实际可得根数
        self._pending_plus_clamp = False
        self._pending_plus_clamp_code = ''

    def set_scroll_area(self, scroll_area):
        """缓存指标滚动区域"""
        self._scroll_area = scroll_area

    def _set_displayed_bar_count(self, count):
        """写入唯一的柱体数状态，并同步输入框显示。"""
        self.window.displayed_bar_count = count
        bar_count_input = getattr(self.window, 'bar_count_input', None)
        if bar_count_input is not None:
            bar_count_input.setText(str(count))

    def _sync_bar_count_from_input(self):
        """把输入框中的有效值同步到 displayed_bar_count。

        周期切换按钮可能在输入框 editingFinished 之前触发，这里兜底同步，
        保证“周期切换读取的是用户当前设定值”。
        """
        bar_count_input = getattr(self.window, 'bar_count_input', None)
        if bar_count_input is None:
            return
        try:
            value = int(bar_count_input.text().strip())
        except (TypeError, ValueError):
            return
        if value > 0:
            self.window.displayed_bar_count = value

    def _available_bar_rows(self):
        """当前标的已加载的可得根数；无数据时返回 None。"""
        data = getattr(self.window, 'current_stock_data', None)
        if data is None:
            return None
        try:
            return len(data)
        except TypeError:
            return None

    def update_bar_count_controls(self):
        """按当前设定值与可得根数刷新增减按钮可用状态（到边界置灰）。"""
        plus_btn = getattr(self.window, 'plus_btn', None)
        minus_btn = getattr(self.window, 'minus_btn', None)
        bar_count = getattr(self.window, 'displayed_bar_count', DEFAULT_BAR_COUNT)
        available = self._available_bar_rows()

        if minus_btn is not None:
            minus_btn.setEnabled(bar_count > MIN_BAR_COUNT)
        if plus_btn is not None:
            # 无数据时无法判断上限，禁用增加；有数据时不得超过可得行数
            plus_btn.setEnabled(available is not None and bar_count < available)

    def on_data_loaded(self):
        """一次取数/绘图完成后调用：收敛加号越界设置并刷新按钮状态。"""
        if self._pending_plus_clamp:
            current_code = getattr(self.window, 'current_stock_code', '')
            if current_code == self._pending_plus_clamp_code:
                available = self._available_bar_rows()
                if available is not None and self.window.displayed_bar_count > available:
                    logger.info(f"加号超过可得行数，设定值收敛为 {available}")
                    self._set_displayed_bar_count(available)
            self._pending_plus_clamp = False
            self._pending_plus_clamp_code = ''
        self.update_bar_count_controls()

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
            current = window.displayed_bar_count
            available = self._available_bar_rows()
            if available is None:
                logger.warning("current_stock_data 为 None，无法增加柱体数")
                self.update_bar_count_controls()
                return
            if current >= available:
                logger.info(f"柱体数已到上限 {available}，无法继续增加")
                self.update_bar_count_controls()
                return

            new_count = current * 2
            self._pending_plus_clamp = True
            self._pending_plus_clamp_code = getattr(window, 'current_stock_code', '')
            self._set_displayed_bar_count(new_count)
            logger.info(f"柱体数增加1倍，当前柱体数: {new_count}，已加载: {available}")
            self._apply_bar_count_change()
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"增加柱体数失败: {e}")

    def on_minus_btn_clicked(self):
        window = self.window
        try:
            current = window.displayed_bar_count
            if current <= MIN_BAR_COUNT:
                logger.info(f"柱体数已到下限 {MIN_BAR_COUNT}，无法继续减少")
                self.update_bar_count_controls()
                return

            new_count = max(MIN_BAR_COUNT, current // 2)
            self._set_displayed_bar_count(new_count)
            logger.info(f"柱体数减少（减半且不低于 {MIN_BAR_COUNT}），当前柱体数: {new_count}")
            self._apply_bar_count_change()
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"减少柱体数失败: {e}")

    def _redraw_current_data(self):
        """用已加载数据按新柱体数重绘；数据不足时展示实际可得根数。"""
        window = self.window
        if window.current_stock_data is None:
            logger.warning("current_stock_data 为 None，无法重新绘制K线图")
            return
        data_len = len(window.current_stock_data)
        actual_count = min(window.displayed_bar_count, data_len)
        logger.info(
            f"重新绘制K线图，数据长度: {data_len}, "
            f"请求柱体数: {window.displayed_bar_count}, 实际显示: {actual_count}"
        )
        window.plot_k_line(window.current_stock_data, window.current_stock_name, window.current_stock_code)

    def _apply_bar_count_change(self):
        """柱体数变更后要么原地重绘，要么按新设定值重新取数。

        已加载行数足够时直接重绘（快）；设定值超过已加载行数时重新取数，
        取数窗口由 history_window_for_bars 按周期换算，避免“设 1000 根却只有 2 年数据”。
        """
        window = self.window
        setting = window.displayed_bar_count
        loaded = self._available_bar_rows()

        has_context = bool(getattr(window, 'current_stock_code', '')) and bool(
            getattr(window, 'current_stock_name', '')
        )
        if loaded is not None and setting > loaded and has_context:
            logger.info(f"柱体数 {setting} 超过已加载 {loaded} 行，按新设定值重新取数")
            window.process_stock_data(window.current_stock_code, window.current_stock_name)
        elif window.current_stock_data is not None:
            self._redraw_current_data()

        self.update_bar_count_controls()

    def on_left_arrow_clicked(self):
        try:
            scroll_area = self._scroll_area
            if scroll_area:
                current_pos = scroll_area.horizontalScrollBar().value()
                new_pos = max(0, current_pos - 100)
                scroll_area.horizontalScrollBar().setValue(new_pos)
                logger.info(f"指标选择栏向左滚动，当前位置: {new_pos}")
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"指标选择栏向左滚动失败: {e}")

    def on_right_arrow_clicked(self):
        try:
            scroll_area = self._scroll_area
            if scroll_area:
                current_pos = scroll_area.horizontalScrollBar().value()
                max_pos = scroll_area.horizontalScrollBar().maximum()
                new_pos = min(max_pos, current_pos + 100)
                scroll_area.horizontalScrollBar().setValue(new_pos)
                logger.info(f"指标选择栏向右滚动，当前位置: {new_pos}")
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"指标选择栏向右滚动失败: {e}")

    def on_period_changed(self, period, checked):
        window = self.window
        if checked:
            for name, button in window.period_buttons.items():
                if name != period:
                    button.setChecked(False)

            # 输入框可能尚未触发 editingFinished，先同步到唯一状态变量，
            # 保证周期切换后仍按用户当前设定的柱体数取数。
            self._sync_bar_count_from_input()
            window.current_period = period

            # 切换周期时，如果有当前股票数据，重新获取数据并绘制
            if hasattr(window, 'current_stock_code') and hasattr(window, 'current_stock_name'):
                stock_code = window.current_stock_code
                stock_name = window.current_stock_name
                if stock_code and stock_name:
                    logger.info(f"切换到{period}，按照柱体数 {window.displayed_bar_count} 重新获取数据")
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

            # 画布数量变化后立即重算最小高度与分配比例（高度不足时由滚动容器兜底）
            if hasattr(window, '_update_chart_height_policy'):
                window._update_chart_height_policy(rebalance=True)

            logger.info(f"切换到{window_count}个窗口")

    def on_bar_count_changed(self):
        window = self.window
        try:
            bar_count_text = window.bar_count_input.text().strip()
            bar_count = int(bar_count_text)

            if bar_count > 0:
                self._set_displayed_bar_count(bar_count)
                logger.info(f"显示柱体数量更新为: {bar_count}")
                self._apply_bar_count_change()
            else:
                self._set_displayed_bar_count(DEFAULT_BAR_COUNT)
                logger.info(f"无效的柱体数量，已恢复为默认值: {DEFAULT_BAR_COUNT}")
        except ValueError:
            self._set_displayed_bar_count(DEFAULT_BAR_COUNT)
            logger.info(f"无效的输入，已恢复为默认值: {DEFAULT_BAR_COUNT}")

    def on_adjustment_clicked(self, checked):
        window = self.window
        try:
            # 始终显示复权选择菜单，不检查 checked 状态
            menu = self._create_adjustment_menu()
            menu.exec_(window.adjustment_btn.mapToGlobal(window.adjustment_btn.rect().bottomLeft()))
            # 恢复按钮状态
            window.adjustment_btn.setChecked(True)
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"处理复权按钮点击失败: {e}")

    def _create_adjustment_menu(self):
        """创建复权选择菜单"""
        from PySide6.QtGui import QAction
        from PySide6.QtWidgets import QMenu

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
                # 强制重绘图表
                if hasattr(window, 'tech_plot_widget') and window.tech_plot_widget:
                    window.tech_plot_widget.repaint()
