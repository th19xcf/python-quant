from src.ui.event_handlers.indicator_display import (
    WINDOW2_TEXTS,
    WINDOW3_TEXTS,
    render_indicator_text,
)
from src.utils.logger import logger


class MouseHandler:
    """
    鼠标事件处理器，负责处理鼠标相关事件
    """

    def __init__(self, main_window):
        """初始化鼠标事件处理器，main_window 为主窗口实例。"""
        self.main_window = main_window
        # 指标文本渲染缓存键：(id(dates), index)，同一根K线不重复重建富文本
        self._last_render_key = None

    def _format_value(self, value, format_str="{:.2f}"):
        """格式化指标值，None 或格式错误时返回 "--"。"""
        if value is None:
            return "--"
        try:
            return format_str.format(value)
        except (TypeError, ValueError):
            return "--"

    def handle_mouse_moved(self, pos, dates, opens, highs, lows, closes):
        """处理鼠标移动事件：更新十字线、分红提示与各窗口指标文本。"""
        try:
            index = self._resolve_index(pos, dates)
            if not 0 <= index < len(dates):
                return

            main_window = self.main_window
            main_window.current_mouse_pos = pos
            main_window.current_kline_index = index
            main_window.current_kline_data = {
                'dates': dates,
                'opens': opens,
                'highs': highs,
                'lows': lows,
                'closes': closes,
                'index': index
            }

            info_changed = self._update_tooltips(index, dates, opens, highs, lows, closes)
            self._check_dividend_hover(index, pos)
            self._update_crosshair(pos, index, len(dates), info_changed)
        except (OSError, RuntimeError) as e:
            logger.exception(f"处理鼠标移动事件时发生错误: {e}")

    def _resolve_index(self, pos, dates):
        """依次在 K线/成交量/KDJ 视图中换算鼠标位置，返回最接近的K线索引。"""
        x_val = -1
        for widget in (self.main_window.tech_plot_widget,
                       self.main_window.volume_plot_widget,
                       self.main_window.kdj_plot_widget):
            x = widget.getViewBox().mapSceneToView(pos).x()
            if 0 <= x < len(dates):
                x_val = x
                break
        return int(round(x_val))

    def _update_tooltips(self, index, dates, opens, highs, lows, closes):
        """更新均线值与两处指标提示，返回本次是否发生了重建。"""
        key = (id(dates), index)
        if key == self._last_render_key:
            return False
        # 切换股票/刷新数据时 dates 是新列表对象，id() 随之变化即自动失效，无需额外清理
        self._last_render_key = key

        main_window = self.main_window
        main_window.update_ma_values_display(index, dates, opens, highs, lows, closes)
        fmt = self._format_value
        for window, texts, label_name in (
            (2, WINDOW2_TEXTS, 'volume_values_label'),
            (3, WINDOW3_TEXTS, 'kdj_values_label'),
        ):
            if not hasattr(main_window, label_name):
                continue
            indicator = main_window.window_indicators[window]
            attr = 'current_volume_data' if indicator == 'VOL' else f'current_{indicator.lower()}_data'
            data = getattr(main_window, attr, None)
            text = render_indicator_text(texts, indicator, data, index, fmt)
            if text:
                getattr(main_window, label_name).setText(text)
        return True

    def _update_crosshair(self, pos, index, dates_len, info_changed):
        """更新三个图表的十字线；同一根K线只在索引变化时刷新信息框。"""
        main_window = self.main_window
        if not (main_window.crosshair_enabled and 0 <= index < dates_len):
            return
        names = ('vline', 'hline', 'volume_vline', 'volume_hline', 'kdj_vline', 'kdj_hline')
        if not all(hasattr(main_window, name) for name in names):
            return

        # 水平线 y 值依赖鼠标位置，每次移动都必须更新
        tech_y = main_window.tech_plot_widget.getViewBox().mapSceneToView(pos).y()
        volume_y = main_window.volume_plot_widget.getViewBox().mapSceneToView(pos).y()
        kdj_y = main_window.kdj_plot_widget.getViewBox().mapSceneToView(pos).y()
        for name in ('vline', 'volume_vline', 'kdj_vline'):
            getattr(main_window, name).setValue(index)
            getattr(main_window, name).show()
        for name, y_val in (('hline', tech_y), ('volume_hline', volume_y), ('kdj_hline', kdj_y)):
            getattr(main_window, name).setValue(y_val)
            getattr(main_window, name).show()

        if info_changed:
            main_window.show_info_box()

    def _check_dividend_hover(self, index, pos):
        """
        检查是否悬停在分红标记上，如果是则显示分红信息提示框

        Args:
            index: K线索引
            pos: 鼠标位置
        """
        try:
            # 检查主窗口是否有分红标记管理器
            if hasattr(self.main_window, 'dividend_marker_manager') and self.main_window.dividend_marker_manager:
                # 如果十字线已启用（显示股票信息框），则不显示股息变动信息，避免重叠
                if getattr(self.main_window, 'crosshair_enabled', False):
                    self.main_window.hide_dividend_tooltip()
                    return

                # 检查当前索引位置是否有分红数据
                if self.main_window.dividend_marker_manager.has_dividend_at_index(index):
                    self.main_window.show_dividend_tooltip(index, pos)
                else:
                    self.main_window.hide_dividend_tooltip()
        except (OSError, RuntimeError) as e:
            logger.debug(f"检查分红悬停失败: {e}")

    def handle_mouse_clicked(self, event, dates, opens, highs, lows, closes):
        """
        处理鼠标点击事件

        Args:
            event: 鼠标点击事件
            dates: 日期列表
            opens: 开盘价列表
            highs: 最高价列表
            lows: 最低价列表
            closes: 收盘价列表
        """
        try:
            if not event.double():
                # 单击事件，调用均线点击处理函数
                self.main_window.on_ma_clicked(event)
                return

            # 双击切换十字线和信息框显示状态
            self.main_window.crosshair_enabled = not self.main_window.crosshair_enabled
            if not self.main_window.crosshair_enabled:
                logger.debug("双击K线图，禁用十字线和信息框")
                self.main_window.vline.hide()
                self.main_window.hline.hide()
                self.main_window.volume_vline.hide()
                self.main_window.volume_hline.hide()
                self.main_window.kdj_vline.hide()
                self.main_window.kdj_hline.hide()
                if self.main_window.info_text is not None:
                    self.main_window.info_text.hide()
                return

            logger.debug("双击K线图，启用十字线和信息框")
            # 如果当前有K线数据，显示十字线
            if self.main_window.current_kline_data:
                pos = event.pos()
                view_pos = self.main_window.tech_plot_widget.getViewBox().mapSceneToView(pos)
                index = int(round(view_pos.x()))
                index = max(0, min(len(dates) - 1, index))

                self.main_window.current_kline_index = index
                self.main_window.current_mouse_pos = pos
                self.main_window.current_kline_data = {
                    'dates': dates,
                    'opens': opens,
                    'highs': highs,
                    'lows': lows,
                    'closes': closes,
                    'index': index
                }

                # 显示K线图十字线
                self.main_window.vline.setValue(index)
                self.main_window.hline.setValue(view_pos.y())
                self.main_window.vline.show()
                self.main_window.hline.show()

                # 显示成交量图十字线
                self.main_window.volume_vline.setValue(index)
                volume_pos = self.main_window.volume_plot_widget.getViewBox().mapSceneToView(pos)
                self.main_window.volume_hline.setValue(volume_pos.y())
                self.main_window.volume_vline.show()
                self.main_window.volume_hline.show()

                # 显示KDJ图十字线
                self.main_window.kdj_vline.setValue(index)
                kdj_pos = self.main_window.kdj_plot_widget.getViewBox().mapSceneToView(pos)
                self.main_window.kdj_hline.setValue(kdj_pos.y())
                self.main_window.kdj_vline.show()
                self.main_window.kdj_hline.show()

                # 直接显示信息框，不需要等待鼠标移动
                self.main_window.show_info_box()
        except (OSError, RuntimeError) as e:
            logger.exception(f"处理鼠标点击事件时发生错误: {e}")
