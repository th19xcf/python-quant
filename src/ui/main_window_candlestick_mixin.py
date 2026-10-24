#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
主窗口 K 线主图 Mixin：蜡烛图、极值标注、坐标轴与标题。
"""

import pyqtgraph as pg
import numpy as np

from src.utils.logger import logger
from src.ui.chart_items import CandleStickItem
from src.ui.chart_components import ChartUIBuilder
from src.ui.utils.chart_utils import ChartUtils


class MainWindowCandlestickMixin:
    def plot_k_line(self, df, stock_name, stock_code):
        """
        绘制K线图（入口方法）
        
        Args:
            df: 股票历史数据DataFrame
            stock_name: 股票名称
            stock_code: 股票代码
        """
        return self.chart_manager.plot_k_line(df, stock_name, stock_code)
    
    def _plot_k_line_impl(self, df, stock_name, stock_code):
        """
        使用pyqtgraph绘制K线图（核心实现）
        
        Args:
            df: 股票历史数据DataFrame
            stock_name: 股票名称
            stock_code: 股票代码
        """
        try:
            # 1. 重置状态
            self._reset_drawing_state()
            
            # 2. 清空图表
            self._clear_plots()
            
            # 3. 准备数据
            df_pl = self._prepare_data(df)
            if df_pl is None:
                return

            # 3.5 渲染前一次性批量补齐所选指标的缺失列（单查询计划，避免渲染时逐指标计算）
            df_pl = self._ensure_indicators(df_pl)

            # 4. 提取价格数据
            dates, opens, highs, lows, closes = self._extract_prices(df_pl)
            
            # 5. 创建标题区域
            self._create_title_section(stock_name, stock_code)
            
            # 6. 绘制K线图
            self._draw_candlestick(dates, opens, highs, lows, closes)
            
            # 7. 绘制MA线
            x = np.arange(len(dates))
            self._indicator_renderer.render_ma_lines(self.tech_plot_widget, df_pl, x)
            
            # 8. 绘制主图叠加指标（如SAR、BOLL等）
            self._render_overlay_indicators(df_pl, x)
            
            # 9. 绘制高低点标注
            self._draw_price_extremes(dates, highs, lows)
            
            # 9. 加载分红标记
            self._load_dividend_markers(stock_code, dates)
            
            # 10. 设置坐标轴
            self._setup_axes(dates)
            
            # 11. 绘制指标窗口
            self._render_indicator_windows(df_pl, x, dates)
            
            # 12. 设置十字线
            self._setup_crosshair()
            
            # 13. 绑定事件
            self._bind_events(dates, opens, highs, lows, closes)
            
            # 14. 保存当前数据
            self._save_current_data(df, stock_name, stock_code, dates, opens, highs, lows, closes)
            
            # 15. 更新窗口数量
            self.indicator_interaction_manager.on_window_count_changed(
                self.current_window_count, True
            )
            
            # 16. 更新标题栏显示最后一个交易日的均线数据
            self._update_ma_values_display(df_pl, dates)
            
            logger.info(f"成功绘制 {stock_name}({stock_code}) 的K线图")
            
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"绘制K线图失败: {e}")
            self.statusBar().showMessage(f"绘制K线图失败: {str(e)[:50]}...", 5000)
    
    def _create_title_section(self, stock_name: str, stock_code: str):
        """
        创建标题区域
        
        Args:
            stock_name: 股票名称
            stock_code: 股票代码
        """
        current_period = getattr(self, 'current_period', '日线')
        
        ui_builder = ChartUIBuilder(self)
        ui_builder.create_title_section(stock_name, stock_code, current_period)
    
    def _draw_candlestick(self, dates, opens, highs, lows, closes):
        """
        绘制K线
        
        Args:
            dates: 日期数组
            opens: 开盘价数组
            highs: 最高价数组
            lows: 最低价数组
            closes: 收盘价数组
        """
        # 创建OHLC数据
        ohlc_list = self._data_preparer.create_ohlc_data(opens, highs, lows, closes)
        
        # 创建K线图项
        self.candle_plot_item = CandleStickItem(ohlc_list)
        self.tech_plot_widget.addItem(self.candle_plot_item)
    
    def _draw_price_extremes(self, dates, highs, lows):
        """
        绘制价格极值标注
        
        Args:
            dates: 日期数组
            highs: 最高价数组
            lows: 最低价数组
        """
        # 计算极值
        current_high, current_low, high_index, low_index = \
            self._data_preparer.calculate_price_extremes(highs, lows)
        
        # 清除旧的标注
        self._clear_old_extreme_labels()
        
        # 绘制最高点标注
        self._draw_high_point_label(dates, highs, high_index, current_high)
        
        # 绘制最低点标注
        self._draw_low_point_label(dates, lows, low_index, current_low)
        
        # 设置Y轴范围（考虑K线价格和MA线）
        y_min = np.min(lows)
        y_max = np.max(highs)
        
        # 如果有MA数据，也考虑MA线的范围
        if hasattr(self, 'ma_data') and self.ma_data:
            for ma_name, ma_values in self.ma_data.items():
                if len(ma_values) > 0:
                    valid_ma = ma_values[~np.isnan(ma_values)]
                    if len(valid_ma) > 0:
                        y_min = min(y_min, np.min(valid_ma))
                        y_max = max(y_max, np.max(valid_ma))
        
        # 添加边距
        y_range = y_max - y_min
        y_min = y_min - y_range * 0.05
        y_max = y_max + y_range * 0.05
        
        self.tech_plot_widget.setYRange(y_min, y_max)
        
        # 设置X轴范围
        self.tech_plot_widget.setXRange(-1, len(dates) - 1)
    
    def _clear_old_extreme_labels(self):
        """清除旧的极值标注"""
        items_to_remove = [
            ('high_text_item', self.tech_plot_widget),
            ('low_text_item', self.tech_plot_widget),
            ('high_arrow_item', self.tech_plot_widget),
            ('low_arrow_item', self.tech_plot_widget),
        ]
        
        for attr_name, widget in items_to_remove:
            if hasattr(self, attr_name):
                item = getattr(self, attr_name)
                if item is not None:
                    try:
                        widget.removeItem(item)
                    except (OSError, RuntimeError, ValueError):
                        pass
    
    def _draw_high_point_label(self, dates, highs, high_index, current_high):
        """绘制最高点标注"""
        # 使用原生方法格式化日期，避免 pd.Timestamp 开销
        high_date = self._format_date(dates[high_index])

        self.high_text_item = pg.TextItem(f" {high_date} {current_high:.2f} ", color='w')
        self.high_text_item.setHtml(
            f'<div style="background-color: rgba(0, 0, 0, 0.8); padding: 3px; '
            f'border: 1px solid #666; font-family: monospace; font-size: 10px;">'
            f'{high_date} {current_high:.2f}</div>'
        )
        self.tech_plot_widget.addItem(self.high_text_item)
        
        self.high_arrow_item = pg.ArrowItem(
            pos=(high_index, current_high),
            angle=-45,
            brush=pg.mkBrush('w'),
            pen=pg.mkPen('w', width=1),
            tipAngle=30,
            headLen=8,
            headWidth=6,
            tailLen=0,
            tailWidth=1
        )
        self.tech_plot_widget.addItem(self.high_arrow_item)
        
        # 定位标注
        y_range = self.tech_plot_widget.viewRange()[1]
        y_min, y_max = y_range[0], y_range[1]
        self.high_text_item.setPos(high_index + 0.5, current_high + (y_max - y_min) * 0.02)
    
    def _draw_low_point_label(self, dates, lows, low_index, current_low):
        """绘制最低点标注"""
        # 使用原生方法格式化日期，避免 pd.Timestamp 开销
        low_date = self._format_date(dates[low_index])

        self.low_text_item = pg.TextItem(f" {low_date} {current_low:.2f} ", color='w')
        self.low_text_item.setHtml(
            f'<div style="background-color: rgba(0, 0, 0, 0.8); padding: 3px; '
            f'border: 1px solid #666; font-family: monospace; font-size: 10px;">'
            f'{low_date} {current_low:.2f}</div>'
        )
        self.tech_plot_widget.addItem(self.low_text_item)
        
        self.low_arrow_item = pg.ArrowItem(
            pos=(low_index, current_low),
            angle=45,
            brush=pg.mkBrush('w'),
            pen=pg.mkPen('w', width=1),
            tipAngle=30,
            headLen=8,
            headWidth=6,
            tailLen=0,
            tailWidth=1
        )
        self.tech_plot_widget.addItem(self.low_arrow_item)
        
        # 定位标注
        y_range = self.tech_plot_widget.viewRange()[1]
        y_min, y_max = y_range[0], y_range[1]
        self.low_text_item.setPos(low_index + 0.5, current_low - (y_max - y_min) * 0.02)
    
    def _setup_axes(self, dates):
        """
        设置坐标轴
        
        Args:
            dates: 日期数组
        """
        # 设置x轴刻度标签
        ax = self.tech_plot_widget.getAxis('bottom')
        tick_interval = ChartUtils.calculate_x_axis_tick_interval(len(dates))
        ax.setTicks([[
            (i, self._format_date(dates[i]))
            for i in range(0, len(dates), tick_interval)
        ]])
