#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
主窗口绘图逻辑混入类（重构后）
负责处理K线图绘制、指标绘制和图表交互
"""

import polars as pl

from src.utils.logger import logger
from src.ui.chart_components import (
    ChartDataPreparer,
    IndicatorRenderer,
    ChartEventBinder
)


class MainWindowDrawingMixin:
    def __init__(self, *args, **kwargs):
        """初始化绘图Mixin"""
        # 调用父类初始化（支持多重继承）
        super().__init__(*args, **kwargs)
        
        # 初始化各组件
        self._data_preparer = ChartDataPreparer()
        self._indicator_renderer = IndicatorRenderer(self)
        self._event_binder = ChartEventBinder(self)
        
        # 初始化状态属性
        self.vline = None
        self.hline = None
        self.volume_vline = None
        self.volume_hline = None
        self.kdj_vline = None
        self.kdj_hline = None
        
        self.ma_points = []
        self.moving_averages = {}
        self.selected_ma = None
        self.crosshair_enabled = False
        
        # MA线显示状态（默认显示）
        self.show_ma_lines = True
        
        self.current_kline_index = -1
        self.current_kline_data = {}
        self.current_mouse_pos = None

        # 日期格式化缓存
        self._date_format_cache = {}

    def _format_date(self, date_value):
        """
        格式化日期，使用缓存避免重复计算

        Args:
            date_value: 日期值（可以是字符串、datetime、numpy.datetime64等）

        Returns:
            str: 格式化后的日期字符串 'YYYY-MM-DD'
        """
        # 检查缓存
        if date_value in self._date_format_cache:
            return self._date_format_cache[date_value]

        # 格式化日期
        if isinstance(date_value, str):
            # 如果已经是字符串，直接返回
            formatted = date_value[:10] if len(date_value) >= 10 else date_value
        elif hasattr(date_value, 'strftime'):
            # datetime 或 pandas Timestamp 对象
            formatted = date_value.strftime('%Y-%m-%d')
        else:
            # numpy.datetime64 或其他类型，转换为字符串
            formatted = str(date_value)[:10]

        # 缓存结果
        self._date_format_cache[date_value] = formatted
        return formatted

    def _reset_drawing_state(self):
        """重置绘图状态"""
        self.crosshair_enabled = False
        self.current_kline_index = -1
    
    def _clear_plots(self):
        """清空图表"""
        self.tech_plot_widget.clear()
        self.volume_plot_widget.clear()
        # 清除指标图表（如KDJ）
        if hasattr(self, 'kdj_plot_widget') and self.kdj_plot_widget:
            self.kdj_plot_widget.clear()
    
    def _prepare_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        准备K线图数据
        
        Args:
            df: 原始数据
            
        Returns:
            pl.DataFrame: 处理后的数据
        """
        bar_count = getattr(self, 'displayed_bar_count', 100)
        adjustment_type = getattr(self, 'adjustment_type', 'qfq')
        
        return self._data_preparer.prepare_kline_data(df, bar_count, adjustment_type)
    
    def _ensure_indicators(self, df_pl):
        """
        渲染前一次性批量补齐所选指标的缺失列

        汇总副图窗口指标与主图叠加指标（SAR/BOLL）的勾选状态，
        通过单个 Polars 查询计划算齐，避免渲染过程中逐指标
        重复构造分析器和 Pandas↔Polars 往返转换。

        Args:
            df_pl: Polars DataFrame

        Returns:
            pl.DataFrame: 补齐指标列后的数据（失败时返回原数据）
        """
        try:
            from src.ui.chart_components.indicator_renderer import ensure_indicator_columns

            names = []
            if hasattr(self, 'window_indicators') and self.window_indicators:
                for window_index in (2, 3):
                    name = self.window_indicators.get(window_index)
                    if name:
                        names.append(name)
            if hasattr(self, 'indicator_buttons') and self.indicator_buttons:
                for name in ('SAR', 'BOLL'):
                    btn = self.indicator_buttons.get(name)
                    if btn is not None and btn.isChecked():
                        names.append(name)
            return ensure_indicator_columns(df_pl, names)
        except Exception as e:
            logger.warning(f"批量补齐指标列失败，按原数据继续渲染: {e}")
            return df_pl

    def _extract_prices(self, df: pl.DataFrame) -> tuple:
        """
        提取价格数据
        
        Args:
            df: 已应用复权的数据
            
        Returns:
            tuple: (dates, opens, highs, lows, closes)
        """
        return self._data_preparer.extract_price_data(df)
    
    def _setup_crosshair(self):
        """设置十字线"""
        (self.vline, self.hline,
         self.volume_vline, self.volume_hline,
         self.kdj_vline, self.kdj_hline) = self._event_binder.setup_crosshair()
        
        # 设置信息文本项
        self.info_text = self._event_binder.setup_info_text()
    
    def _bind_events(self, dates, opens, highs, lows, closes):
        """
        绑定事件
        
        Args:
            dates: 日期数组
            opens: 开盘价数组
            highs: 最高价数组
            lows: 最低价数组
            closes: 收盘价数组
        """
        self._event_binder.bind_all_events(dates, opens, highs, lows, closes)
    
    def _save_current_data(self, df, stock_name, stock_code, dates, opens, highs, lows, closes):
        """
        保存当前数据
        
        Args:
            df: 完整数据
            stock_name: 股票名称
            stock_code: 股票代码
            dates: 日期数组
            opens: 开盘价数组
            highs: 最高价数组
            lows: 最低价数组
            closes: 收盘价数组
        """
        self.current_stock_data = df
        self.current_stock_name = stock_name
        self.current_stock_code = stock_code
        
        # 创建OHLC列表
        ohlc_list = self._data_preparer.create_ohlc_data(opens, highs, lows, closes)
        
        self.current_kline_data = {
            'dates': dates,
            'opens': opens,
            'highs': highs,
            'lows': lows,
            'closes': closes,
            'ohlc_list': ohlc_list
        }
        
        self.current_mouse_pos = None
        self.current_kline_index = -1
