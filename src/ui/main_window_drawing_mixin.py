#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
主窗口绘图逻辑混入类（重构后）
负责处理K线图绘制、指标绘制和图表交互
"""

import pyqtgraph as pg
import polars as pl
import numpy as np
import pandas as pd
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QLabel

from src.utils.logger import logger
from src.ui.chart_items import CandleStickItem
from src.ui.dividend_marker import DividendMarkerManager
from src.ui.chart_components import (
    ChartDataPreparer,
    ChartUIBuilder,
    IndicatorRenderer,
    ChartEventBinder
)
from src.ui.utils.chart_utils import ChartUtils


class MainWindowDrawingMixin:
    """
    主窗口绘图逻辑混入类（重构后）
    使用组件化设计，将职责委托给各个专门的类
    """
    
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
        
        self.current_kline_index = -1
        self.current_kline_data = {}
        self.current_mouse_pos = None
    
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
            
            # 4. 提取价格数据
            dates, opens, highs, lows, closes = self._extract_prices(df_pl)
            
            # 5. 创建标题区域
            self._create_title_section(stock_name, stock_code)
            
            # 6. 绘制K线图
            self._draw_candlestick(dates, opens, highs, lows, closes)
            
            # 7. 绘制MA线
            x = np.arange(len(dates))
            self._indicator_renderer.render_ma_lines(self.tech_plot_widget, df_pl, x)
            
            # 8. 绘制高低点标注
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
            
            logger.info(f"成功绘制 {stock_name}({stock_code}) 的K线图")
            
        except Exception as e:
            logger.exception(f"绘制K线图失败: {e}")
            self.statusBar().showMessage(f"绘制K线图失败: {str(e)[:50]}...", 5000)
    
    def _reset_drawing_state(self):
        """重置绘图状态"""
        self.crosshair_enabled = False
        self.current_kline_index = -1
    
    def _clear_plots(self):
        """清空图表"""
        self.tech_plot_widget.clear()
        self.volume_plot_widget.clear()
    
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
    
    def _extract_prices(self, df: pl.DataFrame) -> tuple:
        """
        提取价格数据
        
        Args:
            df: 数据
            
        Returns:
            tuple: (dates, opens, highs, lows, closes)
        """
        adjustment_type = getattr(self, 'adjustment_type', 'qfq')
        return self._data_preparer.extract_price_data(df, adjustment_type)
    
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
        
        # 设置Y轴范围
        y_min = np.min(lows) * 0.99
        y_max = np.max(highs) * 1.01
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
                    except Exception:
                        pass
    
    def _draw_high_point_label(self, dates, highs, high_index, current_high):
        """绘制最高点标注"""
        high_date = pd.Timestamp(dates[high_index]).strftime('%Y-%m-%d')
        
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
        low_date = pd.Timestamp(dates[low_index]).strftime('%Y-%m-%d')
        
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
    
    def _load_dividend_markers(self, stock_code: str, dates):
        """
        加载分红标记
        
        Args:
            stock_code: 股票代码
            dates: 日期数组
        """
        self._load_and_show_dividend_markers(stock_code, dates)
    
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
            (i, pd.Timestamp(dates[i]).strftime('%Y-%m-%d'))
            for i in range(0, len(dates), tick_interval)
        ]])
    
    def _render_indicator_windows(self, df_pl, x, dates):
        """
        渲染指标窗口
        
        Args:
            df_pl: 数据
            x: x轴坐标
            dates: 日期数组
        """
        # 第2窗口指标
        indicator_2 = self.window_indicators[2]
        self._render_indicator_window(2, indicator_2, df_pl, x, dates)
        
        # 第3窗口指标
        indicator_3 = self.window_indicators[3]
        self._render_indicator_window(3, indicator_3, df_pl, x, dates)
    
    def _render_indicator_window(self, window_index, indicator_name, df_pl, x, dates):
        """
        渲染单个指标窗口
        
        Args:
            window_index: 窗口索引
            indicator_name: 指标名称
            df_pl: 数据
            x: x轴坐标
            dates: 日期数组
        """
        # 获取对应的plot widget
        if window_index == 2:
            plot_widget = self.volume_plot_widget
        elif window_index == 3:
            plot_widget = self.kdj_plot_widget
        else:
            return
        
        # 渲染指标
        self._indicator_renderer.render_indicator(plot_widget, indicator_name, x, df_pl)
        
        # 创建标签栏
        ui_builder = ChartUIBuilder(self)
        container = ui_builder.create_indicator_label_bar(window_index, indicator_name, df_pl)
        
        if container:
            # 设置X轴范围和刻度
            plot_widget.setXRange(-1, len(dates) - 1)
            
            # 设置X轴刻度标签
            ax = plot_widget.getAxis('bottom')
            tick_interval = ChartUtils.calculate_x_axis_tick_interval(len(dates))
            ax.setTicks([[
                (i, pd.Timestamp(dates[i]).strftime('%Y-%m-%d'))
                for i in range(0, len(dates), tick_interval)
            ]])
            
            # 添加到容器布局
            ui_builder.add_label_to_container(container, None, window_index)
        
        # 保存指标数据
        self._save_indicator_data(indicator_name, df_pl)
    
    def _save_indicator_data(self, indicator_name: str, df_pl):
        """
        保存指标数据
        
        Args:
            indicator_name: 指标名称
            df_pl: 数据
        """
        # KDJ数据
        if 'k' in df_pl.columns:
            self.current_kdj_data = {
                'k': df_pl['k'].to_list(),
                'd': df_pl['d'].to_list() if 'd' in df_pl.columns else [],
                'j': df_pl['j'].to_list() if 'j' in df_pl.columns else [],
            }
        
        # RSI数据
        if 'rsi14' in df_pl.columns:
            self.current_rsi_data = {'rsi': df_pl['rsi14'].to_list()}
        
        # MACD数据
        if 'macd' in df_pl.columns:
            self.current_macd_data = {
                'macd': df_pl['macd'].to_list(),
                'macd_signal': df_pl['macd_signal'].to_list() if 'macd_signal' in df_pl.columns else [],
                'macd_hist': df_pl['macd_hist'].to_list() if 'macd_hist' in df_pl.columns else [],
            }
        
        # WR数据
        if 'wr1' in df_pl.columns:
            self.current_wr_data = {
                'wr1': df_pl['wr1'].to_list(),
                'wr2': df_pl['wr2'].to_list() if 'wr2' in df_pl.columns else [],
            }
        
        # BOLL数据
        if 'mb' in df_pl.columns:
            self.current_boll_data = {
                'mb': df_pl['mb'].to_list(),
                'up': df_pl['up'].to_list() if 'up' in df_pl.columns else [],
                'dn': df_pl['dn'].to_list() if 'dn' in df_pl.columns else [],
            }
        
        # 成交量数据
        if 'volume' in df_pl.columns:
            self.current_volume_data = {
                'volume': df_pl['volume'].to_list(),
                'vol_ma5': df_pl['vol_ma5'].to_list() if 'vol_ma5' in df_pl.columns else [],
                'vol_ma10': df_pl['vol_ma10'].to_list() if 'vol_ma10' in df_pl.columns else [],
            }
        
        # VR数据
        if 'vr' in df_pl.columns:
            self.current_vr_data = {
                'vr': df_pl['vr'].to_list(),
                'mavr': df_pl['mavr'].to_list() if 'mavr' in df_pl.columns else [],
            }
        
        # DMI数据
        if 'pdi' in df_pl.columns:
            self.current_dmi_data = {
                'pdi': df_pl['pdi'].to_list(),
                'ndi': df_pl['ndi'].to_list() if 'ndi' in df_pl.columns else [],
                'adx': df_pl['adx'].to_list() if 'adx' in df_pl.columns else [],
                'adxr': df_pl['adxr'].to_list() if 'adxr' in df_pl.columns else [],
            }
        
        # TRIX数据
        if 'trix' in df_pl.columns:
            self.current_trix_data = {
                'trix': df_pl['trix'].to_list(),
                'trma': df_pl['trma'].to_list() if 'trma' in df_pl.columns else [],
            }
        
        # BRAR数据
        if 'br' in df_pl.columns:
            self.current_brar_data = {
                'br': df_pl['br'].to_list(),
                'ar': df_pl['ar'].to_list() if 'ar' in df_pl.columns else [],
            }
    
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
    
    def draw_indicator(self, plot_widget, indicator_name, x, df_pl):
        """
        根据指标名称绘制相应的指标（兼容旧接口）
        
        Args:
            plot_widget: 图表控件
            indicator_name: 指标名称
            x: x轴坐标
            df_pl: 数据
            
        Returns:
            Any: 处理后的数据
        """
        return self.chart_manager.draw_indicator(plot_widget, indicator_name, x, df_pl)
    
    def keyPressEvent(self, event):
        """
        处理键盘事件
        
        Args:
            event: 键盘事件
        """
        # 检查是否按下了ESC键
        if event.key() == Qt.Key_Escape:
            # 检查当前是否在技术分析窗口
            if self.tab_widget.currentWidget() == self.tech_tab:
                # 切换到行情窗口
                self.tab_widget.setCurrentWidget(self.market_tab)
        
        # 调用父类方法处理其他键盘事件
        try:
            super().keyPressEvent(event)
        except AttributeError:
            pass
    
    # ===== 分红标记相关方法（保持原有逻辑） =====
    
    def _load_and_show_dividend_markers(self, stock_code, dates):
        """加载并显示分红配股标记"""
        try:
            # 初始化分红标记管理器
            if not hasattr(self, 'dividend_marker_manager'):
                self.dividend_marker_manager = DividendMarkerManager(self.tech_plot_widget)
            else:
                self.dividend_marker_manager.clear_markers()
            
            # 从数据管理器获取分红数据
            if hasattr(self, 'data_manager') and self.data_manager and len(dates) > 0:
                dividend_data = self._get_stock_dividend_data(stock_code)
                
                if dividend_data:
                    filtered_dividends = self._filter_dividends_by_dates(dividend_data, dates)
                    if filtered_dividends:
                        self.dividend_marker_manager.set_dividend_data(filtered_dividends, dates)
        
        except Exception as e:
            logger.exception(f"加载分红标记失败: {e}")
    
    def _get_stock_dividend_data(self, stock_code):
        """获取股票分红数据"""
        try:
            from src.database.models.stock import StockDividend
            
            # 处理股票代码格式
            ts_code = self._format_stock_code(stock_code)
            
            # 查询数据库
            if hasattr(self, 'data_manager') and self.data_manager:
                try:
                    if hasattr(self.data_manager, 'db_manager'):
                        db_manager = self.data_manager.db_manager
                        session = db_manager.get_session()
                    elif hasattr(self.data_manager, 'get_session'):
                        session = self.data_manager.get_session()
                    else:
                        return []
                    
                    if session:
                        try:
                            dividends = session.query(StockDividend).filter(
                                StockDividend.ts_code == ts_code
                            ).all()
                            
                            result = []
                            for div in dividends:
                                result.append({
                                    'ex_date': div.ex_date,
                                    'record_date': div.record_date,
                                    'pay_date': div.pay_date,
                                    'cash_div': div.cash_div,
                                    'share_div': div.share_div
                                })
                            return result
                        finally:
                            session.close()
                except Exception as e:
                    logger.exception(f"获取分红数据失败: {e}")
            
            return []
        except Exception as e:
            logger.exception(f"获取分红数据失败: {e}")
            return []
    
    def _format_stock_code(self, stock_code: str) -> str:
        """格式化股票代码"""
        if '.' in stock_code:
            return stock_code
        elif stock_code.startswith(('sh', 'sz')):
            market = 'SH' if stock_code.startswith('sh') else 'SZ'
            code = stock_code[2:]
            return f"{code}.{market}"
        else:
            if stock_code.startswith('6'):
                return f"{stock_code}.SH"
            else:
                return f"{stock_code}.SZ"
    
    def _filter_dividends_by_dates(self, dividend_data, kline_dates):
        """过滤在K线日期范围内的分红数据"""
        try:
            if len(kline_dates) == 0:
                return []
            
            # 转换K线日期为datetime.date
            kline_date_set = set()
            for d in kline_dates:
                if hasattr(d, 'date') and callable(getattr(d, 'date')):
                    kline_date_set.add(d.date())
                elif hasattr(d, 'strftime'):
                    kline_date_set.add(d.date() if hasattr(d, 'date') else d)
                else:
                    try:
                        if hasattr(d, 'astype'):
                            dt = d.astype('datetime64[D]').item()
                            if hasattr(dt, 'date'):
                                kline_date_set.add(dt.date())
                            else:
                                kline_date_set.add(dt)
                    except Exception:
                        kline_date_set.add(d)
            
            # 过滤分红数据
            filtered = []
            for div in dividend_data:
                ex_date = div.get('ex_date')
                if ex_date:
                    if hasattr(ex_date, 'date') and callable(getattr(ex_date, 'date')):
                        ex_date = ex_date.date()
                    
                    if ex_date in kline_date_set:
                        filtered.append(div)
            
            return filtered
        except Exception as e:
            logger.exception(f"过滤分红数据失败: {e}")
            return []
    
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
        except Exception as e:
            logger.debug(f"显示分红提示框失败: {e}")
    
    def hide_dividend_tooltip(self):
        """隐藏分红信息提示框"""
        try:
            if hasattr(self, 'dividend_marker_manager') and self.dividend_marker_manager:
                self.dividend_marker_manager.hide_tooltip()
        except Exception as e:
            logger.debug(f"隐藏分红提示框失败: {e}")
