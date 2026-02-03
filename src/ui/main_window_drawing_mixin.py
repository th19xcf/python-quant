import pyqtgraph as pg
import polars as pl
import numpy as np
import math
import pandas as pd
from PySide6.QtWidgets import (
    QLabel, QHBoxLayout, QWidget, QMenu, 
    QLineEdit, QPushButton, QDialog, QVBoxLayout
)
from PySide6.QtGui import QColor, QAction
from PySide6.QtCore import Qt, QPoint, QTimer

from src.utils.logger import logger
from src.ui.chart_items import CandleStickItem
from src.ui.dividend_marker import DividendMarkerManager
from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
from src.ui.utils.chart_utils import ChartUtils

class MainWindowDrawingMixin:
    """
    主窗口绘图逻辑混入类
    负责处理K线图绘制、指标绘制和图表交互
    """
    
    def plot_k_line(self, df, stock_name, stock_code):
        """
        使用ChartManager绘制K线图
        """
        return self.chart_manager.plot_k_line(df, stock_name, stock_code)

    def _plot_k_line_impl(self, df, stock_name, stock_code):
        """
        使用pyqtgraph绘制K线图
        
        Args:
            df: 股票历史数据DataFrame
            stock_name: 股票名称
            stock_code: 股票代码
        """
        # 初始化变量，避免UnboundLocalError
        df_pl = None
        x = None
        
        try:
            
            # 修复：在绘制K线图开始时重置十字线状态
            self.crosshair_enabled = False
            self.current_kline_index = -1

            # 清空图表
            self.tech_plot_widget.clear()
            self.volume_plot_widget.clear()
            
            # 检查是否已经存在标题标签，如果存在则移除
            if hasattr(self, 'chart_title_label'):
                try:
                    self.chart_title_label.deleteLater()
                except Exception as e:
                    logger.warning(f"移除旧标题标签时发生错误: {e}")
            
            # 检查是否已经存在MA标签，如果存在则移除
            if hasattr(self, 'ma_values_label'):
                try:
                    self.ma_values_label.deleteLater()
                except Exception as e:
                    logger.warning(f"移除旧MA标签时发生错误: {e}")
            
            # 创建图表标题标签，放置在左上角
            self.chart_title_label = QLabel()
            self.chart_title_label.setStyleSheet("font-family: Consolas, monospace; padding: 5px; color: #C0C0C0; background-color: transparent; border: none;")
            # 获取当前周期，如果没有设置则默认为日线
            current_period = getattr(self, 'current_period', '日线')
            self.chart_title_label.setText(f"{stock_name}({stock_code}) {current_period}")
            self.chart_title_label.setWordWrap(False)
            
            # 创建MA值显示标签
            self.ma_values_label = QLabel()
            self.ma_values_label.setStyleSheet("font-family: Consolas, monospace; padding: 5px; color: #C0C0C0; background-color: transparent; border: none;")
            # 使用HTML设置初始文本和颜色，添加日期显示
            self.ma_values_label.setText("<font color='#C0C0C0'>日期: --</font>  <font color='white'>MA5: --</font>  <font color='cyan'>MA10: --</font>  <font color='red'>MA20: --</font>  <font color='#00FF00'>MA60: --</font>")
            # 确保不换行
            self.ma_values_label.setWordWrap(False)
            
            # 检查是否已经创建了图表容器
            if hasattr(self, 'chart_container') and hasattr(self, 'chart_layout'):
                # 移除旧的标题和MA标签相关组件
                for i in range(self.chart_layout.count()):
                    item = self.chart_layout.itemAt(i)
                    if isinstance(item, QHBoxLayout):
                        # 移除旧的水平布局
                        self.chart_layout.removeItem(item)
                        break
                    elif hasattr(self, 'title_ma_container') and item.widget() == self.title_ma_container:
                        # 移除旧的容器
                        self.chart_layout.removeWidget(self.title_ma_container)
                        break
                
                # 创建一个统一的背景容器
                self.title_ma_container = QWidget()
                self.title_ma_container.setStyleSheet("background-color: #222222;")
                
                # 创建水平布局
                title_ma_layout = QHBoxLayout(self.title_ma_container)
                title_ma_layout.setSpacing(0)
                title_ma_layout.setContentsMargins(0, 0, 0, 0)
                
                # 添加功能菜单按钮到布局最左端
                self.menu_btn = self.create_indicator_menu_button(self.current_selected_window)
                title_ma_layout.addWidget(self.menu_btn)
                
                # 添加窗口标题标签
                window_title_label = QLabel("K线")
                window_title_label.setStyleSheet("background-color: transparent; color: #C0C0C0; font-size: 12px; padding: 0 5px;")
                title_ma_layout.addWidget(window_title_label)
                
                # 添加标签到布局
                title_ma_layout.addWidget(self.chart_title_label)
                title_ma_layout.addWidget(self.ma_values_label)
                title_ma_layout.addStretch(1)
                
                # 将容器添加到图表布局顶部
                self.chart_layout.insertWidget(0, self.title_ma_container)
                # logger.info("已添加标题标签和MA值显示标签，在同一行显示")
            
            # 准备K线图数据
            # 只显示指定数量的柱体
            bar_count = getattr(self, 'displayed_bar_count', 100)
            
            # 只取显示数量的数据
            df_pl = df
            
            # 修复：确保 df_pl 不为 None
            if df_pl is None:
                return
            
            # 截取指定数量的数据（当柱体数小于数据长度时才截取）
            if bar_count < len(df_pl):
                df_pl = df_pl.tail(bar_count)
            
            # 直接使用Polars的to_numpy()方法，避免多次转换
            dates = df_pl['date'].to_numpy()
            
            # 根据复权类型选择对应的价格字段
            adjustment_type = getattr(self, 'adjustment_type', 'qfq')
            if adjustment_type == 'qfq':
                # 前复权
                opens = df_pl['qfq_open'].to_numpy() if 'qfq_open' in df_pl.columns else df_pl['open'].to_numpy()
                highs = df_pl['qfq_high'].to_numpy() if 'qfq_high' in df_pl.columns else df_pl['high'].to_numpy()
                lows = df_pl['qfq_low'].to_numpy() if 'qfq_low' in df_pl.columns else df_pl['low'].to_numpy()
                closes = df_pl['qfq_close'].to_numpy() if 'qfq_close' in df_pl.columns else df_pl['close'].to_numpy()
            elif adjustment_type == 'hfq':
                # 后复权
                opens = df_pl['hfq_open'].to_numpy() if 'hfq_open' in df_pl.columns else df_pl['open'].to_numpy()
                highs = df_pl['hfq_high'].to_numpy() if 'hfq_high' in df_pl.columns else df_pl['high'].to_numpy()
                lows = df_pl['hfq_low'].to_numpy() if 'hfq_low' in df_pl.columns else df_pl['low'].to_numpy()
                closes = df_pl['hfq_close'].to_numpy() if 'hfq_close' in df_pl.columns else df_pl['close'].to_numpy()
            else:
                # 不复权
                opens = df_pl['open'].to_numpy()
                highs = df_pl['high'].to_numpy()
                lows = df_pl['low'].to_numpy()
                closes = df_pl['close'].to_numpy()
            
            # 创建x轴坐标（使用索引）
            x = np.arange(len(dates))
            
            # 创建K线图数据
            # K线图由OHLC数据组成：(x, open, high, low, close)
            ohlc = np.column_stack((x, opens, highs, lows, closes))
            
            # 转换为列表格式，适合自定义CandleStickItem
            ohlc_list = [tuple(row) for row in ohlc]
            
            # 创建K线图项
            self.candle_plot_item = CandleStickItem(ohlc_list)
            
            # 添加K线图到图表
            self.tech_plot_widget.addItem(self.candle_plot_item)
            
            # 加载并显示分红配股标记
            self._load_and_show_dividend_markers(stock_code, dates)
            
            # 设置x轴刻度标签（显示日期）
            ax = self.tech_plot_widget.getAxis('bottom')
            tick_interval = ChartUtils.calculate_x_axis_tick_interval(len(dates))
            ax.setTicks([[(i, pd.Timestamp(dates[i]).strftime('%Y-%m-%d')) for i in range(0, len(dates), tick_interval)]])
            
            # 设置Y轴范围，留出一定的边距
            y_min = np.min(lows) * 0.99
            y_max = np.max(highs) * 1.01
            self.tech_plot_widget.setYRange(y_min, y_max)
            
            # 计算当前显示区域的最高、最低点及其位置
            current_high = np.max(highs)
            current_low = np.min(lows)
            high_index = np.argmax(highs)
            low_index = np.argmin(lows)
            
            # 清除之前的最高、最低点标注
            if hasattr(self, 'high_text_item') and self.high_text_item is not None:
                self.tech_plot_widget.removeItem(self.high_text_item)
            if hasattr(self, 'low_text_item') and self.low_text_item is not None:
                self.tech_plot_widget.removeItem(self.low_text_item)
            if hasattr(self, 'high_arrow_item') and self.high_arrow_item is not None:
                self.tech_plot_widget.removeItem(self.high_arrow_item)
            if hasattr(self, 'low_arrow_item') and self.low_arrow_item is not None:
                self.tech_plot_widget.removeItem(self.low_arrow_item)
            
            # 创建最高点标注，加上日期
            high_date = pd.Timestamp(dates[high_index]).strftime('%Y-%m-%d')
            self.high_text_item = pg.TextItem(f" {high_date} {current_high:.2f} ", color='w')
            self.high_text_item.setHtml(f'<div style="background-color: rgba(0, 0, 0, 0.8); padding: 3px; border: 1px solid #666; font-family: monospace; font-size: 10px;">{high_date} {current_high:.2f}</div>')
            self.tech_plot_widget.addItem(self.high_text_item)
            
            # 创建最高点箭头 - 简单三角形箭头
            self.high_arrow_item = pg.ArrowItem(
                pos=(high_index, current_high), 
                angle=-45, 
                brush=pg.mkBrush('w'), 
                pen=pg.mkPen('w', width=1), 
                tipAngle=30, 
                headLen=8, 
                headWidth=6,
                tailLen=0,  # 没有尾巴
                tailWidth=1
            )
            self.tech_plot_widget.addItem(self.high_arrow_item)
            
            # 创建最低点标注，加上日期
            low_date = pd.Timestamp(dates[low_index]).strftime('%Y-%m-%d')
            self.low_text_item = pg.TextItem(f" {low_date} {current_low:.2f} ", color='w')
            self.low_text_item.setHtml(f'<div style="background-color: rgba(0, 0, 0, 0.8); padding: 3px; border: 1px solid #666; font-family: monospace; font-size: 10px;">{low_date} {current_low:.2f}</div>')
            self.tech_plot_widget.addItem(self.low_text_item)
            
            # 创建最低点箭头 - 简单三角形箭头
            self.low_arrow_item = pg.ArrowItem(
                pos=(low_index, current_low), 
                angle=45, 
                brush=pg.mkBrush('w'), 
                pen=pg.mkPen('w', width=1), 
                tipAngle=30, 
                headLen=8, 
                headWidth=6,
                tailLen=0,  # 没有尾巴
                tailWidth=1
            )
            self.tech_plot_widget.addItem(self.low_arrow_item)
            
            # 定位标注位置（在对应柱图旁边）
            self.high_text_item.setPos(high_index + 0.5, current_high + (y_max - y_min) * 0.02)
            self.low_text_item.setPos(low_index + 0.5, current_low - (y_max - y_min) * 0.02)
            
            # 设置X轴范围，不使用autoRange，确保与成交量图一致，左边留出空间显示第一个标签
            self.tech_plot_widget.setXRange(-1, len(dates) - 1)
            
            # 创建定时器，用于实现停留显示信息框
            self.info_timer = QTimer()
            self.info_timer.setSingleShot(True)
            self.info_timer.setInterval(200)  # 200毫秒
            self.info_timer.timeout.connect(self.show_info_box)
            
            # 禁用pyqtgraph的默认右键菜单
            
            # 方法1: 禁用viewBox的右键菜单
            if hasattr(self.tech_plot_widget, 'getViewBox') and hasattr(self.volume_plot_widget, 'getViewBox'):
                # 获取K线图viewBox
                view_box = self.tech_plot_widget.getViewBox()
                view_box.setMenuEnabled(False)
                
                # 获取成交量图viewBox
                volume_view_box = self.volume_plot_widget.getViewBox()
                volume_view_box.setMenuEnabled(False)
                
                # 连接K线图viewBox范围变化事件，将X轴范围同步到成交量图和KDJ图
                def on_kline_range_changed(view_range):
                    # 获取新的X轴范围
                    x_min, x_max = view_box.viewRange()[0]

                    # 将X轴范围应用到成交量图
                    volume_view_box.setXRange(x_min, x_max, padding=0)

                    # 将X轴范围应用到KDJ图
                    kdj_view_box = self.kdj_plot_widget.getViewBox()
                    kdj_view_box.setXRange(x_min, x_max, padding=0)

                    # 更新分红标记位置
                    if hasattr(self, 'dividend_marker_manager') and self.dividend_marker_manager:
                        self.dividend_marker_manager.update_position()

                    # logger.debug(f"从K线图同步X轴范围到成交量图和KDJ图: {x_min:.2f} - {x_max:.2f}")
                
                # 连接成交量图viewBox范围变化事件，将X轴范围同步到K线图和KDJ图
                def on_volume_range_changed(view_range):
                    # 获取新的X轴范围
                    x_min, x_max = volume_view_box.viewRange()[0]

                    # 将X轴范围应用到K线图
                    view_box.setXRange(x_min, x_max, padding=0)

                    # 将X轴范围应用到KDJ图
                    kdj_view_box = self.kdj_plot_widget.getViewBox()
                    kdj_view_box.setXRange(x_min, x_max, padding=0)

                    # 更新分红标记位置
                    if hasattr(self, 'dividend_marker_manager') and self.dividend_marker_manager:
                        self.dividend_marker_manager.update_position()

                    # logger.debug(f"从成交量图同步X轴范围到K线图和KDJ图: {x_min:.2f} - {x_max:.2f}")
                
                # 连接KDJ图viewBox范围变化事件，将X轴范围同步到K线图和成交量图
                def on_kdj_range_changed(view_range):
                    # 获取新的X轴范围
                    x_min, x_max = self.kdj_plot_widget.getViewBox().viewRange()[0]

                    # 将X轴范围应用到K线图
                    view_box.setXRange(x_min, x_max, padding=0)

                    # 将X轴范围应用到成交量图
                    volume_view_box.setXRange(x_min, x_max, padding=0)

                    # 更新分红标记位置
                    if hasattr(self, 'dividend_marker_manager') and self.dividend_marker_manager:
                        self.dividend_marker_manager.update_position()

                    # logger.debug(f"从KDJ图同步X轴范围到K线图和成交量图: {x_min:.2f} - {x_max:.2f}")
                
                # 连接三个viewBox的范围变化事件
                view_box.sigRangeChanged.connect(on_kline_range_changed)
                volume_view_box.sigRangeChanged.connect(on_volume_range_changed)
                self.kdj_plot_widget.getViewBox().sigRangeChanged.connect(on_kdj_range_changed)
            
            # 方法2: 禁用所有子项的右键菜单
            for item in self.tech_plot_widget.items():
                if hasattr(item, 'setMenuEnabled'):
                    item.setMenuEnabled(False)
            
            # 方法3: 完全替换右键菜单事件处理
            def custom_context_menu(event):
                # logger.info(f"自定义右键菜单被调用")
                
                # 创建自定义菜单
                menu = QMenu(self.tech_plot_widget)
                
                # 如果有选中的均线，添加修改指标参数选项
                if hasattr(self, 'selected_ma') and self.selected_ma:
                    modify_action = QAction(f"修改{self.selected_ma}指标参数", self)
                    modify_action.triggered.connect(lambda: self.on_modify_indicator(self.selected_ma))
                    menu.addAction(modify_action)
                else:
                    # 如果没有选中均线，添加提示信息
                    no_select_action = QAction("未选中均线，请先点击选中均线", self)
                    no_select_action.setEnabled(False)  # 禁用选项
                    menu.addAction(no_select_action)
                
                # 在鼠标位置显示菜单，确保使用QPoint类型
                qpoint = event.globalPos().toPoint()
                menu.exec(qpoint)
                
                # 阻止事件传播，防止显示默认菜单
                event.accept()
            
            # 设置自定义右键菜单
            self.tech_plot_widget.contextMenuEvent = custom_context_menu
            
            # 方法4: 连接全局上下文菜单事件
            self.tech_plot_widget.setContextMenuPolicy(Qt.CustomContextMenu)
            self.tech_plot_widget.customContextMenuRequested.connect(lambda pos: self.on_custom_context_menu(pos))
            
            # 处理技术指标绘制
            try:
                # logger.info("开始处理技术指标绘制")
                
                # 确保数据索引正确
                x = np.arange(len(df_pl))
                
                # 绘制K线图
                # K线已经由CandleStickItem绘制
                
                # 初始化均线相关属性
                if not hasattr(self, 'moving_averages'):
                    self.moving_averages = {}
                if not hasattr(self, 'selected_ma'):
                    self.selected_ma = None
                if not hasattr(self, 'ma_points'):
                    self.ma_points = []
                
                # 清除之前的标注点
                for point_item in self.ma_points:
                    self.tech_plot_widget.removeItem(point_item)
                self.ma_points.clear()
                
                # 确保ma5、ma10、ma20和ma60列存在
                if 'ma5' not in df_pl.columns or 'ma10' not in df_pl.columns or 'ma20' not in df_pl.columns or 'ma60' not in df_pl.columns:
                    # 计算均线指标
                    analyzer = TechnicalAnalyzer(df_pl)
                    analyzer.calculate_ma([5, 10, 20, 60])
                    df_pl = analyzer.get_data(return_polars=True)
                
                # 绘制5日均线（白色）
                ma5_data = df_pl['ma5'].to_numpy()
                ma5_item = self.tech_plot_widget.plot(x, ma5_data, pen=pg.mkPen('w', width=1), name='MA5')
                self.moving_averages['MA5'] = {'item': ma5_item, 'data': (x, ma5_data), 'color': 'w'}
                
                # 绘制10日均线（青色）
                ma10_data = df_pl['ma10'].to_numpy()
                ma10_item = self.tech_plot_widget.plot(x, ma10_data, pen=pg.mkPen('c', width=1), name='MA10')
                self.moving_averages['MA10'] = {'item': ma10_item, 'data': (x, ma10_data), 'color': 'c'}
                
                # 绘制20日均线（红色）
                ma20_data = df_pl['ma20'].to_numpy()
                ma20_item = self.tech_plot_widget.plot(x, ma20_data, pen=pg.mkPen('r', width=1), name='MA20')
                self.moving_averages['MA20'] = {'item': ma20_item, 'data': (x, ma20_data), 'color': 'r'}
                
                # 绘制60日均线（绿色，与K线绿色一致）
                ma60_data = df_pl['ma60'].to_numpy()
                ma60_item = self.tech_plot_widget.plot(x, ma60_data, pen=pg.mkPen(pg.mkColor(0, 255, 0), width=1), name='MA60')
                self.moving_averages['MA60'] = {'item': ma60_item, 'data': (x, ma60_data), 'color': pg.mkColor(0, 255, 0)}
                
                # 保存当前鼠标位置和K线索引
                self.current_kline_data = {
                    'dates': dates,
                    'opens': opens,
                    'highs': highs,
                    'lows': lows,
                    'closes': closes
                }
                
                # 保存计算好的MA值和颜色，用于鼠标移动时更新显示
                self.ma_data = {
                    'MA5': df_pl['ma5'].to_numpy(),
                    'MA10': df_pl['ma10'].to_numpy(),
                    'MA20': df_pl['ma20'].to_numpy(),
                    'MA60': df_pl['ma60'].to_numpy()
                }
                
                # 保存MA线的颜色映射，使用与绘制线条一致的颜色值
                self.ma_colors = {
                    'MA10': 'cyan',
                    'MA20': 'red',
                    'MA60': '#00FF00'  # 使用亮绿色，与pyqtgraph的'g'颜色一致
                }
                
                # 保存K线图数据项
                # self.candle_plot_item = candle_plot_item # Already saved above
                
                # 绘制KDJ指标
                current_indicator = self.window_indicators[3]
                # logger.info(f"绘制{current_indicator}指标")
                df_pl = self.draw_indicator(self.kdj_plot_widget, current_indicator, x, df_pl)
                
                # 仅在第3窗口显示VOL指标时，进行额外的特殊处理
                if current_indicator == "VOL":
                    # 设置成交量图的x轴与K线图一致，实现柱体对齐
                    self.kdj_plot_widget.setXRange(-1, len(df_pl) - 1)
                    
                    # 设置成交量图的X轴刻度标签，与K线图保持一致
                    kdj_ax = self.kdj_plot_widget.getAxis('bottom')
                    tick_interval = ChartUtils.calculate_x_axis_tick_interval(len(dates))
                    kdj_ax.setTicks([[(i, pd.Timestamp(dates[i]).strftime('%Y-%m-%d')) for i in range(0, len(dates), tick_interval)]])
                    
                    # 仅在当前指标是VOL时设置成交量相关的Y轴范围和样式
                    # 获取成交量数据
                    volume_data = df_pl['volume'].to_numpy()
                    volume_min = volume_data.min()
                    volume_max = volume_data.max()
                    
                    # 重置对数模式，默认使用线性刻度
                    self.kdj_plot_widget.setLogMode(y=False)
                    
                    # 计算成交量的统计信息
                    volume_mean = volume_data.mean()
                    volume_std = volume_data.std()
                    
                    # 计算合理的Y轴范围，进一步增加顶部空间
                    if volume_max > 0:
                        # 如果数据差异不大，使用基于均值和标准差的范围
                        if volume_std / volume_mean < 0.1:  # 标准差小于均值的10%，数据比较集中
                            # 扩大Y轴范围，显示更多细节，特别是顶部留出更多空间
                            y_min = max(0, volume_mean - volume_std * 2)
                            y_max = volume_mean + volume_std * 3.5  # 进一步增加顶部空间
                            self.kdj_plot_widget.setYRange(y_min, y_max)
                        else:
                            # 数据有一定差异，使用基于最小值和最大值的范围
                            y_range = volume_max - volume_min
                            y_min = max(0, volume_min - y_range * 0.1)
                            y_max = volume_max + y_range * 0.1  # 进一步增加顶部空间，从20%调整为30%
                            self.kdj_plot_widget.setYRange(y_min, y_max)
                    else:
                        # 成交量都是0，使用默认范围
                        self.kdj_plot_widget.setYRange(0, 100)
                    
                    # 禁用科学计数法，使用正常的数值显示
                    y_axis = self.kdj_plot_widget.getAxis('left')
                    y_axis.enableAutoSIPrefix(False)
                    y_axis.setStyle(tickTextOffset=20)
                    
                    # 设置X轴范围
                    self.kdj_plot_widget.setXRange(-1, len(df_pl) - 1)
                # 重新添加十字线到KDJ图中
                if hasattr(self, 'kdj_vline') and hasattr(self, 'kdj_hline') and self.kdj_vline is not None and self.kdj_hline is not None:
                    self.kdj_plot_widget.addItem(self.kdj_vline, ignoreBounds=True)
                    self.kdj_plot_widget.addItem(self.kdj_hline, ignoreBounds=True)
                    # 恢复十字线的显示状态
                    if self.crosshair_enabled:
                        self.kdj_vline.show()
                        self.kdj_hline.show()
                    else:
                        self.kdj_vline.hide()
                        self.kdj_hline.hide()
                
                # 根据当前指标更新标签栏数值
                current_indicator = self.window_indicators[3]
                
                # 创建标签栏容器
                
                # 检查是否已经存在标签，如果存在则移除
                if hasattr(self, 'kdj_values_label'):
                    try:
                        self.kdj_values_label.deleteLater()
                    except Exception as e:
                        logger.warning(f"移除旧KDJ标签时发生错误: {e}")
                
                # 检查是否已经存在标签栏容器，如果存在则先移除
                if hasattr(self, 'kdj_label_container'):
                    try:
                        # 移除旧的标签栏容器
                        if hasattr(self, 'kdj_container') and hasattr(self, 'kdj_container_layout'):
                            self.kdj_container_layout.removeWidget(self.kdj_label_container)
                        self.kdj_label_container.deleteLater()
                        delattr(self, 'kdj_label_container')
                    except Exception as e:
                        logger.warning(f"移除旧KDJ标签栏容器时发生错误: {e}")
                
                # 检查是否已经存在指标数值标签，如果存在则移除
                if hasattr(self, 'kdj_values_label'):
                    try:
                        self.kdj_values_label.deleteLater()
                        delattr(self, 'kdj_values_label')
                    except Exception as e:
                        logger.warning(f"移除旧KDJ数值标签时发生错误: {e}")
                
                # 创建标签栏容器
                self.kdj_label_container = QWidget()
                # 设置标签栏容器背景色与第1窗口一致
                self.kdj_label_container.setStyleSheet("background-color: #222222;")
                self.kdj_label_layout = QHBoxLayout(self.kdj_label_container)
                self.kdj_label_layout.setSpacing(0)
                self.kdj_label_layout.setContentsMargins(0, 0, 0, 0)
                
                # 添加功能菜单按钮到标签栏最左端
                self.kdj_menu_btn = self.create_indicator_menu_button(3)
                self.kdj_label_layout.addWidget(self.kdj_menu_btn)
                
                # 创建KDJ值显示标签
                self.kdj_values_label = QLabel()
                # 设置标签样式与第1窗口一致
                self.kdj_values_label.setStyleSheet("font-family: Consolas, monospace; padding: 5px; color: #C0C0C0; background-color: transparent; border: none;")
                # 确保不换行
                self.kdj_values_label.setWordWrap(False)
                
                # 根据当前指标更新标签文本
                if current_indicator == "KDJ":
                    # 获取最新的KDJ值
                    if 'k' in df_pl.columns and 'd' in df_pl.columns and 'j' in df_pl.columns:
                        latest_k = df_pl['k'].tail(1)[0]
                        latest_d = df_pl['d'].tail(1)[0]
                        latest_j = df_pl['j'].tail(1)[0]
                        # 更新标签文本
                        kdj_text = f"<font color='white'>K: {latest_k:.2f}</font>  <font color='yellow'>D: {latest_d:.2f}</font>  <font color='magenta'>J: {latest_j:.2f}</font>"
                    else:
                        kdj_text = f"<font color='white'>KDJ指标数据不可用</font>"
                    self.kdj_values_label.setText(kdj_text)
                elif current_indicator == "RSI":
                    # 获取最新的RSI值
                    if 'rsi14' in df_pl.columns:
                        latest_rsi = df_pl['rsi14'].tail(1)[0]
                        # 更新标签文本
                        rsi_text = f"<font color='blue'>RSI14: {latest_rsi:.2f}</font>"
                    else:
                        rsi_text = f"<font color='white'>RSI指标数据不可用</font>"
                    self.kdj_values_label.setText(rsi_text)
                elif current_indicator == "MACD":
                    # 获取最新的MACD值
                    if 'macd' in df_pl.columns and 'macd_signal' in df_pl.columns and 'macd_hist' in df_pl.columns:
                        latest_macd = df_pl['macd'].tail(1)[0]
                        latest_macd_signal = df_pl['macd_signal'].tail(1)[0]
                        latest_macd_hist = df_pl['macd_hist'].tail(1)[0]
                        # 更新标签文本，使用通达信风格：DIF白色，DEA黄色，MACD根据正负值变色
                        macd_hist_color = '#FF0000' if latest_macd_hist >= 0 else '#00FF00'
                        macd_text = f"<font color='white'>MACD(12,26,9) </font><font color='#FFFFFF'>DIF: {latest_macd:.2f}</font> <font color='#FFFF00'>DEA: {latest_macd_signal:.2f}</font> <font color='{macd_hist_color}'>MACD: {latest_macd_hist:.2f}</font>"
                    else:
                        macd_text = f"<font color='white'>MACD指标数据不可用</font>"
                    self.kdj_values_label.setText(macd_text)
                elif current_indicator == "VOL":
                    # 获取最新的成交量值
                    if 'volume' in df_pl.columns and 'vol_ma5' in df_pl.columns and 'vol_ma10' in df_pl.columns:
                        latest_volume = df_pl['volume'].tail(1)[0]
                        latest_vol_ma5 = df_pl['vol_ma5'].tail(1)[0]
                        latest_vol_ma10 = df_pl['vol_ma10'].tail(1)[0]
                        # 更新标签文本
                        vol_text = f"<font color='#C0C0C0'>VOLUME: {int(latest_volume):,}</font>  <font color='white'>MA5: {int(latest_vol_ma5):,}</font>  <font color='cyan'>MA10: {int(latest_vol_ma10):,}</font>"
                    else:
                        vol_text = f"<font color='white'>成交量数据不可用</font>"
                    self.kdj_values_label.setText(vol_text)
                elif current_indicator == "WR":
                    # 获取最新的WR值（通达信风格：WR1和WR2）
                    if 'wr1' in df_pl.columns and 'wr2' in df_pl.columns:
                        # 通达信风格，显示指标名称、参数和WR1/WR2，颜色与图中指标一致（WR1黄色，WR2白色）
                        latest_wr1 = df_pl['wr1'].tail(1)[0]
                        latest_wr2 = df_pl['wr2'].tail(1)[0]
                        # 通达信默认参数为WR(10,6)
                        wr_text = f"<font color='white'>WR(10,6) </font><font color='yellow'>WR1: {latest_wr1:.2f}</font> <font color='white'>WR2: {latest_wr2:.2f}</font>"
                    elif 'wr' in df_pl.columns:
                        # 兼容旧格式
                        latest_wr = df_pl['wr'].tail(1)[0]
                        wr_text = f"<font color='white'>WR: {latest_wr:.2f}</font>"
                    else:
                        wr_text = ""
                    self.kdj_values_label.setText(wr_text)
                elif current_indicator == "BOLL":
                    # 获取最新的BOLL值
                    if 'mb' in df_pl.columns and 'up' in df_pl.columns and 'dn' in df_pl.columns:
                        latest_mb = df_pl['mb'].tail(1)[0]
                        latest_up = df_pl['up'].tail(1)[0]
                        latest_dn = df_pl['dn'].tail(1)[0]
                        boll_text = f"<font color='white'>MB: {latest_mb:.2f}</font>  <font color='red'>UP: {latest_up:.2f}</font>  <font color='#00FF00'>DN: {latest_dn:.2f}</font>"
                    else:
                        boll_text = ""
                    self.kdj_values_label.setText(boll_text)
                elif current_indicator == "VR":
                    if 'vr' in df_pl.columns:
                        latest_vr = df_pl['vr'].tail(1)[0]
                        # 检查mavr列是否存在，如果不存在则计算
                        if 'mavr' in df_pl.columns:
                            latest_mavr = df_pl['mavr'].tail(1)[0]
                            # 更新标签文本，使用通达信风格：VR: xxx MAVR: xxx
                            vr_text = f"<font color='#FFFFFF'>VR: {latest_vr:.2f}</font>  <font color='#FFFF00'>MAVR: {latest_mavr:.2f}</font>"
                        else:
                            # 如果mavr列不存在，只显示VR值
                            vr_text = f"<font color='#FFFFFF'>VR: {latest_vr:.2f}</font>"
                    else:
                        vr_text = ""
                    self.kdj_values_label.setText(vr_text)
                else:
                    # 默认情况下不显示数值，避免错误
                    self.kdj_values_label.setText("")
                
                # 检查是否已经创建了KDJ容器和布局
                if hasattr(self, 'kdj_container') and hasattr(self, 'kdj_container_layout'):
                    # 首先移除可能存在的旧标签容器和标签，然后添加新标签
                    try:
                        self.kdj_container_layout.removeWidget(self.kdj_label_container)
                    except Exception:
                        pass
                    
                    try:
                        self.kdj_container_layout.removeWidget(self.kdj_values_label)
                    except Exception:
                        pass
                    
                    # 首先确保KDJ图已经在布局中，如果不在则添加
                    try:
                        self.kdj_container_layout.removeWidget(self.kdj_plot_widget)
                    except Exception:
                        pass
                    
                    # 将标签添加到水平布局中
                    self.kdj_label_layout.addWidget(self.kdj_values_label)
                    # 添加KDJ标签栏容器到容器布局顶部
                    self.kdj_container_layout.addWidget(self.kdj_label_container)
                    # 添加KDJ图到容器布局
                    self.kdj_container_layout.addWidget(self.kdj_plot_widget)
                    
                    # logger.info("已添加KDJ值显示标签")
                
                # 保存指标数据，用于鼠标移动时更新指标数值
                self.current_kdj_data = {
                    'k': df_pl['k'].to_list() if 'k' in df_pl.columns else [],
                    'd': df_pl['d'].to_list() if 'd' in df_pl.columns else [],
                    'j': df_pl['j'].to_list() if 'j' in df_pl.columns else []
                }
                self.current_rsi_data = {
                    'rsi': df_pl['rsi14'].to_list() if 'rsi14' in df_pl.columns else []
                }
                self.current_macd_data = {
                    'macd': df_pl['macd'].to_list() if 'macd' in df_pl.columns else [],
                    'macd_signal': df_pl['macd_signal'].to_list() if 'macd_signal' in df_pl.columns else [],
                    'macd_hist': df_pl['macd_hist'].to_list() if 'macd_hist' in df_pl.columns else []
                }
                # 保存WR指标数据
                self.current_wr_data = {
                    'wr1': df_pl['wr1'].to_list() if 'wr1' in df_pl.columns else [],
                    'wr2': df_pl['wr2'].to_list() if 'wr2' in df_pl.columns else [],
                    'wr': df_pl['wr'].to_list() if 'wr' in df_pl.columns else []
                }
                # 保存BOLL指标数据
                self.current_boll_data = {
                    'mb': df_pl['mb'].to_list() if 'mb' in df_pl.columns else [],
                    'up': df_pl['up'].to_list() if 'up' in df_pl.columns else [],
                    'dn': df_pl['dn'].to_list() if 'dn' in df_pl.columns else []
                }
                # 保存成交量数据
                self.current_volume_data = {
                    'volume': df_pl['volume'].to_list() if 'volume' in df_pl.columns else [],
                    'vol_ma5': df_pl['vol_ma5'].to_list() if 'vol_ma5' in df_pl.columns else [],
                    'vol_ma10': df_pl['vol_ma10'].to_list() if 'vol_ma10' in df_pl.columns else []
                }
                # 保存VR指标数据
                self.current_vr_data = {
                    'vr': df_pl['vr'].to_list() if 'vr' in df_pl.columns else [],
                    'mavr': df_pl['mavr'].to_list() if 'mavr' in df_pl.columns else []
                }

                # 保存DMI指标数据 (Needs check if exist)
                if 'pdi' in df_pl.columns:
                     self.current_dmi_data = {
                        'pdi': df_pl['pdi'].to_list() if 'pdi' in df_pl.columns else [],
                        'ndi': df_pl['ndi'].to_list() if 'ndi' in df_pl.columns else [],
                        'adx': df_pl['adx'].to_list() if 'adx' in df_pl.columns else [],
                        'adxr': df_pl['adxr'].to_list() if 'adxr' in df_pl.columns else []
                     }
                
                # 保存TRIX
                if 'trix' in df_pl.columns:
                    self.current_trix_data = {
                        'trix': df_pl['trix'].to_list() if 'trix' in df_pl.columns else [],
                        'trma': df_pl['trma'].to_list() if 'trma' in df_pl.columns else []
                    }
                
                # 保存BRAR
                if 'br' in df_pl.columns:
                    self.current_brar_data = {
                        'br': df_pl['br'].to_list() if 'br' in df_pl.columns else [],
                        'ar': df_pl['ar'].to_list() if 'ar' in df_pl.columns else []
                    }
                
                # 仅在第3窗口显示VOL指标时，应用与第2窗口相同的绘制逻辑
                if current_indicator == "VOL":
                    # 设置X轴刻度标签，与K线图保持一致
                    kdj_ax = self.kdj_plot_widget.getAxis('bottom')
                    tick_interval = ChartUtils.calculate_x_axis_tick_interval(len(dates))
                    kdj_ax.setTicks([[(i, pd.Timestamp(dates[i]).strftime('%Y-%m-%d')) for i in range(0, len(dates), tick_interval)]])
                    
                    # 设置X轴范围，与K线图一致
                    self.kdj_plot_widget.setXRange(0, len(dates) - 1)
                    
                    # 设置Y轴范围，与第2窗口的处理逻辑完全相同
                    # 确保使用真实的成交量数值，而不是相对比例
                    volumes = df_pl['volume'].to_numpy()
                    if len(volumes) > 0:
                        # 计算合理的Y轴范围，进一步增加顶部空间
                        volume_mean = volumes.mean()
                        volume_std = volumes.std()
                        if volume_std / volume_mean < 0.1:  # 标准差小于均值的10%，数据比较集中
                            # 扩大Y轴范围，显示更多细节，特别是顶部留出更多空间
                            y_min = max(0, volume_mean - volume_std * 2)
                            y_max = volume_mean + volume_std * 3.5  # 进一步增加顶部空间
                            self.kdj_plot_widget.setYRange(y_min, y_max)
                        else:
                            # 数据有一定差异，使用基于最小值和最大值的范围
                            y_range = volumes.max() - volumes.min()
                            y_min = max(0, volumes.min() - y_range * 0.1)
                            y_max = volumes.max() + y_range * 0.1  # 进一步增加顶部空间，从20%调整为30%
                            self.kdj_plot_widget.setYRange(y_min, y_max)
                    else:
                        # 成交量都是0，使用默认范围
                        self.kdj_plot_widget.setYRange(0, 100)
                    
                    # 禁用科学计数法，使用正常的数值显示
                    y_axis = self.kdj_plot_widget.getAxis('left')
                    y_axis.enableAutoSIPrefix(False)
                    y_axis.setStyle(tickTextOffset=20)
                    
                    # 重置缩放比例，确保显示真实数值
                    y_axis.setScale(1.0)
                    
                    # 确保X轴范围和刻度与K线图完全一致，实现柱体对齐，左边留出空间显示第一个标签
                    self.tech_plot_widget.setXRange(-1, len(dates) - 1)
                    self.kdj_plot_widget.setXRange(-1, len(dates) - 1)
                
            except Exception as e:
                logger.exception(f"计算或绘制技术指标时发生错误: {e}")
            
            # 保存当前鼠标位置和K线索引
            self.current_mouse_pos = None
            self.current_kline_index = -1
            # 绘制第二个窗口的指标图
            try:
                # 检查df_pl是否已经定义，避免UnboundLocalError
                if df_pl is None:
                    logger.warning("指标数据未计算，跳过第二个窗口指标绘制")
                    return
                    
                # 确保x变量已经定义
                if x is None:
                    x = np.arange(len(df_pl))
                    
                # 获取当前窗口指标
                current_indicator = self.window_indicators[2]
                
                # 绘制指标，并保存返回的包含计算后指标数据的df_pl
                self.draw_indicator(self.volume_plot_widget, current_indicator, x, df_pl)

                # 添加数值显示
                if len(df_pl) > 0:
                    if current_indicator == "VOL":
                        # 获取最新的成交量数据
                        latest_volume = df_pl.tail(1)['volume'][0]
                        
                        # 检查vol_ma5和vol_ma10列是否存在
                        latest_vol_ma5 = df_pl.tail(1)['vol_ma5'][0] if 'vol_ma5' in df_pl.columns else 0
                        latest_vol_ma10 = df_pl.tail(1)['vol_ma10'][0] if 'vol_ma10' in df_pl.columns else 0
                        
                        # 保存成交量数据和成交量均线数据，用于鼠标移动时更新标题
                        self.current_volume_data = {
                            'volume': df_pl['volume'].to_list(),
                            'vol_ma5': df_pl['vol_ma5'].to_list() if 'vol_ma5' in df_pl.columns else [],
                            'vol_ma10': df_pl['vol_ma10'].to_list() if 'vol_ma10' in df_pl.columns else []
                        }
                    
                    # 检查是否已经存在标签，如果存在则移除
                    if hasattr(self, 'volume_values_label'):
                        try:
                            self.volume_values_label.deleteLater()
                        except Exception as e:
                            logger.warning(f"移除旧标签时发生错误: {e}")
                    
                    # 检查是否已经存在标签栏容器，如果存在则先移除
                    if hasattr(self, 'volume_label_container'):
                        try:
                            # 移除旧的标签栏容器
                            if hasattr(self, 'volume_container') and hasattr(self, 'volume_container_layout'):
                                self.volume_container_layout.removeWidget(self.volume_label_container)
                            self.volume_label_container.deleteLater()
                            delattr(self, 'volume_label_container')
                        except Exception as e:
                            logger.warning(f"移除旧成交量标签栏容器时发生错误: {e}")
                    
                    # 检查是否已经存在指标数值标签，如果存在则移除
                    if hasattr(self, 'volume_values_label'):
                        try:
                            self.volume_values_label.deleteLater()
                            delattr(self, 'volume_values_label')
                        except Exception as e:
                            logger.warning(f"移除旧成交量数值标签时发生错误: {e}")
                    
                    # 创建标签栏容器
                    self.volume_label_container = QWidget()
                    # 设置标签栏容器背景色与第1窗口一致
                    self.volume_label_container.setStyleSheet("background-color: #222222;")
                    self.volume_label_layout = QHBoxLayout(self.volume_label_container)
                    self.volume_label_layout.setSpacing(0)
                    self.volume_label_layout.setContentsMargins(0, 0, 0, 0)
                    
                    # 添加功能菜单按钮到标签栏最左端
                    self.volume_menu_btn = self.create_indicator_menu_button(2)
                    self.volume_label_layout.addWidget(self.volume_menu_btn)
                    
                    # 创建标签，使用与K线图均线标签相同的样式
                    self.volume_values_label = QLabel()
                    # 设置标签样式与第1窗口一致
                    self.volume_values_label.setStyleSheet("font-family: Consolas, monospace; padding: 5px; color: #C0C0C0; background-color: transparent; border: none;")
                    # 确保不换行
                    self.volume_values_label.setWordWrap(False)
                    
                    # 根据不同指标设置标签文本
                    if current_indicator == "VOL":
                        # 使用HTML设置初始文本和颜色，与K线图均线标签样式一致
                        # 确保vol_ma5和vol_ma10列存在
                        if 'vol_ma5' not in df_pl.columns or 'vol_ma10' not in df_pl.columns:
                            # 计算成交量均线
                            analyzer = TechnicalAnalyzer(df_pl)
                            analyzer.calculate_vol_ma([5, 10])
                            df_pl = analyzer.get_data(return_polars=True)
                            
                            # 更新最新值
                            latest_volume = df_pl.tail(1)['volume'][0]
                            latest_vol_ma5 = df_pl.tail(1)['vol_ma5'][0]
                            latest_vol_ma10 = df_pl.tail(1)['vol_ma10'][0]
                        
                        self.volume_values_label.setText(f"<font color='#C0C0C0'>VOLUME: {int(latest_volume):,}</font>  <font color='white'>MA5: {int(latest_vol_ma5):,}</font>  <font color='cyan'>MA10: {int(latest_vol_ma10):,}</font>")
                    elif current_indicator == "MACD":
                        # 添加MACD数值显示
                        latest_macd = df_pl['macd'].tail(1)[0]
                        latest_macd_signal = df_pl['macd_signal'].tail(1)[0]
                        latest_macd_hist = df_pl['macd_hist'].tail(1)[0]
                        
                        self.volume_values_label.setText(f"<font color='#FFFFFF'>DIF: {latest_macd:.2f}</font>  <font color='#FFFF00'>DEA: {latest_macd_signal:.2f}</font>  <font color='#C0C0C0'>MACD: {latest_macd_hist:.2f}</font>")
                    elif current_indicator == "RSI":
                        # 添加RSI数值显示
                        latest_rsi = df_pl['rsi14'].tail(1)[0]
                        
                        self.volume_values_label.setText(f"<font color='blue'>RSI14: {latest_rsi:.2f}</font>")
                    elif current_indicator == "KDJ":
                        # 添加KDJ数值显示
                        latest_k = df_pl['k'].tail(1)[0]
                        latest_d = df_pl['d'].tail(1)[0]
                        latest_j = df_pl['j'].tail(1)[0]
                        
                        self.volume_values_label.setText(f"<font color='white'>K: {latest_k:.2f}</font>  <font color='yellow'>D: {latest_d:.2f}</font>  <font color='magenta'>J: {latest_j:.2f}</font>")
                    
                    # 检查是否已经创建了成交量容器和布局
                    if hasattr(self, 'volume_container') and hasattr(self, 'volume_container_layout'):
                        # 首先移除可能存在的旧标签容器和标签，然后添加新标签
                        try:
                            self.volume_container_layout.removeWidget(self.volume_label_container)
                        except Exception:
                            pass
                        
                        try:
                            self.volume_container_layout.removeWidget(self.volume_values_label)
                        except Exception:
                            pass
                        
                        # 将成交量图添加到容器布局中
                        # 首先移除可能存在的旧成交量图，然后添加新的成交量图
                        try:
                            self.volume_container_layout.removeWidget(self.volume_plot_widget)
                        except Exception:
                            pass
                        
                        # 将标签添加到水平布局中
                        self.volume_label_layout.addWidget(self.volume_values_label)
                        # 在成交量图上方添加成交量标签容器
                        self.volume_container_layout.addWidget(self.volume_label_container)
                        self.volume_container_layout.addWidget(self.volume_plot_widget)
                    
                    # 检查窗口数量，如果是1个窗口模式，隐藏成交量标签
                    if hasattr(self, 'current_window_count') and self.current_window_count == 1:
                        self.volume_values_label.hide()
                        self.volume_plot_widget.hide()
                    else:
                        self.volume_values_label.show()
                        self.volume_plot_widget.show()
                        
                        # logger.info("已添加成交量值显示标签")
                
                # 设置成交量图的x轴与K线图一致，实现柱体对齐，左边留出空间显示第一个标签
                self.volume_plot_widget.setXRange(-1, len(df_pl) - 1)
                
                # 设置成交量图的X轴刻度标签，与K线图保持一致
                volume_ax = self.volume_plot_widget.getAxis('bottom')
                tick_interval = ChartUtils.calculate_x_axis_tick_interval(len(dates))
                volume_ax.setTicks([[(i, pd.Timestamp(dates[i]).strftime('%Y-%m-%d')) for i in range(0, len(dates), tick_interval)]])
                
                # 确保两个图的X轴范围和刻度完全一致，实现柱体对齐，左边留出空间显示第一个标签
                self.tech_plot_widget.setXRange(-1, len(dates) - 1)
                self.volume_plot_widget.setXRange(-1, len(dates) - 1)
                
                # 仅在当前指标是VOL时设置成交量相关的Y轴范围和样式
                if current_indicator == "VOL":
                    # 获取成交量数据
                    volume_data = df_pl['volume'].to_numpy()
                    volume_min = volume_data.min()
                    volume_max = volume_data.max()
                    
                    # 重置对数模式，默认使用线性刻度
                    self.volume_plot_widget.setLogMode(y=False)
                    
                    # 计算成交量的统计信息
                    volume_mean = volume_data.mean()
                    volume_std = volume_data.std()
                    
                    # 计算合理的Y轴范围，进一步增加顶部空间
                    if volume_max > 0:
                        # 如果数据差异不大，使用基于均值和标准差的范围
                        if volume_std / volume_mean < 0.1:  # 标准差小于均值的10%，数据比较集中
                            # 扩大Y轴范围，显示更多细节，特别是顶部留出更多空间
                            y_min = max(0, volume_mean - volume_std * 2)
                            y_max = volume_mean + volume_std * 3.5  # 进一步增加顶部空间
                            self.volume_plot_widget.setYRange(y_min, y_max)
                        else:
                            # 数据有一定差异，使用基于最小值和最大值的范围
                            y_range = volume_max - volume_min
                            y_min = max(0, volume_min - y_range * 0.1)
                            y_max = volume_max + y_range * 0.1  # 进一步增加顶部空间，从20%调整为30%
                            self.volume_plot_widget.setYRange(y_min, y_max)
                    else:
                        # 成交量都是0，使用默认范围
                        self.volume_plot_widget.setYRange(0, 100)
                
                # 禁用科学计数法，使用正常的数值显示
                y_axis = self.volume_plot_widget.getAxis('left')
                y_axis.enableAutoSIPrefix(False)
                y_axis.setStyle(tickTextOffset=20)
                
                # 设置X轴范围
                self.volume_plot_widget.setXRange(-1, len(df_pl) - 1)
                
                # logger.info(f"成功绘制{stock_name}({stock_code})的{current_indicator}图")
                
            except Exception as e:
                logger.exception(f"绘制{current_indicator}图失败: {e}")
            
            # 保存当前显示的个股信息
            self.current_stock_data = df
            self.current_stock_name = stock_name
            self.current_stock_code = stock_code
            
            # 移除旧的十字线对象，避免多个十字线对象导致的问题
            try:
                # 移除K线图中的旧十字线
                if hasattr(self, 'vline') and self.vline is not None:
                    self.tech_plot_widget.removeItem(self.vline)
                if hasattr(self, 'hline') and self.hline is not None:
                    self.tech_plot_widget.removeItem(self.hline)
                # 移除成交量图中的旧十字线
                if hasattr(self, 'volume_vline') and self.volume_vline is not None:
                    self.volume_plot_widget.removeItem(self.volume_vline)
                if hasattr(self, 'volume_hline') and self.volume_hline is not None:
                    self.volume_plot_widget.removeItem(self.volume_hline)
                # 移除KDJ图中的旧十字线
                if hasattr(self, 'kdj_vline') and self.kdj_vline is not None:
                    self.kdj_plot_widget.removeItem(self.kdj_vline)
                if hasattr(self, 'kdj_hline') and self.kdj_hline is not None:
                    self.kdj_plot_widget.removeItem(self.kdj_hline)
            except Exception as e:
                logger.debug(f"移除旧的十字线对象时发生错误: {e}")
            
            # 添加十字线
            self.vline = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen('w', width=1, style=Qt.DotLine))
            self.hline = pg.InfiniteLine(angle=0, movable=False, pen=pg.mkPen('w', width=1, style=Qt.DotLine))
            self.volume_vline = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen('w', width=1, style=Qt.DotLine))
            self.volume_hline = pg.InfiniteLine(angle=0, movable=False, pen=pg.mkPen('w', width=1, style=Qt.DotLine))
            # 为KDJ图添加十字线
            self.kdj_vline = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen('w', width=1, style=Qt.DotLine))
            self.kdj_hline = pg.InfiniteLine(angle=0, movable=False, pen=pg.mkPen('w', width=1, style=Qt.DotLine))
            
            # 添加十字线到K线图
            self.tech_plot_widget.addItem(self.vline, ignoreBounds=True)
            self.tech_plot_widget.addItem(self.hline, ignoreBounds=True)
            
            # 添加十字线到成交量图
            self.volume_plot_widget.addItem(self.volume_vline, ignoreBounds=True)
            self.volume_plot_widget.addItem(self.volume_hline, ignoreBounds=True)
            
            # 添加十字线到KDJ图
            self.kdj_plot_widget.addItem(self.kdj_vline, ignoreBounds=True)
            self.kdj_plot_widget.addItem(self.kdj_hline, ignoreBounds=True)
            
            # 初始隐藏十字线
            self.vline.hide()
            self.hline.hide()
            self.volume_vline.hide()
            self.volume_hline.hide()
            # 初始隐藏KDJ图十字线
            self.kdj_vline.hide()
            self.kdj_hline.hide()
            
            # 创建信息文本项
            self.info_text = pg.TextItem(anchor=(0, 1))  # 锚点在左下角，确保信息框左下角在指定位置
            self.info_text.setColor(pg.mkColor('w'))
            self.info_text.setHtml('<div style="background-color: rgba(0, 0, 0, 0.8); padding: 5px; border: 1px solid #666; font-family: monospace;"></div>')
            self.tech_plot_widget.addItem(self.info_text)
            self.info_text.hide()
            
            # 保存当前K线数据，用于双击显示信息
            self.current_kline_data = {
                'dates': dates,
                'opens': opens,
                'highs': highs,
                'lows': lows,
                'closes': closes,
                'ohlc_list': ohlc_list
            }
            
            # 断开之前的所有事件连接，避免多次连接导致的问题
            try:
                # 断开鼠标移动事件的所有连接
                if hasattr(self.tech_plot_widget.scene().sigMouseMoved, 'disconnect'):
                    self.tech_plot_widget.scene().sigMouseMoved.disconnect()
            except Exception:
                pass
            
            try:
                if hasattr(self.volume_plot_widget.scene().sigMouseMoved, 'disconnect'):
                    self.volume_plot_widget.scene().sigMouseMoved.disconnect()
            except Exception:
                pass
            
            try:
                if hasattr(self.kdj_plot_widget.scene().sigMouseMoved, 'disconnect'):
                    self.kdj_plot_widget.scene().sigMouseMoved.disconnect()
            except Exception as e:
                logger.debug(f"断开鼠标移动事件连接时发生错误: {e}")
            
            try:
                # 断开鼠标点击事件的所有连接
                if hasattr(self.tech_plot_widget.scene().sigMouseClicked, 'disconnect'):
                    self.tech_plot_widget.scene().sigMouseClicked.disconnect()
            except Exception as e:
                logger.debug(f"断开鼠标点击事件连接时发生错误: {e}")
            
            # 连接鼠标移动事件，实现十字线跟随和指标值更新
            # 使用事件处理器管理器处理鼠标移动事件
            mouse_handler = self.event_handler_manager.get_mouse_handler()
            self.tech_plot_widget.scene().sigMouseMoved.connect(lambda pos: mouse_handler.handle_mouse_moved(pos, dates, opens, highs, lows, closes))
            self.volume_plot_widget.scene().sigMouseMoved.connect(lambda pos: mouse_handler.handle_mouse_moved(pos, dates, opens, highs, lows, closes))
            self.kdj_plot_widget.scene().sigMouseMoved.connect(lambda pos: mouse_handler.handle_mouse_moved(pos, dates, opens, highs, lows, closes))
            
            # 连接鼠标点击事件，处理左键和右键点击
            mouse_handler = self.event_handler_manager.get_mouse_handler()
            self.tech_plot_widget.scene().sigMouseClicked.connect(lambda event: mouse_handler.handle_mouse_clicked(event, dates, opens, highs, lows, closes))
            # 连接成交量图鼠标点击事件
            self.volume_plot_widget.scene().sigMouseClicked.connect(lambda event: mouse_handler.handle_mouse_clicked(event, dates, opens, highs, lows, closes))
            # 连接KDJ图鼠标点击事件
            self.kdj_plot_widget.scene().sigMouseClicked.connect(lambda event: mouse_handler.handle_mouse_clicked(event, dates, opens, highs, lows, closes))
            
            # 连接鼠标离开视图事件，通过监控鼠标位置实现
            self.tech_plot_widget.viewport().setMouseTracking(True)
            
            # 使用当前的窗口数量，不强制重置为3个
            self.indicator_interaction_manager.on_window_count_changed(self.current_window_count, True)
            
        except Exception as e:
            logger.exception(f"绘制K线图失败: {e}")
            self.statusBar().showMessage(f"绘制K线图失败: {str(e)[:50]}...", 5000)

    def on_custom_context_menu(self, pos):
        """
        处理customContextMenuRequested信号，显示自定义右键菜单
        """
        # logger.info(f"customContextMenuRequested信号被调用，位置: {pos}")
        
        # 创建自定义菜单
        menu = QMenu(self.tech_plot_widget)
        
        # 如果有选中的均线，添加修改指标参数选项
        if hasattr(self, 'selected_ma') and self.selected_ma:
            modify_action = QAction(f"修改{self.selected_ma}指标参数", self)
            modify_action.triggered.connect(lambda: self.on_modify_indicator(self.selected_ma))
            menu.addAction(modify_action)
        else:
            # 如果没有选中均线，添加提示信息
            no_select_action = QAction("未选中均线，请先点击选中均线", self)
            no_select_action.setEnabled(False)  # 禁用选项
            menu.addAction(no_select_action)
        
        # 转换为全局坐标
        global_pos = self.tech_plot_widget.mapToGlobal(pos)
        
        # 显示菜单
        menu.exec(global_pos)
    
    def on_modify_indicator(self, ma_name):
        """
        处理修改指标参数的菜单动作
        """
        logger.info(f"修改指标参数: {ma_name}")
        
        # 创建修改指标参数的对话框
        dialog = QDialog(self)
        dialog.setWindowTitle(f"修改{ma_name}指标参数")
        dialog.setGeometry(300, 300, 300, 200)
        
        # 创建布局
        layout = QVBoxLayout(dialog)
        
        # 获取当前的窗口参数
        current_window = int(ma_name.replace("MA", ""))
        
        # 创建标签和输入框
        window_label = QLabel("周期:", dialog)
        layout.addWidget(window_label)
        
        window_input = QLineEdit(dialog)
        window_input.setText(str(current_window))
        layout.addWidget(window_input)
        
        # 创建按钮布局
        button_layout = QHBoxLayout()
        
        ok_button = QPushButton("确定", dialog)
        cancel_button = QPushButton("取消", dialog)
        
        button_layout.addWidget(ok_button)
        button_layout.addWidget(cancel_button)
        
        layout.addLayout(button_layout)
        
        # 连接按钮信号
        def on_ok():
            try:
                # 获取新的窗口参数
                new_window = int(window_input.text())
                if new_window <= 0:
                    raise ValueError("周期必须大于0")
                logger.info(f"修改{ma_name}周期为: {new_window}")
                dialog.accept()
            except ValueError as e:
                logger.error(f"周期输入错误: {e}")
        
        ok_button.clicked.connect(on_ok)
        cancel_button.clicked.connect(dialog.reject)
        
        # 显示对话框
        dialog.exec()
    
    def on_ma_clicked(self, event):
        """
        处理均线点击事件，在选中的均线上显示白点标注
        
        Args:
            event: 鼠标点击事件
        """
        try:
            # 获取点击位置
            pos = event.scenePos()
            view_box = self.tech_plot_widget.getViewBox()
            view_pos = view_box.mapSceneToView(pos)
            # x_val = view_pos.x()
            y_val = view_pos.y()
            
            # 找到最接近的K线索引
            index = int(round(view_pos.x()))
            
            # 检测点击位置是否在某个均线上
            clicked_ma = None
            min_distance = float('inf')
            
            # 定义点击容忍度（Y轴方向的容忍度）
            tolerance = 0.02  # 2%的价格容忍度
            
            # 获取当前价格范围，用于计算相对容忍度
            y_range = self.tech_plot_widget.viewRange()[1]
            y_min, y_max = y_range
            price_tolerance = (y_max - y_min) * tolerance
            
            # 确保moving_averages属性存在
            if hasattr(self, 'moving_averages'):
                # 遍历所有均线，检查点击位置是否在均线上
                for ma_name, ma_info in self.moving_averages.items():
                    x_data, y_data = ma_info['data']
                    if 0 <= index < len(x_data):
                        # 获取该位置的均线值
                        ma_value = y_data[index]
                        
                        # 计算点击位置与均线的距离
                        distance = abs(y_val - ma_value)
                        
                        # 如果距离小于容忍度，认为点击了该均线
                        if distance < price_tolerance and distance < min_distance:
                            min_distance = distance
                            clicked_ma = ma_name
            
            # 如果点击了均线
            if clicked_ma:
                # logger.info(f"点击了{clicked_ma}")
                
                # 确保ma_points属性存在
                if not hasattr(self, 'ma_points'):
                    self.ma_points = []
                
                # 清除之前的标注点
                for point_item in self.ma_points:
                    self.tech_plot_widget.removeItem(point_item)
                self.ma_points.clear()
                
                # 确保moving_averages属性存在
                if hasattr(self, 'moving_averages'):
                    # 绘制新的标注点
                    ma_info = self.moving_averages.get(clicked_ma)
                    if ma_info:
                        x_data, y_data = ma_info['data']
                        
                        # 在均线上每隔几个点绘制一个白点
                        step = max(1, len(x_data) // 20)  # 最多绘制20个点
                        for i in range(0, len(x_data), step):
                            if y_data[i] is not None and not (isinstance(y_data[i], (int, float)) and math.isnan(y_data[i])):
                                # 创建白点标注
                                point = pg.ScatterPlotItem([x_data[i]], [y_data[i]], size=6, pen=pg.mkPen('w', width=1), brush=pg.mkBrush('w'))
                                self.tech_plot_widget.addItem(point)
                                self.ma_points.append(point)
                
                # 更新选中的均线
                self.selected_ma = clicked_ma
            else:
                # 点击位置不在均线上，取消选中状态
                
                # 确保ma_points属性存在
                if hasattr(self, 'ma_points'):
                    # 清除之前的标注点
                    for point_item in self.ma_points:
                        self.tech_plot_widget.removeItem(point_item)
                    self.ma_points.clear()
                
                # 重置选中状态
                self.selected_ma = None
                
                # 检查是否是右键点击
                if event.button() == Qt.RightButton:
                    # 创建右键菜单
                    menu = QMenu(self)
                    
                    # 如果点击了均线，添加修改指标参数选项
                    if clicked_ma:
                        modify_action = QAction(f"修改{clicked_ma}指标参数", self)
                        modify_action.triggered.connect(lambda: self.on_modify_indicator(clicked_ma))
                        menu.addAction(modify_action)
                    else:
                        # 如果没有点击在均线上，添加提示信息
                        no_select_action = QAction("未选中均线，请先点击选中均线", self)
                        no_select_action.setEnabled(False)  # 禁用选项
                        menu.addAction(no_select_action)
                    
                    # 使用event的pos方法获取场景位置，然后转换为屏幕位置
                    scene_pos = event.pos()
                    screen_pos = self.tech_plot_widget.mapToGlobal(scene_pos)
                    qpoint = screen_pos.toPoint()
                    menu.exec(qpoint)
                    
                    # 阻止事件传播，防止显示默认菜单
                    event.accept()
        except Exception as e:
            logger.exception(f"处理均线点击事件时发生错误: {e}")

    def update_ma_values_display(self, index, dates, opens, highs, lows, closes):
        """
        更新顶部均线值显示
        """
        try:
            if not hasattr(self, 'ma_values_label'):
                return
            
            # 确保索引有效
            if index < 0 or index >= len(dates):
                return
            
            # 获取当前日期
            current_date = pd.Timestamp(dates[index]).strftime('%Y-%m-%d')
            
            # 获取当前的MA值
            ma_values = {}
            
            # 检查是否有保存的MA数据
            if hasattr(self, 'ma_data'):
                # 使用保存的MA值，确保与绘制的MA线一致
                if 0 <= index < len(self.ma_data['MA5']):
                    ma5 = self.ma_data['MA5'][index]
                    if ma5 != '' and ma5 is not None and str(ma5) != 'nan':
                        ma_values['MA5'] = f"{ma5:.2f}"
                    else:
                        ma_values['MA5'] = "--"
                else:
                    ma_values['MA5'] = "--"
                
                if 0 <= index < len(self.ma_data['MA10']):
                    ma10 = self.ma_data['MA10'][index]
                    if ma10 != '' and ma10 is not None and str(ma10) != 'nan':
                        ma_values['MA10'] = f"{ma10:.2f}"
                    else:
                        ma_values['MA10'] = "--"
                else:
                    ma_values['MA10'] = "--"
                
                if 0 <= index < len(self.ma_data['MA20']):
                    ma20 = self.ma_data['MA20'][index]
                    if ma20 != '' and ma20 is not None and str(ma20) != 'nan':
                        ma_values['MA20'] = f"{ma20:.2f}"
                    else:
                        ma_values['MA20'] = "--"
                else:
                    ma_values['MA20'] = "--"
                
                if 0 <= index < len(self.ma_data['MA60']):
                    ma60 = self.ma_data['MA60'][index]
                    if ma60 != '' and ma60 is not None and str(ma60) != 'nan':
                        ma_values['MA60'] = f"{ma60:.2f}"
                    else:
                        ma_values['MA60'] = "--"
                else:
                    ma_values['MA60'] = "--"
            else:
                # 如果没有保存的MA数据，使用默认值
                ma_values['MA5'] = "--"
                ma_values['MA10'] = "--"
                ma_values['MA20'] = "--"
                ma_values['MA60'] = "--"
            
            # 获取MA线的颜色，默认使用当前设置的颜色
            if hasattr(self, 'ma_colors'):
                ma5_color = self.ma_colors.get('MA5', 'white')
                ma10_color = self.ma_colors.get('MA10', 'cyan')
                ma20_color = self.ma_colors.get('MA20', 'red')
                ma60_color = self.ma_colors.get('MA60', '#00FF00')
            else:
                # 默认颜色设置
                ma5_color = 'white'
                ma10_color = 'cyan'
                ma20_color = 'red'
                ma60_color = '#00FF00'
            
            # 更新标签文本，使用HTML格式设置不同颜色，添加日期显示
            ma_text = f"<font color='#C0C0C0'>日期: {current_date}</font>  <font color='{ma5_color}'>MA5: {ma_values['MA5']}</font>  <font color='{ma10_color}'>MA10: {ma_values['MA10']}</font>  <font color='{ma20_color}'>MA20: {ma_values['MA20']}</font>  <font color='{ma60_color}'>MA60: {ma_values['MA60']}</font>"
            self.ma_values_label.setText(ma_text)
        except Exception as e:
            logger.exception(f"更新MA值显示时发生错误: {e}")

    def show_info_box(self):
        """显示信息框"""
        try:
            if self.current_kline_index >= 0 and self.current_kline_data:
                dates = self.current_kline_data['dates']
                opens = self.current_kline_data['opens']
                highs = self.current_kline_data['highs']
                lows = self.current_kline_data['lows']
                closes = self.current_kline_data['closes']
                index = self.current_kline_index
                
                # 确保索引在有效范围内
                if 0 <= index < len(dates):
                    # 计算前一天的收盘价，用于计算涨跌幅
                    pre_close = closes[index-1] if index > 0 else closes[index]
                    
                    # 计算涨跌幅和涨跌额
                    change = closes[index] - pre_close
                    pct_change = (change / pre_close) * 100 if pre_close != 0 else 0
                    
                    # 获取星期几，0=周一，1=周二，2=周三，3=周四，4=周五，5=周六，6=周日
                    weekday = dates[index].weekday()
                    # 转换为中文星期
                    weekday_map = {0: '一', 1: '二', 2: '三', 3: '四', 4: '五', 5: '六', 6: '日'}
                    weekday_str = weekday_map.get(weekday, '')
                    
                    # 生成信息文本
                    info_html = f"""
                    <div style="background-color: rgba(0, 0, 0, 0.8); padding: 8px; border: 1px solid #666; color: white; font-family: monospace;">
                    <div style="font-weight: bold;">{pd.Timestamp(dates[index]).strftime('%Y-%m-%d')}/{weekday_str}</div>
                    <div>开盘: {opens[index]:.2f}</div>
                    <div>最高: {highs[index]:.2f}</div>
                    <div>最低: {lows[index]:.2f}</div>
                    <div>收盘: {closes[index]:.2f}</div>
                    <div>涨跌: {change:.2f}</div>
                    <div>涨幅: {pct_change:.2f}%</div>
                    </div>
                    """
                    
                    # 更新信息文本
                    if self.info_text is not None:
                        self.info_text.setHtml(info_html)
                        # 设置信息文本位置，跟随鼠标显示
                        if self.current_mouse_pos is not None:
                            # 将场景坐标转换为视图坐标
                            view_box = self.tech_plot_widget.getViewBox()
                            
                            # 获取当前视图范围
                            x_min, x_max = view_box.viewRange()[0]
                            y_min, y_max = view_box.viewRange()[1]
                            
                            # 使用K线位置作为信息框的基准位置
                            kline_x = self.current_kline_index
                            kline_y = lows[index]
                            
                            # 计算信息框的尺寸（基于视图范围的百分比）
                            view_height = y_max - y_min
                            view_width = x_max - x_min
                            
                            # 使用实际像素或视图坐标的百分比来确定信息框尺寸
                            info_box_height = view_height * 0.2  # 信息框高度为视图高度的20%
                            info_box_width = view_width * 0.25  # 信息框宽度为视图宽度的25%
                            margin = view_height * 0.02  # 边距为视图高度的2%
                            
                            # 计算K线在视图中的相对位置
                            kline_relative_y = (kline_y - y_min) / view_height
                            
                            # 检查K线是否靠近右侧边界，动态调整信息框显示方向
                            kline_offset = 0.1  # 约为半个K线宽度
                            
                            if kline_x > x_max - info_box_width - margin:
                                # K线在右侧区域，信息框显示在K线左侧
                                pos_x = kline_x - 9 + kline_offset
                            else:
                                # K线在左侧区域，信息框显示在K线右侧
                                pos_x = kline_x + kline_offset
                            
                            if kline_relative_y > 0.6:  # K线位于视图上60%
                                # 信息框显示在K线下方
                                pos_y = kline_y - info_box_height - margin - 10
                            else:  # K线位于视图下40%
                                # 信息框显示在K线上方
                                pos_y = kline_y + margin - 20
                            
                            # 垂直方向：确保信息框不会超出上下边界
                            if pos_y < y_min + margin:
                                pos_y = y_min + margin
                            elif pos_y + info_box_height + 10 > y_max - margin:
                                pos_y = y_max - info_box_height - margin - 10
                            
                            self.info_text.setPos(pos_x, pos_y)
                            self.info_text.show()
        except Exception as e:
            logger.exception(f"显示信息框失败: {e}")

    def draw_indicator(self, plot_widget, indicator_name, x, df_pl):
        """
        根据指标名称绘制相应的指标
        """
        return self.chart_manager.draw_indicator(plot_widget, indicator_name, x, df_pl)
        
    def keyPressEvent(self, event):
        """
        处理键盘事件，实现按ESC键从技术分析窗口返回行情窗口
        """
        # 检查是否按下了ESC键
        if event.key() == Qt.Key_Escape:
            # 检查当前是否在技术分析窗口
            if self.tab_widget.currentWidget() == self.tech_tab:
                # logger.info("按ESC键，从技术分析窗口返回行情窗口")
                # 切换到行情窗口
                self.tab_widget.setCurrentWidget(self.market_tab)
        
        # 调用父类方法处理其他键盘事件
        # 注意：这里我们假设Mixin是被多重继承的，并且有super()可以调用
        # 如果是Base Class，可能需要不同的处理
        try:
             super().keyPressEvent(event)
        except AttributeError:
             pass
    
    def _load_and_show_dividend_markers(self, stock_code, dates):
        """
        加载并显示分红配股标记
        
        Args:
            stock_code: 股票代码
            dates: K线日期列表
        """
        try:
            # 初始化分红标记管理器
            if not hasattr(self, 'dividend_marker_manager'):
                self.dividend_marker_manager = DividendMarkerManager(self.tech_plot_widget)
            else:
                # 清除旧的分红标记
                self.dividend_marker_manager.clear_markers()
            
            # 从数据管理器获取分红数据
            if hasattr(self, 'data_manager') and self.data_manager:
                # 获取日期范围
                if len(dates) > 0:
                    # 扩大日期范围，查询所有历史分红数据
                    start_date = pd.Timestamp(dates[0]).strftime('%Y-%m-%d')
                    end_date = pd.Timestamp(dates[-1]).strftime('%Y-%m-%d')
                    
                    logger.info(f"查询股票{stock_code}的分红数据，K线日期范围: {start_date} 至 {end_date}")
                    
                    # 查询分红数据（不限制日期范围，获取所有历史分红）
                    dividend_data = self._get_stock_dividend_data(stock_code)
                    
                    if dividend_data:
                        logger.info(f"获取到股票{stock_code}的{len(dividend_data)}条分红数据")
                        # 过滤出在K线日期范围内的分红数据
                        filtered_dividends = self._filter_dividends_by_dates(dividend_data, dates)
                        logger.info(f"过滤后在K线日期范围内的分红数据: {len(filtered_dividends)}条")
                        
                        if filtered_dividends:
                            # 设置分红数据并显示标记
                            self.dividend_marker_manager.set_dividend_data(filtered_dividends, dates)
                        else:
                            logger.debug(f"股票{stock_code}在K线日期范围内没有分红数据")
                    else:
                        logger.debug(f"股票{stock_code}没有分红数据")
            
        except Exception as e:
            logger.exception(f"加载分红标记失败: {e}")
    
    def _get_stock_dividend_data(self, stock_code):
        """
        获取股票所有历史分红数据
        
        Args:
            stock_code: 股票代码
        
        Returns:
            list: 分红数据列表
        """
        try:
            # 从数据库查询分红数据
            from src.database.models.stock import StockDividend
            
            # 处理股票代码格式
            # 支持多种格式：600000.SH, sh600000, 600000
            if '.' in stock_code:
                ts_code = stock_code
            elif stock_code.startswith(('sh', 'sz')):
                market = 'SH' if stock_code.startswith('sh') else 'SZ'
                code = stock_code[2:]
                ts_code = f"{code}.{market}"
            else:
                # 默认假设是数字代码，根据代码判断市场
                if stock_code.startswith('6'):
                    ts_code = f"{stock_code}.SH"
                else:
                    ts_code = f"{stock_code}.SZ"
            
            logger.info(f"查询股票{ts_code}的所有历史分红数据")
            
            # 查询数据库（获取所有历史分红数据）
            # 通过data_manager获取数据库会话
            if hasattr(self, 'data_manager') and self.data_manager:
                logger.info(f"正在获取数据库会话...")
                try:
                    # 尝试从data_manager获取数据库管理器
                    if hasattr(self.data_manager, 'db_manager'):
                        db_manager = self.data_manager.db_manager
                        session = db_manager.get_session()
                    elif hasattr(self.data_manager, 'get_session'):
                        session = self.data_manager.get_session()
                    else:
                        logger.warning("data_manager没有db_manager或get_session方法")
                        return []
                    
                    if session:
                        try:
                            logger.info(f"开始查询数据库，ts_code={ts_code}")
                            dividends = session.query(StockDividend).filter(
                                StockDividend.ts_code == ts_code
                            ).all()
                            
                            logger.info(f"从数据库查询到{len(dividends)}条分红数据")
                            
                            # 转换为字典列表
                            result = []
                            for div in dividends:
                                result.append({
                                    'ex_date': div.ex_date,
                                    'record_date': div.record_date,
                                    'pay_date': div.pay_date,
                                    'cash_div': div.cash_div,
                                    'share_div': div.share_div
                                })
                            
                            logger.info(f"转换后的分红数据数量: {len(result)}")
                            return result
                        except Exception as e:
                            logger.exception(f"查询分红数据时发生异常: {e}")
                            return []
                        finally:
                            session.close()
                    else:
                        logger.warning("无法获取数据库会话")
                except Exception as e:
                    logger.exception(f"获取数据库会话失败: {e}")
            else:
                logger.warning("data_manager不可用")
            
            return []
            
        except Exception as e:
            logger.exception(f"获取分红数据失败: {e}")
            return []
    
    def _filter_dividends_by_dates(self, dividend_data, kline_dates):
        """
        过滤出在K线日期范围内的分红数据
        
        Args:
            dividend_data: 所有分红数据
            kline_dates: K线日期列表
        
        Returns:
            list: 在K线日期范围内的分红数据
        """
        try:
            # 获取K线日期范围
            if len(kline_dates) == 0:
                return []
            
            # 转换K线日期为datetime.date
            import datetime
            kline_date_set = set()
            for d in kline_dates:
                if hasattr(d, 'date') and callable(getattr(d, 'date')):
                    kline_date_set.add(d.date())
                elif hasattr(d, 'strftime'):
                    # 如果是datetime对象，转换为date
                    kline_date_set.add(d.date() if hasattr(d, 'date') else d)
                elif isinstance(d, datetime.date):
                    kline_date_set.add(d)
                else:
                    # 处理numpy.datetime64类型
                    try:
                        # 转换为Python datetime，然后转换为date
                        if hasattr(d, 'astype'):
                            # numpy.datetime64
                            dt = d.astype('datetime64[D]').item()
                            if isinstance(dt, datetime.date):
                                kline_date_set.add(dt)
                            elif isinstance(dt, datetime.datetime):
                                kline_date_set.add(dt.date())
                        else:
                            kline_date_set.add(d)
                    except Exception as e:
                        logger.warning(f"无法转换日期: {d}, 类型: {type(d)}, 错误: {e}")
                        kline_date_set.add(d)
            
            logger.info(f"K线日期范围: {min(kline_date_set) if kline_date_set else 'None'} 至 {max(kline_date_set) if kline_date_set else 'None'}, 共{len(kline_date_set)}个日期")
            
            # 打印前几个K线日期用于调试
            sample_dates = list(kline_date_set)[:5]
            logger.info(f"K线日期示例: {sample_dates}, 类型: {type(sample_dates[0]) if sample_dates else 'None'}")
            
            # 过滤分红数据
            filtered = []
            for div in dividend_data:
                ex_date = div.get('ex_date')
                if ex_date:
                    # 转换除权除息日为datetime.date
                    if hasattr(ex_date, 'date') and callable(getattr(ex_date, 'date')):
                        ex_date = ex_date.date()
                    
                    logger.debug(f"检查分红日期: {ex_date}, 类型: {type(ex_date)}")
                    
                    # 检查是否在K线日期范围内
                    if ex_date in kline_date_set:
                        filtered.append(div)
                        logger.info(f"找到匹配的分红日期: {ex_date}")
            
            logger.info(f"过滤后找到 {len(filtered)} 条分红数据")
            return filtered
            
        except Exception as e:
            logger.exception(f"过滤分红数据失败: {e}")
            return []
    
    def show_dividend_tooltip(self, index, pos):
        """
        显示分红信息提示框
        
        Args:
            index: K线索引
            pos: 鼠标位置
        """
        try:
            if hasattr(self, 'dividend_marker_manager') and self.dividend_marker_manager:
                if self.dividend_marker_manager.has_dividend_at_index(index):
                    # 获取K线图的位置
                    view_box = self.tech_plot_widget.getViewBox()
                    view_pos = view_box.mapSceneToView(pos)
                    
                    # 显示分红提示框
                    self.dividend_marker_manager.show_tooltip(index, view_pos)
                else:
                    # 隐藏分红提示框
                    self.dividend_marker_manager.hide_tooltip()
        except Exception as e:
            logger.debug(f"显示分红提示框失败: {e}")
    
    def hide_dividend_tooltip(self):
        """
        隐藏分红信息提示框
        """
        try:
            if hasattr(self, 'dividend_marker_manager') and self.dividend_marker_manager:
                self.dividend_marker_manager.hide_tooltip()
        except Exception as e:
            logger.debug(f"隐藏分红提示框失败: {e}")
