#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
图表管理器，封装图表刷新相关逻辑
"""

from src.utils.logger import logger
from src.ui.utils.chart_utils import ChartUtils


class ChartManager:
    """图表管理器"""

    def __init__(self, window):
        self.window = window

    def refresh_kline_chart(self):
        """刷新K线图"""
        pass

    def refresh_indicator_charts(self):
        """刷新指标图表"""
        pass

    def _setup_chart_labels(self, stock_name, stock_code):
        """
        设置图表标签
        """
        from PySide6.QtWidgets import QLabel, QHBoxLayout, QWidget

        window = self.window

        if hasattr(window, 'chart_title_label'):
            try:
                window.chart_title_label.deleteLater()
            except Exception as e:
                logger.warning(f"移除旧标题标签时发生错误: {e}")

        if hasattr(window, 'ma_values_label'):
            try:
                window.ma_values_label.deleteLater()
            except Exception as e:
                logger.warning(f"移除旧MA标签时发生错误: {e}")

        window.chart_title_label = QLabel()
        window.chart_title_label.setStyleSheet("font-family: Consolas, monospace; padding: 5px; color: #C0C0C0; background-color: transparent; border: none;")
        current_period = getattr(window, 'current_period', '日线')
        window.chart_title_label.setText(f"{stock_name}({stock_code}) {current_period}")
        window.chart_title_label.setWordWrap(False)

        window.ma_values_label = QLabel()
        window.ma_values_label.setStyleSheet("font-family: Consolas, monospace; padding: 5px; color: #C0C0C0; background-color: transparent; border: none;")
        window.ma_values_label.setText("<font color='#C0C0C0'>日期: --</font>  <font color='white'>MA5: --</font>  <font color='cyan'>MA10: --</font>  <font color='red'>MA20: --</font>  <font color='#00FF00'>MA60: --</font>")
        window.ma_values_label.setWordWrap(False)

        if hasattr(window, 'chart_container') and hasattr(window, 'chart_layout'):
            for i in range(window.chart_layout.count()):
                item = window.chart_layout.itemAt(i)
                if isinstance(item, QHBoxLayout):
                    window.chart_layout.removeItem(item)
                    break
                elif hasattr(window, 'title_ma_container') and item.widget() == window.title_ma_container:
                    window.chart_layout.removeWidget(window.title_ma_container)
                    break

            window.title_ma_container = QWidget()
            window.title_ma_container.setStyleSheet("background-color: #222222;")

            title_ma_layout = QHBoxLayout(window.title_ma_container)
            title_ma_layout.setSpacing(0)
            title_ma_layout.setContentsMargins(0, 0, 0, 0)

            window.menu_btn = window.create_indicator_menu_button(window.current_selected_window)
            title_ma_layout.addWidget(window.menu_btn)

            window_title_label = QLabel("K线")
            window_title_label.setStyleSheet("background-color: transparent; color: #C0C0C0; font-size: 12px; padding: 0 5px;")
            title_ma_layout.addWidget(window_title_label)

            title_ma_layout.addWidget(window.chart_title_label)
            title_ma_layout.addWidget(window.ma_values_label)
            title_ma_layout.addStretch(1)

            window.chart_layout.insertWidget(0, window.title_ma_container)

    def _prepare_chart_data(self, df):
        """准备图表数据"""
        import numpy as np

        dates = df['date'].to_list()
        opens = df['open'].to_list()
        highs = df['high'].to_list()
        lows = df['low'].to_list()
        closes = df['close'].to_list()
        
        # 获取柱体数设置
        bar_count = getattr(self.window, 'displayed_bar_count', 100)
        # 截取指定数量的数据（当柱体数小于数据长度时才截取）
        if bar_count < len(dates):
            dates = dates[-bar_count:]
            opens = opens[-bar_count:]
            highs = highs[-bar_count:]
            lows = lows[-bar_count:]
            closes = closes[-bar_count:]

        x = np.arange(len(dates))
        ohlc = np.column_stack((x, opens, highs, lows, closes))
        ohlc_list = [tuple(row) for row in ohlc]

        return dates, opens, highs, lows, closes, x, ohlc_list

    def _setup_chart_axes(self, plot_widget, dates, highs, lows):
        """设置图表坐标轴"""
        import numpy as np

        ax = plot_widget.getAxis('bottom')
        tick_interval = ChartUtils.calculate_x_axis_tick_interval(len(dates))
        ax.setTicks([[(i, dates[i].strftime('%Y-%m-%d')) for i in range(0, len(dates), tick_interval)]])

        y_min = np.min(lows) * 0.99
        y_max = np.max(highs) * 1.01
        plot_widget.setYRange(y_min, y_max)

    def _draw_indicator_curve(self, plot_widget, x, data, color, width=1, name=None):
        """绘制指标曲线的通用方法"""
        import pyqtgraph as pg
        plot_widget.plot(x, data, pen=pg.mkPen(color, width=width), name=name)

    def _draw_indicator_histogram(self, plot_widget, x, data, positive_color='r', negative_color='g', width=0.35):
        """绘制指标柱状图的通用方法"""
        import pyqtgraph as pg
        import numpy as np

        colors = []
        for val in data:
            if val >= 0:
                colors.append(positive_color)
            else:
                colors.append(negative_color)

        bar_item = pg.BarGraphItem(
            x=np.array(x),
            height=np.array(data),
            width=width,
            brush=colors,
            pen=None
        )
        plot_widget.addItem(bar_item)

    def _update_indicator_values(self, label_widget, indicator_name, data_df):
        """更新指标数值显示的通用方法"""
        if indicator_name == "KDJ":
            latest_k = data_df['k'].iloc[-1]
            latest_d = data_df['d'].iloc[-1]
            latest_j = data_df['j'].iloc[-1]
            text = f"<font color='white'>K: {latest_k:.2f}</font>  <font color='yellow'>D: {latest_d:.2f}</font>  <font color='magenta'>J: {latest_j:.2f}</font>"
        elif indicator_name == "MACD":
            latest_macd = data_df['macd'].iloc[-1]
            latest_macd_signal = data_df['macd_signal'].iloc[-1]
            latest_macd_hist = data_df['macd_hist'].iloc[-1]
            text = f"<font color='white'>MACD(12,26,9) DIF: {latest_macd:.2f} DEA: {latest_macd_signal:.2f} MACD: {latest_macd_hist:.2f}</font>"
        elif indicator_name == "MA":
            latest_ma5 = data_df['ma5'].iloc[-1]
            latest_ma10 = data_df['ma10'].iloc[-1]
            latest_ma20 = data_df['ma20'].iloc[-1]
            latest_ma60 = data_df['ma60'].iloc[-1]
            text = f"<font color='white'>MA5: {latest_ma5:.2f}</font>  <font color='cyan'>MA10: {latest_ma10:.2f}</font>  <font color='red'>MA20: {latest_ma20:.2f}</font>  <font color='#00FF00'>MA60: {latest_ma60:.2f}</font>"
        elif indicator_name == "WR":
            if 'wr1' in data_df.columns and 'wr2' in data_df.columns:
                latest_wr1 = data_df['wr1'].iloc[-1]
                latest_wr2 = data_df['wr2'].iloc[-1]
                text = f"<font color='white'>WR(10,6) </font><font color='yellow'>WR1: {latest_wr1:.2f}</font> <font color='white'>WR2: {latest_wr2:.2f}</font>"
            elif 'wr' in data_df.columns:
                latest_wr = data_df['wr'].iloc[-1]
                text = f"<font color='white'>WR: {latest_wr:.2f}</font>"
            else:
                text = ""
        elif indicator_name == "BOLL":
            if 'mb' in data_df.columns and 'up' in data_df.columns and 'dn' in data_df.columns:
                latest_mb = data_df['mb'].iloc[-1]
                latest_up = data_df['up'].iloc[-1]
                latest_dn = data_df['dn'].iloc[-1]
                text = f"<font color='white'>MB: {latest_mb:.2f}</font>  <font color='red'>UP: {latest_up:.2f}</font>  <font color='#00FF00'>DN: {latest_dn:.2f}</font>"
            else:
                text = ""
        elif indicator_name == "DMI":
            if 'pdi' in data_df.columns and 'ndi' in data_df.columns and 'adx' in data_df.columns and 'adxr' in data_df.columns:
                latest_pdi = data_df['pdi'].iloc[-1]
                latest_ndi = data_df['ndi'].iloc[-1]
                latest_adx = data_df['adx'].iloc[-1]
                latest_adxr = data_df['adxr'].iloc[-1]
                text = f"<font color='#FFFFFF'>PDI: {latest_pdi:.2f}</font>  <font color='#FFFF00'>NDI: {latest_ndi:.2f}</font>  <font color='#FF00FF'>ADX: {latest_adx:.2f}</font>  <font color='#00FF00'>ADXR: {latest_adxr:.2f}</font>"
            else:
                text = ""
        else:
            text = ""

        if label_widget:
            label_widget.setText(text)

    def _setup_indicator_axis(self, plot_widget, x_data, y_data):
        """设置指标图表坐标轴的通用方法"""
        import numpy as np

        plot_widget.setXRange(0, len(x_data) - 1)

        y_min = np.min(y_data) * 1.2 if np.min(y_data) < 0 else 0
        y_max = np.max(y_data) * 1.2
        plot_widget.setYRange(y_min, y_max)

    def _draw_technical_indicator(self, plot_widget, indicator_name, x, data_df):
        """绘制技术指标的通用方法"""
        import pyqtgraph as pg
        import numpy as np

        def get_col_values(df, col_name):
            if hasattr(df, 'to_pandas'):
                return df[col_name].to_numpy()
            return df[col_name].values

        def get_all_values(df):
            if hasattr(df, 'to_pandas'):
                return df.to_numpy()
            return df.values

        if indicator_name == "KDJ":
            k_values = get_col_values(data_df, 'k')
            d_values = get_col_values(data_df, 'd')
            j_values = get_col_values(data_df, 'j')
            self._draw_indicator_curve(plot_widget, x, k_values, 'white', 1, 'K')
            self._draw_indicator_curve(plot_widget, x, d_values, 'yellow', 1, 'D')
            self._draw_indicator_curve(plot_widget, x, j_values, 'magenta', 1, 'J')
            self._setup_indicator_axis(plot_widget, x, np.column_stack((k_values, d_values, j_values)).flatten())
        elif indicator_name == "MACD":
            macd_values = get_col_values(data_df, 'macd')
            macd_signal_values = get_col_values(data_df, 'macd_signal')
            macd_hist_values = get_col_values(data_df, 'macd_hist')
            self._draw_indicator_curve(plot_widget, x, macd_values, '#FFFFFF', 1, 'DIF')
            self._draw_indicator_curve(plot_widget, x, macd_signal_values, '#FFFF00', 1, 'DEA')
            self._draw_indicator_histogram(plot_widget, x, macd_hist_values, '#FF0000', '#00FF00')
            self._setup_indicator_axis(plot_widget, x, np.column_stack((macd_values, macd_signal_values, macd_hist_values)).flatten())
        elif indicator_name == "RSI":
            rsi_values = get_col_values(data_df, 'rsi')
            self._draw_indicator_curve(plot_widget, x, rsi_values, 'white', 1, 'RSI')
            self._setup_indicator_axis(plot_widget, x, rsi_values)
        elif indicator_name == "VOL":
            volume_values = get_col_values(data_df, 'volume')
            self._draw_indicator_histogram(plot_widget, x, volume_values, 'r', 'g', width=0.35)
            self._setup_indicator_axis(plot_widget, x, volume_values)
        elif indicator_name == "MA":
            ma5_values = get_col_values(data_df, 'ma5')
            ma10_values = get_col_values(data_df, 'ma10')
            ma20_values = get_col_values(data_df, 'ma20')
            ma60_values = get_col_values(data_df, 'ma60')
            self._draw_indicator_curve(plot_widget, x, ma5_values, 'white', 1, 'MA5')
            self._draw_indicator_curve(plot_widget, x, ma10_values, 'cyan', 1, 'MA10')
            self._draw_indicator_curve(plot_widget, x, ma20_values, 'red', 1, 'MA20')
            self._draw_indicator_curve(plot_widget, x, ma60_values, '#00FF00', 1, 'MA60')
            self._setup_indicator_axis(plot_widget, x, np.column_stack((ma5_values, ma10_values, ma20_values, ma60_values)).flatten())
        elif indicator_name == "DMI":
            pdi_values = get_col_values(data_df, 'pdi')
            ndi_values = get_col_values(data_df, 'ndi')
            adx_values = get_col_values(data_df, 'adx')
            adxr_values = get_col_values(data_df, 'adxr')
            self._draw_indicator_curve(plot_widget, x, pdi_values, '#FFFFFF', 1.0, '+DI')
            self._draw_indicator_curve(plot_widget, x, ndi_values, '#FFFF00', 1.0, '-DI')
            self._draw_indicator_curve(plot_widget, x, adx_values, '#FF00FF', 1.0, 'ADX')
            self._draw_indicator_curve(plot_widget, x, adxr_values, '#00FF00', 1.0, 'ADXR')
            self._setup_indicator_axis(plot_widget, x, np.column_stack((pdi_values, ndi_values, adx_values, adxr_values)).flatten())
        else:
            for column in data_df.columns:
                if column not in ['date', 'open', 'high', 'low', 'close', 'volume']:
                    self._draw_indicator_curve(plot_widget, x, get_col_values(data_df, column), 'white', 1, column)
            self._setup_indicator_axis(plot_widget, x, get_all_values(data_df).flatten())

    def draw_indicator(self, plot_widget, indicator_name, x, df_pl):
        """
        根据指标名称绘制相应的指标
        """
        import pyqtgraph as pg

        window = self.window

        try:
            plot_widget.clear()
        except Exception as e:
            logger.warning(f"Error clearing plot widget: {e}")

        plot_widget.setBackground('#000000')
        plot_widget.setLabel('left', indicator_name, color='#C0C0C0')
        plot_widget.setLabel('bottom', '', color='#C0C0C0')
        plot_widget.getAxis('left').setPen(pg.mkPen('#C0C0C0'))
        plot_widget.getAxis('bottom').setPen(pg.mkPen('#C0C0C0'))
        plot_widget.getAxis('left').setTextPen(pg.mkPen('#C0C0C0'))
        plot_widget.getAxis('bottom').setTextPen(pg.mkPen('#C0C0C0'))
        plot_widget.showGrid(x=True, y=True, alpha=0.3)

        indicator_y_ranges = {
            "VOL": (0, 1000000000),
            "MACD": (-5, 5),
            "KDJ": (-50, 150),
            "RSI": (-50, 150),
            "BOLL": (0, 100),
            "WR": (-50, 150),
            "DMI": (0, 100),
            "VR": (0, 200)
        }

        if indicator_name in indicator_y_ranges:
            y_min, y_max = indicator_y_ranges[indicator_name]
            plot_widget.setYRange(y_min, y_max)

            if indicator_name == "VOL":
                plot_widget.setLogMode(y=False)

        updated_df_pl = window.indicator_drawer_manager.draw_indicator(indicator_name, plot_widget, x, df_pl)

        self.save_indicator_data(updated_df_pl)
        window.indicator_label_manager.update_indicator_values_label(indicator_name, updated_df_pl)

        return updated_df_pl

    def update_indicator_values_label(self, indicator_name, df_pl):
        """更新指标值显示标签"""
        from PySide6.QtWidgets import QLabel

        window = self.window

        if not hasattr(window, 'kdj_values_label'):
            window.kdj_values_label = QLabel()
            window.kdj_values_label.setStyleSheet("font-family: Consolas, monospace; background-color: rgba(0, 0, 0, 0.5); padding: 5px; color: #C0C0C0;")
            window.kdj_values_label.setWordWrap(False)

        self.save_indicator_data(df_pl)

        current_indicator = window.window_indicators.get(3, "KDJ")

        # 使用 Polars 的 item() 方法获取最后一行数据，避免转换为 pandas
        def get_latest_value(df, col):
            """获取列的最后一个值"""
            if col in df.columns:
                return df[col].item(-1) if len(df) > 0 else None
            return None

        if current_indicator == "KDJ":
            latest_k = get_latest_value(df_pl, 'k')
            latest_d = get_latest_value(df_pl, 'd')
            latest_j = get_latest_value(df_pl, 'j')
            if latest_k is not None and latest_d is not None and latest_j is not None:
                kdj_text = f"<font color='white'>K: {latest_k:.2f}</font>  <font color='yellow'>D: {latest_d:.2f}</font>  <font color='magenta'>J: {latest_j:.2f}</font>"
            else:
                kdj_text = "<font color='white'>KDJ指标数据不可用</font>"
            window.kdj_values_label.setText(kdj_text)
        elif current_indicator == "RSI":
            latest_rsi = get_latest_value(df_pl, 'rsi14')
            if latest_rsi is not None:
                rsi_text = f"<font color='blue'>RSI14: {latest_rsi:.2f}</font>"
            else:
                rsi_text = "<font color='white'>RSI指标数据不可用</font>"
            window.kdj_values_label.setText(rsi_text)
        elif current_indicator == "MACD":
            latest_macd = get_latest_value(df_pl, 'macd')
            latest_macd_signal = get_latest_value(df_pl, 'macd_signal')
            latest_macd_hist = get_latest_value(df_pl, 'macd_hist')
            if all(v is not None for v in [latest_macd, latest_macd_signal, latest_macd_hist]):
                macd_hist_color = '#FF0000' if latest_macd_hist >= 0 else '#00FF00'
                macd_text = f"<font color='white'>MACD(12,26,9) </font><font color='#FFFFFF'>DIF: {latest_macd:.2f}</font> <font color='#FFFF00'>DEA: {latest_macd_signal:.2f}</font> <font color='{macd_hist_color}'>MACD: {latest_macd_hist:.2f}</font>"
            else:
                macd_text = "<font color='white'>MACD指标数据不可用</font>"
            window.kdj_values_label.setText(macd_text)
        elif current_indicator == "WR":
            latest_wr1 = get_latest_value(df_pl, 'wr1')
            latest_wr2 = get_latest_value(df_pl, 'wr2')
            if latest_wr1 is not None and latest_wr2 is not None:
                wr_text = f"<font color='white'>WR(10,6) </font><font color='yellow'>WR1: {latest_wr1:.2f}</font> <font color='white'>WR2: {latest_wr2:.2f}</font>"
            else:
                wr_text = "<font color='white'>WR指标数据不可用</font>"
            window.kdj_values_label.setText(wr_text)
        elif current_indicator == "BOLL":
            latest_mb = get_latest_value(df_pl, 'mb')
            latest_up = get_latest_value(df_pl, 'up')
            latest_dn = get_latest_value(df_pl, 'dn')
            if all(v is not None for v in [latest_mb, latest_up, latest_dn]):
                boll_text = f"<font color='white'>MB: {latest_mb:.2f}</font>  <font color='red'>UP: {latest_up:.2f}</font>  <font color='#00FF00'>DN: {latest_dn:.2f}</font>"
            else:
                boll_text = "<font color='white'>BOLL指标数据不可用</font>"
            window.kdj_values_label.setText(boll_text)
        elif current_indicator == "DMI":
            latest_pdi = get_latest_value(df_pl, 'pdi')
            latest_ndi = get_latest_value(df_pl, 'ndi')
            latest_adx = get_latest_value(df_pl, 'adx')
            latest_adxr = get_latest_value(df_pl, 'adxr')
            if all(v is not None for v in [latest_pdi, latest_ndi, latest_adx, latest_adxr]):
                dmi_text = f"<font color='#FFFFFF'>PDI: {latest_pdi:.2f}</font>  <font color='#FFFF00'>NDI: {latest_ndi:.2f}</font>  <font color='#FF00FF'>ADX: {latest_adx:.2f}</font>  <font color='#00FF00'>ADXR: {latest_adxr:.2f}</font>"
            else:
                dmi_text = "<font color='white'>DMI指标数据不可用</font>"
            window.kdj_values_label.setText(dmi_text)
        elif current_indicator == "TRIX":
            latest_trix = get_latest_value(df_pl, 'trix')
            latest_trma = get_latest_value(df_pl, 'trma')
            if latest_trix is not None and latest_trma is not None:
                trix_text = f"<font color='#FFFF00'>TRIX: {latest_trix:.2f}</font>  <font color='#FFFFFF'>TRMA: {latest_trma:.2f}</font>"
            else:
                trix_text = "<font color='white'>TRIX指标数据不可用</font>"
            window.kdj_values_label.setText(trix_text)
        elif current_indicator == "BRAR":
            latest_br = get_latest_value(df_pl, 'br')
            latest_ar = get_latest_value(df_pl, 'ar')
            if latest_br is not None and latest_ar is not None:
                brar_text = f"<font color='#FFFF00'>BR: {latest_br:.2f}</font>  <font color='#FFFFFF'>AR: {latest_ar:.2f}</font>"
            else:
                brar_text = "<font color='white'>BRAR指标数据不可用</font>"
            window.kdj_values_label.setText(brar_text)
        else:
            latest_k = get_latest_value(df_pl, 'k')
            latest_d = get_latest_value(df_pl, 'd')
            latest_j = get_latest_value(df_pl, 'j')
            if all(v is not None for v in [latest_k, latest_d, latest_j]):
                kdj_text = f"<font color='white'>K: {latest_k:.2f}</font>  <font color='yellow'>D: {latest_d:.2f}</font>  <font color='magenta'>J: {latest_j:.2f}</font>"
            else:
                kdj_text = "<font color='white'>指标数据不可用</font>"
            window.kdj_values_label.setText(kdj_text)

    def save_indicator_data(self, df_pl):
        """保存指标数据，用于鼠标移动时更新指标数值"""
        window = self.window

        window.current_kdj_data = {
            'k': df_pl['k'].to_list() if 'k' in df_pl.columns else [],
            'd': df_pl['d'].to_list() if 'd' in df_pl.columns else [],
            'j': df_pl['j'].to_list() if 'j' in df_pl.columns else []
        }

        window.current_rsi_data = {
            'rsi': df_pl['rsi14'].to_list() if 'rsi14' in df_pl.columns else []
        }

        window.current_macd_data = {
            'macd': df_pl['macd'].to_list() if 'macd' in df_pl.columns else [],
            'macd_signal': df_pl['macd_signal'].to_list() if 'macd_signal' in df_pl.columns else [],
            'macd_hist': df_pl['macd_hist'].to_list() if 'macd_hist' in df_pl.columns else []
        }

        window.current_volume_data = {
            'volume': df_pl['volume'].to_list() if 'volume' in df_pl.columns else [],
            'vol_ma5': df_pl['vol_ma5'].to_list() if 'vol_ma5' in df_pl.columns else [],
            'vol_ma10': df_pl['vol_ma10'].to_list() if 'vol_ma10' in df_pl.columns else []
        }

        window.current_wr_data = {
            'wr1': df_pl['wr1'].to_list() if 'wr1' in df_pl.columns else [],
            'wr2': df_pl['wr2'].to_list() if 'wr2' in df_pl.columns else []
        }

        window.current_boll_data = {
            'mb': df_pl['mb'].to_list() if 'mb' in df_pl.columns else [],
            'up': df_pl['up'].to_list() if 'up' in df_pl.columns else [],
            'dn': df_pl['dn'].to_list() if 'dn' in df_pl.columns else []
        }

        window.current_dmi_data = {
            'pdi': df_pl['pdi'].to_list() if 'pdi' in df_pl.columns else [],
            'ndi': df_pl['ndi'].to_list() if 'ndi' in df_pl.columns else [],
            'adx': df_pl['adx'].to_list() if 'adx' in df_pl.columns else [],
            'adxr': df_pl['adxr'].to_list() if 'adxr' in df_pl.columns else []
        }

        window.current_trix_data = {
            'trix': df_pl['trix'].to_list() if 'trix' in df_pl.columns else [],
            'trma': df_pl['trma'].to_list() if 'trma' in df_pl.columns else []
        }

        window.current_brar_data = {
            'br': df_pl['br'].to_list() if 'br' in df_pl.columns else [],
            'ar': df_pl['ar'].to_list() if 'ar' in df_pl.columns else []
        }

        window.current_vr_data = {
            'vr': df_pl['vr'].to_list() if 'vr' in df_pl.columns else [],
            'mavr': df_pl['mavr'].to_list() if 'mavr' in df_pl.columns else []
        }

    def plot_k_line(self, df, stock_name, stock_code):
        """委托主窗口实现K线绘制"""
        return self.window._plot_k_line_impl(df, stock_name, stock_code)
