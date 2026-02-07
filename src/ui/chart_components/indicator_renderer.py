#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
指标渲染类
负责各类技术指标的绘制和渲染
"""

import pyqtgraph as pg
import numpy as np
import pandas as pd
from typing import Any, Dict, Tuple, Optional
from loguru import logger


class IndicatorRenderer:
    """
    指标渲染类
    负责绘制各类技术指标
    """
    
    def __init__(self, main_window: Any):
        """
        初始化指标渲染器
        
        Args:
            main_window: 主窗口实例
        """
        self.main_window = main_window
    
    def render_ma_lines(self, plot_widget: Any, df: Any, x: np.ndarray):
        """
        渲染移动平均线
        
        Args:
            plot_widget: 图表控件
            df: 数据
            x: x轴坐标
        """
        try:
            # 初始化MA相关属性
            if not hasattr(self.main_window, 'moving_averages'):
                self.main_window.moving_averages = {}
            if not hasattr(self.main_window, 'selected_ma'):
                self.main_window.selected_ma = None
            if not hasattr(self.main_window, 'ma_points'):
                self.main_window.ma_points = []
            
            # 清除之前的标注点
            for point_item in self.main_window.ma_points:
                plot_widget.removeItem(point_item)
            self.main_window.ma_points.clear()
            
            # 定义MA配置
            ma_configs = [
                ('ma5', 'MA5', 'w', 'white'),
                ('ma10', 'MA10', 'c', 'cyan'),
                ('ma20', 'MA20', 'r', 'red'),
                ('ma60', 'MA60', pg.mkColor(0, 255, 0), '#00FF00'),
            ]
            
            # 绘制各条MA线
            for col_name, ma_name, pen_color, label_color in ma_configs:
                if col_name in df.columns:
                    self._draw_ma_line(plot_widget, df, x, col_name, ma_name, pen_color, label_color)
            
            # 保存MA数据
            self._save_ma_data(df)
            
        except Exception as e:
            logger.exception(f"渲染MA线失败: {e}")
    
    def _draw_ma_line(
        self, 
        plot_widget: Any, 
        df: Any, 
        x: np.ndarray,
        col_name: str, 
        ma_name: str, 
        pen_color: Any,
        label_color: str
    ):
        """绘制单条MA线"""
        ma_data = df[col_name].to_numpy().astype(np.float64)
        ma_mask = ~np.isnan(ma_data)
        
        if np.any(ma_mask):
            ma_item = plot_widget.plot(
                x[ma_mask], 
                ma_data[ma_mask], 
                pen=pg.mkPen(pen_color, width=1), 
                name=ma_name
            )
            self.main_window.moving_averages[ma_name] = {
                'item': ma_item,
                'data': (x[ma_mask], ma_data[ma_mask]),
                'color': pen_color if isinstance(pen_color, str) else label_color
            }
    
    def _save_ma_data(self, df: Any):
        """保存MA数据供后续使用"""
        self.main_window.ma_data = {
            'MA5': df['ma5'].to_numpy() if 'ma5' in df.columns else np.array([]),
            'MA10': df['ma10'].to_numpy() if 'ma10' in df.columns else np.array([]),
            'MA20': df['ma20'].to_numpy() if 'ma20' in df.columns else np.array([]),
            'MA60': df['ma60'].to_numpy() if 'ma60' in df.columns else np.array([]),
        }
        
        self.main_window.ma_colors = {
            'MA10': 'cyan',
            'MA20': 'red',
            'MA60': '#00FF00'
        }
    
    def render_volume(self, plot_widget: Any, df: Any, x: np.ndarray):
        """
        渲染成交量指标
        
        Args:
            plot_widget: 图表控件
            df: 数据
            x: x轴坐标
        """
        try:
            if 'volume' not in df.columns:
                return
            
            volumes = df['volume'].to_numpy()
            if len(volumes) == 0:
                return
            
            # 计算Y轴范围
            self._set_volume_y_range(plot_widget, volumes)
            
            # 禁用科学计数法
            y_axis = plot_widget.getAxis('left')
            y_axis.enableAutoSIPrefix(False)
            y_axis.setStyle(tickTextOffset=20)
            
            # 绘制成交量柱状图
            self._draw_volume_bars(plot_widget, x, volumes, df)
            
            # 绘制成交量MA线
            self._draw_volume_ma_lines(plot_widget, x, df)
            
        except Exception as e:
            logger.exception(f"渲染成交量失败: {e}")
    
    def _set_volume_y_range(self, plot_widget: Any, volumes: np.ndarray):
        """设置成交量Y轴范围"""
        volume_min = volumes.min()
        volume_max = volumes.max()
        
        if volume_max > 0:
            volume_mean = volumes.mean()
            volume_std = volumes.std()
            
            if volume_std / volume_mean < 0.1:
                # 数据比较集中
                y_min = max(0, volume_mean - volume_std * 2)
                y_max = volume_mean + volume_std * 3.5
            else:
                # 数据有一定差异
                y_range = volume_max - volume_min
                y_min = max(0, volume_min - y_range * 0.1)
                y_max = volume_max + y_range * 0.1
            
            plot_widget.setYRange(y_min, y_max)
        else:
            plot_widget.setYRange(0, 100)
    
    def _draw_volume_bars(self, plot_widget: Any, x: np.ndarray, volumes: np.ndarray, df: Any):
        """绘制成交量柱状图"""
        closes = df['close'].to_numpy() if 'close' in df.columns else None
        opens = df['open'].to_numpy() if 'open' in df.columns else None
        
        for i, vol in enumerate(volumes):
            # 确定颜色：根据涨跌
            if closes is not None and opens is not None and i < len(closes) and i < len(opens):
                color = '#FF0000' if closes[i] >= opens[i] else '#00FF00'
            else:
                color = '#C0C0C0'
            
            bar = pg.BarGraphItem(
                x=[x[i]], 
                height=[vol], 
                width=0.8, 
                brush=color,
                pen=None
            )
            plot_widget.addItem(bar)
    
    def _draw_volume_ma_lines(self, plot_widget: Any, x: np.ndarray, df: Any):
        """绘制成交量MA线"""
        ma_configs = [
            ('vol_ma5', 'white'),
            ('vol_ma10', 'cyan'),
        ]
        
        for col_name, color in ma_configs:
            if col_name in df.columns:
                ma_data = df[col_name].to_numpy().astype(np.float64)
                ma_mask = ~np.isnan(ma_data)
                if np.any(ma_mask):
                    plot_widget.plot(
                        x[ma_mask], 
                        ma_data[ma_mask], 
                        pen=pg.mkPen(color, width=1)
                    )
    
    def render_kdj(self, plot_widget: Any, df: Any, x: np.ndarray):
        """
        渲染KDJ指标
        
        Args:
            plot_widget: 图表控件
            df: 数据
            x: x轴坐标
        """
        try:
            if 'k' not in df.columns or 'd' not in df.columns:
                return
            
            k_data = df['k'].to_numpy().astype(np.float64)
            d_data = df['d'].to_numpy().astype(np.float64)
            
            # 过滤NaN值
            k_mask = ~np.isnan(k_data)
            d_mask = ~np.isnan(d_data)
            
            # 绘制K线和D线
            if np.any(k_mask):
                plot_widget.plot(
                    x[k_mask], 
                    k_data[k_mask], 
                    pen=pg.mkPen('w', width=1), 
                    name='K'
                )
            
            if np.any(d_mask):
                plot_widget.plot(
                    x[d_mask], 
                    d_data[d_mask], 
                    pen=pg.mkPen('y', width=1), 
                    name='D'
                )
            
            # 绘制J线（如果存在）
            if 'j' in df.columns:
                j_data = df['j'].to_numpy().astype(np.float64)
                j_mask = ~np.isnan(j_data)
                if np.any(j_mask):
                    plot_widget.plot(
                        x[j_mask], 
                        j_data[j_mask], 
                        pen=pg.mkPen('m', width=1), 
                        name='J'
                    )
            
            # 添加参考线
            self._add_kdj_reference_lines(plot_widget, x)
            
        except Exception as e:
            logger.exception(f"渲染KDJ失败: {e}")
    
    def _add_kdj_reference_lines(self, plot_widget: Any, x: np.ndarray):
        """添加KDJ参考线"""
        # 20线（超卖线）
        plot_widget.addLine(y=20, pen=pg.mkPen('#666666', width=1, style=Qt.DotLine))
        # 50线（中线）
        plot_widget.addLine(y=50, pen=pg.mkPen('#666666', width=1, style=Qt.DotLine))
        # 80线（超买线）
        plot_widget.addLine(y=80, pen=pg.mkPen('#666666', width=1, style=Qt.DotLine))
    
    def render_macd(self, plot_widget: Any, df: Any, x: np.ndarray):
        """
        渲染MACD指标
        
        Args:
            plot_widget: 图表控件
            df: 数据
            x: x轴坐标
        """
        try:
            if 'macd' not in df.columns or 'macd_signal' not in df.columns:
                return
            
            macd_data = df['macd'].to_numpy().astype(np.float64)
            signal_data = df['macd_signal'].to_numpy().astype(np.float64)
            
            # 绘制MACD线（DIF）
            macd_mask = ~np.isnan(macd_data)
            if np.any(macd_mask):
                plot_widget.plot(
                    x[macd_mask], 
                    macd_data[macd_mask], 
                    pen=pg.mkPen('w', width=1), 
                    name='DIF'
                )
            
            # 绘制信号线（DEA）
            signal_mask = ~np.isnan(signal_data)
            if np.any(signal_mask):
                plot_widget.plot(
                    x[signal_mask], 
                    signal_data[signal_mask], 
                    pen=pg.mkPen('y', width=1), 
                    name='DEA'
                )
            
            # 绘制柱状图（MACD柱）
            if 'macd_hist' in df.columns:
                self._draw_macd_histogram(plot_widget, x, df['macd_hist'].to_numpy())
            
            # 添加零轴
            plot_widget.addLine(y=0, pen=pg.mkPen('#666666', width=1))
            
        except Exception as e:
            logger.exception(f"渲染MACD失败: {e}")
    
    def _draw_macd_histogram(self, plot_widget: Any, x: np.ndarray, hist_data: np.ndarray):
        """绘制MACD柱状图"""
        for i, val in enumerate(hist_data):
            if not np.isnan(val):
                # 正值红色，负值绿色
                color = '#FF0000' if val >= 0 else '#00FF00'
                bar = pg.BarGraphItem(
                    x=[x[i]], 
                    height=[abs(val)], 
                    width=0.8, 
                    brush=color,
                    pen=None
                )
                # 设置柱状图位置
                if val >= 0:
                    bar.setOpts(x0=x[i]-0.4, y0=0, x1=x[i]+0.4, y1=val)
                else:
                    bar.setOpts(x0=x[i]-0.4, y0=val, x1=x[i]+0.4, y1=0)
                plot_widget.addItem(bar)
    
    def render_rsi(self, plot_widget: Any, df: Any, x: np.ndarray):
        """
        渲染RSI指标
        
        Args:
            plot_widget: 图表控件
            df: 数据
            x: x轴坐标
        """
        try:
            if 'rsi14' not in df.columns:
                return
            
            rsi_data = df['rsi14'].to_numpy().astype(np.float64)
            rsi_mask = ~np.isnan(rsi_data)
            
            if np.any(rsi_mask):
                plot_widget.plot(
                    x[rsi_mask], 
                    rsi_data[rsi_mask], 
                    pen=pg.mkPen('b', width=1), 
                    name='RSI14'
                )
            
            # 添加参考线
            plot_widget.addLine(y=20, pen=pg.mkPen('#666666', width=1, style=Qt.DotLine))
            plot_widget.addLine(y=50, pen=pg.mkPen('#666666', width=1, style=Qt.DotLine))
            plot_widget.addLine(y=80, pen=pg.mkPen('#666666', width=1, style=Qt.DotLine))
            
            # 设置Y轴范围
            plot_widget.setYRange(0, 100)
            
        except Exception as e:
            logger.exception(f"渲染RSI失败: {e}")
    
    def render_wr(self, plot_widget: Any, df: Any, x: np.ndarray):
        """
        渲染WR指标
        
        Args:
            plot_widget: 图表控件
            df: 数据
            x: x轴坐标
        """
        try:
            # 优先使用wr1和wr2（通达信风格）
            if 'wr1' in df.columns and 'wr2' in df.columns:
                wr1_data = df['wr1'].to_numpy().astype(np.float64)
                wr2_data = df['wr2'].to_numpy().astype(np.float64)
                
                wr1_mask = ~np.isnan(wr1_data)
                wr2_mask = ~np.isnan(wr2_data)
                
                if np.any(wr1_mask):
                    plot_widget.plot(
                        x[wr1_mask], 
                        wr1_data[wr1_mask], 
                        pen=pg.mkPen('y', width=1), 
                        name='WR1'
                    )
                
                if np.any(wr2_mask):
                    plot_widget.plot(
                        x[wr2_mask], 
                        wr2_data[wr2_mask], 
                        pen=pg.mkPen('w', width=1), 
                        name='WR2'
                    )
            elif 'wr' in df.columns:
                # 兼容旧格式
                wr_data = df['wr'].to_numpy().astype(np.float64)
                wr_mask = ~np.isnan(wr_data)
                
                if np.any(wr_mask):
                    plot_widget.plot(
                        x[wr_mask], 
                        wr_data[wr_mask], 
                        pen=pg.mkPen('w', width=1), 
                        name='WR'
                    )
            
            # 添加参考线
            plot_widget.addLine(y=-20, pen=pg.mkPen('#666666', width=1, style=Qt.DotLine))
            plot_widget.addLine(y=-50, pen=pg.mkPen('#666666', width=1, style=Qt.DotLine))
            plot_widget.addLine(y=-80, pen=pg.mkPen('#666666', width=1, style=Qt.DotLine))
            
        except Exception as e:
            logger.exception(f"渲染WR失败: {e}")
    
    def render_boll(self, plot_widget: Any, df: Any, x: np.ndarray):
        """
        渲染BOLL指标
        
        Args:
            plot_widget: 图表控件
            df: 数据
            x: x轴坐标
        """
        try:
            if 'mb' not in df.columns or 'up' not in df.columns or 'dn' not in df.columns:
                return
            
            mb_data = df['mb'].to_numpy().astype(np.float64)
            up_data = df['up'].to_numpy().astype(np.float64)
            dn_data = df['dn'].to_numpy().astype(np.float64)
            
            # 绘制中轨
            mb_mask = ~np.isnan(mb_data)
            if np.any(mb_mask):
                plot_widget.plot(
                    x[mb_mask], 
                    mb_data[mb_mask], 
                    pen=pg.mkPen('w', width=1), 
                    name='MB'
                )
            
            # 绘制上轨
            up_mask = ~np.isnan(up_data)
            if np.any(up_mask):
                plot_widget.plot(
                    x[up_mask], 
                    up_data[up_mask], 
                    pen=pg.mkPen('r', width=1), 
                    name='UP'
                )
            
            # 绘制下轨
            dn_mask = ~np.isnan(dn_data)
            if np.any(dn_mask):
                plot_widget.plot(
                    x[dn_mask], 
                    dn_data[dn_mask], 
                    pen=pg.mkPen(pg.mkColor(0, 255, 0), width=1), 
                    name='DN'
                )
            
        except Exception as e:
            logger.exception(f"渲染BOLL失败: {e}")
    
    def render_vr(self, plot_widget: Any, df: Any, x: np.ndarray):
        """
        渲染VR指标
        
        Args:
            plot_widget: 图表控件
            df: 数据
            x: x轴坐标
        """
        try:
            if 'vr' not in df.columns:
                return
            
            vr_data = df['vr'].to_numpy().astype(np.float64)
            vr_mask = ~np.isnan(vr_data)
            
            if np.any(vr_mask):
                plot_widget.plot(
                    x[vr_mask], 
                    vr_data[vr_mask], 
                    pen=pg.mkPen('w', width=1), 
                    name='VR'
                )
            
            # 绘制MAVR线
            if 'mavr' in df.columns:
                mavr_data = df['mavr'].to_numpy().astype(np.float64)
                mavr_mask = ~np.isnan(mavr_data)
                if np.any(mavr_mask):
                    plot_widget.plot(
                        x[mavr_mask], 
                        mavr_data[mavr_mask], 
                        pen=pg.mkPen('y', width=1), 
                        name='MAVR'
                    )
            
        except Exception as e:
            logger.exception(f"渲染VR失败: {e}")
    
    def render_indicator(
        self, 
        plot_widget: Any, 
        indicator_name: str, 
        x: np.ndarray, 
        df: Any
    ) -> Any:
        """
        根据指标名称渲染相应的指标
        
        Args:
            plot_widget: 图表控件
            indicator_name: 指标名称
            x: x轴坐标
            df: 数据
            
        Returns:
            Any: 处理后的数据
        """
        renderers = {
            'VOL': self.render_volume,
            'KDJ': self.render_kdj,
            'MACD': self.render_macd,
            'RSI': self.render_rsi,
            'WR': self.render_wr,
            'BOLL': self.render_boll,
            'VR': self.render_vr,
        }
        
        renderer = renderers.get(indicator_name)
        if renderer:
            renderer(plot_widget, df, x)
        else:
            logger.warning(f"未知的指标类型: {indicator_name}")
        
        return df


# 导入Qt常量
from PySide6.QtCore import Qt
