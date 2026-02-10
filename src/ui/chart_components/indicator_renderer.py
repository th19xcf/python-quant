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
            
            # 绘制成交量柱状图
            self._draw_volume_bars(plot_widget, x, volumes, df)
            
            # 绘制成交量MA线
            self._draw_volume_ma_lines(plot_widget, x, df)
            
        except Exception as e:
            logger.exception(f"渲染成交量失败: {e}")
        return df

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
            
            # 设置Y轴范围（KDJ标准范围0-100，但允许超出）
            plot_widget.setYRange(-20, 120)
            logger.debug(f"KDJ Y轴范围设置: -20 - 120")
            
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
        return df

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
            logger.debug(f"渲染MACD，数据列: {df.columns}")
            if 'macd' not in df.columns or 'macd_signal' not in df.columns:
                logger.warning(f"MACD数据列不存在，尝试计算MACD")
                # 尝试计算MACD
                from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
                analyzer = TechnicalAnalyzer(df)
                analyzer.calculate_macd(fast_period=12, slow_period=26, signal_period=9)
                df = analyzer.get_data(return_polars=True)
                logger.debug(f"计算MACD后数据列: {df.columns}")
                if 'macd' not in df.columns or 'macd_signal' not in df.columns:
                    logger.error("计算MACD后数据列仍不存在")
                    return
            
            macd_data = df['macd'].to_numpy().astype(np.float64)
            signal_data = df['macd_signal'].to_numpy().astype(np.float64)
            
            # 设置Y轴范围
            if 'macd_hist' in df.columns:
                hist_data = df['macd_hist'].to_numpy()
                min_val = min(np.nanmin(macd_data), np.nanmin(signal_data), np.nanmin(hist_data))
                max_val = max(np.nanmax(macd_data), np.nanmax(signal_data), np.nanmax(hist_data))
            else:
                min_val = min(np.nanmin(macd_data), np.nanmin(signal_data))
                max_val = max(np.nanmax(macd_data), np.nanmax(signal_data))
            # 添加一些边距
            range_val = max_val - min_val
            min_val = min_val - range_val * 0.1
            max_val = max_val + range_val * 0.1
            plot_widget.setYRange(min_val, max_val)
            logger.debug(f"MACD Y轴范围: {min_val:.2f} - {max_val:.2f}")
            
            # 禁用科学计数法
            y_axis = plot_widget.getAxis('left')
            y_axis.enableAutoSIPrefix(False)
            
            # 绘制MACD线（DIF）
            macd_mask = ~np.isnan(macd_data)
            if np.any(macd_mask):
                plot_widget.plot(
                    x[macd_mask], 
                    macd_data[macd_mask], 
                    pen=pg.mkPen('w', width=1), 
                    name='DIF'
                )
                logger.debug(f"绘制MACD线，数据点: {np.sum(macd_mask)}")
            
            # 绘制信号线（DEA）
            signal_mask = ~np.isnan(signal_data)
            if np.any(signal_mask):
                plot_widget.plot(
                    x[signal_mask], 
                    signal_data[signal_mask], 
                    pen=pg.mkPen('y', width=1), 
                    name='DEA'
                )
                logger.debug(f"绘制DEA线，数据点: {np.sum(signal_mask)}")
            
            # 绘制柱状图（MACD柱）
            if 'macd_hist' in df.columns:
                self._draw_macd_histogram(plot_widget, x, df['macd_hist'].to_numpy())
            
            # 添加零轴
            plot_widget.addLine(y=0, pen=pg.mkPen('#666666', width=1))

        except Exception as e:
            logger.exception(f"渲染MACD失败: {e}")
        return df

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
        return df

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
        return df

    def render_boll(self, plot_widget: Any, df: Any, x: np.ndarray):
        """
        渲染BOLL指标
        
        Args:
            plot_widget: 图表控件
            df: 数据
            x: x轴坐标
        """
        try:
            logger.debug(f"渲染BOLL，数据列: {df.columns}")
            if 'mb' not in df.columns or 'up' not in df.columns or 'dn' not in df.columns:
                logger.warning(f"BOLL数据列不存在，尝试计算BOLL")
                # 尝试计算BOLL
                from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
                analyzer = TechnicalAnalyzer(df)
                analyzer.calculate_boll(windows=[20], std_dev=2.0)
                df = analyzer.get_data(return_polars=True)
                logger.debug(f"计算BOLL后数据列: {df.columns}")
                if 'mb' not in df.columns or 'up' not in df.columns or 'dn' not in df.columns:
                    logger.error("计算BOLL后数据列仍不存在")
                    return
            
            mb_data = df['mb'].to_numpy().astype(np.float64)
            up_data = df['up'].to_numpy().astype(np.float64)
            dn_data = df['dn'].to_numpy().astype(np.float64)
            
            # 设置Y轴范围
            all_data = np.concatenate([mb_data[~np.isnan(mb_data)], up_data[~np.isnan(up_data)], dn_data[~np.isnan(dn_data)]])
            if len(all_data) > 0:
                min_val = np.min(all_data) * 0.95
                max_val = np.max(all_data) * 1.05
                plot_widget.setYRange(min_val, max_val)
            
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
        return df

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
        return df

    def render_brar(self, plot_widget: Any, df: Any, x: np.ndarray):
        """
        渲染BRAR指标

        Args:
            plot_widget: 图表控件
            df: 数据
            x: x轴坐标
        """
        try:
            logger.debug(f"渲染BRAR，数据列: {df.columns}")
            if 'ar' not in df.columns or 'br' not in df.columns:
                logger.warning(f"BRAR数据列不存在，尝试计算BRAR")
                # 尝试计算BRAR
                from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
                analyzer = TechnicalAnalyzer(df)
                analyzer.calculate_brar(windows=[26])
                df = analyzer.get_data(return_polars=True)
                logger.debug(f"计算BRAR后数据列: {df.columns}")
                if 'ar' not in df.columns or 'br' not in df.columns:
                    logger.error("计算BRAR后数据列仍不存在")
                    return df

            ar_data = df['ar'].to_numpy().astype(np.float64)
            br_data = df['br'].to_numpy().astype(np.float64)

            # 设置Y轴范围（BRAR通常以100为基准，范围可以较大）
            all_data = np.concatenate([ar_data[~np.isnan(ar_data)], br_data[~np.isnan(br_data)]])
            if len(all_data) > 0:
                min_val = max(0, np.min(all_data) * 0.8)  # BRAR通常不小于0
                max_val = np.max(all_data) * 1.2
                # 确保范围至少包含100参考线
                min_val = min(min_val, 80)
                max_val = max(max_val, 120)
                plot_widget.setYRange(min_val, max_val)

            # 绘制AR线
            ar_mask = ~np.isnan(ar_data)
            if np.any(ar_mask):
                plot_widget.plot(
                    x[ar_mask],
                    ar_data[ar_mask],
                    pen=pg.mkPen('w', width=1),
                    name='AR'
                )

            # 绘制BR线
            br_mask = ~np.isnan(br_data)
            if np.any(br_mask):
                plot_widget.plot(
                    x[br_mask],
                    br_data[br_mask],
                    pen=pg.mkPen('y', width=1),
                    name='BR'
                )

            # 添加参考线
            plot_widget.addLine(y=100, pen=pg.mkPen('#666666', width=1, style=Qt.DotLine))

        except Exception as e:
            logger.exception(f"渲染BRAR失败: {e}")
        return df

    def render_dmi(self, plot_widget: Any, df: Any, x: np.ndarray):
        """
        渲染DMI指标

        Args:
            plot_widget: 图表控件
            df: 数据
            x: x轴坐标
        """
        try:
            logger.debug(f"渲染DMI，数据列: {df.columns}")
            if 'pdi' not in df.columns or 'ndi' not in df.columns:
                logger.warning(f"DMI数据列不存在，尝试计算DMI")
                # 尝试计算DMI
                from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
                analyzer = TechnicalAnalyzer(df)
                analyzer.calculate_dmi(windows=[14])
                df = analyzer.get_data(return_polars=True)
                logger.debug(f"计算DMI后数据列: {df.columns}")
                if 'pdi' not in df.columns or 'ndi' not in df.columns:
                    logger.error("计算DMI后数据列仍不存在")
                    return df

            pdi_data = df['pdi'].to_numpy().astype(np.float64)
            ndi_data = df['ndi'].to_numpy().astype(np.float64)

            # 设置Y轴范围（DMI范围0-100）
            plot_widget.setYRange(0, 100)

            # 禁用科学计数法
            y_axis = plot_widget.getAxis('left')
            y_axis.enableAutoSIPrefix(False)

            # 绘制PDI线（上升方向线，白色）
            pdi_mask = ~np.isnan(pdi_data)
            if np.any(pdi_mask):
                plot_widget.plot(
                    x[pdi_mask],
                    pdi_data[pdi_mask],
                    pen=pg.mkPen('w', width=1),
                    name='PDI'
                )

            # 绘制NDI线（下降方向线，黄色）
            ndi_mask = ~np.isnan(ndi_data)
            if np.any(ndi_mask):
                plot_widget.plot(
                    x[ndi_mask],
                    ndi_data[ndi_mask],
                    pen=pg.mkPen('y', width=1),
                    name='NDI'
                )

            # 绘制ADX线（平均趋向指数，红色）
            if 'adx' in df.columns:
                adx_data = df['adx'].to_numpy().astype(np.float64)
                adx_mask = ~np.isnan(adx_data)
                if np.any(adx_mask):
                    plot_widget.plot(
                        x[adx_mask],
                        adx_data[adx_mask],
                        pen=pg.mkPen('r', width=1),
                        name='ADX'
                    )

            # 绘制ADXR线（平均趋向评估，绿色）
            if 'adxr' in df.columns:
                adxr_data = df['adxr'].to_numpy().astype(np.float64)
                adxr_mask = ~np.isnan(adxr_data)
                if np.any(adxr_mask):
                    plot_widget.plot(
                        x[adxr_mask],
                        adxr_data[adxr_mask],
                        pen=pg.mkPen(pg.mkColor(0, 255, 0), width=1),
                        name='ADXR'
                    )

            # 添加参考线
            plot_widget.addLine(y=20, pen=pg.mkPen('#666666', width=1, style=Qt.DotLine))
            plot_widget.addLine(y=50, pen=pg.mkPen('#666666', width=1, style=Qt.DotLine))

        except Exception as e:
            logger.exception(f"渲染DMI失败: {e}")
        return df

    def render_trix(self, plot_widget: Any, df: Any, x: np.ndarray):
        """
        渲染TRIX指标

        Args:
            plot_widget: 图表控件
            df: 数据
            x: x轴坐标
        """
        try:
            logger.debug(f"渲染TRIX，数据列: {df.columns}")
            if 'trix' not in df.columns:
                logger.warning(f"TRIX数据列不存在，尝试计算TRIX")
                # 尝试计算TRIX
                from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
                analyzer = TechnicalAnalyzer(df)
                analyzer.calculate_trix(windows=[12], signal_period=9)
                df = analyzer.get_data(return_polars=True)
                logger.debug(f"计算TRIX后数据列: {df.columns}")
                if 'trix' not in df.columns:
                    logger.error("计算TRIX后数据列仍不存在")
                    return df

            trix_data = df['trix'].to_numpy().astype(np.float64)

            # 设置Y轴范围
            all_data = trix_data[~np.isnan(trix_data)]
            if len(all_data) > 0:
                min_val = np.min(all_data)
                max_val = np.max(all_data)
                # 添加边距
                range_val = max_val - min_val
                if range_val == 0:
                    range_val = 1
                min_val = min_val - range_val * 0.1
                max_val = max_val + range_val * 0.1
                plot_widget.setYRange(min_val, max_val)
                logger.debug(f"TRIX Y轴范围: {min_val:.4f} - {max_val:.4f}")

            # 禁用科学计数法
            y_axis = plot_widget.getAxis('left')
            y_axis.enableAutoSIPrefix(False)

            # 绘制TRIX线（白色）
            trix_mask = ~np.isnan(trix_data)
            if np.any(trix_mask):
                plot_widget.plot(
                    x[trix_mask],
                    trix_data[trix_mask],
                    pen=pg.mkPen('w', width=1),
                    name='TRIX'
                )

            # 绘制TRMA线（黄色，信号线）
            if 'trma' in df.columns:
                trma_data = df['trma'].to_numpy().astype(np.float64)
                trma_mask = ~np.isnan(trma_data)
                if np.any(trma_mask):
                    plot_widget.plot(
                        x[trma_mask],
                        trma_data[trma_mask],
                        pen=pg.mkPen('y', width=1),
                        name='TRMA'
                    )

            # 添加零轴
            plot_widget.addLine(y=0, pen=pg.mkPen('#666666', width=1))

        except Exception as e:
            logger.exception(f"渲染TRIX失败: {e}")
        return df

    def render_obv(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染OBV指标"""
        try:
            if 'obv' not in df.columns:
                from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
                analyzer = TechnicalAnalyzer(df)
                analyzer.calculate_obv()
                df = analyzer.get_data(return_polars=True)
            if 'obv' in df.columns:
                obv_data = df['obv'].to_numpy().astype(np.float64)
                mask = ~np.isnan(obv_data)
                if np.any(mask):
                    # 设置Y轴范围
                    all_data = obv_data[mask]
                    if len(all_data) > 0:
                        min_val = np.min(all_data)
                        max_val = np.max(all_data)
                        range_val = max_val - min_val
                        if range_val == 0:
                            range_val = abs(max_val) * 0.1 if max_val != 0 else 1
                        min_val = min_val - range_val * 0.1
                        max_val = max_val + range_val * 0.1
                        plot_widget.setYRange(min_val, max_val)
                    plot_widget.plot(x[mask], obv_data[mask], pen=pg.mkPen('w', width=1), name='OBV')
                    y_axis = plot_widget.getAxis('left')
                    y_axis.enableAutoSIPrefix(False)
        except Exception as e:
            logger.exception(f"渲染OBV失败: {e}")
        return df

    def render_asi(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染ASI指标"""
        try:
            if 'asi' not in df.columns:
                from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
                analyzer = TechnicalAnalyzer(df)
                analyzer.calculate_asi(signal_period=20)
                df = analyzer.get_data(return_polars=True)
            if 'asi' in df.columns:
                asi_data = df['asi'].to_numpy().astype(np.float64)
                mask = ~np.isnan(asi_data)
                if np.any(mask):
                    # 设置Y轴范围
                    all_data = asi_data[mask]
                    if 'asi_sig' in df.columns:
                        sig_data = df['asi_sig'].to_numpy().astype(np.float64)
                        sig_mask = ~np.isnan(sig_data)
                        if np.any(sig_mask):
                            all_data = np.concatenate([all_data, sig_data[sig_mask]])
                    if len(all_data) > 0:
                        min_val = np.min(all_data)
                        max_val = np.max(all_data)
                        range_val = max_val - min_val
                        if range_val == 0:
                            range_val = abs(max_val) * 0.1 if max_val != 0 else 1
                        min_val = min_val - range_val * 0.1
                        max_val = max_val + range_val * 0.1
                        plot_widget.setYRange(min_val, max_val)
                    plot_widget.plot(x[mask], asi_data[mask], pen=pg.mkPen('w', width=1), name='ASI')
                if 'asi_sig' in df.columns:
                    sig_data = df['asi_sig'].to_numpy().astype(np.float64)
                    sig_mask = ~np.isnan(sig_data)
                    if np.any(sig_mask):
                        plot_widget.plot(x[sig_mask], sig_data[sig_mask], pen=pg.mkPen('y', width=1), name='ASI_SIG')
                y_axis = plot_widget.getAxis('left')
                y_axis.enableAutoSIPrefix(False)
        except Exception as e:
            logger.exception(f"渲染ASI失败: {e}")
        return df

    def render_emv(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染EMV指标"""
        try:
            if 'emv' not in df.columns:
                from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
                analyzer = TechnicalAnalyzer(df)
                analyzer.calculate_emv(windows=[14])
                df = analyzer.get_data(return_polars=True)
            if 'emv' in df.columns:
                emv_data = df['emv'].to_numpy().astype(np.float64)
                mask = ~np.isnan(emv_data)
                if np.any(mask):
                    # 设置Y轴范围
                    all_data = emv_data[mask]
                    if len(all_data) > 0:
                        min_val = np.min(all_data)
                        max_val = np.max(all_data)
                        range_val = max_val - min_val
                        if range_val == 0:
                            range_val = abs(max_val) * 0.1 if max_val != 0 else 1
                        min_val = min_val - range_val * 0.1
                        max_val = max_val + range_val * 0.1
                        plot_widget.setYRange(min_val, max_val)
                    plot_widget.plot(x[mask], emv_data[mask], pen=pg.mkPen('w', width=1), name='EMV')
                y_axis = plot_widget.getAxis('left')
                y_axis.enableAutoSIPrefix(False)
                plot_widget.addLine(y=0, pen=pg.mkPen('#666666', width=1))
        except Exception as e:
            logger.exception(f"渲染EMV失败: {e}")
        return df

    def render_cci(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染CCI指标"""
        try:
            if 'cci' not in df.columns:
                from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
                analyzer = TechnicalAnalyzer(df)
                analyzer.calculate_cci(windows=[14])
                df = analyzer.get_data(return_polars=True)
            if 'cci' in df.columns:
                cci_data = df['cci'].to_numpy().astype(np.float64)
                mask = ~np.isnan(cci_data)
                if np.any(mask):
                    plot_widget.plot(x[mask], cci_data[mask], pen=pg.mkPen('w', width=1), name='CCI')
                plot_widget.setYRange(-200, 200)
                y_axis = plot_widget.getAxis('left')
                y_axis.enableAutoSIPrefix(False)
                plot_widget.addLine(y=100, pen=pg.mkPen('#666666', width=1, style=Qt.DotLine))
                plot_widget.addLine(y=-100, pen=pg.mkPen('#666666', width=1, style=Qt.DotLine))
        except Exception as e:
            logger.exception(f"渲染CCI失败: {e}")
        return df

    def render_roc(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染ROC指标"""
        try:
            if 'roc' not in df.columns:
                from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
                analyzer = TechnicalAnalyzer(df)
                analyzer.calculate_roc(windows=[12])
                df = analyzer.get_data(return_polars=True)
            if 'roc' in df.columns:
                roc_data = df['roc'].to_numpy().astype(np.float64)
                mask = ~np.isnan(roc_data)
                if np.any(mask):
                    # 设置Y轴范围
                    all_data = roc_data[mask]
                    if len(all_data) > 0:
                        min_val = np.min(all_data)
                        max_val = np.max(all_data)
                        range_val = max_val - min_val
                        if range_val == 0:
                            range_val = abs(max_val) * 0.1 if max_val != 0 else 1
                        min_val = min_val - range_val * 0.1
                        max_val = max_val + range_val * 0.1
                        plot_widget.setYRange(min_val, max_val)
                    plot_widget.plot(x[mask], roc_data[mask], pen=pg.mkPen('w', width=1), name='ROC')
                y_axis = plot_widget.getAxis('left')
                y_axis.enableAutoSIPrefix(False)
                plot_widget.addLine(y=0, pen=pg.mkPen('#666666', width=1))
        except Exception as e:
            logger.exception(f"渲染ROC失败: {e}")
        return df

    def render_mtm(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染MTM指标"""
        try:
            if 'mtm' not in df.columns:
                from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
                analyzer = TechnicalAnalyzer(df)
                analyzer.calculate_mtm(windows=[12])
                df = analyzer.get_data(return_polars=True)
            if 'mtm' in df.columns:
                mtm_data = df['mtm'].to_numpy().astype(np.float64)
                mask = ~np.isnan(mtm_data)
                if np.any(mask):
                    # 设置Y轴范围
                    all_data = mtm_data[mask]
                    if len(all_data) > 0:
                        min_val = np.min(all_data)
                        max_val = np.max(all_data)
                        range_val = max_val - min_val
                        if range_val == 0:
                            range_val = abs(max_val) * 0.1 if max_val != 0 else 1
                        min_val = min_val - range_val * 0.1
                        max_val = max_val + range_val * 0.1
                        plot_widget.setYRange(min_val, max_val)
                    plot_widget.plot(x[mask], mtm_data[mask], pen=pg.mkPen('w', width=1), name='MTM')
                y_axis = plot_widget.getAxis('left')
                y_axis.enableAutoSIPrefix(False)
                plot_widget.addLine(y=0, pen=pg.mkPen('#666666', width=1))
        except Exception as e:
            logger.exception(f"渲染MTM失败: {e}")
        return df

    def render_psy(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染PSY指标"""
        try:
            if 'psy' not in df.columns:
                from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
                analyzer = TechnicalAnalyzer(df)
                analyzer.calculate_psy(windows=[12])
                df = analyzer.get_data(return_polars=True)
            if 'psy' in df.columns:
                psy_data = df['psy'].to_numpy().astype(np.float64)
                mask = ~np.isnan(psy_data)
                if np.any(mask):
                    plot_widget.plot(x[mask], psy_data[mask], pen=pg.mkPen('w', width=1), name='PSY')
                plot_widget.setYRange(0, 100)
                y_axis = plot_widget.getAxis('left')
                y_axis.enableAutoSIPrefix(False)
                plot_widget.addLine(y=50, pen=pg.mkPen('#666666', width=1, style=Qt.DotLine))
        except Exception as e:
            logger.exception(f"渲染PSY失败: {e}")
        return df

    def render_mcst(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染MCST指标"""
        try:
            if 'mcst' not in df.columns:
                from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
                analyzer = TechnicalAnalyzer(df)
                analyzer.calculate_mcst(windows=[12])
                df = analyzer.get_data(return_polars=True)
            if 'mcst' in df.columns:
                mcst_data = df['mcst'].to_numpy().astype(np.float64)
                mask = ~np.isnan(mcst_data)
                if np.any(mask):
                    # 设置Y轴范围
                    all_data = mcst_data[mask]
                    if 'mcst_ma' in df.columns:
                        ma_data = df['mcst_ma'].to_numpy().astype(np.float64)
                        ma_mask = ~np.isnan(ma_data)
                        if np.any(ma_mask):
                            all_data = np.concatenate([all_data, ma_data[ma_mask]])
                    if len(all_data) > 0:
                        min_val = np.min(all_data)
                        max_val = np.max(all_data)
                        range_val = max_val - min_val
                        if range_val == 0:
                            range_val = abs(max_val) * 0.1 if max_val != 0 else 1
                        min_val = min_val - range_val * 0.1
                        max_val = max_val + range_val * 0.1
                        plot_widget.setYRange(min_val, max_val)
                    plot_widget.plot(x[mask], mcst_data[mask], pen=pg.mkPen('w', width=1), name='MCST')
                if 'mcst_ma' in df.columns:
                    ma_data = df['mcst_ma'].to_numpy().astype(np.float64)
                    ma_mask = ~np.isnan(ma_data)
                    if np.any(ma_mask):
                        plot_widget.plot(x[ma_mask], ma_data[ma_mask], pen=pg.mkPen('y', width=1), name='MCST_MA')
                y_axis = plot_widget.getAxis('left')
                y_axis.enableAutoSIPrefix(False)
        except Exception as e:
            logger.exception(f"渲染MCST失败: {e}")
        return df

    def render_dma(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染DMA指标"""
        try:
            if 'dma' not in df.columns or 'ama' not in df.columns:
                from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
                analyzer = TechnicalAnalyzer(df)
                analyzer.calculate_indicator_parallel('dma', short_period=10, long_period=50, signal_period=10)
                df = analyzer.get_data(return_polars=True)
            if 'dma' in df.columns and 'ama' in df.columns:
                dma_data = df['dma'].to_numpy().astype(np.float64)
                ama_data = df['ama'].to_numpy().astype(np.float64)
                mask = ~np.isnan(dma_data)
                ama_mask = ~np.isnan(ama_data)
                if np.any(mask) or np.any(ama_mask):
                    # 设置Y轴范围
                    all_data = np.concatenate([dma_data[mask], ama_data[ama_mask]])
                    if len(all_data) > 0:
                        min_val = np.min(all_data)
                        max_val = np.max(all_data)
                        range_val = max_val - min_val
                        if range_val == 0:
                            range_val = abs(max_val) * 0.1 if max_val != 0 else 1
                        min_val = min_val - range_val * 0.1
                        max_val = max_val + range_val * 0.1
                        plot_widget.setYRange(min_val, max_val)
                    # 绘制DMA线（白色）
                    if np.any(mask):
                        plot_widget.plot(x[mask], dma_data[mask], pen=pg.mkPen('w', width=1), name='DMA')
                    # 绘制AMA线（黄色）
                    if np.any(ama_mask):
                        plot_widget.plot(x[ama_mask], ama_data[ama_mask], pen=pg.mkPen('y', width=1), name='AMA')
                    # 绘制零线
                    plot_widget.addLine(y=0, pen=pg.mkPen('#666666', width=1, style=pg.QtCore.Qt.DashLine))
        except Exception as e:
            logger.exception(f"渲染DMA失败: {e}")
        return df

    def render_fsl(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染FSL指标"""
        try:
            if 'swl' not in df.columns or 'sws' not in df.columns:
                from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
                analyzer = TechnicalAnalyzer(df)
                analyzer.calculate_indicator_parallel('fsl')
                df = analyzer.get_data(return_polars=True)
            if 'swl' in df.columns and 'sws' in df.columns:
                swl_data = df['swl'].to_numpy().astype(np.float64)
                sws_data = df['sws'].to_numpy().astype(np.float64)
                mask = ~np.isnan(swl_data)
                sws_mask = ~np.isnan(sws_data)
                if np.any(mask) or np.any(sws_mask):
                    # 设置Y轴范围
                    all_data = np.concatenate([swl_data[mask], sws_data[sws_mask]])
                    if len(all_data) > 0:
                        min_val = np.min(all_data)
                        max_val = np.max(all_data)
                        range_val = max_val - min_val
                        if range_val == 0:
                            range_val = abs(max_val) * 0.1 if max_val != 0 else 1
                        min_val = min_val - range_val * 0.1
                        max_val = max_val + range_val * 0.1
                        plot_widget.setYRange(min_val, max_val)
                    # 绘制SWL线（白色）
                    if np.any(mask):
                        plot_widget.plot(x[mask], swl_data[mask], pen=pg.mkPen('w', width=1), name='SWL')
                    # 绘制SWS线（黄色）
                    if np.any(sws_mask):
                        plot_widget.plot(x[sws_mask], sws_data[sws_mask], pen=pg.mkPen('y', width=1), name='SWS')
        except Exception as e:
            logger.exception(f"渲染FSL失败: {e}")
        return df

    def render_sar(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染SAR指标"""
        try:
            if 'sar' not in df.columns:
                from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
                analyzer = TechnicalAnalyzer(df)
                analyzer.calculate_indicator_parallel('sar', af_step=0.02, max_af=0.2)
                df = analyzer.get_data(return_polars=True)
            if 'sar' in df.columns:
                sar_data = df['sar'].to_numpy().astype(np.float64)
                mask = ~np.isnan(sar_data)
                if np.any(mask):
                    # 绘制SAR点（白色圆点）
                    plot_widget.plot(x[mask], sar_data[mask], pen=None, symbol='o', symbolSize=3, symbolBrush='w', name='SAR')
        except Exception as e:
            logger.exception(f"渲染SAR失败: {e}")
        return df

    def render_vol_tdx(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染VOL-TDX指标"""
        try:
            if 'vol_tdx' not in df.columns:
                from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
                analyzer = TechnicalAnalyzer(df)
                analyzer.calculate_indicator_parallel('vol_tdx', ma_period=5)
                df = analyzer.get_data(return_polars=True)
            if 'vol_tdx' in df.columns:
                vol_tdx_data = df['vol_tdx'].to_numpy().astype(np.float64)
                mask = ~np.isnan(vol_tdx_data)
                if np.any(mask):
                    # 根据涨跌决定颜色
                    close_data = df['close'].to_numpy().astype(np.float64)
                    prev_close = np.roll(close_data, 1)
                    prev_close[0] = close_data[0]
                    colors = ['r' if close_data[i] >= prev_close[i] else 'g' for i in range(len(close_data))]
                    # 绘制柱状图
                    for i in range(len(x)):
                        if mask[i]:
                            plot_widget.plot([x[i], x[i]], [0, vol_tdx_data[i]], pen=pg.mkPen(colors[i], width=2))
        except Exception as e:
            logger.exception(f"渲染VOL-TDX失败: {e}")
        return df

    def render_cr(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染CR指标"""
        try:
            if 'cr' not in df.columns:
                from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
                analyzer = TechnicalAnalyzer(df)
                analyzer.calculate_indicator_parallel('cr', windows=[26])
                df = analyzer.get_data(return_polars=True)
            if 'cr' in df.columns:
                cr_data = df['cr'].to_numpy().astype(np.float64)
                mask = ~np.isnan(cr_data)
                if np.any(mask):
                    # 设置Y轴范围
                    valid_data = cr_data[mask]
                    if len(valid_data) > 0:
                        min_val = np.min(valid_data)
                        max_val = np.max(valid_data)
                        range_val = max_val - min_val
                        if range_val == 0:
                            range_val = abs(max_val) * 0.1 if max_val != 0 else 1
                        min_val = min_val - range_val * 0.1
                        max_val = max_val + range_val * 0.1
                        plot_widget.setYRange(min_val, max_val)
                    # 绘制CR线（白色）
                    plot_widget.plot(x[mask], cr_data[mask], pen=pg.mkPen('w', width=1), name='CR')
                    # 绘制参考线（100）
                    plot_widget.addLine(y=100, pen=pg.mkPen('#666666', width=1, style=pg.QtCore.Qt.DashLine))
        except Exception as e:
            logger.exception(f"渲染CR失败: {e}")
        return df

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
            'BRAR': self.render_brar,
            'DMI': self.render_dmi,
            'TRIX': self.render_trix,
            'OBV': self.render_obv,
            'ASI': self.render_asi,
            'EMV': self.render_emv,
            'CCI': self.render_cci,
            'ROC': self.render_roc,
            'MTM': self.render_mtm,
            'PSY': self.render_psy,
            'MCST': self.render_mcst,
            'DMA': self.render_dma,
            'FSL': self.render_fsl,
            'SAR': self.render_sar,
            'VOL-TDX': self.render_vol_tdx,
            'CR': self.render_cr,
        }
        
        renderer = renderers.get(indicator_name)
        if renderer:
            df = renderer(plot_widget, df, x)
        else:
            logger.warning(f"未知的指标类型: {indicator_name}")

        return df


# 导入Qt常量
from PySide6.QtCore import Qt
