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
            
            # 检查是否显示MA线
            if not hasattr(self.main_window, 'show_ma_lines') or not self.main_window.show_ma_lines:
                logger.debug("MA线显示已关闭，跳过绘制")
                # 仍然保存MA数据供其他功能使用
                self._save_ma_data(df)
                return
            
            # 调试：检查数据列
            logger.debug(f"render_ma_lines - 数据列: {list(df.columns) if hasattr(df, 'columns') else 'N/A'}")
            logger.debug(f"render_ma_lines - 数据行数: {len(df)}")
            
            # 定义MA配置
            ma_configs = [
                ('ma5', 'MA5', 'w', 'white'),
                ('ma10', 'MA10', 'c', 'cyan'),
                ('ma20', 'MA20', 'r', 'red'),
                ('ma60', 'MA60', pg.mkColor(0, 255, 0), '#00FF00'),
            ]
            
            # 绘制各条MA线
            ma_drawn_count = 0
            for col_name, ma_name, pen_color, label_color in ma_configs:
                if col_name in df.columns:
                    # 检查MA数据是否有效
                    ma_data = df[col_name].to_numpy()
                    valid_count = np.sum(~np.isnan(ma_data))
                    logger.debug(f"绘制MA线: {ma_name}, 列名: {col_name}, 有效数据点: {valid_count}/{len(ma_data)}")
                    if valid_count > 0:
                        self._draw_ma_line(plot_widget, df, x, col_name, ma_name, pen_color, label_color)
                        ma_drawn_count += 1
                    else:
                        logger.warning(f"MA线 {ma_name} 没有有效数据")
                else:
                    logger.warning(f"MA列不存在: {col_name}")
            
            logger.debug(f"render_ma_lines - 成功绘制 {ma_drawn_count} 条MA线")
            
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
            logger.debug(f"渲染VR指标，数据列: {df.columns if hasattr(df, 'columns') else 'N/A'}")
            
            # 如果没有VR数据，先计算
            if 'vr' not in df.columns:
                from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
                analyzer = TechnicalAnalyzer(df)
                analyzer.calculate_indicator_parallel('vr', windows=[24])
                df = analyzer.get_data(return_polars=True)
            
            if 'vr' in df.columns:
                vr_data = df['vr'].to_numpy().astype(np.float64)
                vr_mask = ~np.isnan(vr_data)
                valid_count = np.sum(vr_mask)
                logger.debug(f"VR数据有效点数: {valid_count}")
                
                if np.any(vr_mask):
                    # 设置Y轴范围
                    valid_vr = vr_data[vr_mask]
                    min_val = np.min(valid_vr)
                    max_val = np.max(valid_vr)
                    range_val = max_val - min_val
                    if range_val > 0:
                        plot_widget.setYRange(min_val - range_val * 0.1, max_val + range_val * 0.1)
                    
                    plot_widget.plot(
                        x[vr_mask], 
                        vr_data[vr_mask], 
                        pen=pg.mkPen('w', width=1), 
                        name='VR'
                    )
                    logger.debug(f"VR指标渲染完成，绘制了 {valid_count} 个点")
                else:
                    logger.warning("VR数据全部为空值，无法绘制")
            else:
                logger.warning("VR列不存在，无法渲染")
            
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
            # DMA指标已经在数据准备阶段计算完成，直接使用
            if 'dma' not in df.columns or 'ama' not in df.columns:
                logger.warning("DMA数据列不存在，跳过渲染")
                return df

            dma_data = df['dma'].to_numpy().astype(np.float64)
            ama_data = df['ama'].to_numpy().astype(np.float64)
            mask = ~np.isnan(dma_data)
            ama_mask = ~np.isnan(ama_data)

            if np.any(mask) or np.any(ama_mask):
                # 设置Y轴范围（使用所有有效数据）
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
                    plot_widget.plot(x, dma_data, pen=pg.mkPen('w', width=1), name='DMA')
                    logger.debug(f"绘制DMA线，数据点: {np.sum(mask)}")

                # 绘制AMA线（黄色）
                if np.any(ama_mask):
                    plot_widget.plot(x, ama_data, pen=pg.mkPen('y', width=1), name='AMA')
                    logger.debug(f"绘制AMA线，数据点: {np.sum(ama_mask)}")

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
            logger.debug(f"渲染SAR指标，数据列: {df.columns if hasattr(df, 'columns') else 'N/A'}")
            logger.debug(f"输入数据行数: {len(df)}, x坐标长度: {len(x)}")
            
            # 强制重新计算SAR指标，确保数据正确
            from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
            analyzer = TechnicalAnalyzer(df)
            analyzer.calculate_indicator_parallel('sar', af_step=0.02, max_af=0.2)
            df = analyzer.get_data(return_polars=True)
            
            if 'sar' in df.columns:
                sar_data = df['sar'].to_numpy().astype(np.float64)
                logger.debug(f"SAR数据长度: {len(sar_data)}")
                
                # 确保数据长度与x坐标一致
                if len(sar_data) != len(x):
                    logger.warning(f"SAR数据长度({len(sar_data)})与x坐标长度({len(x)})不一致，进行调整")
                    # 如果数据长度不一致，截取或填充
                    if len(sar_data) > len(x):
                        sar_data = sar_data[-len(x):]
                    elif len(sar_data) < len(x):
                        # 在开头填充nan
                        padding = np.full(len(x) - len(sar_data), np.nan)
                        sar_data = np.concatenate([padding, sar_data])
                
                mask = ~np.isnan(sar_data)
                valid_count = np.sum(mask)
                logger.debug(f"SAR数据有效点数: {valid_count}")
                
                if np.any(mask):
                    # 绘制SAR点（白色圆点）
                    plot_widget.plot(x[mask], sar_data[mask], pen=None, symbol='o', symbolSize=3, symbolBrush='w', name='SAR')
                    logger.debug(f"SAR指标渲染完成，绘制了 {valid_count} 个点")
                else:
                    logger.warning("SAR数据全部为空值，无法绘制")
            else:
                logger.warning("SAR列不存在，无法渲染")
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

    def render_expma(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染EXPMA指标"""
        try:
            if 'expma12' not in df.columns or 'expma50' not in df.columns:
                from src.tech_analysis.indicator_manager import global_indicator_manager
                import polars as pl
                # 确保数据是polars DataFrame
                if hasattr(df, 'to_pandas'):
                    pl_df = df
                else:
                    pl_df = pl.from_pandas(df)
                df = global_indicator_manager.calculate_indicator(pl_df, 'expma', return_polars=True, windows=[12, 50])
            if 'expma12' in df.columns and 'expma50' in df.columns:
                expma12_data = df['expma12'].to_numpy().astype(np.float64)
                expma50_data = df['expma50'].to_numpy().astype(np.float64)
                mask12 = ~np.isnan(expma12_data)
                mask50 = ~np.isnan(expma50_data)
                # 绘制EXPMA12（黄色）
                if np.any(mask12):
                    plot_widget.plot(x[mask12], expma12_data[mask12], pen=pg.mkPen('#FFFF00', width=1.5), name='EXPMA12')
                # 绘制EXPMA50（紫色）
                if np.any(mask50):
                    plot_widget.plot(x[mask50], expma50_data[mask50], pen=pg.mkPen('#FF00FF', width=1.5), name='EXPMA50')
                # 设置Y轴范围
                valid_data = []
                if np.any(mask12):
                    valid_data.extend(expma12_data[mask12])
                if np.any(mask50):
                    valid_data.extend(expma50_data[mask50])
                if valid_data:
                    min_val = np.min(valid_data)
                    max_val = np.max(valid_data)
                    range_val = max_val - min_val
                    if range_val == 0:
                        range_val = abs(max_val) * 0.1 if max_val != 0 else 1
                    plot_widget.setYRange(min_val - range_val * 0.1, max_val + range_val * 0.1)
        except Exception as e:
            logger.exception(f"渲染EXPMA失败: {e}")
        return df

    def render_bbi(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染BBI指标"""
        try:
            if 'bbi' not in df.columns:
                from src.tech_analysis.indicator_manager import global_indicator_manager
                import polars as pl
                # 确保数据是polars DataFrame
                if hasattr(df, 'to_pandas'):
                    pl_df = df
                else:
                    pl_df = pl.from_pandas(df)
                df = global_indicator_manager.calculate_indicator(pl_df, 'bbi', return_polars=True)
            if 'bbi' in df.columns:
                bbi_data = df['bbi'].to_numpy().astype(np.float64)
                mask = ~np.isnan(bbi_data)
                if np.any(mask):
                    # 设置Y轴范围
                    valid_data = bbi_data[mask]
                    if len(valid_data) > 0:
                        min_val = np.min(valid_data)
                        max_val = np.max(valid_data)
                        range_val = max_val - min_val
                        if range_val == 0:
                            range_val = abs(max_val) * 0.1 if max_val != 0 else 1
                        plot_widget.setYRange(min_val - range_val * 0.1, max_val + range_val * 0.1)
                    # 绘制BBI线（橙色）
                    plot_widget.plot(x[mask], bbi_data[mask], pen=pg.mkPen('#FFA500', width=2), name='BBI')
        except Exception as e:
            logger.exception(f"渲染BBI失败: {e}")
        return df

    def render_hsl(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染HSL（换手率）指标"""
        try:
            if 'hsl' not in df.columns:
                from src.tech_analysis.indicator_manager import global_indicator_manager
                import polars as pl
                # 确保数据是polars DataFrame
                if hasattr(df, 'to_pandas'):
                    pl_df = df
                else:
                    pl_df = pl.from_pandas(df)
                df = global_indicator_manager.calculate_indicator(pl_df, 'hsl', return_polars=True)
            if 'hsl' in df.columns:
                hsl_data = df['hsl'].to_numpy().astype(np.float64)
                mask = ~np.isnan(hsl_data)
                if np.any(mask):
                    # 设置Y轴范围
                    valid_data = hsl_data[mask]
                    if len(valid_data) > 0:
                        max_val = np.max(valid_data)
                        plot_widget.setYRange(0, max(max_val * 1.1, 20))
                    # 根据换手率大小设置颜色
                    for i in range(len(x)):
                        if mask[i]:
                            if hsl_data[i] > 10:
                                color = 'r'  # 高换手率 - 红色
                            elif hsl_data[i] > 5:
                                color = '#FFA500'  # 中等换手率 - 橙色
                            else:
                                color = '#00BFFF'  # 低换手率 - 蓝色
                            plot_widget.plot([x[i], x[i]], [0, hsl_data[i]], pen=pg.mkPen(color, width=2))
                    # 绘制HSL_MA5和HSL_MA10
                    if 'hsl_ma5' in df.columns:
                        hsl_ma5 = df['hsl_ma5'].to_numpy().astype(np.float64)
                        mask_ma5 = ~np.isnan(hsl_ma5)
                        if np.any(mask_ma5):
                            plot_widget.plot(x[mask_ma5], hsl_ma5[mask_ma5], pen=pg.mkPen('#FFA500', width=1), name='MA5')
                    if 'hsl_ma10' in df.columns:
                        hsl_ma10 = df['hsl_ma10'].to_numpy().astype(np.float64)
                        mask_ma10 = ~np.isnan(hsl_ma10)
                        if np.any(mask_ma10):
                            plot_widget.plot(x[mask_ma10], hsl_ma10[mask_ma10], pen=pg.mkPen('#FF00FF', width=1), name='MA10')
                    # 添加参考线
                    plot_widget.addLine(y=5, pen=pg.mkPen('#666666', width=1, style=pg.QtCore.Qt.DashLine))
                    plot_widget.addLine(y=10, pen=pg.mkPen('#FF0000', width=1, style=pg.QtCore.Qt.DashLine))
        except Exception as e:
            logger.exception(f"渲染HSL失败: {e}")
        return df

    def render_lb(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染LB（量比）指标"""
        try:
            if 'lb' not in df.columns:
                from src.tech_analysis.indicator_manager import global_indicator_manager
                import polars as pl
                # 确保数据是polars DataFrame
                if hasattr(df, 'to_pandas'):
                    pl_df = df
                else:
                    pl_df = pl.from_pandas(df)
                df = global_indicator_manager.calculate_indicator(pl_df, 'lb', return_polars=True, period=5)
            if 'lb' in df.columns:
                lb_data = df['lb'].to_numpy().astype(np.float64)
                mask = ~np.isnan(lb_data)
                if np.any(mask):
                    # 设置Y轴范围
                    valid_data = lb_data[mask]
                    if len(valid_data) > 0:
                        min_val = np.min(valid_data)
                        max_val = np.max(valid_data)
                        plot_widget.setYRange(max(0, min_val * 0.8), max(3, max_val * 1.1))
                    # 根据量比大小设置颜色
                    for i in range(len(x)):
                        if mask[i]:
                            if lb_data[i] > 2:
                                color = 'r'  # 高量比 - 红色
                            elif lb_data[i] > 1.5:
                                color = '#FFA500'  # 较高量比 - 橙色
                            elif lb_data[i] > 1:
                                color = '#00FF7F'  # 正常量比 - 绿色
                            elif lb_data[i] > 0.5:
                                color = '#00BFFF'  # 较低量比 - 蓝色
                            else:
                                color = '#808080'  # 低量比 - 灰色
                            plot_widget.plot([x[i], x[i]], [0, lb_data[i]], pen=pg.mkPen(color, width=2))
                    # 添加参考线
                    plot_widget.addLine(y=1, pen=pg.mkPen('#FFFFFF', width=1.5))
                    plot_widget.addLine(y=0.5, pen=pg.mkPen('#666666', width=1, style=pg.QtCore.Qt.DashLine))
                    plot_widget.addLine(y=1.5, pen=pg.mkPen('#666666', width=1, style=pg.QtCore.Qt.DashLine))
                    plot_widget.addLine(y=2, pen=pg.mkPen('#FF0000', width=1, style=pg.QtCore.Qt.DashLine))
        except Exception as e:
            logger.exception(f"渲染LB失败: {e}")
        return df

    def render_cyc(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染CYC（成本均线）指标"""
        try:
            if 'cyc5' not in df.columns or 'cyc13' not in df.columns:
                from src.tech_analysis.indicator_manager import global_indicator_manager
                import polars as pl
                # 确保数据是polars DataFrame
                if hasattr(df, 'to_pandas'):
                    pl_df = df
                else:
                    pl_df = pl.from_pandas(df)
                df = global_indicator_manager.calculate_indicator(pl_df, 'cyc', return_polars=True, windows=[5, 13, 34])
            # 绘制各条成本均线
            cyc_configs = [
                ('cyc5', 'CYC5', '#FFFF00'),      # 黄色 - 短期
                ('cyc13', 'CYC13', '#FFA500'),    # 橙色 - 中期
                ('cyc34', 'CYC34', '#FF00FF'),    # 紫色 - 长期
                ('cyc_inf', 'CYC∞', '#00FFFF'),   # 青色 - 无穷
            ]
            valid_data = []
            for col_name, name, color in cyc_configs:
                if col_name in df.columns:
                    cyc_data = df[col_name].to_numpy().astype(np.float64)
                    mask = ~np.isnan(cyc_data)
                    if np.any(mask):
                        plot_widget.plot(x[mask], cyc_data[mask], pen=pg.mkPen(color, width=1.5), name=name)
                        valid_data.extend(cyc_data[mask])
            # 设置Y轴范围
            if valid_data:
                min_val = np.min(valid_data)
                max_val = np.max(valid_data)
                range_val = max_val - min_val
                if range_val == 0:
                    range_val = abs(max_val) * 0.1 if max_val != 0 else 1
                plot_widget.setYRange(min_val - range_val * 0.1, max_val + range_val * 0.1)
        except Exception as e:
            logger.exception(f"渲染CYC失败: {e}")
        return df

    def render_cys(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染CYS（市场盈亏）指标"""
        try:
            if 'cys' not in df.columns:
                from src.tech_analysis.indicator_manager import global_indicator_manager
                import polars as pl
                # 确保数据是polars DataFrame
                if hasattr(df, 'to_pandas'):
                    pl_df = df
                else:
                    pl_df = pl.from_pandas(df)
                df = global_indicator_manager.calculate_indicator(pl_df, 'cys', return_polars=True, cyc_window=13)
            if 'cys' in df.columns:
                cys_data = df['cys'].to_numpy().astype(np.float64)
                mask = ~np.isnan(cys_data)
                if np.any(mask):
                    # 设置Y轴范围
                    valid_data = cys_data[mask]
                    if len(valid_data) > 0:
                        min_val = np.min(valid_data)
                        max_val = np.max(valid_data)
                        range_val = max_val - min_val
                        if range_val == 0:
                            range_val = abs(max_val) * 0.1 if max_val != 0 else 1
                        plot_widget.setYRange(min_val - range_val * 0.1, max_val + range_val * 0.1)
                    # 根据盈亏设置颜色（红色盈利，绿色亏损）
                    for i in range(len(x)):
                        if mask[i]:
                            color = 'r' if cys_data[i] >= 0 else 'g'
                            plot_widget.plot([x[i], x[i]], [0, cys_data[i]], pen=pg.mkPen(color, width=2))
                    # 绘制CYS_MA5
                    if 'cys_ma5' in df.columns:
                        cys_ma5 = df['cys_ma5'].to_numpy().astype(np.float64)
                        mask_ma5 = ~np.isnan(cys_ma5)
                        if np.any(mask_ma5):
                            plot_widget.plot(x[mask_ma5], cys_ma5[mask_ma5], pen=pg.mkPen('w', width=1), name='MA5')
                    # 添加零线
                    plot_widget.addLine(y=0, pen=pg.mkPen('#666666', width=1))
        except Exception as e:
            logger.exception(f"渲染CYS失败: {e}")
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
            # 新增指标
            'EXPMA': self.render_expma,
            'BBI': self.render_bbi,
            'HSL': self.render_hsl,
            'LB': self.render_lb,
            'CYC': self.render_cyc,
            'CYS': self.render_cys,
        }
        
        renderer = renderers.get(indicator_name)
        if renderer:
            df = renderer(plot_widget, df, x)
        else:
            logger.warning(f"未知的指标类型: {indicator_name}")

        return df


# 导入Qt常量
from PySide6.QtCore import Qt
