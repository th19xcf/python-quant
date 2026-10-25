#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
震荡类指标渲染 Mixin
"""

import pyqtgraph as pg
import numpy as np
from typing import Any
from PySide6.QtCore import Qt
from loguru import logger


class OscillatorRenderMixin:
    """震荡类指标渲染"""

    def render_kdj(self, plot_widget: Any, df: Any, x: np.ndarray):
        """
        渲染KDJ指标
        
        Args:
            plot_widget: 图表控件
            df: 数据
            x: x轴坐标
        """
        try:
            # 检查df是否为None
            if df is None:
                logger.warning("render_kdj: df is None, skipping")
                return df
            
            if not hasattr(df, 'columns') or 'k' not in df.columns or 'd' not in df.columns:
                return df
            
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
            if hasattr(df, 'columns') and 'j' in df.columns:
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

        except (OSError, RuntimeError, ValueError) as e:
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
            # 检查df是否为None
            if df is None:
                logger.warning("render_macd: df is None, skipping")
                return df
            
            logger.debug(f"渲染MACD，数据列: {df.columns if hasattr(df, 'columns') else 'N/A'}")
            if not hasattr(df, 'columns') or 'macd' not in df.columns or 'macd_signal' not in df.columns:
                logger.warning("MACD数据列不存在，跳过渲染（应由渲染前批量计算提供）")
                return df
            
            macd_data = df['macd'].to_numpy().astype(np.float64)
            signal_data = df['macd_signal'].to_numpy().astype(np.float64)
            
            # 设置Y轴范围
            if hasattr(df, 'columns') and 'macd_hist' in df.columns:
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
            if hasattr(df, 'columns') and 'macd_hist' in df.columns:
                self._draw_macd_histogram(plot_widget, x, df['macd_hist'].to_numpy())
            
            # 添加零轴
            plot_widget.addLine(y=0, pen=pg.mkPen('#666666', width=1))

        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染MACD失败: {e}")
        return df

    def _draw_macd_histogram(self, plot_widget: Any, x: np.ndarray, hist_data: np.ndarray):
        """绘制MACD柱状图（单 BarGraphItem + brushes 数组批量绘制）"""
        mask = ~np.isnan(hist_data)
        if not np.any(mask):
            return
        xv = x[mask]
        hv = hist_data[mask]
        # 正值红色，负值绿色；y0/y1 直接给出柱体上下沿，等价于原逐柱 setOpts
        y0 = np.minimum(hv, 0.0)
        y1 = np.maximum(hv, 0.0)
        brushes = np.where(hv >= 0, '#FF0000', '#00FF00')
        bar = pg.BarGraphItem(
            x0=xv - 0.4, x1=xv + 0.4,
            y0=y0, y1=y1,
            brushes=list(brushes),
            pen=None
        )
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
            # 检查df是否为None
            if df is None:
                logger.warning("render_rsi: df is None, skipping")
                return df
            
            if not hasattr(df, 'columns') or 'rsi14' not in df.columns:
                return df
            
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

        except (OSError, RuntimeError, ValueError) as e:
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
            # 检查df是否为None
            if df is None:
                logger.warning("render_wr: df is None, skipping")
                return df
            
            # 优先使用wr1和wr2（通达信风格）
            if hasattr(df, 'columns') and 'wr1' in df.columns and 'wr2' in df.columns:
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
            elif hasattr(df, 'columns') and 'wr' in df.columns:
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

        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染WR失败: {e}")
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
                logger.warning("BRAR数据列不存在，跳过渲染（应由渲染前批量计算提供）")
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

        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染BRAR失败: {e}")
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
                logger.warning("TRIX数据列不存在，跳过渲染（应由渲染前批量计算提供）")
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

        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染TRIX失败: {e}")
        return df

    def render_cci(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染CCI指标"""
        try:
            if 'cci' not in df.columns:
                logger.warning("CCI数据列不存在，跳过渲染（应由渲染前批量计算提供）")
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
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染CCI失败: {e}")
        return df

    def render_roc(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染ROC指标"""
        try:
            if 'roc' not in df.columns:
                logger.warning("ROC数据列不存在，跳过渲染（应由渲染前批量计算提供）")
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
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染ROC失败: {e}")
        return df

    def render_mtm(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染MTM指标"""
        try:
            if 'mtm' not in df.columns:
                logger.warning("MTM数据列不存在，跳过渲染（应由渲染前批量计算提供）")
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
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染MTM失败: {e}")
        return df

    def render_psy(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染PSY指标"""
        try:
            if 'psy' not in df.columns:
                logger.warning("PSY数据列不存在，跳过渲染（应由渲染前批量计算提供）")
            if 'psy' in df.columns:
                psy_data = df['psy'].to_numpy().astype(np.float64)
                mask = ~np.isnan(psy_data)
                if np.any(mask):
                    plot_widget.plot(x[mask], psy_data[mask], pen=pg.mkPen('w', width=1), name='PSY')
                plot_widget.setYRange(0, 100)
                y_axis = plot_widget.getAxis('left')
                y_axis.enableAutoSIPrefix(False)
                plot_widget.addLine(y=50, pen=pg.mkPen('#666666', width=1, style=Qt.DotLine))
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染PSY失败: {e}")
        return df

    def render_fsl(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染FSL指标"""
        try:
            if 'swl' not in df.columns or 'sws' not in df.columns:
                logger.warning("FSL数据列不存在，跳过渲染（应由渲染前批量计算提供）")
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
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染FSL失败: {e}")
        return df

    def render_cr(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染CR指标"""
        try:
            if 'cr' not in df.columns:
                logger.warning("CR数据列不存在，跳过渲染（应由渲染前批量计算提供）")
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
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染CR失败: {e}")
        return df

    def render_hsl(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染HSL（换手率）指标"""
        try:
            if 'hsl' not in df.columns:
                logger.warning("HSL数据列不存在，跳过渲染（应由渲染前批量计算提供）")
            if 'hsl' in df.columns:
                hsl_data = df['hsl'].to_numpy().astype(np.float64)
                mask = ~np.isnan(hsl_data)
                if np.any(mask):
                    # 设置Y轴范围
                    valid_data = hsl_data[mask]
                    if len(valid_data) > 0:
                        max_val = np.max(valid_data)
                        plot_widget.setYRange(0, max(max_val * 1.1, 20))
                    # 根据换手率大小设置颜色（向量化）并批量绘制竖线
                    colors = np.select(
                        [hsl_data > 10, hsl_data > 5],
                        ['r', '#FFA500'],
                        default='#00BFFF'
                    )
                    self._draw_colored_vertical_lines(plot_widget, x[mask], hsl_data[mask], colors[mask])
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
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染HSL失败: {e}")
        return df

    def render_abi(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染ABI（绝对广量指标）"""
        try:
            if 'abi10' not in df.columns:
                logger.warning("ABI数据列不存在，跳过渲染（应由渲染前批量计算提供）")
            if 'abi10' in df.columns:
                abi_data = df['abi10'].to_numpy().astype(np.float64)
                mask = ~np.isnan(abi_data)
                if np.any(mask):
                    plot_widget.plot(x[mask], abi_data[mask], pen=pg.mkPen('#00FF00', width=1.5), name='ABI')
                plot_widget.addLine(y=50, pen=pg.mkPen('#666666', width=1, style=Qt.DashLine))
                plot_widget.setYRange(0, 100)
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染ABI失败: {e}")
        return df

    def render_adl(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染ADL（腾落指标）"""
        try:
            if 'adl' not in df.columns:
                logger.warning("ADL数据列不存在，跳过渲染（应由渲染前批量计算提供）")
            if 'adl' in df.columns:
                adl_data = df['adl'].to_numpy().astype(np.float64)
                mask = ~np.isnan(adl_data)
                if np.any(mask):
                    plot_widget.plot(x[mask], adl_data[mask], pen=pg.mkPen('#00FFFF', width=1.5), name='ADL')
                plot_widget.addLine(y=0, pen=pg.mkPen('#666666', width=1, style=Qt.DashLine))
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染ADL失败: {e}")
        return df

    def render_adr(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染ADR（涨跌比率）"""
        try:
            if 'adr10' not in df.columns:
                logger.warning("ADR数据列不存在，跳过渲染（应由渲染前批量计算提供）")
            if 'adr10' in df.columns:
                adr_data = df['adr10'].to_numpy().astype(np.float64)
                mask = ~np.isnan(adr_data)
                if np.any(mask):
                    plot_widget.plot(x[mask], adr_data[mask], pen=pg.mkPen('#FF00FF', width=1.5), name='ADR')
                plot_widget.addLine(y=100, pen=pg.mkPen('#666666', width=1, style=Qt.DashLine))
                plot_widget.setYRange(0, 200)
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染ADR失败: {e}")
        return df

    def render_obos(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染OBOS（超买超卖指标）"""
        try:
            if 'obos10' not in df.columns:
                logger.warning("OBOS数据列不存在，跳过渲染（应由渲染前批量计算提供）")
            if 'obos10' in df.columns:
                obos_data = df['obos10'].to_numpy().astype(np.float64)
                mask = ~np.isnan(obos_data)
                if np.any(mask):
                    plot_widget.plot(x[mask], obos_data[mask], pen=pg.mkPen('#FFFF00', width=1.5), name='OBOS')
                plot_widget.addLine(y=0, pen=pg.mkPen('#666666', width=1, style=Qt.DashLine))
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染OBOS失败: {e}")
        return df

