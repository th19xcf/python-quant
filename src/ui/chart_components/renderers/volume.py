#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
成交量类指标渲染 Mixin
"""

import pyqtgraph as pg
import numpy as np
from typing import Any
from loguru import logger


class VolumeRenderMixin:
    """成交量类指标渲染"""

    def render_volume(self, plot_widget: Any, df: Any, x: np.ndarray):
        """
        渲染成交量指标
        
        Args:
            plot_widget: 图表控件
            df: 数据
            x: x轴坐标
        """
        try:
            # 检查df是否为None
            if df is None:
                logger.warning("render_volume: df is None, skipping")
                return df
            
            if not hasattr(df, 'columns') or 'volume' not in df.columns:
                logger.warning(f"render_volume: volume column not found, columns: {df.columns if hasattr(df, 'columns') else 'N/A'}")
                return df
            
            volumes = df['volume'].to_numpy()
            if len(volumes) == 0:
                logger.warning("render_volume: volumes array is empty")
                return df
            
            # 打印成交量数据的统计信息
            logger.debug(f"render_volume: volumes shape: {volumes.shape}")
            logger.debug(f"render_volume: volumes min: {volumes.min()}")
            logger.debug(f"render_volume: volumes max: {volumes.max()}")
            logger.debug(f"render_volume: volumes mean: {volumes.mean()}")
            logger.debug(f"render_volume: volumes std: {volumes.std()}")
            logger.debug(f"render_volume: volumes sample: {volumes[:5]}")
            
            # 计算Y轴范围
            self._set_volume_y_range(plot_widget, volumes)
            
            # 禁用科学计数法
            y_axis = plot_widget.getAxis('left')
            y_axis.enableAutoSIPrefix(False)
            
            # 绘制成交量柱状图
            self._draw_volume_bars(plot_widget, x, volumes, df)
            
            # 绘制成交量MA线
            self._draw_volume_ma_lines(plot_widget, x, df)
            
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染成交量失败: {e}")
        return df

    def _set_volume_y_range(self, plot_widget: Any, volumes: np.ndarray):
        """设置成交量Y轴范围"""
        if len(volumes) == 0:
            plot_widget.setYRange(0, 100)
            return
        
        # 打印成交量数据的统计信息
        logger.debug(f"_set_volume_y_range: volumes shape: {volumes.shape}")
        logger.debug(f"_set_volume_y_range: volumes min: {volumes.min()}")
        logger.debug(f"_set_volume_y_range: volumes max: {volumes.max()}")
        logger.debug(f"_set_volume_y_range: volumes mean: {volumes.mean()}")
        
        # 计算成交量数据的范围
        volume_min = volumes.min()
        volume_max = volumes.max()
        volume_range = volume_max - volume_min
        
        logger.debug(f"_set_volume_y_range: volume_range: {volume_range}")
        
        if volume_range > 0:
            # 使用实际的最小值和最大值，但是缩小范围以增强视觉差异
            # 这样可以让柱体之间的差异更加明显
            y_min = max(0, volume_min - volume_range * 0.1)  # 底部留10%空间
            y_max = volume_max + volume_range * 0.1  # 顶部留10%空间
            
            logger.debug(f"_set_volume_y_range: y_min: {y_min}, y_max: {y_max}")
            
            plot_widget.setYRange(y_min, y_max)
        else:
            # 所有成交量值相同，使用默认范围
            plot_widget.setYRange(0, volume_max * 1.2)

    def _draw_volume_bars(self, plot_widget: Any, x: np.ndarray, volumes: np.ndarray, df: Any):
        """绘制成交量柱状图（单 BarGraphItem + brushes 数组批量绘制）"""
        closes = df['close'].to_numpy() if 'close' in df.columns else None
        opens = df['open'].to_numpy() if 'open' in df.columns else None

        # 确保只绘制与x数组长度匹配的柱体
        max_count = min(len(x), len(volumes))

        logger.debug(f"_draw_volume_bars: x shape: {x.shape}")
        logger.debug(f"_draw_volume_bars: volumes shape: {volumes.shape}")
        logger.debug(f"_draw_volume_bars: max_count: {max_count}")
        logger.debug(f"_draw_volume_bars: volumes sample: {volumes[:5]}")

        # 向量化生成每根柱体颜色：红涨绿跌，无开收盘数据时灰色
        brushes = np.full(max_count, '#C0C0C0', dtype=object)
        if closes is not None and opens is not None:
            n = min(max_count, len(closes), len(opens))
            brushes[:n] = np.where(closes[:n] >= opens[:n], '#FF0000', '#00FF00')

        bar = pg.BarGraphItem(
            x=x[:max_count],
            height=volumes[:max_count],
            width=0.8,
            brushes=list(brushes),
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
                # 确保只绘制与x数组长度匹配的数据
                if len(ma_data) > len(x):
                    ma_data = ma_data[-len(x):]
                ma_mask = ~np.isnan(ma_data)
                if np.any(ma_mask):
                    # 确保mask与x长度匹配
                    if len(ma_mask) > len(x):
                        ma_mask = ma_mask[-len(x):]
                    plot_widget.plot(
                        x[ma_mask], 
                        ma_data[ma_mask], 
                        pen=pg.mkPen(color, width=1)
                    )

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
            
            # 如果没有VR数据，跳过渲染（应由渲染前批量计算提供）
            if 'vr' not in df.columns:
                logger.warning("VR数据列不存在，跳过渲染（应由渲染前批量计算提供）")
            
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

        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染VR失败: {e}")
        return df

    def render_obv(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染OBV指标"""
        try:
            if 'obv' not in df.columns:
                logger.warning("OBV数据列不存在，跳过渲染（应由渲染前批量计算提供）")
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
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染OBV失败: {e}")
        return df

    def render_asi(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染ASI指标"""
        try:
            if 'asi' not in df.columns:
                logger.warning("ASI数据列不存在，跳过渲染（应由渲染前批量计算提供）")
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
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染ASI失败: {e}")
        return df

    def render_emv(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染EMV指标"""
        try:
            if 'emv' not in df.columns:
                logger.warning("EMV数据列不存在，跳过渲染（应由渲染前批量计算提供）")
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
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染EMV失败: {e}")
        return df

    def render_mcst(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染MCST指标"""
        try:
            if 'mcst' not in df.columns:
                logger.warning("MCST数据列不存在，跳过渲染（应由渲染前批量计算提供）")
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
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染MCST失败: {e}")
        return df

    def render_vol_tdx(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染VOL-TDX指标"""
        try:
            if 'vol_tdx' not in df.columns:
                logger.warning("VOL-TDX数据列不存在，跳过渲染")
            if 'vol_tdx' in df.columns:
                vol_tdx_data = df['vol_tdx'].to_numpy().astype(np.float64)
                mask = ~np.isnan(vol_tdx_data)
                if np.any(mask):
                    # 根据涨跌决定颜色（向量化）
                    close_data = df['close'].to_numpy().astype(np.float64)
                    prev_close = np.roll(close_data, 1)
                    prev_close[0] = close_data[0]
                    colors = np.where(close_data >= prev_close, 'r', 'g')
                    # 批量绘制柱状竖线（按颜色分组的 PlotCurveItem）
                    self._draw_colored_vertical_lines(plot_widget, x[mask], vol_tdx_data[mask], colors[mask])
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染VOL-TDX失败: {e}")
        return df

    def render_lb(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染LB（量比）指标"""
        try:
            if 'lb' not in df.columns:
                logger.warning("LB数据列不存在，跳过渲染（应由渲染前批量计算提供）")
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
                    # 根据量比大小设置颜色（向量化）并批量绘制竖线
                    colors = np.select(
                        [lb_data > 2, lb_data > 1.5, lb_data > 1, lb_data > 0.5],
                        ['r', '#FFA500', '#00FF7F', '#00BFFF'],
                        default='#808080'
                    )
                    self._draw_colored_vertical_lines(plot_widget, x[mask], lb_data[mask], colors[mask])
                    # 添加参考线
                    plot_widget.addLine(y=1, pen=pg.mkPen('#FFFFFF', width=1.5))
                    plot_widget.addLine(y=0.5, pen=pg.mkPen('#666666', width=1, style=pg.QtCore.Qt.DashLine))
                    plot_widget.addLine(y=1.5, pen=pg.mkPen('#666666', width=1, style=pg.QtCore.Qt.DashLine))
                    plot_widget.addLine(y=2, pen=pg.mkPen('#FF0000', width=1, style=pg.QtCore.Qt.DashLine))
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染LB失败: {e}")
        return df

    def render_cys(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染CYS（市场盈亏）指标"""
        try:
            if 'cys' not in df.columns:
                logger.warning("CYS数据列不存在，跳过渲染（应由渲染前批量计算提供）")
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
                    # 根据盈亏设置颜色（红色盈利，绿色亏损，向量化）并批量绘制竖线
                    colors = np.where(cys_data >= 0, 'r', 'g')
                    self._draw_colored_vertical_lines(plot_widget, x[mask], cys_data[mask], colors[mask])
                    # 绘制CYS_MA5
                    if 'cys_ma5' in df.columns:
                        cys_ma5 = df['cys_ma5'].to_numpy().astype(np.float64)
                        mask_ma5 = ~np.isnan(cys_ma5)
                        if np.any(mask_ma5):
                            plot_widget.plot(x[mask_ma5], cys_ma5[mask_ma5], pen=pg.mkPen('w', width=1), name='MA5')
                    # 添加零线
                    plot_widget.addLine(y=0, pen=pg.mkPen('#666666', width=1))
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染CYS失败: {e}")
        return df

    def _draw_colored_vertical_lines(self, plot_widget: Any, x: np.ndarray, heights: np.ndarray,
                                     colors: np.ndarray, width: int = 2):
        """批量绘制按颜色分组的竖线（每根从 0 画到 heights[i]）。

        逐根 plot() 会产生 O(N) 个 PlotDataItem；改为按颜色分组后，每组用
        NaN 分隔的单个 PlotCurveItem(connect='finite') 一次绘制，
        scene item 数从 O(K线数) 降为 O(颜色数)。
        """
        for color in np.unique(colors):
            idx = colors == color
            m = int(idx.sum())
            xs = np.empty(m * 3)
            ys = np.empty(m * 3)
            xs[0::3] = x[idx]
            xs[1::3] = x[idx]
            xs[2::3] = np.nan
            ys[0::3] = 0.0
            ys[1::3] = heights[idx]
            ys[2::3] = np.nan
            curve = pg.PlotCurveItem(xs, ys, pen=pg.mkPen(str(color), width=width), connect='finite')
            plot_widget.addItem(curve)

