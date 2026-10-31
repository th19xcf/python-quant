#!/usr/bin/env python3

"""
趋势类指标渲染 Mixin
"""

from typing import Any

import numpy as np
import pyqtgraph as pg
from loguru import logger
from PySide6.QtCore import Qt

from ._arrays import as_f64


class TrendRenderMixin:
    """趋势类指标渲染"""

    main_window: Any

    def render_ma_lines(self, plot_widget: Any, df: Any, x: np.ndarray):
        """
        渲染移动平均线

        Args:
            plot_widget: 图表控件
            df: 数据
            x: x轴坐标
        """
        try:
            # 检查df是否为None
            if df is None:
                logger.warning("render_ma_lines: df is None, skipping")
                return

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
                if hasattr(df, 'columns') and col_name in df.columns:
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

        except (OSError, RuntimeError, ValueError) as e:
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
        ma_data = as_f64(df[col_name])
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
        if df is None:
            logger.warning("_save_ma_data: df is None, skipping")
            return

        self.main_window.ma_data = {
            'MA5': df['ma5'].to_numpy() if hasattr(df, 'columns') and 'ma5' in df.columns else np.array([]),
            'MA10': df['ma10'].to_numpy() if hasattr(df, 'columns') and 'ma10' in df.columns else np.array([]),
            'MA20': df['ma20'].to_numpy() if hasattr(df, 'columns') and 'ma20' in df.columns else np.array([]),
            'MA60': df['ma60'].to_numpy() if hasattr(df, 'columns') and 'ma60' in df.columns else np.array([]),
        }

        self.main_window.ma_colors = {
            'MA10': 'cyan',
            'MA20': 'red',
            'MA60': '#00FF00'
        }

    def render_boll(self, plot_widget: Any, df: Any, x: np.ndarray):
        """
        渲染BOLL指标

        Args:
            plot_widget: 图表控件
            df: 数据
            x: x轴坐标
        """
        try:
            # 检查df是否为None
            if df is None:
                logger.warning("render_boll: df is None, skipping")
                return df

            logger.debug(f"渲染BOLL，数据列: {df.columns if hasattr(df, 'columns') else 'N/A'}")
            if not hasattr(df, 'columns') or 'mb' not in df.columns or 'up' not in df.columns or 'dn' not in df.columns:
                logger.warning("BOLL数据列不存在，跳过渲染（应由渲染前批量计算提供）")
                return df

            mb_data = as_f64(df['mb'])
            up_data = as_f64(df['up'])
            dn_data = as_f64(df['dn'])

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

        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染BOLL失败: {e}")
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
                logger.warning("DMI数据列不存在，跳过渲染（应由渲染前批量计算提供）")
                return df

            pdi_data = as_f64(df['pdi'])
            ndi_data = as_f64(df['ndi'])

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
                adx_data = as_f64(df['adx'])
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
                adxr_data = as_f64(df['adxr'])
                adxr_mask = ~np.isnan(adxr_data)
                if np.any(adxr_mask):
                    plot_widget.plot(
                        x[adxr_mask],
                        adxr_data[adxr_mask],
                        pen=pg.mkPen(pg.mkColor(0, 255, 0), width=1),
                        name='ADXR'
                    )

            # 添加参考线
            plot_widget.addLine(y=20, pen=pg.mkPen('#666666', width=1, style=Qt.PenStyle.DotLine))
            plot_widget.addLine(y=50, pen=pg.mkPen('#666666', width=1, style=Qt.PenStyle.DotLine))

        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染DMI失败: {e}")
        return df

    def render_dma(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染DMA指标"""
        try:
            # DMA指标已经在数据准备阶段计算完成，直接使用
            if 'dma' not in df.columns or 'ama' not in df.columns:
                logger.warning("DMA数据列不存在，跳过渲染")
                return df

            dma_data = as_f64(df['dma'])
            ama_data = as_f64(df['ama'])
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
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染DMA失败: {e}")
        return df

    def render_sar(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染SAR指标"""
        try:
            logger.debug(f"渲染SAR指标，数据列: {df.columns if hasattr(df, 'columns') else 'N/A'}")
            logger.debug(f"输入数据行数: {len(df)}, x坐标长度: {len(x)}")

            if 'sar' in df.columns:
                sar_data = as_f64(df['sar'])
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
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染SAR失败: {e}")
        return df

    def render_expma(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染EXPMA指标"""
        try:
            if 'expma12' not in df.columns or 'expma50' not in df.columns:
                logger.warning("EXPMA数据列不存在，跳过渲染（应由渲染前批量计算提供）")
            if 'expma12' in df.columns and 'expma50' in df.columns:
                expma12_data = as_f64(df['expma12'])
                expma50_data = as_f64(df['expma50'])
                mask12 = ~np.isnan(expma12_data)
                mask50 = ~np.isnan(expma50_data)
                # 绘制EXPMA12（黄色）
                if np.any(mask12):
                    plot_widget.plot(x[mask12], expma12_data[mask12], pen=pg.mkPen('#FFFF00', width=1.5), name='EXPMA12')
                # 绘制EXPMA50（紫色）
                if np.any(mask50):
                    plot_widget.plot(x[mask50], expma50_data[mask50], pen=pg.mkPen('#FF00FF', width=1.5), name='EXPMA50')
                # 设置Y轴范围
                valid_data: list[float] = []
                if np.any(mask12):
                    valid_data.extend(expma12_data[mask12])
                if np.any(mask50):
                    valid_data.extend(expma50_data[mask50])
                if valid_data:
                    min_val = np.min(valid_data)
                    max_val = np.max(valid_data)
                    range_val: float = max_val - min_val
                    if range_val == 0:
                        range_val = abs(max_val) * 0.1 if max_val != 0 else 1
                    plot_widget.setYRange(min_val - range_val * 0.1, max_val + range_val * 0.1)
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染EXPMA失败: {e}")
        return df

    def render_bbi(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染BBI指标"""
        try:
            if 'bbi' not in df.columns:
                logger.warning("BBI数据列不存在，跳过渲染（应由渲染前批量计算提供）")
            if 'bbi' in df.columns:
                bbi_data = as_f64(df['bbi'])
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
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染BBI失败: {e}")
        return df

    def render_cyc(self, plot_widget: Any, df: Any, x: np.ndarray):
        """渲染CYC（成本均线）指标"""
        try:
            if 'cyc5' not in df.columns or 'cyc13' not in df.columns:
                logger.warning("CYC数据列不存在，跳过渲染（应由渲染前批量计算提供）")
            # 绘制各条成本均线
            cyc_configs = [
                ('cyc5', 'CYC5', '#FFFF00'),      # 黄色 - 短期
                ('cyc13', 'CYC13', '#FFA500'),    # 橙色 - 中期
                ('cyc34', 'CYC34', '#FF00FF'),    # 紫色 - 长期
                ('cyc_inf', 'CYC∞', '#00FFFF'),   # 青色 - 无穷
            ]
            valid_data: list[float] = []
            for col_name, name, color in cyc_configs:
                if col_name in df.columns:
                    cyc_data = as_f64(df[col_name])
                    mask = ~np.isnan(cyc_data)
                    if np.any(mask):
                        plot_widget.plot(x[mask], cyc_data[mask], pen=pg.mkPen(color, width=1.5), name=name)
                        valid_data.extend(cyc_data[mask])
            # 设置Y轴范围
            if valid_data:
                min_val = np.min(valid_data)
                max_val = np.max(valid_data)
                range_val: float = max_val - min_val
                if range_val == 0:
                    range_val = abs(max_val) * 0.1 if max_val != 0 else 1
                plot_widget.setYRange(min_val - range_val * 0.1, max_val + range_val * 0.1)
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染CYC失败: {e}")
        return df

