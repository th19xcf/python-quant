#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
主窗口副图指标 Mixin：叠加/副图指标渲染、指标数据保存与 MA 值显示。
"""

import pyqtgraph as pg
import polars as pl
import numpy as np

from src.utils.logger import logger
from src.ui.chart_components import ChartUIBuilder
from src.ui.utils.chart_utils import ChartUtils


class MainWindowIndicatorWindowMixin:
    def _update_ma_values_display(self, df: pl.DataFrame, dates: list):
        """
        更新标题栏显示最后一个交易日的均线数据和主图指标
        
        Args:
            df: 股票数据DataFrame
            dates: 日期列表
        """
        try:
            if not hasattr(self, 'ma_values_label') or self.ma_values_label is None:
                return
            
            if len(dates) == 0:
                return
            
            # 获取最后一个交易日的数据
            last_date = dates[-1]
            
            # 获取均线数据
            ma_values = {}
            ma_columns = {
                'MA5': ('ma5', 'white'),
                'MA10': ('ma10', 'cyan'),
                'MA20': ('ma20', 'red'),
                'MA60': ('ma60', '#00FF00')
            }
            
            for ma_name, (col_name, color) in ma_columns.items():
                if col_name in df.columns:
                    ma_data = df[col_name].to_numpy()
                    if len(ma_data) > 0 and not np.isnan(ma_data[-1]):
                        ma_values[ma_name] = f"{ma_data[-1]:.2f}"
                    else:
                        ma_values[ma_name] = "--"
                else:
                    ma_values[ma_name] = "--"
            
            # 构建基础文本（日期和MA值）
            ma_text = (
                f"<font color='#C0C0C0'>日期: {last_date}</font>  "
                f"<font color='white'>MA5: {ma_values['MA5']}</font>  "
                f"<font color='cyan'>MA10: {ma_values['MA10']}</font>  "
                f"<font color='red'>MA20: {ma_values['MA20']}</font>  "
                f"<font color='#00FF00'>MA60: {ma_values['MA60']}</font>"
            )
            
            # 添加主图指标显示（SAR、BOLL等）
            overlay_text = ""
            
            # SAR指标
            if hasattr(self, 'indicator_buttons') and self.indicator_buttons.get('SAR', {}).isChecked():
                if 'sar' in df.columns:
                    sar_data = df['sar'].to_numpy()
                    if len(sar_data) > 0 and not np.isnan(sar_data[-1]):
                        overlay_text += f"  <font color='white'>SAR: {sar_data[-1]:.2f}</font>"
            
            # BOLL指标
            if hasattr(self, 'indicator_buttons') and self.indicator_buttons.get('BOLL', {}).isChecked():
                if 'mb' in df.columns and 'up' in df.columns and 'dn' in df.columns:
                    mb_data = df['mb'].to_numpy()
                    up_data = df['up'].to_numpy()
                    dn_data = df['dn'].to_numpy()
                    if len(mb_data) > 0 and not np.isnan(mb_data[-1]):
                        overlay_text += f"  <font color='white'>BOLL: {mb_data[-1]:.2f}/{up_data[-1]:.2f}/{dn_data[-1]:.2f}</font>"
            
            # 更新显示
            self.ma_values_label.setText(ma_text + overlay_text)
            
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"更新均线数据显示失败: {e}")
    
    def _render_overlay_indicators(self, df_pl, x):
        """
        渲染主图叠加指标（如SAR、BOLL等）
        
        Args:
            df_pl: 数据
            x: x轴坐标
        """
        try:
            # 检查是否有主图叠加指标被选中
            sar_checked = hasattr(self, 'indicator_buttons') and self.indicator_buttons.get('SAR', {}).isChecked()
            boll_checked = hasattr(self, 'indicator_buttons') and self.indicator_buttons.get('BOLL', {}).isChecked()
            logger.debug(f"主图叠加指标状态: SAR={sar_checked}, BOLL={boll_checked}")
            
            # SAR指标
            if sar_checked:
                logger.debug("在主图绘制SAR指标")
                df_pl = self._indicator_renderer.render_sar(self.tech_plot_widget, df_pl, x)
            
            # BOLL指标
            if boll_checked:
                logger.debug("在主图绘制BOLL指标")
                df_pl = self._indicator_renderer.render_boll(self.tech_plot_widget, df_pl, x)
                
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"渲染主图叠加指标失败: {e}")
        
        return df_pl
    
    def _render_indicator_windows(self, df_pl, x, dates):
        """
        渲染指标窗口
        
        Args:
            df_pl: 数据
            x: x轴坐标
            dates: 日期数组
        """
        logger.debug(f"渲染指标窗口，当前设置: window_2={self.window_indicators.get(2)}, window_3={self.window_indicators.get(3)}")
        
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
        # 如果没有指定指标，清除窗口但不渲染
        if window_index == 2:
            plot_widget = self.volume_plot_widget
        elif window_index == 3:
            plot_widget = self.kdj_plot_widget
        else:
            return
        
        # 清除之前的指标图形
        plot_widget.clear()
        
        # 如果没有指定指标，直接返回
        if not indicator_name:
            logger.debug(f"窗口 {window_index} 没有指定指标，已清除")
            return
        
        logger.debug(f"渲染窗口 {window_index} 的指标: {indicator_name}")
        
        # 设置Y轴标签和样式（与第一窗口保持一致）
        plot_widget.setLabel('left', indicator_name, color='#C0C0C0')
        plot_widget.getAxis('left').setPen(pg.mkPen('#C0C0C0'))
        plot_widget.getAxis('left').setTextPen(pg.mkPen('#C0C0C0'))
        plot_widget.getAxis('left').setWidth(50)  # 与第一窗口保持一致
        
        # 渲染指标（可能返回更新后的数据）
        df_pl = self._indicator_renderer.render_indicator(plot_widget, indicator_name, x, df_pl)

        # 再次设置Y轴宽度和样式（确保在渲染后仍然有效）
        plot_widget.getAxis('left').setWidth(50)
        plot_widget.getAxis('left').setStyle(showValues=True)
        
        # 对于SAR指标，设置合适的Y轴范围
        if indicator_name == "SAR" and df_pl is not None and 'sar' in df_pl.columns:
            try:
                sar_data = df_pl['sar'].drop_nulls()
                if len(sar_data) > 0:
                    min_val = sar_data.min()
                    max_val = sar_data.max()
                    range_val = max_val - min_val
                    if range_val > 0:
                        plot_widget.setYRange(min_val - range_val * 0.1, max_val + range_val * 0.1)
                        logger.debug(f"SAR指标Y轴范围: {min_val - range_val * 0.1:.2f} - {max_val + range_val * 0.1:.2f}")
            except (OSError, RuntimeError, ValueError) as e:
                logger.warning(f"设置SAR指标Y轴范围失败: {e}")

        # 创建标签栏（使用可能更新后的数据）
        ui_builder = ChartUIBuilder(self)
        container = ui_builder.create_indicator_label_bar(window_index, indicator_name, df_pl)
        
        if container:
            # 设置X轴范围和刻度
            plot_widget.setXRange(-1, len(dates) - 1)
            
            # 设置X轴刻度标签
            ax = plot_widget.getAxis('bottom')
            tick_interval = ChartUtils.calculate_x_axis_tick_interval(len(dates))
            ax.setTicks([[
                (i, self._format_date(dates[i]))
                for i in range(0, len(dates), tick_interval)
            ]])
            
            # 添加到容器布局
            ui_builder.add_label_to_container(container, None, window_index)
        
        # 保存指标数据
        self._save_indicator_data(indicator_name, df_pl)
        logger.debug(f"窗口 {window_index} 的指标 {indicator_name} 渲染完成")
    
    def _save_indicator_data(self, indicator_name: str, df_pl):
        """
        保存指标数据
        
        Args:
            indicator_name: 指标名称
            df_pl: 数据
        """
        # 清除之前可能存在的其他指标数据（避免数据混乱）
        # 根据当前指标类型，清除不相关的指标数据
        if indicator_name == "SAR":
            # SAR指标只保留SAR数据
            self.current_kdj_data = None
            self.current_rsi_data = None
            self.current_macd_data = None
            self.current_wr_data = None
            self.current_boll_data = None
            self.current_dmi_data = None
            self.current_trix_data = None
            self.current_brar_data = None
            self.current_vr_data = None
            self.current_dma_data = None
        elif indicator_name in ["KDJ", "RSI", "MACD", "WR", "BOLL", "DMI", "TRIX", "BRAR", "VR", "DMA"]:
            # 其他副图指标清除SAR数据（避免主图标题栏显示错误的SAR值）
            self.current_sar_data = None
        
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

        # DMA数据
        if 'dma' in df_pl.columns:
            self.current_dma_data = {
                'dma': df_pl['dma'].to_list(),
                'ama': df_pl['ama'].to_list() if 'ama' in df_pl.columns else [],
            }
        
        # SAR数据
        if 'sar' in df_pl.columns:
            self.current_sar_data = {
                'sar': df_pl['sar'].to_list(),
            }
        
        # FSL数据
        if 'swl' in df_pl.columns:
            self.current_fsl_data = {
                'swl': df_pl['swl'].to_list(),
                'sws': df_pl['sws'].to_list() if 'sws' in df_pl.columns else [],
            }
        
        # CR数据
        if 'cr' in df_pl.columns:
            self.current_cr_data = {
                'cr': df_pl['cr'].to_list(),
            }
        
        # OBV数据
        if 'obv' in df_pl.columns:
            self.current_obv_data = {
                'obv': df_pl['obv'].to_list(),
            }
        
        # 新增指标数据保存
        # EXPMA数据
        if 'expma12' in df_pl.columns:
            self.current_expma_data = {
                'expma12': df_pl['expma12'].to_list(),
                'expma50': df_pl['expma50'].to_list() if 'expma50' in df_pl.columns else [],
            }
        
        # BBI数据
        if 'bbi' in df_pl.columns:
            self.current_bbi_data = {
                'bbi': df_pl['bbi'].to_list(),
            }
        
        # HSL数据
        if 'hsl' in df_pl.columns:
            self.current_hsl_data = {
                'hsl': df_pl['hsl'].to_list(),
                'hsl_ma5': df_pl['hsl_ma5'].to_list() if 'hsl_ma5' in df_pl.columns else [],
                'hsl_ma10': df_pl['hsl_ma10'].to_list() if 'hsl_ma10' in df_pl.columns else [],
            }
        
        # LB数据
        if 'lb' in df_pl.columns:
            self.current_lb_data = {
                'lb': df_pl['lb'].to_list(),
            }
        
        # CYC数据
        if 'cyc5' in df_pl.columns:
            self.current_cyc_data = {
                'cyc5': df_pl['cyc5'].to_list(),
                'cyc13': df_pl['cyc13'].to_list() if 'cyc13' in df_pl.columns else [],
                'cyc34': df_pl['cyc34'].to_list() if 'cyc34' in df_pl.columns else [],
                'cyc_inf': df_pl['cyc_inf'].to_list() if 'cyc_inf' in df_pl.columns else [],
            }
        
        # CYS数据
        if 'cys' in df_pl.columns:
            self.current_cys_data = {
                'cys': df_pl['cys'].to_list(),
                'cys_ma5': df_pl['cys_ma5'].to_list() if 'cys_ma5' in df_pl.columns else [],
            }
        
        # ASI数据
        if 'asi' in df_pl.columns:
            self.current_asi_data = {
                'asi': df_pl['asi'].to_list(),
                'asi_sig': df_pl['asi_sig'].to_list() if 'asi_sig' in df_pl.columns else [],
            }
        
        # EMV数据
        if 'emv' in df_pl.columns:
            self.current_emv_data = {
                'emv': df_pl['emv'].to_list(),
            }
        
        # CCI数据
        if 'cci' in df_pl.columns:
            self.current_cci_data = {
                'cci': df_pl['cci'].to_list(),
            }
        
        # ROC数据
        if 'roc' in df_pl.columns:
            self.current_roc_data = {
                'roc': df_pl['roc'].to_list(),
            }
        
        # MTM数据
        if 'mtm' in df_pl.columns:
            self.current_mtm_data = {
                'mtm': df_pl['mtm'].to_list(),
            }
        
        # PSY数据
        if 'psy' in df_pl.columns:
            self.current_psy_data = {
                'psy': df_pl['psy'].to_list(),
            }
        
        # MCST数据
        if 'mcst' in df_pl.columns:
            self.current_mcst_data = {
                'mcst': df_pl['mcst'].to_list(),
            }
    
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
    
    # ===== 分红标记相关方法（保持原有逻辑） =====
