#!/usr/bin/env python3

"""
主窗口副图指标 Mixin：叠加/副图指标渲染、指标数据保存与 MA 值显示。
"""

import numpy as np
import polars as pl
import pyqtgraph as pg

from src.ui.chart_components import ChartUIBuilder
from src.ui.utils.chart_utils import ChartUtils
from src.utils.logger import logger

# 指标数据落库表： (触发列, 目标属性, ((输出键, 源列), ...))
# 触发列存在即写入该属性；同名输出键无对应源列时填 []（保持旧行为的容错）。
# 新增副图指标只需在此追加一行，无需再改 _save_indicator_data。
_INDICATOR_FIELD_SPECS: tuple[tuple[str, str, tuple[tuple[str, str], ...]], ...] = (
    ('k', 'current_kdj_data', (('k', 'k'), ('d', 'd'), ('j', 'j'))),
    ('rsi14', 'current_rsi_data', (('rsi', 'rsi14'),)),
    ('macd', 'current_macd_data', (
        ('macd', 'macd'), ('macd_signal', 'macd_signal'), ('macd_hist', 'macd_hist'))),
    ('wr1', 'current_wr_data', (('wr1', 'wr1'), ('wr2', 'wr2'))),
    ('mb', 'current_boll_data', (('mb', 'mb'), ('up', 'up'), ('dn', 'dn'))),
    ('volume', 'current_volume_data', (
        ('volume', 'volume'), ('vol_ma5', 'vol_ma5'), ('vol_ma10', 'vol_ma10'))),
    ('vr', 'current_vr_data', (('vr', 'vr'), ('mavr', 'mavr'))),
    ('pdi', 'current_dmi_data', (
        ('pdi', 'pdi'), ('ndi', 'ndi'), ('adx', 'adx'), ('adxr', 'adxr'))),
    ('trix', 'current_trix_data', (('trix', 'trix'), ('trma', 'trma'))),
    ('br', 'current_brar_data', (('br', 'br'), ('ar', 'ar'))),
    ('dma', 'current_dma_data', (('dma', 'dma'), ('ama', 'ama'))),
    ('sar', 'current_sar_data', (('sar', 'sar'),)),
    ('swl', 'current_fsl_data', (('swl', 'swl'), ('sws', 'sws'))),
    ('cr', 'current_cr_data', (('cr', 'cr'),)),
    ('obv', 'current_obv_data', (('obv', 'obv'),)),
    ('expma12', 'current_expma_data', (('expma12', 'expma12'), ('expma50', 'expma50'))),
    ('bbi', 'current_bbi_data', (('bbi', 'bbi'),)),
    ('hsl', 'current_hsl_data', (
        ('hsl', 'hsl'), ('hsl_ma5', 'hsl_ma5'), ('hsl_ma10', 'hsl_ma10'))),
    ('lb', 'current_lb_data', (('lb', 'lb'),)),
    ('cyc5', 'current_cyc_data', (
        ('cyc5', 'cyc5'), ('cyc13', 'cyc13'), ('cyc34', 'cyc34'), ('cyc_inf', 'cyc_inf'))),
    ('cys', 'current_cys_data', (('cys', 'cys'), ('cys_ma5', 'cys_ma5'))),
    ('asi', 'current_asi_data', (('asi', 'asi'), ('asi_sig', 'asi_sig'))),
    ('emv', 'current_emv_data', (('emv', 'emv'),)),
    ('cci', 'current_cci_data', (('cci', 'cci'),)),
    ('roc', 'current_roc_data', (('roc', 'roc'),)),
    ('mtm', 'current_mtm_data', (('mtm', 'mtm'),)),
    ('psy', 'current_psy_data', (('psy', 'psy'),)),
    ('mcst', 'current_mcst_data', (('mcst', 'mcst'),)),
)

# SAR 为叠加型主图指标：切到 SAR 时清空这些副图缓存，避免标题栏读到旧值
_SAR_RESET_ATTRS = (
    'current_kdj_data', 'current_rsi_data', 'current_macd_data', 'current_wr_data',
    'current_boll_data', 'current_dmi_data', 'current_trix_data', 'current_brar_data',
    'current_vr_data', 'current_dma_data',
)

# 切到这些副图指标时清空 SAR 缓存（避免主图标题栏显示错误的 SAR 值）
_SAR_CLEARING_INDICATORS = frozenset({
    'KDJ', 'RSI', 'MACD', 'WR', 'BOLL', 'DMI', 'TRIX', 'BRAR', 'VR', 'DMA',
})


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

            for ma_name, (col_name, _color) in ma_columns.items():
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

    def _save_indicator_data(self, indicator_name: str, df_pl) -> None:
        """
        保存指标数据（表驱动，见模块级 _INDICATOR_FIELD_SPECS）。

        Args:
            indicator_name: 指标名称
            df_pl: 数据
        """
        self._reset_indicator_data(indicator_name)

        for trigger_col, attr_name, fields in _INDICATOR_FIELD_SPECS:
            if trigger_col not in df_pl.columns:
                continue
            # 触发列存在即写缓存；同名子列缺失时填 []，与旧的逐列 if 判断等价
            setattr(self, attr_name, {
                key: df_pl[col].to_list() if col in df_pl.columns else []
                for key, col in fields
            })

    def _reset_indicator_data(self, indicator_name: str) -> None:
        """切换指标时清空互相干扰的缓存，避免标题栏读到上一个指标的值。"""
        if indicator_name == 'SAR':
            # SAR 只保留自身的缓存
            for attr_name in _SAR_RESET_ATTRS:
                setattr(self, attr_name, None)
        elif indicator_name in _SAR_CLEARING_INDICATORS:
            # 其他副图指标清除 SAR 数据
            self.current_sar_data = None

    # ===== 分红标记相关方法（保持原有逻辑） =====
