#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
图表UI构建类
负责图表UI组件的创建和管理
"""

from PySide6.QtWidgets import QLabel, QHBoxLayout, QWidget, QMenu
from PySide6.QtGui import QColor, QAction
from PySide6.QtCore import Qt
from typing import Optional, Callable, Any
from loguru import logger


class ChartUIBuilder:
    """
    图表UI构建类
    负责创建和管理图表的UI组件
    """
    
    def __init__(self, main_window: Any):
        """
        初始化UI构建器
        
        Args:
            main_window: 主窗口实例
        """
        self.main_window = main_window
    
    def create_title_section(
        self, 
        stock_name: str, 
        stock_code: str, 
        period: str = '日线'
    ) -> QWidget:
        """
        创建标题区域
        
        Args:
            stock_name: 股票名称
            stock_code: 股票代码
            period: 周期
            
        Returns:
            QWidget: 标题容器
        """
        # 清理旧组件
        self._cleanup_old_title_widgets()
        
        # 创建标题标签
        chart_title_label = self._create_chart_title_label(stock_name, stock_code, period)
        
        # 创建MA值显示标签
        ma_values_label = self._create_ma_values_label()
        
        # 保存引用到主窗口
        self.main_window.chart_title_label = chart_title_label
        self.main_window.ma_values_label = ma_values_label
        
        # 创建容器和布局
        container = self._create_title_container(chart_title_label, ma_values_label)
        
        # 添加到图表布局
        if hasattr(self.main_window, 'chart_layout'):
            self._insert_title_to_layout(container)
        
        return container
    
    def _cleanup_old_title_widgets(self):
        """清理旧的标题组件"""
        # 移除整个标题容器（包含菜单按钮、标题标签、MA标签）
        if hasattr(self.main_window, 'title_ma_container') and self.main_window.title_ma_container:
            try:
                # 从布局中移除
                if hasattr(self.main_window, 'chart_layout'):
                    self.main_window.chart_layout.removeWidget(self.main_window.title_ma_container)
                # 删除容器及其所有子组件
                self.main_window.title_ma_container.deleteLater()
            except Exception as e:
                logger.warning(f"移除旧标题容器时发生错误: {e}")
        
        # 清理引用
        self.main_window.title_ma_container = None
        self.main_window.chart_title_label = None
        self.main_window.ma_values_label = None
        self.main_window.menu_btn = None
    
    def _create_chart_title_label(self, stock_name: str, stock_code: str, period: str) -> QLabel:
        """创建图表标题标签"""
        label = QLabel()
        label.setStyleSheet(
            "font-family: Consolas, monospace; padding: 5px; color: #C0C0C0; "
            "background-color: transparent; border: none;"
        )
        label.setText(f"{stock_name}({stock_code}) {period}")
        label.setWordWrap(False)
        return label
    
    def _create_ma_values_label(self) -> QLabel:
        """创建MA值显示标签"""
        label = QLabel()
        label.setStyleSheet(
            "font-family: Consolas, monospace; padding: 5px; color: #C0C0C0; "
            "background-color: transparent; border: none;"
        )
        label.setText(
            "<font color='#C0C0C0'>日期: --</font>  "
            "<font color='white'>MA5: --</font>  "
            "<font color='cyan'>MA10: --</font>  "
            "<font color='red'>MA20: --</font>  "
            "<font color='#00FF00'>MA60: --</font>"
        )
        label.setWordWrap(False)
        return label
    
    def _create_title_container(self, title_label: QLabel, ma_label: QLabel) -> QWidget:
        """创建标题容器"""
        container = QWidget()
        container.setStyleSheet("background-color: #222222;")
        
        layout = QHBoxLayout(container)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # 添加功能菜单按钮
        menu_btn = self.main_window.create_indicator_menu_button(
            self.main_window.current_selected_window
        )
        layout.addWidget(menu_btn)
        self.main_window.menu_btn = menu_btn
        
        # 添加窗口标题标签
        window_title_label = QLabel("K线")
        window_title_label.setStyleSheet(
            "background-color: transparent; color: #C0C0C0; font-size: 12px; padding: 0 5px;"
        )
        layout.addWidget(window_title_label)
        
        # 添加标题和MA标签
        layout.addWidget(title_label)
        layout.addWidget(ma_label)
        layout.addStretch(1)
        
        self.main_window.title_ma_container = container
        return container
    
    def _insert_title_to_layout(self, container: QWidget):
        """将标题容器插入到图表布局"""
        chart_layout = self.main_window.chart_layout
        
        # 移除旧的布局项
        for i in range(chart_layout.count()):
            item = chart_layout.itemAt(i)
            if isinstance(item, QHBoxLayout):
                chart_layout.removeItem(item)
                break
            elif hasattr(self.main_window, 'title_ma_container') and item.widget() == self.main_window.title_ma_container:
                chart_layout.removeWidget(self.main_window.title_ma_container)
                break
        
        # 插入新容器
        chart_layout.insertWidget(0, container)
    
    def create_indicator_label_bar(
        self, 
        window_index: int, 
        indicator_type: str, 
        df: Any
    ) -> Optional[QWidget]:
        """
        创建指标标签栏
        
        Args:
            window_index: 窗口索引
            indicator_type: 指标类型
            df: 数据
            
        Returns:
            Optional[QWidget]: 标签栏容器
        """
        # 清理旧组件
        self._cleanup_old_indicator_labels(window_index)
        
        # 创建容器
        container = QWidget()
        container.setStyleSheet("background-color: #222222;")
        
        layout = QHBoxLayout(container)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # 添加功能菜单按钮
        menu_btn = self.main_window.create_indicator_menu_button(window_index)
        layout.addWidget(menu_btn)
        
        # 创建数值标签
        values_label = QLabel()
        values_label.setStyleSheet(
            "font-family: Consolas, monospace; padding: 5px; color: #C0C0C0; "
            "background-color: transparent; border: none;"
        )
        values_label.setWordWrap(False)
        
        # 根据指标类型设置文本
        text = self._get_indicator_label_text(indicator_type, df)
        values_label.setText(text)
        
        layout.addWidget(values_label)
        layout.addStretch(1)
        
        # 保存引用
        if window_index == 2:
            self.main_window.volume_label_container = container
            self.main_window.volume_values_label = values_label
            self.main_window.volume_menu_btn = menu_btn
            self.main_window.volume_label_layout = layout  # 保存layout引用
        elif window_index == 3:
            self.main_window.kdj_label_container = container
            self.main_window.kdj_values_label = values_label
            self.main_window.kdj_menu_btn = menu_btn
            self.main_window.kdj_label_layout = layout  # 保存layout引用
        
        return container
    
    def _cleanup_old_indicator_labels(self, window_index: int):
        """清理旧的指标标签"""
        if window_index == 2:
            self._cleanup_volume_labels()
        elif window_index == 3:
            self._cleanup_kdj_labels()
    
    def _cleanup_volume_labels(self):
        """清理成交量标签"""
        if hasattr(self.main_window, 'volume_values_label'):
            try:
                self.main_window.volume_values_label.deleteLater()
            except Exception as e:
                logger.warning(f"移除旧成交量标签时发生错误: {e}")
        
        if hasattr(self.main_window, 'volume_label_container'):
            try:
                if hasattr(self.main_window, 'volume_container') and hasattr(self.main_window, 'volume_container_layout'):
                    self.main_window.volume_container_layout.removeWidget(self.main_window.volume_label_container)
                self.main_window.volume_label_container.deleteLater()
            except Exception as e:
                logger.warning(f"移除旧成交量标签栏容器时发生错误: {e}")
    
    def _cleanup_kdj_labels(self):
        """清理KDJ标签"""
        if hasattr(self.main_window, 'kdj_values_label'):
            try:
                self.main_window.kdj_values_label.deleteLater()
            except Exception as e:
                logger.warning(f"移除旧KDJ标签时发生错误: {e}")
        
        if hasattr(self.main_window, 'kdj_label_container'):
            try:
                if hasattr(self.main_window, 'kdj_container') and hasattr(self.main_window, 'kdj_container_layout'):
                    self.main_window.kdj_container_layout.removeWidget(self.main_window.kdj_label_container)
                self.main_window.kdj_label_container.deleteLater()
            except Exception as e:
                logger.warning(f"移除旧KDJ标签栏容器时发生错误: {e}")
    
    def _get_indicator_label_text(self, indicator_type: str, df: Any) -> str:
        """获取指标标签文本"""
        text_generators = {
            'KDJ': self._get_kdj_text,
            'RSI': self._get_rsi_text,
            'MACD': self._get_macd_text,
            'VOL': self._get_vol_text,
            'WR': self._get_wr_text,
            'BOLL': self._get_boll_text,
            'VR': self._get_vr_text,
            'BRAR': self._get_brar_text,
            'DMI': self._get_dmi_text,
            'TRIX': self._get_trix_text,
            'OBV': self._get_obv_text,
            'ASI': self._get_asi_text,
            'EMV': self._get_emv_text,
            'CCI': self._get_cci_text,
            'ROC': self._get_roc_text,
            'MTM': self._get_mtm_text,
            'PSY': self._get_psy_text,
            'MCST': self._get_mcst_text,
        }

        generator = text_generators.get(indicator_type)
        if generator:
            return generator(df)
        return ""
    
    def _get_kdj_text(self, df: Any) -> str:
        """获取KDJ标签文本"""
        if 'k' in df.columns and 'd' in df.columns and 'j' in df.columns:
            latest_k = df['k'].tail(1)[0]
            latest_d = df['d'].tail(1)[0]
            latest_j = df['j'].tail(1)[0]
            return (
                f"<font color='white'>K: {latest_k:.2f}</font>  "
                f"<font color='yellow'>D: {latest_d:.2f}</font>  "
                f"<font color='magenta'>J: {latest_j:.2f}</font>"
            )
        return "<font color='white'>KDJ指标数据不可用</font>"
    
    def _get_rsi_text(self, df: Any) -> str:
        """获取RSI标签文本"""
        if 'rsi14' in df.columns:
            latest_rsi = df['rsi14'].tail(1)[0]
            return f"<font color='blue'>RSI14: {latest_rsi:.2f}</font>"
        return "<font color='white'>RSI指标数据不可用</font>"
    
    def _get_macd_text(self, df: Any) -> str:
        """获取MACD标签文本"""
        if 'macd' in df.columns and 'macd_signal' in df.columns and 'macd_hist' in df.columns:
            latest_macd = df['macd'].tail(1)[0]
            latest_macd_signal = df['macd_signal'].tail(1)[0]
            latest_macd_hist = df['macd_hist'].tail(1)[0]
            macd_hist_color = '#FF0000' if latest_macd_hist >= 0 else '#00FF00'
            return (
                f"<font color='white'>MACD(12,26,9) </font>"
                f"<font color='#FFFFFF'>DIF: {latest_macd:.2f}</font> "
                f"<font color='#FFFF00'>DEA: {latest_macd_signal:.2f}</font> "
                f"<font color='{macd_hist_color}'>MACD: {latest_macd_hist:.2f}</font>"
            )
        return "<font color='white'>MACD指标数据不可用</font>"
    
    def _get_vol_text(self, df: Any) -> str:
        """获取成交量标签文本"""
        if 'volume' in df.columns and 'vol_ma5' in df.columns and 'vol_ma10' in df.columns:
            latest_volume = df['volume'].tail(1)[0]
            latest_vol_ma5 = df['vol_ma5'].tail(1)[0]
            latest_vol_ma10 = df['vol_ma10'].tail(1)[0]
            return (
                f"<font color='#C0C0C0'>VOLUME: {int(latest_volume):,}</font>  "
                f"<font color='white'>MA5: {int(latest_vol_ma5):,}</font>  "
                f"<font color='cyan'>MA10: {int(latest_vol_ma10):,}</font>"
            )
        return "<font color='white'>成交量数据不可用</font>"
    
    def _get_wr_text(self, df: Any) -> str:
        """获取WR标签文本"""
        if 'wr1' in df.columns and 'wr2' in df.columns:
            latest_wr1 = df['wr1'].tail(1)[0]
            latest_wr2 = df['wr2'].tail(1)[0]
            return (
                f"<font color='white'>WR(10,6) </font>"
                f"<font color='yellow'>WR1: {latest_wr1:.2f}</font> "
                f"<font color='white'>WR2: {latest_wr2:.2f}</font>"
            )
        elif 'wr' in df.columns:
            latest_wr = df['wr'].tail(1)[0]
            return f"<font color='white'>WR: {latest_wr:.2f}</font>"
        return ""
    
    def _get_boll_text(self, df: Any) -> str:
        """获取BOLL标签文本"""
        if 'mb' in df.columns and 'up' in df.columns and 'dn' in df.columns:
            latest_mb = df['mb'].tail(1)[0]
            latest_up = df['up'].tail(1)[0]
            latest_dn = df['dn'].tail(1)[0]
            return (
                f"<font color='white'>MB: {latest_mb:.2f}</font>  "
                f"<font color='red'>UP: {latest_up:.2f}</font>  "
                f"<font color='#00FF00'>DN: {latest_dn:.2f}</font>"
            )
        return "<font color='white'>BOLL指标数据不可用</font>"
    
    def _get_vr_text(self, df: Any) -> str:
        """获取VR标签文本"""
        if 'vr' in df.columns:
            latest_vr = df['vr'].tail(1)[0]
            if 'mavr' in df.columns:
                latest_mavr = df['mavr'].tail(1)[0]
                return (
                    f"<font color='#FFFFFF'>VR: {latest_vr:.2f}</font>  "
                    f"<font color='#FFFF00'>MAVR: {latest_mavr:.2f}</font>"
                )
            return f"<font color='#FFFFFF'>VR: {latest_vr:.2f}</font>"
        return ""

    def _get_brar_text(self, df: Any) -> str:
        """获取BRAR标签文本"""
        if 'ar' in df.columns and 'br' in df.columns:
            latest_ar = df['ar'].tail(1)[0]
            latest_br = df['br'].tail(1)[0]
            return (
                f"<font color='white'>AR: {latest_ar:.2f}</font>  "
                f"<font color='yellow'>BR: {latest_br:.2f}</font>"
            )
        return "<font color='white'>BRAR指标数据不可用</font>"

    def _get_dmi_text(self, df: Any) -> str:
        """获取DMI标签文本"""
        if 'pdi' in df.columns and 'ndi' in df.columns:
            latest_pdi = df['pdi'].tail(1)[0]
            latest_ndi = df['ndi'].tail(1)[0]
            text = (
                f"<font color='white'>PDI: {latest_pdi:.2f}</font>  "
                f"<font color='yellow'>NDI: {latest_ndi:.2f}</font>"
            )
            if 'adx' in df.columns:
                latest_adx = df['adx'].tail(1)[0]
                text += f"  <font color='red'>ADX: {latest_adx:.2f}</font>"
            if 'adxr' in df.columns:
                latest_adxr = df['adxr'].tail(1)[0]
                text += f"  <font color='#00FF00'>ADXR: {latest_adxr:.2f}</font>"
            return text
        return "<font color='white'>DMI指标数据不可用</font>"

    def _get_trix_text(self, df: Any) -> str:
        """获取TRIX标签文本"""
        if 'trix' in df.columns:
            latest_trix = df['trix'].tail(1)[0]
            text = f"<font color='white'>TRIX: {latest_trix:.4f}</font>"
            if 'trma' in df.columns:
                latest_trma = df['trma'].tail(1)[0]
                text += f"  <font color='yellow'>TRMA: {latest_trma:.4f}</font>"
            return text
        return "<font color='white'>TRIX指标数据不可用</font>"

    def _get_obv_text(self, df: Any) -> str:
        """获取OBV标签文本"""
        if 'obv' in df.columns:
            latest_obv = df['obv'].tail(1)[0]
            return f"<font color='white'>OBV: {latest_obv:,.0f}</font>"
        return "<font color='white'>OBV指标数据不可用</font>"

    def _get_asi_text(self, df: Any) -> str:
        """获取ASI标签文本"""
        if 'asi' in df.columns:
            latest_asi = df['asi'].tail(1)[0]
            text = f"<font color='white'>ASI: {latest_asi:.2f}</font>"
            if 'asi_sig' in df.columns:
                latest_sig = df['asi_sig'].tail(1)[0]
                text += f"  <font color='yellow'>SIG: {latest_sig:.2f}</font>"
            return text
        return "<font color='white'>ASI指标数据不可用</font>"

    def _get_emv_text(self, df: Any) -> str:
        """获取EMV标签文本"""
        if 'emv' in df.columns:
            latest_emv = df['emv'].tail(1)[0]
            return f"<font color='white'>EMV: {latest_emv:.4f}</font>"
        return "<font color='white'>EMV指标数据不可用</font>"

    def _get_cci_text(self, df: Any) -> str:
        """获取CCI标签文本"""
        if 'cci' in df.columns:
            latest_cci = df['cci'].tail(1)[0]
            return f"<font color='white'>CCI: {latest_cci:.2f}</font>"
        return "<font color='white'>CCI指标数据不可用</font>"

    def _get_roc_text(self, df: Any) -> str:
        """获取ROC标签文本"""
        if 'roc' in df.columns:
            latest_roc = df['roc'].tail(1)[0]
            return f"<font color='white'>ROC: {latest_roc:.2f}</font>"
        return "<font color='white'>ROC指标数据不可用</font>"

    def _get_mtm_text(self, df: Any) -> str:
        """获取MTM标签文本"""
        if 'mtm' in df.columns:
            latest_mtm = df['mtm'].tail(1)[0]
            return f"<font color='white'>MTM: {latest_mtm:.2f}</font>"
        return "<font color='white'>MTM指标数据不可用</font>"

    def _get_psy_text(self, df: Any) -> str:
        """获取PSY标签文本"""
        if 'psy' in df.columns:
            latest_psy = df['psy'].tail(1)[0]
            return f"<font color='white'>PSY: {latest_psy:.2f}</font>"
        return "<font color='white'>PSY指标数据不可用</font>"

    def _get_mcst_text(self, df: Any) -> str:
        """获取MCST标签文本"""
        if 'mcst' in df.columns:
            latest_mcst = df['mcst'].tail(1)[0]
            text = f"<font color='white'>MCST: {latest_mcst:.2f}</font>"
            if 'mcst_ma' in df.columns:
                latest_ma = df['mcst_ma'].tail(1)[0]
                text += f"  <font color='yellow'>MA: {latest_ma:.2f}</font>"
            return text
        return "<font color='white'>MCST指标数据不可用</font>"

    def add_label_to_container(self, container: QWidget, label: QLabel, window_index: int):
        """
        将标签栏容器添加到窗口布局
        
        Args:
            container: 标签栏容器
            label: 数值标签（已包含在container中，此参数保留用于兼容）
            window_index: 窗口索引
        """
        if window_index == 2 and hasattr(self.main_window, 'volume_container_layout'):
            self._add_to_volume_container(container)
        elif window_index == 3 and hasattr(self.main_window, 'kdj_container_layout'):
            self._add_to_kdj_container(container)
    
    def _add_to_volume_container(self, container: QWidget):
        """添加到成交量容器"""
        layout = self.main_window.volume_container_layout
        
        # 移除旧的标签栏容器（如果存在）
        if hasattr(self.main_window, 'volume_label_container') and self.main_window.volume_label_container:
            self._safe_remove_widget(layout, self.main_window.volume_label_container)
        
        # 在图表上方插入标签栏容器（索引0位置）
        layout.insertWidget(0, container)
    
    def _add_to_kdj_container(self, container: QWidget):
        """添加到KDJ容器"""
        layout = self.main_window.kdj_container_layout
        
        # 移除旧的标签栏容器（如果存在）
        if hasattr(self.main_window, 'kdj_label_container') and self.main_window.kdj_label_container:
            self._safe_remove_widget(layout, self.main_window.kdj_label_container)
        
        # 在图表上方插入标签栏容器（索引0位置）
        layout.insertWidget(0, container)
    
    def _safe_remove_widget(self, layout: Any, widget: QWidget):
        """安全移除widget"""
        if widget:
            try:
                layout.removeWidget(widget)
            except Exception:
                pass
