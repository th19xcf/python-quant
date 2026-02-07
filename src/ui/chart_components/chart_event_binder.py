#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
图表事件绑定类
负责图表事件的绑定和管理
"""

import pyqtgraph as pg
from PySide6.QtCore import Qt, QTimer
from typing import Any, Callable, Optional
from loguru import logger


class ChartEventBinder:
    """
    图表事件绑定类
    负责绑定和管理图表的各种事件
    """
    
    def __init__(self, main_window: Any):
        """
        初始化事件绑定器
        
        Args:
            main_window: 主窗口实例
        """
        self.main_window = main_window
    
    def bind_all_events(self, dates, opens, highs, lows, closes):
        """
        绑定所有事件
        
        Args:
            dates: 日期数组
            opens: 开盘价数组
            highs: 最高价数组
            lows: 最低价数组
            closes: 收盘价数组
        """
        try:
            # 绑定鼠标移动事件
            self._bind_mouse_move_events(dates, opens, highs, lows, closes)
            
            # 绑定鼠标点击事件
            self._bind_mouse_click_events(dates, opens, highs, lows, closes)
            
            # 绑定范围同步事件
            self._bind_range_sync_events()
            
            # 设置右键菜单
            self._setup_context_menu()
            
            # 创建信息框定时器
            self._setup_info_timer()
            
            logger.debug("图表事件绑定完成")
            
        except Exception as e:
            logger.exception(f"绑定图表事件失败: {e}")
    
    def _bind_mouse_move_events(self, dates, opens, highs, lows, closes):
        """绑定鼠标移动事件"""
        try:
            # 断开之前的事件连接
            self._disconnect_mouse_events()
            
            # 获取鼠标处理器
            mouse_handler = self.main_window.event_handler_manager.get_mouse_handler()
            
            # 绑定K线图鼠标移动事件
            self.main_window.tech_plot_widget.scene().sigMouseMoved.connect(
                lambda pos: mouse_handler.handle_mouse_moved(pos, dates, opens, highs, lows, closes)
            )
            
            # 绑定成交量图鼠标移动事件
            self.main_window.volume_plot_widget.scene().sigMouseMoved.connect(
                lambda pos: mouse_handler.handle_mouse_moved(pos, dates, opens, highs, lows, closes)
            )
            
            # 绑定KDJ图鼠标移动事件
            self.main_window.kdj_plot_widget.scene().sigMouseMoved.connect(
                lambda pos: mouse_handler.handle_mouse_moved(pos, dates, opens, highs, lows, closes)
            )
            
            # 启用鼠标跟踪
            self.main_window.tech_plot_widget.viewport().setMouseTracking(True)
            
        except Exception as e:
            logger.exception(f"绑定鼠标移动事件失败: {e}")
    
    def _bind_mouse_click_events(self, dates, opens, highs, lows, closes):
        """绑定鼠标点击事件"""
        try:
            # 获取鼠标处理器
            mouse_handler = self.main_window.event_handler_manager.get_mouse_handler()
            
            # 绑定K线图鼠标点击事件
            self.main_window.tech_plot_widget.scene().sigMouseClicked.connect(
                lambda event: mouse_handler.handle_mouse_clicked(event, dates, opens, highs, lows, closes)
            )
            
            # 绑定成交量图鼠标点击事件
            self.main_window.volume_plot_widget.scene().sigMouseClicked.connect(
                lambda event: mouse_handler.handle_mouse_clicked(event, dates, opens, highs, lows, closes)
            )
            
            # 绑定KDJ图鼠标点击事件
            self.main_window.kdj_plot_widget.scene().sigMouseClicked.connect(
                lambda event: mouse_handler.handle_mouse_clicked(event, dates, opens, highs, lows, closes)
            )
            
        except Exception as e:
            logger.exception(f"绑定鼠标点击事件失败: {e}")
    
    def _disconnect_mouse_events(self):
        """断开鼠标事件连接"""
        widgets = [
            self.main_window.tech_plot_widget,
            self.main_window.volume_plot_widget,
            self.main_window.kdj_plot_widget
        ]
        
        for widget in widgets:
            try:
                if hasattr(widget.scene(), 'sigMouseMoved') and hasattr(widget.scene().sigMouseMoved, 'disconnect'):
                    widget.scene().sigMouseMoved.disconnect()
            except Exception:
                pass
            
            try:
                if hasattr(widget.scene(), 'sigMouseClicked') and hasattr(widget.scene().sigMouseClicked, 'disconnect'):
                    widget.scene().sigMouseClicked.disconnect()
            except Exception:
                pass
    
    def _bind_range_sync_events(self):
        """绑定范围同步事件"""
        try:
            # 获取viewBox
            tech_view_box = self.main_window.tech_plot_widget.getViewBox()
            volume_view_box = self.main_window.volume_plot_widget.getViewBox()
            kdj_view_box = self.main_window.kdj_plot_widget.getViewBox()
            
            # 禁用右键菜单
            tech_view_box.setMenuEnabled(False)
            volume_view_box.setMenuEnabled(False)
            kdj_view_box.setMenuEnabled(False)
            
            # 定义同步函数
            def sync_from_tech(view_range):
                """从K线图同步范围到其他图"""
                try:
                    x_min, x_max = tech_view_box.viewRange()[0]
                    volume_view_box.setXRange(x_min, x_max, padding=0)
                    kdj_view_box.setXRange(x_min, x_max, padding=0)
                    self._update_dividend_markers()
                except Exception as e:
                    logger.debug(f"同步范围失败: {e}")
            
            def sync_from_volume(view_range):
                """从成交量图同步范围到其他图"""
                try:
                    x_min, x_max = volume_view_box.viewRange()[0]
                    tech_view_box.setXRange(x_min, x_max, padding=0)
                    kdj_view_box.setXRange(x_min, x_max, padding=0)
                    self._update_dividend_markers()
                except Exception as e:
                    logger.debug(f"同步范围失败: {e}")
            
            def sync_from_kdj(view_range):
                """从KDJ图同步范围到其他图"""
                try:
                    x_min, x_max = kdj_view_box.viewRange()[0]
                    tech_view_box.setXRange(x_min, x_max, padding=0)
                    volume_view_box.setXRange(x_min, x_max, padding=0)
                    self._update_dividend_markers()
                except Exception as e:
                    logger.debug(f"同步范围失败: {e}")
            
            # 连接信号
            tech_view_box.sigRangeChanged.connect(sync_from_tech)
            volume_view_box.sigRangeChanged.connect(sync_from_volume)
            kdj_view_box.sigRangeChanged.connect(sync_from_kdj)
            
        except Exception as e:
            logger.exception(f"绑定范围同步事件失败: {e}")
    
    def _update_dividend_markers(self):
        """更新分红标记位置"""
        try:
            if hasattr(self.main_window, 'dividend_marker_manager') and self.main_window.dividend_marker_manager:
                self.main_window.dividend_marker_manager.update_position()
        except Exception as e:
            logger.debug(f"更新分红标记位置失败: {e}")
    
    def _setup_context_menu(self):
        """设置右键菜单"""
        try:
            # 禁用默认右键菜单
            self._disable_default_context_menu()
            
            # 设置自定义右键菜单
            self._setup_custom_context_menu()
            
        except Exception as e:
            logger.exception(f"设置右键菜单失败: {e}")
    
    def _disable_default_context_menu(self):
        """禁用默认右键菜单"""
        # 禁用viewBox的右键菜单
        if hasattr(self.main_window.tech_plot_widget, 'getViewBox'):
            view_box = self.main_window.tech_plot_widget.getViewBox()
            view_box.setMenuEnabled(False)
        
        if hasattr(self.main_window.volume_plot_widget, 'getViewBox'):
            view_box = self.main_window.volume_plot_widget.getViewBox()
            view_box.setMenuEnabled(False)
        
        # 禁用所有子项的右键菜单
        for item in self.main_window.tech_plot_widget.items():
            if hasattr(item, 'setMenuEnabled'):
                item.setMenuEnabled(False)
    
    def _setup_custom_context_menu(self):
        """设置自定义右键菜单"""
        from PySide6.QtWidgets import QMenu
        from PySide6.QtGui import QAction
        
        def custom_context_menu(event):
            """自定义右键菜单处理函数"""
            try:
                # 创建自定义菜单
                menu = QMenu(self.main_window.tech_plot_widget)
                
                # 如果有选中的均线，添加修改指标参数选项
                if hasattr(self.main_window, 'selected_ma') and self.main_window.selected_ma:
                    modify_action = QAction(
                        f"修改{self.main_window.selected_ma}指标参数", 
                        self.main_window
                    )
                    modify_action.triggered.connect(
                        lambda: self.main_window.on_modify_indicator(self.main_window.selected_ma)
                    )
                    menu.addAction(modify_action)
                else:
                    # 如果没有选中均线，添加提示信息
                    no_select_action = QAction("未选中均线，请先点击选中均线", self.main_window)
                    no_select_action.setEnabled(False)
                    menu.addAction(no_select_action)
                
                # 在鼠标位置显示菜单
                qpoint = event.globalPos().toPoint()
                menu.exec(qpoint)
                
                # 阻止事件传播
                event.accept()
                
            except Exception as e:
                logger.debug(f"显示右键菜单失败: {e}")
        
        # 设置自定义右键菜单
        self.main_window.tech_plot_widget.contextMenuEvent = custom_context_menu
        self.main_window.tech_plot_widget.setContextMenuPolicy(Qt.CustomContextMenu)
        self.main_window.tech_plot_widget.customContextMenuRequested.connect(
            lambda pos: self.main_window.on_custom_context_menu(pos)
        )
    
    def _setup_info_timer(self):
        """设置信息框定时器"""
        try:
            # 创建定时器
            timer = QTimer()
            timer.setSingleShot(True)
            timer.setInterval(200)  # 200毫秒
            timer.timeout.connect(self.main_window.show_info_box)
            
            self.main_window.info_timer = timer
            
        except Exception as e:
            logger.exception(f"设置信息框定时器失败: {e}")
    
    def setup_crosshair(self):
        """
        设置十字线
        
        Returns:
            tuple: (vline, hline, volume_vline, volume_hline, kdj_vline, kdj_hline)
        """
        try:
            # 移除旧的十字线
            self._remove_old_crosshair()
            
            # 创建新的十字线
            vline = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen('w', width=1, style=Qt.DotLine))
            hline = pg.InfiniteLine(angle=0, movable=False, pen=pg.mkPen('w', width=1, style=Qt.DotLine))
            volume_vline = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen('w', width=1, style=Qt.DotLine))
            volume_hline = pg.InfiniteLine(angle=0, movable=False, pen=pg.mkPen('w', width=1, style=Qt.DotLine))
            kdj_vline = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen('w', width=1, style=Qt.DotLine))
            kdj_hline = pg.InfiniteLine(angle=0, movable=False, pen=pg.mkPen('w', width=1, style=Qt.DotLine))
            
            # 添加到图表
            self.main_window.tech_plot_widget.addItem(vline, ignoreBounds=True)
            self.main_window.tech_plot_widget.addItem(hline, ignoreBounds=True)
            self.main_window.volume_plot_widget.addItem(volume_vline, ignoreBounds=True)
            self.main_window.volume_plot_widget.addItem(volume_hline, ignoreBounds=True)
            self.main_window.kdj_plot_widget.addItem(kdj_vline, ignoreBounds=True)
            self.main_window.kdj_plot_widget.addItem(kdj_hline, ignoreBounds=True)
            
            # 初始隐藏
            vline.hide()
            hline.hide()
            volume_vline.hide()
            volume_hline.hide()
            kdj_vline.hide()
            kdj_hline.hide()
            
            return vline, hline, volume_vline, volume_hline, kdj_vline, kdj_hline
            
        except Exception as e:
            logger.exception(f"设置十字线失败: {e}")
            return None, None, None, None, None, None
    
    def _remove_old_crosshair(self):
        """移除旧的十字线"""
        lines = [
            ('vline', self.main_window.tech_plot_widget),
            ('hline', self.main_window.tech_plot_widget),
            ('volume_vline', self.main_window.volume_plot_widget),
            ('volume_hline', self.main_window.volume_plot_widget),
            ('kdj_vline', self.main_window.kdj_plot_widget),
            ('kdj_hline', self.main_window.kdj_plot_widget),
        ]
        
        for attr_name, widget in lines:
            try:
                if hasattr(self.main_window, attr_name):
                    line = getattr(self.main_window, attr_name)
                    if line is not None:
                        widget.removeItem(line)
            except Exception as e:
                logger.debug(f"移除旧十字线失败: {e}")
    
    def setup_info_text(self):
        """
        设置信息文本项
        
        Returns:
            pg.TextItem: 信息文本项
        """
        try:
            info_text = pg.TextItem(anchor=(0, 1))
            info_text.setColor(pg.mkColor('w'))
            info_text.setHtml(
                '<div style="background-color: rgba(0, 0, 0, 0.8); padding: 5px; '
                'border: 1px solid #666; font-family: monospace;"></div>'
            )
            self.main_window.tech_plot_widget.addItem(info_text)
            info_text.hide()
            
            return info_text
            
        except Exception as e:
            logger.exception(f"设置信息文本项失败: {e}")
            return None


# 导入Qt常量
from PySide6.QtCore import Qt
