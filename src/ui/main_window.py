#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
主窗口类，参考通达信软件界面设计
Refactored into Mixins
"""

import warnings
from PySide6.QtWidgets import QMainWindow
from PySide6.QtCore import Qt

# 忽略PySide6信号断开警告
warnings.simplefilter("ignore", RuntimeWarning)

from src.utils.logger import logger
from src.ui.ui_manager import UIManager
from src.ui.window_event_manager import WindowEventManager
from src.ui.chart_manager import ChartManager
from src.ui.data_view_manager import DataViewManager
from src.ui.action_manager import ActionManager
from src.ui.table_interaction_manager import TableInteractionManager
from src.ui.indicator_interaction_manager import IndicatorInteractionManager
from src.api.presentation_api import IView, IController

# Import Mixins
from src.ui.main_window_ui_mixin import MainWindowUiMixin
from src.ui.main_window_event_mixin import MainWindowEventMixin
from src.ui.main_window_data_mixin import MainWindowDataMixin
from src.ui.main_window_drawing_mixin import MainWindowDrawingMixin

class MainWindow(MainWindowUiMixin, MainWindowEventMixin, MainWindowDataMixin, 
                 MainWindowDrawingMixin, QMainWindow, IView, IController):
    """
    主窗口类，参考通达信软件界面设计
    实现了IView和IController接口
    """
    
    def __init__(self, config, data_manager, plugin_manager=None):
        """
        初始化主窗口
        
        Args:
            config: 配置对象
            data_manager: 数据管理器实例
            plugin_manager: 插件管理器实例
        """
        super().__init__()
        self.config = config
        self.data_manager = data_manager
        self.plugin_manager = plugin_manager
        
        self.setWindowTitle("中国股市量化分析系统")
        self.setGeometry(100, 100, 1400, 900)
        
        # 初始化管理器
        self.ui_manager = UIManager(self)
        self.event_manager = WindowEventManager(self)
        self.chart_manager = ChartManager(self)
        self.data_view_manager = DataViewManager(self)
        self.action_manager = ActionManager(self)
        self.table_interaction_manager = TableInteractionManager(self)
        self.indicator_interaction_manager = IndicatorInteractionManager(self)

        # 初始化辅助管理器
        # 这些管理器在重构前是在__init__中导入并初始化的
        from src.ui.indicator_drawers import IndicatorDrawerManager
        self.indicator_drawer_manager = IndicatorDrawerManager()
        
        from src.ui.event_handlers import EventHandlerManager
        self.event_handler_manager = EventHandlerManager(self)
        
        from src.ui.indicator_labels import IndicatorLabelManager
        self.indicator_label_manager = IndicatorLabelManager(self)
        
        # 初始化UI (From MainWindowUiMixin)
        self._init_ui_impl()
        
        # 订阅事件
        self.event_manager.subscribe_events()
        
        # 初始化状态属性
        self.vline = None
        self.hline = None
        self.volume_vline = None
        self.volume_hline = None
        self.kdj_vline = None
        self.kdj_hline = None
        
        self.ma_points = []
        self.moving_averages = {}
        self.selected_ma = None
        self.crosshair_enabled = False
        
        self.current_kline_index = -1
        self.current_kline_data = {}
        self.current_mouse_pos = None
        
        self.current_period = '日线' # Default
        
        # Initialize additional properties expected by Mixins
        self.window_indicators = {2: "VOL", 3: "KDJ"} # Default indicators
        self.current_window_count = 3
        self.displayed_bar_count = 100
        self.adjustment_type = 'qfq'  # 复权类型：qfq=前复权, hfq=后复权, none=不复权

        # 初始化后获取实时数据
        if hasattr(self, 'refresh_stock_data'):
            self.refresh_stock_data()
        if hasattr(self, 'refresh_market_info'):
            self.refresh_market_info()

        logger.info("MainWindow initialized successfully (Refactored)")

    # IView Interface Implementation
    def show_message(self, message):
        self.statusBar().showMessage(message, 3000)

    def show_error(self, error):
        self.statusBar().showMessage(f"Error: {error}", 5000)
        
    def refresh(self):
        self.update()

    # IController Interface Implementation (if any specific methods needed)
    pass

