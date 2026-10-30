#!/usr/bin/env python3

"""
主窗口UI混入类的聚合入口。

职责已按界面区域拆分到四个子 Mixin，本类仅做继承聚合，
以保持 MainWindow 的既有继承顺序与全部调用路径不变。
"""

from src.ui.main_window_indicator_bar_mixin import MainWindowIndicatorBarMixin
from src.ui.main_window_layout_mixin import MainWindowLayoutMixin
from src.ui.main_window_menu_mixin import MainWindowMenuMixin
from src.ui.main_window_tech_tab_mixin import MainWindowTechTabMixin


class MainWindowUiMixin(
    MainWindowLayoutMixin,
    MainWindowMenuMixin,
    MainWindowTechTabMixin,
    MainWindowIndicatorBarMixin,
):
    """UI 构建聚合类，具体实现见各子 Mixin。"""
