#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
UI管理器，负责主窗口UI初始化的编排
"""

from typing import TYPE_CHECKING

from src.utils.logger import logger

if TYPE_CHECKING:
    from src.ui.main_window import MainWindow


class UIManager:
    """主窗口UI管理器"""

    def __init__(self, window: "MainWindow"):
        self.window = window

    def init_ui(self):
        """初始化UI组件"""
        logger.debug("UIManager开始初始化UI")
        return self.window._init_ui_impl()
