#!/usr/bin/env python3

"""
主题管理模块
"""

from PySide6.QtGui import QColor, QPalette
from PySide6.QtWidgets import QApplication


class ThemeManager:
    """
    主题管理器，负责管理应用的主题切换
    """

    @staticmethod
    def set_light_theme(app: QApplication):
        """
        设置浅色主题

        Args:
            app: QApplication实例
        """
        palette = QPalette()

        # 背景色
        palette.setColor(QPalette.ColorRole.Window, QColor(240, 240, 240))
        palette.setColor(QPalette.ColorRole.WindowText, QColor(0, 0, 0))
        palette.setColor(QPalette.ColorRole.Base, QColor(255, 255, 255))
        palette.setColor(QPalette.ColorRole.AlternateBase, QColor(245, 245, 245))

        # 文本颜色
        palette.setColor(QPalette.ColorRole.Text, QColor(0, 0, 0))
        palette.setColor(QPalette.ColorRole.ButtonText, QColor(0, 0, 0))
        palette.setColor(QPalette.ColorRole.BrightText, QColor(255, 0, 0))

        # 按钮颜色
        palette.setColor(QPalette.ColorRole.Button, QColor(240, 240, 240))
        palette.setColor(QPalette.ColorRole.Highlight, QColor(65, 105, 225))
        palette.setColor(QPalette.ColorRole.HighlightedText, QColor(255, 255, 255))

        # 禁用状态
        palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.Text, QColor(160, 160, 160))
        palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.ButtonText, QColor(160, 160, 160))

        app.setPalette(palette)
        app.setStyle("Fusion")

    @staticmethod
    def set_dark_theme(app: QApplication):
        """
        设置深色主题

        Args:
            app: QApplication实例
        """
        palette = QPalette()

        # 背景色
        palette.setColor(QPalette.ColorRole.Window, QColor(53, 53, 53))
        palette.setColor(QPalette.ColorRole.WindowText, QColor(255, 255, 255))
        palette.setColor(QPalette.ColorRole.Base, QColor(25, 25, 25))
        palette.setColor(QPalette.ColorRole.AlternateBase, QColor(53, 53, 53))

        # 文本颜色
        palette.setColor(QPalette.ColorRole.Text, QColor(255, 255, 255))
        palette.setColor(QPalette.ColorRole.ButtonText, QColor(255, 255, 255))
        palette.setColor(QPalette.ColorRole.BrightText, QColor(255, 0, 0))

        # 按钮颜色
        palette.setColor(QPalette.ColorRole.Button, QColor(53, 53, 53))
        palette.setColor(QPalette.ColorRole.Highlight, QColor(65, 105, 225))
        palette.setColor(QPalette.ColorRole.HighlightedText, QColor(255, 255, 255))

        # 禁用状态
        palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.Text, QColor(127, 127, 127))
        palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.ButtonText, QColor(127, 127, 127))

        app.setPalette(palette)
        app.setStyle("Fusion")

    @staticmethod
    def toggle_theme(app: QApplication, is_dark: bool):
        """
        切换主题

        Args:
            app: QApplication实例
            is_dark: 是否切换到深色主题
        """
        if is_dark:
            ThemeManager.set_dark_theme(app)
        else:
            ThemeManager.set_light_theme(app)
