#!/usr/bin/env python3

"""
UI 错误提示公共处理

action_manager 与 table_interaction_manager 共用同一套状态栏提示逻辑，
统一在此收口，避免两个管理器各自维护一份重复实现。
"""

from loguru import logger


def show_data_read_error(window, error_msg: str) -> None:
    """数据读取错误：状态栏提示 + 隐藏进度条"""
    window.statusBar().showMessage(error_msg, 5000)
    logger.error(f"数据读取错误: {error_msg}")
    if hasattr(window, 'market_progress_bar'):
        window.market_progress_bar.setVisible(False)


def show_indicator_calculate_error(window, error_msg: str) -> None:
    """指标计算错误：记录日志 + 状态栏提示"""
    logger.error(f"指标计算错误: {error_msg}")
    window.statusBar().showMessage(error_msg, 5000)
