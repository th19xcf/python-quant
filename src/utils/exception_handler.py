#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
全局异常处理器模块
"""

import sys
import traceback
from loguru import logger
from PySide6.QtWidgets import QApplication, QMessageBox


class GlobalExceptionHandler:
    """
    全局异常处理器，用于捕获和处理未捕获的异常
    """
    
    def __init__(self):
        """
        初始化全局异常处理器
        """
        # 保存原始的异常处理器
        self.original_excepthook = sys.excepthook
        # 安装全局异常处理器
        sys.excepthook = self.handle_exception
    
    def handle_exception(self, exc_type, exc_value, exc_traceback):
        """
        处理全局异常
        
        Args:
            exc_type: 异常类型
            exc_value: 异常值
            exc_traceback: 异常回溯
        """
        # 记录异常信息
        logger.exception(f"未捕获的异常: {exc_type.__name__}: {exc_value}")
        
        # 生成详细的错误信息
        error_msg = f"{exc_type.__name__}: {exc_value}\n\n"
        error_msg += "详细错误信息:\n"
        error_msg += ''.join(traceback.format_tb(exc_traceback))
        
        # 显示错误对话框
        try:
            app = QApplication.instance()
            if app:
                QMessageBox.critical(
                    None, 
                    "应用程序错误",
                    f"很抱歉，应用程序遇到了一个错误。\n\n" 
                    f"{exc_type.__name__}: {exc_value}\n\n" 
                    "详细错误信息已记录到日志文件中。\n" 
                    "您可以继续使用应用程序，但某些功能可能不可用。"
                )
        except Exception as e:
            # 确保即使在显示对话框时出错也能继续
            logger.error(f"显示错误对话框时出错: {e}")
        
        # 调用原始的异常处理器（如果需要）
        # self.original_excepthook(exc_type, exc_value, exc_traceback)
    
    def restore(self):
        """
        恢复原始的异常处理器
        """
        sys.excepthook = self.original_excepthook


# 创建全局异常处理器实例
global_exception_handler = GlobalExceptionHandler()


def setup_global_exception_handler():
    """
    设置全局异常处理器
    """
    global global_exception_handler
    return global_exception_handler


def get_error_message(exception):
    """
    获取异常的友好错误信息
    
    Args:
        exception: 异常对象
        
    Returns:
        str: 友好的错误信息
    """
    error_map = {
        FileNotFoundError: "文件不存在或无法访问",
        PermissionError: "权限不足，无法访问文件或目录",
        ConnectionError: "网络连接失败，请检查网络设置",
        TimeoutError: "操作超时，请稍后重试",
        ImportError: "模块导入失败，可能缺少依赖",
        ValueError: "输入值无效，请检查输入参数",
        TypeError: "类型错误，参数类型不匹配",
        KeyError: "键不存在，可能是配置错误",
        RuntimeError: "运行时错误，操作无法完成",
        OSError: "操作系统错误，请检查系统状态"
    }
    
    error_msg = error_map.get(type(exception), "发生未知错误")
    return f"{error_msg}: {str(exception)}"
