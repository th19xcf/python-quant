#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
全局异常处理器模块
"""

import sys
import traceback
import time
from typing import Dict, Any, Optional
from loguru import logger
from PySide6.QtWidgets import QApplication, QMessageBox

from src.utils.exceptions import QuantException, is_retryable_error, get_error_severity


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
        # 错误统计
        self.error_stats = {
            "total_errors": 0,
            "error_types": {},
            "last_error_time": None
        }
    
    def handle_exception(self, exc_type, exc_value, exc_traceback):
        """
        处理全局异常
        
        Args:
            exc_type: 异常类型
            exc_value: 异常值
            exc_traceback: 异常回溯
        """
        # 更新错误统计
        self._update_error_stats(exc_type)
        
        # 生成错误详情
        error_details = self._generate_error_details(exc_type, exc_value, exc_traceback)
        
        # 记录异常信息
        logger.exception(f"未捕获的异常: {error_details['error_message']}")
        
        # 显示错误对话框
        self._show_error_dialog(error_details)
        
        # 调用原始的异常处理器（如果需要）
        # self.original_excepthook(exc_type, exc_value, exc_traceback)
    
    def _update_error_stats(self, exc_type):
        """
        更新错误统计信息
        
        Args:
            exc_type: 异常类型
        """
        self.error_stats["total_errors"] += 1
        error_type_name = exc_type.__name__
        if error_type_name not in self.error_stats["error_types"]:
            self.error_stats["error_types"][error_type_name] = 0
        self.error_stats["error_types"][error_type_name] += 1
        self.error_stats["last_error_time"] = time.time()
    
    def _generate_error_details(self, exc_type, exc_value, exc_traceback) -> Dict[str, Any]:
        """
        生成错误详情
        
        Args:
            exc_type: 异常类型
            exc_value: 异常值
            exc_traceback: 异常回溯
            
        Returns:
            Dict[str, Any]: 错误详情
        """
        error_message = f"{exc_type.__name__}: {exc_value}"
        error_traceback = ''.join(traceback.format_tb(exc_traceback))
        error_severity = get_error_severity(exc_value)
        retryable = is_retryable_error(exc_value)
        
        # 提取业务异常信息
        if isinstance(exc_value, QuantException):
            error_code = exc_value.error_code
            error_details = exc_value.details
        else:
            error_code = "UNKNOWN_ERROR"
            error_details = {}
        
        return {
            "error_message": error_message,
            "error_traceback": error_traceback,
            "error_severity": error_severity,
            "retryable": retryable,
            "error_code": error_code,
            "error_details": error_details
        }
    
    def _show_error_dialog(self, error_details: Dict[str, Any]):
        """
        显示错误对话框
        
        Args:
            error_details: 错误详情
        """
        try:
            app = QApplication.instance()
            if app:
                # 根据错误严重程度显示不同的对话框
                if error_details["error_severity"] == "CRITICAL":
                    QMessageBox.critical(
                        None, 
                        "严重错误",
                        f"应用程序遇到严重错误，可能需要重启。\n\n" 
                        f"{error_details['error_message']}\n\n" 
                        "详细错误信息已记录到日志文件中。"
                    )
                else:
                    QMessageBox.warning(
                        None, 
                        "应用程序错误",
                        f"很抱歉，应用程序遇到了一个错误。\n\n" 
                        f"{error_details['error_message']}\n\n" 
                        "详细错误信息已记录到日志文件中。\n" 
                        "您可以继续使用应用程序，但某些功能可能不可用。"
                    )
        except Exception as e:
            # 确保即使在显示对话框时出错也能继续
            logger.error(f"显示错误对话框时出错: {e}")
    
    def get_error_stats(self) -> Dict[str, Any]:
        """
        获取错误统计信息
        
        Returns:
            Dict[str, Any]: 错误统计
        """
        return self.error_stats
    
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
    from src.utils.exceptions import format_exception_for_user
    
    if isinstance(exception, QuantException):
        return format_exception_for_user(exception)
    
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


def handle_exception_with_retry(max_retries: int = 3, retry_delay: float = 1.0):
    """
    带重试机制的异常处理装饰器
    
    Args:
        max_retries: 最大重试次数
        retry_delay: 重试间隔（秒）
        
    Returns:
        Callable: 包装后的函数
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            retries = 0
            last_exception = None
            
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if not is_retryable_error(e):
                        raise
                    
                    last_exception = e
                    retries += 1
                    if retries < max_retries:
                        logger.warning(f"操作失败，{retry_delay}秒后重试 ({retries}/{max_retries}): {e}")
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"操作失败，已达到最大重试次数: {e}")
            
            if last_exception:
                raise last_exception
        
        return wrapper
    
    return decorator


def handle_error_gracefully(error_handler=None):
    """
    优雅处理错误的装饰器
    
    Args:
        error_handler: 自定义错误处理函数
        
    Returns:
        Callable: 包装后的函数
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.exception(f"执行{func.__name__}时出错: {e}")
                
                if error_handler:
                    return error_handler(e, *args, **kwargs)
                
                # 默认错误处理
                if isinstance(e, QuantException):
                    return {"success": False, "error": e.to_dict()}
                else:
                    return {"success": False, "error": {
                        "error_code": "UNKNOWN_ERROR",
                        "message": str(e),
                        "exception_type": type(e).__name__
                    }}
        
        return wrapper
    
    return decorator
