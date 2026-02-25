#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
任务管理器模块，负责管理后台线程任务，提供任务状态跟踪和取消功能
"""

from PySide6.QtCore import QObject, Signal, QThreadPool, QRunnable
from PySide6.QtWidgets import QApplication
from loguru import logger
import uuid
from typing import Dict, Any, Optional, Callable


class TaskSignals(QObject):
    """
    任务信号类，定义任务相关的信号
    """
    started = Signal(str, str)  # 任务开始信号 (task_id, task_name)
    progress = Signal(str, int, int)  # 任务进度信号 (task_id, current, total)
    completed = Signal(str, Any)  # 任务完成信号 (task_id, result)
    error = Signal(str, str)  # 任务错误信号 (task_id, error_message)
    finished = Signal(str)  # 任务结束信号 (task_id)


class TaskRunner(QRunnable):
    """
    任务运行器，用于在后台线程中执行任务
    """
    
    def __init__(self, task_id: str, task_name: str, func: Callable, args: tuple = (), kwargs: dict = {}):
        """
        初始化任务运行器
        
        Args:
            task_id: 任务ID
            task_name: 任务名称
            func: 要执行的函数
            args: 函数参数
            kwargs: 函数关键字参数
        """
        super().__init__()
        self.task_id = task_id
        self.task_name = task_name
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.signals = TaskSignals()
        self.is_cancelled = False
    
    def run(self):
        """
        运行任务
        """
        import time
        start_time = time.time()
        
        try:
            # 发送任务开始信号
            self.signals.started.emit(self.task_id, self.task_name)
            
            # 执行任务，添加超时检查
            result = self._run_with_timeout()
            
            # 发送任务完成信号
            self.signals.completed.emit(self.task_id, result)
            
        except Exception as e:
            error_msg = f"任务执行失败: {str(e)}"
            logger.exception(error_msg)
            self.signals.error.emit(self.task_id, error_msg)
        finally:
            # 发送任务结束信号
            elapsed_time = time.time() - start_time
            logger.info(f"任务完成: {self.task_name} (ID: {self.task_id})，耗时: {elapsed_time:.2f}秒")
            self.signals.finished.emit(self.task_id)
    
    def _run_with_timeout(self, timeout_seconds=60):
        """
        带超时的任务执行
        
        Args:
            timeout_seconds: 超时时间（秒），默认60秒
            
        Returns:
            任务执行结果
            
        Raises:
            TimeoutError: 任务执行超时
        """
        import threading
        import concurrent.futures
        
        # 使用线程池执行任务，支持超时
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.func, *self.args, **self.kwargs, task_id=self.task_id, signals=self.signals)
            try:
                # 等待任务完成，超时后抛出异常
                return future.result(timeout=timeout_seconds)
            except concurrent.futures.TimeoutError:
                # 任务超时
                self.is_cancelled = True
                raise TimeoutError(f"任务执行超时: {self.task_name} (ID: {self.task_id})，超过 {timeout_seconds} 秒")
    
    def cancel(self):
        """
        取消任务
        """
        self.is_cancelled = True


class TaskManager(QObject):
    """
    任务管理器，负责管理后台任务
    """
    
    task_started = Signal(str, str)  # 任务开始信号 (task_id, task_name)
    task_progress = Signal(str, int, int)  # 任务进度信号 (task_id, current, total)
    task_completed = Signal(str, Any)  # 任务完成信号 (task_id, result)
    task_error = Signal(str, str)  # 任务错误信号 (task_id, error_message)
    task_finished = Signal(str)  # 任务结束信号 (task_id)
    
    def __init__(self):
        """
        初始化任务管理器
        """
        super().__init__()
        self.thread_pool = QThreadPool()
        self.thread_pool.setMaxThreadCount(8)  # 设置最大线程数
        self.tasks: Dict[str, TaskRunner] = {}
        # 任务统计信息
        self.task_stats = {
            'total': 0,
            'completed': 0,
            'failed': 0,
            'cancelled': 0,
            'running': 0
        }
        logger.info(f"任务管理器初始化完成，最大线程数: {self.thread_pool.maxThreadCount()}")
    
    def create_task(self, task_name: str, func: Callable, args: tuple = (), kwargs: dict = {}) -> str:
        """
        创建并启动一个后台任务
        
        Args:
            task_name: 任务名称
            func: 要执行的函数
            args: 函数参数
            kwargs: 函数关键字参数
            
        Returns:
            str: 任务ID
        """
        task_id = str(uuid.uuid4())
        runner = TaskRunner(task_id, task_name, func, args, kwargs)
        
        # 连接信号
        runner.signals.started.connect(self._on_task_started)
        runner.signals.progress.connect(self._on_task_progress)
        runner.signals.completed.connect(self._on_task_completed)
        runner.signals.error.connect(self._on_task_error)
        runner.signals.finished.connect(self._on_task_finished)
        
        # 添加到任务列表
        self.tasks[task_id] = runner
        
        # 更新任务统计
        self.task_stats['total'] += 1
        self.task_stats['running'] += 1
        
        # 启动任务
        self.thread_pool.start(runner)
        logger.info(f"任务已启动: {task_name} (ID: {task_id})")
        
        return task_id
    
    def cancel_task(self, task_id: str):
        """
        取消任务
        
        Args:
            task_id: 任务ID
        """
        if task_id in self.tasks:
            runner = self.tasks[task_id]
            runner.cancel()
            logger.info(f"任务已取消: {runner.task_name} (ID: {task_id})")
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        获取任务状态
        
        Args:
            task_id: 任务ID
            
        Returns:
            dict: 任务状态信息
        """
        if task_id in self.tasks:
            runner = self.tasks[task_id]
            return {
                'task_id': task_id,
                'task_name': runner.task_name,
                'is_running': True
            }
        return None
    
    def get_all_tasks(self) -> Dict[str, Dict[str, Any]]:
        """
        获取所有任务
        
        Returns:
            dict: 所有任务信息
        """
        return {
            task_id: {
                'task_name': runner.task_name,
                'is_running': True
            }
            for task_id, runner in self.tasks.items()
        }
    
    def clear_completed_tasks(self):
        """
        清理已完成的任务
        """
        completed_tasks = [task_id for task_id in self.tasks if task_id not in self.tasks]
        for task_id in completed_tasks:
            del self.tasks[task_id]
    
    def shutdown(self):
        """
        关闭任务管理器
        """
        self.thread_pool.clear()
        self.thread_pool.waitForDone()
        logger.info("任务管理器已关闭")
    
    def _on_task_started(self, task_id: str, task_name: str):
        """
        任务开始回调
        """
        self.task_started.emit(task_id, task_name)
    
    def _on_task_progress(self, task_id: str, current: int, total: int):
        """
        任务进度回调
        """
        self.task_progress.emit(task_id, current, total)
    
    def _on_task_completed(self, task_id: str, result: Any):
        """
        任务完成回调
        """
        self.task_stats['completed'] += 1
        self.task_completed.emit(task_id, result)
    
    def _on_task_error(self, task_id: str, error_message: str):
        """
        任务错误回调
        """
        self.task_stats['failed'] += 1
        self.task_error.emit(task_id, error_message)
    
    def _on_task_finished(self, task_id: str):
        """
        任务结束回调
        """
        if task_id in self.tasks:
            runner = self.tasks[task_id]
            if runner.is_cancelled:
                self.task_stats['cancelled'] += 1
            del self.tasks[task_id]
        
        # 确保running计数不为负数
        if self.task_stats['running'] > 0:
            self.task_stats['running'] -= 1
        
        self.task_finished.emit(task_id)
    
    def get_task_stats(self) -> Dict[str, int]:
        """
        获取任务统计信息
        
        Returns:
            dict: 任务统计信息
        """
        return self.task_stats
    
    def reset_task_stats(self):
        """
        重置任务统计信息
        """
        self.task_stats = {
            'total': 0,
            'completed': 0,
            'failed': 0,
            'cancelled': 0,
            'running': 0
        }
        logger.info("任务统计信息已重置")


# 创建全局任务管理器实例
global_task_manager = TaskManager()
