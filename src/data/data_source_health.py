#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据源健康检查模块

提供数据源的可用性检查、健康状态监控和故障自动切换支持
"""

import time
import threading
from enum import Enum
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, field
from collections import defaultdict
from loguru import logger


class HealthStatus(Enum):
    """健康状态枚举"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class SourceHealthInfo:
    """数据源健康信息"""
    name: str
    status: HealthStatus = HealthStatus.UNKNOWN
    last_check_time: float = 0.0
    last_success_time: float = 0.0
    last_failure_time: float = 0.0
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    avg_response_time: float = 0.0
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    error_messages: List[str] = field(default_factory=list)
    is_enabled: bool = True


class DataSourceHealthChecker:
    """
    数据源健康检查器

    提供数据源的可用性检查和健康状态监控
    """

    def __init__(self, config=None):
        """
        初始化健康检查器

        Args:
            config: 配置对象
        """
        self.config = config
        self._health_info: Dict[str, SourceHealthInfo] = {}
        self._lock = threading.RLock()
        self._check_callbacks: Dict[str, Callable] = {}
        self._monitoring = False
        self._monitor_thread = None
        self._check_interval = 60

    def register_source(self, source_name: str, check_callback: Optional[Callable] = None):
        """
        注册数据源

        Args:
            source_name: 数据源名称
            check_callback: 健康检查回调函数，返回(bool, dict)表示是否健康及可选的详细信息
        """
        with self._lock:
            if source_name not in self._health_info:
                self._health_info[source_name] = SourceHealthInfo(name=source_name)
                logger.info(f"注册数据源健康检查: {source_name}")

            if check_callback:
                self._check_callbacks[source_name] = check_callback

    def unregister_source(self, source_name: str):
        """
        注销数据源

        Args:
            source_name: 数据源名称
        """
        with self._lock:
            if source_name in self._health_info:
                del self._health_info[source_name]
                logger.info(f"注销数据源健康检查: {source_name}")

            if source_name in self._check_callbacks:
                del self._check_callbacks[source_name]

    def record_request(self, source_name: str, success: bool, response_time: float = 0.0, error_message: str = ""):
        """
        记录请求结果

        Args:
            source_name: 数据源名称
            success: 请求是否成功
            response_time: 响应时间（秒）
            error_message: 错误信息
        """
        with self._lock:
            if source_name not in self._health_info:
                self._health_info[source_name] = SourceHealthInfo(name=source_name)

            info = self._health_info[source_name]
            info.total_requests += 1
            info.last_check_time = time.time()

            if success:
                info.consecutive_successes += 1
                info.consecutive_failures = 0
                info.last_success_time = time.time()
                info.successful_requests += 1

                if info.total_requests > 0:
                    info.avg_response_time = (info.avg_response_time * (info.total_requests - 1) + response_time) / info.total_requests

                if info.avg_response_time < 1.0 and info.consecutive_successes >= 3:
                    info.status = HealthStatus.HEALTHY
                elif info.avg_response_time < 3.0 and info.consecutive_successes >= 2:
                    info.status = HealthStatus.DEGRADED
                else:
                    info.status = HealthStatus.HEALTHY
            else:
                info.consecutive_failures += 1
                info.consecutive_successes = 0
                info.last_failure_time = time.time()
                info.failed_requests += 1

                if info.error_messages:
                    info.error_messages.append(error_message)
                    if len(info.error_messages) > 10:
                        info.error_messages = info.error_messages[-10:]

                if info.consecutive_failures >= 5:
                    info.status = HealthStatus.UNHEALTHY
                elif info.consecutive_failures >= 2:
                    info.status = HealthStatus.DEGRADED

            logger.debug(f"记录数据源{source_name}请求: success={success}, response_time={response_time:.3f}s, status={info.status.value}")

    def get_health_status(self, source_name: str) -> HealthStatus:
        """
        获取数据源健康状态

        Args:
            source_name: 数据源名称

        Returns:
            HealthStatus: 健康状态
        """
        with self._lock:
            if source_name not in self._health_info:
                return HealthStatus.UNKNOWN
            return self._health_info[source_name].status

    def get_health_info(self, source_name: str) -> Optional[SourceHealthInfo]:
        """
        获取数据源健康信息

        Args:
            source_name: 数据源名称

        Returns:
            SourceHealthInfo: 健康信息，如果不存在返回None
        """
        with self._lock:
            return self._health_info.get(source_name)

    def get_all_health_info(self) -> Dict[str, SourceHealthInfo]:
        """
        获取所有数据源的健康信息

        Returns:
            Dict[str, SourceHealthInfo]: 数据源名称到健康信息的映射
        """
        with self._lock:
            return dict(self._health_info)

    def get_healthy_sources(self) -> List[str]:
        """
        获取所有健康的数据源

        Returns:
            List[str]: 健康数据源名称列表
        """
        with self._lock:
            return [
                name for name, info in self._health_info.items()
                if info.status in (HealthStatus.HEALTHY, HealthStatus.DEGRADED) and info.is_enabled
            ]

    def get_best_source(self, sources: List[str]) -> Optional[str]:
        """
        获取最佳数据源

        Args:
            sources: 数据源名称列表

        Returns:
            Optional[str]: 最佳数据源名称，如果都不健康返回None
        """
        with self._lock:
            available = [
                (name, self._health_info.get(name))
                for name in sources
                if name in self._health_info and self._health_info[name].is_enabled
            ]

            if not available:
                return None

            def source_score(info):
                if info is None:
                    return -1000
                if info.status == HealthStatus.UNHEALTHY:
                    return -500
                if info.status == HealthStatus.DEGRADED:
                    return 100 - info.avg_response_time
                if info.status == HealthStatus.HEALTHY:
                    return 1000 - info.avg_response_time
                return -500

            available.sort(key=lambda x: source_score(x[1]), reverse=True)
            return available[0][0] if available else None

    def set_source_enabled(self, source_name: str, enabled: bool):
        """
        设置数据源是否启用

        Args:
            source_name: 数据源名称
            enabled: 是否启用
        """
        with self._lock:
            if source_name in self._health_info:
                self._health_info[source_name].is_enabled = enabled
                logger.info(f"设置数据源{source_name}启用状态: {enabled}")

    def is_source_available(self, source_name: str) -> bool:
        """
        检查数据源是否可用

        Args:
            source_name: 数据源名称

        Returns:
            bool: 是否可用
        """
        with self._lock:
            if source_name not in self._health_info:
                return True
            info = self._health_info[source_name]
            return info.is_enabled and info.status != HealthStatus.UNHEALTHY

    def start_monitoring(self, interval: int = 60):
        """
        开始健康状态监控

        Args:
            interval: 检查间隔（秒）
        """
        self._check_interval = interval
        self._monitoring = True

        if self._monitor_thread is None or not self._monitor_thread.is_alive():
            self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self._monitor_thread.start()
            logger.info(f"开始数据源健康监控，间隔{interval}秒")

    def stop_monitoring(self):
        """停止健康状态监控"""
        self._monitoring = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
            self._monitor_thread = None
        logger.info("停止数据源健康监控")

    def _monitor_loop(self):
        """监控循环"""
        while self._monitoring:
            try:
                self._perform_health_checks()
            except Exception as e:
                logger.warning(f"健康检查监控异常: {e}")

            time.sleep(self._check_interval)

    def _perform_health_checks(self):
        """执行健康检查"""
        with self._lock:
            source_names = list(self._check_callbacks.keys())

        for source_name in source_names:
            try:
                callback = self._check_callbacks.get(source_name)
                if callback:
                    start_time = time.time()
                    try:
                        result = callback()
                        response_time = time.time() - start_time

                        if isinstance(result, tuple) and len(result) >= 2:
                            success, details = result[0], result[1] if len(result) > 1 else {}
                            error_msg = details.get('error', '') if isinstance(details, dict) else ''
                        else:
                            success = bool(result)
                            error_msg = ''

                        self.record_request(source_name, success, response_time, error_msg)
                    except Exception as e:
                        self.record_request(source_name, False, time.time() - start_time, str(e))
            except Exception as e:
                logger.warning(f"执行数据源{source_name}健康检查时出错: {e}")

    def reset_health_info(self, source_name: Optional[str] = None):
        """
        重置健康信息

        Args:
            source_name: 数据源名称，如果为None则重置所有
        """
        with self._lock:
            if source_name:
                if source_name in self._health_info:
                    self._health_info[source_name] = SourceHealthInfo(name=source_name)
            else:
                for name in self._health_info:
                    self._health_info[name] = SourceHealthInfo(name=name)


_global_health_checker = None


def get_global_health_checker() -> DataSourceHealthChecker:
    """
    获取全局健康检查器

    Returns:
        DataSourceHealthChecker: 全局健康检查器实例
    """
    global _global_health_checker
    if _global_health_checker is None:
        _global_health_checker = DataSourceHealthChecker()
    return _global_health_checker
