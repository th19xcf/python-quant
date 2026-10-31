#!/usr/bin/env python3

"""
监控系统模块，提供数据源响应时间和缓存使用情况的监控功能

系统级性能（CPU/内存/磁盘/网络）采样曾由 SystemMonitor 承担，但全仓从未调用
MonitoringSystem.start()，该采样线程从未运行；进程级内存监控由
src.utils.memory_manager.MemoryManager 实际承担（main.py 启动时开启），故移除
这条休眠链路，避免两套监控设施并存。
"""

import threading
import time
from collections import defaultdict, deque
from typing import Any, Dict

from loguru import logger


class DataSourceMonitor:
    """
    数据源监控器，记录数据源响应时间和成功率
    """

    def __init__(self, max_history: int = 100):
        """
        初始化数据源监控器

        Args:
            max_history: 每个数据源保存的最大历史记录数
        """
        self.response_times = defaultdict(lambda: deque(maxlen=max_history))
        self.success_rates = defaultdict(lambda: {'success': 0, 'total': 0})
        self.lock = threading.Lock()

    def record_request(self, source_name: str, response_time: float, success: bool):
        """
        记录数据源请求

        Args:
            source_name: 数据源名称
            response_time: 响应时间（秒）
            success: 是否成功
        """
        with self.lock:
            # 记录响应时间
            self.response_times[source_name].append({
                'timestamp': time.time(),
                'response_time': response_time,
                'success': success
            })

            # 更新成功率
            self.success_rates[source_name]['total'] += 1
            if success:
                self.success_rates[source_name]['success'] += 1

    def get_stats(self, source_name: str | None = None) -> Dict[str, Any]:
        """
        获取监控统计信息

        Args:
            source_name: 数据源名称，None表示所有数据源

        Returns:
            dict: 统计信息
        """
        with self.lock:
            if source_name:
                return self._get_source_stats(source_name)
            else:
                stats = {}
                for name in self.response_times:
                    stats[name] = self._get_source_stats(name)
                return stats

    def _get_source_stats(self, source_name: str) -> Dict[str, Any]:
        """
        获取单个数据源的统计信息
        """
        times = self.response_times.get(source_name, deque())
        rates = self.success_rates.get(source_name, {'success': 0, 'total': 0})

        if not times:
            avg_response_time = 0
            max_response_time = 0
            min_response_time = 0
        else:
            response_times = [item['response_time'] for item in times]
            avg_response_time = sum(response_times) / len(response_times)
            max_response_time = max(response_times)
            min_response_time = min(response_times)

        success_rate = rates['success'] / rates['total'] if rates['total'] > 0 else 0

        # 计算最近5分钟的统计
        recent_5min = []
        current_time = time.time()
        for item in times:
            if current_time - item['timestamp'] <= 300:  # 5分钟
                recent_5min.append(item)

        recent_avg = 0
        recent_success_rate = 0
        if recent_5min:
            recent_response_times = [item['response_time'] for item in recent_5min]
            recent_avg = sum(recent_response_times) / len(recent_response_times)
            recent_success = sum(1 for item in recent_5min if item['success'])
            recent_success_rate = recent_success / len(recent_5min)

        return {
            'avg_response_time': avg_response_time,
            'max_response_time': max_response_time,
            'min_response_time': min_response_time,
            'success_rate': success_rate,
            'total_requests': rates['total'],
            'successful_requests': rates['success'],
            'recent_5min': {
                'avg_response_time': recent_avg,
                'success_rate': recent_success_rate,
                'request_count': len(recent_5min)
            },
            'last_requests': list(times)[-10:]  # 最近10次请求
        }

    def log_stats(self):
        """
        记录监控统计信息到日志
        """
        stats = self.get_stats()
        for source_name, stat in stats.items():
            logger.info(f"数据源 {source_name} 监控统计: "
                      f"平均响应时间={stat['avg_response_time']:.3f}s, "
                      f"最大响应时间={stat['max_response_time']:.3f}s, "
                      f"成功率={stat['success_rate']:.2f}, "
                      f"总请求数={stat['total_requests']}, "
                      f"最近5分钟请求数={stat['recent_5min']['request_count']}")

    def reset(self):
        """
        重置监控数据
        """
        with self.lock:
            self.response_times.clear()
            self.success_rates.clear()


class CacheStatsRegistry:
    """
    缓存统计登记表，记录由外部上报的各命名缓存命中/淘汰指标
    （与直接读取 global_data_cache/global_indicator_cache 的 log_cache_stats 职责不同）
    """

    def __init__(self):
        """
        初始化缓存监控器
        """
        self.cache_stats = defaultdict(lambda: {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'size': 0,
            'max_size': 0,
            'last_update': time.time()
        })
        self.lock = threading.Lock()

    def update_stats(self, cache_name: str, stats: Dict[str, Any]):
        """
        更新缓存统计信息

        Args:
            cache_name: 缓存名称
            stats: 缓存统计信息
        """
        with self.lock:
            self.cache_stats[cache_name].update({
                'hits': stats.get('hits', 0),
                'misses': stats.get('misses', 0),
                'evictions': stats.get('evictions', 0),
                'size': stats.get('size', 0),
                'max_size': stats.get('max_size', 0),
                'last_update': time.time()
            })

    def get_stats(self, cache_name: str | None = None) -> Dict[str, Any]:
        """
        获取缓存统计信息

        Args:
            cache_name: 缓存名称，None表示所有缓存

        Returns:
            dict: 统计信息
        """
        with self.lock:
            if cache_name:
                return self.cache_stats.get(cache_name, {})
            else:
                return dict(self.cache_stats)

    def log_stats(self):
        """
        记录缓存统计信息到日志
        """
        stats = self.get_stats()
        for cache_name, stat in stats.items():
            total = stat['hits'] + stat['misses']
            hit_rate = stat['hits'] / total if total > 0 else 0
            logger.info(f"缓存 {cache_name} 监控统计: "
                      f"命中率={hit_rate:.2f}, "
                      f"当前大小={stat['size']}, "
                      f"最大大小={stat['max_size']}, "
                      f"驱逐次数={stat['evictions']}")

    def reset(self):
        """
        重置监控数据
        """
        with self.lock:
            self.cache_stats.clear()


class MonitoringSystem:
    """
    监控系统，整合数据源与缓存监控
    """

    def __init__(self):
        """
        初始化监控系统
        """
        self.data_source_monitor = DataSourceMonitor()
        self.cache_monitor = CacheStatsRegistry()

    def record_data_source_request(self, source_name: str, response_time: float, success: bool):
        """
        记录数据源请求

        Args:
            source_name: 数据源名称
            response_time: 响应时间（秒）
            success: 是否成功
        """
        self.data_source_monitor.record_request(source_name, response_time, success)

    def update_cache_stats(self, cache_name: str, stats: Dict[str, Any]):
        """
        更新缓存统计信息

        Args:
            cache_name: 缓存名称
            stats: 缓存统计信息
        """
        self.cache_monitor.update_stats(cache_name, stats)

    def get_all_stats(self) -> Dict[str, Any]:
        """
        获取所有监控统计信息

        Returns:
            dict: 所有监控统计信息
        """
        return {
            'data_sources': self.data_source_monitor.get_stats(),
            'cache': self.cache_monitor.get_stats(),
        }

    def log_all_stats(self):
        """
        记录所有监控统计信息到日志
        """
        logger.info("=== 监控系统统计信息 ===")
        self.data_source_monitor.log_stats()
        self.cache_monitor.log_stats()
        logger.info("====================")

    def reset_all(self):
        """
        重置所有监控数据
        """
        self.data_source_monitor.reset()
        self.cache_monitor.reset()
        logger.info("监控系统数据已重置")


# 创建全局监控系统实例
global_monitoring_system = MonitoringSystem()


# 监控装饰器
def monitor_data_source(func):
    """
    数据源监控装饰器
    """
    def wrapper(*args, **kwargs):
        source_name = args[0].__class__.__name__ if args else "unknown"
        start_time = time.time()
        success = True

        try:
            result = func(*args, **kwargs)
        except Exception:
            success = False
            raise
        finally:
            response_time = time.time() - start_time
            global_monitoring_system.record_data_source_request(source_name, response_time, success)

        return result

    return wrapper


def monitor_cache(func):
    """
    缓存监控装饰器
    """
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)

        # 假设第一个参数是缓存实例
        if args and hasattr(args[0], 'get_stats'):
            cache_name = args[0].__class__.__name__
            stats = args[0].get_stats()
            global_monitoring_system.update_cache_stats(cache_name, stats)

        return result

    return wrapper


def log_cache_stats():
    """
    记录数据缓存与指标缓存的统计信息（原 cache_monitor.log_cache_stats）

    直接读取两个全局缓存的 get_stats()，不依赖额外的监控器实例。
    """
    # 延迟导入：避免 utils.monitoring 在导入期就拉起 data/tech_analysis 依赖
    from src.data.data_cache import global_data_cache
    from src.tech_analysis.indicator_cache import global_indicator_cache

    data_stats = global_data_cache.get_stats()
    indicator_stats = global_indicator_cache.get_stats()

    logger.info("缓存监控统计:")
    logger.info("  数据缓存:")
    logger.info(f"    缓存大小: {data_stats['size']}/{data_stats['max_size']}")
    logger.info(f"    命中次数: {data_stats['hits']}")
    logger.info(f"    未命中次数: {data_stats['misses']}")
    logger.info(f"    命中率: {data_stats['hit_rate']:.2%}")
    logger.info(f"    淘汰次数: {data_stats['evictions']}")
    logger.info("  指标缓存:")
    logger.info(f"    缓存大小: {indicator_stats['size']}/{indicator_stats['max_size']}")
    logger.info(f"    命中次数: {indicator_stats['hits']}")
    logger.info(f"    未命中次数: {indicator_stats['misses']}")
    logger.info(f"    命中率: {indicator_stats['hit_rate']:.2%}")
    logger.info(f"    淘汰次数: {indicator_stats['evictions']}")
    logger.info("  总计:")
    logger.info(f"    总缓存大小: {data_stats['size'] + indicator_stats['size']}")
    logger.info(f"    总命中次数: {data_stats['hits'] + indicator_stats['hits']}")
    logger.info(f"    总未命中次数: {data_stats['misses'] + indicator_stats['misses']}")
