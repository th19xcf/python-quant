#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
监控系统模块，提供数据源响应时间、缓存使用情况和系统性能的监控功能
"""

import time
import threading
import psutil
from collections import defaultdict, deque
from datetime import datetime
from typing import Dict, List, Any, Optional
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
    
    def get_stats(self, source_name: Optional[str] = None) -> Dict[str, Any]:
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
                for name in self.response_times.keys():
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


class CacheMonitor:
    """
    缓存监控器，记录缓存使用情况
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
    
    def get_stats(self, cache_name: Optional[str] = None) -> Dict[str, Any]:
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


class SystemMonitor:
    """
    系统监控器，记录系统性能指标
    """
    
    def __init__(self, interval: int = 60):
        """
        初始化系统监控器
        
        Args:
            interval: 监控间隔（秒）
        """
        self.interval = interval
        self.cpu_usage = deque(maxlen=60)  # 保存1小时的数据
        self.memory_usage = deque(maxlen=60)
        self.disk_usage = deque(maxlen=60)
        self.network_usage = deque(maxlen=60)
        self.lock = threading.Lock()
        self.running = False
        self.thread = None
    
    def start(self):
        """
        启动系统监控
        """
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self.thread.start()
            logger.info("系统监控已启动")
    
    def stop(self):
        """
        停止系统监控
        """
        if self.running:
            self.running = False
            if self.thread:
                self.thread.join()
            logger.info("系统监控已停止")
    
    def _monitor_loop(self):
        """
        监控循环
        """
        while self.running:
            try:
                # 收集系统指标
                cpu = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory().percent
                disk = psutil.disk_usage('/').percent
                net_io = psutil.net_io_counters()
                
                with self.lock:
                    self.cpu_usage.append({
                        'timestamp': time.time(),
                        'value': cpu
                    })
                    self.memory_usage.append({
                        'timestamp': time.time(),
                        'value': memory
                    })
                    self.disk_usage.append({
                        'timestamp': time.time(),
                        'value': disk
                    })
                    self.network_usage.append({
                        'timestamp': time.time(),
                        'bytes_sent': net_io.bytes_sent,
                        'bytes_recv': net_io.bytes_recv
                    })
                
                time.sleep(self.interval - 1)  # 减去1秒的cpu_percent等待时间
            except Exception as e:
                logger.error(f"系统监控出错: {e}")
                time.sleep(self.interval)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取系统统计信息
        
        Returns:
            dict: 系统统计信息
        """
        with self.lock:
            cpu_values = [item['value'] for item in self.cpu_usage]
            memory_values = [item['value'] for item in self.memory_usage]
            disk_values = [item['value'] for item in self.disk_usage]
            
            avg_cpu = sum(cpu_values) / len(cpu_values) if cpu_values else 0
            avg_memory = sum(memory_values) / len(memory_values) if memory_values else 0
            avg_disk = sum(disk_values) / len(disk_values) if disk_values else 0
            
            return {
                'cpu': {
                    'avg': avg_cpu,
                    'last': cpu_values[-1] if cpu_values else 0,
                    'max': max(cpu_values) if cpu_values else 0,
                    'min': min(cpu_values) if cpu_values else 0,
                    'history': list(self.cpu_usage)
                },
                'memory': {
                    'avg': avg_memory,
                    'last': memory_values[-1] if memory_values else 0,
                    'max': max(memory_values) if memory_values else 0,
                    'min': min(memory_values) if memory_values else 0,
                    'history': list(self.memory_usage)
                },
                'disk': {
                    'avg': avg_disk,
                    'last': disk_values[-1] if disk_values else 0,
                    'max': max(disk_values) if disk_values else 0,
                    'min': min(disk_values) if disk_values else 0,
                    'history': list(self.disk_usage)
                },
                'network': {
                    'history': list(self.network_usage)
                }
            }
    
    def log_stats(self):
        """
        记录系统统计信息到日志
        """
        stats = self.get_stats()
        logger.info(f"系统监控统计: "
                  f"CPU使用率={stats['cpu']['last']:.1f}% (平均={stats['cpu']['avg']:.1f}%), "
                  f"内存使用率={stats['memory']['last']:.1f}% (平均={stats['memory']['avg']:.1f}%), "
                  f"磁盘使用率={stats['disk']['last']:.1f}% (平均={stats['disk']['avg']:.1f}%)")
    
    def reset(self):
        """
        重置监控数据
        """
        with self.lock:
            self.cpu_usage.clear()
            self.memory_usage.clear()
            self.disk_usage.clear()
            self.network_usage.clear()


class MonitoringSystem:
    """
    监控系统，整合所有监控器
    """
    
    def __init__(self):
        """
        初始化监控系统
        """
        self.data_source_monitor = DataSourceMonitor()
        self.cache_monitor = CacheMonitor()
        self.system_monitor = SystemMonitor()
    
    def start(self):
        """
        启动监控系统
        """
        self.system_monitor.start()
        logger.info("监控系统已启动")
    
    def stop(self):
        """
        停止监控系统
        """
        self.system_monitor.stop()
        logger.info("监控系统已停止")
    
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
            'system': self.system_monitor.get_stats()
        }
    
    def log_all_stats(self):
        """
        记录所有监控统计信息到日志
        """
        logger.info("=== 监控系统统计信息 ===")
        self.data_source_monitor.log_stats()
        self.cache_monitor.log_stats()
        self.system_monitor.log_stats()
        logger.info("====================")
    
    def reset_all(self):
        """
        重置所有监控数据
        """
        self.data_source_monitor.reset()
        self.cache_monitor.reset()
        self.system_monitor.reset()
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
        except Exception as e:
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
