#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
内存管理器模块
提供更高级的内存管理功能，包括内存监控、智能缓存管理和资源限制
"""

import psutil
import os
import gc
import time
from loguru import logger
from typing import Dict, Any, Optional, Callable
import threading
from src.data.data_cache import global_data_cache
from src.utils.memory_optimizer import MemoryOptimizer


class MemoryManager:
    """
    内存管理器
    提供内存监控、智能缓存管理和资源限制功能
    """
    
    def __init__(self, max_memory_percent: float = 70.0):
        """
        初始化内存管理器
        
        Args:
            max_memory_percent: 最大内存使用百分比（相对于总可用内存）
        """
        self.max_memory_percent = max_memory_percent
        self.process = psutil.Process(os.getpid())
        self.memory_thresholds = {
            'critical': 90.0,  # 临界内存使用百分比
            'high': 80.0,       # 高内存使用百分比
            'medium': 60.0,     # 中等内存使用百分比
            'low': 40.0         # 低内存使用百分比
        }
        self.memory_history = []  # 内存使用历史记录
        self.monitoring_enabled = False
        self.monitoring_thread = None
        self.monitoring_interval = 5  # 监控间隔（秒）
        
    def get_memory_usage(self) -> Dict[str, Any]:
        """
        获取当前内存使用情况
        
        Returns:
            Dict[str, Any]: 内存使用信息
        """
        memory_info = self.process.memory_info()
        virtual_memory = psutil.virtual_memory()
        
        # 计算内存使用百分比
        process_memory_percent = (memory_info.rss / 1024 / 1024) / (virtual_memory.total / 1024 / 1024) * 100
        
        return {
            'process_rss_mb': memory_info.rss / 1024 / 1024,  # 进程使用的物理内存
            'process_vms_mb': memory_info.vms / 1024 / 1024,  # 进程使用的虚拟内存
            'process_percent': process_memory_percent,
            'system_total_mb': virtual_memory.total / 1024 / 1024,
            'system_available_mb': virtual_memory.available / 1024 / 1024,
            'system_used_mb': virtual_memory.used / 1024 / 1024,
            'system_percent': virtual_memory.percent,
            'memory_level': self._get_memory_level(process_memory_percent)
        }
    
    def _get_memory_level(self, memory_percent: float) -> str:
        """
        获取内存使用级别
        
        Args:
            memory_percent: 内存使用百分比
            
        Returns:
            str: 内存使用级别
        """
        if memory_percent >= self.memory_thresholds['critical']:
            return 'critical'
        elif memory_percent >= self.memory_thresholds['high']:
            return 'high'
        elif memory_percent >= self.memory_thresholds['medium']:
            return 'medium'
        elif memory_percent >= self.memory_thresholds['low']:
            return 'low'
        else:
            return 'very_low'
    
    def start_monitoring(self):
        """
        启动内存监控
        """
        if not self.monitoring_enabled:
            self.monitoring_enabled = True
            self.monitoring_thread = threading.Thread(target=self._monitor_memory, daemon=True)
            self.monitoring_thread.start()
            logger.info("内存监控已启动")
    
    def stop_monitoring(self):
        """
        停止内存监控
        """
        self.monitoring_enabled = False
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=2)
            logger.info("内存监控已停止")
    
    def _monitor_memory(self):
        """
        内存监控线程函数
        """
        while self.monitoring_enabled:
            try:
                memory_info = self.get_memory_usage()
                self.memory_history.append({
                    'timestamp': time.time(),
                    'memory_usage': memory_info
                })
                
                # 只保留最近100条记录
                if len(self.memory_history) > 100:
                    self.memory_history = self.memory_history[-100:]
                
                # 检查内存使用情况并采取相应措施
                self._check_memory_and_act(memory_info)
                
                time.sleep(self.monitoring_interval)
            except Exception as e:
                logger.error(f"内存监控出错: {str(e)}")
                time.sleep(self.monitoring_interval)
    
    def _check_memory_and_act(self, memory_info: Dict[str, Any]):
        """
        检查内存使用情况并采取相应措施
        
        Args:
            memory_info: 内存使用信息
        """
        memory_level = memory_info['memory_level']
        
        if memory_level == 'critical':
            logger.warning(f"内存使用临界: {memory_info['process_percent']:.2f}%")
            # 采取紧急措施
            self._take_emergency_measures()
        elif memory_level == 'high':
            logger.warning(f"内存使用较高: {memory_info['process_percent']:.2f}%")
            # 采取缓解措施
            self._take_relief_measures()
        elif memory_level == 'medium':
            logger.info(f"内存使用中等: {memory_info['process_percent']:.2f}%")
            # 采取预防措施
            self._take_preventive_measures()
        else:
            # 内存使用正常
            pass
    
    def _take_emergency_measures(self):
        """
        采取紧急内存管理措施
        """
        logger.info("采取紧急内存管理措施")
        
        # 1. 强制垃圾回收
        gc.collect()
        
        # 2. 清空所有缓存
        try:
            global_data_cache.clear()
            logger.info("已清空所有数据缓存")
        except Exception as e:
            logger.error(f"清空缓存失败: {str(e)}")
        
        # 3. 减少线程池大小
        # 注意：这里需要根据实际情况调整
    
    def _take_relief_measures(self):
        """
        采取缓解内存管理措施
        """
        logger.info("采取缓解内存管理措施")
        
        # 1. 执行垃圾回收
        gc.collect()
        
        # 2. 清理过期缓存
        try:
            # 这里可以实现清理过期缓存的逻辑
            logger.info("已清理过期缓存")
        except Exception as e:
            logger.error(f"清理过期缓存失败: {str(e)}")
    
    def _take_preventive_measures(self):
        """
        采取预防内存管理措施
        """
        # 定期执行垃圾回收
        gc.collect()
    
    def optimize_cache(self, target_size: Optional[int] = None):
        """
        优化缓存大小
        
        Args:
            target_size: 目标缓存大小，None表示根据内存情况自动调整
        """
        memory_info = self.get_memory_usage()
        
        if target_size is None:
            # 根据内存使用情况自动调整缓存大小
            if memory_info['memory_level'] in ['high', 'critical']:
                # 减少缓存大小
                new_size = max(100, global_data_cache._max_size // 2)
                logger.info(f"内存使用较高，将缓存大小从 {global_data_cache._max_size} 减少到 {new_size}")
                global_data_cache.set_max_size(new_size)
            elif memory_info['memory_level'] in ['low', 'very_low']:
                # 增加缓存大小
                new_size = min(1000, global_data_cache._max_size * 2)
                logger.info(f"内存使用较低，将缓存大小从 {global_data_cache._max_size} 增加到 {new_size}")
                global_data_cache.set_max_size(new_size)
        else:
            # 使用指定的目标缓存大小
            logger.info(f"将缓存大小设置为 {target_size}")
            global_data_cache.set_max_size(target_size)
    
    def get_memory_stats(self) -> Dict[str, Any]:
        """
        获取内存统计信息
        
        Returns:
            Dict[str, Any]: 内存统计信息
        """
        current_memory = self.get_memory_usage()
        
        # 计算内存使用趋势
        memory_trend = []
        if len(self.memory_history) > 1:
            for i in range(1, len(self.memory_history)):
                prev_usage = self.memory_history[i-1]['memory_usage']['process_percent']
                current_usage = self.memory_history[i]['memory_usage']['process_percent']
                trend = current_usage - prev_usage
                memory_trend.append(trend)
        
        return {
            'current': current_memory,
            'history': self.memory_history[-10:],  # 最近10条记录
            'trend': memory_trend[-5:],  # 最近5个趋势点
            'cache_stats': global_data_cache.get_stats() if global_data_cache else None
        }
    
    def print_memory_stats(self):
        """
        打印内存统计信息
        """
        stats = self.get_memory_stats()
        current = stats['current']
        
        print(f"\n{'='*60}")
        print("内存使用统计")
        print(f"{'='*60}")
        print(f"进程内存: {current['process_rss_mb']:.2f} MB ({current['process_percent']:.1f}%)")
        print(f"系统内存: {current['system_used_mb']:.2f} MB / {current['system_total_mb']:.2f} MB ({current['system_percent']:.1f}%)")
        print(f"内存级别: {current['memory_level']}")
        
        if stats['cache_stats']:
            cache_stats = stats['cache_stats']
            print(f"\n缓存统计:")
            print(f"  缓存大小: {cache_stats['size']} / {cache_stats['max_size']}")
            print(f"  命中率: {cache_stats['hit_rate']:.2f}%")
            print(f"  总请求数: {cache_stats['total_requests']}")
            print(f"  淘汰次数: {cache_stats['evictions']}")
        
        print(f"{'='*60}\n")
    
    def memory_guard(self, func: Callable) -> Callable:
        """
        内存保护装饰器
        在函数执行前后检查内存使用情况，并在必要时采取措施
        
        Args:
            func: 要装饰的函数
            
        Returns:
            Callable: 装饰后的函数
        """
        def wrapper(*args, **kwargs):
            # 执行前检查内存
            pre_memory = self.get_memory_usage()
            
            if pre_memory['memory_level'] in ['high', 'critical']:
                logger.warning(f"执行 {func.__name__} 前内存使用较高: {pre_memory['process_percent']:.2f}%")
                self._take_relief_measures()
            
            # 执行函数
            result = func(*args, **kwargs)
            
            # 执行后检查内存
            post_memory = self.get_memory_usage()
            memory_increase = post_memory['process_rss_mb'] - pre_memory['process_rss_mb']
            
            if memory_increase > 100:  # 内存增加超过100MB
                logger.warning(f"执行 {func.__name__} 后内存增加: {memory_increase:.2f} MB")
                # 执行垃圾回收
                gc.collect()
            
            return result
        
        return wrapper


# 创建全局内存管理器实例
global_memory_manager = MemoryManager(max_memory_percent=70.0)


def get_memory_usage() -> Dict[str, Any]:
    """
    便捷函数：获取当前内存使用情况
    """
    return global_memory_manager.get_memory_usage()


def print_memory_usage():
    """
    便捷函数：打印内存使用情况
    """
    global_memory_manager.print_memory_stats()


def optimize_memory_usage():
    """
    便捷函数：优化内存使用
    """
    global_memory_manager._take_relief_measures()
    gc.collect()
    logger.info("内存使用已优化")


def memory_guard(func: Callable) -> Callable:
    """
    便捷函数：内存保护装饰器
    """
    return global_memory_manager.memory_guard(func)
