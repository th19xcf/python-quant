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
import tracemalloc
import linecache
from loguru import logger
from typing import Dict, Any, Optional, Callable, List, Tuple
import threading
import json
from datetime import datetime
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
        self.tracemalloc_enabled = False
        self.memory_snapshots = []  # 内存快照
        self.auto_optimization_enabled = True  # 自动优化开关
        self.memory_usage_patterns = []  # 内存使用模式
        self.data_structure_memory = {}  # 数据结构内存使用
        self.linecache_trim_threshold = 5000  # 超过该条目数时清理 linecache
        
        # 启动tracemalloc
        try:
            tracemalloc.start()
            self.tracemalloc_enabled = True
            logger.info("内存跟踪已启动")
        except Exception as e:
            logger.warning(f"启动内存跟踪失败: {e}")
    
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
            'memory_level': self._get_memory_level(process_memory_percent),
            'timestamp': time.time()
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
                
                # 分析内存使用模式
                self._analyze_memory_patterns()
                
                # 检查内存使用情况并采取相应措施
                if self.auto_optimization_enabled:
                    self._check_memory_and_act(memory_info)
                
                # 定期拍摄内存快照
                if self.tracemalloc_enabled and len(self.memory_history) % 10 == 0:
                    self._take_memory_snapshot()
                
                # 动态调整监控间隔
                self._adjust_monitoring_interval(memory_info)
                
                time.sleep(self.monitoring_interval)
            except Exception as e:
                logger.error(f"内存监控出错: {str(e)}")
                time.sleep(self.monitoring_interval)
    
    def _analyze_memory_patterns(self):
        """
        分析内存使用模式
        """
        if len(self.memory_history) < 10:
            return
        
        # 计算最近10分钟的内存使用趋势
        recent_history = self.memory_history[-10:]
        memory_values = [h['memory_usage']['process_percent'] for h in recent_history]
        
        # 计算平均值和标准差
        avg_memory = sum(memory_values) / len(memory_values)
        std_memory = (sum((x - avg_memory) ** 2 for x in memory_values) / len(memory_values)) ** 0.5
        
        # 检测内存使用模式
        pattern = {
            'timestamp': time.time(),
            'average_memory': avg_memory,
            'std_memory': std_memory,
            'trend': 'stable' if std_memory < 5 else 'volatile',
            'level': self._get_memory_level(avg_memory)
        }
        
        self.memory_usage_patterns.append(pattern)
        
        # 只保留最近20个模式
        if len(self.memory_usage_patterns) > 20:
            self.memory_usage_patterns = self.memory_usage_patterns[-20:]
    
    def _adjust_monitoring_interval(self, memory_info: Dict[str, Any]):
        """
        动态调整监控间隔
        
        Args:
            memory_info: 内存使用信息
        """
        memory_level = memory_info['memory_level']
        
        # 根据内存使用级别调整监控间隔
        if memory_level in ['critical', 'high']:
            # 内存使用较高时，增加监控频率
            self.monitoring_interval = 2
        elif memory_level == 'medium':
            # 内存使用中等时，保持默认监控频率
            self.monitoring_interval = 5
        else:
            # 内存使用较低时，减少监控频率
            self.monitoring_interval = 10
    
    def _take_memory_snapshot(self):
        """
        拍摄内存快照
        """
        try:
            # 先裁剪 linecache，减少 stdlib 缓存对快照结果的干扰
            self._trim_linecache_cache()

            snapshot = tracemalloc.take_snapshot()
            self.memory_snapshots.append({
                'timestamp': time.time(),
                'snapshot': snapshot
            })
            
            # 只保留最近5个快照
            if len(self.memory_snapshots) > 5:
                self.memory_snapshots = self.memory_snapshots[-5:]
            
            # 分析快照，检测内存泄漏
            self._analyze_memory_snapshot(snapshot)
        except Exception as e:
            logger.error(f"拍摄内存快照失败: {e}")
    
    def _analyze_memory_snapshot(self, snapshot):
        """
        分析内存快照，检测内存泄漏
        
        Args:
            snapshot: 内存快照
        """
        try:
            # 按内存使用量排序，取前10个
            top_stats = snapshot.statistics('lineno')
            
            logger.debug("内存使用前10项:")
            for stat in top_stats[:10]:
                logger.debug(f"  {stat}")
            
            # 检测内存泄漏
            if len(self.memory_snapshots) >= 2:
                prev_snapshot = self.memory_snapshots[-2]['snapshot']
                stats = snapshot.compare_to(prev_snapshot, 'lineno')
                
                # 检测增长的内存使用
                growing_stats = [stat for stat in stats if stat.size_diff > 1024 * 1024]  # 只关注增长超过1MB的

                # 过滤标准库 linecache 噪音，避免误报“内存泄漏”
                leak_candidates = []
                filtered_noise = []
                for stat in growing_stats:
                    trace_text = str(stat.traceback).lower()
                    if 'linecache.py' in trace_text:
                        filtered_noise.append(stat)
                        continue
                    leak_candidates.append(stat)

                if filtered_noise:
                    logger.info(
                        f"检测到 {len(filtered_noise)} 条 linecache 缓存增长，"
                        "已按缓存噪音忽略"
                    )
                
                if leak_candidates:
                    logger.warning("检测到可能的内存泄漏:")
                    for stat in leak_candidates[:5]:
                        logger.warning(f"  {stat}")
        except Exception as e:
            logger.error(f"分析内存快照失败: {e}")

    def _trim_linecache_cache(self):
        """
        清理 linecache 缓存，降低快照分析噪音。
        """
        try:
            cache_size = len(linecache.cache)
            if cache_size >= self.linecache_trim_threshold:
                linecache.clearcache()
                logger.debug(f"linecache 缓存已清理，原条目数: {cache_size}")
        except Exception as e:
            logger.debug(f"裁剪 linecache 缓存失败: {e}")
    
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
        
        # 3. 清理linecache
        try:
            linecache.clearcache()
            logger.info("已清空linecache缓存")
        except Exception as e:
            logger.error(f"清空linecache失败: {str(e)}")
        
        # 4. 减少线程池大小
        # 注意：这里需要根据实际情况调整
        
        # 5. 记录内存使用情况
        self._log_memory_crisis()
    
    def _take_relief_measures(self):
        """
        采取缓解内存管理措施
        """
        logger.info("采取缓解内存管理措施")
        
        # 1. 执行垃圾回收
        gc.collect()
        
        # 2. 清理过期缓存
        try:
            # 清理过期缓存
            self._clean_expired_cache()
            logger.info("已清理过期缓存")
        except Exception as e:
            logger.error(f"清理过期缓存失败: {str(e)}")
        
        # 3. 清理linecache
        try:
            linecache.clearcache()
            logger.info("已清空linecache缓存")
        except Exception as e:
            logger.error(f"清空linecache失败: {str(e)}")
        
        # 4. 优化缓存大小
        self.optimize_cache()
    
    def _take_preventive_measures(self):
        """
        采取预防内存管理措施
        """
        # 定期执行垃圾回收
        gc.collect()
        
        # 分析内存使用趋势，预测未来内存需求
        self._predict_memory_usage()
    
    def _clean_expired_cache(self):
        """
        清理过期缓存
        """
        # 这里可以实现清理过期缓存的逻辑
        # 例如，遍历缓存条目，删除过期的条目
        pass
    
    def _log_memory_crisis(self):
        """
        记录内存危机情况
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"memory_crisis_{timestamp}.json"
        
        try:
            crisis_info = {
                'timestamp': time.time(),
                'memory_info': self.get_memory_usage(),
                'memory_history': self.memory_history[-20:],
                'cache_stats': global_data_cache.get_stats() if global_data_cache else None
            }
            
            with open(log_file, 'w', encoding='utf-8') as f:
                json.dump(crisis_info, f, ensure_ascii=False, indent=2)
            
            logger.info(f"内存危机信息已记录到 {log_file}")
        except Exception as e:
            logger.error(f"记录内存危机信息失败: {e}")
    
    def _predict_memory_usage(self):
        """
        预测内存使用趋势
        """
        if len(self.memory_history) < 5:
            return
        
        # 使用简单的线性回归预测内存使用
        recent_history = self.memory_history[-5:]
        times = [h['timestamp'] for h in recent_history]
        memory_values = [h['memory_usage']['process_percent'] for h in recent_history]
        
        # 计算趋势
        if len(times) > 1:
            # 计算斜率
            slope = (memory_values[-1] - memory_values[0]) / (times[-1] - times[0])
            
            # 预测5分钟后的内存使用
            predicted_memory = memory_values[-1] + slope * 300
            
            if predicted_memory > self.memory_thresholds['high']:
                logger.warning(f"预测5分钟后内存使用将达到 {predicted_memory:.2f}%，接近高内存阈值")
                # 提前采取预防措施
                self._take_preventive_measures()
    
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
        
        # 计算内存使用模式
        recent_patterns = self.memory_usage_patterns[-5:] if self.memory_usage_patterns else []
        
        return {
            'current': current_memory,
            'history': self.memory_history[-10:],  # 最近10条记录
            'trend': memory_trend[-5:],  # 最近5个趋势点
            'patterns': recent_patterns,
            'cache_stats': global_data_cache.get_stats() if global_data_cache else None,
            'snapshot_count': len(self.memory_snapshots)
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
        
        if stats['patterns']:
            recent_pattern = stats['patterns'][-1]
            print(f"\n内存使用模式:")
            print(f"  平均内存: {recent_pattern['average_memory']:.2f}%")
            print(f"  内存波动: {recent_pattern['std_memory']:.2f}%")
            print(f"  趋势: {recent_pattern['trend']}")
        
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
    
    def enable_auto_optimization(self, enabled: bool):
        """
        启用或禁用自动内存优化
        
        Args:
            enabled: 是否启用自动优化
        """
        self.auto_optimization_enabled = enabled
        logger.info(f"自动内存优化已{'启用' if enabled else '禁用'}")
    
    def get_memory_analysis(self) -> Dict[str, Any]:
        """
        获取内存使用分析报告
        
        Returns:
            Dict[str, Any]: 内存分析报告
        """
        analysis = {
            'summary': self.get_memory_stats(),
            'recommendations': self._generate_memory_recommendations(),
            'snapshot_analysis': self._analyze_latest_snapshot(),
            'memory_trends': self._analyze_memory_trends()
        }
        
        return analysis
    
    def _generate_memory_recommendations(self) -> List[str]:
        """
        生成内存优化建议
        
        Returns:
            List[str]: 优化建议
        """
        recommendations = []
        memory_info = self.get_memory_usage()
        
        if memory_info['memory_level'] in ['high', 'critical']:
            recommendations.append("当前内存使用较高，建议减少缓存大小")
            recommendations.append("考虑优化数据结构，减少内存占用")
            recommendations.append("检查是否存在内存泄漏")
        
        if len(self.memory_usage_patterns) > 0:
            recent_pattern = self.memory_usage_patterns[-1]
            if recent_pattern['trend'] == 'volatile':
                recommendations.append("内存使用波动较大，建议检查代码中的内存管理")
        
        return recommendations
    
    def _analyze_latest_snapshot(self) -> Dict[str, Any]:
        """
        分析最新的内存快照
        
        Returns:
            Dict[str, Any]: 快照分析结果
        """
        if not self.memory_snapshots:
            return {"error": "无内存快照"}
        
        try:
            latest_snapshot = self.memory_snapshots[-1]['snapshot']
            top_stats = latest_snapshot.statistics('lineno')
            
            top_items = []
            for stat in top_stats[:10]:
                top_items.append({
                    'filename': stat.filename,
                    'lineno': stat.lineno,
                    'size': stat.size,
                    'size_str': f"{stat.size / 1024 / 1024:.2f} MB"
                })
            
            return {
                'top_memory_usage': top_items,
                'timestamp': self.memory_snapshots[-1]['timestamp']
            }
        except Exception as e:
            return {"error": str(e)}
    
    def _analyze_memory_trends(self) -> Dict[str, Any]:
        """
        分析内存使用趋势
        
        Returns:
            Dict[str, Any]: 趋势分析结果
        """
        if len(self.memory_history) < 5:
            return {"error": "历史数据不足"}
        
        # 计算最近30分钟的内存趋势
        recent_history = [h for h in self.memory_history if time.time() - h['timestamp'] < 1800]
        
        if len(recent_history) < 5:
            return {"error": "最近30分钟数据不足"}
        
        memory_values = [h['memory_usage']['process_percent'] for h in recent_history]
        times = [h['timestamp'] for h in recent_history]
        
        # 计算趋势
        avg_memory = sum(memory_values) / len(memory_values)
        max_memory = max(memory_values)
        min_memory = min(memory_values)
        
        # 计算斜率
        slope = (memory_values[-1] - memory_values[0]) / (times[-1] - times[0])
        
        trend = "stable"
        if slope > 0.01:
            trend = "increasing"
        elif slope < -0.01:
            trend = "decreasing"
        
        return {
            'average_memory': avg_memory,
            'max_memory': max_memory,
            'min_memory': min_memory,
            'trend': trend,
            'slope': slope,
            'data_points': len(recent_history)
        }
    
    def save_memory_report(self, filename: str = "memory_report.json"):
        """
        保存内存使用报告
        
        Args:
            filename: 报告文件名
        """
        try:
            report = {
                'timestamp': time.time(),
                'analysis': self.get_memory_analysis(),
                'system_info': {
                    'python_version': f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
                    'os': os.name,
                    'cpu_count': os.cpu_count()
                }
            }
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            
            logger.info(f"内存使用报告已保存到 {filename}")
        except Exception as e:
            logger.error(f"保存内存使用报告失败: {e}")


# 导入sys模块
import sys

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


def enable_auto_memory_optimization(enabled: bool):
    """
    便捷函数：启用或禁用自动内存优化
    
    Args:
        enabled: 是否启用自动优化
    """
    global_memory_manager.enable_auto_optimization(enabled)


def get_memory_analysis() -> Dict[str, Any]:
    """
    便捷函数：获取内存使用分析报告
    
    Returns:
        Dict[str, Any]: 内存分析报告
    """
    return global_memory_manager.get_memory_analysis()


def save_memory_report(filename: str = "memory_report.json"):
    """
    便捷函数：保存内存使用报告
    
    Args:
        filename: 报告文件名
    """
    global_memory_manager.save_memory_report(filename)
