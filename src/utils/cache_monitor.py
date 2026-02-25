#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
缓存监控模块，用于监控和统计缓存使用情况
"""

from datetime import datetime
from typing import Dict, Any, List
from loguru import logger
from src.data.data_cache import global_data_cache
from src.tech_analysis.indicator_cache import global_indicator_cache


class CacheMonitor:
    """
    缓存监控类，用于监控和统计缓存使用情况
    """
    
    def __init__(self):
        """
        初始化缓存监控器
        """
        self.start_time = datetime.now()
        self.monitoring_history = []
    
    def collect_stats(self) -> Dict[str, Any]:
        """
        收集所有缓存的统计信息
        
        Returns:
            Dict[str, Any]: 缓存统计信息
        """
        # 收集数据缓存统计
        data_cache_stats = global_data_cache.get_stats()
        
        # 收集指标缓存统计
        indicator_cache_stats = global_indicator_cache.get_stats()
        
        # 计算监控时间
        uptime = (datetime.now() - self.start_time).total_seconds()
        
        stats = {
            'timestamp': datetime.now().isoformat(),
            'uptime_seconds': uptime,
            'data_cache': data_cache_stats,
            'indicator_cache': indicator_cache_stats,
            'total_cache_size': data_cache_stats['size'] + indicator_cache_stats['size'],
            'total_hits': data_cache_stats['hits'] + indicator_cache_stats['hits'],
            'total_misses': data_cache_stats['misses'] + indicator_cache_stats['misses'],
            'total_hit_rate': (data_cache_stats['hits'] + indicator_cache_stats['hits']) / \
                           ((data_cache_stats['hits'] + indicator_cache_stats['hits'] + 
                             data_cache_stats['misses'] + indicator_cache_stats['misses']) or 1)
        }
        
        # 添加到历史记录
        self.monitoring_history.append(stats)
        
        # 限制历史记录数量
        if len(self.monitoring_history) > 100:
            self.monitoring_history = self.monitoring_history[-100:]
        
        return stats
    
    def log_stats(self):
        """
        记录缓存统计信息到日志
        """
        stats = self.collect_stats()
        
        logger.info("缓存监控统计:")
        logger.info(f"  监控时间: {stats['timestamp']}")
        logger.info(f"  系统运行时间: {stats['uptime_seconds']:.2f}秒")
        logger.info(f"  数据缓存:")
        logger.info(f"    缓存大小: {stats['data_cache']['size']}/{stats['data_cache']['max_size']}")
        logger.info(f"    命中次数: {stats['data_cache']['hits']}")
        logger.info(f"    未命中次数: {stats['data_cache']['misses']}")
        logger.info(f"    命中率: {stats['data_cache']['hit_rate']:.2%}")
        logger.info(f"    淘汰次数: {stats['data_cache']['evictions']}")
        logger.info(f"  指标缓存:")
        logger.info(f"    缓存大小: {stats['indicator_cache']['size']}/{stats['indicator_cache']['max_size']}")
        logger.info(f"    命中次数: {stats['indicator_cache']['hits']}")
        logger.info(f"    未命中次数: {stats['indicator_cache']['misses']}")
        logger.info(f"    命中率: {stats['indicator_cache']['hit_rate']:.2%}")
        logger.info(f"    淘汰次数: {stats['indicator_cache']['evictions']}")
        logger.info(f"  总计:")
        logger.info(f"    总缓存大小: {stats['total_cache_size']}")
        logger.info(f"    总命中次数: {stats['total_hits']}")
        logger.info(f"    总未命中次数: {stats['total_misses']}")
        logger.info(f"    总命中率: {stats['total_hit_rate']:.2%}")
    
    def get_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        获取缓存监控历史记录
        
        Args:
            limit: 返回记录数量限制
        
        Returns:
            List[Dict[str, Any]]: 缓存监控历史记录
        """
        return self.monitoring_history[-limit:]
    
    def get_summary(self) -> Dict[str, Any]:
        """
        获取缓存监控摘要
        
        Returns:
            Dict[str, Any]: 缓存监控摘要
        """
        if not self.monitoring_history:
            return {}
        
        # 计算平均值
        avg_stats = {
            'avg_data_cache_size': sum(s['data_cache']['size'] for s in self.monitoring_history) / len(self.monitoring_history),
            'avg_indicator_cache_size': sum(s['indicator_cache']['size'] for s in self.monitoring_history) / len(self.monitoring_history),
            'avg_data_cache_hit_rate': sum(s['data_cache']['hit_rate'] for s in self.monitoring_history) / len(self.monitoring_history),
            'avg_indicator_cache_hit_rate': sum(s['indicator_cache']['hit_rate'] for s in self.monitoring_history) / len(self.monitoring_history),
            'avg_total_hit_rate': sum(s['total_hit_rate'] for s in self.monitoring_history) / len(self.monitoring_history),
            'total_evictions': sum(s['data_cache']['evictions'] + s['indicator_cache']['evictions'] for s in self.monitoring_history)
        }
        
        # 获取最新统计
        latest_stats = self.monitoring_history[-1]
        
        summary = {
            'start_time': self.start_time.isoformat(),
            'latest_timestamp': latest_stats['timestamp'],
            'uptime_seconds': latest_stats['uptime_seconds'],
            'latest_stats': latest_stats,
            'average_stats': avg_stats,
            'history_count': len(self.monitoring_history)
        }
        
        return summary
    
    def reset(self):
        """
        重置缓存监控
        """
        self.start_time = datetime.now()
        self.monitoring_history = []
        
        # 重置缓存统计
        global_data_cache.reset_stats()
        global_indicator_cache.reset_stats()
        
        logger.info("缓存监控已重置")


# 创建全局缓存监控实例
global_cache_monitor = CacheMonitor()


def log_cache_stats():
    """
    记录缓存统计信息的便捷函数
    """
    global_cache_monitor.log_stats()


def get_cache_summary():
    """
    获取缓存监控摘要的便捷函数
    
    Returns:
        Dict[str, Any]: 缓存监控摘要
    """
    return global_cache_monitor.get_summary()
