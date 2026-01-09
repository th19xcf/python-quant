#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
通信监控模块
用于监控和统计插件间的通信情况
"""

import time
from typing import Dict, Any, List, Optional
from collections import defaultdict, deque
from loguru import logger
from src.utils.message_validator import MessageValidator


class CommunicationMonitor:
    """
    通信监控类
    """
    
    def __init__(self):
        self.enabled = True
        self.debug_mode = False
        
        # 通信统计
        self.stats = {
            'total_messages': 0,
            'total_requests': 0,
            'total_responses': 0,
            'total_events': 0,
            'message_count_by_type': defaultdict(int),
            'message_count_by_sender': defaultdict(int),
            'message_count_by_recipient': defaultdict(int),
            'avg_response_time': 0.0,
            'response_times': deque(maxlen=1000),  # 最近1000条响应的延迟
            'pending_requests': defaultdict(dict),  # 等待响应的请求
            'error_count': 0,
            'last_reset_time': time.time()
        }
        
        # 通信日志
        self.log_enabled = True
        self.log_queue = deque(maxlen=10000)  # 最近10000条通信日志
        
        # 通信关系图
        self.communication_graph = defaultdict(set)  # sender -> {recipient1, recipient2, ...}
    
    def enable(self):
        """
        启用监控
        """
        self.enabled = True
        logger.info("通信监控已启用")
    
    def disable(self):
        """
        禁用监控
        """
        self.enabled = False
        logger.info("通信监控已禁用")
    
    def enable_debug(self):
        """
        启用调试模式
        """
        self.debug_mode = True
        logger.info("通信监控调试模式已启用")
    
    def disable_debug(self):
        """
        禁用调试模式
        """
        self.debug_mode = False
        logger.info("通信监控调试模式已禁用")
    
    def enable_log(self):
        """
        启用通信日志
        """
        self.log_enabled = True
        logger.info("通信日志已启用")
    
    def disable_log(self):
        """
        禁用通信日志
        """
        self.log_enabled = False
        logger.info("通信日志已禁用")
    
    def reset_stats(self):
        """
        重置统计数据
        """
        self.stats = {
            'total_messages': 0,
            'total_requests': 0,
            'total_responses': 0,
            'total_events': 0,
            'message_count_by_type': defaultdict(int),
            'message_count_by_sender': defaultdict(int),
            'message_count_by_recipient': defaultdict(int),
            'avg_response_time': 0.0,
            'response_times': deque(maxlen=1000),
            'pending_requests': defaultdict(dict),
            'error_count': 0,
            'last_reset_time': time.time()
        }
        logger.info("通信统计已重置")
    
    def log_message(self, message: Dict[str, Any]):
        """
        记录通信日志
        
        Args:
            message: 消息内容
        """
        if not self.enabled:
            return
        
        # 验证消息格式
        if not MessageValidator.is_valid(message):
            logger.warning(f"收到格式无效的消息: {message}")
            self.stats['error_count'] += 1
            return
        
        # 标准化消息
        normalized_message = MessageValidator.normalize_message(message)
        
        # 记录日志
        if self.log_enabled:
            log_entry = {
                'timestamp': time.time(),
                'message': normalized_message
            }
            self.log_queue.append(log_entry)
        
        # 调试日志
        if self.debug_mode:
            logger.debug(f"通信消息: {normalized_message}")
        
        # 更新统计数据
        self._update_stats(normalized_message)
        
        # 更新通信关系图
        self._update_communication_graph(normalized_message)
    
    def _update_stats(self, message: Dict[str, Any]):
        """
        更新统计数据
        
        Args:
            message: 消息内容
        """
        # 总消息数
        self.stats['total_messages'] += 1
        
        # 按类型统计
        message_type = message['message_type']
        self.stats['message_count_by_type'][message_type] += 1
        
        # 按发送者统计
        sender = message['sender']
        self.stats['message_count_by_sender'][sender] += 1
        
        # 按接收者统计
        recipient = message['recipient']
        self.stats['message_count_by_recipient'][recipient] += 1
        
        # 请求-响应统计
        if message_type == 'request':
            self.stats['total_requests'] += 1
            # 记录请求，等待响应
            self.stats['pending_requests'][message['correlation_id']] = {
                'timestamp': message['timestamp'],
                'sender': sender,
                'recipient': recipient,
                'method': message['data'].get('method', '')
            }
        elif message_type == 'response':
            self.stats['total_responses'] += 1
            # 计算响应时间
            correlation_id = message['correlation_id']
            if correlation_id in self.stats['pending_requests']:
                request_info = self.stats['pending_requests'].pop(correlation_id)
                response_time = message['timestamp'] - request_info['timestamp']
                self.stats['response_times'].append(response_time)
                # 更新平均响应时间
                if self.stats['response_times']:
                    self.stats['avg_response_time'] = sum(self.stats['response_times']) / len(self.stats['response_times'])
        elif message_type == 'event':
            self.stats['total_events'] += 1
    
    def _update_communication_graph(self, message: Dict[str, Any]):
        """
        更新通信关系图
        
        Args:
            message: 消息内容
        """
        sender = message['sender']
        recipient = message['recipient']
        
        if recipient != '*':
            self.communication_graph[sender].add(recipient)
        else:
            # 广播消息，不更新关系图
            pass
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取统计数据
        
        Returns:
            Dict[str, Any]: 统计数据
        """
        return {
            **self.stats,
            'response_count': len(self.stats['response_times']),
            'current_pending_requests': len(self.stats['pending_requests']),
            'uptime': time.time() - self.stats['last_reset_time']
        }
    
    def get_logs(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        获取通信日志
        
        Args:
            limit: 返回日志数量限制
            
        Returns:
            List[Dict[str, Any]]: 日志列表
        """
        return list(self.log_queue)[-limit:]
    
    def get_communication_graph(self) -> Dict[str, List[str]]:
        """
        获取通信关系图
        
        Returns:
            Dict[str, List[str]]: 通信关系图
        """
        return {sender: list(recipients) for sender, recipients in self.communication_graph.items()}
    
    def get_top_senders(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        获取发送消息最多的插件
        
        Args:
            limit: 返回数量限制
            
        Returns:
            List[Dict[str, Any]]: 发送者列表
        """
        top_senders = sorted(
            self.stats['message_count_by_sender'].items(),
            key=lambda x: x[1],
            reverse=True
        )[:limit]
        return [{'sender': sender, 'count': count} for sender, count in top_senders]
    
    def get_top_recipients(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        获取接收消息最多的插件
        
        Args:
            limit: 返回数量限制
            
        Returns:
            List[Dict[str, Any]]: 接收者列表
        """
        top_recipients = sorted(
            self.stats['message_count_by_recipient'].items(),
            key=lambda x: x[1],
            reverse=True
        )[:limit]
        return [{'recipient': recipient, 'count': count} for recipient, count in top_recipients]
    
    def get_response_time_stats(self) -> Dict[str, Any]:
        """
        获取响应时间统计
        
        Returns:
            Dict[str, Any]: 响应时间统计
        """
        if not self.stats['response_times']:
            return {
                'avg': 0.0,
                'min': 0.0,
                'max': 0.0,
                'p50': 0.0,
                'p90': 0.0,
                'p95': 0.0,
                'p99': 0.0
            }
        
        response_times = sorted(self.stats['response_times'])
        n = len(response_times)
        return {
            'avg': sum(response_times) / n,
            'min': response_times[0],
            'max': response_times[-1],
            'p50': response_times[int(n * 0.5)],
            'p90': response_times[int(n * 0.9)],
            'p95': response_times[int(n * 0.95)],
            'p99': response_times[int(n * 0.99)]
        }
    
    def export_stats(self) -> Dict[str, Any]:
        """
        导出统计数据
        
        Returns:
            Dict[str, Any]: 完整统计数据
        """
        return {
            'stats': self.get_stats(),
            'response_time_stats': self.get_response_time_stats(),
            'top_senders': self.get_top_senders(),
            'top_recipients': self.get_top_recipients(),
            'communication_graph': self.get_communication_graph(),
            'export_time': time.time()
        }
    
    def clear_logs(self):
        """
        清空通信日志
        """
        self.log_queue.clear()
        logger.info("通信日志已清空")
    
    def clear_graph(self):
        """
        清空通信关系图
        """
        self.communication_graph.clear()
        logger.info("通信关系图已清空")
    
    def __str__(self) -> str:
        """
        字符串表示
        """
        stats = self.get_stats()
        response_stats = self.get_response_time_stats()
        
        return f"通信监控统计:\n" \
               f"  总消息数: {stats['total_messages']}\n" \
               f"  请求数: {stats['total_requests']}\n" \
               f"  响应数: {stats['total_responses']}\n" \
               f"  事件数: {stats['total_events']}\n" \
               f"  错误数: {stats['error_count']}\n" \
               f"  待处理请求: {stats['current_pending_requests']}\n" \
               f"  平均响应时间: {response_stats['avg']:.3f}秒\n" \
               f"  响应时间范围: {response_stats['min']:.3f} - {response_stats['max']:.3f}秒\n" \
               f"  监控时长: {stats['uptime']:.1f}秒\n" \
               f"  通信关系数: {sum(len(recipients) for recipients in self.communication_graph.values())}"


# 创建全局通信监控实例
global_monitor = CommunicationMonitor()


def get_monitor() -> CommunicationMonitor:
    """
    获取全局通信监控实例
    
    Returns:
        CommunicationMonitor: 通信监控实例
    """
    return global_monitor


def monitor_message(message: Dict[str, Any]):
    """
    监控消息
    
    Args:
        message: 消息内容
    """
    global_monitor.log_message(message)