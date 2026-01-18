#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
服务发现模块，用于管理和发现系统中的服务
"""

from typing import Any, Dict, List, Optional, Type, Union
import threading
import time
from datetime import datetime, timedelta
from loguru import logger


class ServiceInfo:
    """服务信息类"""
    
    def __init__(self, name: str, address: str, port: int, version: str = "1.0.0", 
                 service_type: str = "http", metadata: Dict[str, Any] = None):
        """初始化服务信息
        
        Args:
            name: 服务名称
            address: 服务地址
            port: 服务端口
            version: 服务版本
            service_type: 服务类型，如http, grpc等
            metadata: 服务元数据
        """
        self.name = name
        self.address = address
        self.port = port
        self.version = version
        self.service_type = service_type
        self.metadata = metadata or {}
        self.created_at = datetime.now()
        self.updated_at = datetime.now()
        self.last_heartbeat = datetime.now()
        self.health_status = "healthy"
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式
        
        Returns:
            Dict[str, Any]: 服务信息字典
        """
        return {
            "name": self.name,
            "address": self.address,
            "port": self.port,
            "version": self.version,
            "service_type": self.service_type,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "last_heartbeat": self.last_heartbeat.isoformat(),
            "health_status": self.health_status
        }
    
    def update_heartbeat(self):
        """更新心跳时间"""
        self.last_heartbeat = datetime.now()
        self.updated_at = datetime.now()
    
    def set_health_status(self, status: str):
        """设置健康状态
        
        Args:
            status: 健康状态，如healthy, unhealthy, unknown
        """
        self.health_status = status
        self.updated_at = datetime.now()
    
    def update_metadata(self, metadata: Dict[str, Any]):
        """更新服务元数据
        
        Args:
            metadata: 新的元数据
        """
        self.metadata.update(metadata)
        self.updated_at = datetime.now()
    
    def is_healthy(self) -> bool:
        """检查服务是否健康
        
        Returns:
            bool: 是否健康
        """
        return self.health_status == "healthy"
    
    def is_alive(self, timeout: timedelta = timedelta(seconds=30)) -> bool:
        """检查服务是否存活（基于心跳）
        
        Args:
            timeout: 心跳超时时间
        
        Returns:
            bool: 是否存活
        """
        return (datetime.now() - self.last_heartbeat) < timeout


class ServiceDiscovery:
    """服务发现类"""
    
    def __init__(self, heartbeat_timeout: int = 30, health_check_interval: int = 60):
        """初始化服务发现
        
        Args:
            heartbeat_timeout: 心跳超时时间（秒）
            health_check_interval: 健康检查间隔（秒）
        """
        self._services: Dict[str, List[ServiceInfo]] = {}
        self._lock = threading.RLock()
        self._heartbeat_timeout = timedelta(seconds=heartbeat_timeout)
        self._health_check_interval = health_check_interval
        self._health_check_thread = None
        self._running = False
    
    def start(self):
        """启动服务发现
        
        启动健康检查线程
        """
        if not self._running:
            self._running = True
            self._health_check_thread = threading.Thread(target=self._health_check_loop, daemon=True)
            self._health_check_thread.start()
            logger.info("服务发现已启动")
    
    def stop(self):
        """停止服务发现
        
        停止健康检查线程
        """
        if self._running:
            self._running = False
            if self._health_check_thread:
                self._health_check_thread.join(timeout=5.0)
            logger.info("服务发现已停止")
    
    def register_service(self, service_info: ServiceInfo) -> str:
        """注册服务
        
        Args:
            service_info: 服务信息
        
        Returns:
            str: 服务ID
        """
        with self._lock:
            # 按服务名称分组
            if service_info.name not in self._services:
                self._services[service_info.name] = []
            
            # 检查是否已存在相同的服务
            for existing_service in self._services[service_info.name]:
                if (existing_service.address == service_info.address and 
                    existing_service.port == service_info.port and
                    existing_service.version == service_info.version):
                    # 更新现有服务的信息
                    existing_service.update_heartbeat()
                    existing_service.update_metadata(service_info.metadata)
                    logger.info(f"服务{service_info.name}已存在，更新心跳和元数据")
                    return f"{service_info.name}_{service_info.address}_{service_info.port}"
            
            # 添加新服务
            self._services[service_info.name].append(service_info)
            logger.info(f"服务{service_info.name}注册成功: {service_info.address}:{service_info.port}")
            return f"{service_info.name}_{service_info.address}_{service_info.port}"
    
    def deregister_service(self, service_id: str) -> bool:
        """注销服务
        
        Args:
            service_id: 服务ID，格式：name_address_port
        
        Returns:
            bool: 注销是否成功
        """
        with self._lock:
            try:
                # 解析服务ID
                parts = service_id.split("_")
                if len(parts) < 3:
                    logger.error(f"无效的服务ID格式: {service_id}")
                    return False
                
                name = parts[0]
                address = parts[1]
                port = int(parts[2])
                
                # 查找并移除服务
                if name in self._services:
                    for i, service in enumerate(self._services[name]):
                        if service.address == address and service.port == port:
                            del self._services[name][i]
                            logger.info(f"服务{service_id}注销成功")
                            
                            # 如果该服务名称下没有服务了，移除该服务名称
                            if not self._services[name]:
                                del self._services[name]
                            
                            return True
                
                logger.warning(f"服务{service_id}不存在")
                return False
            except Exception as e:
                logger.exception(f"注销服务{service_id}失败: {e}")
                return False
    
    def get_service(self, name: str, version: str = None, 
                   healthy_only: bool = True) -> Optional[ServiceInfo]:
        """获取单个服务
        
        Args:
            name: 服务名称
            version: 服务版本，None表示任意版本
            healthy_only: 是否只返回健康的服务
        
        Returns:
            Optional[ServiceInfo]: 服务信息
        """
        services = self.get_services(name, version, healthy_only)
        if services:
            return services[0]
        return None
    
    def get_services(self, name: str, version: str = None, 
                    healthy_only: bool = True) -> List[ServiceInfo]:
        """获取服务列表
        
        Args:
            name: 服务名称，None表示获取所有服务
            version: 服务版本，None表示任意版本
            healthy_only: 是否只返回健康的服务
        
        Returns:
            List[ServiceInfo]: 服务列表
        """
        with self._lock:
            result = []
            
            if name:
                # 获取指定名称的服务
                if name in self._services:
                    result = self._services[name].copy()
            else:
                # 获取所有服务
                for service_list in self._services.values():
                    result.extend(service_list)
            
            # 过滤版本
            if version:
                result = [service for service in result if service.version == version]
            
            # 过滤健康状态
            if healthy_only:
                result = [service for service in result 
                         if service.is_healthy() and service.is_alive(self._heartbeat_timeout)]
            
            return result
    
    def get_all_services(self) -> Dict[str, List[ServiceInfo]]:
        """获取所有服务
        
        Returns:
            Dict[str, List[ServiceInfo]]: 按服务名称分组的服务列表
        """
        with self._lock:
            return self._services.copy()
    
    def heartbeat(self, service_id: str) -> bool:
        """更新服务心跳
        
        Args:
            service_id: 服务ID
        
        Returns:
            bool: 更新是否成功
        """
        with self._lock:
            try:
                # 解析服务ID
                parts = service_id.split("_")
                if len(parts) < 3:
                    logger.error(f"无效的服务ID格式: {service_id}")
                    return False
                
                name = parts[0]
                address = parts[1]
                port = int(parts[2])
                
                # 查找服务并更新心跳
                if name in self._services:
                    for service in self._services[name]:
                        if service.address == address and service.port == port:
                            service.update_heartbeat()
                            return True
                
                logger.warning(f"服务{service_id}不存在，心跳更新失败")
                return False
            except Exception as e:
                logger.exception(f"更新服务{service_id}心跳失败: {e}")
                return False
    
    def update_service(self, service_id: str, metadata: Dict[str, Any] = None, 
                      health_status: str = None) -> bool:
        """更新服务信息
        
        Args:
            service_id: 服务ID
            metadata: 新的元数据
            health_status: 新的健康状态
        
        Returns:
            bool: 更新是否成功
        """
        with self._lock:
            try:
                # 解析服务ID
                parts = service_id.split("_")
                if len(parts) < 3:
                    logger.error(f"无效的服务ID格式: {service_id}")
                    return False
                
                name = parts[0]
                address = parts[1]
                port = int(parts[2])
                
                # 查找服务并更新信息
                if name in self._services:
                    for service in self._services[name]:
                        if service.address == address and service.port == port:
                            if metadata:
                                service.update_metadata(metadata)
                            if health_status:
                                service.set_health_status(health_status)
                            service.update_heartbeat()
                            return True
                
                logger.warning(f"服务{service_id}不存在，更新失败")
                return False
            except Exception as e:
                logger.exception(f"更新服务{service_id}信息失败: {e}")
                return False
    
    def _health_check_loop(self):
        """健康检查循环"""
        while self._running:
            try:
                self._perform_health_check()
            except Exception as e:
                logger.exception(f"健康检查失败: {e}")
            
            # 等待下一次检查
            time.sleep(self._health_check_interval)
    
    def _perform_health_check(self):
        """执行健康检查
        
        检查所有服务的健康状态，标记超时的服务为不健康
        """
        with self._lock:
            current_time = datetime.now()
            services_to_remove = []
            
            # 遍历所有服务
            for service_name, service_list in self._services.items():
                for service in service_list:
                    # 检查心跳是否超时
                    if (current_time - service.last_heartbeat) > self._heartbeat_timeout:
                        service.set_health_status("unhealthy")
                        logger.warning(f"服务{service_name}@{service.address}:{service.port}心跳超时，标记为不健康")
                    
                    # 检查是否长时间不健康，考虑移除
                    if (current_time - service.last_heartbeat) > self._heartbeat_timeout * 2:
                        services_to_remove.append((service_name, service))
            
            # 移除长时间不健康的服务
            for service_name, service in services_to_remove:
                if service in self._services[service_name]:
                    self._services[service_name].remove(service)
                    logger.info(f"服务{service_name}@{service.address}:{service.port}长时间不健康，已移除")
                
                # 如果该服务名称下没有服务了，移除该服务名称
                if service_name in self._services and not self._services[service_name]:
                    del self._services[service_name]


class ServiceClient:
    """服务客户端基类"""
    
    def __init__(self, service_discovery: ServiceDiscovery, service_name: str, 
                 version: str = None):
        """初始化服务客户端
        
        Args:
            service_discovery: 服务发现实例
            service_name: 服务名称
            version: 服务版本
        """
        self.service_discovery = service_discovery
        self.service_name = service_name
        self.version = version
    
    def get_service_instance(self) -> Optional[ServiceInfo]:
        """获取一个健康的服务实例
        
        Returns:
            Optional[ServiceInfo]: 服务实例
        """
        return self.service_discovery.get_service(self.service_name, self.version)
    
    def get_all_service_instances(self) -> List[ServiceInfo]:
        """获取所有健康的服务实例
        
        Returns:
            List[ServiceInfo]: 服务实例列表
        """
        return self.service_discovery.get_services(self.service_name, self.version)


# 创建全局服务发现实例
service_discovery = ServiceDiscovery()
