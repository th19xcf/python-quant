#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
API版本控制模块，用于管理API的版本和路由
"""

from typing import Any, Callable, Dict, List, Optional, Union


class APIVersion:
    """API版本信息"""
    
    def __init__(self, major: int, minor: int, patch: int = 0):
        """初始化API版本
        
        Args:
            major: 主版本号
            minor: 次版本号
            patch: 修订号
        """
        self.major = major
        self.minor = minor
        self.patch = patch
    
    def __str__(self) -> str:
        """返回版本字符串
        
        Returns:
            str: 版本字符串，格式：major.minor.patch
        """
        return f"{self.major}.{self.minor}.{self.patch}"
    
    def __eq__(self, other: Any) -> bool:
        """比较版本是否相等
        
        Args:
            other: 另一个版本对象
        
        Returns:
            bool: 是否相等
        """
        if not isinstance(other, APIVersion):
            return False
        return self.major == other.major and self.minor == other.minor and self.patch == other.patch
    
    def __lt__(self, other: Any) -> bool:
        """比较版本是否小于另一个版本
        
        Args:
            other: 另一个版本对象
        
        Returns:
            bool: 是否小于
        """
        if not isinstance(other, APIVersion):
            raise TypeError(f"无法比较APIVersion和{type(other)}")
        if self.major < other.major:
            return True
        elif self.major == other.major:
            if self.minor < other.minor:
                return True
            elif self.minor == other.minor:
                return self.patch < other.patch
        return False
    
    def __le__(self, other: Any) -> bool:
        """比较版本是否小于等于另一个版本
        
        Args:
            other: 另一个版本对象
        
        Returns:
            bool: 是否小于等于
        """
        return self < other or self == other
    
    def __gt__(self, other: Any) -> bool:
        """比较版本是否大于另一个版本
        
        Args:
            other: 另一个版本对象
        
        Returns:
            bool: 是否大于
        """
        return not self <= other
    
    def __ge__(self, other: Any) -> bool:
        """比较版本是否大于等于另一个版本
        
        Args:
            other: 另一个版本对象
        
        Returns:
            bool: 是否大于等于
        """
        return not self < other
    
    @classmethod
    def from_string(cls, version_str: str) -> 'APIVersion':
        """从字符串创建API版本
        
        Args:
            version_str: 版本字符串，格式：major.minor.patch
        
        Returns:
            APIVersion: API版本对象
        """
        parts = version_str.split('.')
        major = int(parts[0])
        minor = int(parts[1]) if len(parts) > 1 else 0
        patch = int(parts[2]) if len(parts) > 2 else 0
        return cls(major, minor, patch)


class APIRouter:
    """API路由器，用于管理API路由和版本"""
    
    def __init__(self):
        """初始化API路由器"""
        self._routes: Dict[str, Dict[str, Any]] = {}
        self._version_routes: Dict[str, Dict[str, Any]] = {}
    
    def register_route(self, path: str, handler: Callable, methods: List[str] = None, **kwargs):
        """注册API路由
        
        Args:
            path: 路由路径
            handler: 路由处理函数
            methods: HTTP方法列表，默认：['GET']
            **kwargs: 其他参数
        """
        if methods is None:
            methods = ['GET']
        
        self._routes[path] = {
            'handler': handler,
            'methods': methods,
            'kwargs': kwargs
        }
    
    def register_versioned_route(self, version: Union[str, APIVersion], path: str, handler: Callable, methods: List[str] = None, **kwargs):
        """注册带版本的API路由
        
        Args:
            version: API版本
            path: 路由路径
            handler: 路由处理函数
            methods: HTTP方法列表，默认：['GET']
            **kwargs: 其他参数
        """
        if methods is None:
            methods = ['GET']
        
        if isinstance(version, APIVersion):
            version_str = str(version)
        else:
            version_str = version
        
        if version_str not in self._version_routes:
            self._version_routes[version_str] = {}
        
        self._version_routes[version_str][path] = {
            'handler': handler,
            'methods': methods,
            'kwargs': kwargs
        }
    
    def get_route(self, path: str) -> Optional[Dict[str, Any]]:
        """获取路由信息
        
        Args:
            path: 路由路径
        
        Returns:
            Optional[Dict[str, Any]]: 路由信息，如果不存在则返回None
        """
        return self._routes.get(path)
    
    def get_versioned_route(self, version: Union[str, APIVersion], path: str) -> Optional[Dict[str, Any]]:
        """获取带版本的路由信息
        
        Args:
            version: API版本
            path: 路由路径
        
        Returns:
            Optional[Dict[str, Any]]: 路由信息，如果不存在则返回None
        """
        if isinstance(version, APIVersion):
            version_str = str(version)
        else:
            version_str = version
        
        if version_str not in self._version_routes:
            return None
        
        return self._version_routes[version_str].get(path)
    
    def get_all_routes(self) -> Dict[str, Dict[str, Any]]:
        """获取所有路由信息
        
        Returns:
            Dict[str, Dict[str, Any]]: 所有路由信息
        """
        return self._routes.copy()
    
    def get_all_versioned_routes(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """获取所有带版本的路由信息
        
        Returns:
            Dict[str, Dict[str, Dict[str, Any]]]: 所有带版本的路由信息
        """
        return self._version_routes.copy()


class APIRequest:
    """API请求对象"""
    
    def __init__(self, path: str, method: str = 'GET', headers: Optional[Dict[str, str]] = None, 
                 params: Optional[Dict[str, Any]] = None, body: Optional[Any] = None):
        """初始化API请求
        
        Args:
            path: 请求路径
            method: HTTP方法，默认：GET
            headers: 请求头
            params: 请求参数
            body: 请求体
        """
        self.path = path
        self.method = method
        self.headers = headers or {}
        self.params = params or {}
        self.body = body
        self._version = None
    
    @property
    def version(self) -> Optional[APIVersion]:
        """获取API版本
        
        Returns:
            Optional[APIVersion]: API版本
        """
        if self._version is None:
            # 从请求头获取版本
            version_header = self.headers.get('X-API-Version')
            if version_header:
                self._version = APIVersion.from_string(version_header)
        return self._version
    
    @version.setter
    def version(self, version: Union[str, APIVersion]):
        """设置API版本
        
        Args:
            version: API版本
        """
        if isinstance(version, str):
            self._version = APIVersion.from_string(version)
        else:
            self._version = version


class APIResponse:
    """API响应对象"""
    
    def __init__(self, status_code: int = 200, data: Optional[Any] = None, 
                 headers: Optional[Dict[str, str]] = None, error: Optional[Dict[str, Any]] = None):
        """初始化API响应
        
        Args:
            status_code: HTTP状态码，默认：200
            data: 响应数据
            headers: 响应头
            error: 错误信息
        """
        self.status_code = status_code
        self.data = data
        self.headers = headers or {}
        self.error = error
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式
        
        Returns:
            Dict[str, Any]: 响应字典
        """
        response = {
            'status_code': self.status_code,
            'headers': self.headers
        }
        if self.data is not None:
            response['data'] = self.data
        if self.error is not None:
            response['error'] = self.error
        return response
    
    @classmethod
    def success(cls, data: Any, status_code: int = 200, headers: Optional[Dict[str, str]] = None) -> 'APIResponse':
        """创建成功响应
        
        Args:
            data: 响应数据
            status_code: HTTP状态码，默认：200
            headers: 响应头
        
        Returns:
            APIResponse: 成功响应对象
        """
        return cls(status_code=status_code, data=data, headers=headers)
    
    @classmethod
    def error(cls, error_msg: str, status_code: int = 400, error_code: Optional[str] = None, 
              headers: Optional[Dict[str, str]] = None) -> 'APIResponse':
        """创建错误响应
        
        Args:
            error_msg: 错误信息
            status_code: HTTP状态码，默认：400
            error_code: 错误代码
            headers: 响应头
        
        Returns:
            APIResponse: 错误响应对象
        """
        error = {
            'message': error_msg
        }
        if error_code:
            error['code'] = error_code
        return cls(status_code=status_code, error=error, headers=headers)


# 创建全局API路由器实例
api_router = APIRouter()

# 定义当前API版本
CURRENT_API_VERSION = APIVersion(1, 0, 0)
DEFAULT_API_VERSION = CURRENT_API_VERSION
