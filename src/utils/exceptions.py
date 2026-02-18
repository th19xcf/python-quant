#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
业务异常定义模块

定义量化交易系统中使用的所有业务异常，实现精细化异常处理
"""

from typing import List, Optional, Dict, Any


class QuantException(Exception):
    """基础业务异常
    
    所有业务异常的基类，提供统一的异常处理接口
    """
    
    def __init__(self, message: str, error_code: str = None, details: Dict[str, Any] = None):
        self.message = message
        self.error_code = error_code or "UNKNOWN_ERROR"
        self.details = details or {}
        super().__init__(self.message)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，便于序列化"""
        return {
            "error_code": self.error_code,
            "message": self.message,
            "details": self.details,
            "exception_type": self.__class__.__name__
        }


# =============================================================================
# 数据源异常
# =============================================================================

class DataSourceException(QuantException):
    """数据源异常基类"""
    
    def __init__(self, source: str, message: str, error_code: str = None, details: Dict[str, Any] = None):
        self.source = source
        super().__init__(
            message=f"[{source}] {message}",
            error_code=error_code or "DATASOURCE_ERROR",
            details=details
        )


class DataSourceConnectionError(DataSourceException):
    """数据源连接错误
    
    当无法连接到数据源时抛出
    """
    
    def __init__(self, source: str, message: str, retryable: bool = True, retry_count: int = 0):
        self.retryable = retryable
        self.retry_count = retry_count
        super().__init__(
            source=source,
            message=message,
            error_code="DATASOURCE_CONNECTION_ERROR",
            details={"retryable": retryable, "retry_count": retry_count}
        )


class DataSourceTimeoutError(DataSourceException):
    """数据源请求超时"""
    
    def __init__(self, source: str, timeout: float = None):
        self.timeout = timeout
        super().__init__(
            source=source,
            message=f"请求超时{' (%.1fs)' % timeout if timeout else ''}",
            error_code="DATASOURCE_TIMEOUT_ERROR",
            details={"timeout": timeout}
        )


class DataSourceConfigError(DataSourceException):
    """数据源配置错误"""
    
    def __init__(self, source: str, config_key: str = None, message: str = None):
        self.config_key = config_key
        super().__init__(
            source=source,
            message=message or f"配置错误: {config_key}",
            error_code="DATASOURCE_CONFIG_ERROR",
            details={"config_key": config_key}
        )


class DataSourceNotAvailableError(DataSourceException):
    """数据源不可用（离线模式）"""
    
    def __init__(self, source: str, reason: str = None):
        super().__init__(
            source=source,
            message=reason or "数据源当前不可用",
            error_code="DATASOURCE_NOT_AVAILABLE",
            details={"reason": reason}
        )


# =============================================================================
# 数据异常
# =============================================================================

class DataException(QuantException):
    """数据异常基类"""
    
    def __init__(self, message: str, error_code: str = None, details: Dict[str, Any] = None):
        super().__init__(
            message=message,
            error_code=error_code or "DATA_ERROR",
            details=details
        )


class DataValidationError(DataException):
    """数据验证错误
    
    当数据不符合预期格式或约束时抛出
    """
    
    def __init__(self, errors: List[str], field: str = None, value: Any = None):
        self.errors = errors if isinstance(errors, list) else [errors]
        self.field = field
        self.value = value
        super().__init__(
            message=f"数据验证失败: {self.errors}",
            error_code="DATA_VALIDATION_ERROR",
            details={
                "errors": self.errors,
                "field": field,
                "value": str(value) if value is not None else None
            }
        )


class DataNotFoundError(DataException):
    """数据不存在"""
    
    def __init__(self, data_type: str, identifier: str = None):
        self.data_type = data_type
        self.identifier = identifier
        super().__init__(
            message=f"{data_type}不存在" + (f": {identifier}" if identifier else ""),
            error_code="DATA_NOT_FOUND",
            details={"data_type": data_type, "identifier": identifier}
        )


class DataIntegrityError(DataException):
    """数据完整性错误"""
    
    def __init__(self, message: str, constraint: str = None):
        self.constraint = constraint
        super().__init__(
            message=message,
            error_code="DATA_INTEGRITY_ERROR",
            details={"constraint": constraint}
        )


class DataSaveError(DataException):
    """数据保存错误"""
    
    def __init__(self, message: str, operation: str = None):
        self.operation = operation
        super().__init__(
            message=message,
            error_code="DATA_SAVE_ERROR",
            details={"operation": operation}
        )


# =============================================================================
# 计算异常
# =============================================================================

class CalculationException(QuantException):
    """计算异常基类"""
    
    def __init__(self, message: str, error_code: str = None, details: Dict[str, Any] = None):
        super().__init__(
            message=message,
            error_code=error_code or "CALCULATION_ERROR",
            details=details
        )


class IndicatorCalculationError(CalculationException):
    """指标计算错误"""
    
    def __init__(self, indicator: str, message: str, params: Dict[str, Any] = None):
        self.indicator = indicator
        self.params = params or {}
        super().__init__(
            message=f"指标[{indicator}]计算失败: {message}",
            error_code="INDICATOR_CALCULATION_ERROR",
            details={"indicator": indicator, "params": params}
        )


class IndicatorNotFoundError(CalculationException):
    """指标不存在"""
    
    def __init__(self, indicator: str, available_indicators: List[str] = None):
        self.indicator = indicator
        self.available_indicators = available_indicators or []
        super().__init__(
            message=f"指标[{indicator}]不存在",
            error_code="INDICATOR_NOT_FOUND",
            details={
                "indicator": indicator,
                "available_count": len(available_indicators),
                "suggestions": available_indicators[:5] if available_indicators else []
            }
        )


class InsufficientDataError(CalculationException):
    """数据不足无法计算"""
    
    def __init__(self, indicator: str, required: int, actual: int):
        self.required = required
        self.actual = actual
        super().__init__(
            message=f"指标[{indicator}]计算需要{required}条数据，实际只有{actual}条",
            error_code="INSUFFICIENT_DATA_ERROR",
            details={"indicator": indicator, "required": required, "actual": actual}
        )


# =============================================================================
# UI异常
# =============================================================================

class UIException(QuantException):
    """UI异常基类"""
    
    def __init__(self, message: str, error_code: str = None, details: Dict[str, Any] = None):
        super().__init__(
            message=message,
            error_code=error_code or "UI_ERROR",
            details=details
        )


class ComponentNotFoundError(UIException):
    """UI组件不存在"""
    
    def __init__(self, component_name: str, container: str = None):
        super().__init__(
            message=f"组件[{component_name}]不存在" + (f" in {container}" if container else ""),
            error_code="COMPONENT_NOT_FOUND",
            details={"component_name": component_name, "container": container}
        )


class RenderError(UIException):
    """渲染错误"""
    
    def __init__(self, component: str, message: str):
        super().__init__(
            message=f"组件[{component}]渲染失败: {message}",
            error_code="RENDER_ERROR",
            details={"component": component}
        )


# =============================================================================
# 配置异常
# =============================================================================

class ConfigException(QuantException):
    """配置异常基类"""
    
    def __init__(self, message: str, error_code: str = None, details: Dict[str, Any] = None):
        super().__init__(
            message=message,
            error_code=error_code or "CONFIG_ERROR",
            details=details
        )


class ConfigNotFoundError(ConfigException):
    """配置文件不存在"""
    
    def __init__(self, config_path: str):
        self.config_path = config_path
        super().__init__(
            message=f"配置文件不存在: {config_path}",
            error_code="CONFIG_NOT_FOUND",
            details={"config_path": config_path}
        )


class ConfigValidationError(ConfigException):
    """配置验证错误"""
    
    def __init__(self, field: str, value: Any, expected_type: type = None):
        self.field = field
        self.value = value
        self.expected_type = expected_type
        super().__init__(
            message=f"配置项[{field}]验证失败，当前值: {value}" + (f"，期望类型: {expected_type}" if expected_type else ""),
            error_code="CONFIG_VALIDATION_ERROR",
            details={"field": field, "value": str(value), "expected_type": str(expected_type) if expected_type else None}
        )


# =============================================================================
# 插件异常
# =============================================================================

class PluginException(QuantException):
    """插件异常基类"""
    
    def __init__(self, plugin_name: str, message: str, error_code: str = None):
        self.plugin_name = plugin_name
        super().__init__(
            message=f"插件[{plugin_name}]: {message}",
            error_code=error_code or "PLUGIN_ERROR",
            details={"plugin_name": plugin_name}
        )


class PluginNotFoundError(PluginException):
    """插件不存在"""
    
    def __init__(self, plugin_name: str):
        super().__init__(
            plugin_name=plugin_name,
            message="插件不存在",
            error_code="PLUGIN_NOT_FOUND"
        )


class PluginLoadError(PluginException):
    """插件加载错误"""
    
    def __init__(self, plugin_name: str, reason: str):
        super().__init__(
            plugin_name=plugin_name,
            message=f"加载失败: {reason}",
            error_code="PLUGIN_LOAD_ERROR"
        )


class PluginInitError(PluginException):
    """插件初始化错误"""
    
    def __init__(self, plugin_name: str, reason: str):
        super().__init__(
            plugin_name=plugin_name,
            message=f"初始化失败: {reason}",
            error_code="PLUGIN_INIT_ERROR"
        )


# =============================================================================
# 工具函数
# =============================================================================

def is_retryable_error(exception: Exception) -> bool:
    """判断异常是否可以重试
    
    Args:
        exception: 异常对象
        
    Returns:
        bool: 是否可以重试
    """
    if isinstance(exception, DataSourceConnectionError):
        return exception.retryable
    
    if isinstance(exception, (ConnectionError, TimeoutError)):
        return True
    
    return False


def get_error_severity(exception: Exception) -> str:
    """获取错误严重程度
    
    Args:
        exception: 异常对象
        
    Returns:
        str: 严重程度 (CRITICAL, ERROR, WARNING, INFO)
    """
    if isinstance(exception, (DataSourceConnectionError, DataSaveError)):
        return "CRITICAL"
    
    if isinstance(exception, (DataValidationError, ConfigValidationError)):
        return "WARNING"
    
    if isinstance(exception, (DataSourceNotAvailableError, DataNotFoundError)):
        return "INFO"
    
    return "ERROR"


def format_exception_for_user(exception: Exception) -> str:
    """格式化异常信息给用户
    
    Args:
        exception: 异常对象
        
    Returns:
        str: 用户友好的错误信息
    """
    if isinstance(exception, QuantException):
        return exception.message
    
    # 对于非业务异常，返回通用错误信息
    error_type = type(exception).__name__
    return f"操作失败 ({error_type}): {str(exception)}"
