#!/usr/bin/env python3

"""
测试优化后的异常处理功能
"""

import sys
from pathlib import Path

from loguru import logger

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.exception_handler import (
    global_exception_handler,
    handle_error_gracefully,
    handle_exception_with_retry,
    setup_global_exception_handler,
)
from src.utils.exceptions import (
    CalculationException,
    ConfigException,
    ConfigValidationError,
    DataNotFoundError,
    DataSourceConnectionError,
    DataSourceException,
    DataSourceTimeoutError,
    DataValidationError,
    ErrorCodeManager,
    IndicatorCalculationError,
    PluginException,
    PluginNotFoundError,
    QuantException,
    format_exception_for_user,
    get_error_code_category,
    get_error_severity,
    is_retryable_error,
)


def test_error_code_manager():
    """测试错误码管理器"""
    logger.info("=== 测试错误码管理器 ===")

    # 测试获取错误信息
    error_code = "DATASOURCE_CONNECTION_ERROR"
    error_message = ErrorCodeManager.get_error_message(error_code)
    logger.info(f"错误码 {error_code} 的错误信息: {error_message}")

    # 测试错误码有效性检查
    valid_code = "DATA_VALIDATION_ERROR"
    invalid_code = "INVALID_ERROR"
    logger.info(f"错误码 {valid_code} 是否有效: {ErrorCodeManager.is_valid_error_code(valid_code)}")
    logger.info(f"错误码 {invalid_code} 是否有效: {ErrorCodeManager.is_valid_error_code(invalid_code)}")

    # 测试错误码分类
    logger.info(f"错误码 {error_code} 的分类: {get_error_code_category(error_code)}")
    logger.info(f"错误码 {valid_code} 的分类: {get_error_code_category(valid_code)}")


def test_exception_inheritance():
    """测试异常继承关系"""
    logger.info("\n=== 测试异常继承关系 ===")

    # 测试所有异常都继承自QuantException
    exceptions = [
        DataSourceException("TEST", "测试数据源异常"),
        DataSourceConnectionError("TEST", "测试连接错误"),
        DataSourceTimeoutError("TEST"),
        DataValidationError(["测试验证错误"], "field"),
        DataNotFoundError("test_data"),
        CalculationException("测试计算异常"),
        IndicatorCalculationError("MA", "测试指标计算错误"),
        ConfigException("测试配置异常"),
        ConfigValidationError("field", "value"),
        PluginException("test_plugin", "测试插件异常"),
        PluginNotFoundError("test_plugin")
    ]

    for exception in exceptions:
        is_quant_exception = isinstance(exception, QuantException)
        logger.info(f"{exception.__class__.__name__} 继承自 QuantException: {is_quant_exception}")
        assert is_quant_exception, f"{exception.__class__.__name__} 应该继承自 QuantException"


def test_exception_methods():
    """测试异常方法"""
    logger.info("\n=== 测试异常方法 ===")

    # 测试基础异常方法
    base_exception = QuantException("测试基础异常", "TEST_ERROR", {"key": "value"})
    logger.info(f"异常字符串表示: {base_exception}")
    logger.info(f"异常字典表示: {base_exception.to_dict()}")
    logger.info(f"异常严重程度: {base_exception.get_severity()}")

    # 测试添加上下文
    exception_with_context = base_exception.with_context(user="test", action="get_data")
    logger.info(f"带上下文的异常: {exception_with_context.details}")


def test_exception_decorators():
    """测试异常处理装饰器"""
    logger.info("\n=== 测试异常处理装饰器 ===")

    # 测试重试装饰器
    @handle_exception_with_retry(max_retries=2, retry_delay=0.1)
    def test_retry_function():
        logger.info("执行测试函数")
        # 模拟网络错误
        raise DataSourceConnectionError("TEST", "网络连接失败")

    try:
        test_retry_function()
    except DataSourceConnectionError as e:
        logger.info(f"最终捕获到异常: {e}")

    # 测试优雅处理装饰器
    @handle_error_gracefully()
    def test_graceful_function():
        raise ValueError("测试错误")

    result = test_graceful_function()
    logger.info(f"优雅处理结果: {result}")


def test_exception_utils():
    """测试异常工具函数"""
    logger.info("\n=== 测试异常工具函数 ===")

    # 测试is_retryable_error
    conn_error = DataSourceConnectionError("TEST", "连接失败", retryable=True)
    val_error = ValueError("测试错误")
    logger.info(f"DataSourceConnectionError是否可重试: {is_retryable_error(conn_error)}")
    logger.info(f"ValueError是否可重试: {is_retryable_error(val_error)}")

    # 测试get_error_severity
    logger.info(f"DataSourceConnectionError严重程度: {get_error_severity(conn_error)}")
    logger.info(f"ValueError严重程度: {get_error_severity(val_error)}")

    # 测试format_exception_for_user
    logger.info(f"用户友好错误信息: {format_exception_for_user(conn_error)}")


def test_global_exception_handler():
    """测试全局异常处理器"""
    logger.info("\n=== 测试全局异常处理器 ===")

    # 获取全局异常处理器
    handler = setup_global_exception_handler()
    logger.info("全局异常处理器设置成功")

    # 测试错误统计
    logger.info("{} {}", "初始错误统计:", handler.get_error_stats())

    # 模拟异常
    try:
        raise DataSourceConnectionError("TEST", "测试连接错误")
    except DataSourceConnectionError as e:
        logger.info(f"捕获到异常: {e}")

    # 查看错误统计
    stats = handler.get_error_stats()
    logger.info(f"错误统计: 总错误数={stats['total_errors']}")
    logger.info(f"错误类型: {stats['error_types']}")
    logger.info(f"错误分类: {stats['error_categories']}")
    logger.info(f"错误码: {stats['error_codes']}")
    logger.info(f"严重程度: {stats['severity_counts']}")


def test_alert_mechanism():
    """测试告警机制"""
    logger.info("\n=== 测试告警机制 ===")

    # 获取全局异常处理器
    handler = global_exception_handler

    # 清除历史记录
    handler.clear_alert_history()

    # 模拟严重错误
    try:
        raise DataSourceConnectionError("TEST", "测试严重错误")
    except DataSourceConnectionError as e:
        logger.info(f"捕获到严重异常: {e}")

    # 查看告警历史
    alert_history = handler.get_alert_history()
    logger.info(f"告警历史记录数: {len(alert_history)}")
    if alert_history:
        logger.info(f"第一个告警: {alert_history[0]['message']}")


def test_error_frequency_alert():
    """测试错误频率告警"""
    logger.info("\n=== 测试错误频率告警 ===")

    # 获取全局异常处理器
    handler = global_exception_handler

    # 清除历史记录
    handler.clear_alert_history()

    # 模拟多个错误
    for i in range(6):  # 超过严重错误阈值
        try:
            raise DataSourceConnectionError("TEST", f"测试错误 {i+1}")
        except DataSourceConnectionError as e:
            logger.info(f"捕获到异常 {i+1}: {e}")

    # 查看告警历史
    alert_history = handler.get_alert_history()
    logger.info(f"告警历史记录数: {len(alert_history)}")
    for alert in alert_history:
        logger.info(f"告警: {alert['level']} - {alert['message']}")


if __name__ == "__main__":
    logger.info("开始测试优化后的异常处理功能...")
    logger.info("=" * 60)

    # 运行各个测试
    test_error_code_manager()
    test_exception_inheritance()
    test_exception_methods()
    test_exception_decorators()
    test_exception_utils()
    test_global_exception_handler()
    test_alert_mechanism()
    test_error_frequency_alert()

    logger.info("\n" + "=" * 60)
    logger.info("异常处理功能测试完成！")
