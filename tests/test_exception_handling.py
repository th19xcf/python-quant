#!/usr/bin/env python3

"""
测试异常处理机制
"""

import sys
from pathlib import Path

from loguru import logger

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.exception_handler import (
    handle_error_gracefully,
    handle_exception_with_retry,
    setup_global_exception_handler,
)
from src.utils.exceptions import (
    DataSourceConnectionError,
    DataValidationError,
    QuantException,
    format_exception_for_user,
    get_error_severity,
    is_retryable_error,
)


def test_global_exception_handler():
    """测试全局异常处理器"""
    logger.info("测试全局异常处理器...")
    # 设置全局异常处理器
    setup_global_exception_handler()
    logger.info("全局异常处理器设置成功")


def test_custom_exceptions():
    """测试自定义异常"""
    logger.info("\n测试自定义异常...")

    # 测试基础异常
    try:
        raise QuantException("测试基础异常", "TEST_ERROR", {"key": "value"})
    except QuantException as e:
        logger.info(f"捕获到QuantException: {e}")
        logger.info(f"错误代码: {e.error_code}")
        logger.info(f"错误详情: {e.details}")
        logger.info(f"异常转换为字典: {e.to_dict()}")

    # 测试数据源连接异常
    try:
        raise DataSourceConnectionError("TDX", "连接失败", retryable=True, retry_count=3)
    except DataSourceConnectionError as e:
        logger.info(f"\n捕获到DataSourceConnectionError: {e}")
        logger.info(f"是否可重试: {e.is_retryable()}")

    # 测试数据验证异常
    try:
        raise DataValidationError(["字段不能为空", "值必须大于0"], "price", 0)
    except DataValidationError as e:
        logger.info(f"\n捕获到DataValidationError: {e}")
        logger.info(f"错误列表: {e.errors}")


def test_exception_decorators():
    """测试异常处理装饰器"""
    logger.info("\n测试异常处理装饰器...")

    # 测试重试装饰器
    @handle_exception_with_retry(max_retries=3, retry_delay=0.1)
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
    logger.info("\n测试异常工具函数...")

    # 测试is_retryable_error
    conn_error = DataSourceConnectionError("TEST", "连接失败", retryable=True)
    logger.info(f"DataSourceConnectionError是否可重试: {is_retryable_error(conn_error)}")

    val_error = ValueError("测试错误")
    logger.info(f"ValueError是否可重试: {is_retryable_error(val_error)}")

    # 测试get_error_severity
    logger.info(f"DataSourceConnectionError严重程度: {get_error_severity(conn_error)}")
    logger.info(f"ValueError严重程度: {get_error_severity(val_error)}")

    # 测试format_exception_for_user
    logger.info(f"用户友好错误信息: {format_exception_for_user(conn_error)}")


def test_exception_context():
    """测试异常上下文"""
    logger.info("\n测试异常上下文...")

    try:
        try:
            raise ValueError("内部错误")
        except ValueError as e:
            # 添加上下文信息
            raise QuantException("外部错误", cause=e).with_context(user="test", action="get_data") from e
    except QuantException as e:
        logger.info(f"捕获到带上下文的异常: {e}")
        logger.info(f"异常原因: {e.cause}")
        logger.info(f"异常上下文: {e.details}")


if __name__ == "__main__":
    logger.info("开始测试异常处理机制...")

    test_global_exception_handler()
    test_custom_exceptions()
    test_exception_decorators()
    test_exception_utils()
    test_exception_context()

    logger.info("\n测试完成！")
