#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
日志管理模块
"""

from loguru import logger
from pathlib import Path


def setup_logger(config):
    """
    配置日志系统
    
    Args:
        config: 配置对象，包含日志相关配置
    """
    # 移除默认的控制台输出
    logger.remove()
    
    # 创建日志目录
    log_path = Path(config.log.file_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # 添加文件输出
    logger.add(
        sink=config.log.file_path,
        level=config.log.level,
        rotation=config.log.rotation,
        retention=config.log.retention,
        compression=config.log.compression,
        encoding="utf-8",
        enqueue=True,
        backtrace=True,
        diagnose=True,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"
    )
    
    # 添加控制台输出
    logger.add(
        sink=lambda msg: print(msg, end=""),
        level=config.log.level,
        colorize=True,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}:{function}:{line}</cyan> - <level>{message}</level>"
    )
    
    logger.info("日志系统配置完成")


# 导出logger实例供其他模块使用
__all__ = ["logger"]
