#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
日志管理模块
"""

from pathlib import Path
from loguru import logger


def _patcher(record):
    """预计算结构化字段字符串，避免空 extra 时显示 {}"""
    extra = record["extra"]
    # 排除内部字段
    items = {k: v for k, v in extra.items() if not k.startswith("_")}
    if items:
        record["extra"]["_extra_str"] = " | " + " ".join(f"{k}={v}" for k, v in items.items())
    else:
        record["extra"]["_extra_str"] = ""
    return True


_FILE_FORMAT = (
    "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}{extra[_extra_str]}"
)
_CONSOLE_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}:{function}:{line}</cyan> - "
    "<level>{message}</level>{extra[_extra_str]}"
)


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
        level=config.log.level,  # 使用配置中的日志级别
        rotation="50 MB",  # 增加日志文件大小限制，减少旋转频率
        retention=config.log.retention,
        compression=config.log.compression,
        encoding="utf-8",
        enqueue=True,  # 保持异步写入
        backtrace=True,
        diagnose=True,
        format=_FILE_FORMAT,
        filter=_patcher,
    )
    
    # 添加控制台输出
    logger.add(
        sink=lambda msg: print(msg, end=""),
        level=config.log.level,  # 使用配置中的日志级别
        colorize=True,
        format=_CONSOLE_FORMAT,
        filter=_patcher,
    )
    
    logger.info("日志系统配置完成")


# 导出logger实例供其他模块使用
__all__ = ["logger"]
