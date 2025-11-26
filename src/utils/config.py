#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
配置管理模块
"""

from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional


class DatabaseSettings(BaseSettings):
    """数据库配置"""
    host: str = Field(default="localhost", description="数据库主机")
    port: int = Field(default=3306, description="数据库端口")
    username: str = Field(default="root", description="数据库用户名")
    password: str = Field(default="", description="数据库密码")
    database: str = Field(default="stock_quant", description="数据库名称")
    charset: str = Field(default="utf8mb4", description="数据库字符集")
    pool_size: int = Field(default=10, description="连接池大小")
    pool_recycle: int = Field(default=3600, description="连接池回收时间")


class RedisSettings(BaseSettings):
    """Redis配置"""
    host: str = Field(default="localhost", description="Redis主机")
    port: int = Field(default=6379, description="Redis端口")
    password: Optional[str] = Field(default=None, description="Redis密码")
    db: int = Field(default=0, description="Redis数据库")
    decode_responses: bool = Field(default=True, description="是否解码响应")


class DataSettings(BaseSettings):
    """数据配置"""
    tdx_data_path: str = Field(default="", description="通达信数据路径")
    update_interval: int = Field(default=3600, description="数据更新间隔(秒)")
    max_workers: int = Field(default=4, description="数据获取最大工作线程数")


class LogSettings(BaseSettings):
    """日志配置"""
    level: str = Field(default="INFO", description="日志级别")
    file_path: str = Field(default="logs/app.log", description="日志文件路径")
    rotation: str = Field(default="10 MB", description="日志文件轮换大小")
    retention: str = Field(default="30 days", description="日志文件保留时间")
    compression: str = Field(default="zip", description="日志文件压缩格式")


class Config(BaseSettings):
    """主配置类"""
    database: DatabaseSettings = DatabaseSettings()
    redis: RedisSettings = RedisSettings()
    data: DataSettings = DataSettings()
    log: LogSettings = LogSettings()
    
    class Config:
        """Pydantic配置"""
        env_file = ".env"
        env_file_encoding = "utf-8"
        env_nested_delimiter = "__"
        case_sensitive = False


# 全局配置实例
config = Config()
