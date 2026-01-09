#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
配置管理模块
"""

from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional, Dict, Any


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
    
    # Baostock配置
    default_stock_codes: list = Field(default=["sh.600000", "sz.000001", "sz.300001"], description="默认股票代码列表")
    default_index_codes: list = Field(default=["sh.000001", "sz.399001", "sz.399006"], description="默认指数代码列表")
    supported_frequencies: list = Field(default=["1", "5", "15", "30", "60"], description="支持的分钟线频率")
    default_days: int = Field(default=30, description="默认数据更新天数")
    default_minute_days: int = Field(default=3, description="默认分钟线数据下载天数")
    default_realtime_days: int = Field(default=5, description="默认实时数据替代天数")
    
    # 股票基本信息更新配置
    auto_update_stock_basic: bool = Field(default=False, description="是否在启动时自动更新股票基本信息")
    stock_basic_update_interval: int = Field(default=7, description="股票基本信息更新间隔（天）")


class LogSettings(BaseSettings):
    """日志配置"""
    level: str = Field(default="INFO", description="日志级别")
    file_path: str = Field(default="logs/app.log", description="日志文件路径")
    rotation: str = Field(default="10 MB", description="日志文件轮换大小")
    retention: str = Field(default="30 days", description="日志文件保留时间")
    compression: str = Field(default="zip", description="日志文件压缩格式")


class PluginSettings(BaseSettings):
    """单个插件配置"""
    enabled: bool = Field(default=True, description="插件是否启用")
    version: Optional[str] = Field(default=None, description="插件版本")
    config: Dict[str, Any] = Field(default_factory=dict, description="插件特定配置")
    
    class Config:
        """Pydantic配置"""
        env_file = ".env"
        env_file_encoding = "utf-8"
        env_nested_delimiter = "__"
        case_sensitive = False


class PluginsSettings(BaseSettings):
    """所有插件配置"""
    # 使用字典存储各个插件的配置
    plugins: Dict[str, PluginSettings] = Field(default_factory=dict, description="插件配置字典")
    
    class Config:
        """Pydantic配置"""
        env_file = ".env"
        env_file_encoding = "utf-8"
        env_nested_delimiter = "__"
        case_sensitive = False
        extra = 'ignore'  # 允许忽略额外的输入


class Config(BaseSettings):
    """主配置类"""
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    data: DataSettings = Field(default_factory=DataSettings)
    log: LogSettings = Field(default_factory=LogSettings)
    plugins: PluginsSettings = Field(default_factory=PluginsSettings)
    
    class Config:
        """Pydantic配置"""
        env_file = ".env"
        env_file_encoding = "utf-8"
        env_nested_delimiter = "__"
        case_sensitive = False


# 调用 model_rebuild() 来确保所有类型都已完全定义
PluginsSettings.model_rebuild()
Config.model_rebuild()


# 全局配置实例
config = Config()
