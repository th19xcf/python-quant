#!/usr/bin/env python3

r"""
配置管理模块

该模块负责管理系统的所有配置项，包括：
- 数据库配置
- 数据配置
- 日志配置
- 插件配置

特性：
1. 基于Pydantic V2的类型验证
2. 支持环境变量配置
3. 支持配置热加载
4. 详细的配置文档
"""

import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Set

from loguru import logger
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _validate_port_value(value: int) -> int:
    """端口号合法范围校验（DatabaseSettings 使用）"""
    if value < 1 or value > 65535:
        raise ValueError('端口号必须在1-65535之间')
    return value


class DatabaseSettings(BaseSettings):
    """数据库配置"""
    host: str = Field(default="localhost", description="数据库主机")
    port: int = Field(default=3306, description="数据库端口")
    username: str = Field(default="root", description="数据库用户名")
    password: str = Field(default="", description="数据库密码")
    database: str = Field(default="stock_quant", description="数据库名称")
    charset: str = Field(default="utf8mb4", description="数据库字符集")
    pool_size: int = Field(default=10, description="连接池大小")
    max_overflow: int = Field(default=10, description="连接池溢出上限（池满后最多再开临时连接数）")
    pool_timeout: int = Field(default=30, description="池满后获取连接的最大等待秒数，超时抛 QueuePool 上限错误")
    pool_recycle: int = Field(default=3600, description="连接池回收时间")

    @field_validator('port')
    @classmethod
    def validate_port(cls, v: int) -> int:
        """验证端口号"""
        return _validate_port_value(v)

    @field_validator('pool_size')
    @classmethod
    def validate_pool_size(cls, v: int) -> int:
        """验证连接池大小"""
        if v < 1:
            raise ValueError('连接池大小必须大于0')
        return v

    @field_validator('max_overflow')
    @classmethod
    def validate_max_overflow(cls, v: int) -> int:
        """验证连接池溢出上限。

        显式设置为 0 时池满即等待；负数在 SQLAlchemy 中表示不限量，因此只允许 >= 0。
        """
        if v < 0:
            raise ValueError('连接池溢出上限不能为负数')
        return v

    @field_validator('pool_timeout')
    @classmethod
    def validate_pool_timeout(cls, v: int) -> int:
        """验证获取连接等待时长：必须为正，避免池满后无限阻塞"""
        if v < 1:
            raise ValueError('获取连接等待时长必须大于0秒')
        return v


class DataSettings(BaseSettings):
    """数据配置"""
    tdx_data_path: str = Field(default="", description="通达信数据路径")
    update_interval: int = Field(default=3600, description="数据更新间隔(秒)")
    max_workers: int = Field(default=4, description="数据获取最大工作线程数")

    # 远程数据源开关（默认关闭）：关闭时运行时取数只读本地通达信文件，
    # 不发起任何网络请求；命令行批量同步需要显式开启后才允许调用 Baostock/AkShare。
    allow_remote_sources: bool = Field(
        default=False,
        description="是否允许运行时/批量同步使用 Baostock/AkShare 等远程数据源",
    )

    # 远程数据源子进程隔离（灰度开关，默认关闭）
    # 开启后 AkShare/BaoStock 等纯网络源在独立进程内执行，超时可直接终止进程，
    # 避免第三方库挂死拖住整个主进程。
    use_subprocess_sources: bool = Field(default=False, description="远程数据源是否启用子进程隔离")

    # 本地列存（Parquet）：行情/因子/分红的主读取源。
    # 开启后取数顺序为「本地列存 → 通达信 .day → 远程库（显式允许时）→ 远程源」，
    # 画图与回测不再依赖公网 MySQL 的往返延迟。
    local_store_enabled: bool = Field(
        default=True,
        description="是否启用本地 Parquet 列存作为行情/因子/分红主读取源",
    )
    local_store_path: str = Field(
        default="data/local_store",
        description="本地列存根目录（相对项目根目录或绝对路径）",
    )
    # 远程数据库兜底（默认关闭）：关闭时读侧不发起任何 MySQL 查询；
    # 仅当本地列存与通达信都没有数据时才允许显式打开。
    remote_database_fallback: bool = Field(
        default=False,
        description="本地与通达信均无数据时，是否允许回退到远程 MySQL 读行情",
    )

    # 数据源重试配置
    retry_max_retries: int = Field(default=3, description="数据源请求最大重试次数")
    retry_delay: float = Field(default=1.0, description="首次重试间隔(秒)")
    retry_backoff_factor: float = Field(default=2.0, description="重试退避倍数，1.0 表示固定间隔")
    retry_max_delay: float = Field(default=30.0, description="单次重试最大间隔(秒)")

    # Baostock配置
    default_stock_codes: List[str] = Field(default=["sh.600000", "sz.000001", "sz.300001"], description="默认股票代码列表")
    default_index_codes: List[str] = Field(default=["000001.SH", "399001.SZ", "399006.SZ"], description="默认指数代码列表（标准 ts_code 口径）")
    supported_frequencies: List[str] = Field(default=["1", "5", "15", "30", "60"], description="支持的分钟线频率")
    default_days: int = Field(default=30, description="默认数据更新天数")
    default_minute_days: int = Field(default=3, description="默认分钟线数据下载天数")
    default_realtime_days: int = Field(default=5, description="默认实时数据替代天数")

    # 股票基本信息更新配置
    auto_update_stock_basic: bool = Field(default=False, description="是否在启动时自动更新股票基本信息")
    stock_basic_update_interval: int = Field(default=7, description="股票基本信息更新间隔（天）")

    @field_validator('update_interval')
    @classmethod
    def validate_update_interval(cls, v: int) -> int:
        """验证更新间隔"""
        if v < 60:
            raise ValueError('更新间隔必须大于等于60秒')
        return v

    @field_validator('max_workers')
    @classmethod
    def validate_max_workers(cls, v: int) -> int:
        """验证最大工作线程数"""
        if v < 1:
            raise ValueError('最大工作线程数必须大于0')
        return v

    @field_validator('default_days', 'default_minute_days', 'default_realtime_days')
    @classmethod
    def validate_days(cls, v: int) -> int:
        """验证天数"""
        if v < 1:
            raise ValueError('天数必须大于0')
        return v

    @field_validator('stock_basic_update_interval')
    @classmethod
    def validate_stock_basic_update_interval(cls, v: int) -> int:
        """验证股票基本信息更新间隔"""
        if v < 1:
            raise ValueError('股票基本信息更新间隔必须大于0')
        return v


class LogSettings(BaseSettings):
    """日志配置"""
    level: str = Field(default="INFO", description="日志级别")
    file_path: str = Field(default="logs/app.log", description="日志文件路径")
    rotation: str = Field(default="10 MB", description="日志文件轮换大小")
    retention: str = Field(default="30 days", description="日志文件保留时间")
    compression: str = Field(default="zip", description="日志文件压缩格式")

    @field_validator('level')
    @classmethod
    def validate_level(cls, v: str) -> str:
        """验证日志级别"""
        valid_levels: Set[str] = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
        if v.upper() not in valid_levels:
            raise ValueError(f'日志级别必须是以下之一: {valid_levels}')
        return v.upper()

    @field_validator('file_path')
    @classmethod
    def validate_file_path(cls, v: str) -> str:
        """验证日志文件路径"""
        # 确保日志目录存在
        log_dir = Path(v).parent
        if log_dir and not log_dir.exists():
            log_dir.mkdir(parents=True, exist_ok=True)
        return v


class PluginSettings(BaseSettings):
    """单个插件配置"""
    enabled: bool = Field(default=True, description="插件是否启用")
    version: str | None = Field(default=None, description="插件版本")
    config: Dict[str, Any] = Field(default_factory=dict, description="插件特定配置")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        case_sensitive=False
    )


class PluginsSettings(BaseSettings):
    """所有插件配置"""
    # 使用字典存储各个插件的配置
    plugins: Dict[str, PluginSettings] = Field(default_factory=dict, description="插件配置字典")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra='ignore'  # 允许忽略额外的输入
    )


class Config(BaseSettings):
    """主配置类"""
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    data: DataSettings = Field(default_factory=DataSettings)
    log: LogSettings = Field(default_factory=LogSettings)
    plugins: PluginsSettings = Field(default_factory=PluginsSettings)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        case_sensitive=False,
        # 容忍 .env 中的历史/未知变量（如已下线的 REDIS__*），避免旧环境升级后启动失败
        extra='ignore',
    )


class ConfigManager:
    """配置管理器，支持配置热加载"""

    def __init__(self) -> None:
        """初始化配置管理器"""
        self._config: Config = Config()
        self._last_modified: float = self._get_env_file_mtime()
        self._lock = threading.RLock()
        self._watch_thread: threading.Thread | None = None
        self._running = False
        # 停止信号：用 Event 等待代替 sleep，退出时立即唤醒热加载线程，
        # 避免关窗被 join(timeout) 里的轮询间隔拖住
        self._stop_event = threading.Event()

    def _get_env_file_mtime(self) -> float:
        """获取.env文件的修改时间"""
        env_file = Path(".env")
        if env_file.exists():
            return env_file.stat().st_mtime
        return 0

    def get_config(self) -> Config:
        """获取配置实例，支持热加载"""
        with self._lock:
            # 检查配置文件是否被修改
            current_mtime = self._get_env_file_mtime()
            if current_mtime > self._last_modified:
                # 配置文件已修改，重新加载
                self._config = Config()
                self._last_modified = current_mtime
            return self._config

    def start_watching(self, interval: int = 5) -> None:
        """开始监控配置文件变化

        Args:
            interval: 监控间隔（秒）
        """
        self._running = True
        self._stop_event.clear()
        self._watch_thread = threading.Thread(target=self._watch_config, args=(interval,), daemon=True)
        self._watch_thread.start()

    def stop_watching(self) -> None:
        """停止监控配置文件变化"""
        self._running = False
        # 立即唤醒轮询线程，join 只作兜底
        self._stop_event.set()
        if self._watch_thread:
            self._watch_thread.join(timeout=1)

    def _watch_config(self, interval: int) -> None:
        """监控配置文件变化的线程函数

        Args:
            interval: 监控间隔（秒）
        """
        while self._running:
            # 可被 stop_watching 立即唤醒，退出时不必等满一个轮询间隔
            if self._stop_event.wait(interval):
                break
            current_mtime = self._get_env_file_mtime()
            if current_mtime > self._last_modified:
                with self._lock:
                    try:
                        # 重新加载配置
                        new_config = Config()
                        self._config = new_config
                        self._last_modified = current_mtime
                        logger.info("配置文件已更新，热加载成功")
                    # 兜底理由：配置热加载失败保留旧配置
                    except Exception as e:  # noqa: BLE001
                        logger.warning(f"配置文件更新失败: {e}")

    def reload(self) -> Config:
        """手动重新加载配置

        Returns:
            重新加载后的配置实例
        """
        with self._lock:
            self._config = Config()
            self._last_modified = self._get_env_file_mtime()
            return self._config


# 调用 model_rebuild() 来确保所有类型都已完全定义
PluginsSettings.model_rebuild()
Config.model_rebuild()


# 创建配置管理器实例
config_manager = ConfigManager()

# 全局配置实例，通过配置管理器获取
def get_config() -> Config:
    """获取全局配置实例"""
    return config_manager.get_config()


def remote_sources_enabled(config_obj: Any) -> bool:
    """判断给定配置是否允许使用远程数据源。

    统一收口运行时取数与命令行同步的开关判定：
    - 真实 Config 默认 `data.allow_remote_sources=False`（离线优先）；
    - 未声明该字段的轻量配置对象（测试替身/历史调用）缺省视为允许，
      避免为兼容测试替身而改写全部调用点。
    """
    data_cfg = getattr(config_obj, 'data', None)
    return bool(getattr(data_cfg, 'allow_remote_sources', True))

# 导出配置实例
config = get_config()


# 配置文档
CONFIG_DOCUMENTATION = r"""
# 配置文档

## 配置文件格式

系统使用 `.env` 文件进行配置，支持环境变量和配置文件两种方式。

## 配置项说明

### 1. 数据库配置

| 配置项 | 默认值 | 说明 | 验证规则 |
|--------|--------|------|----------|
| DATABASE__HOST | localhost | 数据库主机 | 字符串 |
| DATABASE__PORT | 3306 | 数据库端口 | 1-65535 |
| DATABASE__USERNAME | root | 数据库用户名 | 字符串 |
| DATABASE__PASSWORD | "" | 数据库密码 | 字符串 |
| DATABASE__DATABASE | stock_quant | 数据库名称 | 字符串 |
| DATABASE__CHARSET | utf8mb4 | 数据库字符集 | 字符串 |
| DATABASE__POOL_SIZE | 10 | 连接池大小 | 大于0 |
| DATABASE__MAX_OVERFLOW | 10 | 连接池溢出上限（池满后可再开的临时连接数） | 大于等于0 |
| DATABASE__POOL_TIMEOUT | 30 | 池满后获取连接的最大等待秒数 | 大于0 |
| DATABASE__POOL_RECYCLE | 3600 | 连接池回收时间 | 整数 |

### 2. 数据配置

| 配置项 | 默认值 | 说明 | 验证规则 |
|--------|--------|------|----------|
| DATA__TDX_DATA_PATH | "" | 通达信数据路径 | 字符串 |
| DATA__UPDATE_INTERVAL | 3600 | 数据更新间隔(秒) | 大于等于60 |
| DATA__MAX_WORKERS | 4 | 数据获取最大工作线程数 | 大于0 |
| DATA__ALLOW_REMOTE_SOURCES | False | 是否允许运行时/批量同步调用远程数据源（Baostock/AkShare） | 布尔值 |
| DATA__DEFAULT_STOCK_CODES | ["sh.600000", "sz.000001", "sz.300001"] | 默认股票代码列表 | 字符串列表 |
| DATA__DEFAULT_INDEX_CODES | ["000001.SH", "399001.SZ", "399006.SZ"] | 默认指数代码列表（标准 ts_code 口径） | 字符串列表 |
| DATA__SUPPORTED_FREQUENCIES | ["1", "5", "15", "30", "60"] | 支持的分钟线频率 | 字符串列表 |
| DATA__DEFAULT_DAYS | 30 | 默认数据更新天数 | 大于0 |
| DATA__DEFAULT_MINUTE_DAYS | 3 | 默认分钟线数据下载天数 | 大于0 |
| DATA__DEFAULT_REALTIME_DAYS | 5 | 默认实时数据替代天数 | 大于0 |
| DATA__AUTO_UPDATE_STOCK_BASIC | False | 是否在启动时自动更新股票基本信息 | 布尔值 |
| DATA__STOCK_BASIC_UPDATE_INTERVAL | 7 | 股票基本信息更新间隔（天） | 大于0 |

### 3. 日志配置

| 配置项 | 默认值 | 说明 | 验证规则 |
|--------|--------|------|----------|
| LOG__LEVEL | INFO | 日志级别 | DEBUG/INFO/WARNING/ERROR/CRITICAL |
| LOG__FILE_PATH | logs/app.log | 日志文件路径 | 字符串 |
| LOG__ROTATION | 10 MB | 日志文件轮换大小 | 字符串 |
| LOG__RETENTION | 30 days | 日志文件保留时间 | 字符串 |
| LOG__COMPRESSION | zip | 日志文件压缩格式 | 字符串 |

### 4. 插件配置

| 配置项 | 默认值 | 说明 | 验证规则 |
|--------|--------|------|----------|
| PLUGINS__PLUGINS__{plugin_name}__ENABLED | True | 插件是否启用 | 布尔值 |
| PLUGINS__PLUGINS__{plugin_name}__VERSION | None | 插件版本 | 字符串或None |
| PLUGINS__PLUGINS__{plugin_name}__CONFIG__{config_key} | {} | 插件特定配置 | 任意类型 |

## 配置热加载

系统支持配置热加载功能，当修改 `.env` 文件后，配置会自动重新加载，无需重启应用。

## 配置验证

系统会对配置项进行验证，确保配置值符合预期格式和范围。如果配置无效，系统会在启动时抛出异常并提示具体错误信息。

## 示例配置文件

```
# 数据库配置
DATABASE__HOST=localhost
DATABASE__PORT=3306
DATABASE__USERNAME=root
DATABASE__PASSWORD=
DATABASE__DATABASE=stock_quant

# 数据配置
DATA__TDX_DATA_PATH=D:\TDX\vipdoc
DATA__UPDATE_INTERVAL=3600
DATA__MAX_WORKERS=4

# 日志配置
LOG__LEVEL=INFO
LOG__FILE_PATH=logs/app.log
```
"""

