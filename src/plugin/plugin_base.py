#!/usr/bin/env python3

"""
插件基类定义
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Type

import pandas as pd
import polars as pl
from loguru import logger
from pydantic import ValidationError
from pydantic_settings import BaseSettings

from src.utils.plugin_config_manager import get_plugin_config_manager


class PluginBase(ABC):
    """
    所有插件的基类，定义插件的基本接口
    """

    def __init__(self):
        self.name = "Unknown"
        self.version = "0.0.1"
        self.author = "Unknown"
        self.description = ""
        self.enabled = True
        self.config = {}
        self.plugin_manager = None  # 插件管理器实例，由PluginManager在初始化时设置
        self._config_model = None  # 插件配置模型类
        self._default_config = {}  # 插件默认配置
        self._plugin_config_manager = get_plugin_config_manager()  # 插件配置管理器实例
        # 插件依赖声明，格式: [{'name': 'plugin_name', 'version': '>=1.0.0'}, ...]
        self.dependencies = []
        # 插件加载状态: 'unloaded', 'loaded', 'initialized'
        self._load_status = 'unloaded'

    @abstractmethod
    def get_name(self) -> str:
        """
        获取插件名称

        Returns:
            str: 插件名称
        """
        pass

    @abstractmethod
    def get_version(self) -> str:
        """
        获取插件版本

        Returns:
            str: 插件版本
        """
        pass

    @abstractmethod
    def get_author(self) -> str:
        """
        获取插件作者

        Returns:
            str: 插件作者
        """
        pass

    @abstractmethod
    def get_description(self) -> str:
        """
        获取插件描述

        Returns:
            str: 插件描述
        """
        pass

    def initialize(self, config: Dict[str, Any]) -> bool:
        """
        初始化插件

        Args:
            config: 插件配置

        Returns:
            bool: 初始化是否成功
        """
        self.config = config
        return True

    def shutdown(self) -> bool:
        """
        关闭插件

        Returns:
            bool: 关闭是否成功
        """
        return True

    def is_enabled(self) -> bool:
        """
        检查插件是否启用

        Returns:
            bool: 是否启用
        """
        return self.enabled

    def set_enabled(self, enabled: bool) -> None:
        """
        设置插件启用状态

        Args:
            enabled: 是否启用
        """
        self.enabled = enabled

    def get_config(self) -> Dict[str, Any]:
        """
        获取插件配置

        Returns:
            Dict[str, Any]: 插件配置
        """
        return self.config

    def update_config(self, config: Dict[str, Any]) -> None:
        """
        更新插件配置

        Args:
            config: 新的配置
        """
        old_config = self.config.copy()
        self.config.update(config)

        # 发布配置变更事件
        from src.utils.event_bus import publish
        publish('plugin_config_changed',
                       plugin_name=self.get_name(),
                       old_config=old_config,
                       new_config=self.config)

    def reload_config(self) -> None:
        """
        重新加载插件配置
        """
        # 使用插件配置管理器创建配置
        merged_config = self._plugin_config_manager.create_plugin_config(
            self.get_name(),
            self._default_config
        )

        # 更新插件配置
        self.config = merged_config

        # 发布配置变更事件
        from src.utils.event_bus import publish
        publish('plugin_config_reloaded',
                       plugin_name=self.get_name(),
                       config=self.config)

    def validate_config(self) -> bool:
        """
        验证插件配置

        Returns:
            bool: 配置是否有效
        """
        if not self._config_model:
            return True

        try:
            self._config_model(**self.config)
            return True
        except ValidationError as e:
            logger.error(f"插件配置验证失败: {self.get_name()}, 错误: {e}")
            return False

    def get_config_documentation(self) -> str | None:
        """
        获取插件配置文档

        Returns:
            Optional[str]: 插件配置文档
        """
        if not self._config_model:
            return None

        return self._plugin_config_manager.get_config_documentation(self.get_name())

    def on_config_changed(self, old_config: Dict[str, Any], new_config: Dict[str, Any]) -> None:
        """
        配置变更时的回调方法，插件可以重写此方法

        Args:
            old_config: 旧配置
            new_config: 新配置
        """
        pass

    def get_dependencies(self) -> List[Dict[str, str]]:
        """
        获取插件依赖列表

        Returns:
            List[Dict[str, str]]: 依赖列表，格式: [{'name': 'plugin_name', 'version': '>=1.0.0'}, ...]
        """
        return self.dependencies.copy()

    def get_load_status(self) -> str:
        """
        获取插件加载状态

        Returns:
            str: 加载状态，'unloaded', 'loaded', 'initialized'
        """
        return self._load_status

    def set_load_status(self, status: str) -> None:
        """
        设置插件加载状态

        Args:
            status: 加载状态，'unloaded', 'loaded', 'initialized'
        """
        self._load_status = status

    def supports_polars(self) -> bool:
        """
        检查插件是否支持polars数据处理

        Returns:
            bool: 是否支持polars数据处理
        """
        return False


class DataSourcePlugin(PluginBase):
    """
    数据源插件基类
    """

    @abstractmethod
    def get_stock_data(self, ts_code: str, start_date: str, end_date: str, freq: str = "daily") -> Any:
        """
        获取股票数据

        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            freq: 周期，daily或minute

        Returns:
            Any: 股票数据，通常为DataFrame
        """
        pass

    @abstractmethod
    def get_index_data(self, ts_code: str, start_date: str, end_date: str, freq: str = "daily") -> Any:
        """
        获取指数数据

        Args:
            ts_code: 指数代码
            start_date: 开始日期
            end_date: 结束日期
            freq: 周期，daily或minute

        Returns:
            Any: 指数数据，通常为DataFrame
        """
        pass

    @abstractmethod
    def update_stock_basic(self) -> bool:
        """
        更新股票基本信息

        Returns:
            bool: 更新是否成功
        """
        pass


class IndicatorPlugin(PluginBase):
    """
    技术指标插件基类
    """

    @abstractmethod
    def calculate(self, data: Any, **kwargs) -> Any:
        """
        计算技术指标

        Args:
            data: 股票数据，通常为DataFrame
            **kwargs: 指标参数

        Returns:
            Any: 包含指标的数据，通常为DataFrame
        """
        pass

    @abstractmethod
    def get_required_columns(self) -> List[str]:
        """
        获取计算指标所需的列名

        Returns:
            List[str]: 所需列名列表
        """
        pass

    @abstractmethod
    def get_output_columns(self) -> List[str]:
        """
        获取指标计算输出的列名

        Returns:
            List[str]: 输出列名列表
        """
        pass

    def calculate_polars(self, data: Any, **kwargs) -> Any:
        """
        使用polars计算技术指标

        默认实现：按 get_required_columns() 声明裁剪输入列后再转 pandas，
        输出仅回转 get_output_columns() 声明的列（+date），避免全量列
        在 Polars↔Pandas 之间往返转换。插件未声明所需列时回退为全量转换。

        Args:
            data: 股票数据，polars DataFrame
            **kwargs: 指标参数

        Returns:
            Any: 包含指标的polars DataFrame
        """
        source = data
        try:
            required = self.get_required_columns()
        # 兜底理由：插件为外部扩展代码，异常类型不可预知
        except Exception:  # noqa: BLE001
            required = None
        if required:
            base = ['date', 'open', 'high', 'low', 'close', 'volume']
            cols = [c for c in dict.fromkeys(base + list(required)) if c in data.columns]
            if cols:
                source = data.select(cols)
        df_pd = source.to_pandas()
        result_pd = self.calculate(df_pd, **kwargs)
        try:
            output_cols = self.get_output_columns()
        # 兜底理由：插件为外部扩展代码，异常类型不可预知
        except Exception:  # noqa: BLE001
            output_cols = None
        if output_cols and isinstance(result_pd, pd.DataFrame):
            keep = [c for c in ['date'] + list(output_cols) if c in result_pd.columns]
            if keep:
                result_pd = result_pd[keep]
        return pl.from_pandas(result_pd)


class StrategyPlugin(PluginBase):
    """
    策略插件基类
    """

    @abstractmethod
    def initialize(self, config: Dict[str, Any]) -> bool:
        """
        初始化策略

        Args:
            config: 策略配置

        Returns:
            bool: 初始化是否成功
        """
        pass

    @abstractmethod
    def on_bar(self, data: Any) -> Dict[str, Any]:
        """
        K线数据更新时调用

        Args:
            data: K线数据

        Returns:
            Dict[str, Any]: 策略信号，如{'signal': 'buy', 'price': 10.0}
        """
        pass

    @abstractmethod
    def on_tick(self, data: Any) -> Dict[str, Any]:
        """
         tick数据更新时调用

        Args:
            data: tick数据

        Returns:
            Dict[str, Any]: 策略信号
        """
        pass

    @abstractmethod
    def get_parameters(self) -> Dict[str, Any]:
        """
        获取策略参数

        Returns:
            Dict[str, Any]: 策略参数
        """
        pass

    @abstractmethod
    def set_parameters(self, params: Dict[str, Any]) -> bool:
        """
        设置策略参数

        Args:
            params: 策略参数

        Returns:
            bool: 设置是否成功
        """
        pass


class VisualizationPlugin(PluginBase):
    """
    可视化插件基类
    """

    @abstractmethod
    def render(self, data: Any, container: Any = None, **kwargs) -> Any:
        """
        渲染可视化内容

        Args:
            data: 要可视化的数据
            container: 渲染容器，如Qt的Widget
            **kwargs: 渲染参数

        Returns:
            Any: 渲染结果，如图表对象
        """
        pass

    @abstractmethod
    def get_supported_data_types(self) -> List[str]:
        """
        获取支持的数据类型

        Returns:
            List[str]: 支持的数据类型列表，如['stock', 'index', 'indicator']
        """
        pass

    @abstractmethod
    def update_data(self, data: Any) -> None:
        """
        更新可视化数据

        Args:
            data: 新的数据
        """
        pass
