#!/usr/bin/env python3

"""
插件系统模块（已暂停）

当前状态：启动流程不再扫描/加载/初始化插件。历史数据源插件
（tdx / baostock / macro / news 等）已删除，仅剩的可视化插件也没有
实际消费方；本模块保留注册表与管理器实现供未来重启，但不应被当作
已生效的能力。
"""

from .plugin_base import (
    DataSourcePlugin,
    IndicatorPlugin,
    PluginBase,
    StrategyPlugin,
    VisualizationPlugin,
)
from .plugin_manager import PluginManager
from .plugin_registry import PluginRegistry

__all__ = [
    'PluginBase',
    'DataSourcePlugin',
    'IndicatorPlugin',
    'StrategyPlugin',
    'VisualizationPlugin',
    'PluginManager',
    'PluginRegistry'
]
