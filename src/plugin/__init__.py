#!/usr/bin/env python3

"""
插件系统模块
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
