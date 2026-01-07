#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
插件系统模块
"""

from .plugin_base import PluginBase, DataSourcePlugin, IndicatorPlugin, StrategyPlugin, VisualizationPlugin
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