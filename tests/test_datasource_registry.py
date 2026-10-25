#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据源注册唯一性回归测试。

项目只允许一套数据源注册路径：DataService._init_builtin_datasources。
禁止在 plugins/ 下再放一层包装同一 handler 的 DataSourcePlugin 适配器，
否则同一 handler 会被实例化两次（两套数据库连接、重复抓取、结果不一致）。
"""

from src.data.services.data_service import DataService
from src.plugin.plugin_manager import PluginManager
from src.utils.config import Config


def test_plugins_directory_has_no_datasource_adapters():
    """插件目录不得再提供数据源插件（数据源已由 src/data 内置注册）。"""
    plugin_manager = PluginManager(Config())
    plugin_manager.load_plugins()

    assert plugin_manager.get_available_datasource_plugins() == {}


def test_builtin_sources_are_registered_exactly_once():
    """内置数据源必须各注册一次，且不存在插件数据源重复注册。"""
    config = Config()
    plugin_manager = PluginManager(config)
    plugin_manager.load_plugins()

    service = DataService(config, db_manager=None, plugin_manager=plugin_manager)
    source_types = [type(source) for source in service.data_fetcher.sources]

    assert len(source_types) == len(set(source_types)), f'数据源重复注册: {[t.__name__ for t in source_types]}'
    assert not any(t.__name__.endswith('DataSourcePlugin') for t in source_types)

    registered = {t.__name__ for t in source_types}
    assert {'TdxHandler', 'BaostockHandler'} <= registered
    assert registered <= {'TdxHandler', 'BaostockHandler', 'AkShareHandler', 'MacroHandler', 'NewsHandler'}
