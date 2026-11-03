#!/usr/bin/env python3

"""
数据源注册唯一性回归测试。

项目只允许一套数据源注册路径：DataService._init_builtin_datasources。
禁止在 plugins/ 下再放一层包装同一 handler 的 DataSourcePlugin 适配器，
否则同一 handler 会被实例化两次（两套数据库连接、重复抓取、结果不一致）。
"""

import importlib.util

from src.data.services.data_service import DataService
from src.plugin.plugin_manager import PluginManager
from src.utils.config import Config


def test_plugins_directory_has_no_datasource_adapters():
    """插件目录不得再提供数据源插件（数据源已由 src/data 内置注册）。"""
    plugin_manager = PluginManager(Config())
    plugin_manager.load_plugins()

    assert plugin_manager.get_available_datasource_plugins() == {}


def test_builtin_sources_are_registered_exactly_once():
    """远程开关开启时内置数据源必须各注册一次，且不存在插件数据源重复注册。"""
    config = Config()
    # 本用例校验「注册唯一性」，需显式开启远程源（默认离线只注册 TDX）
    config.data.allow_remote_sources = True
    plugin_manager = PluginManager(config)
    plugin_manager.load_plugins()

    service = DataService(config, db_manager=None, plugin_manager=plugin_manager)
    source_types = [type(source) for source in service.data_fetcher.sources]

    assert len(source_types) == len(set(source_types)), f'数据源重复注册: {[t.__name__ for t in source_types]}'
    assert not any(t.__name__.endswith('DataSourcePlugin') for t in source_types)

    registered = {t.__name__ for t in source_types}
    # BaoStock 为可选依赖（poetry group `baostock`），未安装时不应注册该数据源
    expected = {'TdxHandler', 'AkShareHandler'}
    if importlib.util.find_spec('baostock') is not None:
        expected.add('BaostockHandler')
    assert expected <= registered
    assert registered <= {'TdxHandler', 'BaostockHandler', 'AkShareHandler', 'MacroHandler', 'NewsHandler'}


def test_offline_config_only_registers_local_tdx():
    """默认离线配置下只注册本地 TDX，BaoStock/AkShare 完全不实例化。"""
    config = Config()
    assert config.data.allow_remote_sources is False

    plugin_manager = PluginManager(config)
    plugin_manager.load_plugins()

    service = DataService(config, db_manager=None, plugin_manager=plugin_manager)
    registered = {type(source).__name__ for source in service.data_fetcher.sources}

    assert 'TdxHandler' in registered
    assert 'AkShareHandler' not in registered
    assert 'BaostockHandler' not in registered
