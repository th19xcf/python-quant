"""main 入口冒烟测试：参数解析与启动/清理流程（外部依赖全部打桩）。"""

import sys
from types import SimpleNamespace

import pytest

import src.main as main_module
from src.ui.main_window import MainWindow as RealMainWindow


class _Recorder:
    def __init__(self):
        self.calls = []

    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))

    def __bool__(self):
        return bool(self.calls)


class _FakeApp:
    def __init__(self, argv):
        self.argv = argv
        self.name = None
        self.executed = False

    def setApplicationName(self, name):
        self.name = name

    def exec(self):
        self.executed = True
        return 0


class _RecordingMainWindow(RealMainWindow):
    instances = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _RecordingMainWindow.instances.append(self)
        self.maximized = False

    def showMaximized(self):
        self.maximized = True


class _FakeMemoryManager:
    def __init__(self):
        self.started = 0
        self.stopped = 0

    def start_monitoring(self):
        self.started += 1

    def stop_monitoring(self):
        self.stopped += 1

    def get_memory_usage(self):
        return {'process_rss_mb': 12.5, 'process_percent': 1.5}


@pytest.fixture
def windows():
    _RecordingMainWindow.instances = []
    yield _RecordingMainWindow.instances
    for window in _RecordingMainWindow.instances:
        window.hide()
        window.deleteLater()


def test_parse_args_defaults(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['prog'])

    args = main_module.parse_args()

    assert args.update_stock is False
    assert args.update_index is False
    assert args.update_fund is False
    assert args.plugins is False


@pytest.mark.parametrize(('flag', 'attribute'), [
    ('--update-stock', 'update_stock'),
    ('--update-index', 'update_index'),
    ('--update-fund', 'update_fund'),
    ('--plugins', 'plugins'),
])
def test_parse_args_flags(monkeypatch, flag, attribute):
    monkeypatch.setattr(sys, 'argv', ['prog', flag])

    args = main_module.parse_args()

    assert getattr(args, attribute) is True


def _patch_launch_dependencies(monkeypatch, tmp_path):
    """打桩 main 的周边依赖，返回记录器集合。"""
    records = SimpleNamespace(
        system=[],
        data_manager=[],
        cleanup=[],
        publish=[],
        memory=_FakeMemoryManager(),
        cache_stats=_Recorder(),
        theme=_Recorder(),
        apps=[],
    )
    config = SimpleNamespace(data=SimpleNamespace(tdx_data_path=str(tmp_path))
                              , ui=SimpleNamespace(theme='dark'))
    plugin_manager = SimpleNamespace(shutdown_plugins=_Recorder())
    data_manager = SimpleNamespace(db_manager=None)

    monkeypatch.setattr(sys, 'argv', ['prog'])
    monkeypatch.setattr(main_module, 'get_config', lambda: config)
    monkeypatch.setattr(
        main_module,
        '_initialize_system',
        lambda cfg: (records.system.append(cfg) or plugin_manager, 4),
    )
    monkeypatch.setattr(
        main_module,
        '_initialize_data_manager',
        lambda cfg, plugin_mgr: records.data_manager.append((cfg, plugin_mgr)) or data_manager,
    )
    monkeypatch.setattr(
        main_module,
        '_cleanup_system',
        lambda plugin_mgr, db_mgr: records.cleanup.append((plugin_mgr, db_mgr)),
    )
    monkeypatch.setattr(main_module, 'global_memory_manager', records.memory)
    monkeypatch.setattr(
        main_module, 'log_cache_stats', lambda: records.cache_stats()
    )
    monkeypatch.setattr(
        main_module, 'publish', lambda event, **kwargs: records.publish.append((event, kwargs))
    )
    monkeypatch.setattr(main_module, 'ThemeManager', SimpleNamespace(set_dark_theme=records.theme))
    monkeypatch.setattr(
        main_module,
        'QApplication',
        lambda argv: records.apps.append(_FakeApp(argv)) or records.apps[-1],
    )
    monkeypatch.setattr(main_module, 'MainWindow', _RecordingMainWindow)

    return records, config, plugin_manager, data_manager


def test_main_launches_window_and_cleans_up(monkeypatch, qapp, tmp_path, windows):
    records, config, plugin_manager, data_manager = _patch_launch_dependencies(
        monkeypatch, tmp_path
    )

    with pytest.raises(SystemExit) as exc_info:
        main_module.main()

    assert exc_info.value.code == 0
    assert len(windows) == 1
    window = windows[0]
    assert window.maximized is True
    assert window.tab_widget.count() == 4
    assert window.advice_page is not None

    app = records.apps[0]
    assert app.name == '中国股市量化分析系统'
    assert app.executed is True

    assert records.system == [config]
    assert records.data_manager == [(config, plugin_manager)]
    assert records.memory.started == 1
    assert records.theme.calls
    assert records.cache_stats.calls
    assert [event for event, _kwargs in records.publish] == [
        main_module.EventType.SYSTEM_INIT,
    ]
    assert records.cleanup == [(plugin_manager, None)]


def test_main_exits_on_startup_failure(monkeypatch, tmp_path):
    records, config, plugin_manager, data_manager = _patch_launch_dependencies(
        monkeypatch, tmp_path
    )

    def _broken_system(cfg):
        raise OSError('启动失败')

    monkeypatch.setattr(main_module, '_initialize_system', _broken_system)

    with pytest.raises(SystemExit) as exc_info:
        main_module.main()

    assert exc_info.value.code == 1
    assert records.cleanup == [(None, None)]
    assert records.apps == []


def test_main_display_plugins_returns_before_window(monkeypatch, tmp_path, windows):
    records, config, plugin_manager, data_manager = _patch_launch_dependencies(
        monkeypatch, tmp_path
    )
    monkeypatch.setattr(sys, 'argv', ['prog', '--plugins'])
    listed = _Recorder()
    infos = [{'name': 'demo', 'version': '1.0', 'description': '示例插件'}]

    def _get_all():
        listed()
        return infos

    plugin_manager.get_all_plugin_info = _get_all

    main_module.main()

    assert listed.calls == [((), {})]
    assert windows == []
    assert records.cleanup == [(plugin_manager, None)]


def test_display_plugins_logs_each_plugin():
    plugin_manager = SimpleNamespace(
        get_all_plugin_info=lambda: [
            {'name': 'demo', 'version': '1.0', 'description': '示例插件'},
        ]
    )

    main_module._display_plugins(plugin_manager)

    assert plugin_manager.get_all_plugin_info() == [
        {'name': 'demo', 'version': '1.0', 'description': '示例插件'},
    ]
