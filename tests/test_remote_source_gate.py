"""远程数据源开关回归测试：离线模式下不得落到 BaoStock/AkShare。"""

from types import SimpleNamespace

import pytest

import src.main as main_module
from src.data.managers.data_updater import DataUpdater
from src.data.services.sync_stock import repair_stock_basic


def _config(allow_remote: bool):
    return SimpleNamespace(data=SimpleNamespace(allow_remote_sources=allow_remote))


class _RemoteSource:
    name = 'FakeRemote'
    is_local = False

    def __init__(self):
        self.called = 0

    def update_stock_daily(self, **_kwargs):
        self.called += 1


class _LocalSource:
    name = 'FakeLocal'
    is_local = True


def test_try_sources_skips_remote_when_disabled():
    """远程关闭时：远程源不被调用，本地源无该方法 → 返回 False。"""
    updater = DataUpdater(_config(allow_remote=False))
    remote = _RemoteSource()
    updater.register_source(remote)
    updater.register_source(_LocalSource())

    assert updater._try_sources('update_stock_daily', '股票日线') is False
    assert remote.called == 0


def test_try_sources_calls_remote_when_enabled():
    """远程开启时维持原行为：首个成功的远程源被调用。"""
    updater = DataUpdater(_config(allow_remote=True))
    remote = _RemoteSource()
    updater.register_source(remote)

    assert updater._try_sources('update_stock_daily', '股票日线') is True
    assert remote.called == 1


def test_repair_stock_basic_skips_when_remote_disabled():
    """离线模式下 repair_stock_basic 直接跳过，不发起任何远程请求。"""
    result = repair_stock_basic(_config(allow_remote=False), None)

    assert result['skipped_reason'] == 'remote_sources_disabled'
    assert result['filled_industry'] == 0
    assert result['deep_fetched'] == 0


class _FakeDB:
    def connect(self):
        pass

    def create_tables(self):
        pass

    def cleanup(self):
        pass


def test_update_stock_cli_does_not_construct_remote_handler_when_disabled(monkeypatch):
    """--update-stock 在离线配置下不得构造 AkShareHandler（不会发起网络请求）。"""
    monkeypatch.setattr(main_module, 'DatabaseManager', lambda _config: _FakeDB())
    monkeypatch.setattr(main_module, 'sync_tdx_stock_to_database',
                        lambda config, db_manager: None)
    # 本地优先收口（合并导出 + stock_basic 重建）会读写真实数据文件，
    # 本用例只校验远程源门禁，故一并隔离；两者的自身行为由各自用例覆盖
    import src.data.services.local_first_basic as local_first_module

    monkeypatch.setattr(local_first_module, 'export_fund_basic',
                        lambda *_args, **_kwargs: {'counters': {}, 'diff_rows': 0,
                                                   'diff_csv': ''})
    monkeypatch.setattr(main_module, '_run_local_stock_basic_builder', lambda: True)

    import src.data.akshare_handler as akshare_module

    def _forbidden(*_args, **_kwargs):
        pytest.fail('离线模式下不应构造 AkShareHandler')

    monkeypatch.setattr(akshare_module, 'AkShareHandler', _forbidden)

    assert main_module._handle_update_stock_arg(_config(allow_remote=False)) is True


def test_sync_stock_daily_cli_forwards_codes_and_flags(monkeypatch):
    """--sync-stock-daily 把 --codes/--full/--dry-run 原样传给补库入口。"""
    captured = {}

    def _fake_fill(config, db_manager, **kwargs):
        captured.update(kwargs)
        return {'rows': 0}

    monkeypatch.setattr(main_module, 'DatabaseManager', lambda _config: _FakeDB())
    monkeypatch.setattr(
        'src.data.services.stock_daily_fill.fill_stock_daily', _fake_fill)

    args = SimpleNamespace(codes='600000.SH, 000001.SZ', full=True, dry_run=True)
    main_module._handle_sync_stock_daily_arg(_config(allow_remote=False), args)

    assert captured['ts_codes'] == ['600000.SH', '000001.SZ']
    assert captured['full'] is True
    assert captured['dry_run'] is True
