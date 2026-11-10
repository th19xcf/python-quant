#!/usr/bin/env python3

"""``--source auto`` 必须受 ``data.allow_remote_sources`` 约束（离线不得触网）。"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.main import (
    OFFLINE_SOURCE_NOTICE,
    _handle_sync_stock_daily_arg,
    resolve_fill_source,
)
from src.utils.exceptions import DataSourceException


class _DummyDB:
    """替身：避免测试连 MySQL。"""

    def __init__(self, config):
        self.config = config

    def connect(self):
        return None

    def cleanup(self):
        return None


def _args(source: str = 'auto') -> SimpleNamespace:
    return SimpleNamespace(codes=None, max_date='', since='', full=False,
                           dry_run=True, progress_file='', source=source)


def _config(allow_remote: bool) -> SimpleNamespace:
    return SimpleNamespace(
        data=SimpleNamespace(allow_remote_sources=allow_remote))


def _run(monkeypatch, allow_remote: bool,
         source: str = 'auto') -> tuple[list[str], list[str]]:
    """跑一遍 CLI 处理函数，返回传给补库函数的 source 与告警输出。"""
    import src.data.services.stock_daily_fill as fill_mod
    import src.main as main_mod

    calls: list[str] = []
    warned: list[str] = []

    def _fake_fill(config, db_manager, **kwargs):
        calls.append(kwargs['source'])
        return {}

    monkeypatch.setattr(main_mod, 'DatabaseManager', _DummyDB)
    monkeypatch.setattr(fill_mod, 'fill_stock_daily', _fake_fill)
    monkeypatch.setattr(main_mod.logger, 'warning',
                        lambda message, *args, **kwargs: warned.append(str(message)))
    _handle_sync_stock_daily_arg(_config(allow_remote), _args(source))
    return calls, warned


@pytest.mark.parametrize('requested,allowed,expected', [
    ('auto', False, ('local', True)),
    ('auto', True, ('auto', False)),
    ('local', False, ('local', False)),
    ('local', True, ('local', False)),
    ('mootdx', True, ('mootdx', False)),
])
def test_resolve_fill_source(requested, allowed, expected):
    assert resolve_fill_source(requested, allowed) == expected


def test_auto_offline_degrades_to_local_and_says_so(monkeypatch):
    calls, warned = _run(monkeypatch, allow_remote=False)
    assert calls == ['local']
    assert OFFLINE_SOURCE_NOTICE in warned


def test_auto_online_keeps_auto(monkeypatch):
    calls, warned = _run(monkeypatch, allow_remote=True)
    assert calls == ['auto']
    assert OFFLINE_SOURCE_NOTICE not in warned


def test_mootdx_offline_is_refused(monkeypatch):
    with pytest.raises(DataSourceException):
        _run(monkeypatch, allow_remote=False, source='mootdx')


def test_local_source_never_constructs_online_client(monkeypatch):
    """``local`` 分支不创建任何网络客户端；``auto`` 才会创建。"""
    import src.data.services.stock_daily_fill as fill_mod

    constructed: list[str] = []

    class _Client:
        def __init__(self):
            constructed.append('client')

    monkeypatch.setattr(fill_mod, 'TdxOnlineClient', _Client)
    assert fill_mod._prepare_client('local', None) == (None, False)
    assert constructed == []
    fill_mod._prepare_client('auto', None)
    assert constructed == ['client']
