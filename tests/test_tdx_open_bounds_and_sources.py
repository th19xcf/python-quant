"""通达信参数语义与内置数据源注册的用例。

覆盖两件事：
1. 空串 / ``None`` 日期表示开放区间（不限上下界），非法格式必须抛
   ``InvalidDateArgument`` 而不是被兜底 ``except`` 吞成空表；
2. ``run_backtest`` 自己 new 的 ``DataFetcher`` 必须注册内置源，
   否则本地列存判 stale 后没有通达信可回退，会被误报成「文件无数据」。
"""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

import polars as pl
import pytest

from src.core.usecases.run_backtest import _register_builtin_sources
from src.data.managers.data_fetcher import DataFetcher
from src.data.tdx_handler import InvalidDateArgument, TdxHandler, _parse_optional_date

CODE = '600601.SH'


def _config() -> SimpleNamespace:
    return SimpleNamespace(data=SimpleNamespace(
        local_store_enabled=True, allow_remote_sources=False,
        max_workers=2, tdx_data_path=''))


@pytest.mark.parametrize('value,expected', [
    ('', None),
    ('   ', None),
    (None, None),
    ('2026-11-09', dt.date(2026, 11, 9)),
])
def test_parse_optional_date_accepts_open_bounds(value, expected):
    assert _parse_optional_date(value, 'start_date') == expected


@pytest.mark.parametrize('value', ['2026/11/09', '20261109', 'not-a-date'])
def test_parse_optional_date_rejects_invalid_format(value):
    with pytest.raises(InvalidDateArgument):
        _parse_optional_date(value, 'start_date')


def test_tdx_get_stock_data_propagates_invalid_date():
    handler = TdxHandler(_config(), None)
    try:
        with pytest.raises(InvalidDateArgument):
            handler.get_stock_data(CODE, '2026/11/09', '')
    finally:
        handler.close()


def test_tdx_get_stock_data_treats_empty_bounds_as_open(monkeypatch):
    """空串必须读成「不限」，而不是 strptime('') 抛错后被吞成空表。"""
    handler = TdxHandler(_config(), None)

    def _fake_kline(*args, **kwargs):  # noqa: ANN002, ANN003, ARG001
        return pl.DataFrame({
            'date': [dt.date(2026, 11, 9), dt.date(2026, 11, 10)],
            'open': [10.0, 10.1], 'high': [10.2, 10.3], 'low': [9.9, 10.0],
            'close': [10.1, 10.2], 'volume': [1000, 1100], 'amount': [1.0e4, 1.1e4],
            'qfq_factor': [1.0, 1.0], 'hfq_factor': [1.0, 1.0],
        }).lazy()

    monkeypatch.setattr(handler, 'get_kline_data', _fake_kline)
    try:
        frame = handler.get_stock_data(CODE, '', '')
    finally:
        handler.close()

    assert not frame.is_empty(), '空串端点应返回全部行，而不是空表'
    assert frame.height == 2


def test_register_builtin_sources_makes_tdx_available():
    fetcher = DataFetcher(_config())
    _register_builtin_sources(fetcher, _config(), None)

    available = fetcher.get_available_sources()
    assert available, '注册后必须有可用数据源，否则 stale 时无处回退'
    assert len(available) == 1, '远程关闭时只应注册本地通达信'
    assert isinstance(available[0], TdxHandler)


class _StubLocalSource:
    """最小本地源替身：证明「列存判 stale → 回退已注册源 → 取到数据」链路可用。"""

    name = 'stub_local'
    is_local = True

    def get_stock_data(self, *args, **kwargs):  # noqa: ANN002, ANN003, ARG002
        return pl.DataFrame({'date': [dt.date(2026, 11, 10)], 'close': [10.0]})


def test_stale_local_store_falls_back_to_registered_source(monkeypatch, tmp_path):
    class _StaleEmptyStore:
        enabled = True

        def is_stale(self, data_type, code, source_path):  # noqa: ARG002
            return True

        def read_daily(self, *args, **kwargs):  # noqa: ANN002, ANN003
            return pl.DataFrame({'date': [], 'close': []})

        def coverage(self, data_type, code):  # noqa: ARG002
            return SimpleNamespace(start=dt.date(1990, 12, 19), end=dt.date(2026, 11, 9))

    fetcher = DataFetcher(_config())
    fetcher.register_source(_StubLocalSource())
    path = tmp_path / 'sh600601.day'
    path.write_bytes(b'\x00')
    monkeypatch.setattr(fetcher, '_local_store_instance', lambda: _StaleEmptyStore())
    monkeypatch.setattr(fetcher, '_local_day_file_path', lambda code: path)

    declined: list[str] = []
    local = fetcher._get_data_from_local_store(
        'stock', CODE, '', '', '1d', declined)
    assert local is None
    assert declined == ['stale'], '列存无数据时必须把原因交给上层归因'

    frame = fetcher._get_data_from_sources(
        'stock', CODE, '', '', '1d', 'none', local_declined=declined)
    assert not frame.is_empty(), '回退源已注册时 stale 情形必须能取到数据'
    assert frame.height == 1
