"""本地列存滞后（stale）语义的用例。

覆盖三件事：
1. stale 时返回列存数据并带结构化告警（不硬失败）；
2. 非 stale 时行为与改动前一致（不产生告警）；
3. 文件不存在 / 源端解析失败仍按错误处理，不被降级成空表。

空日期参数与内置源注册的用例见 ``tests/test_tdx_open_bounds_and_sources.py``。
"""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

import polars as pl
import pytest

from src.data.managers.data_fetcher import DataFetcher, DataSourceException

CODE = '600601.SH'


class _FakeStore:
    """最小列存替身：只实现 stale 路径用到的四个方法。"""

    enabled = True

    def __init__(self, frame: pl.DataFrame, stale: bool,
                 end: dt.date = dt.date(2026, 11, 9)) -> None:
        self.frame = frame
        self.stale = stale
        self.end = end
        self.read_calls = 0

    def is_stale(self, data_type, code, source_path) -> bool:  # noqa: ARG002
        return self.stale

    def read_daily(self, *args, **kwargs):  # noqa: ANN002, ANN003
        self.read_calls += 1
        return self.frame

    def coverage(self, data_type, code):  # noqa: ARG002
        return SimpleNamespace(start=dt.date(1990, 12, 19), end=self.end)


def _frame() -> pl.DataFrame:
    return pl.DataFrame({'date': [dt.date(2026, 11, 9)], 'close': [10.0]})


def _fetcher(monkeypatch, tmp_path, store, *, source_exists=True,
             source_last=dt.date(2026, 11, 10)) -> DataFetcher:
    fetcher = DataFetcher(SimpleNamespace(
        data=SimpleNamespace(local_store_enabled=True, allow_remote_sources=False,
                             max_workers=2)))
    monkeypatch.setattr(fetcher, '_local_store_instance', lambda: store)
    path = tmp_path / 'sh600601.day'
    if source_exists:
        path.write_bytes(b'\x00')
    monkeypatch.setattr(fetcher, '_tdx_source_path', lambda data_type, code: path)
    monkeypatch.setattr(fetcher, '_local_day_file_path', lambda code: path)
    monkeypatch.setattr('src.data.tdx_day_parser.read_first_last_trade_date',
                        lambda p: (dt.date(1990, 12, 19), source_last))
    return fetcher


def test_stale_returns_local_frame_and_notice(monkeypatch, tmp_path):
    store = _FakeStore(_frame(), stale=True)
    fetcher = _fetcher(monkeypatch, tmp_path, store)

    frame = fetcher._get_data_from_local_store('stock', CODE, '2026-11-01', '2026-11-09', '1d')

    assert frame is not None and not frame.is_empty(), 'stale 必须返回列存数据而不是 None'
    assert store.read_calls == 1
    notices = fetcher.drain_stale_notices()
    assert len(notices) == 1
    payload = notices[0].to_dict()
    assert payload == {
        'ts_code': CODE,
        'data_type': 'stock',
        'is_stale': True,
        'local_last_trade_date': '2026-11-09',
        'source_last_trade_date': '2026-11-10',
        'lag_days': 1,
    }
    # drain 必须清空，避免同一告警被下一次回测重复计入
    assert fetcher.drain_stale_notices() == []


def test_non_stale_returns_local_frame_without_notice(monkeypatch, tmp_path):
    store = _FakeStore(_frame(), stale=False)
    fetcher = _fetcher(monkeypatch, tmp_path, store)

    frame = fetcher._get_data_from_local_store('stock', CODE, '2026-11-01', '2026-11-09', '1d')

    assert frame is not None and frame.equals(_frame())
    assert store.read_calls == 1, '非 stale 仍走原有 read_daily 路径，只读一次'
    assert fetcher.peek_stale_notices() == []


def test_stale_with_empty_local_falls_back_to_source(monkeypatch, tmp_path):
    store = _FakeStore(pl.DataFrame({'date': [], 'close': []}), stale=True)
    fetcher = _fetcher(monkeypatch, tmp_path, store)
    declined: list[str] = []

    frame = fetcher._get_data_from_local_store(
        'stock', CODE, '2026-11-01', '2026-11-09', '1d', declined)

    assert frame is None
    assert declined == ['stale'], '列存无可用数据时必须回退数据源并保留原因'
    assert len(fetcher.peek_stale_notices()) == 1


def test_missing_local_file_raises_file_missing(monkeypatch, tmp_path):
    fetcher = _fetcher(monkeypatch, tmp_path, _FakeStore(_frame(), stale=True),
                       source_exists=False)

    with pytest.raises(DataSourceException) as excinfo:
        fetcher._get_data_from_sources('stock', CODE, '', '', '1d')

    assert excinfo.value.details['reason'] == 'file_missing'
    assert '本地无此文件' in str(excinfo.value)


class _BrokenSource:
    name = 'broken'

    def get_stock_data(self, *args, **kwargs):  # noqa: ANN002, ANN003, ARG002
        raise ValueError('payload 解析失败')


def test_source_parse_failure_is_reported_not_swallowed(monkeypatch, tmp_path):
    fetcher = _fetcher(monkeypatch, tmp_path, _FakeStore(_frame(), stale=True))
    broken = _BrokenSource()
    monkeypatch.setattr(fetcher, '_get_candidate_sources', lambda data_type: [broken])
    fetcher._source_capabilities = {
        broken: {'supports_stock': True, 'supports_index': False, 'is_local': True}}

    with pytest.raises(DataSourceException) as excinfo:
        fetcher._get_data_from_sources('stock', CODE, '', '', '1d')

    assert excinfo.value.details['reason'] == 'parse_failed'
    assert '解析失败' in str(excinfo.value)
