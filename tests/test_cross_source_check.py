#!/usr/bin/env python3

"""跨源对拍逻辑测试（全部离线，不访问任何远程源）。"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

from src.data.quality import cross_check as module
from src.data.quality.cross_check import (
    RateLimiter,
    build_adapters,
    build_remote_config,
    compare_frames,
    default_date_range,
    run_cross_check,
    sample_codes,
)


def make_frame(rows: int = 4, start: date = date(2024, 1, 2),
               closes: list[float] | None = None) -> pl.DataFrame:
    dates: list[date] = []
    current = start
    while len(dates) < rows:
        if current.weekday() < 5:
            dates.append(current)
        current += timedelta(days=1)
    close_values = closes or [10.0 + index for index in range(rows)]
    return pl.DataFrame({
        'date': dates,
        'open': close_values,
        'high': [value + 0.5 for value in close_values],
        'low': [value - 0.5 for value in close_values],
        'close': close_values,
        'volume': [1000.0 for _ in close_values],
        'amount': [value * 1000 * 100 for value in close_values],
    })


def test_compare_frames_identical_is_all_ok() -> None:
    frame = make_frame()
    fields, overlap, missing_local, missing_remote, local_rows = compare_frames(
        frame, frame.clone())
    assert overlap == 4
    assert missing_local == missing_remote == 0
    assert local_rows == 4
    assert all(item.status == 'ok' for item in fields)
    assert all(item.diff_rows == 0 for item in fields)


def test_compare_frames_detects_price_diff_and_missing_dates() -> None:
    local = make_frame()
    remote = make_frame(closes=[10.0, 11.0, 12.0, 99.0])
    fields, overlap, missing_local, missing_remote, _ = compare_frames(
        local, remote)
    close = next(item for item in fields if item.field == 'close')
    assert close.diff_rows == 1
    assert close.max_abs_diff == pytest.approx(86.0)
    assert missing_local == 0
    assert missing_remote == 0

    remote_short = make_frame(rows=3)
    fields, overlap, missing_local, missing_remote, _ = compare_frames(
        local, remote_short)
    assert overlap == 3
    assert missing_local == 0
    assert missing_remote == 1
    assert all(item.status == 'ok' for item in fields)


def test_compare_frames_empty_remote() -> None:
    local = make_frame()
    empty = pl.DataFrame(schema={'date': pl.Date})
    fields, overlap, missing_local, missing_remote, local_rows = compare_frames(
        local, empty)
    assert fields == []
    assert overlap == 0
    assert missing_local == 0
    assert missing_remote == 4
    assert local_rows == 4


def test_rate_limiter_spacing_and_backoff(monkeypatch) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr(module.time, 'sleep', lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(module.time, 'monotonic', lambda: 100.0)

    limiter = RateLimiter(0.5, 0, 0, 0)
    limiter.call(lambda: make_frame())
    limiter.call(lambda: make_frame())
    assert sleeps == [0.5]

    sleeps.clear()
    attempts = {'count': 0}

    def flaky() -> pl.DataFrame:
        attempts['count'] += 1
        if attempts['count'] < 3:
            raise RuntimeError('temporary')
        return make_frame()

    retry_limiter = RateLimiter(0.0, 2, 2.0, 8.0)
    result = retry_limiter.call(flaky)
    assert result.height == 4
    assert attempts['count'] == 3
    assert sleeps == [2.0, 4.0]


def test_rate_limiter_raises_after_retries() -> None:
    limiter = RateLimiter(0.0, 1, 1.0, 1.0)
    with pytest.raises(RuntimeError):
        limiter.call(lambda: (_ for _ in ()).throw(RuntimeError('boom')))


class FakeStore:
    def __init__(self, frames: dict[str, pl.DataFrame]) -> None:
        self._frames = frames

    def codes(self, data_type: str = 'stock') -> list[str]:
        return sorted(self._frames) if data_type == 'stock' else []

    def read_file(self, data_type: str, ts_code: str) -> pl.DataFrame | None:
        return self._frames.get(ts_code) if data_type == 'stock' else None

    def covers(self, data_type: str, ts_code: str, start: date,
               end: date) -> bool:
        return ts_code in self._frames


class FakeAdapter:
    def __init__(self, name: str, responses: dict[str, pl.DataFrame]) -> None:
        self.name = name
        self._responses = responses
        self.calls: list[str] = []

    def fetch(self, ts_code: str, start: date, end: date) -> pl.DataFrame:
        self.calls.append(ts_code)
        return self._responses.get(ts_code, pl.DataFrame())


def test_run_cross_check_matched_diff_unmatched(tmp_path) -> None:
    base = make_frame()
    store = FakeStore({
        '600000.SH': base,
        '000001.SZ': base,
        '600519.SH': base,
    })
    adapter = FakeAdapter('baostock', {
        '600000.SH': base.clone(),
        '000001.SZ': make_frame(closes=[10.0, 11.0, 12.0, 99.0]),
        '600519.SH': pl.DataFrame(),
    })
    report = run_cross_check(
        store=store,
        codes=['600000.SH', '000001.SZ', '600519.SH'],
        start=date(2024, 1, 2),
        end=date(2024, 1, 5),
        sources=['baostock'],
        adapters={'baostock': adapter},
        limiters={'baostock': RateLimiter(0, 0, 0, 0)},
        output_dir=str(tmp_path),
    )
    statuses = {result.ts_code: result.status for result in report.results}
    assert statuses == {
        '600000.SH': 'matched',
        '000001.SZ': 'diff',
        '600519.SH': 'unmatched',
    }
    unmatched = next(result for result in report.results
                     if result.ts_code == '600519.SH')
    assert '无法对拍' in unmatched.note
    assert report.report_files
    payload = json.loads(Path(
        report.report_files['json']).read_text(encoding='utf-8'))
    assert payload['scope']['sample_size'] == 3
    assert payload['summary']['checks'] == 3
    assert Path(report.report_files['fields_csv']).exists()
    assert Path(report.report_files['codes_csv']).exists()


def test_run_cross_check_dry_run_writes_nothing(tmp_path) -> None:
    store = FakeStore({'600000.SH': make_frame()})
    adapter = FakeAdapter('baostock', {'600000.SH': make_frame()})
    report = run_cross_check(
        store=store,
        codes=['600000.SH'],
        start=date(2024, 1, 2),
        end=date(2024, 1, 5),
        sources=['baostock'],
        adapters={'baostock': adapter},
        limiters={'baostock': RateLimiter(0, 0, 0, 0)},
        dry_run=True,
        output_dir=str(tmp_path),
    )
    assert report.results[0].status == 'matched'
    assert report.report_files == {}
    assert not list(tmp_path.glob('cross_source_*'))


def test_run_cross_check_stops_after_consecutive_empty() -> None:
    store = FakeStore({
        '600000.SH': make_frame(),
        '000001.SZ': make_frame(),
        '600519.SH': make_frame(),
        '000858.SZ': make_frame(),
    })
    adapter = FakeAdapter('baostock', {})
    report = run_cross_check(
        store=store,
        codes=['600000.SH', '000001.SZ', '600519.SH', '000858.SZ'],
        start=date(2024, 1, 2),
        end=date(2024, 1, 5),
        sources=['baostock'],
        adapters={'baostock': adapter},
        limiters={'baostock': RateLimiter(0, 0, 0, 0)},
        max_consecutive_empty=2,
        dry_run=True,
    )
    statuses = [result.status for result in report.results]
    assert statuses == ['unmatched', 'unmatched', 'skipped', 'skipped']
    assert len(adapter.calls) == 2


def test_sample_codes_keeps_core_and_is_deterministic() -> None:
    frames = {
        code: make_frame()
        for code in [
            '600000.SH', '600519.SH', '000001.SZ', '000858.SZ',
            '601318.SH', '600036.SH', '300750.SZ', '002594.SZ',
            '600010.SH', '600011.SH', '600012.SH', '600015.SH',
        ]
    }
    store = FakeStore(frames)
    first = sample_codes(store, count=5, seed=7)
    second = sample_codes(store, count=5, seed=7)
    assert first == second
    assert len(first) == 5
    assert '600519.SH' in first
    assert all(code.endswith(('.SH', '.SZ')) for code in first)


def test_build_adapters_rejects_unknown_source() -> None:
    with pytest.raises(ValueError):
        build_adapters(['unknown'], build_remote_config())


def test_build_remote_config_toggles_only_in_memory() -> None:
    config = build_remote_config()
    assert config.data.allow_remote_sources is True
    from src.utils.config import Config

    assert Config().data.allow_remote_sources is False


def test_default_date_range() -> None:
    start, end = default_date_range(3, end=date(2024, 1, 1))
    assert end == date(2024, 1, 1)
    assert start == date(2021, 1, 1)
    with pytest.raises(ValueError):
        run_cross_check(
            store=FakeStore({}), codes=[], start=date(2024, 2, 1),
            end=date(2024, 1, 1), sources=['baostock'], adapters={},
            limiters={}, dry_run=True,
        )
