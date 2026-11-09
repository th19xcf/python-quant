"""官方未来交易日历生成器的口径、离线重放与幂等测试。"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl

from tools.build_trading_calendar import (
    build_frame,
    main,
    read_quote_authoritative_end,
    weekday_window,
    write_outputs,
)


def _write_index(root: Path, days: tuple[date, ...]) -> Path:
    path = root / 'quotes' / 'index' / 'sh' / '000001.SH.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'date': list(days)}, schema={'date': pl.Date}).write_parquet(path)
    return path


def test_weekday_window_excludes_holidays() -> None:
    days = weekday_window(date(2026, 11, 9), date(2026, 11, 13),
                          {date(2026, 11, 11)})
    assert days == (date(2026, 11, 9), date(2026, 11, 10),
                    date(2026, 11, 12), date(2026, 11, 13))


def test_read_quote_end_uses_latest_bar(tmp_path: Path) -> None:
    _write_index(tmp_path, (date(2026, 11, 5), date(2026, 11, 6)))
    assert read_quote_authoritative_end(tmp_path) == date(2026, 11, 6)


def test_read_quote_end_missing_returns_none(tmp_path: Path) -> None:
    assert read_quote_authoritative_end(tmp_path) is None


def test_build_frame_keeps_only_dates_after_quote_end() -> None:
    official = [date(2026, 11, 5), date(2026, 11, 6),
                date(2026, 11, 9), date(2026, 11, 10)]
    frame, coverage = build_frame(official, date(2026, 11, 6))

    assert frame.schema == {'trade_date': pl.Date}
    assert frame['trade_date'].to_list() == [date(2026, 11, 9), date(2026, 11, 10)]
    assert coverage['quote_authoritative_end'] == '2026-11-06'
    assert coverage['official_end'] == '2026-11-10'
    assert coverage['official_future_days'] == 2
    assert coverage['weekday_approx_days_same_window'] == 2
    assert coverage['weekday_only_dates'] == []
    assert coverage['official_only_dates'] == []


def test_build_frame_flags_official_gap_against_weekday_window() -> None:
    # 官方日历缺 10-13（周二），工作日近似会把它算成交易日
    frame, coverage = build_frame(
        [date(2026, 10, 12), date(2026, 10, 14)], date(2026, 10, 9)
    )
    assert frame['trade_date'].to_list() == [date(2026, 10, 12), date(2026, 10, 14)]
    assert coverage['official_future_days'] == 2
    assert coverage['weekday_approx_days_same_window'] == 3
    assert coverage['weekday_only_dates'] == ['2026-10-13']
    assert coverage['official_only_dates'] == []


def test_write_outputs_is_idempotent(tmp_path: Path) -> None:
    store = tmp_path / 'store'
    out = tmp_path / 'out'
    frame, coverage = build_frame(
        [date(2026, 11, 9), date(2026, 11, 10)], date(2026, 11, 6)
    )
    coverage['fetched_at'] = '2026-11-08T19:11:25'

    assert write_outputs(frame, coverage, out, store) is True
    assert write_outputs(frame, coverage, out, store) is False

    written = pl.read_parquet(out / 'official_future.parquet')
    assert written['trade_date'].to_list() == frame['trade_date'].to_list()
    payload = json.loads((out / 'coverage.json').read_text(encoding='utf-8'))
    assert payload == coverage


def test_main_offline_snapshot_and_check(tmp_path: Path) -> None:
    store = tmp_path / 'store'
    _write_index(store, (date(2026, 11, 6),))
    snapshot = tmp_path / 'trade_dates.csv'
    snapshot.write_text(
        'trade_date\n2026-11-06\n2026-11-09\n2026-11-10\n', encoding='utf-8'
    )
    out = tmp_path / 'out'
    common = [
        '--store-root', str(store),
        '--input', str(snapshot),
        '--output-dir', str(out),
        '--fetched-at', '2026-11-08T19:11:25',
    ]

    assert main([*common, '--apply']) == 0
    rows = pl.read_parquet(out / 'official_future.parquet')['trade_date'].to_list()
    assert rows == [date(2026, 11, 9), date(2026, 11, 10)]
    assert main([*common, '--check']) == 0


def test_check_detects_drift(tmp_path: Path) -> None:
    store = tmp_path / 'store'
    _write_index(store, (date(2026, 11, 6),))
    out = tmp_path / 'out'
    out.mkdir()
    pl.DataFrame(
        {'trade_date': [date(2026, 12, 1)]}, schema={'trade_date': pl.Date}
    ).write_parquet(out / 'official_future.parquet')
    snapshot = tmp_path / 'trade_dates.csv'
    snapshot.write_text('trade_date\n2026-11-09\n', encoding='utf-8')

    rc = main([
        '--store-root', str(store),
        '--input', str(snapshot),
        '--output-dir', str(out),
        '--check',
    ])
    assert rc == 1
