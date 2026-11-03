"""腾讯日线补数工具的单测（全部离线，用假客户端替代网络）。"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import polars as pl
import pytest

from src.data.local_store.store import LocalQuoteStore
from src.data.tencent.client import (
    normalize_daily_volume,
    parse_daily_rows,
)
from src.data.tencent.daily_fill import (
    _EVIDENCE_KLINE_COLUMNS,
    QuoteCoverage,
    build_new_rows,
    compare_with_tdx,
    fill_daily_gap,
    guard_sample,
    merge_incremental,
    read_meta_rows,
    scan_quote_files,
)


def _existing_frame() -> pl.DataFrame:
    """两根本地已有日线（原始价 + 独立因子列）。"""
    dates = [date(2026, 10, 16), date(2026, 10, 19)]
    closes = [9.9, 10.0]
    return pl.DataFrame({
        'date': dates,
        'ts_code': ['600000.SH', '600000.SH'],
        'open': [9.8, 9.95],
        'high': [9.95, 10.05],
        'low': [9.75, 9.9],
        'close': closes,
        'pre_close': [9.8, 9.9],
        'volume': [600000.0, 550000.0],
        'amount': [588000000.0, 550000000.0],
        'pct_chg': [1.02, 1.01],
        'change': [0.1, 0.1],
        'qfq_factor': [1.0, 1.0],
        'hfq_factor': [12.77, 12.77],
    }, schema_overrides={'date': pl.Date}).select(list(_EVIDENCE_KLINE_COLUMNS))


def _remote_gap_frame() -> pl.DataFrame:
    """缺口区间（2026-10-20 ~ 2026-11-02，共 10 个交易日）。"""
    days = [20, 21, 22, 23, 26, 27, 28, 29, 30]
    dates = [date(2026, 10, day) for day in days] + [date(2026, 11, 2)]
    closes = [10.2 + 0.02 * index for index in range(len(dates))]
    return pl.DataFrame({
        'date': dates,
        'open': closes,
        'high': [value + 0.05 for value in closes],
        'low': [value - 0.05 for value in closes],
        'close': closes,
        'volume': [500000.0] * len(dates),
        'amount': [500000000.0] * len(dates),
    }, schema_overrides={'date': pl.Date})


class FakeClient:
    """最小客户端替身：只实现 daily_fill 用到的两个方法。"""

    def __init__(self, frames: dict[str, pl.DataFrame] | None = None) -> None:
        self.frames = frames or {'600000.SH': _remote_gap_frame()}
        self.calls: list[tuple[str, int]] = []

    def fetch_daily(self, ts_code: str, count: int = 800) -> pl.DataFrame:
        self.calls.append((ts_code, count))
        return self.frames.get(ts_code, pl.DataFrame())

    def consecutive_failures(self) -> int:
        return 0


def _config() -> SimpleNamespace:
    return SimpleNamespace(data=SimpleNamespace(tdx_data_path=''))


# ----------------------------------------------------------------------
# 解析
# ----------------------------------------------------------------------
def test_parse_daily_rows_maps_columns_and_wan_amount() -> None:
    rows = [['2026-10-19', '9.15', '9.18', '9.25', '9.09',
             '805097.00', {}, '0.24', '73974.10', '0.00', '0.00']]
    frame = parse_daily_rows(rows)
    row = frame.row(0, named=True)
    assert row['date'] == date(2026, 10, 19)
    assert row['open'] == pytest.approx(9.15)
    assert row['close'] == pytest.approx(9.18)  # 第 3 列才是 close
    assert row['high'] == pytest.approx(9.25)
    assert row['low'] == pytest.approx(9.09)
    assert row['volume'] == pytest.approx(805097.0)
    # 接口 amount 单位「万元」，本模块统一为「元」
    assert row['amount'] == pytest.approx(739741000.0)


def test_parse_daily_rows_without_amount_keeps_null() -> None:
    """6 列老格式没有成交额时保持 null，绝不按 close*volume 反推。"""
    frame = parse_daily_rows([['2026-10-19', '9.15', '9.18', '9.25', '9.09', '805097.00']])
    assert frame.height == 1
    assert frame['amount'][0] is None


def test_parse_daily_rows_dedupes_and_sorts() -> None:
    rows = [
        ['2026-10-21', '10.0', '10.1', '10.2', '9.9', '1.0', {}, '0.1', '0.1', '0', '0'],
        ['2026-10-20', '9.9', '10.0', '10.1', '9.8', '2.0', {}, '0.1', '0.1', '0', '0'],
        ['2026-10-21', '10.0', '10.2', '10.3', '9.9', '3.0', {}, '0.1', '0.1', '0', '0'],
    ]
    frame = parse_daily_rows(rows)
    assert frame['date'].to_list() == [date(2026, 10, 20), date(2026, 10, 21)]
    assert frame['volume'].to_list()[-1] == 3.0  # 重复日期保留最后一条


def test_normalize_daily_volume_converts_star_shares_to_hands() -> None:
    """科创板成交量以「股」返回时按 100 股/手换算，其他板块保持「手」。"""
    star = pl.DataFrame({
        'date': [date(2026, 11, 2)],
        'open': [60.0], 'high': [65.6], 'low': [59.9], 'close': [65.54],
        'volume': [1470651.0], 'amount': [95533200.0],
    }, schema_overrides={'date': pl.Date})
    converted = normalize_daily_volume(star, '688190.SH')
    assert converted['volume'][0] == pytest.approx(14706.51)

    main_board = star.with_columns((pl.col('volume') / 100).alias('volume'))
    unchanged = normalize_daily_volume(main_board, '688190.SH')
    assert unchanged['volume'][0] == pytest.approx(14706.51)

    # 指数成交额口径不同，比值远小于 1，不能被误判为「股」
    index_frame = star.with_columns((pl.lit(1.9e12)).alias('amount'))
    kept = normalize_daily_volume(index_frame, '000001.SH')
    assert kept['volume'][0] == pytest.approx(1470651.0)


# ----------------------------------------------------------------------
# 增量构造 / 幂等
# ----------------------------------------------------------------------
def test_build_new_rows_chains_pre_close_and_keeps_factors() -> None:
    existing = _existing_frame()
    fresh = build_new_rows(_remote_gap_frame(), existing, '600000.SH')
    assert fresh.height == 10
    assert fresh.columns == list(_EVIDENCE_KLINE_COLUMNS)
    first = fresh.row(0, named=True)
    assert first['pre_close'] == pytest.approx(10.0)  # 接本地末根
    assert first['change'] == pytest.approx(0.2)
    assert first['pct_chg'] == pytest.approx(2.0)
    assert first['qfq_factor'] == pytest.approx(1.0)
    assert first['hfq_factor'] == pytest.approx(12.77)
    second = fresh.row(1, named=True)
    assert second['pre_close'] == pytest.approx(first['close'])


def test_merge_incremental_is_idempotent() -> None:
    existing = _existing_frame()
    fresh = build_new_rows(_remote_gap_frame(), existing, '600000.SH')
    merged = merge_incremental(existing, fresh)
    assert merged.height == 12
    assert merged['date'].is_sorted()
    again = merge_incremental(merged, fresh)
    assert again.height == 12
    assert again.equals(merged)


# ----------------------------------------------------------------------
# 端到端（离线）
# ----------------------------------------------------------------------
def test_fill_daily_gap_writes_rows_and_refreshes_meta(tmp_path) -> None:
    store = LocalQuoteStore(tmp_path, enabled=True)
    store.write_daily('stock', '600000.SH', _existing_frame(), full=True)
    store.flush_meta()
    assert scan_quote_files(tmp_path)  # 磁盘上有文件

    stats = fill_daily_gap(
        _config(), store, FakeClient(),
        latest_trade_date=date(2026, 11, 2), guard_sample_size=0)

    assert stats['filled_symbols'] == 1
    assert stats['rows_added'] == 10
    assert stats['by_type']['stock'] == {'symbols': 1, 'rows': 10}
    frame = store.read_file('stock', '600000.SH')
    assert frame is not None and frame.height == 12
    assert frame['date'].max() == date(2026, 11, 2)
    meta = read_meta_rows(store)
    assert ('stock', '600000.SH') in meta
    assert meta[('stock', '600000.SH')]['end'] == date(2026, 11, 2)
    assert stats['meta_entries'] >= 1


def test_fill_daily_gap_is_resumable_and_idempotent(tmp_path) -> None:
    store = LocalQuoteStore(tmp_path, enabled=True)
    store.write_daily('stock', '600000.SH', _existing_frame(), full=True)
    store.flush_meta()
    client = FakeClient()

    first = fill_daily_gap(_config(), store, client,
                           latest_trade_date=date(2026, 11, 2), guard_sample_size=0)
    second = fill_daily_gap(_config(), store, client,
                            latest_trade_date=date(2026, 11, 2), guard_sample_size=0)

    assert first['rows_added'] == 10
    assert second['rows_added'] == 0
    assert second['stale_symbols'] == 0
    assert store.read_file('stock', '600000.SH')['date'].max() == date(2026, 11, 2)


def test_fill_daily_gap_dry_run_does_not_write(tmp_path) -> None:
    store = LocalQuoteStore(tmp_path, enabled=True)
    store.write_daily('stock', '600000.SH', _existing_frame(), full=True)
    store.flush_meta()
    before = store.read_file('stock', '600000.SH')

    stats = fill_daily_gap(_config(), store, FakeClient(), dry_run=True,
                           latest_trade_date=date(2026, 11, 2))
    assert stats['dry_run'] is True
    assert stats['rows_added'] == 0
    assert stats['stale_symbols'] == 1
    after = store.read_file('stock', '600000.SH')
    assert before is not None and after is not None and before.equals(after)


def test_fill_daily_gap_skips_long_stale_symbols(tmp_path) -> None:
    """末根距目标交易日超过 max_gap_days 的标的（长期停更/退市）不回填历史。"""
    store = LocalQuoteStore(tmp_path, enabled=True)
    store.write_daily('stock', '600000.SH', _existing_frame(), full=True)
    store.flush_meta()
    stats = fill_daily_gap(_config(), store, FakeClient(), max_gap_days=5,
                           latest_trade_date=date(2026, 11, 2), guard_sample_size=0)
    assert stats['stale_symbols'] == 0
    assert stats['skipped_gap_symbols'] == 1
    assert stats['rows_added'] == 0
    assert store.read_file('stock', '600000.SH').height == 2


def test_fill_daily_gap_respects_since(tmp_path) -> None:
    store = LocalQuoteStore(tmp_path, enabled=True)
    store.write_daily('stock', '600000.SH', _existing_frame(), full=True)
    store.flush_meta()
    stats = fill_daily_gap(_config(), store, FakeClient(), since=date(2026, 10, 28),
                           latest_trade_date=date(2026, 11, 2), guard_sample_size=0)
    frame = store.read_file('stock', '600000.SH')
    assert stats['rows_added'] == 4  # 10-28 / 10-29 / 10-30 / 11-02
    assert frame is not None and frame['date'].min() == date(2026, 10, 16)
    assert frame.height == 6


# ----------------------------------------------------------------------
# 对拍 / 写前闸门
# ----------------------------------------------------------------------
def test_compare_with_tdx_flags_price_mismatch() -> None:
    tdx = pl.DataFrame({
        'date': [date(2026, 10, 19)],
        'open': [9.15], 'high': [9.25], 'low': [9.09], 'close': [9.18],
        'volume': [805097.0], 'amount': [739741000.0],
    }, schema_overrides={'date': pl.Date})
    remote_ok = tdx.with_columns((pl.col('close') + 0.01).alias('close'))
    ok = compare_with_tdx(remote_ok, tdx)
    assert ok['overlap_rows'] == 1
    assert ok['fields']['close']['diff_rows'] == 0

    remote_bad = tdx.with_columns((pl.col('close') + 0.5).alias('close'))
    bad = compare_with_tdx(remote_bad, tdx)
    assert bad['fields']['close']['diff_rows'] == 1
    assert bad['fields']['close']['max_abs'] == pytest.approx(0.5)


def test_guard_sample_rejects_inconsistent_source(tmp_path, monkeypatch) -> None:
    store = LocalQuoteStore(tmp_path, enabled=True)
    store.write_daily('stock', '600000.SH', _existing_frame(), full=True)
    cov = QuoteCoverage(
        ts_code='600000.SH', data_type='stock', rows=2,
        start=date(2026, 10, 16), end=date(2026, 10, 19), file_size=0)

    tdx = pl.DataFrame({
        'date': [date(2026, 10, 16), date(2026, 10, 19)],
        'open': [9.8, 9.95], 'high': [9.95, 10.05], 'low': [9.75, 9.9],
        'close': [9.9, 10.0], 'volume': [600000.0, 550000.0],
        'amount': [588000000.0, 550000000.0],
    }, schema_overrides={'date': pl.Date})

    import src.data.tencent.daily_fill as daily_fill  # noqa: PLC0415

    monkeypatch.setattr(daily_fill, '_tdx_frame', lambda _path, _code: tdx)
    bad_client = FakeClient({'600000.SH': tdx.with_columns(
        (pl.col('close') + 1.0).alias('close'))})

    guard = guard_sample(bad_client, [cov], None, sample_size=1)
    assert guard['ok'] is False
    assert guard['max_price_abs'] == pytest.approx(1.0)

    stats = fill_daily_gap(_config(), store, bad_client, latest_trade_date=date(2026, 11, 2),
                           guard_sample_size=1)
    assert stats['aborted'] == 'guard_failed'
    assert stats['rows_added'] == 0
    assert store.read_file('stock', '600000.SH').height == 2


def test_cli_refuses_network_without_allow_remote() -> None:
    from src.data.tencent.__main__ import main  # noqa: PLC0415

    assert main(['fill-daily']) == 2
    assert main(['verify-daily']) == 2
    assert main(['minute', '--codes', '600000.SH']) == 2
