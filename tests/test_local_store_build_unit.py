#!/usr/bin/env python3

"""``src/data/local_store/build.py`` 的分支级单元测试。

覆盖范围：
* 文件发现与分类（stock / index / fund / 非法代码 / 类型过滤）；
* 因子来源优先级（快照 → 前向填充 → 1.0 兜底；分红现算；二者皆无）；
* 派生列重算（含空帧）、增量落后判定的六个分支；
* 目标日分类（at_target / source_stale / local_behind / not_built /
  source_missing / unreadable / local_ahead_of_source / content_mismatch）；
* 危险覆盖校验：损坏快照按「无快照」处理、本地增量列回接与错位中止；
* 快照门禁、治理入口、换手率派生、异常台账收尾、全量前快照复制；
* ``build`` 主流程的成功 / 跳过 / 异常 / 收尾错误路径与 CLI 分发。

全部使用临时目录与合成 ``.day`` / parquet，不读取正式库。
"""

import struct
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

import src.data.local_store.build as build_module
from src.data.local_store.build import (
    _attach_local_only_columns,
    _classify_target,
    _derive_price_change,
    _derive_turnover_step,
    _factor_arrays,
    _factor_governance_step,
    _factor_mismatch_totals,
    _factor_snapshot_gate,
    _incremental_plan,
    _load_snapshot_columns,
    _merge_source_with_existing,
    _rebuild_target,
    _recent_dates,
    _refresh_anomaly_lists_step,
    _run_build_command,
    _snapshot_quotes_dir,
    _source_record_at,
    _target_is_stale,
    build,
    build_frame,
    discover_local_files,
    main,
    refresh_factors,
    verify_incremental_target,
)
from src.data.local_store.session_guard import filter_nontrading_rows
from src.data.local_store.store import LocalFactorStore, LocalQuoteStore

_DATES = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]


# ----------------------------------------------------------------------
# 夹具
# ----------------------------------------------------------------------
def _store_root(tmp_path: Path) -> Path:
    """两级深的 store 根，与正式布局 <repo>/data/local_store 一致。"""
    return tmp_path / 'data' / 'local_store'


def _write_day(root: Path, market: str, code: str, records) -> Path:
    """.day 布局：day/open/high/low/close/volume/amount（价格 ×100 存整数）。"""
    path = Path(root) / market / 'lday' / f'{market}{code}.day'
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'wb') as f:
        for day, open_, high, low, close, volume, amount in records:
            f.write(struct.pack(
                '<IIIIIfII',
                int(day), int(open_ * 100), int(high * 100), int(low * 100),
                int(close * 100), float(amount), int(volume * 100), 0,
            ))
    return path


def _records(count: int, start: date = _DATES[0], amount: float = 1000.0):
    """合成 count 条日线记录（价格逐日 +0.5，便于识别末行变化）。"""
    rows = []
    for i in range(count):
        day = start.replace(day=start.day + i)
        close = 10.0 + i * 0.5
        rows.append((day.strftime('%Y%m%d'), close, close + 0.5,
                     close - 0.5, close, 100.0 + i, amount))
    return rows


def _quote_frame(dates=_DATES, closes=None) -> pl.DataFrame:
    days = list(dates)
    values = list(closes) if closes is not None else [
        10.0 + i * 0.5 for i in range(len(days))]
    return pl.DataFrame({
        'date': days,
        'open': values,
        'high': [c + 0.5 for c in values],
        'low': [c - 0.5 for c in values],
        'close': values,
        'volume': [100.0] * len(days),
        'amount': [1000.0] * len(days),
        'qfq_factor': [1.0] * len(days),
        'hfq_factor': [1.0] * len(days),
    })


def _build_config(tdx: Path) -> SimpleNamespace:
    return SimpleNamespace(data=SimpleNamespace(tdx_data_path=str(tdx)))


def _prepare_local(tmp_path: Path, code: str = '600000.SH',
                   market: str = 'sh', records=None):
    """写源 .day，按正式口径构造本地行情并落盘，返回 (path, store, factor)。"""
    tdx = tmp_path / 'tdx'
    path = _write_day(tdx, market, code[:6], records or _records(3))
    root = _store_root(tmp_path)
    store = LocalQuoteStore(root)
    factor_store = LocalFactorStore(root)
    frame = build_frame('stock', code, path, factor_store)
    store.write_daily('stock', code, frame, full=True)
    store.flush_meta()
    return path, store, factor_store


# ----------------------------------------------------------------------
# 文件发现
# ----------------------------------------------------------------------
def test_discover_local_files_classifies_and_filters(tmp_path):
    """stock/index/fund 分流正确；非法代码与类型过滤都被跳过。"""
    tdx = tmp_path / 'tdx'
    _write_day(tdx, 'sh', '600000', _records(1))
    _write_day(tdx, 'sh', '000001', _records(1))
    _write_day(tdx, 'sh', '510300', _records(1))
    _write_day(tdx, 'sh', '900901', _records(1))   # B 股：既非指数/基金/个股
    (tdx / 'sh' / 'lday' / 'sh00001.day').write_bytes(b'\x00' * 32)  # 位数不足
    (tdx / 'sh' / 'lday' / 'shabc123.day').write_bytes(b'\x00' * 32)  # 非数字

    found = discover_local_files(tdx, 'all')
    kinds = {code: kind for kind, code, _ in found}
    assert kinds == {
        '600000.SH': 'stock',
        '000001.SH': 'index',
        '510300.SH': 'fund',
    }

    only_stock = discover_local_files(tdx, 'stock')
    assert [code for _, code, _ in only_stock] == ['600000.SH']


# ----------------------------------------------------------------------
# 因子来源
# ----------------------------------------------------------------------
def _write_factor_snapshot(root: Path, rows: list[tuple]) -> None:
    path = root / 'factors' / 'stock_adj_factor.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        rows, schema=['ts_code', 'trade_date', 'qfq_factor', 'hfq_factor'],
        orient='row',
    ).with_columns(pl.col('trade_date').cast(pl.Date)).write_parquet(path)


def _write_dividend_snapshot(root: Path, rows: list[dict]) -> None:
    path = root / 'factors' / 'stock_dividend.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).with_columns(
        pl.col('ex_date').cast(pl.Date)).write_parquet(path)


def test_factor_arrays_uses_snapshot_with_forward_fill(tmp_path):
    """快照命中时按日期左连接：缺失日向前填充，序列开头兜底 1.0。"""
    root = _store_root(tmp_path)
    factor_store = LocalFactorStore(root)
    _write_factor_snapshot(root, [('600000.SH', _DATES[1], 0.9, 1.1)])

    qfq, hfq = _factor_arrays(
        factor_store, '600000.SH', _DATES, [10.0, 10.5, 11.0])

    assert qfq.to_list() == [1.0, 0.9, 0.9]
    assert hfq.to_list() == [1.0, 1.1, 1.1]


def test_factor_arrays_derives_from_dividend_snapshot(tmp_path):
    """无因子快照时按本地分红现算：除息日之前 qfq<1，当日及之后为 1。"""
    root = _store_root(tmp_path)
    factor_store = LocalFactorStore(root)
    _write_dividend_snapshot(root, [{
        'ts_code': '600000.SH', 'ex_date': _DATES[1],
        'cash_div': 1.0, 'share_div': 0.0,
        'rights_issue_ratio': 0.0, 'rights_issue_price': 0.0,
    }])

    qfq, hfq = _factor_arrays(
        factor_store, '600000.SH', _DATES, [10.0, 10.0, 10.0])

    assert qfq[0] < 1.0
    assert qfq[-1] == pytest.approx(1.0)
    assert hfq[0] == pytest.approx(1.0)


def test_factor_arrays_ignores_dividends_without_ex_date(tmp_path):
    """分红快照全为「已公告未除权」（ex_date 空）时按无因子处理。"""
    root = _store_root(tmp_path)
    factor_store = LocalFactorStore(root)
    _write_dividend_snapshot(root, [{
        'ts_code': '600000.SH', 'ex_date': None,
        'cash_div': 1.0, 'share_div': 0.0,
        'rights_issue_ratio': 0.0, 'rights_issue_price': 0.0,
    }])

    assert _factor_arrays(factor_store, '600000.SH', _DATES, [10.0] * 3) == (
        None, None)


def test_factor_arrays_without_any_snapshot_returns_none(tmp_path):
    factor_store = LocalFactorStore(_store_root(tmp_path))
    assert _factor_arrays(factor_store, '600000.SH', _DATES, [10.0] * 3) == (
        None, None)


def test_build_frame_includes_snapshot_factors(tmp_path):
    """快照存在时 build_frame 输出的因子列与快照一致（非伪 1.0）。"""
    root = _store_root(tmp_path)
    factor_store = LocalFactorStore(root)
    _write_factor_snapshot(root, [('600000.SH', _DATES[1], 0.9, 1.1)])
    path = _write_day(tmp_path / 'tdx', 'sh', '600000', _records(3))

    frame = build_frame('stock', '600000.SH', path, factor_store)

    assert frame['qfq_factor'].to_list() == [1.0, 0.9, 0.9]
    assert frame['hfq_factor'].to_list() == [1.0, 1.1, 1.1]


def test_build_frame_empty_day_file_returns_empty(tmp_path):
    path = _write_day(tmp_path / 'tdx', 'sh', '600000', [])
    frame = build_frame(
        'stock', '600000.SH', path, LocalFactorStore(_store_root(tmp_path)))
    assert frame.is_empty()


# ----------------------------------------------------------------------
# 派生列与末行比对
# ----------------------------------------------------------------------
def test_derive_price_change_empty_frame_is_returned_as_is():
    empty = pl.DataFrame({'date': [], 'close': []})
    assert _derive_price_change(empty).is_empty()


def test_last_row_matches_rejects_different_date():
    local = {'date': _DATES[0], 'open': 1.0, 'high': 1.0, 'low': 1.0,
             'close': 1.0, 'volume': 1.0, 'amount': 1.0}
    source = dict(local, date=_DATES[1])
    assert build_module._last_row_matches(local, source) is False


def test_source_record_at_returns_none_when_file_missing(tmp_path):
    assert _source_record_at(tmp_path / 'missing.day', _DATES[0]) is None


def test_source_record_at_returns_none_when_day_absent(tmp_path):
    path = _write_day(tmp_path / 'tdx', 'sh', '600000', _records(2))
    assert _source_record_at(path, _DATES[2]) is None


def test_recent_dates_returns_empty_on_unreadable_file(tmp_path):
    assert _recent_dates(tmp_path / 'missing.day') == set()


# ----------------------------------------------------------------------
# 增量落后判定
# ----------------------------------------------------------------------
def test_target_is_stale_when_not_built(tmp_path):
    path = _write_day(tmp_path / 'tdx', 'sh', '600000', _records(2))
    store = LocalQuoteStore(_store_root(tmp_path))
    assert _target_is_stale(store, 'stock', '600000.SH', path) is True


def test_target_is_stale_false_when_source_unreadable(tmp_path):
    """源文件为空（无首末日期）时无法判定落后，保守返回 False。"""
    path, store, _ = _prepare_local(tmp_path)
    empty = path.parent / 'sh600000_empty.day'
    empty.write_bytes(b'')
    assert _target_is_stale(store, 'stock', '600000.SH', empty) is False


def test_target_is_stale_respects_max_date_truncation(tmp_path):
    """源末行被 max_date 截到本地之前时判「不落后」，避免截断本地历史。"""
    path, store, _ = _prepare_local(tmp_path, records=_records(3))
    path = _write_day(tmp_path / 'tdx', 'sh', '600000', _records(4))
    assert _target_is_stale(
        store, 'stock', '600000.SH', path, max_date=_DATES[1]) is False


def test_target_is_stale_false_when_source_row_unavailable(tmp_path):
    """同日内容比对拿不到源记录（>5 条更晚记录）时保守判不落后。"""
    path, store, _ = _prepare_local(tmp_path, records=_records(5))
    path = _write_day(tmp_path / 'tdx', 'sh', '600000', _records(10))
    assert _target_is_stale(
        store, 'stock', '600000.SH', path, max_date=date(2024, 1, 6)) is False


def test_target_is_stale_detects_changed_last_row(tmp_path):
    """同日但末行成交额被源端修订：必须判落后，避免静默停在旧值。"""
    path, store, _ = _prepare_local(tmp_path)
    _write_day(tmp_path / 'tdx', 'sh', '600000', _records(3, amount=9999.0))
    assert _target_is_stale(store, 'stock', '600000.SH', path) is True


def test_incremental_plan_counts_stale_rows(tmp_path):
    records = _records(3)
    path = _write_day(tmp_path / 'tdx', 'sh', '600000', records)
    store = LocalQuoteStore(_store_root(tmp_path))

    stale, rows = _incremental_plan([('stock', '600000.SH', path)], store)
    assert (stale, rows) == (1, path.stat().st_size // 32)

    _, store, _ = _prepare_local(tmp_path, records=records)
    assert _incremental_plan([('stock', '600000.SH', path)], store) == (0, 0)


# ----------------------------------------------------------------------
# 单标的重建
# ----------------------------------------------------------------------
def test_rebuild_target_returns_none_when_max_date_filters_all(tmp_path):
    path = _write_day(tmp_path / 'tdx', 'sh', '600000', _records(3))
    root = _store_root(tmp_path)
    store, factor_store = LocalQuoteStore(root), LocalFactorStore(root)

    assert _rebuild_target(
        store, factor_store, 'stock', '600000.SH', path,
        since=None, max_date=date(2023, 12, 1), force_intraday=False) is None


def test_rebuild_target_returns_none_when_since_excludes_new_frame(tmp_path):
    """无存量 + since 之后无记录：既不能写空帧，也不能标记为完整覆盖。"""
    path = _write_day(tmp_path / 'tdx', 'sh', '600000', _records(3))
    root = _store_root(tmp_path)
    store, factor_store = LocalQuoteStore(root), LocalFactorStore(root)

    assert _rebuild_target(
        store, factor_store, 'stock', '600000.SH', path,
        since=date(2025, 1, 1), max_date=None, force_intraday=False) is None
    assert store.coverage('stock', '600000.SH') is None


def test_merge_source_with_existing_since_without_existing_marks_incomplete(
        tmp_path):
    fresh = _quote_frame()
    store = LocalQuoteStore(_store_root(tmp_path))
    merged, full = _merge_source_with_existing(
        store, 'stock', '600000.SH', fresh, since=_DATES[1])
    assert merged.height == 2
    assert full is False


# ----------------------------------------------------------------------
# 非交易日过滤（落盘前）
# ----------------------------------------------------------------------
# 合成日历：2024-01-02/03/04 为交易日，01-06 周六、01-07 周日、
# 01-08 周一但不在日历内（模拟法定假日口径——周一也可能不是交易日）。
_CAL = frozenset({date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)})
_MIXED = [date(2024, 1, 2), date(2024, 1, 6), date(2024, 1, 3),
          date(2024, 1, 7), date(2024, 1, 4), date(2024, 1, 8)]


def test_filter_nontrading_rows_drops_only_dates_outside_calendar():
    """日历外的日期（含周一假日）被挡，合法行逐值不变，行数差等于被挡日期数。"""
    frame = _quote_frame(_MIXED)

    out, blocked = filter_nontrading_rows(frame, _CAL)

    assert out.height == 3
    assert blocked == (date(2024, 1, 6), date(2024, 1, 7), date(2024, 1, 8))
    assert frame.height - out.height == len(blocked)
    legal = frame.filter(pl.col('date').is_in(list(_CAL))).sort('date')
    assert out.sort('date').equals(legal)


def test_filter_nontrading_rows_keeps_all_rows_when_all_dates_legal():
    """全合法输入必须一行不少、逐值不变。"""
    frame = _quote_frame(_DATES)

    out, blocked = filter_nontrading_rows(frame, _CAL)

    assert blocked == ()
    assert out.height == frame.height == 3
    assert out.equals(frame)


def test_filter_nontrading_rows_passes_through_without_calendar():
    """日历不可用（None / 空集）时原样放行，不能把整帧静默清空。"""
    frame = _quote_frame(_DATES)

    for calendar in (None, frozenset()):
        out, blocked = filter_nontrading_rows(frame, calendar)
        assert blocked == ()
        assert out.equals(frame)


def _mixed_day_records():
    """两条真实交易日 + 两条周末占位行（价格=成交量前值、volume=0）。"""
    return [
        ('20240102', 10.0, 10.5, 9.5, 10.0, 100.0, 1000.0),
        ('20240103', 10.5, 11.0, 10.0, 10.5, 101.0, 1000.0),
        ('20240106', 10.5, 10.5, 10.5, 10.5, 0.0, 0.0),
        ('20240107', 10.5, 10.5, 10.5, 10.5, 0.0, 0.0),
    ]


def test_rebuild_target_blocks_nontrading_rows_before_write(tmp_path):
    """端到端：源 .day 含 2024-01-06/07 时，落盘帧里没有它们，其余行逐值不变。"""
    path = _write_day(tmp_path / 'tdx', 'sh', '600000', _mixed_day_records())
    root = _store_root(tmp_path)
    store, factor_store = LocalQuoteStore(root), LocalFactorStore(root)
    blocked: list[tuple[str, date]] = []

    written = _rebuild_target(
        store, factor_store, 'stock', '600000.SH', path,
        since=None, max_date=None, force_intraday=False,
        calendar_dates=_CAL, blocked=blocked)

    assert written == 2
    written_frame = store.read_file('stock', '600000.SH')
    assert written_frame is not None
    assert written_frame['date'].to_list() == [date(2024, 1, 2), date(2024, 1, 3)]
    # 其余行逐值不变：与「直接从源 .day 解析出的合法行」完全一致。
    expected = build_frame(
        'stock', '600000.SH', path, factor_store).filter(
            pl.col('date').is_in([date(2024, 1, 2), date(2024, 1, 3)]))
    assert written_frame.select(expected.columns).equals(expected)
    # 行数差等于被挡日期数，且被挡日期如实回传。
    assert len(_mixed_day_records()) - written == len(blocked) == 2
    assert blocked == [('600000.SH', date(2024, 1, 6)),
                       ('600000.SH', date(2024, 1, 7))]


def test_rebuild_target_without_calendar_writes_nontrading_rows(tmp_path):
    """对照组：同一份源不传日历（等于过滤失效）时必须写出 4 行。

    这条与上一条互为反例——若 ``_rebuild_target`` 里的日历过滤被误删或失效，
    ``test_rebuild_target_blocks_nontrading_rows_before_write`` 的
    ``written == 2`` 会变成 4 而失败，本条的 4 行则保持不变。
    """
    path = _write_day(tmp_path / 'tdx', 'sh', '600000', _mixed_day_records())
    root = _store_root(tmp_path)
    store, factor_store = LocalQuoteStore(root), LocalFactorStore(root)

    written = _rebuild_target(
        store, factor_store, 'stock', '600000.SH', path,
        since=None, max_date=None, force_intraday=False, calendar_dates=None)

    assert written == 4
    written_frame = store.read_file('stock', '600000.SH')
    assert written_frame is not None
    assert date(2024, 1, 6) in written_frame['date'].to_list()


# ----------------------------------------------------------------------
# 目标日分类
# ----------------------------------------------------------------------
def test_classify_target_source_missing(tmp_path):
    store = LocalQuoteStore(_store_root(tmp_path))
    status, info = _classify_target(
        store, 'stock', '600000.SH', tmp_path / 'nope.day', _DATES[-1])
    assert (status, info) == ('source_missing', {})


def test_classify_target_unreadable(tmp_path):
    path = tmp_path / 'tdx' / 'sh' / 'lday' / 'sh600000.day'
    path.parent.mkdir(parents=True)
    path.write_bytes(b'')
    store = LocalQuoteStore(_store_root(tmp_path))
    assert _classify_target(
        store, 'stock', '600000.SH', path, _DATES[-1])[0] == 'unreadable'


def test_classify_target_not_built(tmp_path):
    path = _write_day(tmp_path / 'tdx', 'sh', '600000', _records(3))
    store = LocalQuoteStore(_store_root(tmp_path))
    assert _classify_target(
        store, 'stock', '600000.SH', path, _DATES[-1])[0] == 'not_built'


def test_classify_target_at_target(tmp_path):
    path, store, _ = _prepare_local(tmp_path)
    status, info = _classify_target(
        store, 'stock', '600000.SH', path, _DATES[2])
    assert status == 'at_target'
    assert info['local_end'] == '2024-01-04'
    assert info['source_last'] == '2024-01-04'


def test_classify_target_local_ahead_of_source(tmp_path):
    """本地已有目标日而源文件回退：判 local_ahead，不当成本地漏建。"""
    path, store, _ = _prepare_local(tmp_path)
    _write_day(tmp_path / 'tdx', 'sh', '600000', _records(2))
    status, _ = _classify_target(store, 'stock', '600000.SH', path, _DATES[2])
    assert status == 'local_ahead_of_source'


def test_classify_target_content_mismatch(tmp_path):
    path, store, _ = _prepare_local(tmp_path)
    _write_day(tmp_path / 'tdx', 'sh', '600000', _records(3, amount=9999.0))
    status, _ = _classify_target(store, 'stock', '600000.SH', path, _DATES[2])
    assert status == 'content_mismatch'


def test_classify_target_local_behind(tmp_path):
    """源含目标日而本地只到前一天：才是真正的构建遗漏。"""
    records = _records(3)
    path = _write_day(tmp_path / 'tdx', 'sh', '600000', records)
    root = _store_root(tmp_path)
    store, factor_store = LocalQuoteStore(root), LocalFactorStore(root)
    frame = build_frame('stock', '600000.SH', path, factor_store)
    store.write_daily(
        'stock', '600000.SH', frame.filter(pl.col('date') <= _DATES[1]),
        full=True)
    store.flush_meta()

    status, info = _classify_target(
        store, 'stock', '600000.SH', path, _DATES[2])
    assert status == 'local_behind'
    assert info['local_end'] == '2024-01-03'


def test_verify_incremental_target_collects_ahead_and_content_samples(tmp_path):
    path, store, _ = _prepare_local(tmp_path)
    _write_day(tmp_path / 'tdx', 'sh', '600000', _records(3, amount=9999.0))
    cfg = _build_config(tmp_path / 'tdx')

    result = verify_incremental_target(
        cfg, store, target_date=_DATES[2], data_type='stock')

    assert result['content_mismatch'] == 1
    assert result['content_mismatch_samples'] == ['600000.SH(stock)']


def test_verify_incremental_target_collects_ahead_samples(tmp_path):
    path, store, _ = _prepare_local(tmp_path)
    _write_day(tmp_path / 'tdx', 'sh', '600000', _records(2))
    cfg = _build_config(tmp_path / 'tdx')

    result = verify_incremental_target(
        cfg, store, target_date=_DATES[2], data_type='stock')

    assert result['local_ahead_of_source'] == 1
    assert result['local_ahead_samples'][0]['local_end'] == '2024-01-04'


# ----------------------------------------------------------------------
# 危险覆盖校验
# ----------------------------------------------------------------------
def test_load_snapshot_columns_treats_corrupt_parquet_as_missing(tmp_path):
    """缺陷 3 回归：损坏 parquet 抛 PolarsError，也必须按「无快照」返回 None。

    旧实现只捕 OSError/ValueError，ComputeError 会直接冒泡中断一次全量重建。
    """
    path = tmp_path / 'brokendown.parquet'
    path.write_bytes(b'this is not a parquet file')

    assert _load_snapshot_columns(path, ['ts_code']) is None


def test_load_snapshot_columns_missing_file_returns_none(tmp_path):
    assert _load_snapshot_columns(tmp_path / 'missing.parquet', ['a']) is None


def _dividend_rows() -> pl.DataFrame:
    return pl.DataFrame({
        'ts_code': ['600000.SH', '600000.SH'],
        'ex_date': [date(2020, 6, 1), date(2021, 6, 1)],
        'cash_div': [0.3, 0.4],
        'share_div': [0.0, 0.0],
        'report_date': [date(2019, 12, 31), date(2020, 12, 31)],
        'pay_date': [date(2020, 6, 10), date(2021, 6, 10)],
    })


def test_attach_local_only_columns_without_snapshot_returns_remote():
    remote = _dividend_rows()
    out, stats = _attach_local_only_columns(None, remote)
    assert out.equals(remote)
    assert stats == {'columns': [], 'matched_rows': 0, 'unmatched_rows': 0}


def test_attach_local_only_columns_without_extra_columns_returns_remote():
    remote = _dividend_rows()
    out, stats = _attach_local_only_columns(remote.clone(), remote)
    assert stats['columns'] == []
    assert out.columns == remote.columns


def test_attach_local_only_columns_restores_extra_values():
    """本地增量列按事件身份回接，且保持快照列顺序。"""
    snapshot = _dividend_rows().with_columns(
        pl.Series('source', ['local:a', 'local:b']))
    remote = _dividend_rows()

    out, stats = _attach_local_only_columns(snapshot, remote)

    assert stats['columns'] == ['source']
    assert stats['matched_rows'] == 2
    assert stats['unmatched_rows'] == 0
    assert out.columns == snapshot.columns
    assert out['source'].to_list() == ['local:a', 'local:b']


def test_attach_local_only_columns_aborts_when_remote_rows_fewer():
    """远端同日事件行数少于本地会让增量列错位，必须报错而不是静默搬值。"""
    snapshot = pl.concat([
        _dividend_rows(),
        pl.DataFrame({
            'ts_code': ['600000.SH'], 'ex_date': [date(2020, 6, 1)],
            'cash_div': [0.5], 'share_div': [0.0],
            'report_date': [date(2019, 12, 31)], 'pay_date': [date(2020, 6, 10)],
        }),
    ]).with_columns(pl.Series('source', ['a', 'b', 'c']))
    remote = _dividend_rows()

    with pytest.raises(RuntimeError, match='远端行数少于本地'):
        _attach_local_only_columns(snapshot, remote)


def test_refresh_factors_raises_when_database_unavailable(tmp_path, monkeypatch):
    class _DB:
        def __init__(self, config):
            pass

        def connect(self):
            return False

    import src.database.db_manager as db_module  # noqa: PLC0415

    monkeypatch.setattr(db_module, 'DatabaseManager', _DB)
    with pytest.raises(RuntimeError, match='数据库连接失败'):
        refresh_factors(
            SimpleNamespace(), LocalFactorStore(_store_root(tmp_path)),
            allow_remote=True, force_overwrite=True,
            confirm='OVERWRITE-LOCAL-SNAPSHOT')


def test_refresh_factors_dry_run_validates_without_writing(tmp_path, monkeypatch):
    """校验通过 + dry_run：只返回计划，不落任何快照文件。"""
    factors = pl.DataFrame({
        'ts_code': ['600000.SH'],
        'trade_date': [_DATES[0]],
        'qfq_factor': [1.0],
        'hfq_factor': [1.0],
    })
    dividends = _dividend_rows()

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _Engine:
        def connect(self):
            return _Conn()

    class _DB:
        def __init__(self, config):
            self.engine = _Engine()

        def connect(self):
            return True

        def disconnect(self):
            return None

    import src.database.db_manager as db_module  # noqa: PLC0415

    monkeypatch.setattr(db_module, 'DatabaseManager', _DB)
    monkeypatch.setattr(
        pl, 'read_database',
        lambda sql, conn: dividends if 'stock_dividend' in sql else factors)

    root = _store_root(tmp_path)
    planned = refresh_factors(
        SimpleNamespace(), LocalFactorStore(root),
        allow_remote=True, force_overwrite=True,
        confirm='OVERWRITE-LOCAL-SNAPSHOT', dry_run=True)

    assert planned['factors'] == 1
    assert planned['dividends'] == 2
    assert planned['validation']['ok'] is True
    assert not (root / 'factors' / 'stock_adj_factor.parquet').exists()


# ----------------------------------------------------------------------
# 快照门禁 / 治理 / 收尾
# ----------------------------------------------------------------------
def _touch_factor_snapshots(root: Path) -> None:
    factors = root / 'factors'
    factors.mkdir(parents=True, exist_ok=True)
    empty = pl.DataFrame(
        {'ts_code': [], 'trade_date': [], 'qfq_factor': [], 'hfq_factor': []},
        schema_overrides={'trade_date': pl.Date()})
    empty.write_parquet(factors / 'stock_adj_factor.parquet')
    empty.write_parquet(factors / 'fund_adj_factor.parquet')
    empty.drop('qfq_factor', 'hfq_factor').write_parquet(
        factors / 'stock_dividend.parquet')


def test_factor_snapshot_gate_warns_when_freshness_check_fails(
        tmp_path, monkeypatch):
    """三份快照都在但索引缺失：只告警、不阻断（返回 0）。"""
    root = _store_root(tmp_path)
    _touch_factor_snapshots(root)
    warnings: list[str] = []
    monkeypatch.setattr(build_module.logger, 'warning',
                        lambda message, *a, **k: warnings.append(str(message)))

    assert _factor_snapshot_gate(LocalFactorStore(root), root) == 0
    assert any('新鲜度检查失败' in w for w in warnings)


def test_factor_mismatch_totals_ignores_non_dict_entries():
    stats = {
        'stock': {'mismatch_codes': 2, 'mismatch_rows': 10},
        'index': {'mismatch_codes': 1, 'mismatch_rows': 3},
        'fund': {'mismatch_codes': None, 'mismatch_rows': None},
        'note': 'not-a-dict',
    }
    assert _factor_mismatch_totals(stats) == (3, 13)


def test_factor_governance_reports_verify_error(monkeypatch):
    monkeypatch.setattr(
        build_module, 'verify_quote_factor_consistency',
        lambda q, f: {'error': 'meta missing'})
    assert _factor_governance_step(None, None) == 1


def test_factor_governance_sync_dry_run_emits_json(monkeypatch, capsys):
    before = {'stock': {'mismatch_codes': 1, 'mismatch_rows': 2}}
    monkeypatch.setattr(
        build_module, 'verify_quote_factor_consistency', lambda q, f: before)
    calls: list[bool] = []

    def _fake_sync(quote_store, factor_store, *, codes=None, kind='stock',
                   dry_run=False):
        calls.append(dry_run)
        return {'codes': 1}

    monkeypatch.setattr(build_module, 'refresh_quote_factor_columns', _fake_sync)

    code = _factor_governance_step(
        None, None, sync=True, commit=False, json_output=True)

    assert code == 0
    assert calls == [True]
    assert '"committed": false' in capsys.readouterr().out


def test_factor_governance_commit_reverifies_and_gates(monkeypatch):
    results = [
        {'stock': {'mismatch_codes': 1, 'mismatch_rows': 2}},
        {'stock': {'mismatch_codes': 1, 'mismatch_rows': 1}},
    ]
    monkeypatch.setattr(
        build_module, 'verify_quote_factor_consistency',
        lambda q, f: results.pop(0))
    monkeypatch.setattr(
        build_module, 'refresh_quote_factor_columns',
        lambda q, f, **kwargs: {'codes': 1})

    assert _factor_governance_step(
        None, None, sync=True, commit=True, fail_on_mismatch=True) == 1


def test_factor_governance_commit_clean_after_sync(monkeypatch):
    results = [
        {'stock': {'mismatch_codes': 1, 'mismatch_rows': 2}},
        {'stock': {'mismatch_codes': 0, 'mismatch_rows': 0}},
    ]
    monkeypatch.setattr(
        build_module, 'verify_quote_factor_consistency',
        lambda q, f: results.pop(0))
    monkeypatch.setattr(
        build_module, 'refresh_quote_factor_columns',
        lambda q, f, **kwargs: {'codes': 0})

    assert _factor_governance_step(
        None, None, sync=True, commit=True, fail_on_mismatch=True) == 0


def test_factor_governance_sync_skipped_without_mismatch(monkeypatch):
    monkeypatch.setattr(
        build_module, 'verify_quote_factor_consistency',
        lambda q, f: {'stock': {'mismatch_codes': 0, 'mismatch_rows': 0}})
    monkeypatch.setattr(
        build_module, 'refresh_quote_factor_columns',
        lambda *a, **k: pytest.fail('无差异时不得同步'))

    assert _factor_governance_step(None, None, sync=True) == 0


def test_derive_turnover_step_reports_undeliverable_gap(monkeypatch):
    """有缺口但无可派生行（无股本）：只记录，不改写，返回统计。"""
    import src.data.local_store.turnover as turnover_module  # noqa: PLC0415

    payload = {'gap_rows': 5, 'derived_rows': 0, 'no_share_rows': 5,
               'written': False}
    monkeypatch.setattr(
        turnover_module, 'derive_turnover_local', lambda root, dry_run=False:
        payload)

    assert _derive_turnover_step(Path('root')) is payload


def test_derive_turnover_step_skips_when_requested(monkeypatch):
    import src.data.local_store.turnover as turnover_module  # noqa: PLC0415

    monkeypatch.setattr(
        turnover_module, 'derive_turnover_local',
        lambda *a, **k: pytest.fail('skip 时不得调用派生'))
    assert _derive_turnover_step(Path('root'), skip=True) is None


def test_refresh_anomaly_lists_step_handles_failure(tmp_path, monkeypatch):
    """清单刷新失败只告警；新鲜度巡检仍执行，退出码不受影响。"""
    import src.data.quality.anomaly_lists as anomaly_module  # noqa: PLC0415

    monkeypatch.setattr(
        anomaly_module, 'refresh_anomaly_lists',
        lambda root: {'status': 'failed', 'error': 'disk full'})
    checked: list[bool] = []
    monkeypatch.setattr(
        build_module, '_freshness_check_step',
        lambda root, dry_run=False: checked.append(dry_run))

    result = _refresh_anomaly_lists_step(tmp_path, skip=False)

    assert result['status'] == 'failed'
    assert checked == [False]


def test_refresh_anomaly_lists_step_dry_run_skips_refresh(tmp_path, monkeypatch):
    """dry-run 不刷新清单，但新鲜度巡检照跑（只读）。"""
    import src.data.quality.anomaly_lists as anomaly_module  # noqa: PLC0415

    monkeypatch.setattr(
        anomaly_module, 'refresh_anomaly_lists',
        lambda root: pytest.fail('dry-run 不得刷新台账'))
    checked: list[bool] = []
    monkeypatch.setattr(
        build_module, '_freshness_check_step',
        lambda root, dry_run=False: checked.append(dry_run))

    assert _refresh_anomaly_lists_step(
        tmp_path, skip=False, dry_run=True) is None
    assert checked == [True]


def test_refresh_anomaly_lists_step_respects_skip_flag(tmp_path, monkeypatch):
    import src.data.quality.anomaly_lists as anomaly_module  # noqa: PLC0415

    monkeypatch.setattr(
        anomaly_module, 'refresh_anomaly_lists',
        lambda root: pytest.fail('skip 时不得刷新'))
    monkeypatch.setattr(
        build_module, '_freshness_check_step', lambda root, dry_run=False: None)

    assert _refresh_anomaly_lists_step(tmp_path, skip=True) is None


def test_freshness_check_step_writes_outputs_only_when_not_dry_run(
        tmp_path, monkeypatch):
    import src.data.local_store.paths as paths_module  # noqa: PLC0415
    import src.data.quality.extended_contracts as contracts_module  # noqa: PLC0415
    import src.data.quality.freshness as freshness_module  # noqa: PLC0415

    results = [
        SimpleNamespace(key='ok_item', severity='ok', reasons=[]),
        SimpleNamespace(key='warn_item', severity='warn', reasons=['stale']),
    ]
    monkeypatch.setattr(freshness_module, 'check_all', lambda root: results)
    monkeypatch.setattr(
        freshness_module, 'summarize',
        lambda items: {'ok': 1, 'warn': 1, 'critical': 0})
    written: list[tuple] = []
    monkeypatch.setattr(
        freshness_module, 'write_outputs',
        lambda out_dir, items, today: written.append((out_dir, today)))
    monkeypatch.setattr(
        paths_module, 'freshness_artifacts_dir', lambda: tmp_path / 'artifacts')
    monkeypatch.setattr(
        contracts_module, 'check_contracts',
        lambda root: [{'key': 'suspension', 'status': 'missing'},
                      {'key': 'industry', 'status': 'ready'}])

    build_module._freshness_check_step(tmp_path, dry_run=False)
    assert len(written) == 1

    build_module._freshness_check_step(tmp_path, dry_run=True)
    assert len(written) == 1


# ----------------------------------------------------------------------
# 全量前快照
# ----------------------------------------------------------------------
def test_snapshot_quotes_dir_copies_and_never_overwrites(tmp_path):
    root = _store_root(tmp_path)
    quote_file = root / 'quotes' / 'stock' / 'SH' / '600000.SH.parquet'
    quote_file.parent.mkdir(parents=True)
    quote_file.write_bytes(b'x')
    store = LocalQuoteStore(root)

    first = _snapshot_quotes_dir(store, 'all')
    assert first is not None
    assert (first / 'stock' / 'SH' / '600000.SH.parquet').read_bytes() == b'x'

    second = _snapshot_quotes_dir(store, 'all')
    assert second is not None
    assert second.name.endswith('_1')

    by_type = _snapshot_quotes_dir(store, 'stock')
    assert by_type is not None
    assert (by_type / 'SH' / '600000.SH.parquet').exists()

    by_type_again = _snapshot_quotes_dir(store, 'stock')
    assert by_type_again is not None
    assert by_type_again.name == 'stock_2'


def test_snapshot_quotes_dir_returns_none_for_empty_or_missing(tmp_path):
    empty_root = _store_root(tmp_path)
    (empty_root / 'quotes').mkdir(parents=True)
    assert _snapshot_quotes_dir(LocalQuoteStore(empty_root), 'all') is None

    missing_root = tmp_path / 'other' / 'data' / 'local_store'
    assert _snapshot_quotes_dir(LocalQuoteStore(missing_root)) is None


# ----------------------------------------------------------------------
# build 主流程
# ----------------------------------------------------------------------
def test_build_raises_when_tdx_path_missing(tmp_path):
    root = _store_root(tmp_path)
    with pytest.raises(FileNotFoundError):
        build(_build_config(tmp_path / 'nope'),
              LocalQuoteStore(root), LocalFactorStore(root))


def test_build_returns_zero_without_targets(tmp_path):
    tdx = tmp_path / 'tdx'
    (tdx / 'sh' / 'lday').mkdir(parents=True)
    root = _store_root(tmp_path)

    result = build(_build_config(tdx),
                   LocalQuoteStore(root), LocalFactorStore(root))
    assert result == {'symbols': 0, 'rows': 0, 'elapsed_s': 0.0, 'files': 0}


def test_build_codes_filter_without_match_returns_zero(tmp_path):
    tdx = tmp_path / 'tdx'
    _write_day(tdx, 'sh', '600000', _records(2))
    root = _store_root(tmp_path)

    result = build(_build_config(tdx), LocalQuoteStore(root),
                   LocalFactorStore(root), codes=['999999.SH'])
    assert result['symbols'] == 0
    assert result['rows'] == 0


def test_build_dry_run_plans_rows_without_writing(tmp_path):
    tdx = tmp_path / 'tdx'
    path_a = _write_day(tdx, 'sh', '600000', _records(3))
    path_b = _write_day(tdx, 'sz', '000001', _records(2))
    root = _store_root(tmp_path)
    store = LocalQuoteStore(root)

    planned = build(_build_config(tdx), store, LocalFactorStore(root),
                    dry_run=True)
    expected = path_a.stat().st_size // 32 + path_b.stat().st_size // 32
    assert planned['symbols'] == 2
    assert planned['rows'] == expected
    assert planned['files'] == 0
    assert planned['dry_run'] is True
    assert store.coverage('stock', '600000.SH') is None

    incremental = build(_build_config(tdx), store, LocalFactorStore(root),
                        dry_run=True, incremental=True)
    assert incremental['symbols'] == 2
    assert incremental['rows'] == expected


def test_build_incremental_skips_uptodate_symbols(tmp_path, monkeypatch):
    tdx = tmp_path / 'tdx'
    _write_day(tdx, 'sh', '600000', _records(3))
    root = _store_root(tmp_path)
    store = LocalQuoteStore(root)
    factor_store = LocalFactorStore(root)
    cfg = _build_config(tdx)

    build(cfg, store, factor_store)
    monkeypatch.setattr(
        build_module, '_rebuild_target',
        lambda *a, **k: pytest.fail('已最新的标的不得重建'))
    result = build(cfg, store, factor_store, incremental=True)

    assert result['symbols'] == 0
    assert result['skipped_uptodate'] == 1


def test_build_counts_target_skipped_by_max_date(tmp_path):
    """max_date 把源记录全过滤掉时计入 skipped 而不写盘。"""
    tdx = tmp_path / 'tdx'
    _write_day(tdx, 'sh', '600000', _records(3))
    root = _store_root(tmp_path)
    store = LocalQuoteStore(root)

    result = build(_build_config(tdx), store, LocalFactorStore(root),
                   codes=['600000.SH'], max_date=date(2023, 12, 1))

    assert result['symbols'] == 0
    assert result['skipped'] == 1
    assert store.coverage('stock', '600000.SH') is None


def test_build_records_rebuild_failure_and_continues(tmp_path, monkeypatch):
    tdx = tmp_path / 'tdx'
    _write_day(tdx, 'sh', '600000', _records(2))
    root = _store_root(tmp_path)
    store = LocalQuoteStore(root)

    def _boom(*args, **kwargs):
        raise RuntimeError('源文件被占用')

    monkeypatch.setattr(build_module, '_rebuild_target', _boom)
    result = build(_build_config(tdx), store, LocalFactorStore(root),
                   codes=['600000.SH'])

    assert result['symbols'] == 0
    assert result['skipped'] == 1


def test_build_reports_state_write_error(tmp_path, monkeypatch):
    tdx = tmp_path / 'tdx'
    _write_day(tdx, 'sh', '600000', _records(2))
    root = _store_root(tmp_path)
    store = LocalQuoteStore(root)

    def _fail_state(payload):
        raise OSError('state dir readonly')

    monkeypatch.setattr(store, 'write_state', _fail_state)
    result = build(_build_config(tdx), store, LocalFactorStore(root),
                   codes=['600000.SH'])

    assert result['symbols'] == 1
    assert result['state_error'].startswith('OSError')


def test_build_calls_target_verification_when_requested(tmp_path, monkeypatch):
    tdx = tmp_path / 'tdx'
    _write_day(tdx, 'sh', '600000', _records(3))
    root = _store_root(tmp_path)
    store = LocalQuoteStore(root)
    calls: list[dict] = []
    monkeypatch.setattr(
        build_module, 'verify_incremental_target',
        lambda config, quote_store, *, target_date, data_type='all':
        calls.append({'target_date': target_date, 'data_type': data_type}))

    build(_build_config(tdx), store, LocalFactorStore(root),
          codes=['600000.SH'], target_date=_DATES[2])

    assert calls == [{'target_date': _DATES[2], 'data_type': 'all'}]


def test_build_takes_snapshot_before_full_rebuild(tmp_path, monkeypatch):
    tdx = tmp_path / 'tdx'
    _write_day(tdx, 'sh', '600000', _records(2))
    root = _store_root(tmp_path)
    store = LocalQuoteStore(root)
    store.write_daily('stock', '600000.SH', _quote_frame(_DATES[:2]), full=True)
    store.flush_meta()
    snapped: list[str] = []
    monkeypatch.setattr(
        build_module, '_snapshot_quotes_dir',
        lambda quote_store, data_type='all': snapped.append(data_type))

    build(_build_config(tdx), store, LocalFactorStore(root))

    assert snapped == ['all']


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------
def test_run_build_command_blocks_on_factor_gate(tmp_path, monkeypatch):
    args = build_module._parse_args(['build', '--codes', '600000.SH'])
    monkeypatch.setattr(build_module, '_factor_snapshot_gate', lambda *a, **k: 3)

    assert _run_build_command(
        args, SimpleNamespace(), None, None, _store_root(tmp_path)) == 3


def test_run_build_command_rejects_conflicting_modes(tmp_path, monkeypatch):
    args = build_module._parse_args(
        ['build', '--all', '--codes', '600000.SH'])
    monkeypatch.setattr(build_module, '_factor_snapshot_gate', lambda *a, **k: 0)

    assert _run_build_command(
        args, SimpleNamespace(), None, None, _store_root(tmp_path)) == 2


def test_run_build_command_defaults_to_incremental(tmp_path, monkeypatch):
    args = build_module._parse_args(['build'])
    recorded: dict = {}
    monkeypatch.setattr(build_module, '_factor_snapshot_gate', lambda *a, **k: 0)
    monkeypatch.setattr(
        build_module, 'build', lambda *a, **k: recorded.update(k) or {})
    monkeypatch.setattr(
        build_module, '_derive_turnover_step', lambda *a, **k: None)
    monkeypatch.setattr(
        build_module, '_refresh_anomaly_lists_step', lambda *a, **k: None)
    monkeypatch.setattr(
        build_module, '_factor_governance_step', lambda *a, **k: 0)

    code = _run_build_command(
        args, SimpleNamespace(), None, None, _store_root(tmp_path))

    assert code == 0
    assert recorded['incremental'] is True


def test_run_build_command_returns_nonzero_when_governance_fails(
        tmp_path, monkeypatch):
    args = build_module._parse_args(['build', '--codes', '600000.SH'])
    monkeypatch.setattr(build_module, '_factor_snapshot_gate', lambda *a, **k: 0)
    monkeypatch.setattr(build_module, 'build', lambda *a, **k: {})
    monkeypatch.setattr(
        build_module, '_derive_turnover_step', lambda *a, **k: None)
    monkeypatch.setattr(
        build_module, '_refresh_anomaly_lists_step', lambda *a, **k: None)
    monkeypatch.setattr(
        build_module, '_factor_governance_step', lambda *a, **k: 1)

    assert _run_build_command(
        args, SimpleNamespace(), None, None, _store_root(tmp_path)) == 1


def test_run_build_command_reports_meta_error(tmp_path, monkeypatch):
    args = build_module._parse_args(['build', '--codes', '600000.SH'])
    monkeypatch.setattr(build_module, '_factor_snapshot_gate', lambda *a, **k: 0)
    monkeypatch.setattr(
        build_module, 'build', lambda *a, **k: {'meta_error': 'disk full'})
    monkeypatch.setattr(
        build_module, '_derive_turnover_step', lambda *a, **k: None)
    monkeypatch.setattr(
        build_module, '_refresh_anomaly_lists_step', lambda *a, **k: None)
    monkeypatch.setattr(
        build_module, '_factor_governance_step', lambda *a, **k: 0)

    assert _run_build_command(
        args, SimpleNamespace(), None, None, _store_root(tmp_path)) == 1


@pytest.fixture
def cli_root(tmp_path, monkeypatch):
    """把 main 的 Config / store 根重定向到临时目录。"""
    root = _store_root(tmp_path)
    root.mkdir(parents=True)
    config = SimpleNamespace(
        data=SimpleNamespace(tdx_data_path='', local_store_path=str(root)))

    import src.utils.config as config_module  # noqa: PLC0415

    monkeypatch.setattr(config_module, 'Config', lambda: config)
    monkeypatch.setattr(build_module, 'resolve_store_root', lambda cfg: root)
    return root


def test_main_stats(cli_root):
    assert main(['stats']) == 0


def test_main_rescan_meta(cli_root):
    assert main(['rescan-meta']) == 0


def test_main_verify_quote_factors_json(cli_root, monkeypatch, capsys):
    monkeypatch.setattr(
        build_module, 'verify_quote_factor_consistency',
        lambda q, f: {'stock': {'mismatch_codes': 0}})

    assert main(['verify-quote-factors', '--json']) == 0
    assert '"mismatch_codes": 0' in capsys.readouterr().out


def test_main_sync_quote_factors(cli_root, monkeypatch):
    recorded: dict = {}

    def _fake(quote_store, factor_store, *, codes, kind, dry_run):
        recorded.update(codes=codes, kind=kind, dry_run=dry_run)
        return {'codes': 0}

    monkeypatch.setattr(build_module, 'refresh_quote_factor_columns', _fake)

    assert main([
        'sync-quote-factors', '--codes', '600000.SH,', '--kind', 'stock',
        '--dry-run']) == 0
    assert recorded == {'codes': ['600000.SH'], 'kind': 'stock', 'dry_run': True}


def test_main_factor_governance_delegates(cli_root, monkeypatch):
    monkeypatch.setattr(build_module, '_factor_governance_step', lambda *a, **k: 7)
    assert main(['factor-governance', '--json']) == 7


def test_main_refresh_factors_delegates(cli_root, monkeypatch):
    monkeypatch.setattr(build_module, 'refresh_factors', lambda *a, **k: {})
    monkeypatch.setattr(
        build_module, '_refresh_anomaly_lists_step', lambda *a, **k: None)

    assert main([
        'refresh-factors', '--allow-remote', '--force-overwrite',
        '--confirm', 'OVERWRITE-LOCAL-SNAPSHOT', '--dry-run']) == 0


def test_main_rebuild_factors_delegates(cli_root, monkeypatch):
    import src.data.local_store.factor_rebuild as rebuild_module  # noqa: PLC0415

    recorded: dict = {}
    monkeypatch.setattr(
        rebuild_module, 'rebuild_factors_from_local',
        lambda *a, **k: recorded.update(k) or {'rebuilt': 1})
    monkeypatch.setattr(
        build_module, '_refresh_anomaly_lists_step', lambda *a, **k: None)

    assert main(['rebuild-factors', '--codes', '600000.SH', '--dry-run']) == 0
    assert recorded['codes'] == ['600000.SH']
    assert recorded['resume'] is True


def test_main_sync_turnover_delegates(cli_root, monkeypatch):
    import src.data.local_store.turnover as turnover_module  # noqa: PLC0415

    recorded: dict = {}
    monkeypatch.setattr(
        turnover_module, 'backfill_turnover',
        lambda *a, **k: recorded.update(k) or {'rows': 0})

    assert main(['sync-turnover', '--codes', '600000.SH', '--dry-run']) == 0
    assert recorded['codes'] == ['600000.SH']
    assert recorded['dry_run'] is True


def test_main_derive_turnover_delegates(cli_root, monkeypatch):
    import src.data.local_store.turnover as turnover_module  # noqa: PLC0415

    recorded: dict = {}
    monkeypatch.setattr(
        turnover_module, 'derive_turnover_local',
        lambda *a, **k: recorded.update(k) or {'written': False})

    assert main([
        'derive-turnover', '--start', '2024-01-01', '--end', '2024-01-03',
        '--codes', '600000.SH', '--commit']) == 0
    assert recorded['start'] == date(2024, 1, 1)
    assert recorded['end'] == date(2024, 1, 3)
    assert recorded['dry_run'] is False


def test_main_dispatches_build_command(cli_root, monkeypatch):
    monkeypatch.setattr(build_module, '_run_build_command', lambda *a, **k: 9)
    assert main(['build', '--dry-run']) == 9


def test_module_is_importable_as_main_guard():
    """``python -m src.data.local_store`` 的入口函数存在且可调用。"""
    assert callable(sys.modules[build_module.__name__].main)
