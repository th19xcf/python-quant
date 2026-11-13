"""G-134：``name_history`` 的深交所简称变更补充源。

约束（与 ``tools/build_name_history.py`` 的模块 docstring 一致）：
① 只补 tushare 名下 0 行的代码，已有代码一行不动；
② 首段回溯到 ``list_date``，末段收口到 ``delist_date``；
③ 上市首日的行情端标记（``N`` / ``C`` / ``XD`` / ``XR`` / ``DR``）不写成简称；
④ 事件链不自洽 / 缺 ``stock_basic`` / 缺 ``list_date`` 一律跳过，不猜。
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import polars as pl

from src.data.name_consistency import PRECISION_CARRIED, precision_of
from tools.build_name_history import (
    SCHEMA,
    carried_code_segments,
    sz_segments,
    sz_supplement,
)

BASIC = {'ts_code': pl.Utf8, 'list_date': pl.Date, 'delist_date': pl.Date}
CHANGE = {'code': pl.Utf8, 'change_date': pl.Date,
          'name_prev': pl.Utf8, 'name_new': pl.Utf8}


def _store(tmp_path: Path, basic: list[tuple], changes: list[tuple]) -> Path:
    meta = tmp_path / 'meta'
    (meta / 'st' / 'raw').mkdir(parents=True)
    pl.DataFrame(basic, schema=BASIC, orient='row').write_parquet(
        meta / 'stock_basic.parquet')
    pl.DataFrame(changes, schema=CHANGE, orient='row').write_parquet(
        meta / 'st' / 'raw' / 'sz_change_name.parquet')
    return tmp_path


def _existing(codes: list[str]) -> pl.DataFrame:
    return pl.DataFrame(
        [{'ts_code': code, 'name': '旧名', 'start_date': date(2000, 1, 1),
          'end_date': None, 'ann_date': None, 'change_reason': '',
          'source': 'tushare_namechange', 'fetched_at': datetime(2026, 11, 13)}
         for code in codes],
        schema=SCHEMA, strict=False)


def test_absent_code_gets_segments_closed_at_delist(tmp_path: Path) -> None:
    store = _store(
        tmp_path,
        [('000043.SZ', date(1994, 9, 28), date(2019, 12, 13))],
        [('000043.SZ', date(2006, 4, 11), '深南光Ａ', 'G 深南光'),
         ('000043.SZ', date(2006, 10, 9), 'G 深南光', '深南光Ａ'),
         ('000043.SZ', date(2007, 11, 30), '深南光Ａ', '中航地产'),
         ('000043.SZ', date(2018, 6, 1), '中航地产', '中航善达')])
    frame, stats = sz_supplement(store, _existing([]))

    assert stats['codes'] == 1 and stats['segments'] == 5
    assert frame.select('name', 'start_date', 'end_date').rows() == [
        ('深南光A', date(1994, 9, 28), date(2006, 4, 10)),
        ('G深南光', date(2006, 4, 11), date(2006, 10, 8)),
        ('深南光A', date(2006, 10, 9), date(2007, 11, 29)),
        ('中航地产', date(2007, 11, 30), date(2018, 5, 31)),
        ('中航善达', date(2018, 6, 1), date(2019, 12, 13)),
    ]
    assert set(frame['source'].unique()) == {'sz_change_name'}


def test_existing_code_is_never_touched(tmp_path: Path) -> None:
    store = _store(
        tmp_path,
        [('000001.SZ', date(1991, 4, 3), None)],
        [('000001.SZ', date(2006, 4, 11), '深发展Ａ', 'G 深发展')])
    frame, stats = sz_supplement(store, _existing(['000001.SZ']))

    assert frame.height == 0 and stats['codes'] == 0


def test_listing_day_marker_not_written_as_name(tmp_path: Path) -> None:
    store = _store(
        tmp_path,
        [('300114.SZ', date(2010, 8, 27), date(2025, 2, 14))],
        [('300114.SZ', date(2010, 8, 30), 'N中航电', '中航电测')])
    frame, _ = sz_supplement(store, _existing([]))

    assert frame.select('name', 'start_date', 'end_date').rows() == [
        ('中航电测', date(2010, 8, 27), date(2025, 2, 14))]


def test_inconsistent_chain_is_skipped(tmp_path: Path) -> None:
    store = _store(
        tmp_path,
        [('000009.SZ', date(1991, 1, 2), None)],
        [('000009.SZ', date(2006, 4, 11), 'A', 'B'),
         ('000009.SZ', date(2006, 10, 9), 'C', 'D')])
    frame, stats = sz_supplement(store, _existing([]))

    assert frame.height == 0 and stats['skipped_inconsistent_chain'] == 1


def test_missing_stock_basic_or_list_date_is_skipped(tmp_path: Path) -> None:
    store = _store(
        tmp_path,
        [('000010.SZ', None, None)],
        [('000010.SZ', date(2006, 4, 11), 'A', 'B'),
         ('000011.SZ', date(2006, 4, 11), 'A', 'B')])
    frame, stats = sz_supplement(store, _existing([]))

    assert frame.height == 0
    assert stats['skipped_no_list_date'] == 1
    assert stats['skipped_no_stock_basic'] == 1


def test_empty_code_is_skipped(tmp_path: Path) -> None:
    store = _store(
        tmp_path,
        [('000043.SZ', date(1994, 9, 28), None)],
        [('', date(2006, 4, 11), 'A', 'B')])
    frame, stats = sz_supplement(store, _existing([]))

    assert frame.height == 0 and stats['skipped_empty_code'] == 1


def test_same_day_change_does_not_create_zero_length_segment() -> None:
    segments, reason = sz_segments(
        [{'change_date': date(1994, 9, 28), 'name_prev': 'A', 'name_new': 'B'}],
        date(1994, 9, 28), None)

    assert reason is None
    assert segments == [('B', date(1994, 9, 28), None)]


def test_supplement_is_deterministic(tmp_path: Path) -> None:
    store = _store(
        tmp_path,
        [('000043.SZ', date(1994, 9, 28), date(2019, 12, 13))],
        [('000043.SZ', date(2006, 4, 11), '深南光Ａ', 'G 深南光'),
         ('000043.SZ', date(2006, 10, 9), 'G 深南光', '深南光Ａ')])
    first, _ = sz_supplement(store, _existing([]))
    second, _ = sz_supplement(store, _existing([]))

    assert first.select('ts_code', 'name', 'start_date', 'end_date').rows() == \
        second.select('ts_code', 'name', 'start_date', 'end_date').rows()


def _segment(code: str, name: str, start: date, end: date | None,
             source: str = 'tushare_namechange') -> dict:
    return {'ts_code': code, 'name': name, 'start_date': start, 'end_date': end,
            'ann_date': None, 'change_reason': '', 'source': source,
            'fetched_at': datetime(2026, 11, 13)}


def _pair_frame(old_extra: tuple[date, date] | None = None) -> pl.DataFrame:
    """G-146 fixture：新码 5 段，旧码 5 段（可选把空洞段替换成给定区间）。"""
    new_rows = [
        _segment('001914.SZ', '深南光A', date(1994, 9, 28), date(2006, 4, 10)),
        _segment('001914.SZ', 'G深南光', date(2006, 4, 11), date(2006, 10, 8)),
        _segment('001914.SZ', '中航地产', date(2007, 11, 30), date(2018, 5, 31)),
        _segment('001914.SZ', '中航善达', date(2018, 6, 1), date(2019, 12, 15)),
        _segment('001914.SZ', '招商积余', date(2019, 12, 16), None),
    ]
    gap = old_extra or (date(2006, 10, 9), date(2007, 11, 29))
    old_rows = [
        _segment('000043.SZ', '深南光A', date(1994, 9, 28), date(2006, 4, 10)),
        _segment('000043.SZ', 'G深南光', date(2006, 4, 11), date(2006, 10, 8)),
        _segment('000043.SZ', '深南光A', gap[0], gap[1]),
        _segment('000043.SZ', '中航地产', date(2007, 11, 30), date(2018, 5, 31)),
        _segment('000043.SZ', '中航善达', date(2018, 6, 1), date(2019, 12, 13)),
    ]
    return pl.DataFrame(new_rows + old_rows, schema=SCHEMA, strict=False)


def test_code_carry_fills_only_the_registered_gap() -> None:
    frame = _pair_frame()
    extra, stats = carried_code_segments(
        frame, {'001914.SZ': '000043.SZ'})

    assert stats['segments'] == 1
    # 旧码 3 段与新码既有段同日历区间（重复）；末段 2019-12-13 与新码 2019-12-15 错位
    assert stats['skipped_duplicate'] == 3
    assert stats['skipped_outside_gap'] == 1
    assert stats['added'] == [[
        '001914.SZ', '深南光A', '2006-10-09', '2007-11-29',
        'carried_from:000043.SZ']]
    assert extra.select('ts_code', 'name', 'start_date', 'end_date').rows() == [
        ('001914.SZ', '深南光A', date(2006, 10, 9), date(2007, 11, 29))]
    assert set(extra['source'].unique()) == {'carried_from:000043.SZ'}


def test_code_carry_never_overlaps_existing_segments() -> None:
    # 旧码段跨过空洞右边界（伸进「中航地产」），必须整段跳过而不是截断写入
    frame = _pair_frame((date(2006, 10, 9), date(2008, 1, 31)))
    extra, stats = carried_code_segments(frame, {'001914.SZ': '000043.SZ'})

    assert extra.height == 0 and stats['segments'] == 0
    assert stats['skipped_duplicate'] == 3
    assert stats['skipped_outside_gap'] == 2


def test_code_carry_skips_when_new_code_absent() -> None:
    frame = _pair_frame()
    extra, stats = carried_code_segments(frame, {'999999.SZ': '000043.SZ'})

    assert extra.height == 0 and stats['skipped_no_new_code'] == 1


def test_carried_source_has_registered_precision() -> None:
    assert precision_of('carried_from:000043.SZ') == PRECISION_CARRIED
    assert precision_of('unknown_source') == '未登记来源（需核实）'
