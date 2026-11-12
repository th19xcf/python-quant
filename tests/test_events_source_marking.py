#!/usr/bin/env python3

"""公告事件来源标记（DG-19）：写入侧必须显式声明来源，覆盖声明按表内实测重算。"""

from __future__ import annotations

from datetime import datetime

import polars as pl
import pytest

from src.data.events import (
    import_file,
    load_coverage,
    load_events,
    normalize_announcements,
)

FROZEN_CNINFO_ROWS = [
    {'event_id': 'a4-600000', 'ts_code': '600000.SH', 'event_type': '减持', 'source': 'cninfo'},
    {'event_id': 'a1-600000', 'ts_code': '600000.SH', 'event_type': '并购重组', 'source': 'cninfo'},
    {'event_id': 'a3-600000', 'ts_code': '600000.SH', 'event_type': '监管处罚', 'source': 'cninfo'},
    {'event_id': 'a2-600000', 'ts_code': '600000.SH', 'event_type': '重大合同/中标', 'source': 'cninfo'},
]


def _raw_frame(source: str | None = None) -> pl.DataFrame:
    frame = pl.DataFrame({
        'keyword': ['并购', '中标', '处罚', '减持', '并购'],
        'stock_code': ['600000'] * 5,
        'art_code': ['a1', 'a2', 'a3', 'a4', 'a5'],
        'title': ['重大资产重组公告', '重大合同中标公告', '行政处罚决定书',
                  '股东减持公告', '重组进展公告'],
        'display_time': [
            datetime(2025, 1, 3, 16, 0),
            datetime(2025, 1, 3, 16, 5),
            datetime(2025, 1, 3, 16, 10),
            datetime(2025, 1, 3, 16, 15),
            datetime(2025, 1, 10, 16, 0),
        ],
    })
    return frame if source is None else frame.with_columns(pl.lit(source).alias('source'))


def _raw_csv(path, rows: list[dict]) -> None:
    pl.DataFrame(rows).write_csv(path)


def test_em_ann_source_is_not_relabelled_as_cninfo():
    """东财口径的记录必须保持 ``em_ann``：参数传入与记录自带两种形态都不得改标。"""
    by_param = normalize_announcements(_raw_frame(), source='em_ann')
    assert set(by_param['source'].to_list()) == {'em_ann'}

    # 记录自带来源时以记录为准，调用方参数不得覆盖
    by_record = normalize_announcements(_raw_frame(source='em_yjyg'), source='cninfo')
    assert set(by_record['source'].to_list()) == {'em_yjyg'}


def test_em_ann_survives_import_file(tmp_path):
    csv = tmp_path / 'raw.csv'
    _raw_csv(csv, [{
        'keyword': '股权激励', 'stock_code': '600000', 'art_code': 'x1',
        'title': '限制性股票激励计划', 'display_time': '2021-10-11 16:00:00',
    }])
    import_file(csv, tmp_path, mode='replace', source='em_ann')
    sources = set(load_events(tmp_path)['source'].to_list())
    assert sources == {'em_ann'}, f'来源被改标: {sources}'


def test_missing_source_raises_instead_of_defaulting_to_cninfo(tmp_path):
    with pytest.raises(ValueError, match='source'):
        normalize_announcements(_raw_frame())

    csv = tmp_path / 'raw.csv'
    _raw_csv(csv, [{
        'keyword': '股权激励', 'stock_code': '600000', 'art_code': 'x1',
        'title': '限制性股票激励计划', 'display_time': '2021-10-11 16:00:00',
    }])
    with pytest.raises(ValueError, match='source'):
        import_file(csv, tmp_path, mode='replace')


def test_write_coverage_recomputes_field_level_source(tmp_path):
    first = tmp_path / 'first.csv'
    _raw_csv(first, [
        {'keyword': '并购', 'stock_code': '600000', 'art_code': 'c1',
         'title': '重大资产重组公告', 'display_time': '2025-01-03 16:00:00'},
        {'keyword': '中标', 'stock_code': '600001', 'art_code': 'c2',
         'title': '重大合同中标公告', 'display_time': '2025-01-06 16:00:00'},
    ])
    second = tmp_path / 'second.csv'
    _raw_csv(second, [
        {'keyword': '股权激励', 'stock_code': '600002', 'art_code': 'e1',
         'title': '限制性股票激励计划', 'display_time': '2021-10-11 16:00:00'},
    ])
    import_file(first, tmp_path, mode='replace', source='cninfo')
    import_file(second, tmp_path, mode='append', source='em_ann')

    frame = load_events(tmp_path)
    counts = dict(frame.group_by('source').len().iter_rows())
    coverage = load_coverage(tmp_path)
    expected = '|'.join(f'{key}={counts[key]}' for key in ('cninfo', 'em_ann') if key in counts)
    assert coverage['source'] == expected, (coverage['source'], expected)
    assert coverage['source'] != 'em_ann'   # 不得残留「最后一次导入的值」


def test_pure_cninfo_path_unchanged():
    events = normalize_announcements(_raw_frame(), source='cninfo')
    rows = events.select('event_id', 'ts_code', 'event_type', 'source').to_dicts()
    assert rows == FROZEN_CNINFO_ROWS
