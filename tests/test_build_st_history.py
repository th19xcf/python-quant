"""tushare namechange → ST 区间推导的离线单测（不联网、不碰 data/）。"""

from datetime import date

import polars as pl
import pytest

from tools.build_st_history_from_tushare import (
    RAW_SCHEMA,
    apply_output,
    classify_name,
    derive_intervals,
)


def _raw(rows: list[tuple[str, str, str, str | None]]) -> pl.DataFrame:
    return pl.DataFrame(
        [{'ts_code': code, 'name': name, 'start_date': start,
          'end_date': end, 'change_reason': '其他'}
         for code, name, start, end in rows],
        schema=RAW_SCHEMA)


def test_merge_adjacent_same_type_boundary_day():
    """次日无缝接续（next_start == prev_end + 1）应合并为一段。"""
    frame = derive_intervals(_raw([
        ('000001.SZ', 'ST甲', '20000104', '20001229'),
        ('000001.SZ', 'ST甲', '20001230', '20010629'),
    ]))
    assert frame.height == 1
    row = frame.to_dicts()[0]
    assert row['start_date'] == date(2000, 1, 4)
    assert row['end_date'] == date(2001, 6, 29)
    assert row['st_type'] == 'ST'
    assert row['st_flag'] is True
    assert row['source'] == 'tushare_namechange'


def test_star_st_has_priority_over_st():
    """``*ST`` 与 ``ST`` 是不同风险等级，即使相邻也不得合并。"""
    assert classify_name('S*ST国农') == '*ST'
    assert classify_name('G*ST国农') == '*ST'
    assert classify_name('GST星源') == 'ST'
    assert classify_name('PT网点') == '其他风险警示'
    assert classify_name('*金亚') == '其他风险警示'
    assert classify_name('浦发银行') is None

    frame = derive_intervals(_raw([
        ('000002.SZ', '*ST乙', '20050104', '20050630'),
        ('000002.SZ', 'ST乙', '20050701', '20051230'),
        ('000002.SZ', 'ST乙', '20051231', '20060310'),
    ]))
    rows = frame.to_dicts()
    assert [row['st_type'] for row in rows] == ['*ST', 'ST']
    assert rows[0]['end_date'] == date(2005, 6, 30)
    # 同类型的后两段仍按相邻规则合并
    assert rows[1]['end_date'] == date(2006, 3, 10)


def test_open_interval_kept_when_not_lifted():
    """未摘帽的区间 end_date 保持为空，且合并后不被后续闭合段截断。"""
    frame = derive_intervals(_raw([
        ('000003.SZ', 'ST丙', '20100104', '20101231'),
        ('000003.SZ', 'ST丙', '20110101', None),
    ]))
    assert frame.height == 1
    assert frame.to_dicts()[0]['end_date'] is None

    only_open = derive_intervals(_raw([('000004.SZ', '*ST丁', '20190102', None)]))
    assert only_open.height == 1
    assert only_open.to_dicts()[0]['end_date'] is None


def test_duplicate_rows_collapse():
    """tushare 偶发完全重复行，去重后不得产生重复区间。"""
    frame = derive_intervals(_raw([
        ('000005.SZ', 'ST戊', '20150105', '20151231'),
        ('000005.SZ', 'ST戊', '20150105', '20151231'),
    ]))
    assert frame.height == 1


def test_empty_data_is_not_written(tmp_path):
    """空数据：推导出空表，且 apply_output 拒绝写盘、不留目标文件。"""
    empty = derive_intervals(_raw([]))
    assert empty.height == 0
    assert empty.schema == {
        'ts_code': pl.Utf8, 'st_flag': pl.Boolean, 'start_date': pl.Date,
        'end_date': pl.Date, 'st_type': pl.Utf8, 'source': pl.Utf8}

    target = tmp_path / 'st_history.parquet'
    with pytest.raises(RuntimeError, match='拒绝写盘'):
        apply_output(empty, target, tmp_path / 'backup')
    assert not target.exists()
