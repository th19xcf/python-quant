"""新表接入回测读取路径的口径单测：旧表优先、新表只补、开关可回退。"""

from datetime import date

import polars as pl

from src.data import st_history

OLD_INTERVALS = [
    {'ts_code': 'AAA.SZ', 'start_date': date(2012, 1, 4), 'end_date': date(2012, 2, 1),
     'flag': 'st', 'source': 'szse_change_name', 'precision': 'exact',
     'fetched_at': None},
]
OLD_COVERAGE = [
    {'ts_code': 'AAA.SZ', 'coverage_start': date(2010, 1, 1),
     'coverage_end': date(2020, 12, 31), 'source': 'merged', 'fetched_at': None},
]
NEW_INTERVALS = [
    # 旧表覆盖窗口之前：应被补进来
    {'ts_code': 'AAA.SZ', 'st_flag': True, 'start_date': date(2005, 3, 1),
     'end_date': date(2005, 9, 30), 'st_type': '*ST', 'source': 'tushare_namechange'},
    # 旧表覆盖窗口之内且旧表判非 ST：旧表优先，不得改判
    {'ts_code': 'AAA.SZ', 'st_flag': True, 'start_date': date(2013, 1, 2),
     'end_date': date(2013, 3, 1), 'st_type': 'ST', 'source': 'tushare_namechange'},
    # 旧表完全没有的标的 + 未摘帽开区间
    {'ts_code': 'BBB.SZ', 'st_flag': True, 'start_date': date(2015, 5, 4),
     'end_date': None, 'st_type': 'ST', 'source': 'tushare_namechange'},
]


def _seed(root):
    (root / 'meta' / 'st').mkdir(parents=True, exist_ok=True)
    pl.DataFrame(OLD_INTERVALS, schema=st_history._INTERVAL_SCHEMA).write_parquet(
        st_history.intervals_path(root))
    pl.DataFrame(OLD_COVERAGE, schema=st_history._COVERAGE_SCHEMA).write_parquet(
        st_history.coverage_path(root))
    pl.DataFrame(NEW_INTERVALS, schema=st_history._NEW_INTERVAL_SCHEMA).write_parquet(
        st_history.tushare_intervals_path(root))


def _st_segments(result, code):
    return [(seg['start_date'], seg['end_date'])
            for seg in result.get(code, []) if seg['is_st']]


def _covers(result, code, day):
    text = day.isoformat()
    for seg in result.get(code, []):
        if seg['start_date'] <= text <= seg['end_date']:
            return seg['is_st']
    return None


def test_switch_off_is_old_table_only(tmp_path, monkeypatch):
    """开关关闭：新表完全不参与，BBB（旧表无覆盖）不出现。"""
    monkeypatch.setenv(st_history.NEW_TABLE_ENV, '0')
    root = tmp_path
    _seed(root)

    result = st_history.intervals_for_backtest(
        root, ['AAA.SZ', 'BBB.SZ'], '2005-01-01', '2026-01-01')

    assert _st_segments(result, 'AAA.SZ') == [('2012-01-04', '2012-02-01')]
    assert 'BBB.SZ' not in result


def test_switch_on_supplements_only_uncovered_spans(tmp_path, monkeypatch):
    """开关开启：补旧表窗口外的 ST，窗口内仍以旧表为准，且不补非 ST。"""
    monkeypatch.delenv(st_history.NEW_TABLE_ENV, raising=False)
    root = tmp_path
    _seed(root)

    result = st_history.intervals_for_backtest(
        root, ['AAA.SZ', 'BBB.SZ'], '2005-01-01', '2026-01-01')

    assert _st_segments(result, 'AAA.SZ') == [
        ('2005-03-01', '2005-09-30'), ('2012-01-04', '2012-02-01')]
    # 旧表窗口内 2013-01-02 仍判非 ST（新表说法被旧表覆盖）
    assert _covers(result, 'AAA.SZ', date(2013, 1, 2)) is False

    # 旧表无覆盖的标的：只补 ST 区间，不补非 ST（未命中仍是 unknown）
    assert _st_segments(result, 'BBB.SZ') == [('2015-05-04', '2026-01-01')]
    assert all(seg['is_st'] for seg in result['BBB.SZ'])
    assert _covers(result, 'BBB.SZ', date(2011, 6, 1)) is None


def test_new_table_enabled_default_and_falsy(monkeypatch):
    monkeypatch.delenv(st_history.NEW_TABLE_ENV, raising=False)
    assert st_history.new_table_enabled() is True
    for value in ('0', 'false', 'OFF', 'no'):
        monkeypatch.setenv(st_history.NEW_TABLE_ENV, value)
        assert st_history.new_table_enabled() is False
