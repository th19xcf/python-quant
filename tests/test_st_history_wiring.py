"""ST 区间表人工裁定覆盖层的口径单测：只补裁定段、不改旧行、可重建。"""

from datetime import date, datetime

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
    {'ts_code': 'BBB.SZ', 'coverage_start': date(2014, 1, 1),
     'coverage_end': date(2020, 12, 31), 'source': 'merged', 'fetched_at': None},
]
REVIEWED = [
    # 旧表覆盖窗口之内、旧表判非 ST：裁定层必须补进来
    {'ts_code': 'AAA.SZ', 'start_date': date(2013, 1, 2), 'end_date': date(2013, 3, 1),
     'flag': '*st', 'source': st_history.SOURCE_REVIEWED, 'precision': 'exact',
     'fetched_at': datetime(2026, 11, 10, 12, 0), 'evidence': '公告A',
     'raw_fetched_at': datetime(2026, 11, 8, 17, 0), 'adjudication': '新表对-公告支持'},
    # 与上一段同标的同 flag 且紧邻：连续性合并后应为一段
    {'ts_code': 'AAA.SZ', 'start_date': date(2013, 3, 2), 'end_date': date(2013, 6, 1),
     'flag': '*st', 'source': st_history.SOURCE_REVIEWED,
     'precision': 'announcement', 'fetched_at': datetime(2026, 11, 10, 12, 0),
     'evidence': '公告B', 'raw_fetched_at': datetime(2026, 11, 8, 17, 0),
     'adjudication': '新表对-公告支持'},
    # 旧表完全没有的标的 + 未摘帽开区间
    {'ts_code': 'BBB.SZ', 'start_date': date(2015, 5, 4), 'end_date': None,
     'flag': 'st', 'source': st_history.SOURCE_REVIEWED, 'precision': 'inferred',
     'fetched_at': datetime(2026, 11, 10, 12, 0), 'evidence': None,
     'raw_fetched_at': datetime(2026, 11, 8, 17, 0), 'adjudication': '新表对-仅 tushare'},
]
PENDING = [{
    'ts_code': 'CCC.SH', 'sec_name': '测试股', 'start_date': date(2008, 4, 15),
    'end_date': date(2010, 9, 20), 'new_st_type': '*ST', 'flag': '*st',
    'reason': 'insufficient_evidence', 'source': 'tushare_namechange',
    'adjudication': '证据不足', 'created_at': datetime(2026, 11, 10, 12, 0),
}]


def _seed(root, *, with_reviewed=True):
    (root / 'meta' / 'st' / 'raw').mkdir(parents=True, exist_ok=True)
    pl.DataFrame(OLD_INTERVALS, schema=st_history._INTERVAL_SCHEMA).write_parquet(
        st_history.intervals_path(root))
    pl.DataFrame(OLD_COVERAGE, schema=st_history._COVERAGE_SCHEMA).write_parquet(
        st_history.coverage_path(root))
    if with_reviewed:
        pl.DataFrame(REVIEWED, schema=st_history._REVIEWED_SCHEMA).write_parquet(
            st_history.reviewed_conflicts_path(root))
        pl.DataFrame(PENDING).write_parquet(st_history.reviewed_pending_path(root))
    # 原始源至少要有一条记录，build_store 才会认为本地有可重建的输入
    pl.DataFrame([{'code': '000005.SZ', 'change_date': date(2015, 1, 5),
                   'name_prev': '正常', 'name_new': 'ST戊'}],
                 schema=st_history._RAW_SZ_SCHEMA).write_parquet(
        root / 'meta' / 'st' / 'raw' / 'sz_change_name.parquet')


def _st_segments(result, code):
    return [(seg['start_date'], seg['end_date'])
            for seg in result.get(code, []) if seg['is_st']]


def test_reviewed_overlay_appends_and_keeps_old_rows(tmp_path):
    root = tmp_path
    _seed(root)

    old = pl.read_parquet(st_history.intervals_path(root))
    merged = st_history.apply_reviewed_conflicts(old, root)

    assert merged.height == 3
    assert merged.columns == list(st_history._INTERVAL_OUTPUT_SCHEMA)
    # 旧行原样保留，新列填 null
    kept = merged.filter(pl.col('source') == 'szse_change_name').to_dicts()[0]
    assert kept['start_date'] == date(2012, 1, 4)
    assert kept['evidence'] is None and kept['raw_fetched_at'] is None
    # 相邻同 flag 的两段裁定合并为一段（取较弱 precision）
    reviewed = merged.filter(pl.col('source') == st_history.SOURCE_REVIEWED)
    aaa = reviewed.filter(pl.col('ts_code') == 'AAA.SZ').to_dicts()
    assert len(aaa) == 1
    assert (aaa[0]['start_date'], aaa[0]['end_date']) == (date(2013, 1, 2), date(2013, 6, 1))
    assert aaa[0]['precision'] == 'announcement'
    assert aaa[0]['evidence'] == '公告A ; 公告B'


def test_reviewed_segments_reach_backtest_within_coverage(tmp_path):
    root = tmp_path
    _seed(root)
    merged = st_history.apply_reviewed_conflicts(
        pl.read_parquet(st_history.intervals_path(root)), root)
    st_history._write_parquet_atomic(merged, st_history.intervals_path(root))

    result = st_history.intervals_for_backtest(
        root, ['AAA.SZ', 'BBB.SZ'], '2010-01-01', '2020-12-31')

    assert _st_segments(result, 'AAA.SZ') == [
        ('2012-01-04', '2012-02-01'), ('2013-01-02', '2013-06-01')]
    assert _st_segments(result, 'BBB.SZ') == [('2015-05-04', '2020-12-31')]


def test_build_store_extends_coverage_for_pre2005_reviewed_segment(tmp_path):
    """沪市裁定段早于 2005 年时，窗口下界取段起点（不是上市日）并标注来源。"""
    root = tmp_path
    _seed(root)
    pl.DataFrame([{'notice_id': 'N1', 'sec_code': '600001', 'sec_name': '演示',
                   'title': '关于实施退市风险警示的公告',
                   'publish_date': date(2004, 5, 10)}],
                 schema=st_history._RAW_NOTICE_SCHEMA).write_parquet(
        root / 'meta' / 'st' / 'raw' / 'cninfo_notices.parquet')
    pl.DataFrame([{'ts_code': '600001.SH', 'list_date': date(1998, 6, 1)}]).write_parquet(
        root / 'meta' / 'stock_basic.parquet')
    pl.DataFrame([{
        'ts_code': '600001.SH', 'start_date': date(2004, 5, 10),
        'end_date': date(2004, 12, 31), 'flag': 'st',
        'source': st_history.SOURCE_REVIEWED, 'precision': 'exact',
        'fetched_at': datetime(2026, 11, 10, 12, 0), 'evidence': '公告C',
        'raw_fetched_at': datetime(2026, 11, 8, 17, 0),
        'adjudication': '新表对-公告支持'}],
        schema=st_history._REVIEWED_SCHEMA).write_parquet(
        st_history.reviewed_conflicts_path(root))

    out = tmp_path / 'rebuild'
    summary = st_history.build_store(root, apply_snapshot=False, output_dir=out)
    cov = pl.read_parquet(st_history.coverage_path(out))

    sh_row = cov.filter(pl.col('ts_code') == '600001.SH').to_dicts()[0]
    # 窗口下界 = 段起点，段前日子仍为 unknown（不得变成「已知非 ST」）
    assert sh_row['coverage_start'] == date(2004, 5, 10)
    assert sh_row['source'] == st_history.COV_SOURCE_SEGMENT_LISTING
    # 深市行按行情/上市日建窗口，不做 2005 下延
    sz_row = cov.filter(pl.col('ts_code') == '000005.SZ').to_dicts()[0]
    assert sz_row['source'] == st_history.COV_SOURCE_MERGED
    assert summary['coverage_extended_rows'] == 1
    assert summary['coverage_extended_by_source'] == {
        st_history.COV_SOURCE_SEGMENT_LISTING: 1}
    assert summary['coverage_extension_skipped'] == []


def test_build_store_reproduces_reviewed_and_pending(tmp_path):
    root = tmp_path
    _seed(root)
    out = tmp_path / 'rebuild'
    st_history.build_store(root, apply_snapshot=False, output_dir=out)

    rebuilt = pl.read_parquet(st_history.intervals_path(out))
    reviewed = rebuilt.filter(pl.col('source') == st_history.SOURCE_REVIEWED)
    expected = st_history._merge_reviewed_contiguous(
        pl.DataFrame(REVIEWED, schema=st_history._REVIEWED_SCHEMA))
    assert reviewed.select(list(st_history._INTERVAL_OUTPUT_SCHEMA)).equals(
        expected.drop('adjudication').select(list(st_history._INTERVAL_OUTPUT_SCHEMA)))
    # 证据不足的段同样从固定输入复现
    pending = pl.read_parquet(st_history.pending_review_path(out))
    assert pending.height == 1
    assert pending['reason'].to_list() == ['insufficient_evidence']
