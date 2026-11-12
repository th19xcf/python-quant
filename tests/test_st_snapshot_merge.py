"""快照合并：按日期升序逐日合并，状态只向后生效、不向前回推。"""

from datetime import date

import polars as pl

from src.data import st_history

D1 = date(2026, 11, 9)
D2 = date(2026, 11, 10)

LISTING = {'000010.SZ': date(2010, 1, 1), '000011.SZ': date(2010, 1, 1)}


def _seed(root, snapshots):
    """构造最小输入：深市名称变更（全程非 ST）+ 上市日 + 逐日状态快照。"""
    raw = root / 'meta' / 'st' / 'raw'
    raw.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([
        {'code': code, 'change_date': date(2020, 6, 1),
         'name_prev': '正常', 'name_new': f'正常{index}'}
        for index, code in enumerate(sorted(LISTING))
    ], schema=st_history._RAW_SZ_SCHEMA).write_parquet(raw / 'sz_change_name.parquet')
    pl.DataFrame([{'ts_code': code, 'list_date': day}
                  for code, day in LISTING.items()]).write_parquet(
        root / 'meta' / 'stock_basic.parquet')

    snap_dir = root / 'meta' / 'st_status'
    snap_dir.mkdir(parents=True, exist_ok=True)
    layers = [
        pl.DataFrame(rows, schema={'code': pl.Utf8, 'name': pl.Utf8,
                                   'status': pl.Utf8}).with_columns(
            pl.lit(day).alias('as_of'))
        for day, rows in snapshots.items()
    ]
    pl.concat(layers, how='vertical').sort(['as_of', 'code']).write_parquet(
        snap_dir / 'st_status_history.parquet')


def _build(root, out):
    return st_history.build_store(
        root, apply_snapshot=True, output_dir=out, as_of=D2)


def _rows(out, code):
    frame = pl.read_parquet(st_history.intervals_path(out))
    return frame.filter(pl.col('ts_code') == code).sort('start_date').to_dicts()


def test_ascending_merge_does_not_backfill_later_status(tmp_path):
    """D2 才出现的 ST 状态不得回推到 D1。"""
    _seed(tmp_path, {
        D1: [{'code': '000010.SZ', 'name': '正常甲', 'status': 'non_st'}],
        D2: [{'code': '000010.SZ', 'name': 'ST甲', 'status': 'st'}],
    })
    out = tmp_path / 'rebuild'
    summary = _build(tmp_path, out)

    rows = _rows(out, '000010.SZ')
    assert len(rows) == 1
    assert rows[0]['start_date'] == D2
    assert rows[0]['end_date'] is None
    assert rows[0]['source'] == st_history.SOURCE_SNAPSHOT
    # D1 时该标的仍是非 ST，区间起点不能早于 D2
    assert st_history.status_at(out, '000010.SZ', D1).status == \
        st_history.STATUS_NON_ST
    assert st_history.status_at(out, '000010.SZ', D2).status == st_history.STATUS_ST
    assert summary['snapshot_anchors'] == 1


def test_unknown_does_not_extend_interval(tmp_path):
    """D1 锚定的开区间遇到 D2 unknown 只停在 D1，不延展到 D2。"""
    _seed(tmp_path, {
        D1: [{'code': '000010.SZ', 'name': 'ST甲', 'status': 'st'}],
        D2: [{'code': '000010.SZ', 'name': '正常甲', 'status': 'unknown'}],
    })
    out = tmp_path / 'rebuild'
    summary = _build(tmp_path, out)

    rows = _rows(out, '000010.SZ')
    assert len(rows) == 1
    assert rows[0]['start_date'] == D1
    assert rows[0]['end_date'] == D1, 'unknown 当日不延展'
    assert st_history.status_at(out, '000010.SZ', D1).status == st_history.STATUS_ST
    assert st_history.status_at(out, '000010.SZ', D2).status != st_history.STATUS_ST
    assert summary['snapshot_closed_intervals'] == 1
    assert summary['snapshot_conflicts'] == 1
    conflicts = summary['conflicts']
    assert conflicts == [{'ts_code': '000010.SZ', 'snapshot': 'unknown',
                          'action': 'closed', 'interval_flag': 'st',
                          'interval_start': D1.isoformat()}]


def test_later_snapshot_wins_on_conflict(tmp_path):
    """连续两日状态冲突时以较晚日期为准：D1 st → D2 non_st，区间在 D1 收起。"""
    _seed(tmp_path, {
        D1: [{'code': '000011.SZ', 'name': 'ST乙', 'status': 'st'}],
        D2: [{'code': '000011.SZ', 'name': '正常乙', 'status': 'non_st'}],
    })
    out = tmp_path / 'rebuild'
    _build(tmp_path, out)

    rows = _rows(out, '000011.SZ')
    assert len(rows) == 1
    assert rows[0]['start_date'] == D1
    assert rows[0]['end_date'] == D1
    assert st_history.status_at(out, '000011.SZ', D1).status == st_history.STATUS_ST
    assert st_history.status_at(out, '000011.SZ', D2).status == \
        st_history.STATUS_NON_ST


def test_start_on_snapshot_day_conflict_drops_empty_interval(tmp_path):
    """官方源起算日与当日快照冲突：丢弃该行，不落出 start > end 的空区间。"""
    _seed(tmp_path, {
        D1: [{'code': '000011.SZ', 'name': '正常乙', 'status': 'non_st'}],
    })
    raw = tmp_path / 'meta' / 'st' / 'raw'
    pl.DataFrame([
        {'code': '000010.SZ', 'change_date': date(2021, 1, 4),
         'name_prev': '正常0', 'name_new': 'ST0'},
        {'code': '000011.SZ', 'change_date': date(2020, 6, 1),
         'name_prev': '正常', 'name_new': '正常1'},
        {'code': '000011.SZ', 'change_date': D1, 'name_prev': '正常1',
         'name_new': 'ST乙'},
    ], schema=st_history._RAW_SZ_SCHEMA).write_parquet(raw / 'sz_change_name.parquet')

    out = tmp_path / 'rebuild'
    summary = _build(tmp_path, out)

    frame = pl.read_parquet(st_history.intervals_path(out))
    assert frame.filter(
        pl.col('end_date').is_not_null()
        & (pl.col('start_date') > pl.col('end_date'))).height == 0
    assert _rows(out, '000011.SZ') == []
    assert st_history.status_at(out, '000011.SZ', D1).status == \
        st_history.STATUS_NON_ST
    assert summary['snapshot_dropped_intervals'] == 1
