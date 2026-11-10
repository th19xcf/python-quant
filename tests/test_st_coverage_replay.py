"""覆盖窗口重放层：只命中裁定标的、可重建、同输入逐字节一致。"""

from datetime import date

import polars as pl

from src.data import st_history

LISTING = {'000005.SZ': date(2010, 1, 1), '000006.SZ': date(2014, 6, 1)}


def _seed(root, *, overlay=None):
    raw = root / 'meta' / 'st' / 'raw'
    raw.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([
        {'code': '000005.SZ', 'change_date': date(2015, 1, 5),
         'name_prev': '正常', 'name_new': 'ST戊'},
        {'code': '000006.SZ', 'change_date': date(2019, 3, 4),
         'name_prev': '正常', 'name_new': '*ST己'},
    ], schema=st_history._RAW_SZ_SCHEMA).write_parquet(raw / 'sz_change_name.parquet')
    pl.DataFrame([{'ts_code': code, 'list_date': day}
                  for code, day in LISTING.items()]).write_parquet(
        root / 'meta' / 'stock_basic.parquet')
    if overlay is not None:
        pl.DataFrame(overlay, schema=st_history._REVIEWED_COVERAGE_SCHEMA).write_parquet(
            st_history.reviewed_coverage_path(root))


def _overlay(**extra):
    rows = [{'ts_code': '000005.SZ', 'coverage_start': date(2015, 1, 5),
             'coverage_end': None,
             'source': st_history.COV_SOURCE_SEGMENT_LISTING,
             'evidence': '最早裁定 ST 段起点', 'decided_at': date(2026, 11, 10)}]
    rows.extend(extra.get('rows', []))
    return rows


def test_replay_pins_reviewed_window_and_leaves_others(tmp_path):
    _seed(tmp_path, overlay=_overlay())
    out = tmp_path / 'rebuild'
    summary = st_history.build_store(
        tmp_path, apply_snapshot=False, output_dir=out, as_of=date(2026, 11, 10))
    cov = pl.read_parquet(st_history.coverage_path(out))

    pinned = cov.filter(pl.col('ts_code') == '000005.SZ').to_dicts()[0]
    assert pinned['coverage_start'] == date(2015, 1, 5)
    assert pinned['source'] == st_history.COV_SOURCE_SEGMENT_LISTING
    # 未列入裁定表的标的保持常规口径：起点 = 上市日，来源 = merged
    other = cov.filter(pl.col('ts_code') == '000006.SZ').to_dicts()[0]
    assert other['coverage_start'] == LISTING['000006.SZ']
    assert other['source'] == st_history.COV_SOURCE_MERGED
    assert summary['coverage_replay_applied'] == 1
    assert summary['coverage_replay_appended'] == 0


def test_replay_can_be_switched_off(tmp_path):
    _seed(tmp_path, overlay=_overlay())
    out = tmp_path / 'rebuild'
    summary = st_history.build_store(
        tmp_path, apply_snapshot=False, output_dir=out,
        as_of=date(2026, 11, 10), apply_coverage_replay=False)
    cov = pl.read_parquet(st_history.coverage_path(out))

    row = cov.filter(pl.col('ts_code') == '000005.SZ').to_dicts()[0]
    assert row['coverage_start'] == LISTING['000005.SZ']
    assert row['source'] == st_history.COV_SOURCE_MERGED
    assert summary['coverage_replay_applied'] == 0


def test_replay_appends_code_missing_from_build(tmp_path):
    extra = [{'ts_code': '900999.SH', 'coverage_start': date(2026, 11, 9),
              'coverage_end': None, 'source': st_history.COV_SOURCE_SEGMENT_QUOTE,
              'evidence': '快照锚点', 'decided_at': date(2026, 11, 10)}]
    _seed(tmp_path, overlay=_overlay(rows=extra))
    out = tmp_path / 'rebuild'
    summary = st_history.build_store(
        tmp_path, apply_snapshot=False, output_dir=out, as_of=date(2026, 11, 10))
    cov = pl.read_parquet(st_history.coverage_path(out))

    row = cov.filter(pl.col('ts_code') == '900999.SH').to_dicts()[0]
    assert row['coverage_start'] == date(2026, 11, 9)
    assert row['coverage_end'] == date(2026, 11, 10)
    assert summary['coverage_replay_appended'] == 1


def test_same_inputs_produce_identical_bytes(tmp_path):
    _seed(tmp_path, overlay=_overlay())
    build = lambda out: st_history.build_store(  # noqa: E731
        tmp_path, apply_snapshot=False, output_dir=tmp_path / out,
        as_of=date(2026, 11, 10))
    build('a')
    build('b')

    a = (tmp_path / 'a' / 'meta' / 'st' / 'st_coverage.parquet').read_bytes()
    b = (tmp_path / 'b' / 'meta' / 'st' / 'st_coverage.parquet').read_bytes()
    assert a == b
    # 逐值相等（不是只有字节相等）
    assert pl.read_parquet(tmp_path / 'a' / 'meta' / 'st' / 'st_coverage.parquet').equals(
        pl.read_parquet(tmp_path / 'b' / 'meta' / 'st' / 'st_coverage.parquet'))
