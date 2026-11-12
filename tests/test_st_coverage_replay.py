"""覆盖窗口重放层：只命中裁定标的、可重建、同输入逐字节一致。"""

import hashlib
import json
from datetime import date

import polars as pl
import pytest

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
    payload = json.loads(
        st_history.coverage_json_path(out).read_text(encoding='utf-8'))
    assert payload['coverage_replay_applied'] == 0
    assert payload['coverage_replay_enabled'] is False


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


def _sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_coverage_json_and_parquet_stamped_by_single_write_path(tmp_path):
    """coverage.json 与 st_coverage.parquet 必须同批产出、互相对得上。"""
    _seed(tmp_path, overlay=_overlay())
    out = tmp_path / 'rebuild'
    summary = st_history.build_store(
        tmp_path, apply_snapshot=False, output_dir=out, as_of=date(2026, 11, 10))

    parquet = st_history.coverage_path(out)
    payload = json.loads(
        st_history.coverage_json_path(out).read_text(encoding='utf-8'))
    assert payload['coverage_sha256'] == _sha256(parquet)
    assert payload['coverage_rows'] == pl.read_parquet(parquet).height
    assert payload['coverage_replay_applied'] == summary['coverage_replay_applied'] == 1
    assert payload['coverage_replay_applied'] is not None

    status = st_history.coverage_meta_status(out)
    assert status['ok'] is True
    assert status['sha_match'] is True and status['rows_match'] is True


def test_coverage_meta_detects_external_parquet_rewrite(tmp_path):
    """绕过 build_store 单独重写 parquet：sha 或行数不符时必须报不一致。"""
    _seed(tmp_path, overlay=_overlay())
    out = tmp_path / 'rebuild'
    st_history.build_store(tmp_path, apply_snapshot=False, output_dir=out,
                           as_of=date(2026, 11, 10))
    assert st_history.coverage_meta_status(out)['ok'] is True

    parquet = st_history.coverage_path(out)
    original = pl.read_parquet(parquet)
    # 行数不变、只改一个值：只有 SHA256 能发现
    tampered = original.with_columns(
        pl.when(pl.col('ts_code') == '000005.SZ')
        .then(pl.lit(date(2020, 1, 1)))
        .otherwise(pl.col('coverage_end'))
        .alias('coverage_end'))
    assert tampered.height == original.height
    assert tampered['coverage_end'].to_list() != original['coverage_end'].to_list()
    tampered.write_parquet(parquet)

    status = st_history.coverage_meta_status(out)
    assert status['sha_match'] is False
    assert status['rows_match'] is True
    assert status['ok'] is False

    # 行数被改也必须检出，不能只靠 sha
    pl.concat([original, original.head(1)]).write_parquet(parquet)
    status = st_history.coverage_meta_status(out)
    assert status['rows_match'] is False
    assert status['ok'] is False


def _cov(rows):
    return pl.DataFrame(
        [{'ts_code': code, 'coverage_start': start, 'coverage_end': end,
          'source': 'merged', 'fetched_at': None}
         for code, start, end in rows],
        schema=st_history._COVERAGE_SCHEMA)


def test_replay_records_null_counts_in_coverage_json(tmp_path):
    """写回前后各自 null 数、未命中行数、非空→空翻转数必须随 json 留档。"""
    _seed(tmp_path, overlay=_overlay())
    out = tmp_path / 'rebuild'
    st_history.build_store(tmp_path, apply_snapshot=False, output_dir=out,
                           as_of=date(2026, 11, 10))

    payload = json.loads(
        st_history.coverage_json_path(out).read_text(encoding='utf-8'))
    check = payload['coverage_replay_null_check']
    assert check['before'] == {'coverage_start': 0, 'coverage_end': 0}
    assert check['after'] == {'coverage_start': 0, 'coverage_end': 0}
    assert check['appended'] == {'coverage_start': 0, 'coverage_end': 0}
    assert check['non_null_to_null_rows'] == {'coverage_start': 0, 'coverage_end': 0}
    # 000006.SZ 未列入裁定表，必须被计入「未命中行」并逐值保持不变
    assert check['untouched_rows'] == 1


def test_null_guard_fails_when_unmatched_row_is_nulled():
    """反例自测：未命中行的 coverage_start 被写成 null → 断言必须失败（0 -> N）。"""
    before = _cov([('000005.SZ', date(2015, 1, 5), None),
                   ('000006.SZ', date(2014, 6, 1), None)])
    after = before.with_columns(
        pl.when(pl.col('ts_code') == '000006.SZ')
        .then(pl.lit(None, dtype=pl.Date))
        .otherwise(pl.col('coverage_start'))
        .alias('coverage_start'))

    with pytest.raises(AssertionError, match='空值'):
        st_history.assert_replay_preserves_nulls(
            before, after, {'000005.SZ'}, _cov([]))


def test_null_guard_fails_when_nulls_move_between_matched_rows():
    """反例自测：null 总数不变、只是换了一行也必须失败（不能只看计数）。"""
    before = _cov([('000005.SZ', date(2015, 1, 5), None),
                   ('000006.SZ', None, date(2020, 1, 1))])
    after = _cov([('000005.SZ', None, None),
                  ('000006.SZ', date(2014, 6, 1), date(2020, 1, 1))])

    with pytest.raises(AssertionError, match='非空写成 null'):
        st_history.assert_replay_preserves_nulls(
            before, after, {'000005.SZ', '000006.SZ'}, _cov([]))


def test_null_guard_fails_when_unmatched_value_changes():
    """反例自测：未命中行没变 null 但值被改写，同样必须失败。"""
    before = _cov([('000005.SZ', date(2015, 1, 5), None),
                   ('000006.SZ', date(2014, 6, 1), None)])
    after = _cov([('000005.SZ', date(2015, 1, 5), None),
                  ('000006.SZ', date(2014, 6, 2), None)])

    with pytest.raises(AssertionError, match='未命中标的'):
        st_history.assert_replay_preserves_nulls(
            before, after, {'000005.SZ'}, _cov([]))


def test_null_guard_fails_when_row_count_not_conserved():
    """反例自测：after 少了行（吞行）也必须失败。"""
    before = _cov([('000005.SZ', date(2015, 1, 5), None),
                   ('000006.SZ', date(2014, 6, 1), None)])
    after = before.head(1)

    with pytest.raises(AssertionError, match='行数不守恒'):
        st_history.assert_replay_preserves_nulls(
            before, after, {'000005.SZ'}, _cov([]))
