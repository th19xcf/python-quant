"""自由流通口径单测：``filled <= float`` 断言的真实性 + 补全规则。

运行：``pytest tests/test_free_float_fill.py -q``
"""

from __future__ import annotations

import hashlib
from datetime import date

import polars as pl
import pytest

from tools.derive_free_float_fill import (
    _snapshot_backup,
    check,
    clip_filled_to_float,
    derive,
    filled_gt_float_rows,
)
from tools.fill_free_float_shares import _check_plan, _stats, plan_fill

BASE_SCHEMA = {
    'ts_code': pl.Utf8,
    'effective_date': pl.Date,
    'total_share': pl.Int64,
    'float_share': pl.Int64,
    'free_float_share': pl.Int64,
    'change_reason': pl.Utf8,
    'source': pl.Utf8,
    'fetched_at': pl.Datetime('us'),
}

CALENDAR = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7), date(2026, 1, 8)]


def _table(rows: list[dict[str, object]]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=BASE_SCHEMA)


def _row(day: int, total: int, float_: int | None, free: int | None) -> dict[str, object]:
    return {
        'ts_code': '000001.SZ',
        'effective_date': date(2026, 1, day),
        'total_share': total,
        'float_share': float_,
        'free_float_share': free,
        'change_reason': 'test',
        'source': 'eastmoney:stock_zh_a_gbjg_em',
        'fetched_at': None,
    }


# ----------------------------------------------------------------------
# filled <= float 断言：必须能对真实违例行失败
# ----------------------------------------------------------------------
def test_filled_gt_float_raises() -> None:
    """``clip=False`` 的 ffill 继承会越界：前一条 free=900/float=1000，后一条 float 降到 500。"""
    frame = _table([_row(5, 1000, 1000, 900), _row(6, 1000, 500, None)])
    raw = derive(frame, CALENDAR, clip=False)
    assert filled_gt_float_rows(raw).height == 1
    with pytest.raises(AssertionError, match='free_float_share_filled > float_share'):
        check(raw, frame)


def test_derive_clips_by_default() -> None:
    """默认路径必须已截断：越界沿用值收敛到当期 float_share，断言额度 0 自然通过。"""
    frame = _table([_row(5, 1000, 1000, 900), _row(6, 1000, 500, None)])
    out = derive(frame, CALENDAR)
    assert out['free_float_share_filled'].to_list() == [900, 500]
    assert filled_gt_float_rows(out).height == 0
    check(out, frame)


def test_no_violation_passes() -> None:
    """free 与 float 同步下降时 ffill 不越界，断言不应误报。"""
    frame = _table([_row(5, 1000, 1000, 400), _row(6, 1000, 500, None)])
    out = derive(frame, CALENDAR)
    assert filled_gt_float_rows(out).height == 0
    check(out, frame)


# ----------------------------------------------------------------------
# 截断函数：只动派生列，未命中行原样保留
# ----------------------------------------------------------------------
def test_clip_filled_to_float_only_touches_derived_column() -> None:
    frame = _table([_row(5, 1000, 1000, 900), _row(6, 1000, 500, None)])
    raw = derive(frame, CALENDAR, clip=False)
    clipped, hits = clip_filled_to_float(raw)
    assert hits == 1
    assert clipped['free_float_share_filled'].to_list() == [900, 500]
    # 原列与三态都不动，截断后可通过断言
    assert clipped.select(list(BASE_SCHEMA)).equals(raw.select(list(BASE_SCHEMA)))
    assert clipped['free_float_source'].to_list() == ['raw', 'ffill_250d']
    check(clipped, frame)


def test_clip_is_noop_when_clean() -> None:
    """无越界时不改任何值、计数为 0（不得把正常行误改）。"""
    frame = _table([_row(5, 1000, 1000, 400), _row(6, 1000, 500, None)])
    out = derive(frame, CALENDAR)
    clipped, hits = clip_filled_to_float(out)
    assert hits == 0
    assert clipped.equals(out)


def test_check_plan_rejects_broken_fallback() -> None:
    """未命中分支被写成别的值时必须失败（DG-31：断言不能对兜底失效空过）。

    构造：一条 keep、一条 fill、一条 unmatched（原值为空）。把 unmatched 行写成
    999 同时把 fill 行写回 null，使总空值数与 ``free_null_before - fill`` 仍然
    相等——这样前面的 keep 检查与空值守恒检查都会通过，只有兜底检查能拦住。
    """
    table = _table([_row(5, 1000, 1000, 600), _row(6, 1000, 1000, None),
                    _row(7, 1000, 1000, None)])
    em = _em([('000001.SZ', date(2026, 1, 6), 1000, 400)])
    planned = plan_fill(table, em)
    assert planned['action'].to_list() == ['keep', 'fill', 'unmatched']
    _check_plan(table, planned, _stats(planned))

    broken = planned.with_columns(
        pl.when(pl.col('action') == 'unmatched').then(pl.lit(999))
        .when(pl.col('action') == 'fill').then(pl.lit(None, dtype=pl.Int64))
        .otherwise(pl.col('free_after')).alias('free_after'))
    with pytest.raises(AssertionError, match='兜底失效'):
        _check_plan(table, broken, _stats(broken))


# ----------------------------------------------------------------------
# 补全规则：只填空值、free>0 且 <= 补全后的 float
# ----------------------------------------------------------------------
def _em(rows: list[tuple[str, date, int | None, int | None]]) -> pl.DataFrame:
    """东财口径帧；``_em_matched`` 与 ``load_em()`` 一致，用来区分「未配对」与「配对但该字段为空」。"""
    return pl.DataFrame(rows, schema={
        'ts_code': pl.Utf8, 'effective_date': pl.Date,
        'em_float': pl.Int64, 'em_free': pl.Int64}).with_columns(
        pl.lit(True).alias('_em_matched'))


def test_plan_fill_only_fills_nulls() -> None:
    table = _table([_row(5, 1000, 1000, 800)])
    planned = plan_fill(table, _em([('000001.SZ', date(2026, 1, 5), 1000, 700)]))
    assert planned['action'].to_list() == ['keep']
    assert planned['free_after'].to_list() == [800]


def test_plan_fill_rejects_over_float_and_nonpositive() -> None:
    table = _table([_row(5, 1000, 1000, None), _row(7, 1000, 1000, None),
                    _row(8, 1000, None, None)])
    em = _em([('000001.SZ', date(2026, 1, 5), 1000, 1200),
              ('000001.SZ', date(2026, 1, 7), 1000, 0),
              ('000001.SZ', date(2026, 1, 8), None, 500)])
    planned = plan_fill(table, em)
    # 无流通股本可作上界校验时不得填充（保守口径，与原一次性脚本一致）
    assert planned['action'].to_list() == ['source_over_float', 'source_nonpositive', 'no_float']
    assert planned['free_after'].to_list() == [None, None, None]


def test_plan_fill_uses_float_filled_in_same_pass() -> None:
    """float_share 为空但可由东财补全时，free 判定必须用补全后的 float（修复漏填）。"""
    table = _table([_row(5, 1000, None, None)])
    planned = plan_fill(table, _em([('000001.SZ', date(2026, 1, 5), 1000, 500)]))
    assert planned['float_eff'].to_list() == [1000]
    assert planned['action'].to_list() == ['fill']
    assert planned['free_after'].to_list() == [500]


def test_plan_fill_unmatched_keeps_original() -> None:
    """源端无对应记录时必须写回原值，不得变 null（DG-31），且判定为 unmatched。"""
    table = _table([_row(5, 1000, 1000, 600), _row(6, 1000, 1000, None)])
    planned = plan_fill(table, _em([('000002.SZ', date(2026, 1, 6), 1000, 400)]))
    assert planned['action'].to_list() == ['keep', 'unmatched']
    assert planned['free_after'].to_list() == [600, None]


def test_plan_fill_no_source_vs_unmatched() -> None:
    """配对成功但 ``FREELIQCI_SHARES`` 为空记 no_source，未配对记 unmatched，不得混为一类。"""
    table = _table([_row(5, 1000, 1000, None), _row(6, 1000, 1000, None)])
    em = _em([('000001.SZ', date(2026, 1, 5), 1000, None),
              ('000002.SZ', date(2026, 1, 6), 1000, 400)])
    planned = plan_fill(table, em)
    assert planned['action'].to_list() == ['no_source', 'unmatched']
    assert planned['free_after'].to_list() == [None, None]


# ----------------------------------------------------------------------
# 改前快照：同一天多次运行各自留档，不覆盖、不误判失败
# ----------------------------------------------------------------------
def test_snapshot_backup_keeps_each_run_of_the_same_day(tmp_path) -> None:
    """同日两次写入不同内容 → 两份快照并存；第三次写相同内容 → 不新增文件。"""
    root = tmp_path / '_backup'
    src = tmp_path / 'stock_shares.parquet'

    src.write_bytes(b'v1')
    sha1 = hashlib.sha256(b'v1').hexdigest()
    first = _snapshot_backup(src, sha1, backup_root=root)
    assert first.name == 'stock_shares.parquet'
    assert first.read_bytes() == b'v1'

    src.write_bytes(b'v2')
    sha2 = hashlib.sha256(b'v2').hexdigest()
    second = _snapshot_backup(src, sha2, backup_root=root)
    assert second != first
    assert second.name == f'stock_shares_{sha2[:8]}.parquet'
    assert second.read_bytes() == b'v2'
    assert first.read_bytes() == b'v1'  # 旧快照未被覆盖

    third = _snapshot_backup(src, sha2, backup_root=root)
    assert third == second
    assert sorted(p.name for p in first.parent.iterdir()) == sorted(
        [first.name, second.name])


def test_snapshot_backup_reuses_identical_snapshot(tmp_path) -> None:
    """同名快照内容与改前一致时直接复用，不重复拷贝、不产生第二个文件。"""
    root = tmp_path / '_backup'
    src = tmp_path / 'stock_shares.parquet'
    src.write_bytes(b'v1')
    sha1 = hashlib.sha256(b'v1').hexdigest()
    first = _snapshot_backup(src, sha1, backup_root=root)
    again = _snapshot_backup(src, sha1, backup_root=root)
    assert again == first
    assert len(list(first.parent.iterdir())) == 1
