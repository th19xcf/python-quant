#!/usr/bin/env python3

"""``tools/margin_daily.py`` 的分片刷新语义（DG-31）。

锁定的三条行为：
1. 分片缺失 → 抓取；
2. 分片已覆盖全部应有交易日 → 跳过（不发请求）；
3. 分片存在但缺应有交易日 → 重取刷新。

旧 ``_staging/state_inputs_20261110/collect.py`` 用 ``if target.exists(): continue``
做断点，第 3 条永远不成立——源端补发布一天，本地就永久滞后。本文件最后一条用例
用旧规则跑同一份 fixture，并对「刷新后应覆盖末个交易日」的同一断言取反，
证明该断言在旧行为下**会失败**（DG-31：断言必须能失败）。
"""
from datetime import date

import polars as pl
import pytest

import tools.margin_daily as M

D1 = date(2026, 11, 10)
D2 = date(2026, 11, 11)


def _frame(days, value=1.0e10):
    return pl.DataFrame({'date': list(days),
                         **{c: [value] * len(days) for c in M.VALUE_COLS}})


def test_missing_shard_is_fetched(tmp_path):
    calls = []

    def fetch(year):
        calls.append(year)
        return _frame([D1, D2])

    M.collect_sse(tmp_path, {2026: [D1, D2]}, fetch, sleep=0)

    assert calls == [2026]
    out = pl.read_parquet(tmp_path / 'margin_sse_2026.parquet')
    assert out['date'].max() == D2


def test_covered_shard_is_skipped(tmp_path):
    _frame([D1, D2]).write_parquet(tmp_path / 'margin_sse_2026.parquet')
    calls = []

    def fetch(year):
        calls.append(year)
        return _frame([D1, D2])

    status = M.collect_sse(tmp_path, {2026: [D1, D2]}, fetch, sleep=0)

    assert status == {2026: 'ok'}
    assert calls == [], '已覆盖应有交易日的分片不应发请求'


def test_stale_shard_is_refreshed(tmp_path):
    """旧行为下这条失败：分片存在即跳过，11-11 永远不会补进来。"""
    _frame([D1]).write_parquet(tmp_path / 'margin_sse_2026.parquet')
    calls = []

    def fetch(year):
        calls.append(year)
        return _frame([D1, D2])

    status = M.collect_sse(tmp_path, {2026: [D1, D2]}, fetch, sleep=0)

    assert status == {2026: 'gap'}
    assert calls == [2026]
    assert pl.read_parquet(tmp_path / 'margin_sse_2026.parquet')['date'].max() == D2


def test_shard_state_triplet(tmp_path):
    part = tmp_path / 'margin_sse_2026.parquet'
    assert M.shard_state(part, [D1, D2]) == 'missing'
    _frame([D1]).write_parquet(part)
    assert M.shard_state(part, [D1, D2]) == 'gap'
    _frame([D1, D2]).write_parquet(part)
    assert M.shard_state(part, [D1, D2]) == 'ok'


def test_old_exists_skip_rule_fails_refresh_assertion(tmp_path):
    """DG-31 对照组：旧规则下「刷新后覆盖末个交易日」必然失败。"""
    part = tmp_path / 'margin_sse_2026.parquet'
    _frame([D1]).write_parquet(part)
    fetched = []

    def old_collect(year):
        if part.exists():      # 旧 collect.py：断点即跳过
            return
        fetched.append(year)
        _frame([D1, D2]).write_parquet(part)

    old_collect(2026)
    assert fetched == [], '旧规则不会重取已存在的分片'
    with pytest.raises(AssertionError):
        assert pl.read_parquet(part)['date'].max() == D2


def test_refresh_current_forces_current_year_refetch(tmp_path):
    _frame([D1, D2]).write_parquet(tmp_path / 'margin_sse_2026.parquet')
    calls = []

    def fetch(year):
        calls.append(year)
        return _frame([D1, D2])

    M.collect_sse(tmp_path, {2026: [D1, D2]}, fetch, refresh_current=True, sleep=0)

    assert calls == [2026]


def test_sz_gap_days_only_returns_missing_and_not_done():
    d3 = date(2026, 11, 12)
    assert M.sz_gap_days([D1, D2, d3], [D1], done=[str(D2)]) == [d3]


def test_assemble_marks_sources_and_units(tmp_path):
    _frame([D1, D2]).write_parquet(tmp_path / 'margin_sse_2026.parquet')
    _frame([D1]).write_parquet(tmp_path / 'margin_szse_macro.parquet')
    rec = {'date': str(D2), 'unit': '100M_yuan',
           **dict.fromkeys(M.SZ_GAP_MAP, 1.0)}
    (tmp_path / 'szse_official_days.jsonl').write_text(
        __import__('json').dumps(rec) + '\n', encoding='utf-8')

    margin, meta = M.assemble(tmp_path)

    sz = margin.filter(pl.col('market') == 'sz')
    assert meta['sz_gap_rows'] == 1
    assert sz.filter(pl.col('date') == D2)['fin_buy'].item() == 1.0e8
    assert set(sz['source'].to_list()) == {
        'eastmoney:macro_china_market_margin_sz', 'szse:stock_margin_szse'}
    assert margin.height == 4


# ------------------------------------------------------------ 字段级回填
def _margin_rows():
    """sz 三行：D1 融券两列为空（应回填）、D2 已有非空值（不得改）、D3 原始行。"""
    return pl.DataFrame({
        'date': [D1, D2, date(2024, 8, 8)],
        'market': ['sz', 'sz', 'sz'],
        'source': ['eastmoney:macro_china_market_margin_sz'] * 3,
        'fin_buy': [1.0, 2.0, 0.0],
        'fin_balance': [1.0, 2.0, 0.0],
        'sec_sell_qty': [None, 7.0, 0.0],
        'sec_qty': [None, 9.0, 0.0],
        'sec_balance': [1.0, 2.0, 0.0],
        'total_balance': [1.0, 2.0, 0.0],
    })


def _official_jsonl(tmp_path):
    rows = [
        {'date': str(D1), 'sec_qty': 3.0, 'sec_sell_qty': 2.0,
         'fin_buy': 5.0, 'fin_balance': 6.0, 'sec_balance': 0.3, 'total_balance': 6.3},
        {'date': str(D2), 'sec_qty': 99.0, 'sec_sell_qty': 99.0,
         'fin_buy': 99.0, 'fin_balance': 99.0, 'sec_balance': 99.0, 'total_balance': 99.0},
        {'date': '2024-08-08', 'sec_qty': 4.0, 'sec_sell_qty': 8.0,
         'fin_buy': 11.0, 'fin_balance': 12.0, 'sec_balance': 0.5, 'total_balance': 12.5},
    ]
    p = tmp_path / 'szse_gap_days.jsonl'
    p.write_text('\n'.join(__import__('json').dumps(r) for r in rows) + '\n', encoding='utf-8')
    return p


def test_patches_fill_only_nulls_and_fix_defect_row(tmp_path):
    out, stats = M.apply_official_patches(_margin_rows(), _official_jsonl(tmp_path))

    d1 = out.filter(pl.col('date') == D1)
    assert d1['sec_qty'].item() == 3.0e8
    assert d1['sec_sell_qty'].item() == 2.0e8
    d2 = out.filter(pl.col('date') == D2)
    assert d2['sec_qty'].item() == 9.0, '已有非空值不得被官方值覆盖'
    defect = out.filter(pl.col('date') == M.DEFECT_ROW)
    assert defect['fin_buy'].item() == 11.0e8
    assert defect['total_balance'].item() == 12.5e8
    assert stats['changed_by_column']['fin_buy'] == 1


def test_patches_leave_uncovered_sz_row_null(tmp_path):
    """未命中官方记录的 sz 空值行必须保持为空（显式兜底，不是 coalesce 出 null 别值）。"""
    extra = date(2026, 11, 12)
    margin = pl.concat([_margin_rows(), pl.DataFrame({
        'date': [extra], 'market': ['sz'],
        'source': ['eastmoney:macro_china_market_margin_sz'],
        'fin_buy': [1.0], 'fin_balance': [1.0], 'sec_sell_qty': [None],
        'sec_qty': [None], 'sec_balance': [1.0], 'total_balance': [1.0]})])
    out, stats = M.apply_official_patches(margin, _official_jsonl(tmp_path))

    row = out.filter(pl.col('date') == extra)
    assert row['sec_qty'].item() is None
    assert row['sec_sell_qty'].item() is None
    assert stats['changed_by_column']['sec_qty'] == 1


def test_patches_missing_input_raises(tmp_path):
    with pytest.raises(FileNotFoundError, match='不可重建'):
        M.apply_official_patches(_margin_rows(), tmp_path / 'nope.jsonl')
