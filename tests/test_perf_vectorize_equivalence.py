"""向量化改写与原逐行函数逐值等价（2026-11-24 性能优化回归）。"""
from datetime import date

import polars as pl

from src.data.financials import collector as c
from src.data.margin_quality import _exchange


def test_statutory_deadline_expr_matches_scalar() -> None:
    ends = [date(2023, 3, 31), date(2023, 6, 30), date(2023, 9, 30),
            date(2023, 12, 31), date(2024, 2, 29), None]
    got = pl.DataFrame({'e': ends}, schema={'e': pl.Date}).select(
        c._statutory_deadline_expr(pl.col('e')).alias('d'))['d'].to_list()
    want = [c.statutory_deadline(e) if e else None for e in ends]
    assert got[:-1] == want[:-1] and got[-1] is None


def test_ann_date_expr_matches_lookup() -> None:
    ann_map = {'000001': (date(2023, 4, 20), None, 1.0),
               '000002': (date(2023, 3, 1), None, None),     # 早于报告期 → 截止日
               '000003': (None, None, 2.0)}
    frame = pl.DataFrame({
        'symbol': ['000001', '000002', '000003', '999999', '000001'],
        'end_date': [date(2023, 3, 31)] * 4 + [None]},
        schema={'symbol': pl.Utf8, 'end_date': pl.Date})
    got = frame.with_columns(c._ann_date_expr(ann_map)).unnest('_ann_info')
    lookup = c._ann_lookup(ann_map)
    want = [lookup(r) for r in frame.to_dicts()]
    assert got['ann_date'].to_list() == [w['ann_date'] for w in want]
    assert got['ann_date_source'].to_list() == [w['ann_date_source'] for w in want]


def test_parent_expr_matches_lookup() -> None:
    ann_map = {'000001': (date(2023, 4, 20), None, 1.5), '000002': (None, None, None)}
    syms = ['000001', '000002', '999999', None]
    got = pl.DataFrame({'s': syms}, schema={'s': pl.Utf8}).select(
        c._parent_expr(pl.col('s'), ann_map).alias('v'))['v'].to_list()
    lookup = c._parent_lookup(ann_map)
    assert got == [lookup(s) for s in syms]
    empty = pl.DataFrame({'s': syms}, schema={'s': pl.Utf8}).select(
        c._parent_expr(pl.col('s'), {}).alias('v'))['v'].to_list()
    assert empty == [None] * 4


def test_exchange_vectorized_matches_scalar() -> None:
    codes = ['600000.SH', '000001.sz', '830799.BJ', '123', 'A.B.SH', '']
    got = pl.DataFrame({'ts_code': codes}).select(
        pl.col('ts_code').cast(pl.Utf8).str.to_uppercase()
        .str.split('.').list.last()
        .replace_strict({'SH': 'sh', 'SZ': 'sz', 'BJ': 'bj'}, default='other',
                        return_dtype=pl.Utf8))['ts_code'].to_list()
    assert got == [_exchange(x) for x in codes]
