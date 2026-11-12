#!/usr/bin/env python3

"""`verify_state_vector.check_turnover` 的缺数兜底（DG-31）。

背景：两融表缺当日行时，原实现 `margin.filter(...)['fin_buy'].item()` 对空
Series 抛 ``ValueError``，且 ``main`` 无兜底，导致其后五项检查从未执行。
本用例锁定三件事：
1. 缺当日行只记一条失败项，其余检查继续（不抛异常）；
2. 失败项写明缺哪天、读的哪张表、该表末行日期；
3. 旧写法在同样输入上确实会抛异常（对照组，证断言可失败）。
"""
from datetime import date

import polars as pl
import pytest

import tools.verify_state_vector as V

DAY_HAVE = date(2026, 11, 10)
DAY_MISS = date(2026, 11, 11)


def _make_store(tmp_path, margin_dates):
    """最小 store：1 只个股 + 1 条股本 + 指定的两融日期。"""
    store = tmp_path / 'local_store'
    shard = store / 'quotes' / 'stock' / 'sz' / '000001.SZ.parquet'
    shard.parent.mkdir(parents=True)
    frame = pl.DataFrame({
        'date': [DAY_HAVE, DAY_MISS],
        'ts_code': ['000001.SZ', '000001.SZ'],
        'close': [10.0, 11.0],
        'volume': [1000.0, 1200.0],
        'amount': [10_000.0, 13_200.0],
    })
    frame.write_parquet(shard)

    shares = store / 'meta' / 'factors' / 'stock_shares.parquet'
    shares.parent.mkdir(parents=True)
    pl.DataFrame({
        'ts_code': ['000001.SZ'],
        'effective_date': [date(2026, 1, 5)],
        'free_float_share_filled': [1_000.0],
        'free_float_source': ['raw'],
    }).write_parquet(shares)

    macro = store / 'meta' / 'macro' / 'cn_market_margin_daily.parquet'
    macro.parent.mkdir(parents=True)
    pl.DataFrame({
        'date': list(margin_dates),
        'market': ['sh'] * len(margin_dates),
        'fin_buy': [1.0e10] * len(margin_dates),
    }).write_parquet(macro)
    return store, frame, macro


def _run(tmp_path, margin_dates, panel_dates):
    store, frame, macro = _make_store(tmp_path, margin_dates)
    builder = V.load_builder()
    recomputed = builder.process_chunk(frame, builder.load_shares(store)).sort('date')
    margin_sum = dict.fromkeys(margin_dates, 1.0e10)
    rows = []
    for item in recomputed.iter_rows(named=True):
        fin_buy = margin_sum.get(item['date'])
        rows.append({
            'date': item['date'],
            'sent_ff_turnover': item['amount_ff'] / item['ff_mv'] * 100.0,
            'sent_n_codes': item['n_ff'],
            'lev_margin_buy_ratio': (
                None if fin_buy is None else fin_buy / item['amount_shsz'] * 100.0),
        })
    panel = pl.DataFrame(rows)
    report = V.Report()
    V.check_turnover(panel, panel_dates, store, builder, report)
    return report, macro


def test_missing_margin_day_records_failure_and_keeps_checking(tmp_path):
    report, macro = _run(tmp_path, [DAY_HAVE], [DAY_HAVE, DAY_MISS])

    assert len(report.failures) == 1, report.failures
    message = report.failures[0]
    assert str(DAY_MISS) in message
    assert str(macro) in message
    assert str(DAY_HAVE) in message, '失败项须写明该表末行日期'
    assert '其余检查继续' in message
    # 两个日期的成交额/只数校验都跑过，缺数日没有把它们一起吞掉
    assert report.checks >= 4
    assert not [m for m in report.failures if 'sent_ff_turnover' in m]


def test_all_days_missing_margin_still_iterates(tmp_path):
    earlier = date(2026, 11, 6)
    report, _ = _run(tmp_path, [earlier], [DAY_HAVE, DAY_MISS])

    assert len(report.failures) == 2, report.failures
    assert all('两融表无当日行' in m for m in report.failures)


def test_old_item_call_raises_on_missing_day(tmp_path):
    """对照组：改动前的写法在同样输入上抛 ValueError，而非记失败项。"""
    _, _, macro = _make_store(tmp_path, [DAY_HAVE])
    margin = pl.read_parquet(
        macro, columns=['date', 'fin_buy']).group_by('date').agg(pl.col('fin_buy').sum())

    with pytest.raises(ValueError, match='item'):
        margin.filter(pl.col('date') == DAY_MISS)['fin_buy'].item()
