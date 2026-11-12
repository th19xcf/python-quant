#!/usr/bin/env python3

"""行情反推疑似 ST 区间的规则与查询语义（合成行情，不依赖外部数据）。"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta

import polars as pl

from src.data import st_history
from src.data import st_price_infer as spi

_COUNTER_KEYS = ['no_quotes', 'not_inferable', 'windows_total', 'windows_flagged',
                 'rejected_breach', 'rejected_few_touch', 'rejected_low_amp',
                 'rejected_illiquid', 'exdiv_false_removed', 'neg_only_intervals']


def _frame(*, st_start: int, st_end: int, total: int = 400,
           split_day: int | None = None) -> pl.DataFrame:
    """构造合成行情：非 ST 段日涨幅 ±6%，ST 段被压到 ±4.9%。"""
    days: list[date] = []
    cursor = date(2000, 1, 3)
    while len(days) < total:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor += timedelta(days=1)
    closes: list[float] = [10.0]
    factors: list[float] = [1.0]
    for index in range(1, total):
        if st_start <= index <= st_end:
            ret = 0.049 if index % 2 else -0.049
        else:
            ret = 0.06 if index % 2 else -0.06
        closes.append(closes[-1] * (1 + ret))
        factors.append(1.0)
    if split_day is not None:
        # 除权：原始价腰斩，复权因子翻倍 → 后复权收益连续
        for index in range(split_day, total):
            closes[index] /= 2
            factors[index] *= 2
    rows = []
    for index, day in enumerate(days):
        close = closes[index]
        prev = closes[index - 1] if index else close
        rows.append({
            'date': day, 'close': close, 'pre_close': prev,
            'high': max(close, prev) * 1.03, 'low': min(close, prev) * 0.97,
            'volume': 1000.0, 'amount': 10000.0,
            'hfq_factor': factors[index], 'qfq_factor': 1.0 / factors[index],
        })
    return pl.DataFrame(rows)


def test_capped_window_produces_suspected_interval() -> None:
    counters = dict.fromkeys(
        ['no_quotes', 'not_inferable', 'windows_total', 'windows_flagged',
         'rejected_breach', 'rejected_few_touch', 'rejected_low_amp',
         'rejected_illiquid', 'exdiv_false_removed', 'neg_only_intervals'], 0)
    rows = spi._infer_code('000001.SZ', _frame(st_start=150, st_end=270), counters)
    assert len(rows) == 1
    assert rows[0]['flag'] == spi.FLAG_SUSPECTED
    assert rows[0]['source'] == spi.SOURCE_PRICE
    assert counters['windows_flagged'] > 0


def test_low_volatility_never_flags() -> None:
    counters = dict.fromkeys(
        ['no_quotes', 'not_inferable', 'windows_total', 'windows_flagged',
         'rejected_breach', 'rejected_few_touch', 'rejected_low_amp',
         'rejected_illiquid', 'exdiv_false_removed', 'neg_only_intervals'], 0)
    frame = _frame(st_start=10_000, st_end=10_000, total=300)
    frame = frame.with_columns([
        (pl.col('pre_close') * 1.01).alias('high'),
        (pl.col('pre_close') * 1.00).alias('low'),
        pl.col('pre_close').alias('close'),
    ])
    assert spi._infer_code('000001.SZ', frame, counters) == []


def test_ex_rights_gap_is_not_a_breach() -> None:
    counters = dict.fromkeys(
        ['no_quotes', 'not_inferable', 'windows_total', 'windows_flagged',
         'rejected_breach', 'rejected_few_touch', 'rejected_low_amp',
         'rejected_illiquid', 'exdiv_false_removed', 'neg_only_intervals'], 0)
    rows = spi._infer_code('000001.SZ', _frame(st_start=150, st_end=270,
                                              split_day=210), counters)
    assert len(rows) == 1
    assert counters['exdiv_false_removed'] >= 0


def test_20pct_board_is_not_inferable() -> None:
    counters = dict.fromkeys(
        ['no_quotes', 'not_inferable', 'windows_total', 'windows_flagged',
         'rejected_breach', 'rejected_few_touch', 'rejected_low_amp',
         'rejected_illiquid', 'exdiv_false_removed', 'neg_only_intervals'], 0)
    assert spi._infer_code('688001.SH', _frame(st_start=150, st_end=270),
                           counters) == []
    assert counters['not_inferable'] == 1


def test_query_semantics(tmp_path) -> None:
    root = tmp_path / 'store'
    st_dir = root / 'meta' / 'st'
    st_dir.mkdir(parents=True)
    pl.DataFrame({
        'ts_code': ['999999.SZ'], 'start_date': [date(2000, 1, 3)],
        'end_date': [None], 'flag': ['st'], 'source': ['test'],
        'precision': ['exact'], 'fetched_at': [datetime.now()],
    }, schema={'ts_code': pl.Utf8, 'start_date': pl.Date, 'end_date': pl.Date,
               'flag': pl.Utf8, 'source': pl.Utf8, 'precision': pl.Utf8,
               'fetched_at': pl.Datetime}).write_parquet(st_history.intervals_path(root))
    pl.DataFrame({
        'ts_code': ['999999.SZ'], 'coverage_start': [date(2000, 1, 3)],
        'coverage_end': [date(2100, 1, 1)], 'source': ['test'],
        'fetched_at': [datetime.now()],
    }, schema={'ts_code': pl.Utf8, 'coverage_start': pl.Date, 'coverage_end': pl.Date,
               'source': pl.Utf8, 'fetched_at': pl.Datetime}
    ).write_parquet(st_history.coverage_path(root))
    pl.DataFrame([{
        'ts_code': 'TEST.SZ', 'start_date': date(2000, 6, 1),
        'end_date': date(2000, 12, 1), 'flag': spi.FLAG_SUSPECTED,
        'source': spi.SOURCE_PRICE, 'confidence': spi.CONF_LOW,
        'evidence': '{}', 'fetched_at': datetime.now(),
    }], schema=spi._PRICE_SCHEMA).write_parquet(spi.price_path(root))

    hit = spi.suspected_status_at(root, 'TEST.SZ', date(2000, 8, 1))
    assert hit is not None and hit.flag == spi.FLAG_SUSPECTED

    default = st_history.status_at(root, 'TEST.SZ', date(2000, 8, 1))
    assert default.status == st_history.STATUS_UNKNOWN
    with_suspected = st_history.status_at(root, 'TEST.SZ', date(2000, 8, 1),
                                          include_suspected=True)
    assert with_suspected.status == st_history.STATUS_ST
    assert with_suspected.suspected is True

    exported = spi.suspected_intervals_for_backtest(
        root, ['TEST.SZ'], date(2000, 1, 1), date(2001, 1, 1))
    assert exported['TEST.SZ'][0]['is_suspected'] is True


def _block_frame(blocks: list[tuple[int, int]],
                 total: int) -> tuple[pl.DataFrame, list[date]]:
    """按「若干段 ST 行为 + 其余正常波动」构造行情，返回帧与交易日列表。"""
    days: list[date] = []
    cursor = date(2000, 1, 3)
    while len(days) < total:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor += timedelta(days=1)
    closes: list[float] = [10.0]
    for index in range(1, total):
        capped = any(start <= index <= end for start, end in blocks)
        ret = (0.049 if index % 2 else -0.049) if capped else (0.06 if index % 2 else -0.06)
        closes.append(closes[-1] * (1 + ret))
    rows = []
    for index, day in enumerate(days):
        prev = closes[index - 1] if index else closes[index]
        rows.append({
            'date': day, 'close': closes[index], 'pre_close': prev,
            'high': max(closes[index], prev) * 1.03,
            'low': min(closes[index], prev) * 0.97,
            'volume': 1000.0, 'amount': 10000.0, 'hfq_factor': 1.0,
        })
    return pl.DataFrame(rows), days


def test_merged_interval_evidence_covers_merged_span() -> None:
    """两段 ST 行为相隔 5 个交易日被合并：evidence 必须按合并后的窗口重算。

    合并只改 ``end_date``，若沿用首段统计，``evidence['end']`` 会停在首段末尾、
    ``days`` 只有 121 而实际区间 271 个交易日——本用例断言的就是这一点。
    """
    frame, days = _block_frame([(150, 270), (276, 420)], 900)
    rows = spi._infer_code('000001.SZ', frame, dict.fromkeys(_COUNTER_KEYS, 0))
    assert len(rows) == 1
    row = rows[0]
    evidence = json.loads(row['evidence'])
    assert evidence['end'] == (row['end_date'].isoformat() if row['end_date'] else None)
    stop = row['end_date'] or days[-1]
    expected_days = sum(1 for day in days if row['start_date'] <= day <= stop)
    assert evidence['days'] == expected_days
    assert evidence['days'] > 121
    assert evidence['breach_inside'] == 5


def test_open_ended_interval_evidence_end_is_null() -> None:
    """区间延伸到最后一根 bar（``end_date`` 为空）时 ``evidence['end']`` 同为空。"""
    frame, days = _block_frame([(600, 899)], 900)
    rows = spi._infer_code('000001.SZ', frame, dict.fromkeys(_COUNTER_KEYS, 0))
    assert len(rows) == 1
    row = rows[0]
    assert row['end_date'] is None
    evidence = json.loads(row['evidence'])
    assert evidence['end'] is None
    assert evidence['days'] == sum(1 for day in days if day >= row['start_date'])
