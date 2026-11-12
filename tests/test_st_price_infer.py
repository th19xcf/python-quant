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
        'excluded_s_share': False,
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


# ----------------------------------------------------------------------
# 未股改 S 股：与 ST 同受 ±5% 约束，不得判成疑似 ST
# ----------------------------------------------------------------------
def test_s_share_name_classification() -> None:
    """``S+`` 是未股改；``ST``/``*ST``/``SST``/``S*ST`` 归风险警示，不能当 S 证据。"""
    assert spi.is_s_share_name('S东湖新')
    assert spi.is_s_share_name('S上石化')
    assert spi.is_s_share_name(' S钱江水 ')
    assert not spi.is_s_share_name('ST哈药')
    assert not spi.is_s_share_name('*ST哈药')
    assert not spi.is_s_share_name('SST哈药')
    assert not spi.is_s_share_name('S*ST哈药')
    assert not spi.is_s_share_name('哈药股份')
    assert not spi.is_s_share_name('G皖维')
    assert not spi.is_s_share_name(None)


def _write_s_sources(root, history_rows, em_rows) -> None:
    """落一份最小 ``name_history`` + 东财曾用名，供 S 区间表构建用。"""
    hist_dir = root / 'meta'
    hist_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(history_rows, schema={'ts_code': pl.Utf8, 'name': pl.Utf8,
                                      'start_date': pl.Date, 'end_date': pl.Date}
                 ).write_parquet(hist_dir / 'name_history.parquet')
    em_dir = root / 'meta' / 'st' / 'raw'
    em_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(em_rows, schema={'secucode': pl.Utf8, 'name': pl.Utf8,
                                 'former_names': pl.Utf8}
                 ).write_parquet(em_dir / 'em_org_basic.parquet')


def test_s_share_intervals_cover_history_and_em_gap(tmp_path) -> None:
    """主源给区间；``name_history`` 漏记 S 段时用「前后同名的唯一空档」补。"""
    root = tmp_path / 'store'
    _write_s_sources(
        root,
        [
            # 主源：name_history 直接有 S 行
            {'ts_code': '600688.SH', 'name': 'S上石化',
             'start_date': date(2006, 10, 9), 'end_date': date(2013, 8, 19)},
            # 漏洞：2006-10-09~2007-09-10 缺 S 段，前后都叫「西藏药业」
            {'ts_code': '600211.SH', 'name': '西藏药业',
             'start_date': date(1999, 7, 21), 'end_date': date(2006, 10, 8)},
            {'ts_code': '600211.SH', 'name': '西藏药业',
             'start_date': date(2007, 9, 10), 'end_date': None},
            # 两个空档：无法定性，必须进 unresolved 不猜
            {'ts_code': '600999.SH', 'name': '某公司',
             'start_date': date(2000, 1, 1), 'end_date': date(2002, 1, 1)},
            {'ts_code': '600999.SH', 'name': '某公司',
             'start_date': date(2003, 1, 1), 'end_date': date(2004, 1, 1)},
            {'ts_code': '600999.SH', 'name': '某公司',
             'start_date': date(2005, 1, 1), 'end_date': date(2006, 1, 1)},
        ],
        [
            {'secucode': '600688.SH', 'name': '上石化', 'former_names': '上海石化→S上石化'},
            {'secucode': '600211.SH', 'name': '西藏药业', 'former_names': '西藏药业→S藏药业'},
            {'secucode': '600999.SH', 'name': '某公司', 'former_names': '某公司→S某公司'},
            {'secucode': '600777.SH', 'name': '某无缺口', 'former_names': '某无缺口'},
        ])
    frame, stats = spi.load_s_share_intervals(root)
    got = {(row['ts_code'], row['name']): (row['start_date'], row['end_date'],
                                          row['source']) for row in frame.to_dicts()}
    assert got[('600688.SH', 'S上石化')] == (date(2006, 10, 9), date(2013, 8, 19),
                                            spi.SOURCE_S_NAME_HISTORY)
    assert got[('600211.SH', 'S藏药业')] == (date(2006, 10, 9), date(2007, 9, 9),
                                            spi.SOURCE_S_EM_GAP)
    assert stats['em_gap_filled'] == ['600211.SH']
    assert stats['unresolved_ambiguous_gap'] == ['600999.SH']
    assert stats['em_available'] is True


def test_empty_s_table_restores_suspected_flag(tmp_path) -> None:
    """反例自测：S 区间表置空后，原来被排除的段必须重新算成疑似 ST。

    证明排除逻辑真的在起作用，而不是「本来就不判」。
    """
    suspected = pl.DataFrame([{
        'ts_code': '600211.SH', 'start_date': date(2006, 12, 21),
        'end_date': date(2007, 8, 2), 'flag': spi.FLAG_SUSPECTED,
        'source': spi.SOURCE_PRICE, 'confidence': '',
        'evidence': '{}', 'fetched_at': datetime.now(),
        'excluded_s_share': False,
    }], schema=spi._PRICE_SCHEMA)
    coverage = {'600211.SH': (date(1997, 1, 1), date(2100, 1, 1))}
    s_frame = pl.DataFrame([{
        'ts_code': '600211.SH', 'start_date': date(2006, 10, 9),
        'end_date': date(2007, 9, 10), 'name': 'S藏药业',
        'source': spi.SOURCE_S_EM_GAP,
    }], schema=spi._S_SHARE_SCHEMA)

    counters = {'excluded_s_share_intervals': 0, 's_partial_overlap_intervals': 0}
    marked = spi._apply_s_exclusion(suspected, s_frame, counters)
    assert marked['excluded_s_share'].to_list() == [True]
    assert marked['excluded_s_share'].null_count() == 0
    assert counters['excluded_s_share_intervals'] == 1
    annotated, compare = spi._annotate(marked, {}, coverage)
    assert compare['metrics']['suspected_intervals'] == 0
    assert compare['metrics']['excluded_s_share_intervals'] == 1
    assert compare['classes']['suspected_only'] == []
    # confidence 仍按原有规则给出（既有列逐值不变）
    assert annotated['confidence'].to_list() == [spi.CONF_LOW]

    # 反例：S 区间表置空 → 同一段必须重新计入疑似
    empty = spi._S_SHARE_SCHEMA
    counters_zero = {'excluded_s_share_intervals': 0, 's_partial_overlap_intervals': 0}
    restored = spi._apply_s_exclusion(suspected, pl.DataFrame(schema=empty), counters_zero)
    assert restored['excluded_s_share'].to_list() == [False]
    _, compare_restored = spi._annotate(restored, {}, coverage)
    assert compare_restored['metrics']['suspected_intervals'] == 1
    assert compare_restored['metrics']['excluded_s_share_intervals'] == 0


def test_partial_overlap_is_not_excluded() -> None:
    """跨 S 区间边界的段价格证据混源，保留为疑似并单独计数。"""
    suspected = pl.DataFrame([{
        'ts_code': '600211.SH', 'start_date': date(2006, 1, 1),
        'end_date': date(2007, 8, 2), 'flag': spi.FLAG_SUSPECTED,
        'source': spi.SOURCE_PRICE, 'confidence': '',
        'evidence': '{}', 'fetched_at': datetime.now(),
        'excluded_s_share': False,
    }], schema=spi._PRICE_SCHEMA)
    s_frame = pl.DataFrame([{
        'ts_code': '600211.SH', 'start_date': date(2006, 10, 9),
        'end_date': date(2007, 9, 10), 'name': 'S藏药业',
        'source': spi.SOURCE_S_EM_GAP,
    }], schema=spi._S_SHARE_SCHEMA)
    counters = {'excluded_s_share_intervals': 0, 's_partial_overlap_intervals': 0}
    marked = spi._apply_s_exclusion(suspected, s_frame, counters)
    assert marked['excluded_s_share'].to_list() == [False]
    assert counters['s_partial_overlap_intervals'] == 1
