#!/usr/bin/env python3

"""采集器规范化纯函数与估值增量模式（不发起网络请求）。"""

from __future__ import annotations

import json
from datetime import date

import pandas as pd
import polars as pl
import pytest

from src.data.financials import collector, store
from src.data.financials.collector import (
    FinancialsCollector,
    infer_ts_code,
    normalize_bulk_statement,
    normalize_valuation,
    periods_between,
    statutory_deadline,
)


def test_statutory_deadline() -> None:
    assert statutory_deadline(date(2025, 3, 31)) == date(2025, 4, 30)
    assert statutory_deadline(date(2025, 6, 30)) == date(2025, 8, 31)
    assert statutory_deadline(date(2025, 9, 30)) == date(2025, 10, 31)
    assert statutory_deadline(date(2025, 12, 31)) == date(2026, 4, 30)


def test_periods_between_quarters() -> None:
    periods = periods_between(date(2016, 1, 1), date(2016, 12, 31))
    assert periods == [date(2016, 3, 31), date(2016, 6, 30),
                       date(2016, 9, 30), date(2016, 12, 31)]
    assert len(periods_between(date(2016, 1, 1), date(2026, 9, 30))) == 43


def test_infer_ts_code_by_market() -> None:
    assert infer_ts_code('600519') == '600519.SH'
    assert infer_ts_code('300750') == '300750.SZ'
    assert infer_ts_code('688981') == '688981.SH'
    assert infer_ts_code('920158') == '920158.BJ'
    assert infer_ts_code('830799') == '830799.BJ'


def test_normalize_bulk_income_uses_notice_date() -> None:
    df = pd.DataFrame({
        '股票代码': ['600519', '300750'],
        '公告日期': ['2026-11-01', '2026-11-01'],
        '营业总收入': [1000.0, 800.0],
        '营业总支出-营业支出': [300.0, 500.0],
        '净利润': [200.0, 100.0],
    })
    symbols = {'600519': '600519.SH', '300750': '300750.SZ'}
    ann_map = {'600519': (date(2025, 10, 30), date(2026, 10, 31), 200.0),
               '300750': (date(2025, 10, 29), date(2026, 10, 31), 100.0)}
    frame = normalize_bulk_statement('income', df, date(2025, 9, 30),
                                     ann_map, symbols, source='test')
    assert frame.height == 2
    rows = {r['ts_code']: r for r in frame.to_dicts()}
    assert rows['600519.SH']['ann_date'] == date(2025, 10, 30)
    assert rows['600519.SH']['ann_date_source'] == 'em_notice'
    assert rows['600519.SH']['gross_profit'] == 700.0
    # 批量接口的「公告日期」是更新日，不得作为 ann_date
    assert rows['600519.SH']['ann_date'] != date(2026, 11, 1)


def test_normalize_bulk_falls_back_to_deadline() -> None:
    df = pd.DataFrame({'股票代码': ['600519'], '营业总收入': [1000.0],
                       '营业总支出-营业支出': [300.0], '净利润': [200.0]})
    frame = normalize_bulk_statement('income', df, date(2025, 9, 30),
                                     {}, {'600519': '600519.SH'}, source='test')
    row = frame.to_dicts()[0]
    assert row['ann_date'] == date(2025, 10, 31)
    assert row['ann_date_source'] == 'statutory_deadline'


def test_normalize_bulk_rejects_pre_period_notice() -> None:
    """公告日早于报告期视为脏数据，退化为法定截止日。"""
    df = pd.DataFrame({'股票代码': ['600519'], '营业总收入': [1000.0],
                       '营业总支出-营业支出': [300.0], '净利润': [200.0]})
    ann_map = {'600519': (date(2025, 9, 1), None, 200.0)}
    frame = normalize_bulk_statement('income', df, date(2025, 9, 30),
                                     ann_map, {'600519': '600519.SH'}, source='test')
    row = frame.to_dicts()[0]
    assert row['ann_date'] == date(2025, 10, 31)
    assert row['ann_date_source'] == 'statutory_deadline'


def test_normalize_valuation_and_dividend_yield() -> None:
    df = pd.DataFrame({
        '数据日期': ['2026-11-03', '2026-11-04'],
        '当日收盘价': [100.0, 101.0],
        'PE(TTM)': [20.0, 20.2],
        'PE(静)': [21.0, 21.2],
        '市净率': [8.0, 8.08],
        '市销率': [7.0, 7.07],
        '总市值': [1.0e8, 1.01e8],
        '流通市值': [9.0e7, 9.09e7],
    })
    divs = pl.DataFrame({
        'ts_code': ['600519.SH', '600519.SH'],
        'ex_date': [date(2026, 6, 20), date(2025, 6, 20)],
        'cash_div': [2.0, 1.0],
    })
    frame = normalize_valuation(df, '600519.SH', {'600519.SH': divs})
    rows = frame.to_dicts()
    assert frame.height == 2
    assert rows[0]['pe'] == 20.0
    assert rows[0]['total_mv'] == 1.0e8
    # 过去 365 天只有 2026-06-20 的 2 元，按收盘价 100 计算股息率 2%
    assert rows[0]['dv_ratio'] == 2.0
    assert rows[1]['dv_ratio'] == pytest.approx(2.0 / 101.0 * 100)
    assert 'dv_ratio' in frame.columns


def test_normalize_valuation_without_dividends_is_null() -> None:
    df = pd.DataFrame({'数据日期': ['2026-11-04'], '当日收盘价': [100.0],
                       'PE(TTM)': [20.0], '市净率': [8.0], '市销率': [7.0],
                       '总市值': [1.0e8], '流通市值': [9.0e7]})
    frame = normalize_valuation(df, '600519.SH', None)
    assert frame.to_dicts()[0]['dv_ratio'] is None


def test_normalize_valuation_no_dividend_record_is_zero() -> None:
    """分红快照可用但该标的无记录 / 窗口内无分红时，股息率应为 0。"""
    df = pd.DataFrame({'数据日期': ['2026-11-04'], '当日收盘价': [100.0],
                       'PE(TTM)': [20.0], '市净率': [8.0], '市销率': [7.0],
                       '总市值': [1.0e8], '流通市值': [9.0e7]})
    absent = normalize_valuation(df, '600519.SH', {})
    assert absent.to_dicts()[0]['dv_ratio'] == 0.0

    empty = pl.DataFrame(schema={'ex_date': pl.Date, 'cash_div': pl.Float64})
    empty_frame = normalize_valuation(df, '600519.SH', {'600519.SH': empty})
    assert empty_frame.to_dicts()[0]['dv_ratio'] == 0.0

    future_div = pl.DataFrame({
        'ts_code': ['600519.SH'],
        'ex_date': [date(2026, 12, 20)],
        'cash_div': [2.0],
    })
    future = normalize_valuation(df, '600519.SH', {'600519.SH': future_div})
    assert future.to_dicts()[0]['dv_ratio'] == 0.0


def test_bulk_mapping_covers_schema_columns() -> None:
    """收入/资产/现金流映射的目标列必须存在于 schema 中。"""
    for table, mapping in collector._BULK_MAPPINGS.items():  # noqa: SLF001
        assert set(mapping.values()) <= set(
            __import__('src.data.financials.store', fromlist=['SCHEMAS'])
            .SCHEMAS[table]), table


# ----------------------------------------------------------------------
# 估值区间增量（--since / --until）
# ----------------------------------------------------------------------
def _fake_value_frame(dates: list[date]) -> pd.DataFrame:
    return pd.DataFrame({
        '数据日期': [d.isoformat() for d in dates],
        '当日收盘价': [10.0] * len(dates),
        'PE(TTM)': [20.0] * len(dates),
        'PE(静)': [21.0] * len(dates),
        '市净率': [2.0] * len(dates),
        '市销率': [1.0] * len(dates),
        '总市值': [1.0e8] * len(dates),
        '流通市值': [9.0e7] * len(dates),
    })


def _patch_value_em(monkeypatch: pytest.MonkeyPatch, dates: list[date],
                    calls: list[str]) -> None:
    def fake(symbol: str = '', **_: object) -> pd.DataFrame:
        calls.append(symbol)
        return _fake_value_frame(dates)

    monkeypatch.setattr(collector.ak, 'stock_value_em', fake)


def test_collect_valuation_since_only_writes_rows_in_range(
        tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    _patch_value_em(monkeypatch, [date(2026, 11, 6), date(2026, 11, 9)], calls)
    c = FinancialsCollector(tmp_path, workers=1, valuation_interval=0.0)

    result = c.collect_valuation(['600519.SH', '000001.SZ'],
                                 since=date(2026, 11, 7), until=date(2026, 11, 9))

    assert result.rows == 2
    assert sorted(calls) == ['000001', '600519']
    files = sorted(store.parts_dir('valuation', tmp_path).glob('incr_20261107_*.parquet'))
    assert len(files) == 1
    batch = pl.read_parquet(files[0])
    assert batch.height == 2
    assert batch['trade_date'].unique().to_list() == [date(2026, 11, 9)]


def test_collect_valuation_since_skips_completed_codes(
        tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    _patch_value_em(monkeypatch, [date(2026, 11, 6), date(2026, 11, 9)], calls)
    codes = ['600519.SH', '000001.SZ']

    first = FinancialsCollector(tmp_path, workers=1, valuation_interval=0.0)
    first.collect_valuation(codes, since=date(2026, 11, 7), until=date(2026, 11, 9))
    assert len(calls) == 2

    calls.clear()
    again = FinancialsCollector(tmp_path, workers=1, valuation_interval=0.0)
    result = again.collect_valuation(codes, since=date(2026, 11, 7),
                                     until=date(2026, 11, 9))
    assert result.units == 0
    assert calls == []
    assert result.rows == 0

    progress = json.loads(
        (store.parts_root(tmp_path) / 'progress.json').read_text(encoding='utf-8'))
    assert set(progress['valuation_ranges']['2026-11-07_2026-11-09']) == set(codes)


def test_collect_valuation_since_resumes_from_parts(
        tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """progress.json 丢失时，靠已有区间分片恢复断点，不重复写。"""
    calls: list[str] = []
    _patch_value_em(monkeypatch, [date(2026, 11, 6), date(2026, 11, 9)], calls)
    codes = ['600519.SH', '000001.SZ']

    FinancialsCollector(tmp_path, workers=1, valuation_interval=0.0).collect_valuation(
        codes, since=date(2026, 11, 7), until=date(2026, 11, 9))
    progress_path = store.parts_root(tmp_path) / 'progress.json'
    progress_path.unlink()

    calls.clear()
    resume = FinancialsCollector(tmp_path, workers=1, valuation_interval=0.0)
    result = resume.collect_valuation([*codes, '300750.SZ'], since=date(2026, 11, 7),
                                      until=date(2026, 11, 9))

    assert calls == ['300750']
    assert result.units == 1
    assert result.rows == 1
    merged = store.merge_parts('valuation', tmp_path)
    table = pl.read_parquet(store.table_path('valuation', tmp_path))
    assert merged == 3
    assert table.filter(pl.col('ts_code') == '300750.SZ').height == 1
    assert table.select('ts_code', 'trade_date').is_duplicated().sum() == 0


def test_collect_valuation_force_rewrites_history(
        tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    _patch_value_em(monkeypatch, [date(2026, 11, 6), date(2026, 11, 9)], calls)

    first = FinancialsCollector(tmp_path, workers=1, valuation_interval=0.0)
    first.collect_valuation(['600519.SH'], since=date(2026, 11, 7),
                            until=date(2026, 11, 9))
    with pytest.raises(ValueError, match='互斥'):
        first.collect_valuation(['600519.SH'], force=True, since=date(2026, 11, 7))

    forced = FinancialsCollector(tmp_path, workers=1, valuation_interval=0.0)
    result = forced.collect_valuation(['600519.SH'], force=True)
    assert result.rows == 2  # force 落全历史
    files = sorted(store.parts_dir('valuation', tmp_path).glob('batch_*.parquet'))
    assert len(files) == 1
    assert pl.read_parquet(files[0])['trade_date'].n_unique() == 2
