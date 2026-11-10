"""分红事件表合并与冲突仲裁单元测试。

覆盖 ``tools/collect_stock_dividend.py`` 方案 A 的三条规则：
1. ``(ts_code, ex_date)`` 已有存量事件时，源端同日的未配对行不再补录，只记冲突；
2. 同日同键的源端重复披露按 (每股现金, 每股送转) 去重；
3. 采纳源端金额前用交易所除权参考价复核，反推现金与源端不符则保留本地值。
"""

import sys
from datetime import date
from pathlib import Path

import polars as pl
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / 'tools'))

import collect_stock_dividend as csd  # noqa: E402
from collect_stock_dividend import (  # noqa: E402
    FINAL_SCHEMA,
    merge_frames,
    reconcile_conflicts,
)


def _row(code: str, ex: date, cash: float, share: float, **overrides: object) -> dict:
    row = dict.fromkeys(FINAL_SCHEMA)
    row.update({
        'ts_code': code,
        'symbol': code.split('.')[0],
        'name': code,
        'dividend_year': str(ex.year),
        'report_date': ex.isoformat(),
        'record_date': ex,
        'ex_date': ex,
        'cash_div': cash,
        'share_div': share,
        'rights_issue_ratio': 0.0,
        'source': 'test',
        'report_period': date(ex.year - 1, 12, 31),
        'progress': '实施',
    })
    row.update(overrides)
    return row


def _frame(rows: list[dict]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema=FINAL_SCHEMA)
    return pl.DataFrame(rows).select(
        [pl.col(name).cast(dtype, strict=False) for name, dtype in FINAL_SCHEMA.items()])


def test_same_day_relanded_report_period_not_appended():
    """源端同日两行不同报告期 + 存量一行：不新增行，重复披露进 conflict。"""
    ex = date(2025, 5, 29)
    existing = _frame([_row('300760.SZ', ex, 1.41, 0.0, report_period=date(2024, 12, 31))])
    new = _frame([
        _row('300760.SZ', ex, 1.41, 0.0, report_period=date(2025, 3, 31)),
        _row('300760.SZ', ex, 0.56, 0.0, report_period=date(2024, 12, 31)),
    ])
    final, stats, conflicts = merge_frames(existing, new)
    assert final.height == 1
    assert stats['appended_same_day'] == 0
    assert len(conflicts) == 1
    assert conflicts[0]['src_cash'] == pytest.approx(0.56)
    assert conflicts[0]['local_cash'] == pytest.approx(1.41)


def test_quote_mismatch_keeps_local(tmp_path, monkeypatch):
    """源端金额与交易所参考价不符：记 unresolved，保留本地值。"""
    code = '600000.SH'
    ex = date(2024, 5, 21)
    cache_dir = tmp_path / 'cache'
    (cache_dir / 'cninfo').mkdir(parents=True)
    pl.DataFrame({
        'ex_date': [ex],
        'cash_div': [0.20],
        'share_div': [0.0],
        'pay_date': ['2024-05-25'],
        'notice_date': [date(2024, 5, 10)],
    }).write_parquet(cache_dir / 'cninfo' / '600000.parquet')

    quote_dir = tmp_path / 'quotes' / 'sh'
    quote_dir.mkdir(parents=True)
    # 除权日参考价 = 上一交易日收盘 * 0.97 → 反推现金 0.30，与源端 0.20 不符
    pl.DataFrame({
        'date': [date(2024, 5, 20), ex],
        'close': [10.0, 9.7],
        'pre_close': [9.8, 9.7],
        'qfq_factor': [1.0, 1.0 / 0.97],
    }).write_parquet(quote_dir / f'{code}.parquet')
    monkeypatch.setattr(csd, '_QUOTE_ROOT', tmp_path / 'quotes')

    frame = _frame([_row(code, ex, 0.30, 0.0)])
    conflicts = [{'ts_code': code, 'ex_date': ex, 'local_cash': 0.30,
                  'local_share': 0.0, 'src_cash': 0.20, 'src_share': 0.0,
                  'local_rights': 0.0, 'src_rights': 0.0}]
    out, stats = reconcile_conflicts(frame, conflicts, cache_dir)
    assert stats['quote_checked'] == 1
    assert stats['quote_mismatch'] == 1
    assert stats['corrected_to_source'] == 0
    assert out['cash_div'].to_list() == [pytest.approx(0.30)]
    assert stats['unresolved_rows'][0]['quote_cash'] == pytest.approx(0.30)


def test_same_day_cash_and_share_both_appended():
    """同日现金 + 送转两条不同类型：判重不吞掉，两条都正常入库。"""
    ex = date(2024, 7, 15)
    new = _frame([
        _row('000002.SZ', ex, 0.5, 0.0, report_period=date(2023, 12, 31)),
        _row('000002.SZ', ex, 0.0, 0.1, report_period=date(2023, 12, 31)),
    ])
    final, stats, _ = merge_frames(_frame([]), new)
    assert final.height == 2
    assert stats['duplicate_source_rows'] == 0
    assert stats['appended_same_day'] == 2


def test_duplicate_source_rows_deduped():
    """同日同键金额完全相同的重复披露：只保留一行并计数。"""
    ex = date(2024, 5, 21)
    new = _frame([
        _row('600000.SH', ex, 0.3, 0.0, report_period=date(2023, 12, 31)),
        _row('600000.SH', ex, 0.3, 0.0, report_period=date(2024, 3, 31)),
    ])
    final, stats, _ = merge_frames(_frame([]), new)
    assert final.height == 1
    assert stats['duplicate_source_rows'] == 1


def test_extra_columns_preserved_through_merge():
    """存量里的非 FINAL_SCHEMA 列（*_fix / no_ex_rights 等）必须原样带出。

    2026-11-10 事故：采集脚本只按 FINAL_SCHEMA 重建表，一次采集把
    cash_div_fix / share_div_fix / no_ex_rights 与 11 个派生列全部丢掉，
    因子计算因缺列走 fill_null 兜底，静默退回未修正值。
    """
    ex = date(2024, 6, 20)
    existing = _frame([_row('601258.SH', ex, 0.0, 0.56416)]).with_columns(
        pl.lit(0.04).alias('cash_div_fix'),
        pl.lit(0.0).alias('share_div_fix'),
        pl.lit(False).alias('no_ex_rights'),
        pl.lit('orig').alias('share_div_source'),
        pl.lit(1.23).alias('total_div_est_all'),
        pl.lit(ex, dtype=pl.Date).alias('est_effective_date'))
    new = _frame([_row('600000.SH', date(2024, 7, 1), 0.3, 0.0,
                       report_period=date(2023, 12, 31))])
    final, _, _ = merge_frames(existing, new)

    for col in ('cash_div_fix', 'share_div_fix', 'no_ex_rights',
                'share_div_source', 'total_div_est_all', 'est_effective_date'):
        assert col in final.columns, col
    kept = final.filter(pl.col('ts_code') == '601258.SH')
    assert kept['cash_div_fix'].to_list() == [pytest.approx(0.04)]
    assert kept['share_div_source'].to_list() == ['orig']
    assert kept['total_div_est_all'].to_list() == [pytest.approx(1.23)]
    appended = final.filter(pl.col('ts_code') == '600000.SH')
    assert appended['cash_div_fix'].to_list() == [None]
