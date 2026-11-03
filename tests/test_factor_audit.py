"""复权因子核验模块回归测试（只读、临时目录构造）。"""

from datetime import date, timedelta

import numpy as np
import polars as pl

from src.data.adj_factor_calculator import AdjFactorCalculator
from src.data.local_store.store import LocalFactorStore, LocalQuoteStore
from src.data.quality.factors import _naive_factors, audit_factors


def _weekdays(start: date, rows: int) -> list[date]:
    out: list[date] = []
    current = start
    while len(out) < rows:
        if current.weekday() < 5:
            out.append(current)
        current += timedelta(days=1)
    return out


def _quotesframe(start: date = date(2024, 1, 2), rows: int = 8) -> pl.DataFrame:
    dates = _weekdays(start, rows)
    closes = [10.0 + index for index in range(rows)]
    return pl.DataFrame({
        'date': dates,
        'open': closes,
        'high': [value + 0.2 for value in closes],
        'low': [value - 0.2 for value in closes],
        'close': closes,
        'volume': [1000.0 + index for index in range(rows)],
        'amount': [10000.0 for _ in range(rows)],
        'pre_close': [0.0, *closes[:-1]],
        'change': [0.0, *[1.0 for _ in range(rows - 1)]],
        'pct_chg': [0.0, *[1.0 for _ in range(rows - 1)]],
    })


def _build_store(tmp_path, codes: list[str]) -> LocalQuoteStore:
    store = LocalQuoteStore(tmp_path)
    for code in codes:
        store.write_daily('stock', code, _quotesframe(), full=True,
                          source_mtime_ns=1)
    store.flush_meta()
    return store


def _factors_from_dividends(frame: pl.DataFrame,
                            div: pl.DataFrame) -> pl.DataFrame:
    prices = frame.select([
        pl.col('date').alias('trade_date'), pl.col('close')])
    return AdjFactorCalculator.calculate_factors(prices, div)


def test_naive_factors_matches_expected_event_order():
    dates = _weekdays(date(2024, 1, 2), 5)
    closes = [10.0, 10.0, 10.0, 10.0, 10.0]
    events = [{'ex_date': dates[2], 'cash_div': 0.5, 'share_div': 0.0,
               'rights_issue_ratio': 0.0, 'rights_issue_price': 0.0}]
    qfq, hfq = _naive_factors(dates, closes, events)
    # prev_close=10，cash=0.5 → 事件系数 0.95
    assert np.allclose(qfq[:2], 0.95)
    assert np.allclose(qfq[2:], 1.0)
    assert np.allclose(hfq[:2], 1.0)
    assert np.allclose(hfq[2:], 1 / 0.95)


def test_audit_detects_coverage_sample_and_jumps(tmp_path):
    codes = ['600000.SH', '000001.SZ']
    store = _build_store(tmp_path, codes)
    factor_dir = tmp_path / 'factors'
    factor_dir.mkdir(parents=True)

    quote = _quotesframe()
    div = pl.DataFrame({
        'ts_code': ['600000.SH'],
        'ex_date': [date(2024, 1, 4)],
        'cash_div': [0.5],
        'share_div': [0.0],
        'rights_issue_ratio': [0.0],
        'rights_issue_price': [0.0],
    })
    computed = _factors_from_dividends(quote, div).with_columns(
        pl.lit('600000.SH').alias('ts_code'))
    flat = quote.select([
        pl.col('date').alias('trade_date'),
        pl.lit(1.0).alias('qfq_factor'),
        pl.lit(1.0).alias('hfq_factor'),
        pl.lit('000001.SZ').alias('ts_code'),
    ])
    snapshot = pl.concat([
        computed.select(['ts_code', 'trade_date', 'qfq_factor', 'hfq_factor']),
        flat.select(['ts_code', 'trade_date', 'qfq_factor', 'hfq_factor']),
    ]).sort(['ts_code', 'trade_date'])
    snapshot.write_parquet(factor_dir / 'stock_adj_factor.parquet')
    div.write_parquet(factor_dir / 'stock_dividend.parquet')

    factor_store = LocalFactorStore(tmp_path)
    audit = audit_factors(store, factor_store, output_dir=tmp_path / 'logs',
                          sample_size=2)
    payload = audit.payload

    assert payload['coverage']['factor_codes'] == 2
    assert payload['coverage']['quote_stock_codes'] == 2
    assert payload['coverage']['stock_count_without_factor'] == 0
    assert payload['coverage']['factor_code_count_not_in_stock'] == 0
    assert payload['recompute']['codes_checked'] == 2
    assert payload['recompute']['codes_diff'] == 0
    assert payload['recompute']['max_rel']['qfq_factor'] <= 1e-6
    assert payload['sample']['size'] == 2
    assert payload['sample']['max_rel_vs_snapshot'] <= 1e-9
    assert payload['sample']['reversible_max_rel'] <= 1e-12
    # 2024-01-04 一次除权 → 一行跳变，且能匹配到分红事件
    assert payload['jumps']['jump_rows'] == 1
    assert payload['jumps']['matched_jump_rows'] == 1
    assert payload['jumps']['factor_without_source'] == 0
    assert payload['jumps']['event_without_jump'] == 0
    assert audit.report_files['json']


def test_audit_reports_event_without_jump_and_index_boundary(tmp_path):
    codes = ['600000.SH']
    store = _build_store(tmp_path, codes)
    factor_dir = tmp_path / 'factors'
    factor_dir.mkdir(parents=True)

    quote = _quotesframe()
    div = pl.DataFrame({
        'ts_code': ['600000.SH'],
        'ex_date': [date(2024, 1, 4)],
        'cash_div': [0.5],
        'share_div': [0.0],
        'rights_issue_ratio': [0.0],
        'rights_issue_price': [0.0],
    })
    # 刻意写入全 1 因子：有事件但无跳变
    flat = quote.select([
        pl.col('date').alias('trade_date'),
        pl.lit(1.0).alias('qfq_factor'),
        pl.lit(1.0).alias('hfq_factor'),
        pl.lit('600000.SH').alias('ts_code'),
    ])
    index = quote.select([
        pl.col('date').alias('trade_date'),
        pl.lit(1.0).alias('qfq_factor'),
        pl.lit(1.0).alias('hfq_factor'),
        pl.lit('000001.SH').alias('ts_code'),
    ])
    pl.concat([flat, index]).write_parquet(
        factor_dir / 'stock_adj_factor.parquet')
    div.write_parquet(factor_dir / 'stock_dividend.parquet')

    factor_store = LocalFactorStore(tmp_path)
    payload = audit_factors(store, factor_store,
                            output_dir=tmp_path / 'logs', sample_size=1).payload

    assert payload['jumps']['event_without_jump'] == 1
    assert payload['jumps']['factor_without_source'] == 0
    # 指数被识别出来，但不会进入个股重算
    assert payload['coverage']['factor_code_count_not_in_stock'] == 1
    assert payload['boundary']['index_codes_in_factor'] == 1
    assert payload['recompute']['codes_checked'] == 1
