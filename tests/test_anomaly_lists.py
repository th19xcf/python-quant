#!/usr/bin/env python3

"""异常台账清单刷新：幂等与非致命测试。"""

from __future__ import annotations

import json
from datetime import date

import polars as pl

from src.data.quality.anomaly_lists import refresh_anomaly_lists


def _make_store(root, *, stale_end: date = date(2019, 1, 1)) -> None:
    (root / 'meta').mkdir(parents=True)
    (root / 'factors').mkdir(parents=True)
    pl.DataFrame({
        'ts_code': ['000001.SZ', '600000.SH', '000300.SH'],
        'data_type': ['stock', 'stock', 'fund'],
        'end': [date(2026, 11, 3), date(2026, 11, 3), stale_end],
    }).write_parquet(root / 'meta' / 'daily_index.parquet')
    pl.DataFrame({
        'ts_code': ['000001.SZ', '600000.SH'],
        'trade_date': [date(2026, 11, 3), date(2026, 10, 20)],
    }).write_parquet(root / 'factors' / 'stock_adj_factor.parquet')
    pl.DataFrame({
        'ts_code': ['000001.SZ', '003000.SZ', '003000.SZ'],
        'ex_date': [date(1993, 5, 24), date(1998, 1, 14), date(2010, 6, 18)],
        'rights_issue_ratio': [0.0, 0.5, 0.0],
        'rights_issue_price': [0.0, 4.4, 0.0],
        'source': ['akshare_pg'] * 3,
    }).write_parquet(root / 'factors' / 'stock_dividend.parquet')
    (root / 'quotes' / 'fund' / 'sz').mkdir(parents=True)
    # 整只占位：close=1.0 且零成交（严格判据下 full=True）
    pl.DataFrame({
        'date': [date(2019, 9, 5), date(2019, 9, 17)],
        'close': [1.0, 1.0],
        'volume': [0.0, 0.0],
        'amount': [0.0, 0.0],
    }).write_parquet(root / 'quotes' / 'fund' / 'sz' / '160217.SZ.parquet')
    # 尾部占位：前一行有真实成交
    pl.DataFrame({
        'date': [date(2026, 10, 30), date(2026, 11, 2), date(2026, 11, 3)],
        'close': [1.25, 1.0, 1.0],
        'volume': [1000.0, 0.0, 0.0],
        'amount': [1250.0, 0.0, 0.0],
    }).write_parquet(root / 'quotes' / 'fund' / 'sz' / '159041.SZ.parquet')


def test_refresh_lists_is_idempotent_and_covers_all_categories(tmp_path) -> None:
    store = tmp_path / 'store'
    _make_store(store)
    first = refresh_anomaly_lists(store, list_dir=tmp_path / 'lists')
    assert first['status'] == 'ok'
    assert set(first['files']) == {
        'factor_tail_lag_codes.csv', 'no_local_quote_codes.csv',
        'source_stale_codes.csv', 'rights_issue_codes.csv',
        'placeholder_quote_codes.csv',
    }
    lag = pl.read_csv(tmp_path / 'lists' / 'factor_tail_lag_codes.csv')
    assert lag['ts_code'].to_list() == ['600000.SH']
    no_quote = pl.read_csv(tmp_path / 'lists' / 'no_local_quote_codes.csv')
    assert no_quote['ts_code'].to_list() == ['003000.SZ']
    stale = pl.read_csv(tmp_path / 'lists' / 'source_stale_codes.csv')
    assert stale['ts_code'].to_list() == ['000300.SH']
    rights = pl.read_csv(tmp_path / 'lists' / 'rights_issue_codes.csv')
    assert rights.height == 1
    placeholder = pl.read_csv(tmp_path / 'lists' / 'placeholder_quote_codes.csv')
    assert placeholder['ts_code'].to_list() == ['159041.SZ', '160217.SZ']
    full = placeholder.filter(pl.col('ts_code') == '160217.SZ').row(0, named=True)
    assert (str(full['start']), str(full['end'])) == ('2019-09-05', '2019-09-17')
    tail = placeholder.filter(pl.col('ts_code') == '159041.SZ').row(0, named=True)
    assert (str(tail['start']), str(tail['end'])) == ('2026-11-02', '2026-11-03')

    second = refresh_anomaly_lists(store, list_dir=tmp_path / 'lists')
    assert second['status'] == 'ok'
    assert {name: item['sha256'] for name, item in second['files'].items()} == {
        name: item['sha256'] for name, item in first['files'].items()}


def test_constant_price_run_flagged_but_halt_price_untouched(tmp_path) -> None:
    """常量价 + 零成交的连续段按价格量级区分伪数据与真实停牌。"""
    store = tmp_path / 'store'
    _make_store(store)
    # 伪数据：真实成交价 1.0，随后 4 行 close 恒 100.0 且零成交 → 应判为占位
    pl.DataFrame({
        'date': [date(2010, 1, 4), date(2010, 1, 5), date(2010, 1, 6),
                 date(2010, 1, 7), date(2010, 1, 8), date(2010, 1, 11),
                 date(2010, 1, 12)],
        'close': [1.0, 1.0, 1.0, 100.0, 100.0, 100.0, 100.0],
        'volume': [100.0, 100.0, 100.0, 0.0, 0.0, 0.0, 0.0],
        'amount': [100.0, 100.0, 100.0, 0.0, 0.0, 0.0, 0.0],
    }).write_parquet(store / 'quotes' / 'fund' / 'sz' / '160300.SZ.parquet')
    # 真实停牌：close 恒 2.177 且零成交，但价格在正常区间内 → 不应判为占位
    pl.DataFrame({
        'date': [date(2020, 6, 29), date(2020, 6, 30), date(2020, 7, 1),
                 date(2020, 7, 2), date(2020, 7, 3), date(2020, 7, 6)],
        'close': [2.177] * 6,
        'volume': [100.0, 100.0, 100.0, 0.0, 0.0, 0.0],
        'amount': [217.7, 217.7, 217.7, 0.0, 0.0, 0.0],
    }).write_parquet(store / 'quotes' / 'fund' / 'sz' / '510070.SH.parquet')

    result = refresh_anomaly_lists(store, list_dir=tmp_path / 'lists')
    assert result['status'] == 'ok'
    placeholder = pl.read_csv(tmp_path / 'lists' / 'placeholder_quote_codes.csv')
    codes = placeholder['ts_code'].to_list()
    assert '510070.SH' not in codes
    row = placeholder.filter(pl.col('ts_code') == '160300.SZ').row(0, named=True)
    assert (str(row['start']), str(row['end'])) == ('2010-01-07', '2010-01-12')
    assert int(row['rows']) == 4
    assert bool(row['full']) is False


def test_refresh_lists_failure_is_non_fatal(tmp_path) -> None:
    store = tmp_path / 'store'
    _make_store(store)
    blocked = tmp_path / 'blocked'
    blocked.write_text('not a directory', encoding='utf-8')
    result = refresh_anomaly_lists(store, list_dir=blocked)
    assert result['status'] == 'failed'
    assert result['error']


def _span_keys(frame: pl.DataFrame) -> set[tuple[str, str, str]]:
    return {
        (str(r['ts_code']), str(r['start']), str(r['end']))
        for r in frame.iter_rows(named=True)
    }


def test_refresh_preserves_registered_spans_not_derivable(tmp_path) -> None:
    """``kinds`` 已登记但本次派生不出的段不得被覆盖。

    人工补登的 ``nav_series``（上市前净值序列）与 ``post_delist``（退市后结转）
    在 ``_build_placeholder_quote`` 的判据下永远产不出来，整份覆盖会静默丢掉它们。
    """
    store = tmp_path / 'store'
    _make_store(store)
    lists = tmp_path / 'lists'
    lists.mkdir()
    # 上一版台账：1 段人工补登（派生不出）+ 1 段已退役（不在 kinds 里）
    (lists / 'placeholder_quote_codes.csv').write_text(
        'ts_code,start,end,rows,total,full\n'
        '160311.SZ,2007-05-08,2007-05-11,4,16,false\n'
        '999999.SZ,2010-01-04,2010-01-05,2,2,false\n',
        encoding='utf-8')
    registry_dir = store / 'meta' / 'anomalies'
    registry_dir.mkdir(parents=True)
    (registry_dir / 'fund_pseudo_registry.json').write_text(json.dumps({
        'kinds': {
            '159041.SZ|2026-11-02|2026-11-03': 'placeholder',
            '160217.SZ|2019-09-05|2019-09-17': 'placeholder',
            '160311.SZ|2007-05-08|2007-05-11': 'nav_series',
        },
    }), encoding='utf-8')

    result = refresh_anomaly_lists(store, list_dir=lists)

    assert result['status'] == 'ok'
    rows = pl.read_csv(lists / 'placeholder_quote_codes.csv')
    keys = _span_keys(rows)
    # 人工补登段仍在，且原值未被改写
    kept = rows.filter(pl.col('ts_code') == '160311.SZ')
    assert kept.height == 1
    kept_row = kept.row(0, named=True)
    assert (str(kept_row['start']), str(kept_row['end'])) == (
        '2007-05-08', '2007-05-11')
    assert (int(kept_row['rows']), int(kept_row['total']),
            bool(kept_row['full'])) == (4, 16, False)
    # 派生段照常刷新
    assert ('159041.SZ', '2026-11-02', '2026-11-03') in keys
    assert ('160217.SZ', '2019-09-05', '2019-09-17') in keys
    # 未在 kinds 登记的段（人工退役）不得复现
    assert ('999999.SZ', '2010-01-04', '2010-01-05') not in keys
    stats = result['files']['placeholder_quote_codes.csv']
    assert (stats['derived'], stats['preserved'], stats['retired']) == (2, 1, 1)


def test_refresh_keeps_derived_only_when_registry_missing(tmp_path) -> None:
    """侧车类别表缺失时无法判定登记情况，退化为纯派生并留 warning。"""
    store = tmp_path / 'store'
    _make_store(store)
    lists = tmp_path / 'lists'
    lists.mkdir()
    (lists / 'placeholder_quote_codes.csv').write_text(
        'ts_code,start,end,rows,total,full\n'
        '160311.SZ,2007-05-08,2007-05-11,4,16,false\n',
        encoding='utf-8')

    result = refresh_anomaly_lists(store, list_dir=lists)

    assert result['status'] == 'ok'
    stats = result['files']['placeholder_quote_codes.csv']
    assert stats['preserved'] == 0
    keys = _span_keys(pl.read_csv(lists / 'placeholder_quote_codes.csv'))
    assert ('160311.SZ', '2007-05-08', '2007-05-11') not in keys
