"""本地名录（采集域）与基金本地检查的单测。"""

from __future__ import annotations

import json
from datetime import date, datetime

import polars as pl
import pytest

from src.data.services.local_first_basic import check_fund_basic_local
from src.data.universe_source import (
    freshness_lag_days,
    load_local_codes_with_list_date,
    load_local_universe,
)


def _write_stock_basic(root, rows: list[dict]) -> None:
    meta = root / 'meta'
    meta.mkdir(parents=True, exist_ok=True)
    frame = pl.DataFrame(rows, schema={
        'ts_code': pl.String, 'name': pl.String, 'list_date': pl.Date,
        'delist_date': pl.Date, 'status': pl.String, 'market': pl.String,
    })
    frame.write_parquet(meta / 'stock_basic.parquet')
    (meta / 'stock_basic.manifest.json').write_text(
        json.dumps({'latest_trade_date': '2026-11-13'}), encoding='utf-8')


def _stock_rows() -> list[dict]:
    return [
        {'ts_code': '600000.SH', 'name': '浦发银行', 'list_date': date(1999, 11, 10),
         'delist_date': None, 'status': 'L', 'market': '主板'},
        {'ts_code': '688827.SH', 'name': '钶锐', 'list_date': date(2026, 11, 10),
         'delist_date': None, 'status': 'L', 'market': '科创板'},
        {'ts_code': '002731.SZ', 'name': '萃华退', 'list_date': date(2014, 10, 27),
         'delist_date': date(2026, 11, 12), 'status': 'D', 'market': '主板'},
        {'ts_code': '920680.BJ', 'name': '广道退', 'list_date': date(2021, 11, 15),
         'delist_date': date(2025, 12, 31), 'status': 'D', 'market': '北交所'},
    ]


def test_load_local_universe_uses_status_l(tmp_path):
    _write_stock_basic(tmp_path, _stock_rows())

    symbols, listed = load_local_universe(tmp_path)

    assert listed == ['600000.SH', '688827.SH']
    assert symbols['688827'] == '688827.SH'
    assert symbols['002731'] == '002731.SZ'


def test_load_local_codes_with_list_date(tmp_path):
    _write_stock_basic(tmp_path, _stock_rows())

    codes, dates, label = load_local_codes_with_list_date(tmp_path)

    assert len(codes) == 4
    assert dates['688827.SH'] == date(2026, 11, 10)
    assert 'local stock_basic' in label


def test_freshness_lag_detects_stale_names(tmp_path):
    _write_stock_basic(tmp_path, _stock_rows())

    assert freshness_lag_days(tmp_path, as_of=date(2026, 11, 13)) == 0
    assert freshness_lag_days(tmp_path, as_of=date(2026, 11, 16)) == 3


def test_freshness_lag_none_when_manifest_missing(tmp_path):
    meta = tmp_path / 'meta'
    meta.mkdir(parents=True)
    pl.DataFrame({'ts_code': [], 'status': []}).write_parquet(meta / 'stock_basic.parquet')

    assert freshness_lag_days(tmp_path, as_of=date(2026, 11, 13)) is None


def test_collect_financials_rejects_stale_names(tmp_path, monkeypatch):
    from tools.collect_financials import load_universe

    monkeypatch.setenv('DATA__REMOTE_DATABASE_FALLBACK', 'false')
    _write_stock_basic(tmp_path, _stock_rows())

    with pytest.raises(SystemExit, match='名录落后'):
        load_universe(tmp_path, date(2026, 11, 16))

    symbols, listed = load_universe(tmp_path, date(2026, 11, 13))
    assert listed == ['600000.SH', '688827.SH']
    assert len(symbols) == 4


def _write_fund_basic(root) -> None:
    meta = root / 'meta'
    meta.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [{
            'id': 1, 'ts_code': '159915.SZ', 'symbol': '159915', 'name': '创业板ETF',
            'fund_type': 'ETF', 'market': '深市', 'list_date': date(2011, 12, 9),
            'delist_date': None, 'status': 'L', 'created_at': datetime(2026, 1, 1),
            'updated_at': datetime(2026, 1, 1), 'is_tradable': True,
            'tradable_reason': None, 'source': None,
        }],
        schema={
            'id': pl.Int64, 'ts_code': pl.String, 'symbol': pl.String, 'name': pl.String,
            'fund_type': pl.String, 'market': pl.String, 'list_date': pl.Date,
            'delist_date': pl.Date, 'status': pl.String, 'created_at': pl.Datetime,
            'updated_at': pl.Datetime, 'is_tradable': pl.Boolean,
            'tradable_reason': pl.String, 'source': pl.String,
        },
    ).write_parquet(meta / 'fund_basic.parquet')


def test_check_fund_basic_local_no_diff(tmp_path):
    _write_stock_basic(tmp_path, _stock_rows())
    _write_fund_basic(tmp_path)
    before = (tmp_path / 'meta' / 'fund_basic.parquet').read_bytes()

    result = check_fund_basic_local(store_root=tmp_path)

    assert result['changed'] is False
    assert result['appended'] == 0
    assert (tmp_path / 'meta' / 'fund_basic.parquet').read_bytes() == before
    manifest = json.loads(
        (tmp_path / 'meta' / 'fund_basic.manifest.json').read_text(encoding='utf-8'))
    assert manifest['last_check']['mode'] == 'local_only_no_diff'


def test_check_fund_basic_local_appends_fund_from_stock(tmp_path):
    rows = _stock_rows() + [{
        'ts_code': '510300.SH', 'name': '沪深300ETF', 'list_date': date(2012, 5, 28),
        'delist_date': None, 'status': 'L', 'market': '沪市'}]
    _write_stock_basic(tmp_path, rows)
    _write_fund_basic(tmp_path)

    result = check_fund_basic_local(store_root=tmp_path)

    assert result['changed'] is True
    assert result['appended'] == 1
    frame = pl.read_parquet(tmp_path / 'meta' / 'fund_basic.parquet')
    assert '510300.SH' in frame['ts_code'].to_list()
    assert list((tmp_path / '_backup').iterdir())
