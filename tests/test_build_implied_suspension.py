"""``tools/build_implied_suspension.py`` 的口径与落盘用例（G-113 入口固化）。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from tools import build_implied_suspension as mod


def _store(tmp_path: Path, rows: list[dict], susp: list[dict] | None = None) -> Path:
    quotes = tmp_path / 'quotes' / 'stock' / 'sz'
    quotes.mkdir(parents=True)
    pl.DataFrame(rows, schema_overrides={
        'ts_code': pl.String, 'date': pl.Date, 'open': pl.Float64,
        'high': pl.Float64, 'low': pl.Float64, 'close': pl.Float64,
        'volume': pl.Float64, 'amount': pl.Float64,
    }).write_parquet(quotes / '000001.SZ.parquet')
    meta = tmp_path / 'meta' / 'suspension'
    meta.mkdir(parents=True)
    susp_schema = {'ts_code': pl.String, 'trade_date': pl.Date,
                   'is_suspended': pl.Boolean}
    susp_frame = (pl.DataFrame(susp, schema_overrides=susp_schema) if susp
                  else pl.DataFrame(schema=susp_schema))
    susp_frame.write_parquet(meta / 'suspension_daily.parquet')
    return tmp_path


def test_build_frame_reason_and_flags(tmp_path):
    store = _store(
        tmp_path,
        [
            {'ts_code': '000001.SZ', 'date': date(2024, 1, 2), 'open': 1.0,
             'high': 1.2, 'low': 0.9, 'close': 1.1, 'volume': 100.0, 'amount': 110.0},
            {'ts_code': '000001.SZ', 'date': date(2024, 1, 3), 'open': 5.0,
             'high': 5.0, 'low': 5.0, 'close': 5.0, 'volume': None, 'amount': None},
            {'ts_code': '000001.SZ', 'date': date(2024, 1, 4), 'open': 5.0,
             'high': 6.0, 'low': 4.0, 'close': 5.5, 'volume': None, 'amount': 10.0},
        ],
        susp=[
            {'ts_code': '000001.SZ', 'trade_date': date(2024, 1, 3),
             'is_suspended': True},
            # 2026-11-12 新增的 false 留档行不参与停牌判定
            {'ts_code': '000001.SZ', 'trade_date': date(2024, 1, 4),
             'is_suspended': False},
        ],
    )
    frame = mod.build_frame(store, ('stock',), date(2026, 11, 13))
    assert frame.columns == list(mod.OUT_COLUMNS)
    assert frame.height == 2
    by_day = {str(row['trade_date']): row for row in frame.to_dicts()}
    assert by_day['2024-01-03']['reason'] == 'volume_null+amount_null+ohlc_flat'
    assert by_day['2024-01-03']['ohlc_flat'] is True
    assert by_day['2024-01-03']['in_existing_suspension'] is True
    assert by_day['2024-01-03']['conflicts_with_existing_suspension'] is False
    assert by_day['2024-01-04']['reason'] == 'volume_null'
    assert by_day['2024-01-04']['ohlc_flat'] is False
    # false 留档行不算「已在停牌表」
    assert by_day['2024-01-04']['in_existing_suspension'] is False
    assert by_day['2024-01-04']['conflicts_with_existing_suspension'] is True
    assert set(frame['source'].to_list()) == {mod.SOURCE_TAG}


def test_compare_formal_detects_value_change(tmp_path):
    store = _store(
        tmp_path,
        [{'ts_code': '000001.SZ', 'date': date(2024, 1, 3), 'open': 5.0,
          'high': 5.0, 'low': 5.0, 'close': 5.0, 'volume': None, 'amount': None}],
    )
    cand = mod.build_frame(store, ('stock',), date(2026, 11, 13))
    assert mod.compare_formal(cand, cand)['rows_equal'] is True
    tampered = cand.with_columns(pl.lit(False).alias('ohlc_flat'))
    report = mod.compare_formal(cand, tampered)
    assert report['rows_equal'] is False
    assert report['column_diff_rows']['ohlc_flat'] == 1


def test_apply_formal_backs_up_then_replaces(tmp_path):
    store = _store(
        tmp_path,
        [{'ts_code': '000001.SZ', 'date': date(2024, 1, 3), 'open': 5.0,
          'high': 5.0, 'low': 5.0, 'close': 5.0, 'volume': None, 'amount': None}],
    )
    cand = mod.build_frame(store, ('stock',), date(2026, 11, 13))
    formal = store / mod.FORMAL_REL
    pl.DataFrame({'stale': [1]}).write_parquet(formal)
    backup = tmp_path / '_backup' / 'suspension_20261113'
    report = mod.apply_formal(cand, formal, backup)
    assert (backup / formal.name).is_file()
    assert report['sha256_before'] and report['sha256_after']
    assert report['sha256_before'] != report['sha256_after']
    assert pl.read_parquet(formal).height == cand.height
