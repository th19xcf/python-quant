#!/usr/bin/env python3

"""行情 schema 契约与唯一性：断言、写入闸门、正式库巡检。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from src.data.quote_schema import (
    FORMAL_QUOTES_ROOT,
    QUOTE_COLUMNS,
    QuoteSchemaError,
    assert_formal_quote_schema,
    assert_quote_glob_schema,
    assert_quote_schema,
    quote_data_type_for_path,
)


def _stock_frame(**overrides: object) -> pl.DataFrame:
    row = {
        'date': date(2024, 1, 2), 'ts_code': '600000.SH', 'open': 1.0, 'high': 1.1,
        'low': 0.9, 'close': 1.0, 'pre_close': 1.0, 'volume': 100.0, 'amount': 100.0,
        'pct_chg': 0.0, 'change': 0.0, 'qfq_factor': 1.0, 'hfq_factor': 1.0,
    }
    row.update(overrides)
    columns = list(QUOTE_COLUMNS['stock'])
    columns += [key for key in overrides if key not in columns]
    return pl.DataFrame({key: [row[key]] for key in columns})


def test_assert_quote_schema_accepts_canonical_frame() -> None:
    assert_quote_schema('stock', _stock_frame())
    assert_quote_schema('stock', _stock_frame().lazy())


def test_assert_quote_schema_rejects_extra_column() -> None:
    with pytest.raises(QuoteSchemaError, match='多列'):
        assert_quote_schema('stock', _stock_frame(basis='x'))


def test_assert_quote_schema_extra_allowed_but_missing_still_raises() -> None:
    assert_quote_schema('stock', _stock_frame(basis='x'), allow_extra_columns=True)
    with pytest.raises(QuoteSchemaError, match='缺列'):
        assert_quote_schema(
            'stock', _stock_frame(basis='x').drop('amount'), allow_extra_columns=True)


def test_assert_quote_schema_rejects_dtype_drift() -> None:
    frame = _stock_frame().with_columns(pl.col('volume').cast(pl.Float32))
    with pytest.raises(QuoteSchemaError, match='dtype'):
        assert_quote_schema('stock', frame)


def test_assert_quote_schema_rejects_column_order_drift() -> None:
    frame = _stock_frame().select(['ts_code', 'date', *[
        c for c in QUOTE_COLUMNS['stock'] if c not in ('ts_code', 'date')]])
    with pytest.raises(QuoteSchemaError, match='列顺序'):
        assert_quote_schema('stock', frame)


def test_assert_quote_schema_rejects_unknown_data_type() -> None:
    with pytest.raises(QuoteSchemaError, match='未知行情类型'):
        assert_quote_schema('unknown', _stock_frame())


def test_glob_schema_assertion_detects_mixed_columns(tmp_path: Path) -> None:
    narrow = tmp_path / 'a.parquet'
    wide = tmp_path / 'b.parquet'
    _stock_frame().write_parquet(narrow)
    _stock_frame(basis='x').write_parquet(wide)
    assert_quote_glob_schema('stock', str(narrow))
    with pytest.raises(QuoteSchemaError, match='schema 不一致|多列|缺列'):
        assert_quote_glob_schema('stock', [str(narrow), str(wide)])


def test_formal_path_detection_only_matches_formal_store(tmp_path: Path) -> None:
    assert quote_data_type_for_path(tmp_path / 'quotes' / 'stock' / 'x.parquet') is None
    assert quote_data_type_for_path(
        FORMAL_QUOTES_ROOT / 'stock' / 'sh' / '600000.SH.parquet') == 'stock'
    assert quote_data_type_for_path(FORMAL_QUOTES_ROOT / 'unknown' / 'x.parquet') is None


def test_write_gate_ignores_tmp_store_but_checks_formal_path(tmp_path: Path) -> None:
    from src.data.atomic_write import write_parquet_atomic

    # 临时目录：允许列子集夹具，闸门放行
    assert_formal_quote_schema(tmp_path / 'quotes' / 'stock' / 'x.parquet',
                               _stock_frame().select(['date', 'ts_code']))
    write_parquet_atomic(
        _stock_frame().select(['date', 'ts_code']), tmp_path / 'quotes' / 'stock' / 'x.parquet')

    # 正式路径：多列必须抛错，且不得落盘
    formal = FORMAL_QUOTES_ROOT / 'stock' / '__schema_gate_probe__.parquet'
    with pytest.raises(QuoteSchemaError, match='多列'):
        assert_formal_quote_schema(formal, _stock_frame(basis='x'))
    assert not formal.exists()


@pytest.mark.parametrize('data_type', sorted(QUOTE_COLUMNS))
def test_formal_store_schema_is_uniform(data_type: str) -> None:
    directory = FORMAL_QUOTES_ROOT / data_type
    if not directory.is_dir():
        pytest.skip(f'本地库缺失 {directory}')
    files = sorted(directory.rglob('*.parquet'))
    if not files:
        pytest.skip(f'{directory} 为空')
    signatures = {tuple(pl.read_parquet_schema(p)) for p in files}
    assert len(signatures) == 1, (
        f'{data_type} 存在 {len(signatures)} 种 schema：'
        f'{[list(sig) for sig in signatures]}')
    assert next(iter(signatures)) == QUOTE_COLUMNS[data_type]
