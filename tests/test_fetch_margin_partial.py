"""两融明细「部分交易所」残缺日不得写入/记完成（2026-11-25 回归）。"""
import importlib.util
from datetime import date
from pathlib import Path

import polars as pl
import pytest

_SPEC = importlib.util.spec_from_file_location(
    'fetch_margin_detail', Path(__file__).resolve().parents[1] / 'tools' / 'fetch_margin_detail.py')
fmd = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(fmd)


def _frame(sh: int, sz: int, bj: int) -> pl.DataFrame:
    codes = ([f'{600000 + i}.SH' for i in range(sh)] + [f'{i:06d}.SZ' for i in range(sz)]
             + [f'{830000 + i}.BJ' for i in range(bj)])
    return pl.DataFrame({'ts_code': codes})


@pytest.fixture()
def raw_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(fmd, 'RAW_DIR', tmp_path)
    _frame(2000, 2100, 350).write_parquet(tmp_path / '20261119.parquet')
    return tmp_path


def test_missing_exchange_raises_and_drops_cache(raw_dir):
    day = date(2026, 11, 20)
    partial = _frame(2019, 0, 0)
    partial.write_parquet(raw_dir / '20261120.parquet')
    with pytest.raises(fmd.SourcePartial, match='SZ/BJ'):
        fmd._check_partial(partial, day, {'2026-11-19'})
    assert not (raw_dir / '20261120.parquet').exists()


def test_row_drop_raises(raw_dir):
    with pytest.raises(fmd.SourcePartial, match='行数'):
        fmd._check_partial(_frame(1000, 1000, 300), date(2026, 11, 20), {'2026-11-19'})


def test_complete_day_passes(raw_dir):
    fmd._check_partial(_frame(2019, 2119, 353), date(2026, 11, 20), {'2026-11-19'})


def test_no_reference_passes(raw_dir):
    fmd._check_partial(_frame(10, 0, 0), date(2010, 4, 1), set())
