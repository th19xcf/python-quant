"""``_quote_panel`` 日期映射回归：非交易日 bar 不得顶到下一个交易日。"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.build_universe_layers import _quote_panel  # noqa: E402


def _store(tmp_path: Path, days: list[date], volumes: list[float],
           amounts: list[float]) -> Path:
    root = tmp_path / 'store'
    target = root / 'quotes' / 'stock' / 'sz' / '000001.SZ.parquet'
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        'date': pl.Series(days, dtype=pl.Date),
        'volume': pl.Series(volumes, dtype=pl.Float64),
        'amount': pl.Series(amounts, dtype=pl.Float64),
    }).write_parquet(target)
    return root


def test_non_trading_day_bar_is_dropped_not_shifted(tmp_path: Path) -> None:
    """周日 bar（volume>0）不能被顶到周一，周一的零成交必须保留为 traded=False。"""
    root = _store(
        tmp_path,
        [date(1991, 9, 27), date(1991, 9, 28), date(1991, 9, 29), date(1991, 9, 30)],
        [10.0, 20.0, 30.0, 0.0],
        [1000.0, 2000.0, 3000.0, 0.0],
    )
    dates = [date(1991, 9, 27), date(1991, 9, 30)]
    panel = _quote_panel(root, dates, {'000001.SZ'}, date(1991, 9, 30)).sort('cal_idx')
    assert panel.height == 2, panel
    assert panel['cal_idx'].to_list() == [0, 1]
    assert panel['traded'].to_list() == [True, False]
    assert panel['amount'].to_list() == [1000.0, 0.0]


def test_non_trading_day_bar_without_next_bar_leaves_no_row(tmp_path: Path) -> None:
    """只有周日 bar、下个交易日无 bar 时，该交易日不得凭空出现一条成交记录。"""
    root = _store(
        tmp_path,
        [date(1991, 9, 27), date(1991, 9, 29)],
        [10.0, 30.0],
        [1000.0, 3000.0],
    )
    dates = [date(1991, 9, 27), date(1991, 9, 30)]
    panel = _quote_panel(root, dates, {'000001.SZ'}, date(1991, 9, 30))
    assert panel.height == 1
    assert panel['cal_idx'].to_list() == [0]


def test_bar_after_calendar_end_is_dropped(tmp_path: Path) -> None:
    """超出日历末日的 bar（searchsorted 返回 n）不得产生越界 cal_idx。"""
    root = _store(
        tmp_path,
        [date(1991, 9, 27), date(1991, 10, 8)],
        [10.0, 20.0],
        [1000.0, 2000.0],
    )
    dates = [date(1991, 9, 27), date(1991, 9, 30)]
    panel = _quote_panel(root, dates, {'000001.SZ'}, date(1991, 10, 8))
    assert panel.height == 1
    assert panel['cal_idx'].max() < len(dates)
