"""``_quote_panel`` 日期映射回归：非交易日 bar 不得顶到下一个交易日。"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.build_universe_layers import (  # noqa: E402
    _assert_orphan_bars,
    _quote_panel,
)


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


def test_dropped_bars_are_recorded_as_orphans(tmp_path: Path) -> None:
    """被丢弃的网格外 bar 必须逐行记入 orphans，而不是无声消失。"""
    root = _store(
        tmp_path,
        [date(1991, 9, 27), date(1991, 9, 29), date(1991, 9, 30)],
        [10.0, 30.0, 5.0],
        [1000.0, 3000.0, 500.0],
    )
    orphans: list[tuple[str, date]] = []
    _quote_panel(root, [date(1991, 9, 27), date(1991, 9, 30)],
                 {'000001.SZ'}, date(1991, 9, 30), orphans=orphans)
    assert orphans == [('000001.SZ', date(1991, 9, 29))]


def test_orphan_guard_rejects_weekday_bar() -> None:
    """网格外 bar 落在工作日 = 日历缺交易日，必须报错而不是丢弃。"""
    with pytest.raises(SystemExit) as excinfo:
        _assert_orphan_bars([('000001.SZ', date(1993, 5, 3))])
    assert '工作日' in str(excinfo.value)


def test_orphan_guard_rejects_unregistered_weekend_bar() -> None:
    """周末 bar 但未在 G-112 判定表登记，必须先裁定再构建。"""
    with pytest.raises(SystemExit) as excinfo:
        _assert_orphan_bars([('999999.SZ', date(1991, 4, 6))])
    assert '未在 G-112 判定表登记' in str(excinfo.value)


def test_orphan_guard_accepts_registered_weekend_bar() -> None:
    """已裁定的周末 bar 允许丢弃，但计数必须如实统计（keep 行 = 日历缺口代价）。"""
    stats = _assert_orphan_bars([('000001.SZ', date(1991, 4, 6))])
    assert stats['rows'] == 1
    assert stats['dates'] == ['1991-04-06']
    assert stats['keep_rows'] == 1
    assert stats['drop_rows'] == 0
