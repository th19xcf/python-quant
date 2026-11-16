"""区间收益口径：快照之间必须是区间累计收益，不是快照日当天的单日收益。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from src.quant.analysis import attribution_local as mod


def _write_quotes(base: Path) -> None:
    target = base / 'quotes' / 'stock' / '2020'
    target.mkdir(parents=True)
    rows = []
    for code, closes in {
        'A': [10.0, 11.0, 12.0, 15.0],
        'B': [20.0, 18.0, 19.0, 19.0],
    }.items():
        for day, close in zip(
                (date(2020, 1, 2), date(2020, 1, 15),
                 date(2020, 1, 31), date(2020, 2, 28)),
                closes, strict=False):
            rows.append({'date': day, 'ts_code': code,
                         'close': close, 'hfq_factor': 1.0})
    pl.DataFrame(rows).write_parquet(target / 'quotes.parquet')


def test_period_returns_cover_whole_interval(tmp_path):
    """跨月区间必须取首尾快照价，中间交易日大涨不能被漏掉。"""
    _write_quotes(tmp_path)
    pairs = [('2020-01-02', '2020-01-31'), ('2020-01-31', '2020-02-28')]
    got = mod._period_returns(tmp_path, pairs)

    assert got[pairs[0]]['A'] == pytest.approx(12.0 / 10.0 - 1.0)
    assert got[pairs[1]]['A'] == pytest.approx(15.0 / 12.0 - 1.0)
    # 旧的单日口径会拿 1 月 15 日当基准（15/11-1），与区间口径明显不同
    assert got[pairs[1]]['A'] != pytest.approx(15.0 / 11.0 - 1.0)


def test_period_returns_telescope_to_direct_interval(tmp_path):
    """逐期区间收益连乘必须等于直接首尾区间收益（望远镜恒等）。"""
    _write_quotes(tmp_path)
    pairs = [('2020-01-02', '2020-01-31'), ('2020-01-31', '2020-02-28')]
    got = mod._period_returns(tmp_path, pairs)
    direct = mod._period_returns(tmp_path, [('2020-01-02', '2020-02-28')])

    chained = 1.0
    for pair in pairs:
        chained *= 1.0 + got[pair]['A']
    assert chained == pytest.approx(1.0 + direct[('2020-01-02', '2020-02-28')]['A'])
    assert chained == pytest.approx(15.0 / 10.0)
