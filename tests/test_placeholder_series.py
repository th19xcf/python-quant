#!/usr/bin/env python3

"""基金「占位桩」行情的识别、台账与隔离回归测试。

背景（2026-11-07 发现）：通达信对已退市/转型/长期无成交的 LOF，用
``close=1.0, volume=0, amount=0`` 表示「该日无行情」。本地忠实复制，
全库 232 只基金合计 22,085 行，其中 84 只整只全假。

这批行能通过原有全部 13 条契约规则（close 是正数、OHLC 包含关系成立），
属规则盲区；而 ``--data-type`` 当时只支持 stock/index，基金根本不在巡检
范围。本文件锁住修复后的口径：

1. 判据必须同时包含 ``volume == 0``，避免把新发基金面值 1 元误判；
2. 连续段按段切分，头部/中段/尾部/全序列四种形态都要识别；
3. 池级剔除只认「整只全假」，局部占位走行级，避免误伤真实行情。
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import polars as pl

from src.data.quality import anomaly_lists, quarantine
from src.data.quality.rules import RuleContext, rule_placeholder_bar

_D = dt.date


def _rows(spec: list[tuple[float, float, float]]) -> pl.DataFrame:
    """按 (close, volume, amount) 生成连续交易日行情。"""
    base = _D(2024, 1, 1)
    return pl.DataFrame({
        'date': [base + dt.timedelta(days=i) for i in range(len(spec))],
        'open': [c for c, _, _ in spec],
        'high': [c for c, _, _ in spec],
        'low': [c for c, _, _ in spec],
        'close': [c for c, _, _ in spec],
        'volume': [v for _, v, _ in spec],
        'amount': [a for _, _, a in spec],
    })


_REAL = (10.0, 1000.0, 1.0e6)
_FAKE = (1.0, 0.0, 0.0)
#: 真实成交但净值恰为 1.0 的新发基金：有成交量，不得判为占位
_REAL_ONE = (1.0, 500.0, 5.0e5)


def test_rule_flags_only_segment_start() -> None:
    # 真实 2 行 → 3 行占位 → 真实 2 行：应只在第 3 行报 1 条
    frame = _rows([_REAL, _REAL, _FAKE, _FAKE, _FAKE, _REAL, _REAL])
    bad = rule_placeholder_bar(frame, RuleContext(data_type='fund'))
    assert bad.height == 1
    assert bad['trade_date'][0] == _D(2024, 1, 3)


def test_rule_reports_each_disjoint_segment() -> None:
    frame = _rows([_FAKE, _REAL, _FAKE, _REAL, _FAKE])
    bad = rule_placeholder_bar(frame, RuleContext(data_type='fund'))
    assert bad.height == 3
    assert bad['trade_date'].to_list() == [_D(2024, 1, 1), _D(2024, 1, 3),
                                           _D(2024, 1, 5)]


def test_rule_requires_zero_volume_not_just_close_one() -> None:
    """新发基金面值恰为 1 元但有成交，不能当成占位桩。"""
    frame = _rows([_REAL_ONE, _REAL_ONE])
    assert rule_placeholder_bar(frame, RuleContext(data_type='fund')).is_empty()


def test_rule_accepts_unordered_frame() -> None:
    ordered = _rows([_REAL, _FAKE, _FAKE, _REAL])
    shuffled = ordered.reverse()
    bad = rule_placeholder_bar(shuffled, RuleContext(data_type='fund'))
    assert bad.height == 1
    assert bad['trade_date'][0] == _D(2024, 1, 2)


def _write_ledger(root: Path, frame: pl.DataFrame) -> None:
    (root / 'quotes' / 'fund' / 'sz').mkdir(parents=True)
    frame.write_parquet(root / 'quotes' / 'fund' / 'sz' / '160603.SZ.parquet')


def test_builder_splits_head_middle_and_full(tmp_path: Path) -> None:
    # 头部连续假（上市初期无净值）
    _write_ledger(tmp_path / 'head',
                  _rows([_FAKE] * 3 + [_REAL] * 5))
    head = anomaly_lists._build_placeholder_quote(tmp_path / 'head')
    assert head is not None and head.height == 1
    assert head['rows'][0] == 3
    assert head['total'][0] == 8
    assert head['full'][0] is False

    # 中段分散假：旧判据只看首尾，会整段漏掉
    _write_ledger(tmp_path / 'mid',
                  _rows([_REAL] * 2 + [_FAKE] * 2 + [_REAL] * 2 + [_FAKE] * 2))
    mid = anomaly_lists._build_placeholder_quote(tmp_path / 'mid')
    assert mid is not None and mid.height == 2
    assert mid['rows'].to_list() == [2, 2]
    assert mid['full'].to_list() == [False, False]

    # 整只全假：full=True，才允许池级剔除
    _write_ledger(tmp_path / 'full', _rows([_FAKE] * 4))
    full = anomaly_lists._build_placeholder_quote(tmp_path / 'full')
    assert full is not None and full.height == 1
    assert full['full'][0] is True
    assert full['rows'][0] == 4 and full['total'][0] == 4


def test_placeholder_codes_only_returns_full_series(tmp_path: Path) -> None:
    (tmp_path / 'meta' / 'anomalies').mkdir(parents=True)
    pl.DataFrame({
        'ts_code': ['163803.SZ', '160105.SZ', '160603.SZ'],
        'start': ['2006-02-13', '2004-08-24', '2004-11-03'],
        'end': ['2026-11-06', '2004-09-29', '2004-12-29'],
        'rows': [1468, 27, 40],
        'total': [1468, 5100, 1894],
        'full': [True, False, False],
    }).write_csv(tmp_path / 'meta' / 'anomalies'
                 / 'placeholder_quote_codes.csv')

    # 只有整只全假的才进池级剔除；头部/中段占位不剔除整只
    assert quarantine.placeholder_codes(tmp_path) == frozenset({'163803.SZ'})

    dates = quarantine.placeholder_ranges(tmp_path)
    # 行级清单覆盖全部区间（含区间内每个日历日）
    assert ('163803.SZ', _D(2006, 2, 13)) in dates
    assert ('160105.SZ', _D(2004, 8, 24)) in dates
    assert ('160105.SZ', _D(2004, 8, 25)) in dates
    # 区间外的日期不登记
    assert ('160105.SZ', _D(2004, 9, 30)) not in dates


def test_placeholder_codes_fail_open_on_missing_ledger(tmp_path: Path) -> None:
    assert quarantine.placeholder_codes(tmp_path) == frozenset()
    assert quarantine.placeholder_ranges(tmp_path) == frozenset()


def test_placeholder_codes_reads_legacy_ledger(tmp_path: Path) -> None:
    """旧版清单没有 full 列，按原语义（登记过的都算整只）读取。"""
    (tmp_path / 'meta' / 'anomalies').mkdir(parents=True)
    pl.DataFrame({
        'ts_code': ['163803.SZ', '160603.SZ'],
        'start': ['2006-02-13', '2004-11-03'],
        'end': ['2026-11-06', '2004-12-29'],
    }).write_csv(tmp_path / 'meta' / 'anomalies'
                 / 'placeholder_quote_codes.csv')
    assert quarantine.placeholder_codes(tmp_path) == frozenset(
        {'163803.SZ', '160603.SZ'})
