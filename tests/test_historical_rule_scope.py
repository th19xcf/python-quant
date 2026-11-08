"""1996 年历史口径与量纲带宽的契约规则回归测试。

2026-11-07 第二轮收口：1991-1995 深市有周末交易、volume 单位非「手」，
用现代规则扫这段历史会产生约 7,316 条结构性误报；同时收盘价只是全天
均价代理，ratio 在 1 附近的偏差不是数据错误。本文件锁住这两条口径。
"""

from __future__ import annotations

from datetime import date

import polars as pl

from src.data.quality.rules import (
    AMOUNT_DIMENSION_BAND,
    HISTORICAL_RULE_START,
    RuleContext,
    rule_amount_dimension,
    rule_weekend_trade_date,
)


def _quote(d: date, close: float = 10.0, volume: float = 1000.0,
           amount: float | None = None) -> dict[str, object]:
    if amount is None:
        amount = close * volume * 100
    return {
        'date': d,
        'open': close, 'high': close, 'low': close, 'close': close,
        'volume': volume, 'amount': amount,
    }


def test_historical_start_is_1996() -> None:
    assert HISTORICAL_RULE_START == date(1996, 1, 1)
    assert AMOUNT_DIMENSION_BAND >= 10.0


def test_weekend_rows_before_1996_are_not_violations() -> None:
    # 1991-04-06 是周六：早年深市确有交易，属历史事实而非脏数据
    frame = pl.DataFrame([
        _quote(date(1991, 4, 6)),
        _quote(date(1993, 5, 1)),
    ])
    assert rule_weekend_trade_date(frame, RuleContext()).height == 0


def test_weekend_rows_after_1996_are_violations() -> None:
    # 2026-11-07 是周六：现代 A 股不应有交易
    frame = pl.DataFrame([
        _quote(date(2026, 11, 6)),   # 周五
        _quote(date(2026, 11, 7)),   # 周六
    ])
    bad = rule_weekend_trade_date(frame, RuleContext())
    assert bad.height == 1
    assert bad['trade_date'][0] == date(2026, 11, 7)


def test_amount_dimension_ignores_pre_1996_rows() -> None:
    frame = pl.DataFrame([
        _quote(date(1991, 1, 29), amount=1.0),   # 早年单位口径，ratio 极小
        _quote(date(1995, 11, 17), amount=1.0e9),  # 早年 ratio 极大
    ])
    assert rule_amount_dimension(frame, RuleContext(data_type='stock')).height == 0


def test_amount_dimension_only_flags_order_of_magnitude_shift() -> None:
    ctx = RuleContext(data_type='stock')
    # ratio 1.3 / 0.7：收盘价代理均价的固有误差，不判违规
    near_one = pl.DataFrame([
        _quote(date(2026, 5, 20), amount=1.3 * 10.0 * 1000 * 100),
        _quote(date(2026, 5, 21), amount=0.7 * 10.0 * 1000 * 100),
    ])
    assert rule_amount_dimension(near_one, ctx).height == 0

    # ratio≈100（volume 少写 100 倍）与 ratio≈0.01（volume 单位变成股），
    # 两个方向的量纲错位都必须报出来
    shifted = pl.DataFrame([
        _quote(date(2026, 5, 22), volume=10.0, amount=10.0 * 1000 * 100),
        _quote(date(2026, 5, 25), volume=100000.0, amount=10.0 * 1000 * 100),
    ])
    assert rule_amount_dimension(shifted, ctx).height == 2


def test_amount_dimension_skips_non_stock() -> None:
    frame = pl.DataFrame([_quote(date(2026, 5, 22), amount=1.0)])
    assert rule_amount_dimension(frame, RuleContext(data_type='fund')).height == 0
