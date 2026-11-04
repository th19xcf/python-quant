"""基金份额折算修复回归测试。

锁定三件事：
1. 1:2 / 1:2.5 / 1:1.28775 等官方拆分比例下，拆分日复权收益 ≈ 0，且方向正确
   （拆分前 qfq = 1/比例，拆分后 = 1）；
2. 行情源已前复权的基金（``QUOTE_SPLIT_ADJUSTED_CODES``）不再叠加折算因子，
   复权收益等于原始收益，不产生伪跳变；
3. 修复工具内置的缺事件比例与交易所公告一致。
"""

from datetime import date, datetime

import polars as pl
import pytest

from src.data.adj_factor_calculator import AdjFactorCalculator
from src.data.local_store.fund_factors import (
    QUOTE_SPLIT_ADJUSTED_CODES,
    prepare_events_for_code,
)

_DATES = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5)]


def _split_frame(ts_code: str, ex_date: date, ratio: float) -> pl.DataFrame:
    return pl.DataFrame({
        'ts_code': [ts_code],
        'ex_date': [ex_date],
        'split_type': ['份额拆分'],
        'share_ratio': [ratio],
        'source': ['test'],
        'fetched_at': [datetime(2026, 11, 4)],
    }, schema={
        'ts_code': pl.Utf8, 'ex_date': pl.Date, 'split_type': pl.Utf8,
        'share_ratio': pl.Float64, 'source': pl.Utf8, 'fetched_at': pl.Datetime,
    })


def _factors(ts_code: str, closes: list[float], events: pl.DataFrame) -> pl.DataFrame:
    prices = pl.DataFrame({
        'ts_code': [ts_code] * len(_DATES),
        'trade_date': _DATES,
        'close': closes,
    })
    factors = AdjFactorCalculator.calculate_factors(prices, events, max_factor=1000.0)
    return prices.join(factors, on=['ts_code', 'trade_date']).sort('trade_date')


@pytest.mark.parametrize('ratio', [2.0, 2.5, 1.28775])
def test_official_split_ratio_yields_near_zero_return(ratio: float):
    """官方比例下拆分日复权收益 ≈ 0，且拆分前因子 = 1/ratio（方向正确）。"""
    ts_code = '510500.SH'
    prior_close = 2.373
    post_close = prior_close / ratio  # 真实收益为 0 的理论除权价
    events = prepare_events_for_code(
        ts_code, _DATES, None, _split_frame(ts_code, date(2024, 1, 3), ratio))
    merged = _factors(ts_code, [2.35, prior_close, post_close, post_close], events)
    adjusted = merged['close'].cast(pl.Float64) * merged['qfq_factor'].cast(pl.Float64)
    event_return = adjusted[2] / adjusted[1] - 1.0
    assert event_return == pytest.approx(0.0, abs=1e-9)
    assert merged['qfq_factor'][1] == pytest.approx(1.0 / ratio)
    assert merged['qfq_factor'][2] == pytest.approx(1.0)


def test_already_adjusted_code_skips_split_event():
    """行情源已前复权的基金不叠加因子：复权收益 = 原始收益。"""
    ts_code = '159930.SZ'
    assert ts_code in QUOTE_SPLIT_ADJUSTED_CODES
    events = prepare_events_for_code(
        ts_code, _DATES, None, _split_frame(ts_code, date(2024, 1, 3), 0.4074))
    assert events.height == 0
    merged = _factors(ts_code, [0.967, 0.948, 0.945, 0.950], events)
    adjusted = merged['close'].cast(pl.Float64) * merged['qfq_factor'].cast(pl.Float64)
    assert merged['qfq_factor'].to_list() == [1.0, 1.0, 1.0, 1.0]
    event_return = adjusted[2] / adjusted[1] - 1.0
    assert event_return == pytest.approx(0.945 / 0.948 - 1.0, abs=1e-12)


def test_repair_tool_ratios_match_announcements():
    """修复工具内置的缺事件比例必须与交易所公告一致。"""
    from tools.fund_split_repair import MISSING_SPLIT_EVENTS

    expected = {
        '512890.SH': 2.0,
        '515260.SH': 2.0,
        '561380.SH': 2.5,
        '510950.SH': 1.28775,
    }
    actual = {row['ts_code']: row['share_ratio'] for row in MISSING_SPLIT_EVENTS}
    assert actual == expected
    for row in MISSING_SPLIT_EVENTS:
        assert row['ex_date'] == {
            '512890.SH': date(2021, 10, 22),
            '515260.SH': date(2025, 9, 12),
            '561380.SH': date(2026, 6, 24),
            '510950.SH': date(2026, 4, 24),
        }[row['ts_code']]
        assert row['source']
