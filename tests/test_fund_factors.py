"""基金分红/份额折算事件与复权因子回归测试。

覆盖：
1. 事件日对齐（ETF 分红当日、LOF 分红次日、份额折算次日）
2. 事件因子公式与个股口径一致，且反向折算不被上限丢弃
3. 因子 → 复权价 → 反推原始价的可逆性
4. LocalFactorStore 对基金因子/分红文件的回退路由
"""

from datetime import date, datetime

import polars as pl
import pytest

from src.data.adj_factor_calculator import (
    AdjFactorCalculator,
    calc_dividend_event_factor,
)
from src.data.local_store.fund_factors import (
    _parse_split_ratio,
    fund_ts_code,
    is_etf_code,
    prepare_events_for_code,
)
from src.data.local_store.store import LocalFactorStore

_DATES = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5)]


def _dividend_frame(ts_code: str, ex_date: date, cash: float) -> pl.DataFrame:
    return pl.DataFrame({
        'ts_code': [ts_code],
        'ex_date': [ex_date],
        'record_date': [ex_date],
        'cash_amount': [cash],
        'source': ['test'],
        'fetched_at': [datetime(2026, 11, 4)],
    }, schema={
        'ts_code': pl.Utf8, 'ex_date': pl.Date, 'record_date': pl.Date,
        'cash_amount': pl.Float64, 'source': pl.Utf8, 'fetched_at': pl.Datetime,
    })


def _split_frame(ts_code: str, ex_date: date, ratio: float) -> pl.DataFrame:
    return pl.DataFrame({
        'ts_code': [ts_code],
        'ex_date': [ex_date],
        'split_type': ['份额折算'],
        'share_ratio': [ratio],
        'source': ['test'],
        'fetched_at': [datetime(2026, 11, 4)],
    }, schema={
        'ts_code': pl.Utf8, 'ex_date': pl.Date, 'split_type': pl.Utf8,
        'share_ratio': pl.Float64, 'source': pl.Utf8, 'fetched_at': pl.Datetime,
    })


def test_fund_ts_code_and_etf_prefix():
    assert fund_ts_code('510880') == '510880.SH'
    assert fund_ts_code('159915') == '159915.SZ'
    assert fund_ts_code('161907') == '161907.SZ'
    assert is_etf_code('510050.SH')
    assert is_etf_code('159915.SZ')
    assert not is_etf_code('161907.SZ')
    assert not is_etf_code('508000.SH')


def test_parse_split_ratio():
    assert _parse_split_ratio('1:0.2803') == pytest.approx(0.2803)
    assert _parse_split_ratio('1:1.1454') == pytest.approx(1.1454)
    assert _parse_split_ratio('0.0100') == pytest.approx(0.01)
    assert _parse_split_ratio('') is None
    assert _parse_split_ratio('abc') is None


def test_etf_dividend_applies_on_ex_date():
    events = prepare_events_for_code(
        '510880.SH', _DATES, _dividend_frame('510880.SH', date(2024, 1, 3), 0.1), None)
    assert events.height == 1
    assert events['ex_date'][0] == date(2024, 1, 3)
    assert events['cash_div'][0] == pytest.approx(0.1)


def test_non_etf_dividend_shifts_to_next_trade_date():
    events = prepare_events_for_code(
        '161907.SZ', _DATES, _dividend_frame('161907.SZ', date(2024, 1, 3), 0.2), None)
    assert events.height == 1
    assert events['ex_date'][0] == date(2024, 1, 4)


def test_share_change_shifts_and_maps_to_share_div():
    events = prepare_events_for_code(
        '510500.SH', _DATES, None, _split_frame('510500.SH', date(2024, 1, 3), 0.5))
    assert events.height == 1
    assert events['ex_date'][0] == date(2024, 1, 4)
    assert events['share_div'][0] == pytest.approx(-0.5)


def test_event_factor_matches_stock_semantics():
    assert calc_dividend_event_factor(10.0, 1.0, 0.0) == pytest.approx(0.9)
    assert calc_dividend_event_factor(
        10.0, 0.0, 0.2803 - 1.0) == pytest.approx(10.0 / (10.0 * 0.2803))


def test_reverse_split_factor_not_dropped():
    # 1:0.01 反向折算因子为 100，个股默认上限 10 会丢弃；基金路径放宽到 1000。
    factor = calc_dividend_event_factor(
        1.0, 0.0, 0.01 - 1.0, max_factor=1000.0)
    assert factor == pytest.approx(100.0)


def test_factors_are_reversible():
    prices = pl.DataFrame({
        'ts_code': ['510880.SH'] * 3,
        'trade_date': [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)],
        'close': [10.0, 11.0, 11.5],
    })
    events = pl.DataFrame({
        'ts_code': ['510880.SH'],
        'ex_date': [date(2024, 1, 3)],
        'cash_div': [1.0],
        'share_div': [0.0],
        'rights_issue_ratio': [0.0],
        'rights_issue_price': [0.0],
    })
    factors = AdjFactorCalculator.calculate_factors(
        prices, events, max_factor=1000.0)
    merged = prices.join(factors, on=['ts_code', 'trade_date']).sort('trade_date')

    assert merged['hfq_factor'][1] == pytest.approx(1.0 / 0.9)
    assert merged['qfq_factor'][0] == pytest.approx(0.9)

    raw = merged['close'].cast(pl.Float64)
    hfq_close = raw * merged['hfq_factor'].cast(pl.Float64)
    restored = hfq_close / merged['hfq_factor'].cast(pl.Float64)
    error = float(((restored - raw).abs() / raw.abs()).max() or 0.0)
    assert error <= 1e-12


def test_fund_factor_store_fallback(tmp_path):
    factors_dir = tmp_path / 'factors'
    factors_dir.mkdir()
    pl.DataFrame({
        'ts_code': ['510880.SH', '510880.SH'],
        'trade_date': [date(2024, 1, 2), date(2024, 1, 3)],
        'qfq_factor': [0.9, 1.0],
        'hfq_factor': [1.0, 1.0 / 0.9],
    }).write_parquet(factors_dir / 'fund_adj_factor.parquet')
    pl.DataFrame({
        'ts_code': ['510880.SH'],
        'ex_date': [date(2024, 1, 3)],
        'record_date': [date(2024, 1, 3)],
        'cash_amount': [0.1],
        'source': ['test'],
        'fetched_at': [datetime(2026, 11, 4)],
    }).write_parquet(factors_dir / 'fund_dividend.parquet')

    store = LocalFactorStore(tmp_path, enabled=True)
    factors = store.factors('510880.SH')
    assert factors is not None
    assert factors.height == 2
    dividends = store.dividends('510880.SH')
    assert dividends is not None
    assert dividends['cash_amount'][0] == pytest.approx(0.1)
