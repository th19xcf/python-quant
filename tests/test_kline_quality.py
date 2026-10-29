"""K 线质量校验与交易日缺口检测的单元测试"""
from datetime import date

import polars as pl

from src.data.managers.data_processor import find_missing_trade_dates
from src.data.sources.base import DataSourceBase


def _kline_df() -> pl.DataFrame:
    """构造含正常行、停牌日、非法 OHLC、错误 pct_chg 的 K 线"""
    return pl.DataFrame({
        'date': ['2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'],
        'open': [10.0, 0.0, 7.0, 1.5],
        'high': [11.0, 0.0, 5.0, 2.5],
        'low': [9.0, 0.0, 10.0, 1.4],
        'close': [10.5, 0.0, 6.0, 2.0],
        'pre_close': [10.2, 10.5, 6.2, 1.0],
        'volume': [1000.0, 0.0, 800.0, 900.0],
        'amount': [10000.0, 0.0, 8000.0, 9000.0],
        'change': [0.3, 0.0, -0.2, 999.0],
        'pct_chg': [2.94, 0.0, -3.23, 999.0],
    })


def test_validate_kline_drops_invalid_ohlc_and_keeps_suspended():
    result = DataSourceBase._validate_kline_quality(_kline_df())

    # 停牌日（volume=0）保留，非法 OHLC 行（low > high）剔除
    assert result.height == 3
    assert result['date'].to_list() == ['2024-01-02', '2024-01-03', '2024-01-05']


def test_validate_kline_recomputes_pct_chg_from_pre_close():
    result = DataSourceBase._validate_kline_quality(_kline_df())
    last = result.filter(pl.col('date') == '2024-01-05')

    # 上游写入的 999 被按 close/pre_close 重算为 100%
    assert abs(last['pct_chg'][0] - 100.0) < 1e-9
    assert abs(last['change'][0] - 1.0) < 1e-9


def test_validate_kline_keeps_suspended_pct_chg_untouched():
    result = DataSourceBase._validate_kline_quality(_kline_df())
    suspended = result.filter(pl.col('date') == '2024-01-03')

    # 停牌日不参与重算，避免 close=0 时算出 -100%
    assert suspended['pct_chg'][0] == 0.0


def test_validate_kline_passthrough_when_columns_missing():
    df = pl.DataFrame({'date': ['2024-01-02'], 'close': [10.0]})
    result = DataSourceBase._validate_kline_quality(df)
    assert result.height == 1


def test_find_missing_trade_dates():
    expected = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]
    actual = [date(2024, 1, 2), date(2024, 1, 4)]

    assert find_missing_trade_dates(expected, actual) == [date(2024, 1, 3)]


def test_find_missing_trade_dates_no_gap():
    expected = [date(2024, 1, 2), date(2024, 1, 3)]
    assert find_missing_trade_dates(expected, expected) == []
