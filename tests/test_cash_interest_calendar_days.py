"""现金计息按自然日间隔计提（2026-11-24 回归）。

原实现每个交易日计 ``rate/365``，周末/节假日不计息，1.5% 年化实得约 1.04%。
"""
import pandas as pd
import polars as pl
import pytest

from src.backtest.engine.portfolio_backtest_engine import PortfolioBacktestEngine
from src.backtest.strategies.base_strategy import BaseStrategy


class _Idle(BaseStrategy):
    def __init__(self) -> None:
        super().__init__('idle')

    def generate_signal(self, row, index):  # noqa: ANN001, ANN201
        return None

    def generate_signals_vectorized(self, df: pl.DataFrame) -> pl.Series:
        return pl.Series('signal', [None] * df.height, dtype=pl.Object)


def _panel(dates) -> dict:
    n = len(dates)
    return {'600000.SH': pl.DataFrame({
        'date': dates, 'open': [10.0] * n, 'high': [10.0] * n, 'low': [10.0] * n,
        'close': [10.0] * n, 'volume': [1_000_000.0] * n})}


@pytest.mark.parametrize('pooled', [True, False])
def test_interest_accrues_by_calendar_days(pooled: bool) -> None:
    dates = pd.date_range('2023-01-02', '2023-12-29', freq='B')  # 一年交易日（无节假日）
    engine = PortfolioBacktestEngine(_panel(dates))
    engine.set_strategies({'600000.SH': _Idle()})
    capital = 1_000_000.0
    result = engine.run_backtest(initial_capital=capital, pooled_cash=pooled,
                                 cash_annual_rate=0.015)
    days = (dates[-1] - dates[0]).days
    expected = capital * (1 + 0.015 / 365) ** days
    assert result['final_equity'] == pytest.approx(expected, rel=1e-6)
    # 旧口径（每交易日 1/365）会落在约 1.04% 年化，与新口径相差约 0.46pp
    assert result['final_equity'] / capital - 1 > 0.0145


def test_zero_rate_unchanged() -> None:
    dates = pd.date_range('2023-01-02', periods=30, freq='B')
    engine = PortfolioBacktestEngine(_panel(dates))
    engine.set_strategies({'600000.SH': _Idle()})
    result = engine.run_backtest(initial_capital=1_000_000.0, pooled_cash=True)
    assert result['final_equity'] == pytest.approx(1_000_000.0)
