#!/usr/bin/env python3

"""
向量化策略接口等价性回归测试。

锁死两条硬约束：
    1. generate_signals_vectorized 与逐行 generate_signal 对同一数据
       必须逐 bar 产出完全一致的信号（防止双实现漂移）；
    2. 引擎的向量化查表路径与鸭子类型策略的逐行适配路径必须产出
       完全一致的成交与权益曲线（防止撮合语义分流）。
"""

import numpy as np
import pandas as pd
import polars as pl
import pytest

from src.backtest.engine.backtest_engine import BacktestEngine
from src.backtest.strategies.base_strategy import BaseStrategy
from src.backtest.strategies.macd_strategy import MACDStrategy
from src.backtest.strategies.ma_strategy import MAStrategy
from src.backtest.strategies.mean_reversion_strategy import MeanReversionStrategy
from src.backtest.strategies.multi_factor_strategy import MultiFactorStrategy
from src.backtest.strategies.trend_following_strategy import TrendFollowingStrategy


def _make_df(n=120) -> pl.DataFrame:
    """构造覆盖关键分支的测试数据：null 暖机、相等比较、RSI 边界值。"""
    rng = np.random.default_rng(7)

    # 前 60 根随机游走，后 60 根强趋势上行：
    # 保证趋势段 ma_10 > ma_20 稳定成立（多因子 ma 贡献 +1）
    close = list(100.0 * np.cumprod(1 + rng.normal(0, 0.01, 60)))
    close.extend(close[-1] + 0.5 * (i + 1) for i in range(n - 60))

    ma5 = [None if i < 4 else float(np.mean(close[i - 4:i + 1])) for i in range(n)]
    ma10 = [None if i < 9 else float(np.mean(close[i - 9:i + 1])) for i in range(n)]
    ma20 = [None if i < 19 else float(np.mean(close[i - 19:i + 1])) for i in range(n)]

    # 周期性相等（i % 10 == 3 时 macd == signal，覆盖相等 → hold 分支）
    macd = [round(float(np.sin(i / 5.0)), 6) for i in range(n)]
    macd_signal = [
        macd[i] if i % 10 == 3 else round(float(np.sin(i / 5.0 + 0.37)), 6)
        for i in range(n)
    ]

    rsi = []
    for i in range(n):
        if i < 14:
            rsi.append(None)          # Wilder 暖机，前 period-1 根为 null
        elif i % 25 == 0:
            rsi.append(30.0)          # 边界：rsi == oversold → hold（严格小于才买）
        elif i % 25 == 12:
            rsi.append(70.0)          # 边界：rsi == overbought → hold
        else:
            rsi.append(float(rng.uniform(5, 95)))

    return pl.DataFrame({
        'date': pd.date_range('2023-01-02', periods=n, freq='B'),
        'close': close,
        'ma_5': ma5,
        'ma_10': ma10,
        'ma_20': ma20,
        'macd': macd,
        'macd_signal': macd_signal,
        'rsi': rsi,
    })


def _rowwise(strategy, df: pl.DataFrame) -> list:
    """逐行参考实现：与旧引擎路径完全一致的调用方式。"""
    cols = {name: df[name].to_list() for name in df.columns}
    return [
        strategy.generate_signal({k: v[i] for k, v in cols.items()}, i)
        for i in range(df.height)
    ]


STRATEGIES = [
    MAStrategy(),
    TrendFollowingStrategy(),
    MACDStrategy(),
    MeanReversionStrategy(),
    MultiFactorStrategy(),
    MultiFactorStrategy(weights={'ma': 0.2, 'macd': 0.5, 'rsi': 0.3}, threshold=0.4),
]


@pytest.mark.parametrize('strategy', STRATEGIES, ids=lambda s: type(s).__name__)
def test_vectorized_matches_rowwise(strategy):
    """向量化接口与逐行接口必须逐 bar 一致，且信号非退化。"""
    df = _make_df()
    vec = strategy.generate_signals_vectorized(df).to_list()
    row = _rowwise(strategy, df)

    assert vec == row
    assert set(vec) <= {'buy', 'sell', 'hold'}
    # 非退化：测试数据必须触发至少一次交易信号，防止比较逻辑整体失效
    assert 'buy' in vec and 'sell' in vec


def test_base_default_fallback_matches_rowwise():
    """不覆写向量化接口的策略走基类兜底，结果必须与逐行调用一致。"""

    class _FallbackStrategy(BaseStrategy):
        def __init__(self):
            super().__init__(name='fallback')

        def generate_signal(self, data, index):
            if index < 20:
                return 'hold'
            ma5 = data.get('ma_5')
            ma20 = data.get('ma_20')
            if ma5 is None or ma20 is None:
                return 'hold'
            if ma5 > ma20:
                return 'buy'
            if ma5 < ma20:
                return 'sell'
            return 'hold'

    df = _make_df()
    strategy = _FallbackStrategy()
    assert strategy.generate_signals_vectorized(df).to_list() == _rowwise(strategy, df)


@pytest.mark.parametrize('strategy', STRATEGIES, ids=lambda s: type(s).__name__)
def test_vectorized_on_empty_df(strategy):
    """空数据帧不崩溃，返回空信号序列。"""
    out = strategy.generate_signals_vectorized(_make_df().head(0))
    assert out.len() == 0
    assert out.dtype == pl.Utf8


class _RowOnlyMAStrategy:
    """不走策略基类的鸭子类型对象：触发引擎侧逐行适配路径。"""

    def __init__(self, short_window=5, long_window=20):
        self.name = 'RowOnlyMA'
        self.params = {'short_window': short_window, 'long_window': long_window}

    def generate_signal(self, data, index):
        short_ma = data.get(f"ma_{self.params['short_window']}")
        long_ma = data.get(f"ma_{self.params['long_window']}")
        if short_ma is None or long_ma is None:
            return 'hold'
        if short_ma > long_ma:
            return 'buy'
        if short_ma < long_ma:
            return 'sell'
        return 'hold'


def _oscillating(days=160):
    dates = pd.date_range('2023-01-02', periods=days, freq='B')
    close = 100.0 + 10.0 * np.sin(np.arange(days) / 10.0)
    return pl.DataFrame({
        'date': dates,
        'open': close * 1.0005,
        'high': close * 1.01,
        'low': close * 0.99,
        'close': close,
        'volume': np.full(days, 1_000_000.0),
    })


def test_engine_vectorized_path_equals_row_adapter_path():
    """引擎向量化查表路径与逐行适配路径必须产出完全一致的回测结果。"""
    vec_result = BacktestEngine(_oscillating())
    vec_result.set_strategy(MAStrategy(short_window=5, long_window=20))
    via_vectorized = vec_result.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')

    row_result = BacktestEngine(_oscillating())
    row_result.set_strategy(_RowOnlyMAStrategy(short_window=5, long_window=20))
    via_row_adapter = row_result.run_backtest(initial_capital=1_000_000.0, ts_code='600000.SH')

    assert via_row_adapter['trades'] == via_vectorized['trades']
    assert via_row_adapter['equity_curve'] == via_vectorized['equity_curve']
    assert via_row_adapter['final_equity'] == via_vectorized['final_equity']
    assert len(via_vectorized['trades']) > 0
