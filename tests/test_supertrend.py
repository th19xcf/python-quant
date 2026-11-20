#!/usr/bin/env python3

"""
Supertrend 指标测试

覆盖三件事：
1. ATR 种子行对齐（前 period-1 行为空、方向为 0）
2. 趋势翻转（下跌段为 -1、上涨段为 1，且趋势线取值随方向切换上下轨）
3. 与独立参考实现逐行等价（参考实现直接按 WildER 与 Pine 递推手写，不复用被测代码）

全部使用构造数据，不依赖数据库与网络。
"""

import datetime

import numpy as np
import polars as pl

from src.tech_analysis.indicators.formulas import supertrend


def _make_ohlc(close: np.ndarray) -> pl.DataFrame:
    """由收盘价构造 OHLC：振幅 1%、最高价略高于收盘、最低价略低于收盘"""
    close = np.asarray(close, dtype=float)
    high = close * 1.005
    low = close * 0.995
    return pl.DataFrame({'high': high, 'low': low, 'close': close})


def _ref_supertrend(df: pl.DataFrame, period: int = 10, multiplier: float = 3.0):
    """独立参考实现：手写 Wilder ATR + Pine 轨道递推"""
    high = df['high'].to_numpy()
    low = df['low'].to_numpy()
    close = df['close'].to_numpy()
    n = len(close)

    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    tr = np.maximum(high - low, np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)))
    atr = np.full(n, np.nan)
    atr[period - 1] = tr[:period].mean()
    for i in range(period, n):
        atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period

    st = np.full(n, np.nan)
    lower = np.full(n, np.nan)
    upper = np.full(n, np.nan)
    dirs = np.zeros(n, dtype=np.int8)
    up_prev = dn_prev = None
    dir_prev = 1
    for i in range(n):
        if np.isnan(atr[i]):
            continue
        hl2 = (high[i] + low[i]) / 2.0
        up_raw = hl2 - multiplier * atr[i]
        dn_raw = hl2 + multiplier * atr[i]
        if up_prev is None:
            up, dn, d = up_raw, dn_raw, 1
        else:
            up = max(up_raw, up_prev) if close[i - 1] > up_prev else up_raw
            dn = min(dn_raw, dn_prev) if close[i - 1] < dn_prev else dn_raw
            d = 1 if close[i] > dn else (-1 if close[i] < up else dir_prev)
        up_prev, dn_prev, dir_prev = up, dn, d
        lower[i], upper[i], dirs[i] = up, dn, d
        st[i] = up if d == 1 else dn
    return st, lower, upper, dirs


def test_seed_alignment_constant_price():
    """振幅 1% 的恒定价格：ATR=1、倍数 3 时轨道固定在 97/103；种子前 9 行整段为空"""
    df = _make_ohlc(np.full(30, 100.0))
    out = supertrend(df, period=10, multiplier=3.0)

    assert out['supertrend'][:9].is_null().all()
    assert (out['st_dir'][:9] == 0).all()
    assert np.allclose(out['supertrend'][9:].to_numpy(), 97.0)
    assert np.allclose(out['st_lower'][9:].to_numpy(), 97.0)
    assert np.allclose(out['st_upper'][9:].to_numpy(), 103.0)
    assert (out['st_dir'][9:] == 1).all()


def test_flip_down_then_up():
    """先跌 30 根后涨 30 根：下跌段方向 -1、上涨段翻回 +1，趋势线随方向换轨"""
    close = np.concatenate([np.linspace(100, 70, 30), np.linspace(70, 130, 30)])
    out = supertrend(_make_ohlc(close), period=10, multiplier=3.0)

    assert out['st_dir'][5] == 0
    assert out['st_dir'][29] == -1
    assert out['st_dir'][59] == 1
    assert out['supertrend'][29] == out['st_upper'][29]
    assert out['supertrend'][59] == out['st_lower'][59]


def test_matches_reference_implementation():
    """与独立参考实现逐行等价（含轨道、方向与趋势线）"""
    rng = np.random.default_rng(20261120)
    close = 100.0 + np.cumsum(rng.normal(0, 1.5, 200))
    df = _make_ohlc(close)
    out = supertrend(df, period=10, multiplier=3.0)
    ref_st, ref_lower, ref_upper, ref_dir = _ref_supertrend(df, period=10, multiplier=3.0)

    assert np.allclose(out['supertrend'].to_numpy(), ref_st, equal_nan=True)
    assert np.allclose(out['st_lower'].to_numpy(), ref_lower, equal_nan=True)
    assert np.allclose(out['st_upper'].to_numpy(), ref_upper, equal_nan=True)
    assert (out['st_dir'].to_numpy() == ref_dir).all()


def test_multiplier_zero_bounds_pinned_to_hl2():
    """倍数为 0 时裸轨等于 hl2，收紧只能让下轨不低于 hl2、上轨不高于 hl2"""
    rng = np.random.default_rng(7)
    df = _make_ohlc(100.0 + np.cumsum(rng.normal(0, 1.2, 60)))
    out = supertrend(df, period=10, multiplier=0.0)
    hl2 = ((df['high'] + df['low']) / 2.0).to_numpy()[9:]

    assert (out['st_lower'][9:].to_numpy() >= hl2 - 1e-9).all()
    assert (out['st_upper'][9:].to_numpy() <= hl2 + 1e-9).all()


# ---------------------------------------------------------------------------
# 回测引擎接线层：_precompute_indicators 与 SupertrendStrategy 信号逻辑
# ---------------------------------------------------------------------------


def _make_bar_frame(close: np.ndarray) -> pl.DataFrame:
    """构造满足 BAR_CONTRACT 的最小 OHLCV + date 帧（用于引擎预计算测试）"""
    close = np.asarray(close, dtype=float)
    high = close * 1.01
    low = close * 0.99
    volume = np.full_like(close, 1000.0)
    n = len(close)
    start = datetime.date(2025, 1, 1)
    end = start + datetime.timedelta(days=n - 1)
    dates = pl.date_range(start, end, eager=True)
    return pl.DataFrame({
        'date': dates,
        'open': close * 0.998,
        'high': high,
        'low': low,
        'close': close,
        'volume': volume,
    })


def test_engine_precompute_adds_supertrend_columns():
    """BaseBacktestEngine._precompute_indicators 必须追加 Supertrend 四列，
    且 ATR 种子期前 period-1 行 st_dir 为 0"""
    from src.backtest.engine.base_engine import BaseBacktestEngine
    from src.backtest.strategies.supertrend_strategy import SupertrendStrategy

    rng = np.random.default_rng(42)
    df = _make_bar_frame(100.0 + np.cumsum(rng.normal(0, 2.0, 60)))
    strat = SupertrendStrategy()
    st_period = strat.params['st_period']
    engine = object.__new__(BaseBacktestEngine)
    result = BaseBacktestEngine._precompute_indicators(engine, df, strat)

    for col in ('supertrend', 'st_lower', 'st_upper', 'st_dir'):
        assert col in result.columns, f'缺列 {col}'
    st_dir = result['st_dir'].to_numpy()
    assert (st_dir[:st_period - 1] == 0).all(), \
        f'ATR 种子期前 {st_period - 1} 行应为 0，实得 {st_dir[:st_period - 1]}'
    # 种子期末行（period-1）已触发第一次方向判定
    assert st_dir[st_period - 1] != 0, '种子期末行应已完成首次方向判定'
    # 种子期后方向必须有变化（随机游走必然穿越轨道）
    assert np.any(st_dir[st_period - 1:] != 0), 'st_dir 全程为 0，Supertrend 未触发'


def test_supertrend_strategy_signal_flips():
    """向量化信号：st_dir 翻 +1 发 buy，翻 -1 发 sell"""
    from src.backtest.strategies.supertrend_strategy import SupertrendStrategy

    df = pl.DataFrame({'st_dir': [0] * 5 + [-1] * 5 + [1] * 5 + [1] * 5 + [-1] * 5 + [-1] * 5})
    strat = SupertrendStrategy()
    # warmup 强制关（st_period=0 避免任何行被 warmup 吞掉）
    strat.params['st_period'] = 0
    sig = strat.generate_signals_vectorized(df).to_list()

    buy_idx = [i for i, s in enumerate(sig) if s == 'buy']
    sell_idx = [i for i, s in enumerate(sig) if s == 'sell']
    # 翻 +1 在 index=10（-1→1）、翻 -1 在 index=20（1→-1）
    assert buy_idx == [10]
    assert sell_idx == [20]
