#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
指标公式共享层 —— 全项目指标公式唯一收口点

本模块只提供纯 Polars Expr 工厂函数，不读写数据库、不做任何 IO，
所有调用方（技术指标权威实现、回测引擎预计算、GPU 加速路径、Alpha 因子库）
都必须从这里取公式，禁止再各自手写 EMA / Wilder / RSI / MACD / ATR。

行业口径说明（通达信 / pandas adjust=False 风格）：
    - EMA：y0 = x0，alpha = 2 / (period + 1)，即 pandas ewm(span=period, adjust=False)
    - Wilder 平滑（RSI / ATR 标准）：前 period 个值的 SMA 作为种子，
      之后 y = (x + (period - 1) * y_prev) / period
    - RSI：Wilder 平滑上涨/下跌幅度后 100 - 100 / (1 + avg_gain / avg_loss)，
      avg_loss 为 0 时取 100
    - MACD：DIF = EMA(fast) - EMA(slow)，DEA = EMA(DIF, signal)，柱 = DIF - DEA
    - ATR：TR = max(high - low, |high - prev_close|, |low - prev_close|)，
      再做 Wilder 平滑
    - KDJ：RSV 含当日高低点，K/D 为通达信 SMA(X, M1, 1) 递推（α = 1/M1），
      即 K(t) = (RSV(t) + (M1 - 1) * K(t-1)) / M1，首值取 RSV
"""

import polars as pl


def ema_expr(expr: pl.Expr, period: int) -> pl.Expr:
    """
    标准 EMA 表达式。

    y0 = x0，alpha = 2 / (period + 1)，
    与 pandas ewm(span=period, adjust=False) 逐点等价。

    Args:
        expr: 待平滑的 Polars 表达式
        period: EMA 周期

    Returns:
        pl.Expr: EMA 结果表达式
    """
    return expr.ewm_mean(span=period, adjust=False)


def wilder_expr(expr: pl.Expr, period: int) -> pl.Expr:
    """
    Wilder 平滑表达式（RSI / ATR 行业标准口径）。

    语义：前 period 个值的 SMA 作为种子（落在索引 period-1），
    之后按 y = (x + (period - 1) * y_prev) / period 递推。
    种子行之前保持 null。

    实现：用行号定位种子行，其余行填 null，
    再做 alpha = 1 / period 的 adjust=False 指数平滑（min_samples=1），
    这样从第一个非 null 值（种子行）开始递推，与递推公式完全等价。

    Args:
        expr: 待平滑的 Polars 表达式
        period: 平滑周期

    Returns:
        pl.Expr: Wilder 平滑结果表达式
    """
    idx = pl.int_range(0, pl.len())
    seeded = (
        pl.when(idx > period - 1).then(expr)
        .when(idx == period - 1).then(expr.rolling_mean(window_size=period))
        .otherwise(None)
    )
    return seeded.ewm_mean(alpha=1.0 / period, adjust=False, min_samples=1)


def rsi_expr(close_col: str = 'close', period: int = 14) -> pl.Expr:
    """
    RSI 表达式（Wilder 标准口径）。

    前 period-1 行为 null（种子在索引 period-1），值域 0~100。

    Args:
        close_col: 收盘价列名
        period: RSI 周期

    Returns:
        pl.Expr: RSI 结果表达式
    """
    close = pl.col(close_col)
    delta = close.diff()
    gain = pl.when(delta > 0).then(delta).otherwise(0.0)
    loss = pl.when(delta < 0).then(-delta).otherwise(0.0)

    avg_gain = wilder_expr(gain, period)
    avg_loss = wilder_expr(loss, period)

    return (
        pl.when(avg_loss == 0)
        .then(100.0)
        .otherwise(100.0 - 100.0 / (1.0 + avg_gain / avg_loss))
    )


def macd_exprs(
    close_col: str = 'close',
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> dict:
    """
    MACD 三线表达式（标准口径）。

    信号线直接对 MACD 表达式本身再做 EMA，不落到中间列，
    保证整条公式在单个查询计划内完成。

    Args:
        close_col: 收盘价列名
        fast: 快线 EMA 周期
        slow: 慢线 EMA 周期
        signal: 信号线 EMA 周期

    Returns:
        dict: {'macd': DIF 表达式, 'signal': DEA 表达式, 'hist': 柱状图表达式}
    """
    close = pl.col(close_col)
    macd = ema_expr(close, fast) - ema_expr(close, slow)
    signal_line = ema_expr(macd, signal)
    hist = macd - signal_line
    return {'macd': macd, 'signal': signal_line, 'hist': hist}


def atr_expr(
    high_col: str = 'high',
    low_col: str = 'low',
    close_col: str = 'close',
    period: int = 14,
) -> pl.Expr:
    """
    ATR 表达式（Wilder 标准口径）。

    TR = max(high - low, |high - prev_close|, |low - prev_close|)，
    再按 Wilder 平滑。首行没有 prev_close 时只取 high - low。

    Args:
        high_col: 最高价列名
        low_col: 最低价列名
        close_col: 收盘价列名
        period: ATR 周期

    Returns:
        pl.Expr: ATR 结果表达式
    """
    high = pl.col(high_col)
    low = pl.col(low_col)
    prev_close = pl.col(close_col).shift(1)
    tr = pl.max_horizontal(
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    )
    return wilder_expr(tr, period)


def kdj_exprs(
    high_col: str = 'high',
    low_col: str = 'low',
    close_col: str = 'close',
    window: int = 14,
    m1: int = 3,
) -> dict:
    """
    KDJ 四线表达式（通达信口径）。

    RSV 含当日：RSV = (close - LLV(low, N)) / (HHV(high, N) - LLV(low, N)) * 100，
    最高价等于最低价时取 0；
    K = SMA(RSV, M1, 1)，即 K(t) = (RSV(t) + (M1 - 1) * K(t-1)) / M1，首值取 RSV；
    D = SMA(K, M1, 1)；J = 3K - 2D。

    Args:
        high_col: 最高价列名
        low_col: 最低价列名
        close_col: 收盘价列名
        window: RSV 周期
        m1: K/D 平滑周期（通达信固定为 3）

    Returns:
        dict: {'rsv': RSV, 'k': K, 'd': D, 'j': J} 表达式
    """
    highest = pl.col(high_col).rolling_max(window_size=window, min_samples=1)
    lowest = pl.col(low_col).rolling_min(window_size=window, min_samples=1)
    rsv = (
        pl.when(highest == lowest)
        .then(0.0)
        .otherwise((pl.col(close_col) - lowest) / (highest - lowest) * 100.0)
    )

    # SMA(X, M1, 1) 等价于 alpha = 1/M1、adjust=False 的指数平滑，首值取 X
    alpha = 1.0 / m1
    k = rsv.ewm_mean(alpha=alpha, adjust=False, min_samples=1)
    d = k.ewm_mean(alpha=alpha, adjust=False, min_samples=1)
    return {'rsv': rsv, 'k': k, 'd': d, 'j': 3.0 * k - 2.0 * d}


__all__ = [
    'ema_expr',
    'wilder_expr',
    'rsi_expr',
    'macd_exprs',
    'atr_expr',
    'kdj_exprs',
]
