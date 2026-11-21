#!/usr/bin/env python3

"""
指标公式共享层 —— 全项目指标公式唯一收口点

本模块只提供纯 Polars Expr 工厂函数，不读写数据库、不做任何 IO，
所有调用方（技术指标权威实现、回测引擎预计算、GPU 加速路径、Alpha 因子库）
都必须从这里取公式，禁止再各自手写 EMA / Wilder / RSI / MACD / ATR。

行业口径说明（通达信 / pandas adjust=False 风格）：
    - EMA：y0 = x0，alpha = 2 / (period + 1)，即 pandas ewm(span=period, adjust=False)
    - Wilder 平滑（RSI / ATR 标准）：前 period 个值的 SMA 作为种子，
      之后 y = (x + (period - 1) * y_prev) / period
    - RSI：通达信口径 ``SMA(MAX(C-LC,0),N,1) / SMA(ABS(C-LC),N,1) * 100``，
      其中 ``SMA(X,N,1)`` 即 alpha = 1/N、adjust=False 的指数平滑且首值取 X，
      第 2 根 K 线起即有值
    - MACD：DIF = EMA(fast) - EMA(slow)，DEA = EMA(DIF, signal)，柱 = (DIF - DEA) * 2
    - ATR：TR = max(high - low, |high - prev_close|, |low - prev_close|)，
      再做 Wilder 平滑
    - KDJ：RSV 含当日高低点，K/D 为通达信 SMA(X, M1, 1) 递推（α = 1/M1），
      即 K(t) = (RSV(t) + (M1 - 1) * K(t-1)) / M1，首值取 RSV
    - Supertrend：以 ATR 计上下轨并逐行单向收紧，收盘破轨翻转趋势；
      轨道带路径依赖，故它是本模块唯一以 DataFrame 进出、内部逐行递推的函数
"""

import numpy as np
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


def sma_smooth(expr: pl.Expr, period: int, weight: int = 1) -> pl.Expr:
    """通达信 ``SMA(X,N,M)`` 平滑：``Y = (X*M + Y'*(N-M)) / N``。

    等价于 ``alpha = M/N``、``adjust=False`` 的指数平滑，首值取 X；
    已对 ``H:\\zxzq\\TCalc.dll`` 的函数说明取证。

    Args:
        expr: 待平滑表达式
        period: N
        weight: M，默认 1

    Returns:
        pl.Expr: 平滑后的表达式
    """
    return expr.ewm_mean(alpha=weight / period, adjust=False, min_samples=1)


def sma_expr(column: str, period: int, weight: int = 1) -> pl.Expr:
    """按列名调用 :func:`sma_smooth` 的便捷入口。"""
    return sma_smooth(pl.col(column), period, weight)


def rsi_expr(close_col: str = 'close', period: int = 14) -> pl.Expr:
    """
    RSI 表达式（通达信 SMA 口径，已对拍 ``H:\\zxzq\\TCalc.dll`` 公式原文）。

    通达信原文：
        ``LC:=REF(CLOSE,1); RSI:=SMA(MAX(CLOSE-LC,0),N,1)/SMA(ABS(CLOSE-LC),N,1)*100;``

    ``SMA(X,N,1)`` 即 alpha = 1/N、adjust=False、首值取 X 的指数平滑，
    因此首行（diff 为 null）为空、**第 2 根 K 线起即有值**，
    与旧 Wilder 种子（前 N-1 根为 null）不同。

    Args:
        close_col: 收盘价列名
        period: RSI 周期

    Returns:
        pl.Expr: RSI 结果表达式
    """
    close = pl.col(close_col)
    delta = close.diff()
    gain = pl.when(delta > 0).then(delta).otherwise(0.0)
    abs_delta = delta.abs()

    avg_gain = sma_smooth(gain, period, 1)
    avg_abs = sma_smooth(abs_delta, period, 1)

    return (
        pl.when(avg_abs == 0)
        .then(100.0)
        .otherwise(avg_gain / avg_abs * 100.0)
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
    # 通达信原文 MACD:(DIF-DEA)*2 —— 柱值必须乘 2，否则只有通达信的一半
    hist = (macd - signal_line) * 2.0
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


def supertrend(
    df: pl.DataFrame,
    high_col: str = 'high',
    low_col: str = 'low',
    close_col: str = 'close',
    period: int = 10,
    multiplier: float = 3.0,
) -> pl.DataFrame:
    """
    Supertrend 趋势指标（TradingView ``ta.supertrend`` 口径）。

    与模块内其它工厂函数不同，这里以 DataFrame 进出：轨道带是路径依赖的
    （每行需要上一行的轨道与方向），无法用无状态 Expr 表达。
    ATR 仍调用 :func:`atr_expr`（Wilder 平滑），与全项目 ATR 同源。

    递推式（与原版 Pine 逐行等价，``up`` 为下轨支撑、``dn`` 为上轨压力）：

    .. code-block:: text

        hl2      = (high + low) / 2
        up0(t)   = hl2 - multiplier * ATR(t)
        dn0(t)   = hl2 + multiplier * ATR(t)
        up(t)    = max(up0(t), up(t-1))  if close(t-1) >  up(t-1) else up0(t)
        dn(t)    = min(dn0(t), dn(t-1))  if close(t-1) <  dn(t-1) else dn0(t)
        dir(t)   = +1 if close(t) > dn(t)
                   -1 if close(t) < up(t)
                   dir(t-1) 其余
        supertrend(t) = up(t) if dir(t) == +1 else dn(t)

    ATR 种子行之前（索引 < period-1）整段输出为空。

    Args:
        df: 含 high / low / close 的行情表，按时间升序
        high_col: 最高价列名
        low_col: 最低价列名
        close_col: 收盘价列名
        period: ATR 周期（TradingView 默认 10）
        multiplier: ATR 倍数（TradingView 默认 3.0）

    Returns:
        pl.DataFrame: 原表追加 4 列
            - ``supertrend``：趋势线（多头取下轨、空头取上轨）
            - ``st_lower``：下轨支撑
            - ``st_upper``：上轨压力
            - ``st_dir``：1 多头 / -1 空头 / 0 未初始化
    """
    atr = df.select(atr_expr(high_col, low_col, close_col, period).alias('_atr'))['_atr'].to_numpy()
    high = df[high_col].to_numpy()
    low = df[low_col].to_numpy()
    close = df[close_col].to_numpy()

    n = len(df)
    st = np.full(n, np.nan)
    st_upper = np.full(n, np.nan)
    st_lower = np.full(n, np.nan)
    st_dir = np.zeros(n, dtype=np.int8)

    up_prev = None
    dn_prev = None
    dir_prev = 1
    for i in range(n):
        if np.isnan(atr[i]):
            continue
        hl2 = (high[i] + low[i]) / 2.0
        up_raw = hl2 - multiplier * atr[i]
        dn_raw = hl2 + multiplier * atr[i]

        if up_prev is None:
            up, dn, direction = up_raw, dn_raw, 1
        else:
            up = max(up_raw, up_prev) if close[i - 1] > up_prev else up_raw
            dn = min(dn_raw, dn_prev) if close[i - 1] < dn_prev else dn_raw
            if close[i] > dn:
                direction = 1
            elif close[i] < up:
                direction = -1
            else:
                direction = dir_prev

        up_prev, dn_prev, dir_prev = up, dn, direction
        st_lower[i] = up
        st_upper[i] = dn
        st_dir[i] = direction
        st[i] = up if direction == 1 else dn

    return df.with_columns([
        pl.Series('supertrend', st, nan_to_null=True),
        pl.Series('st_lower', st_lower, nan_to_null=True),
        pl.Series('st_upper', st_upper, nan_to_null=True),
        pl.Series('st_dir', st_dir),
    ])


__all__ = [
    'ema_expr',
    'wilder_expr',
    'rsi_expr',
    'macd_exprs',
    'atr_expr',
    'kdj_exprs',
    'supertrend',
]
