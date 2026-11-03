#!/usr/bin/env python3

"""
技术指标类因子 —— 全部复用 ``src/tech_analysis/indicators/formulas.py``。

公式真源约束：本项目 EMA / Wilder / RSI / MACD / ATR / KDJ 的唯一定义在
``formulas.py``，本文件只做「取指标 → 构造横截面可比的无量纲因子」，
禁止在这里重写平滑公式。TA-Lib / pandas-ta 只作为离线基线对照，
不进入生产口径（依赖不引入）。

因子设计原则：输出必须无量纲（比值 / 百分位），否则复权因子跳变会让
跨股票横截面比较失真。
"""

from __future__ import annotations

import polars as pl

from src.quant.factor_lib import ops as o
from src.quant.factor_lib.registry import factor
from src.tech_analysis.indicators import formulas as f

SOURCE = '本项目 src/tech_analysis/indicators/formulas.py（通达信/pandas adjust=False 口径）'
C = pl.col


@factor('tech_macd_hist', 'technical',
         'MACD_HIST = (EMA(close,12) - EMA(close,26)) - EMA(DIF, 9)',
         'close', '+', SOURCE, lookback=35)
def tech_macd_hist(_panel: pl.DataFrame) -> pl.Expr:
    hist = f.macd_exprs('close', 12, 26, 9)['hist']
    return (hist / C('close')).over(o.GROUP)


@factor('tech_rsi_14', 'technical',
         'RSI14 = 100 - 100 / (1 + Wilder(avg_gain,14) / Wilder(avg_loss,14))',
         'close', '-', SOURCE, lookback=15)
def tech_rsi_14(_panel: pl.DataFrame) -> pl.Expr:
    return f.rsi_expr('close', 14).over(o.GROUP)


@factor('tech_atr_14_pct', 'technical',
         'ATR14% = Wilder(TR, 14) / close',
         'high, low, close', '-', SOURCE, lookback=15)
def tech_atr_14_pct(_panel: pl.DataFrame) -> pl.Expr:
    return o.safe_div(f.atr_expr('high', 'low', 'close', 14).over(o.GROUP), C('close'))


@factor('tech_kdj_k', 'technical',
         'K = SMA(RSV, 3, 1)，RSV = (close - LLV(low,9)) / (HHV(high,9) - LLV(low,9)) * 100',
         'high, low, close', '-', SOURCE, lookback=10)
def tech_kdj_k(_panel: pl.DataFrame) -> pl.Expr:
    return f.kdj_exprs('high', 'low', 'close', 9, 3)['k'].over(o.GROUP)


@factor('tech_kdj_j', 'technical',
         'J = 3K - 2D', 'high, low, close', '?', SOURCE, lookback=10)
def tech_kdj_j(_panel: pl.DataFrame) -> pl.Expr:
    return f.kdj_exprs('high', 'low', 'close', 9, 3)['j'].over(o.GROUP)


@factor('tech_ema_bias_20', 'technical',
         'BIAS20 = close / EMA(close, 20) - 1', 'close', '-', SOURCE, lookback=20)
def tech_ema_bias_20(_panel: pl.DataFrame) -> pl.Expr:
    return o.safe_div(C('close'), f.ema_expr(C('close'), 20).over(o.GROUP)) - 1.0


@factor('tech_boll_pos_20', 'technical',
         'BOLL_POS = (close - MA20) / (2 * STD20)', 'close', '-',
         SOURCE, lookback=20, status='approx',
         note='布林带位置，MA/STD 为 20 日简单均值/样本标准差（非母带口径）')
def tech_boll_pos_20(_panel: pl.DataFrame) -> pl.Expr:
    mean20 = o.ts_mean(C('close'), 20)
    std20 = o.ts_std(C('close'), 20)
    return o.safe_div(C('close') - mean20, 2.0 * std20)


@factor('tech_volume_ratio_5', 'technical',
         'VR5 = volume / mean(volume, 5) - 1', 'volume', '+', SOURCE, lookback=5)
def tech_volume_ratio_5(_panel: pl.DataFrame) -> pl.Expr:
    return o.safe_div(C('volume'), o.ts_mean(C('volume'), 5)) - 1.0


@factor('tech_mom_20', 'technical',
         'MOM20 = close / delay(close, 20) - 1', 'close', '+', SOURCE, lookback=21)
def tech_mom_20(_panel: pl.DataFrame) -> pl.Expr:
    return o.safe_div(C('close'), o.delay(C('close'), 20)) - 1.0


@factor('tech_ma_gap_5_20', 'technical',
         'MA_GAP = MA5 / MA20 - 1', 'close', '+', SOURCE, lookback=20)
def tech_ma_gap_5_20(_panel: pl.DataFrame) -> pl.Expr:
    return o.safe_div(o.ts_mean(C('close'), 5), o.ts_mean(C('close'), 20)) - 1.0


@factor('tech_up_streak', 'technical',
         'UP_STREAK20 = count(close > delay(close,1), 20) / 20', 'close', '+',
         SOURCE, lookback=21)
def tech_up_streak(_panel: pl.DataFrame) -> pl.Expr:
    return o.ts_count(C('close') > o.delay(C('close'), 1), 20) / 20.0


__all__: list[str] = []
