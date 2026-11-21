"""技术指标因子 v2（归一候选）—— 与 v1 并存，默认不进入任何生产链路。

口径来源：``src/tech_analysis/indicators/formulas.py`` 真源 + ``src/alpha`` 的
「窗口错位一期 + ddof=0」组装语义（见 ``docs/技术指标口径归一影响清单_2026-11-21.md``）。

- v1（``technical.py``）：当期窗口 + 样本标准差 ddof=1；
- v2（本模块）：均线/标准差先取 t-1 窗口、标准差用总体口径 ddof=0，避免用当日
  收盘价生成当日信号。

仅登记，不修改 v1；本模块未被 ``src/advice`` 或任何生产入口引用。
"""

from __future__ import annotations

import polars as pl

from src.quant.factor_lib import ops as o
from src.quant.factor_lib.factors.technical import SOURCE
from src.quant.factor_lib.registry import factor
from src.tech_analysis.indicators import formulas as f

C = pl.col
_SQRT_252 = 252.0 ** 0.5


def _std_ddof0(expr: pl.Expr, n: int) -> pl.Expr:
    """总体标准差（ddof=0），与 ``np.std`` / src/alpha 一致。"""
    return expr.rolling_std(window_size=n, min_samples=n, ddof=0).over(o.GROUP)


@factor('tech_boll_pos_20_v2', 'technical_v2',
         'BOLL_POS_v2 = (close_{t-1} - MA20_{t-1}) / (2 * STD20_{t-1}), ddof=0',
         'close', '-', SOURCE, lookback=21, status='approx',
         note='v2：窗口错位一期 + ddof=0，对齐 src/alpha bollinger_position')
def tech_boll_pos_20_v2(_panel: pl.DataFrame) -> pl.Expr:
    close_prev = o.delay(C('close'), 1)
    mean_prev = o.delay(o.ts_mean(C('close'), 20), 1)
    std_prev = o.delay(_std_ddof0(C('close'), 20), 1)
    return o.safe_div(close_prev - mean_prev, 2.0 * std_prev)


@factor('tech_atr_14_pct_v2', 'technical_v2',
         'ATR14%_v2 = ATR14_{t-1} / close_{t-1}',
         'high, low, close', '-', SOURCE, lookback=16)
def tech_atr_14_pct_v2(_panel: pl.DataFrame) -> pl.Expr:
    atr_prev = o.delay(f.atr_expr('high', 'low', 'close', 14).over(o.GROUP), 1)
    return o.safe_div(atr_prev, o.delay(C('close'), 1))


@factor('tech_ma_gap_5_20_v2', 'technical_v2',
         'MA_GAP_v2 = MA5_{t-1} / MA20_{t-1} - 1',
         'close', '+', SOURCE, lookback=21)
def tech_ma_gap_5_20_v2(_panel: pl.DataFrame) -> pl.Expr:
    ma5_prev = o.delay(o.ts_mean(C('close'), 5), 1)
    ma20_prev = o.delay(o.ts_mean(C('close'), 20), 1)
    return o.safe_div(ma5_prev, ma20_prev) - 1.0


@factor('tech_ema_bias_20_v2', 'technical_v2',
         'BIAS20_v2 = close_{t-1} / EMA20_{t-1} - 1',
         'close', '-', SOURCE, lookback=21)
def tech_ema_bias_20_v2(_panel: pl.DataFrame) -> pl.Expr:
    close_prev = o.delay(C('close'), 1)
    ema_prev = o.delay(f.ema_expr(C('close'), 20).over(o.GROUP), 1)
    return o.safe_div(close_prev, ema_prev) - 1.0


@factor('tech_macd_hist_v2', 'technical_v2',
         'MACD_HIST_v2 = EMA(close,12) - EMA(close,26) - EMA(DIF,9)（不除 close）',
         'close', '+', SOURCE, lookback=35)
def tech_macd_hist_v2(_panel: pl.DataFrame) -> pl.Expr:
    return f.macd_exprs('close', 12, 26, 9)['hist'].over(o.GROUP)


@factor('tech_vol_20_v2', 'technical_v2',
         'VOL20_v2 = std(ret, 20, ddof=0)_{t-1} * sqrt(252)',
         'close', '-', SOURCE, lookback=22)
def tech_vol_20_v2(_panel: pl.DataFrame) -> pl.Expr:
    ret = C('close') / o.delay(C('close'), 1) - 1.0
    return o.delay(_std_ddof0(ret, 20), 1) * _SQRT_252


@factor('tech_ma_cross_5_20_v2', 'technical_v2',
         'MA_CROSS_v2 = sign(MA5_{t-1} - MA20_{t-1}) 的当次穿越（+1/-1/0）',
         'close', '+', SOURCE, lookback=22)
def tech_ma_cross_5_20_v2(_panel: pl.DataFrame) -> pl.Expr:
    ma5 = o.delay(o.ts_mean(C('close'), 5), 1)
    ma20 = o.delay(o.ts_mean(C('close'), 20), 1)
    ma5_prev = o.delay(ma5, 1)
    ma20_prev = o.delay(ma20, 1)
    return (
        pl.when((ma5 > ma20) & (ma5_prev <= ma20_prev)).then(1.0)
        .when((ma5 < ma20) & (ma5_prev >= ma20_prev)).then(-1.0)
        .otherwise(0.0)
    )
