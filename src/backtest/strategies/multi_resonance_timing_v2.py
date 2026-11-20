#!/usr/bin/env python3

"""
多指标共振择时策略 v2（正交三维度版，2026-11-20）

v2 针对 v1 的四个结构缺陷重写口径：
    1. 条件 A 冗余 → 三维度正交：D1 方向（MACD 金叉/死叉）、D2 位置（DIF 正负）、
       D3 动能（RSI12 上穿/下穿 50），D1 不再蕴含 D2。
    2. 同日 AND 太严 → D1 / D3 允许「近 3 日内曾成立」，D2 仍要求当日成立。
    3. 五行动作三行不可达 → 震荡低吸 / 高抛改为「近 5 日 RSI 极值 + 近 3 日交叉」，
       趋势持股去掉「死叉不破位」前提，只看 0 轴上多头 + 收盘 >= MA20。
    4. 卖出过松 → 卖出共振要求 D1（死叉/顶背离，近 3 日）与 D3（下穿 50 / 超买后拐头）
       同时具备，且当日 DIF>0，不再允许「拐头」单独成立。

口径细节（与任务书逐条对应）：
    买入共振 = recent3(金叉|底背离) & 当日 DIF<0
               & ( recent3(RSI12 上穿 50) | ( recent5(RSI12<30) & recent3(底部拐头) ) )
    卖出共振 = recent3(死叉|顶背离) & 当日 DIF>0
               & ( recent3(RSI12 下穿 50) | ( recent5(RSI12>70) & recent3(顶部拐头) ) )
    震荡低吸 = 不创新低 & recent5(RSI12<=25) & recent3(金叉)
    震荡高抛 = 不创新高 & recent5(RSI12>=75) & recent3(死叉)
    趋势持股 = DIF>0 & DIF>DEA & 收盘 >= MA20

其余沿用 v1：指标 MACD(12,26,9) / RSI12、3 日拐头、20 与 60 日新高新低、MA20、
仓位映射（100% / 50% / 维持 / 减半 / 清仓）、冲突优先级、T 日信号 T+1 开盘成交。
"""

from __future__ import annotations

from typing import Any, Dict

import polars as pl

from src.backtest.strategies.base_strategy import BaseStrategy
from src.tech_analysis.indicators.formulas import macd_exprs, rsi_expr

TRIGGER_NONE = 0
TRIGGER_BUY = 1
TRIGGER_SELL = 2
TRIGGER_LOW_BUY = 3
TRIGGER_HIGH_SELL = 4
TRIGGER_HOLD = 5

TRIGGER_NAMES = {
    TRIGGER_NONE: '无',
    TRIGGER_BUY: '买入共振',
    TRIGGER_SELL: '卖出共振',
    TRIGGER_LOW_BUY: '震荡低吸',
    TRIGGER_HIGH_SELL: '震荡高抛',
    TRIGGER_HOLD: '趋势持股',
}

_INTENTS = {
    TRIGGER_BUY: {'action': 'buy', 'target_weight': 1.0},
    TRIGGER_LOW_BUY: {'action': 'buy', 'target_weight': 0.5},
    TRIGGER_SELL: {'action': 'sell', 'fraction': 1.0},
    TRIGGER_HIGH_SELL: {'action': 'sell', 'target_weight': 0.5},
    TRIGGER_HOLD: None,
    TRIGGER_NONE: None,
}


def _recent(expr: pl.Expr, n: int) -> pl.Expr:
    """近 n 根 bar（含当日）内曾成立。"""
    out = expr
    for i in range(1, n):
        out = out | expr.shift(i)
    return out.fill_null(False)


class MultiResonanceTimingV2Strategy(BaseStrategy):
    """多指标共振择时 v2：D1 方向 / D2 位置 / D3 动能 三维度。"""

    TRIGGER_COL = 'trigger_type'

    def __init__(
        self,
        rsi_low_center: int = 20,
        rsi_mid_center: int = 50,
        rsi_high_center: int = 80,
        high_low_window: int = 60,
        signal_lag: int = 0,
        enable_divergence: bool = True,
    ) -> None:
        super().__init__(name='多指标共振择时v2')
        self.params.update({
            'rsi_low_center': rsi_low_center,
            'rsi_mid_center': rsi_mid_center,
            'rsi_high_center': rsi_high_center,
            'high_low_window': high_low_window,
            'signal_lag': signal_lag,
            'enable_divergence': enable_divergence,
        })

    def build_signal_frame(self, df: pl.DataFrame) -> pl.DataFrame:
        p = self.params
        low = float(p['rsi_low_center'])
        mid = float(p['rsi_mid_center'])
        high = float(p['rsi_high_center'])
        win = int(p['high_low_window'])
        enable_div = bool(p['enable_divergence'])
        lag = int(p['signal_lag'])

        close = pl.col('close')
        macd = macd_exprs('close', 12, 26, 9)
        dif = macd['macd']
        dea = macd['signal']
        rsi = rsi_expr('close', 12)
        ma20 = close.rolling_mean(window_size=20, min_samples=20)

        golden_cross = (dif > dea) & (dif.shift(1) <= dea.shift(1))
        death_cross = (dif < dea) & (dif.shift(1) >= dea.shift(1))
        bottom_turn = (rsi > rsi.shift(1)) & (rsi.shift(1) < rsi.shift(2))
        top_turn = (rsi < rsi.shift(1)) & (rsi.shift(1) > rsi.shift(2))

        # 背离沿用 v1 最简定义，仅替代 D1（近 3 日内可替代金叉/死叉）
        if enable_div:
            top_divergence = (
                (close == close.rolling_max(window_size=60, min_samples=60))
                & (dif < dif.rolling_max(window_size=60, min_samples=60))
            )
            bottom_divergence = (
                (close == close.rolling_min(window_size=60, min_samples=60))
                & (dif > dif.rolling_min(window_size=60, min_samples=60))
            )
        else:
            top_divergence = pl.lit(False)
            bottom_divergence = pl.lit(False)

        # D1 方向 / D2 位置 / D3 动能
        d1_buy = _recent(golden_cross | bottom_divergence, 3)
        d1_sell = _recent(death_cross | top_divergence, 3)
        d2_low = dif < 0
        d2_high = dif > 0
        up_cross_50 = (rsi > mid) & (rsi.shift(1) <= mid)
        down_cross_50 = (rsi < mid) & (rsi.shift(1) >= mid)
        d3_buy = _recent(up_cross_50, 3) | (
            _recent(rsi < low + 10.0, 5) & _recent(bottom_turn, 3))
        d3_sell = _recent(down_cross_50, 3) | (
            _recent(rsi > high - 10.0, 5) & _recent(top_turn, 3))

        # 震荡低吸 / 高抛：近 5 日 RSI 极值 + 近 3 日交叉
        low_20 = pl.col('low').rolling_min(window_size=20, min_samples=20)
        low_n = pl.col('low').rolling_min(window_size=win, min_samples=win)
        high_20 = pl.col('high').rolling_max(window_size=20, min_samples=20)
        high_n = pl.col('high').rolling_max(window_size=win, min_samples=win)
        no_new_low = low_20 > low_n
        no_new_high = high_20 < high_n
        osc_low_buy = (
            no_new_low.fill_null(False)
            & _recent(rsi <= low + 5.0, 5)
            & _recent(golden_cross, 3))
        osc_high_sell = (
            no_new_high.fill_null(False)
            & _recent(rsi >= high - 5.0, 5)
            & _recent(death_cross, 3))

        trend_hold = (dif > 0) & (dif > dea) & (close >= ma20)

        buy_resonance = d1_buy & d2_low.fill_null(False) & d3_buy
        sell_resonance = d1_sell & d2_high.fill_null(False) & d3_sell

        trigger_code = (
            pl.when(sell_resonance.fill_null(False)).then(pl.lit(TRIGGER_SELL))
            .when(osc_high_sell.fill_null(False)).then(pl.lit(TRIGGER_HIGH_SELL))
            .when(buy_resonance.fill_null(False)).then(pl.lit(TRIGGER_BUY))
            .when(osc_low_buy.fill_null(False)).then(pl.lit(TRIGGER_LOW_BUY))
            .when(trend_hold.fill_null(False)).then(pl.lit(TRIGGER_HOLD))
            .otherwise(pl.lit(TRIGGER_NONE))
            .alias('trigger_code')
        )
        frame = df.with_columns([
            trigger_code,
            d1_buy.cast(pl.Int8).alias('d1_buy'),
            d1_sell.cast(pl.Int8).alias('d1_sell'),
            d2_low.fill_null(False).cast(pl.Int8).alias('d2_low'),
            d2_high.fill_null(False).cast(pl.Int8).alias('d2_high'),
            d3_buy.fill_null(False).cast(pl.Int8).alias('d3_buy'),
            d3_sell.fill_null(False).cast(pl.Int8).alias('d3_sell'),
        ])

        codes = frame['trigger_code'].to_list()
        if lag > 0:
            codes = [TRIGGER_NONE] * lag + codes[:-lag]
        names = [TRIGGER_NAMES[int(c)] for c in codes]
        intents = [_INTENTS[int(c)] for c in codes]
        return frame.with_columns([
            pl.Series(self.TRIGGER_COL, names, dtype=pl.Utf8),
            pl.Series('signal', intents, dtype=pl.Object),
        ])

    def generate_signals_vectorized(self, df: pl.DataFrame) -> pl.Series:
        return self.build_signal_frame(df)['signal']

    def generate_signal(self, row: Dict[str, Any], index: int) -> Any:
        """逐 bar 兜底接口：只回放 build_signal_frame 已写好的触发列。"""
        name = row.get(self.TRIGGER_COL)
        code = next((c for c, n in TRIGGER_NAMES.items() if n == name), TRIGGER_NONE)
        return _INTENTS[code]
