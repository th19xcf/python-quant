#!/usr/bin/env python3

"""
多指标共振择时策略（策略卡基线版）

五行动作矩阵（买入共振 / 卖出共振 / 震荡低吸 / 震荡高抛 / 趋势持股）的
向量化实现。指标全部来自共享公式层 ``src/tech_analysis/indicators/formulas.py``，
信号只读当日及之前的数据（不使用 shift(-n)），成交时点由回测引擎统一
延迟到下一交易日开盘。

口径参数化：
    rsi_low_center / rsi_mid_center / rsi_high_center 对应卡片上的 20 / 50 / 80
    三个中心值，其余阈值按固定偏移派生：
        超卖拐头区间   RSI12 < low_center + 10      （默认 < 30）
        超买拐头区间   RSI12 > high_center - 10     （默认 > 70）
        接近 20        RSI12 <= low_center + 5      （默认 <= 25）
        接近 80        RSI12 >= high_center - 5     （默认 >= 75）
    参数敏感性只移动中心值，派生关系不变，默认值即批准口径。

动作 → 仓位映射（映射信号，引擎按 target_weight / fraction 执行）：
    买入共振 = 100% 仓位；震荡低吸 = 50% 仓位（已满仓则零单）；
    震荡高抛 = 减到 50%；卖出共振 = 清仓；趋势持股 = 维持不变。
冲突优先级：卖出共振 > 震荡高抛 > 买入共振 > 震荡低吸 > 趋势持股。
"""

from typing import Any, Dict

import polars as pl

from src.backtest.strategies.base_strategy import BaseStrategy
from src.tech_analysis.indicators.formulas import macd_exprs, rsi_expr

# 触发类型编码
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

# 显式意图（与引擎 execution_core._normalize_signal 的映射口径一致）
_INTENTS = {
    TRIGGER_BUY: {'action': 'buy', 'target_weight': 1.0},
    TRIGGER_LOW_BUY: {'action': 'buy', 'target_weight': 0.5},
    TRIGGER_SELL: {'action': 'sell', 'fraction': 1.0},
    TRIGGER_HIGH_SELL: {'action': 'sell', 'target_weight': 0.5},
    TRIGGER_HOLD: None,
    TRIGGER_NONE: None,
}


class MultiResonanceTimingStrategy(BaseStrategy):
    """多指标共振择时：MACD(12,26,9) + RSI12 + MA20 三条件 AND。"""

    #: 向量化接口写入的触发类型列（回放/交易归因共用）
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
        """
        Args:
            rsi_low_center: RSI12 低位中心值（卡片 20），超卖区间与「接近 20」由此派生
            rsi_mid_center: RSI12 中位中心值（卡片 50），站稳 / 跌破由此派生
            rsi_high_center: RSI12 高位中心值（卡片 80），超买区间与「接近 80」由此派生
            high_low_window: 新高 / 新低的长期窗口（卡片 60），短期窗口固定 20
            signal_lag: 额外延迟的 bar 数，仅用于无未来函数对照（0 = 正常）
            enable_divergence: 是否启用 60 日顶 / 底背离（口径 10）
        """
        super().__init__(name='多指标共振择时')
        self.params.update({
            'rsi_low_center': rsi_low_center,
            'rsi_mid_center': rsi_mid_center,
            'rsi_high_center': rsi_high_center,
            'high_low_window': high_low_window,
            'signal_lag': signal_lag,
            'enable_divergence': enable_divergence,
        })

    # ------------------------------------------------------------------
    # 信号构造
    # ------------------------------------------------------------------

    def build_signal_frame(self, df: pl.DataFrame) -> pl.DataFrame:
        """返回带 ``trigger_type`` 与 ``signal`` 列的行情帧（按行序对齐）。

        引擎调用 ``generate_signals_vectorized`` 时走本方法；回测脚本也用它
        回放每个信号日，把成交归因到具体规则。
        """
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

        prev_dif = dif.shift(1)
        prev_dea = dea.shift(1)
        rsi_1 = rsi.shift(1)
        rsi_2 = rsi.shift(2)
        prev_close = close.shift(1)
        prev_ma20 = ma20.shift(1)

        # 口径 2：金叉 / 死叉
        golden_cross = (dif > dea) & (prev_dif <= prev_dea)
        death_cross = (dif < dea) & (prev_dif >= prev_dea)
        # 口径 3：低位金叉 / 高位死叉
        low_golden = golden_cross & (dif < 0)
        high_death = death_cross & (dif > 0)
        # 口径 4：0 轴
        above_zero = dif > 0
        above_zero_bull = above_zero & (dif > dea)
        # 口径 5：3 日转向
        bottom_turn = (rsi > rsi_1) & (rsi_1 < rsi_2)
        top_turn = (rsi < rsi_1) & (rsi_1 > rsi_2)
        oversold_turn = bottom_turn & (rsi < low + 10.0)
        overbought_turn = top_turn & (rsi > high - 10.0)
        # 口径 6：站稳 / 跌破 50
        stand_50 = (rsi > mid) & (rsi_1 > mid) & (rsi_2 > mid)
        break_50 = (rsi < mid) & (rsi_1 >= mid)
        # 口径 7：接近 20 / 80
        near_20 = rsi <= low + 5.0
        near_80 = rsi >= high - 5.0
        # 口径 8：不创新低 / 不创新高
        low_20 = pl.col('low').rolling_min(window_size=20, min_samples=20)
        low_n = pl.col('low').rolling_min(window_size=win, min_samples=win)
        high_20 = pl.col('high').rolling_max(window_size=20, min_samples=20)
        high_n = pl.col('high').rolling_max(window_size=win, min_samples=win)
        no_new_low = low_20 > low_n
        no_new_high = high_20 < high_n
        # 口径 9：破位 / 可死叉不破位（破位仅用于口径完整性，卡片动作不单独消费）
        _breakdown = (close < ma20) & (prev_close >= prev_ma20)
        death_not_broken = death_cross & (close >= ma20)
        # 口径 10：60 日顶 / 底背离（窗口按卡片固定 60）
        if enable_div:
            close_max60 = close.rolling_max(window_size=60, min_samples=60)
            close_min60 = close.rolling_min(window_size=60, min_samples=60)
            dif_max60 = dif.rolling_max(window_size=60, min_samples=60)
            dif_min60 = dif.rolling_min(window_size=60, min_samples=60)
            top_divergence = (close == close_max60) & (dif < dif_max60)
            bottom_divergence = (close == close_min60) & (dif > dif_min60)
        else:
            top_divergence = pl.lit(False)
            bottom_divergence = pl.lit(False)

        # 口径 11：三条件 AND
        buy_resonance = (
            (golden_cross | bottom_divergence | above_zero)
            & (oversold_turn | stand_50)
            & low_golden
        )
        sell_resonance = (
            (death_cross | top_divergence | top_turn)
            & (overbought_turn | break_50)
            & high_death
        )
        osc_low_buy = no_new_low & near_20 & low_golden
        osc_high_sell = no_new_high & near_80 & high_death
        trend_hold = above_zero_bull & stand_50 & death_not_broken

        # 口径 12：冲突优先级 卖出 > 高抛 > 买入 > 低吸 > 持有
        trigger_code = (
            pl.when(sell_resonance.fill_null(False)).then(pl.lit(TRIGGER_SELL))
            .when(osc_high_sell.fill_null(False)).then(pl.lit(TRIGGER_HIGH_SELL))
            .when(buy_resonance.fill_null(False)).then(pl.lit(TRIGGER_BUY))
            .when(osc_low_buy.fill_null(False)).then(pl.lit(TRIGGER_LOW_BUY))
            .when(trend_hold.fill_null(False)).then(pl.lit(TRIGGER_HOLD))
            .otherwise(pl.lit(TRIGGER_NONE))
            .alias('trigger_code')
        )
        frame = df.with_columns([trigger_code])

        codes = frame['trigger_code'].to_list()
        if lag > 0:
            # 对照用：整段信号再延迟 lag 根 bar（lag=1 即 T+1 信号 → T+2 成交）
            codes = [TRIGGER_NONE] * lag + codes[:-lag]
        names = [TRIGGER_NAMES[int(c)] for c in codes]
        intents = [_INTENTS[int(c)] for c in codes]
        return frame.with_columns([
            pl.Series(self.TRIGGER_COL, names, dtype=pl.Utf8),
            pl.Series('signal', intents, dtype=pl.Object),
        ])

    def generate_signals_vectorized(self, df: pl.DataFrame) -> pl.Series:
        """整段向量化信号（映射意图 / None），由引擎在回测前批量调用。"""
        return self.build_signal_frame(df)['signal']

    def generate_signal(self, row: Dict[str, Any], index: int) -> Any:
        """逐 bar 兜底接口：只回放 ``build_signal_frame`` 已写好的触发列。

        指标跨越多个窗口（RSI 3 日转向、60 日新高），无法用单根 bar 复现；
        正常路径由 ``generate_signals_vectorized`` 承担，本方法仅用于
        预计算帧回放（行内含 ``trigger_type`` 列时）。
        """
        name = row.get(self.TRIGGER_COL)
        code = next((c for c, n in TRIGGER_NAMES.items() if n == name), TRIGGER_NONE)
        return _INTENTS[code]
