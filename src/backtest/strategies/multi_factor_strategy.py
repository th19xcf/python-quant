#!/usr/bin/env python3

"""
多因子策略：结合向量化预计算的技术指标生成信号

依赖 BacktestEngine._precompute_indicators 预计算好的列：
    ma_{short_window}, ma_{long_window}   （MA 因子）
    macd, macd_signal                     （MACD 因子）
    rsi                                   （RSI 因子）

generate_signal 按权重聚合三个因子的二元贡献（1 / 0 / -1），
加权得分超过阈值（默认 0.5）买入、低于负阈值卖出。
默认权重 {ma:0.3, macd:0.4, rsi:0.3} 下，单因子最大贡献 0.4、
两因子最小共振和 0.6，阈值取 0.5 落在二者之间，
保证至少两个因子同向共振才触发交易（MACD 一票不能独决）。
"""

from typing import Any, Dict, List

from src.backtest.strategies.base_strategy import BaseStrategy


class MultiFactorStrategy(BaseStrategy):
    """结合 MA / MACD / RSI 的综合因子策略（默认需至少两因子共振）"""

    def __init__(self, factors: List[str] = None, weights: Dict[str, float] = None,
                 threshold: float = 0.5):
        super().__init__(name="多因子策略")
        self.params['factors'] = factors or ['ma', 'macd', 'rsi']
        self.params['weights'] = weights or {'ma': 0.3, 'macd': 0.4, 'rsi': 0.3}
        self.params['threshold'] = threshold

    def _ma_factor(self, data: Dict[str, Any]) -> float:
        short_w = self.params.get('short_window', 10)
        long_w = self.params.get('long_window', 20)
        short_ma = data.get(f'ma_{short_w}')
        long_ma = data.get(f'ma_{long_w}')
        if short_ma is None or long_ma is None:
            return 0
        if short_ma > long_ma:
            return 1
        if short_ma < long_ma:
            return -1
        return 0

    def _macd_factor(self, data: Dict[str, Any]) -> float:
        macd = data.get('macd')
        signal = data.get('macd_signal')
        if macd is None or signal is None:
            return 0
        # 当前 MACD 穿越信号线的方向（回测里没有 prev_macd/prev_signal 历史，
        # 这里退化为同方向打分；若需精确金叉/死叉可在引擎里再加 prev_* 列）
        if macd > signal:
            return 1
        if macd < signal:
            return -1
        return 0

    def _rsi_factor(self, data: Dict[str, Any]) -> float:
        rsi = data.get('rsi')
        if rsi is None:
            return 0
        if rsi < 30:
            return 1
        if rsi > 70:
            return -1
        return 0

    def _warmup_bars(self) -> int:
        """暖机根数：窗口未满的指标值是噪声，暖机期内不交易。

        MA 列由引擎以 min_samples=window 预计算（窗口内为 null，因子已判 None 自保），
        RSI 为 Wilder 种子口径（前 period-1 根为 null），
        MACD 按通达信口径从第 0 根有值，需按 慢线+信号线 根数暖机。
        """
        return max(
            int(self.params.get('long_window', 20)),
            int(self.params.get('rsi_period', 14)),
            26 + 9,
        )

    def generate_signal(self, data: Dict[str, Any], index: int) -> str:
        if index < self._warmup_bars():
            return 'hold'
        factor_values = {}
        factors = self.params['factors']
        if 'ma' in factors:
            factor_values['ma'] = self._ma_factor(data)
        if 'macd' in factors:
            factor_values['macd'] = self._macd_factor(data)
        if 'rsi' in factors:
            factor_values['rsi'] = self._rsi_factor(data)

        weights = self.params['weights']
        score = sum(
            factor_values[f] * weights.get(f, 0)
            for f in factor_values
        )

        threshold = self.params['threshold']
        if score > threshold:
            return 'buy'
        if score < -threshold:
            return 'sell'
        return 'hold'
