#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
多因子策略：结合向量化预计算的技术指标生成信号

依赖 BacktestEngine._precompute_indicators 预计算好的列：
    ma_{short_window}, ma_{long_window}   （MA 因子）
    macd, macd_signal                     （MACD 因子）
    rsi                                   （RSI 因子）

generate_signal 按权重聚合三个因子的二元贡献（1 / 0 / -1），
加权得分 > 0.3 买入，< -0.3 卖出。
"""

from typing import Dict, Any, List
from src.backtest.strategies.base_strategy import BaseStrategy


class MultiFactorStrategy(BaseStrategy):
    """结合 MA / MACD / RSI 的等权（或自定义权重）综合因子策略"""

    def __init__(self, factors: List[str] = None, weights: Dict[str, float] = None):
        super().__init__(name="多因子策略")
        self.params['factors'] = factors or ['ma', 'macd', 'rsi']
        self.params['weights'] = weights or {'ma': 0.3, 'macd': 0.4, 'rsi': 0.3}

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

    def generate_signal(self, data: Dict[str, Any], index: int) -> str:
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

        if score > 0.3:
            return 'buy'
        if score < -0.3:
            return 'sell'
        return 'hold'
