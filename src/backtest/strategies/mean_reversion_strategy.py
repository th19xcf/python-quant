#!/usr/bin/env python3

"""
均值回归策略：基于向量化预计算的 RSI 超买超卖

BacktestEngine._precompute_indicators 会提前算好 'rsi' 列，
generate_signal 只读当前行的 rsi 值，不再自己维护历史窗口。
"""

from typing import Any, Dict

from src.backtest.strategies.base_strategy import BaseStrategy


class MeanReversionStrategy(BaseStrategy):
    """
    均值回归策略，基于 RSI 超买超卖信号
    """

    def __init__(self):
        super().__init__("MeanReversion")
        self.params = {
            'rsi_period': 14,
            'overbought': 70,
            'oversold': 30,
        }

    def generate_signal(self, data: Dict[str, Any], index: int) -> str:
        rsi_period = self.params.get('rsi_period', 14)
        overbought = self.params.get('overbought', 70)
        oversold = self.params.get('oversold', 30)

        if index < rsi_period:
            return 'hold'

        rsi = data.get('rsi')
        if rsi is None:
            return 'hold'

        if rsi < oversold:
            return 'buy'
        if rsi > overbought:
            return 'sell'
        return 'hold'
