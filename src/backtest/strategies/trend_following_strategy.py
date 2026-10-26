#!/usr/bin/env python3

"""
趋势跟踪策略：基于向量化预计算的均线交叉信号

BacktestEngine 会在回测启动前调用 _precompute_indicators，
把 ma_{short_window} / ma_{long_window} 直接写到 DataFrame 的列里。
generate_signal 只读当前行的预计算列，不再自己循环求和，
避免在每根 K 线上做 O(n²) 的历史重建。
"""

from typing import Any, Dict

from src.backtest.strategies.base_strategy import BaseStrategy


class TrendFollowingStrategy(BaseStrategy):
    """
    趋势跟踪策略，基于移动平均线交叉信号
    """

    def __init__(self):
        super().__init__("TrendFollowing")
        self.params = {
            'short_window': 10,
            'long_window': 20,
        }

    def generate_signal(self, data: Dict[str, Any], index: int) -> str:
        short_window = self.params.get('short_window', 10)
        long_window = self.params.get('long_window', 20)

        # 等待长均线稳定后再开仓
        if index < long_window:
            return 'hold'

        short_col = f'ma_{short_window}'
        long_col = f'ma_{long_window}'

        short_ma = data.get(short_col)
        long_ma = data.get(long_col)
        if short_ma is None or long_ma is None:
            return 'hold'

        # 金叉买 / 死叉卖
        if short_ma > long_ma:
            return 'buy'
        if short_ma < long_ma:
            return 'sell'
        return 'hold'
