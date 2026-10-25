#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
移动平均线策略

无状态实现：直接读取 BacktestEngine._precompute_indicators 预计算的
ma_{short_window} 与 ma_{long_window} 列，不再用实例 list 累积价格。
信号为同向判定（short>long 买入，< 卖出）；金叉/死叉事件判定需引擎提供
prev_ma_* 列，留待后续扩展。
"""

from src.backtest.strategies.base_strategy import BaseStrategy
from typing import Dict, Any


class MAStrategy(BaseStrategy):
    """
    移动平均线策略
    当短期均线上穿长期均线时买入，下穿时卖出
    """

    def __init__(self, short_window: int = 5, long_window: int = 20):
        """
        初始化移动平均线策略

        Args:
            short_window: 短期均线窗口
            long_window: 长期均线窗口
        """
        super().__init__(name="移动平均线策略")
        self.params['short_window'] = short_window
        self.params['long_window'] = long_window

    def generate_signal(self, data: Dict[str, Any], index: int) -> str:
        """
        生成交易信号

        Args:
            data: 当前数据（含引擎预计算的 ma_{short_window}、ma_{long_window} 列）
            index: 当前数据索引

        Returns:
            str: 交易信号，'buy'、'sell'或'hold'
        """
        short_w = self.params.get('short_window', 5)
        long_w = self.params.get('long_window', 20)

        short_ma = data.get(f'ma_{short_w}')
        long_ma = data.get(f'ma_{long_w}')

        # 预计算列窗口未满的前 long_window-1 根为 None，此时不交易
        if short_ma is None or long_ma is None:
            return 'hold'

        if short_ma > long_ma:
            return 'buy'
        if short_ma < long_ma:
            return 'sell'
        return 'hold'
