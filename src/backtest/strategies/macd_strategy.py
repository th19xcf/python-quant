#!/usr/bin/env python3

"""
MACD策略

无状态实现：直接读取 BacktestEngine._precompute_indicators 预计算的
macd 与 macd_signal 列，不再用实例 list 累积价格、EMA、MACD、signal。
信号为同向判定（macd>signal 买入，< 卖出）；金叉/死叉事件判定需引擎提供
prev_macd/prev_signal 列，留待后续扩展。
"""

from typing import Any, Dict

from src.backtest.strategies.base_strategy import BaseStrategy


class MACDStrategy(BaseStrategy):
    """
    MACD策略
    当MACD线上穿信号线时买入，下穿时卖出
    """

    def __init__(self, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9):
        """
        初始化MACD策略

        Args:
            fast_period: 快速EMA周期
            slow_period: 慢速EMA周期
            signal_period: 信号线EMA周期
        """
        super().__init__(name="MACD策略")
        self.params['fast_period'] = fast_period
        self.params['slow_period'] = slow_period
        self.params['signal_period'] = signal_period

    def generate_signal(self, data: Dict[str, Any], index: int) -> str:
        """
        生成交易信号

        Args:
            data: 当前数据（含引擎预计算的 macd、macd_signal 列）
            index: 当前数据索引

        Returns:
            str: 交易信号，'buy'、'sell'或'hold'
        """
        # MACD 按通达信口径从第 0 根就有值（EMA 种子 y0=x0），慢线+信号线根数内
        # 的差离值未稳定，属暖机噪声，不交易（与引擎预计算的固定 12/26/9 参数对齐）
        if index < 26 + 9:
            return 'hold'

        macd = data.get('macd')
        signal = data.get('macd_signal')

        if macd is None or signal is None:
            return 'hold'

        if macd > signal:
            return 'buy'
        if macd < signal:
            return 'sell'
        return 'hold'
