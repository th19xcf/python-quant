#!/usr/bin/env python3

"""
绩效分析器，用于计算回测的各种绩效指标
"""

from collections import defaultdict, deque
from datetime import date, datetime
from typing import Any, Dict, List

import numpy as np


class PerformanceAnalyzer:
    """
    绩效分析器类，提供回测绩效指标计算功能
    """

    def analyze(self, equity_curve: List[Dict[str, Any]], trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        分析回测绩效

        Args:
            equity_curve: 权益曲线
            trades: 交易记录

        Returns:
            Dict[str, Any]: 绩效分析结果
        """
        if not equity_curve:
            return {}

        # 提取权益数据
        equities = [item['equity'] for item in equity_curve]

        # 计算基本指标
        initial_equity = equities[0]
        final_equity = equities[-1]
        total_return = (final_equity - initial_equity) / initial_equity * 100

        # 年化收益率：优先按真实日历跨度折算；日期缺失时退回 bar 数口径。
        # 原实现用 252/bar 数，在停牌、周线或缺失交易日场景会系统性高估年化。
        days = self._calendar_span_days(equity_curve)
        if initial_equity > 0 and final_equity > 0:
            if days:
                annual_return = (pow(final_equity / initial_equity, 365 / days) - 1) * 100
            else:
                annual_return = (pow(final_equity / initial_equity, 252 / len(equities)) - 1) * 100
        else:
            annual_return = 0

        # 计算收益率序列（向量化：r_t = (e_t - e_{t-1}) / e_{t-1}）
        equity_array = np.asarray(equities, dtype=float)
        prev_equity = equity_array[:-1]
        returns = np.divide(
            equity_array[1:] - prev_equity, prev_equity,
            out=np.zeros_like(prev_equity), where=prev_equity != 0,
        ).tolist()

        # 计算风险指标
        if returns:
            sharpe_ratio = self._calculate_sharpe_ratio(returns)
            max_drawdown = self._calculate_max_drawdown(equities)
            volatility = np.std(returns) * np.sqrt(252) * 100
            winning_rate = self._calculate_winning_rate(trades)
            average_profit_loss = self._calculate_average_profit_loss(trades)
        else:
            sharpe_ratio = 0
            max_drawdown = 0
            volatility = 0
            winning_rate = 0
            average_profit_loss = 0

        return {
            'total_return': total_return,
            'annual_return': annual_return,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'volatility': volatility,
            'winning_rate': winning_rate,
            'average_profit_loss': average_profit_loss,
            'trades_count': len(self._pair_trades(trades))  # 完整买卖对数量
        }

    def _calculate_sharpe_ratio(self, returns: List[float], risk_free_rate: float = 0) -> float:
        """
        计算夏普比率

        Args:
            returns: 收益率序列
            risk_free_rate: 无风险利率

        Returns:
            float: 夏普比率
        """
        if not returns:
            return 0

        avg_return = np.mean(returns)
        std_return = np.std(returns)

        if std_return == 0:
            return 0

        # 年化处理
        sharpe_ratio = (avg_return - risk_free_rate / 252) / std_return * np.sqrt(252)
        return sharpe_ratio

    @staticmethod
    def _calendar_span_days(equity_curve: List[Dict[str, Any]]) -> int | None:
        """返回权益曲线首末日期之间的日历天数；日期缺失或非法时返回 None。"""
        start = PerformanceAnalyzer._parse_date(equity_curve[0].get('date'))
        end = PerformanceAnalyzer._parse_date(equity_curve[-1].get('date'))
        if not start or not end:
            return None
        span = (end - start).days
        return span if span > 0 else None

    @staticmethod
    def _parse_date(value: Any) -> date | None:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            try:
                return date.fromisoformat(value[:10])
            except ValueError:
                return None
        return None

    def _calculate_max_drawdown(self, equities: List[float]) -> float:
        """
        计算最大回撤

        Args:
            equities: 权益序列

        Returns:
            float: 最大回撤（百分比）
        """
        if not equities:
            return 0

        max_drawdown = 0.0
        peak = equities[0]

        for equity in equities:
            if equity > peak:
                peak = equity
            drawdown = (peak - equity) / peak * 100
            if drawdown > max_drawdown:
                max_drawdown = drawdown

        return max_drawdown

    @staticmethod
    def _pair_trades(trades: List[Dict[str, Any]]) -> List[tuple]:
        """
        将交易记录按股票分组并依序配对买卖，返回(buy, sell)对列表

        组合回测中多只股票的交易记录交错存储，不能简单按列表下标两两配对
        """
        grouped = defaultdict(list)
        for t in trades:
            grouped[t.get('stock_code', '_default')].append(t)

        pairs = []
        for stock_trades in grouped.values():
            open_buys = deque()
            for t in stock_trades:
                if t.get('signal') == 'buy':
                    open_buys.append(t)
                elif t.get('signal') == 'sell' and open_buys:
                    pairs.append((open_buys.popleft(), t))
        return pairs

    @staticmethod
    def _pair_profit(buy_trade: Dict[str, Any], sell_trade: Dict[str, Any]) -> float:
        """计算单笔配对交易的净盈亏（按实际成交股数计量，成本按比例分摊）。"""
        buy_shares = buy_trade.get('shares') or 0
        sell_shares = sell_trade.get('shares') or 0
        # 配对数量取买卖重叠部分，避免用买入股数计量部分卖出时高估盈亏
        if buy_shares and sell_shares:
            matched = min(buy_shares, sell_shares)
        else:
            matched = buy_shares or sell_shares

        profit = (sell_trade['price'] - buy_trade['price']) * matched
        profit -= PerformanceAnalyzer._prorated_cost(buy_trade, matched, buy_shares)
        profit -= PerformanceAnalyzer._prorated_cost(sell_trade, matched, sell_shares)
        return profit

    @staticmethod
    def _prorated_cost(trade: Dict[str, Any], matched: float, total_shares: float) -> float:
        """按配对股数占比分摊单笔交易成本；股数缺失时不分摊。"""
        cost = trade.get('cost', 0) or 0
        if matched and total_shares:
            return cost * matched / total_shares
        return cost

    def _calculate_winning_rate(self, trades: List[Dict[str, Any]]) -> float:
        """
        计算胜率

        Args:
            trades: 交易记录

        Returns:
            float: 胜率（百分比）
        """
        pairs = self._pair_trades(trades)
        if not pairs:
            return 0

        winning_trades = sum(1 for buy, sell in pairs if self._pair_profit(buy, sell) > 0)
        return (winning_trades / len(pairs)) * 100

    def _calculate_average_profit_loss(self, trades: List[Dict[str, Any]]) -> float:
        """
        计算平均盈亏比

        Args:
            trades: 交易记录

        Returns:
            float: 平均盈亏比
        """
        pairs = self._pair_trades(trades)
        if not pairs:
            return 0

        profits = []
        losses = []

        for buy_trade, sell_trade in pairs:
            profit = self._pair_profit(buy_trade, sell_trade)
            if profit > 0:
                profits.append(profit)
            else:
                losses.append(abs(profit))

        avg_profit = np.mean(profits) if profits else 0
        # 无亏损时盈亏比无定义，避免用 1 元作为分母产生量纲错误
        avg_loss = np.mean(losses) if losses else 0

        return avg_profit / avg_loss if avg_loss > 0 else 0
