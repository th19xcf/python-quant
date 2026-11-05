#!/usr/bin/env python3

"""
绩效分析器，用于计算回测的各种绩效指标
"""

from collections import defaultdict, deque
from datetime import date, datetime
from typing import Any, Dict, List

import numpy as np

# 不同数据频率对应的年化因子：收益率序列按 bar 计算，波动率/夏普必须
# 按实际频率折算，统一写死 252 会把周线、月线回测的风险指标系统性高估。
_ANNUALIZATION_FACTORS = {
    'daily': 252,
    'weekly': 52,
    'monthly': 12,
}

# 兼容中文/常见缩写输入
_FREQUENCY_ALIASES = {
    '日': 'daily', '日线': 'daily', 'd': 'daily', '1d': 'daily',
    '周': 'weekly', '周线': 'weekly', 'w': 'weekly', '1w': 'weekly',
    '月': 'monthly', '月线': 'monthly', 'm': 'monthly', '1m': 'monthly',
}


class PerformanceAnalyzer:
    """
    绩效分析器类，提供回测绩效指标计算功能
    """

    def analyze(self, equity_curve: List[Dict[str, Any]], trades: List[Dict[str, Any]],
                frequency: str = 'daily') -> Dict[str, Any]:
        """
        分析回测绩效

        Args:
            equity_curve: 权益曲线
            trades: 交易记录
            frequency: 数据频率（daily / weekly / monthly）。未知频率回退到
                daily，并在返回值的 frequency_fallback 中标注。

        Returns:
            Dict[str, Any]: 绩效分析结果
        """
        if not equity_curve:
            return {}

        # 年化因子按实际频率取；未知频率回退日频并显式标注
        resolved_frequency, periods_per_year, fallback = self._resolve_frequency(frequency)

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
                annual_return = (
                    pow(final_equity / initial_equity, periods_per_year / len(equities)) - 1
                ) * 100
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
            sharpe_ratio = self._calculate_sharpe_ratio(returns, periods_per_year=periods_per_year)
            max_drawdown = self._calculate_max_drawdown(equities)
            volatility = np.std(returns) * np.sqrt(periods_per_year) * 100
            winning_rate = self._calculate_winning_rate(trades)
            average_profit_loss = self._calculate_average_profit_loss(trades)
        else:
            sharpe_ratio = 0
            max_drawdown = 0
            volatility = 0
            winning_rate = 0
            average_profit_loss = 0

        # 最长水下按月计（月末净值连续低于前高的月数），与 max_drawdown 同表
        longest_underwater_months = self.calculate_longest_underwater_months(equity_curve)
        # Calmar = 年化收益 / 最大回撤（两者均为百分数，比值无量纲）
        calmar = annual_return / max_drawdown if max_drawdown > 0 else 0

        return {
            'total_return': total_return,
            'annual_return': annual_return,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'longest_underwater_months': longest_underwater_months,
            'calmar': calmar,
            'volatility': volatility,
            'winning_rate': winning_rate,
            'average_profit_loss': average_profit_loss,
            'trades_count': len(self._pair_trades(trades)),  # 完整买卖对数量
            # 口径可观测性：调用方据此确认风险指标使用的年化基准
            'frequency': resolved_frequency,
            'annualization_factor': periods_per_year,
            'frequency_fallback': fallback,
        }

    @staticmethod
    def _resolve_frequency(frequency: str) -> tuple[str, int, bool]:
        """把频率映射为 (规范频率, 年化因子, 是否回退日频)。

        未知频率不静默按日频糊弄，返回 fallback=True 供调用方标注。
        """
        normalized = str(frequency or '').strip().lower()
        normalized = _FREQUENCY_ALIASES.get(normalized, normalized)
        if normalized in _ANNUALIZATION_FACTORS:
            return normalized, _ANNUALIZATION_FACTORS[normalized], False
        return 'daily', _ANNUALIZATION_FACTORS['daily'], True

    def _calculate_sharpe_ratio(self, returns: List[float], risk_free_rate: float = 0,
                                periods_per_year: int = 252) -> float:
        """
        计算夏普比率

        Args:
            returns: 收益率序列
            risk_free_rate: 年化无风险利率
            periods_per_year: 年化期数（日 252 / 周 52 / 月 12）

        Returns:
            float: 夏普比率
        """
        if not returns:
            return 0

        avg_return = np.mean(returns)
        std_return = np.std(returns)

        if std_return == 0:
            return 0

        # 年化处理：无风险利率按年化值折算到单期，波动率按 sqrt(期数) 放大
        sharpe_ratio = (
            (avg_return - risk_free_rate / periods_per_year) / std_return
            * np.sqrt(periods_per_year)
        )
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

    def calculate_longest_underwater_months(
            self, equity_curve: List[Dict[str, Any]]) -> int:
        """计算最长水下月数：月末净值连续低于历史前高的最长月数。

        口径：每月取最后一个交易日的权益作为月末净值；当月末净值 >= 此前
        最高月末净值时视为出坑并重置计数，否则累计一个月。未满整月的最后
        一段按已发生的月末计入。返回整数月数（0 表示从未水下）。
        """
        if not equity_curve:
            return 0

        month_end: List[tuple[tuple[int, int], float]] = []
        for item in equity_curve:
            day = self._parse_date(item.get('date'))
            equity = item.get('equity')
            if day is None or equity is None:
                continue
            key = (day.year, day.month)
            if month_end and month_end[-1][0] == key:
                month_end[-1] = (key, float(equity))
            else:
                month_end.append((key, float(equity)))

        peak = float('-inf')
        longest = 0
        current = 0
        for _, equity in month_end:
            if equity >= peak:
                peak = equity
                current = 0
            else:
                current += 1
                longest = max(longest, current)
        return longest

    @staticmethod
    def annual_turnover(buy_notional: float, average_equity: float,
                        years: float) -> Dict[str, float]:
        """年换手倍数：单边 = 年买入成交额 / 平均权益；双边 = 单边 ×2。

        Args:
            buy_notional: 区间买入成交额合计（与 average_equity 同币种/同口径）
            average_equity: 区间平均权益（建议按日权益均值，而非期末值）
            years: 区间年数（按交易日数 / 252 或日历跨度折算均可，需在报表注明）
        """
        if average_equity <= 0 or years <= 0:
            return {'one_way': 0.0, 'two_way': 0.0}
        one_way = buy_notional / average_equity / years
        return {'one_way': one_way, 'two_way': one_way * 2.0}

    @staticmethod
    def annual_cost_ratio(total_cost: float, average_equity: float,
                          years: float) -> float:
        """年化成本占比（百分点）：总成本 / 平均权益 / 年数 × 100。"""
        if average_equity <= 0 or years <= 0:
            return 0.0
        return total_cost / average_equity / years * 100.0

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
            open_buys: deque[Any] = deque()
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
