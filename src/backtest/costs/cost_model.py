#!/usr/bin/env python3

"""
交易成本模型，用于计算交易成本

默认参数对齐 A 股现行规则（2023-08-28 起印花税降为 0.05%）：
- 佣金双边 0.03%，单笔最低 5 元
- 印花税卖出侧 0.05%
- 过户费双边 0.002%（2015-08-01 起沪深统一）
- 滑点 0.02% 双边，可按流动性覆盖

传入 ``trade_date`` 时按历史时间表取费率（印花税/过户费），避免用现行
费率回测历史区间造成系统性成本偏差；不传时保持原静态参数行为，向后兼容。
"""

from datetime import date, datetime
from typing import Any, Dict, Tuple, Union

#: 印花税历史时间表：(生效日, 税率, 征收方向)，按生效日倒序排列。
#: 2023-08-28 减半至 0.05%（卖方单边）；2008-09-19 起 0.1%（卖方单边）；
#: 2007-05-30 上调至 0.3% 且双边；2005-01-24 起 0.1% 双边；
#: 2001-11-16 起 0.2% 双边；1998-06-12 起 0.4% 双边（更早区间按 0.4% 近似）。
STAMP_TAX_SCHEDULE: Tuple[Tuple[date, float, str], ...] = (
    (date(2023, 8, 28), 0.0005, 'sell'),
    (date(2008, 9, 19), 0.001, 'sell'),
    (date(2007, 5, 30), 0.003, 'both'),
    (date(2005, 1, 24), 0.001, 'both'),
    (date(2001, 11, 16), 0.002, 'both'),
    (date(1998, 6, 12), 0.004, 'both'),
)

#: 过户费切换日：2015-08-01 起沪深两市统一按成交金额 0.002%（双向）。
#: 此前仅沪市按成交金额 0.06% 收取，深市不收。
TRANSFER_FEE_SWITCH = date(2015, 8, 1)
TRANSFER_FEE_RATE_UNIFIED = 0.00002
TRANSFER_FEE_RATE_SH_LEGACY = 0.0006

TradeDate = Union[date, datetime, str, None]


def _as_date(value: TradeDate) -> date | None:
    """把 date/datetime/'YYYY-MM-DD' 归一为 date；无法解析时返回 None。"""
    if value is None:
        return None
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


class CostModel:
    """
    交易成本模型类，提供交易成本计算功能
    """

    def __init__(self, overrides: Dict[str, Any] | None = None):
        """
        初始化成本模型

        Args:
            overrides: 自定义参数，会覆盖默认值。可选键：
                       commission_rate / min_commission / stamp_tax_rate /
                       transfer_fee_rate / slippage
        """
        self.params = {
            'commission_rate': 0.0003,       # 佣金费率（双边）
            'min_commission': 5.0,           # 单笔最低佣金
            'stamp_tax_rate': 0.0005,        # 印花税（仅卖出，2023-08-28 起为 0.05%）
            'transfer_fee_rate': 0.00001,    # 过户费（双边，近似）
            'slippage': 0.0002,              # 滑点（双边）
        }
        if overrides:
            self.params.update(overrides)

    def set_params(self, params: Dict[str, Any]):
        """
        设置成本模型参数
        """
        self.params.update(params)

    def calculate_buy_cost(self, shares: float, price: float,
                           trade_date: TradeDate = None, ts_code: str = '') -> float:
        """
        买入侧成本（佣金 + 过户费 + 滑点）；2008-09-19 前印花税双边征收时含税。

        trade_date 传入时按历史时间表取印花税/过户费；未传时沿用静态参数。
        """
        commission_rate = self.params['commission_rate']
        min_commission = self.params['min_commission']
        slippage = self.params['slippage']

        amount = shares * price
        commission = max(amount * commission_rate, min_commission)
        transfer = amount * self._transfer_fee_rate(trade_date, ts_code)
        tax = amount * self._stamp_tax_rate(trade_date, side='buy')
        slip = amount * slippage

        return commission + transfer + tax + slip

    def calculate_sell_cost(self, shares: float, price: float,
                            trade_date: TradeDate = None, ts_code: str = '') -> float:
        """
        卖出侧成本（佣金 + 印花税 + 过户费 + 滑点）。

        trade_date 传入时按历史时间表取印花税/过户费；2010 年卖出印花税
        应为 0.1%（2008-09-19 至 2023-08-27），而不是现行 0.05%。
        """
        commission_rate = self.params['commission_rate']
        min_commission = self.params['min_commission']
        slippage = self.params['slippage']

        amount = shares * price
        commission = max(amount * commission_rate, min_commission)
        tax = amount * self._stamp_tax_rate(trade_date, side='sell')
        transfer = amount * self._transfer_fee_rate(trade_date, ts_code)
        slip = amount * slippage

        return commission + tax + transfer + slip

    def calculate_cost(self, shares: float, buy_price: float, sell_price: float,
                       trade_date: TradeDate = None, ts_code: str = '') -> float:
        """
        一轮完整买卖的总交易成本
        """
        return (self.calculate_buy_cost(shares, buy_price, trade_date, ts_code)
                + self.calculate_sell_cost(shares, sell_price, trade_date, ts_code))

    def get_buy_cost_rate(self, trade_date: TradeDate = None, ts_code: str = '') -> float:
        """估算买入侧成本费率（用于计算可负担股数）"""
        return (self.params['commission_rate']
                + self._transfer_fee_rate(trade_date, ts_code)
                + self._stamp_tax_rate(trade_date, side='buy')
                + self.params['slippage'])

    # ------------------------------------------------------------------
    # 历史费率解析
    # ------------------------------------------------------------------

    def _stamp_tax_rate(self, trade_date: TradeDate, side: str) -> float:
        """按交易日与买卖方向取印花税率；未传日期时沿用静态参数（买入不计）。"""
        day = _as_date(trade_date)
        if day is None:
            return self.params['stamp_tax_rate'] if side == 'sell' else 0.0
        for effective, rate, direction in STAMP_TAX_SCHEDULE:
            if day >= effective:
                if direction == 'both' or direction == side:
                    return rate
                return 0.0
        return 0.0

    def _transfer_fee_rate(self, trade_date: TradeDate, ts_code: str) -> float:
        """按交易日与市场取过户费率；未传日期时沿用静态参数。"""
        day = _as_date(trade_date)
        if day is None:
            return self.params['transfer_fee_rate']
        if day >= TRANSFER_FEE_SWITCH:
            return TRANSFER_FEE_RATE_UNIFIED
        return TRANSFER_FEE_RATE_SH_LEGACY if self._is_shanghai(ts_code) else 0.0

    @staticmethod
    def _is_shanghai(ts_code: str) -> bool:
        """判定是否沪市标的：后缀优先，无后缀时按 6/9 开头（含 900 B 股）。"""
        code = (ts_code or '').upper()
        if code.endswith('.SH'):
            return True
        if code.endswith(('.SZ', '.BJ')):
            return False
        return code.split('.')[0].startswith(('6', '9'))
