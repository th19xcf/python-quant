#!/usr/bin/env python3

"""
交易成本模型，用于计算交易成本

默认参数对齐 A 股现行规则（2023-08-28 起印花税降为 0.05%）：
- 佣金双边 0.03%，单笔最低 5 元
- 印花税卖出侧 0.05%
- 过户费双边 0.001%（沪市）/ 深市并入监管费整体约 0.0025%，统一近似为 0.001%
- 滑点 0.02% 双边，可按流动性覆盖
"""

from typing import Any, Dict


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

    def calculate_buy_cost(self, shares: float, price: float) -> float:
        """
        买入侧成本（佣金 + 过户费 + 滑点），不含印花税
        """
        commission_rate = self.params['commission_rate']
        min_commission = self.params['min_commission']
        transfer_fee = self.params['transfer_fee_rate']
        slippage = self.params['slippage']

        amount = shares * price
        commission = max(amount * commission_rate, min_commission)
        transfer = amount * transfer_fee
        slip = amount * slippage

        return commission + transfer + slip

    def calculate_sell_cost(self, shares: float, price: float) -> float:
        """
        卖出侧成本（佣金 + 印花税 + 过户费 + 滑点）
        """
        commission_rate = self.params['commission_rate']
        min_commission = self.params['min_commission']
        stamp_tax = self.params['stamp_tax_rate']
        transfer_fee = self.params['transfer_fee_rate']
        slippage = self.params['slippage']

        amount = shares * price
        commission = max(amount * commission_rate, min_commission)
        tax = amount * stamp_tax
        transfer = amount * transfer_fee
        slip = amount * slippage

        return commission + tax + transfer + slip

    def calculate_cost(self, shares: float, buy_price: float, sell_price: float) -> float:
        """
        一轮完整买卖的总交易成本
        """
        return self.calculate_buy_cost(shares, buy_price) + self.calculate_sell_cost(shares, sell_price)

    def get_buy_cost_rate(self) -> float:
        """估算买入侧成本费率（用于计算可负担股数）"""
        return self.params['commission_rate'] + self.params['transfer_fee_rate'] + self.params['slippage']
