#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
交易成本模型，用于计算交易成本
"""

from typing import Dict, Any


class CostModel:
    """
    交易成本模型类，提供交易成本计算功能
    """
    
    def __init__(self):
        """
        初始化成本模型
        """
        # 默认参数
        self.params = {
            'commission_rate': 0.0003,  # 佣金费率
            'min_commission': 5.0,  # 最低佣金
            'slippage': 0.0001,  # 滑点
            'tax_rate': 0.001  # 印花税税率
        }
    
    def set_params(self, params: Dict[str, Any]):
        """
        设置成本模型参数
        
        Args:
            params: 成本模型参数
        """
        self.params.update(params)
    
    def calculate_cost(self, shares: float, buy_price: float, sell_price: float) -> float:
        """
        计算交易成本
        
        Args:
            shares: 交易股数
            buy_price: 买入价格
            sell_price: 卖出价格
            
        Returns:
            float: 交易成本
        """
        commission_rate = self.params.get('commission_rate', 0.0003)
        min_commission = self.params.get('min_commission', 5.0)
        slippage = self.params.get('slippage', 0.0001)
        tax_rate = self.params.get('tax_rate', 0.001)
        
        # 计算佣金
        buy_amount = shares * buy_price
        sell_amount = shares * sell_price
        
        buy_commission = max(buy_amount * commission_rate, min_commission)
        sell_commission = max(sell_amount * commission_rate, min_commission)
        
        # 计算印花税（仅卖出时收取）
        stamp_tax = sell_amount * tax_rate
        
        # 计算滑点成本
        slippage_cost = shares * (sell_price - buy_price) * slippage
        
        # 总交易成本
        total_cost = buy_commission + sell_commission + stamp_tax + slippage_cost
        
        return total_cost
