#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
高级交易成本模型，支持详细交易成本计算
"""

from typing import Dict, Any, Optional


class AdvancedCostModel:
    """
    高级交易成本模型类，提供详细的交易成本计算功能
    支持不同市场的成本结构和多种成本类型
    """
    
    def __init__(self, market: str = 'cn'):
        """
        初始化高级成本模型
        
        Args:
            market: 市场类型，可选值：'cn'（中国A股）、'hk'（港股）、'us'（美股）
        """
        # 初始化市场参数
        self.market = market
        self.params = self._get_market_defaults(market)
    
    def _get_market_defaults(self, market: str) -> Dict[str, Any]:
        """
        获取市场默认参数
        
        Args:
            market: 市场类型
            
        Returns:
            Dict[str, Any]: 市场默认参数
        """
        if market == 'cn':
            # 中国A股默认参数
            return {
                'commission_rate': 0.0003,  # 佣金费率
                'min_commission': 5.0,  # 最低佣金
                'slippage': 0.0001,  # 滑点
                'tax_rate': 0.001,  # 印花税税率
                'transfer_fee': 0.00002,  # 过户费
                'handling_fee': 0.0000487,  # 经手费
                'regulatory_fee': 0.00002,  # 证管费
                'stamp_tax_threshold': 0  # 印花税起征点
            }
        elif market == 'hk':
            # 港股默认参数
            return {
                'commission_rate': 0.0008,  # 佣金费率
                'min_commission': 100.0,  # 最低佣金
                'slippage': 0.0001,  # 滑点
                'tax_rate': 0.001,  # 印花税税率
                'transfer_fee': 0.0002,  # 过户费
                'handling_fee': 0.00005,  # 经手费
                'regulatory_fee': 0.00002,  # 证管费
                'stamp_tax_threshold': 0  # 印花税起征点
            }
        elif market == 'us':
            # 美股默认参数
            return {
                'commission_rate': 0.0,  # 佣金费率（大多数券商免佣金）
                'min_commission': 0.0,  # 最低佣金
                'slippage': 0.0001,  # 滑点
                'tax_rate': 0.0,  # 印花税税率（美股无印花税）
                'transfer_fee': 0.0,  # 过户费
                'handling_fee': 0.000119,  # 经手费
                'regulatory_fee': 0.0000221,  # 证管费
                'stamp_tax_threshold': 0  # 印花税起征点
            }
        else:
            # 默认参数
            return {
                'commission_rate': 0.0003,  # 佣金费率
                'min_commission': 5.0,  # 最低佣金
                'slippage': 0.0001,  # 滑点
                'tax_rate': 0.001,  # 印花税税率
                'transfer_fee': 0.0,  # 过户费
                'handling_fee': 0.0,  # 经手费
                'regulatory_fee': 0.0,  # 证管费
                'stamp_tax_threshold': 0  # 印花税起征点
            }
    
    def set_params(self, params: Dict[str, Any]):
        """
        设置成本模型参数
        
        Args:
            params: 成本模型参数
        """
        self.params.update(params)
    
    def set_market(self, market: str):
        """
        设置市场类型
        
        Args:
            market: 市场类型
        """
        self.market = market
        self.params = self._get_market_defaults(market)
    
    def calculate_cost(self, shares: float, buy_price: float, sell_price: float, trade_type: str = 'stock') -> Dict[str, Any]:
        """
        计算交易成本
        
        Args:
            shares: 交易股数
            buy_price: 买入价格
            sell_price: 卖出价格
            trade_type: 交易类型，可选值：'stock'（股票）、'fund'（基金）、'option'（期权）
            
        Returns:
            Dict[str, Any]: 详细的交易成本明细
        """
        # 获取参数
        commission_rate = self.params.get('commission_rate', 0.0003)
        min_commission = self.params.get('min_commission', 5.0)
        slippage = self.params.get('slippage', 0.0001)
        tax_rate = self.params.get('tax_rate', 0.001)
        transfer_fee = self.params.get('transfer_fee', 0.0)
        handling_fee = self.params.get('handling_fee', 0.0)
        regulatory_fee = self.params.get('regulatory_fee', 0.0)
        stamp_tax_threshold = self.params.get('stamp_tax_threshold', 0)
        
        # 计算交易金额
        buy_amount = shares * buy_price
        sell_amount = shares * sell_price
        
        # 计算佣金
        buy_commission = max(buy_amount * commission_rate, min_commission)
        sell_commission = max(sell_amount * commission_rate, min_commission)
        
        # 计算印花税（仅卖出时收取）
        stamp_tax = 0
        if sell_amount > stamp_tax_threshold:
            stamp_tax = sell_amount * tax_rate
        
        # 计算过户费
        buy_transfer_fee = buy_amount * transfer_fee
        sell_transfer_fee = sell_amount * transfer_fee
        
        # 计算经手费
        buy_handling_fee = buy_amount * handling_fee
        sell_handling_fee = sell_amount * handling_fee
        
        # 计算证管费
        buy_regulatory_fee = buy_amount * regulatory_fee
        sell_regulatory_fee = sell_amount * regulatory_fee
        
        # 计算滑点成本
        slippage_cost = shares * (sell_price - buy_price) * slippage
        
        # 计算总成本
        total_cost = (
            buy_commission + sell_commission + stamp_tax +
            buy_transfer_fee + sell_transfer_fee +
            buy_handling_fee + sell_handling_fee +
            buy_regulatory_fee + sell_regulatory_fee +
            slippage_cost
        )
        
        # 构建成本明细
        cost_details = {
            'total_cost': total_cost,
            'buy_costs': {
                'commission': buy_commission,
                'transfer_fee': buy_transfer_fee,
                'handling_fee': buy_handling_fee,
                'regulatory_fee': buy_regulatory_fee
            },
            'sell_costs': {
                'commission': sell_commission,
                'transfer_fee': sell_transfer_fee,
                'handling_fee': sell_handling_fee,
                'regulatory_fee': sell_regulatory_fee,
                'stamp_tax': stamp_tax
            },
            'other_costs': {
                'slippage': slippage_cost
            },
            'breakdown': {
                'commission': buy_commission + sell_commission,
                'stamp_tax': stamp_tax,
                'transfer_fee': buy_transfer_fee + sell_transfer_fee,
                'handling_fee': buy_handling_fee + sell_handling_fee,
                'regulatory_fee': buy_regulatory_fee + sell_regulatory_fee,
                'slippage': slippage_cost
            },
            'trade_info': {
                'shares': shares,
                'buy_price': buy_price,
                'sell_price': sell_price,
                'buy_amount': buy_amount,
                'sell_amount': sell_amount,
                'market': self.market,
                'trade_type': trade_type
            }
        }
        
        return cost_details
    
    def calculate_round_trip_cost(self, shares: float, price: float, trade_type: str = 'stock') -> Dict[str, Any]:
        """
        计算往返交易成本（买入后卖出）
        
        Args:
            shares: 交易股数
            price: 交易价格
            trade_type: 交易类型
            
        Returns:
            Dict[str, Any]: 详细的交易成本明细
        """
        return self.calculate_cost(shares, price, price, trade_type)
    
    def calculate_cost_rate(self, shares: float, buy_price: float, sell_price: float, trade_type: str = 'stock') -> float:
        """
        计算成本率
        
        Args:
            shares: 交易股数
            buy_price: 买入价格
            sell_price: 卖出价格
            trade_type: 交易类型
            
        Returns:
            float: 成本率（总成本 / 卖出金额）
        """
        cost_details = self.calculate_cost(shares, buy_price, sell_price, trade_type)
        total_cost = cost_details['total_cost']
        sell_amount = cost_details['trade_info']['sell_amount']
        
        if sell_amount == 0:
            return 0
        
        return total_cost / sell_amount
    
    def get_market_info(self) -> Dict[str, Any]:
        """
        获取当前市场的成本参数信息
        
        Returns:
            Dict[str, Any]: 市场成本参数信息
        """
        return {
            'market': self.market,
            'params': self.params
        }
