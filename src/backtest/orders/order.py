#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
订单类，用于表示不同类型的交易订单
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime


class Order(ABC):
    """
    订单基类
    """
    
    def __init__(self, order_id: str, symbol: str, quantity: float, side: str, order_type: str):
        """
        初始化订单
        
        Args:
            order_id: 订单ID
            symbol: 股票代码
            quantity: 交易数量
            side: 交易方向，'buy'或'sell'
            order_type: 订单类型，'market'（市价单）、'limit'（限价单）、'stop'（止损单）
        """
        self.order_id = order_id
        self.symbol = symbol
        self.quantity = quantity
        self.side = side
        self.order_type = order_type
        self.status = 'pending'  # pending, filled, cancelled
        self.created_at = datetime.now()
        self.filled_at = None
        self.filled_price = None
        self.filled_quantity = 0
    
    @abstractmethod
    def should_execute(self, current_price: float) -> bool:
        """
        判断订单是否应该执行
        
        Args:
            current_price: 当前价格
            
        Returns:
            bool: 是否应该执行
        """
        pass
    
    def execute(self, execution_price: float, execution_quantity: Optional[float] = None):
        """
        执行订单
        
        Args:
            execution_price: 执行价格
            execution_quantity: 执行数量，默认全部执行
        """
        if execution_quantity is None:
            execution_quantity = self.quantity
        
        self.filled_price = execution_price
        self.filled_quantity = execution_quantity
        self.filled_at = datetime.now()
        self.status = 'filled'
    
    def cancel(self):
        """
        取消订单
        """
        self.status = 'cancelled'
    
    def to_dict(self) -> Dict[str, Any]:
        """
        将订单转换为字典
        
        Returns:
            Dict[str, Any]: 订单字典
        """
        return {
            'order_id': self.order_id,
            'symbol': self.symbol,
            'quantity': self.quantity,
            'side': self.side,
            'order_type': self.order_type,
            'status': self.status,
            'created_at': self.created_at,
            'filled_at': self.filled_at,
            'filled_price': self.filled_price,
            'filled_quantity': self.filled_quantity
        }


class MarketOrder(Order):
    """
    市价单
    """
    
    def __init__(self, order_id: str, symbol: str, quantity: float, side: str):
        """
        初始化市价单
        
        Args:
            order_id: 订单ID
            symbol: 股票代码
            quantity: 交易数量
            side: 交易方向，'buy'或'sell'
        """
        super().__init__(order_id, symbol, quantity, side, 'market')
    
    def should_execute(self, current_price: float) -> bool:
        """
        判断订单是否应该执行
        
        Args:
            current_price: 当前价格
            
        Returns:
            bool: 是否应该执行
        """
        # 市价单总是应该执行
        return True


class LimitOrder(Order):
    """
    限价单
    """
    
    def __init__(self, order_id: str, symbol: str, quantity: float, side: str, limit_price: float):
        """
        初始化限价单
        
        Args:
            order_id: 订单ID
            symbol: 股票代码
            quantity: 交易数量
            side: 交易方向，'buy'或'sell'
            limit_price: 限价
        """
        super().__init__(order_id, symbol, quantity, side, 'limit')
        self.limit_price = limit_price
    
    def should_execute(self, current_price: float) -> bool:
        """
        判断订单是否应该执行
        
        Args:
            current_price: 当前价格
            
        Returns:
            bool: 是否应该执行
        """
        if self.side == 'buy':
            # 买入限价单，当当前价格小于等于限价时执行
            return current_price <= self.limit_price
        else:
            # 卖出限价单，当当前价格大于等于限价时执行
            return current_price >= self.limit_price
    
    def to_dict(self) -> Dict[str, Any]:
        """
        将订单转换为字典
        
        Returns:
            Dict[str, Any]: 订单字典
        """
        order_dict = super().to_dict()
        order_dict['limit_price'] = self.limit_price
        return order_dict


class StopOrder(Order):
    """
    止损单
    """
    
    def __init__(self, order_id: str, symbol: str, quantity: float, side: str, stop_price: float):
        """
        初始化止损单
        
        Args:
            order_id: 订单ID
            symbol: 股票代码
            quantity: 交易数量
            side: 交易方向，'buy'或'sell'
            stop_price: 止损价格
        """
        super().__init__(order_id, symbol, quantity, side, 'stop')
        self.stop_price = stop_price
    
    def should_execute(self, current_price: float) -> bool:
        """
        判断订单是否应该执行
        
        Args:
            current_price: 当前价格
            
        Returns:
            bool: 是否应该执行
        """
        if self.side == 'buy':
            # 买入止损单，当当前价格大于等于止损价格时执行
            return current_price >= self.stop_price
        else:
            # 卖出止损单，当当前价格小于等于止损价格时执行
            return current_price <= self.stop_price
    
    def to_dict(self) -> Dict[str, Any]:
        """
        将订单转换为字典
        
        Returns:
            Dict[str, Any]: 订单字典
        """
        order_dict = super().to_dict()
        order_dict['stop_price'] = self.stop_price
        return order_dict


class StopLimitOrder(Order):
    """
    止损限价单
    """
    
    def __init__(self, order_id: str, symbol: str, quantity: float, side: str, stop_price: float, limit_price: float):
        """
        初始化止损限价单
        
        Args:
            order_id: 订单ID
            symbol: 股票代码
            quantity: 交易数量
            side: 交易方向，'buy'或'sell'
            stop_price: 止损价格
            limit_price: 限价
        """
        super().__init__(order_id, symbol, quantity, side, 'stop_limit')
        self.stop_price = stop_price
        self.limit_price = limit_price
        self.triggered = False
    
    def should_execute(self, current_price: float) -> bool:
        """
        判断订单是否应该执行
        
        Args:
            current_price: 当前价格
            
        Returns:
            bool: 是否应该执行
        """
        # 首先检查是否触发止损价格
        if not self.triggered:
            if self.side == 'buy':
                self.triggered = current_price >= self.stop_price
            else:
                self.triggered = current_price <= self.stop_price
        
        # 触发后，检查是否满足限价条件
        if self.triggered:
            if self.side == 'buy':
                return current_price <= self.limit_price
            else:
                return current_price >= self.limit_price
        
        return False
    
    def to_dict(self) -> Dict[str, Any]:
        """
        将订单转换为字典
        
        Returns:
            Dict[str, Any]: 订单字典
        """
        order_dict = super().to_dict()
        order_dict['stop_price'] = self.stop_price
        order_dict['limit_price'] = self.limit_price
        order_dict['triggered'] = self.triggered
        return order_dict
