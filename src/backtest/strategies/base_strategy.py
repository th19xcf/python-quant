#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
策略基类，为具体策略实现提供统一的接口
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional


class BaseStrategy(ABC):
    """
    策略基类，所有具体策略都应继承此类
    """
    
    def __init__(self, name: str):
        """
        初始化策略
        
        Args:
            name: 策略名称
        """
        self.name = name
        self.params = {}
    
    def set_params(self, params: Dict[str, Any]):
        """
        设置策略参数
        
        Args:
            params: 策略参数
        """
        self.params.update(params)
    
    @abstractmethod
    def generate_signal(self, data: Dict[str, Any], index: int) -> str:
        """
        生成交易信号
        
        Args:
            data: 当前数据
            index: 当前数据索引
            
        Returns:
            str: 交易信号，'buy'、'sell'或'hold'
        """
        pass
