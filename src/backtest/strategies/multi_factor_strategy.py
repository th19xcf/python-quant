#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
多因子选股策略实现
"""

from typing import Dict, Any, List
from src.backtest.strategies.base_strategy import BaseStrategy


class MultiFactorStrategy(BaseStrategy):
    """
    多因子选股策略，基于多个Alpha因子的综合评分
    """
    
    def __init__(self):
        """
        初始化多因子选股策略
        """
        super().__init__("MultiFactor")
        # 默认参数
        self.params = {
            'factors': ['momentum_1m', 'rsi_14', 'bollinger_position', 'volume_momentum_10'],
            'weights': None,  # 默认为等权重
            'threshold': 0.5  # 买入阈值
        }
    
    def generate_signal(self, data: Dict[str, Any], index: int) -> str:
        """
        生成交易信号
        
        Args:
            data: 当前数据
            index: 当前数据索引
            
        Returns:
            str: 交易信号，'buy'、'sell'或'hold'
        """
        factors = self.params.get('factors', [])
        weights = self.params.get('weights', None)
        threshold = self.params.get('threshold', 0.5)
        
        # 检查是否有足够的数据
        if not factors:
            return 'hold'
        
        # 计算因子评分
        scores = []
        valid_factors = []
        
        for factor in factors:
            if factor in data:
                factor_value = data[factor]
                # 标准化因子值
                normalized_score = self._normalize_factor(factor, factor_value)
                scores.append(normalized_score)
                valid_factors.append(factor)
        
        if not valid_factors:
            return 'hold'
        
        # 计算权重
        if weights is None:
            # 等权重
            factor_weights = [1.0 / len(valid_factors)] * len(valid_factors)
        else:
            # 使用提供的权重
            factor_weights = []
            for factor in valid_factors:
                factor_weights.append(weights.get(factor, 1.0 / len(valid_factors)))
        
        # 计算综合评分
        total_score = 0
        for score, weight in zip(scores, factor_weights):
            total_score += score * weight
        
        # 生成信号
        if total_score > threshold:
            return 'buy'
        elif total_score < -threshold:
            return 'sell'
        else:
            return 'hold'
    
    def _normalize_factor(self, factor_name: str, factor_value: float) -> float:
        """
        标准化因子值
        
        Args:
            factor_name: 因子名称
            factor_value: 因子值
            
        Returns:
            float: 标准化后的因子值，范围在[-1, 1]
        """
        # 根据因子类型进行标准化
        if factor_name in ['momentum_1m', 'momentum_3m', 'momentum_6m']:
            # 动量因子，范围可能很大，使用tanh函数标准化
            return max(-1, min(1, factor_value * 10))
        elif factor_name in ['rsi_14', 'rsi_21']:
            # RSI因子，范围[0, 100]，标准化到[-1, 1]
            return (factor_value - 50) / 50
        elif factor_name in ['bollinger_position']:
            # 布林带位置，已经在[-1, 1]范围内
            return factor_value
        elif factor_name in ['volume_momentum_10', 'volume_momentum_20']:
            # 成交量动量，使用tanh函数标准化
            return max(-1, min(1, factor_value * 5))
        elif factor_name in ['volatility_20', 'volatility_60']:
            # 波动率因子，较低的波动率更好
            return max(-1, min(1, -factor_value * 100))
        else:
            # 默认标准化
            return max(-1, min(1, factor_value))
