#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
风险因子计算模块
"""

import polars as pl
import numpy as np


class RiskFactors:
    """
    风险因子计算类
    """
    
    def __init__(self, data):
        """
        初始化风险因子计算类
        
        Args:
            data: Polars DataFrame，包含价格数据
        """
        self.data = data
    
    def calculate(self, **params) -> pl.DataFrame:
        """
        计算风险因子
        
        Args:
            **params: 计算参数
            
        Returns:
            pl.DataFrame: 包含计算结果的DataFrame
        """
        # 计算各种风险因子
        self._calculate_beta()
        self._calculate_max_drawdown()
        self._calculate_sharpe_ratio()
        self._calculate_downside_risk()
        
        return self.data
    
    def _calculate_beta(self):
        """
        计算Beta系数
        """
        close = self.data['close'].to_numpy()
        
        # 计算收益率
        returns = np.zeros_like(close)
        for i in range(1, len(close)):
            returns[i] = (close[i] - close[i-1]) / close[i-1]
        
        # 计算市场收益率（这里使用自身作为市场代理）
        market_returns = returns.copy()
        
        # 计算Beta系数
        beta = np.zeros_like(close)
        for i in range(20, len(close)):
            cov = np.cov(returns[i-20:i], market_returns[i-20:i])[0, 1]
            var = np.var(market_returns[i-20:i])
            if var > 0:
                beta[i] = cov / var
        
        # 添加到DataFrame
        self.data = self.data.with_columns(
            pl.Series('beta', beta)
        )
    
    def _calculate_max_drawdown(self):
        """
        计算最大回撤
        """
        close = self.data['close'].to_numpy()
        
        max_drawdown = np.zeros_like(close)
        for i in range(20, len(close)):
            window = close[i-20:i]
            peak = np.max(window)
            trough = np.min(window)
            if peak > 0:
                max_drawdown[i] = (trough - peak) / peak
        
        # 添加到DataFrame
        self.data = self.data.with_columns(
            pl.Series('max_drawdown_20', max_drawdown)
        )
    
    def _calculate_sharpe_ratio(self):
        """
        计算夏普比率
        """
        close = self.data['close'].to_numpy()
        
        # 计算收益率
        returns = np.zeros_like(close)
        for i in range(1, len(close)):
            returns[i] = (close[i] - close[i-1]) / close[i-1]
        
        # 计算夏普比率（假设无风险利率为0）
        sharpe_ratio = np.zeros_like(close)
        for i in range(20, len(close)):
            window_returns = returns[i-20:i]
            mean_return = np.mean(window_returns)
            std_return = np.std(window_returns)
            if std_return > 0:
                sharpe_ratio[i] = mean_return / std_return * np.sqrt(252)
        
        # 添加到DataFrame
        self.data = self.data.with_columns(
            pl.Series('sharpe_ratio_20', sharpe_ratio)
        )
    
    def _calculate_downside_risk(self):
        """
        计算下行风险
        """
        close = self.data['close'].to_numpy()
        
        # 计算收益率
        returns = np.zeros_like(close)
        for i in range(1, len(close)):
            returns[i] = (close[i] - close[i-1]) / close[i-1]
        
        # 计算下行风险
        downside_risk = np.zeros_like(close)
        for i in range(20, len(close)):
            window_returns = returns[i-20:i]
            negative_returns = window_returns[window_returns < 0]
            if len(negative_returns) > 0:
                downside_risk[i] = np.std(negative_returns) * np.sqrt(252)
        
        # 添加到DataFrame
        self.data = self.data.with_columns(
            pl.Series('downside_risk_20', downside_risk)
        )
