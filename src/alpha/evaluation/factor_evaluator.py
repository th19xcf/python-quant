#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
因子评估器，用于评估因子的有效性
"""

import polars as pl
import numpy as np
from scipy.stats import spearmanr, pearsonr


class FactorEvaluator:
    """
    因子评估器类，提供因子有效性评估功能
    """
    
    def calculate_ic(self, factor_values: pl.Series, target_returns: pl.Series) -> float:
        """
        计算信息系数(IC)
        
        Args:
            factor_values: 因子值序列
            target_returns: 目标收益率序列
            
        Returns:
            float: 信息系数
        """
        # 转换为numpy数组
        factor = factor_values.to_numpy()
        returns = target_returns.to_numpy()
        
        # 移除NaN值
        valid_mask = ~(np.isnan(factor) | np.isnan(returns))
        factor_valid = factor[valid_mask]
        returns_valid = returns[valid_mask]
        
        if len(factor_valid) < 2:
            return 0.0
        
        # 计算皮尔逊相关系数
        ic, _ = pearsonr(factor_valid, returns_valid)
        return ic
    
    def calculate_rank_ic(self, factor_values: pl.Series, target_returns: pl.Series) -> float:
        """
        计算秩相关系数(Rank IC)
        
        Args:
            factor_values: 因子值序列
            target_returns: 目标收益率序列
            
        Returns:
            float: 秩相关系数
        """
        # 转换为numpy数组
        factor = factor_values.to_numpy()
        returns = target_returns.to_numpy()
        
        # 移除NaN值
        valid_mask = ~(np.isnan(factor) | np.isnan(returns))
        factor_valid = factor[valid_mask]
        returns_valid = returns[valid_mask]
        
        if len(factor_valid) < 2:
            return 0.0
        
        # 计算斯皮尔曼相关系数
        rank_ic, _ = spearmanr(factor_valid, returns_valid)
        return rank_ic
    
    def perform_quantile_test(self, factor_values: pl.Series, target_returns: pl.Series, n_quantiles: int = 5) -> dict:
        """
        执行分层测试
        
        Args:
            factor_values: 因子值序列
            target_returns: 目标收益率序列
            n_quantiles: 分位数数量
            
        Returns:
            dict: 分层测试结果
        """
        # 转换为numpy数组
        factor = factor_values.to_numpy()
        returns = target_returns.to_numpy()
        
        # 移除NaN值
        valid_mask = ~(np.isnan(factor) | np.isnan(returns))
        factor_valid = factor[valid_mask]
        returns_valid = returns[valid_mask]
        
        if len(factor_valid) < n_quantiles:
            return {}
        
        # 计算分位数
        quantiles = np.linspace(0, 1, n_quantiles + 1)
        factor_quantiles = np.percentile(factor_valid, quantiles * 100)
        
        # 分层回测
        quantile_returns = []
        for i in range(n_quantiles):
            mask = (factor_valid >= factor_quantiles[i]) & (factor_valid < factor_quantiles[i+1])
            if np.sum(mask) > 0:
                quantile_return = np.mean(returns_valid[mask])
                quantile_returns.append(quantile_return)
            else:
                quantile_returns.append(0.0)
        
        # 计算多空收益
        if len(quantile_returns) >= 2:
            long_short_return = quantile_returns[-1] - quantile_returns[0]
        else:
            long_short_return = 0.0
        
        return {
            'quantile_returns': quantile_returns,
            'long_short_return': long_short_return,
            'n_quantiles': n_quantiles
        }
    
    def evaluate_factor(self, factor_values: pl.Series, target_returns: pl.Series) -> dict:
        """
        综合评估因子
        
        Args:
            factor_values: 因子值序列
            target_returns: 目标收益率序列
            
        Returns:
            dict: 综合评估结果
        """
        ic = self.calculate_ic(factor_values, target_returns)
        rank_ic = self.calculate_rank_ic(factor_values, target_returns)
        quantile_test = self.perform_quantile_test(factor_values, target_returns)
        
        return {
            'ic': ic,
            'rank_ic': rank_ic,
            'quantile_test': quantile_test,
            'factor_name': factor_values.name
        }
