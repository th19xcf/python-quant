#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Alpha因子计算器，用于计算各种Alpha因子
"""

from typing import List, Dict, Optional, Any
import polars as pl
import pandas as pd
import numpy as np
from loguru import logger

from src.alpha.factors.price_momentum import PriceMomentumFactors
from src.alpha.factors.volatility import VolatilityFactors
from src.alpha.factors.volume import VolumeFactors
from src.alpha.factors.technical import TechnicalFactors
from src.alpha.factors.risk import RiskFactors
from src.alpha.evaluation.factor_evaluator import FactorEvaluator


class AlphaCalculator:
    """
    Alpha因子计算器类，提供各种Alpha因子的计算功能
    """
    
    def __init__(self, data):
        """
        初始化Alpha因子计算器
        
        Args:
            data: 股票数据，可以是Polars DataFrame、LazyFrame或Pandas DataFrame
        """
        # 转换为Polars DataFrame
        if isinstance(data, pd.DataFrame):
            self.pl_df = pl.from_pandas(data)
        elif hasattr(data, 'to_pandas'):
            self.pl_df = data
        else:
            self.pl_df = pl.DataFrame(data)
        
        # 初始化各种因子计算类
        self.price_momentum = PriceMomentumFactors(self.pl_df)
        self.volatility = VolatilityFactors(self.pl_df)
        self.volume = VolumeFactors(self.pl_df)
        self.technical = TechnicalFactors(self.pl_df)
        self.risk = RiskFactors(self.pl_df)
        
        # 初始化因子评估器
        self.evaluator = FactorEvaluator()
        
        # 存储计算结果
        self.factor_results = {}
    
    def calculate_factors(self, factor_types: List[str], **params) -> pl.DataFrame:
        """
        计算指定类型的因子
        
        Args:
            factor_types: 因子类型列表
            **params: 因子计算参数
            
        Returns:
            pl.DataFrame: 包含计算结果的Polars DataFrame
        """
        for factor_type in factor_types:
            try:
                if factor_type == 'price_momentum':
                    # 价格动量因子
                    result = self.price_momentum.calculate(**params)
                elif factor_type == 'volatility':
                    # 波动率因子
                    result = self.volatility.calculate(**params)
                elif factor_type == 'volume':
                    # 成交量因子
                    result = self.volume.calculate(**params)
                elif factor_type == 'technical':
                    # 技术指标因子
                    result = self.technical.calculate(**params)
                elif factor_type == 'risk':
                    # 风险因子
                    result = self.risk.calculate(**params)
                else:
                    logger.warning(f"不支持的因子类型: {factor_type}")
                    continue
                
                self.factor_results[factor_type] = result
                logger.info(f"成功计算因子: {factor_type}")
            except Exception as e:
                logger.error(f"计算因子 {factor_type} 失败: {e}")
        
        return self.pl_df
    
    def evaluate_factors(self, factor_names: List[str], target_returns: Optional[pl.Series] = None) -> Dict[str, Any]:
        """
        评估因子有效性
        
        Args:
            factor_names: 因子名称列表
            target_returns: 目标收益率序列
            
        Returns:
            Dict[str, Any]: 评估结果
        """
        if target_returns is None:
            # 计算下一期收益率作为目标
            close = self.pl_df['close'].to_numpy()
            returns = np.zeros_like(close)
            for i in range(len(close) - 1):
                returns[i] = (close[i+1] - close[i]) / close[i]
            target_returns = pl.Series('returns', returns[:-1])
            # 截取数据以匹配目标收益率
            data = self.pl_df.slice(0, len(target_returns))
        else:
            data = self.pl_df
        
        evaluation_results = {}
        
        for factor_name in factor_names:
            if factor_name in data.columns:
                factor_values = data[factor_name]
                ic = self.evaluator.calculate_ic(factor_values, target_returns)
                rank_ic = self.evaluator.calculate_rank_ic(factor_values, target_returns)
                
                evaluation_results[factor_name] = {
                    'ic': ic,
                    'rank_ic': rank_ic
                }
                logger.info(f"因子 {factor_name} 评估完成: IC={ic:.4f}, Rank IC={rank_ic:.4f}")
            else:
                logger.warning(f"因子 {factor_name} 不存在")
        
        return evaluation_results
    
    def analyze_factor_correlation(self, factor_names: List[str]) -> Dict[str, Any]:
        """
        分析因子相关性
        
        Args:
            factor_names: 因子名称列表
            
        Returns:
            Dict[str, Any]: 相关性分析结果
        """
        # 提取因子数据
        factor_data = []
        valid_factors = []
        
        for factor_name in factor_names:
            if factor_name in self.pl_df.columns:
                factor_data.append(self.pl_df[factor_name].to_numpy())
                valid_factors.append(factor_name)
            else:
                logger.warning(f"因子 {factor_name} 不存在")
        
        if not valid_factors:
            return {}
        
        # 计算相关性矩阵
        correlation_matrix = np.corrcoef(factor_data)
        
        # 构建相关性字典
        correlation_dict = {}
        for i, factor1 in enumerate(valid_factors):
            correlation_dict[factor1] = {}
            for j, factor2 in enumerate(valid_factors):
                correlation_dict[factor1][factor2] = correlation_matrix[i, j]
        
        return correlation_dict
    
    def optimize_factor_weights(self, factor_names: List[str], target_returns: Optional[pl.Series] = None) -> Dict[str, float]:
        """
        优化因子权重
        
        Args:
            factor_names: 因子名称列表
            target_returns: 目标收益率序列
            
        Returns:
            Dict[str, float]: 优化后的因子权重
        """
        if target_returns is None:
            # 计算下一期收益率作为目标
            close = self.pl_df['close'].to_numpy()
            returns = np.zeros_like(close)
            for i in range(len(close) - 1):
                returns[i] = (close[i+1] - close[i]) / close[i]
            target_returns = pl.Series('returns', returns[:-1])
            # 截取数据以匹配目标收益率
            data = self.pl_df.slice(0, len(target_returns))
        else:
            data = self.pl_df
        
        # 提取因子数据
        factor_data = []
        valid_factors = []
        
        for factor_name in factor_names:
            if factor_name in data.columns:
                factor_data.append(data[factor_name].to_numpy())
                valid_factors.append(factor_name)
            else:
                logger.warning(f"因子 {factor_name} 不存在")
        
        if not valid_factors:
            return {}
        
        # 计算因子与目标收益率的相关性
        correlations = []
        for factor_values in factor_data:
            corr = np.corrcoef(factor_values, target_returns.to_numpy())[0, 1]
            correlations.append(abs(corr))
        
        # 基于相关性计算权重
        total_corr = sum(correlations)
        weights = {}
        for i, factor_name in enumerate(valid_factors):
            weights[factor_name] = correlations[i] / total_corr if total_corr > 0 else 1.0 / len(valid_factors)
        
        return weights
    
    def get_factor_results(self) -> Dict[str, Any]:
        """
        获取因子计算结果
        
        Returns:
            Dict[str, Any]: 因子计算结果
        """
        return self.factor_results
