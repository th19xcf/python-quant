#!/usr/bin/env python3

"""
因子评估器（旧版单序列时序 IC，兼容保留）

本模块的 IC 是**时序 IC（ts_ic）**：把因子值与下一期收益按时间顺序做相关。
它在选股/截面场景不可信；截面 IC 请使用 ``alphalens_adapter``（analytics 可选组）。

为避免破坏既有调用方，``calculate_ic`` / ``calculate_rank_ic`` 的签名与返回值
保持不变；另提供 ``calculate_ts_ic`` / ``calculate_ts_rank_ic`` 作为显式别名，
新代码应使用别名以表明口径。
"""

import numpy as np
import polars as pl
from scipy.stats import pearsonr, spearmanr


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

    def calculate_ts_ic(self, factor_values: pl.Series, target_returns: pl.Series) -> float:
        """时序 IC 的显式别名（兼容旧实现，口径见模块文档）。"""
        return self.calculate_ic(factor_values, target_returns)

    def calculate_ts_rank_ic(self, factor_values: pl.Series,
                             target_returns: pl.Series) -> float:
        """时序 Rank IC 的显式别名（兼容旧实现，口径见模块文档）。"""
        return self.calculate_rank_ic(factor_values, target_returns)

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
