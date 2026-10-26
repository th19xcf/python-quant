#!/usr/bin/env python3

"""
风险因子计算模块
"""

import numpy as np
import polars as pl

from src.alpha.factors.factor_registry import register_factor
from src.alpha.factors.partition import PartitionAware

_SQRT_252 = np.sqrt(252)


@register_factor('risk')
class RiskFactors(PartitionAware):
    """
    风险因子计算类
    """

    def __init__(self, data, partition_by=None):
        """
        初始化风险因子计算类

        Args:
            data: Polars DataFrame，包含价格数据
            partition_by: 标的代码列名；传 None 时为单标的序列
        """
        super().__init__(data, partition_by)

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

    def _returns_expr(self) -> pl.Expr:
        """
        收益率表达式，首行为0与原实现一致
        """
        close = pl.col('close')
        prev_close = close.shift(1)
        return (
            pl.when(prev_close.is_not_null())
            .then((close - prev_close) / prev_close)
            .otherwise(0.0)
        )

    def _calculate_beta(self):
        """
        计算Beta系数
        """
        cdt = self.data['close'].dtype
        returns = self._returns_expr()

        # 计算市场收益率（这里使用自身作为市场代理）
        # 与原实现一致：np.cov 默认 ddof=1，np.var 默认 ddof=0
        cov = pl.rolling_cov(returns, returns, window_size=20, ddof=1).shift(1)
        var = returns.rolling_var(20, ddof=0).shift(1)

        # 计算Beta系数
        beta = (
            pl.when(var > 0)
            .then(cov / var)
            .otherwise(0.0)
        )

        # 添加到DataFrame
        self.data = self.data.with_columns(
            self._p(beta).cast(cdt, strict=False).alias('beta')
        )

    def _calculate_max_drawdown(self):
        """
        计算最大回撤
        """
        cdt = self.data['close'].dtype
        close = pl.col('close')

        # 前20日窗口（不含当日）的最高/最低价
        peak = close.rolling_max(20).shift(1)
        trough = close.rolling_min(20).shift(1)
        max_drawdown = (
            pl.when(peak > 0)
            .then((trough - peak) / peak)
            .otherwise(0.0)
        )

        # 添加到DataFrame
        self.data = self.data.with_columns(
            self._p(max_drawdown).cast(cdt, strict=False).alias('max_drawdown_20')
        )

    def _calculate_sharpe_ratio(self):
        """
        计算夏普比率
        """
        cdt = self.data['close'].dtype
        returns = self._returns_expr()

        # 计算夏普比率（假设无风险利率为0；ddof=0与np.std一致）
        mean_return = returns.rolling_mean(20).shift(1)
        std_return = returns.rolling_std(20, ddof=0).shift(1)
        sharpe_ratio = (
            pl.when(std_return > 0)
            .then(mean_return / std_return * _SQRT_252)
            .otherwise(0.0)
        )

        # 添加到DataFrame
        self.data = self.data.with_columns(
            self._p(sharpe_ratio).cast(cdt, strict=False).alias('sharpe_ratio_20')
        )

    def _calculate_downside_risk(self):
        """
        计算下行风险
        """
        cdt = self.data['close'].dtype
        returns = self._returns_expr()

        # 前20日窗口（不含当日）内负收益率的标准差（ddof=0与np.std一致）
        neg_count = (returns < 0).cast(pl.Float64).rolling_sum(20).shift(1)
        neg_sum = (
            pl.when(returns < 0).then(returns).otherwise(0.0)
            .rolling_sum(20).shift(1)
        )
        neg_sq_sum = (
            pl.when(returns < 0).then(returns * returns).otherwise(0.0)
            .rolling_sum(20).shift(1)
        )
        neg_mean = neg_sum / neg_count
        neg_var = (neg_sq_sum / neg_count - neg_mean * neg_mean).clip(lower_bound=0.0)
        downside_risk = (
            pl.when(neg_count > 0)
            .then(neg_var.sqrt() * _SQRT_252)
            .otherwise(0.0)
        )

        # 添加到DataFrame
        self.data = self.data.with_columns(
            self._p(downside_risk).cast(cdt, strict=False).alias('downside_risk_20')
        )
