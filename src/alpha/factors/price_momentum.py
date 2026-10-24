#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
价格动量因子计算模块
"""

import polars as pl
import numpy as np

from src.alpha.factors.factor_registry import register_factor
from src.tech_analysis.indicators.formulas import (
    macd_exprs,
    rsi_expr as shared_rsi_expr,
)


def _seeded_kdj_expr(value_expr: pl.Expr, seed_index: int, seed_value: float) -> pl.Expr:
    """
    与原KDJ递推等价的表达式：
    第 seed_index 个位置取 seed_value，之后按
    out[i] = out[i-1] * 2/3 + value[i] * 1/3 递推，种子位置之前为 0。
    """
    idx = pl.int_range(pl.len())
    seeded = (
        pl.when(idx > seed_index).then(value_expr)
        .when(idx == seed_index).then(seed_value)
        .otherwise(None)
    )
    return seeded.ewm_mean(alpha=1.0 / 3.0, adjust=False, min_samples=1).fill_null(0.0)


@register_factor('price_momentum')
class PriceMomentumFactors:
    """
    价格动量因子计算类
    """

    def __init__(self, data):
        """
        初始化价格动量因子计算类

        Args:
            data: Polars DataFrame，包含价格数据
        """
        self.data = data

    def calculate(self, **params) -> pl.DataFrame:
        """
        计算价格动量因子

        Args:
            **params: 计算参数

        Returns:
            pl.DataFrame: 包含计算结果的DataFrame
        """
        # 计算各种动量因子
        self._calculate_momentum()
        self._calculate_rsi_momentum()
        self._calculate_macd_momentum()
        self._calculate_stochastic_momentum()

        return self.data

    def _calculate_momentum(self):
        """
        计算动量因子
        """
        cdt = self.data['close'].dtype
        close = pl.col('close')

        # 短期动量（1个月）
        momentum_1m = (close - close.shift(20)) / close.shift(20)
        # 中期动量（3个月）
        momentum_3m = (close - close.shift(60)) / close.shift(60)
        # 长期动量（6个月）
        momentum_6m = (close - close.shift(120)) / close.shift(120)
        # 动量反转（12个月）
        momentum_12m = (close - close.shift(240)) / close.shift(240)

        # 添加到DataFrame
        self.data = self.data.with_columns([
            momentum_1m.fill_null(0.0).cast(cdt, strict=False).alias('momentum_1m'),
            momentum_3m.fill_null(0.0).cast(cdt, strict=False).alias('momentum_3m'),
            momentum_6m.fill_null(0.0).cast(cdt, strict=False).alias('momentum_6m'),
            momentum_12m.fill_null(0.0).cast(cdt, strict=False).alias('momentum_12m')
        ])

    def _calculate_rsi_momentum(self):
        """
        计算RSI动量因子
        """
        cdt = self.data['close'].dtype

        # RSI 统一走共享公式层（Wilder 标准口径）
        # 添加到DataFrame
        self.data = self.data.with_columns([
            shared_rsi_expr('close', 14).fill_null(0.0).cast(cdt, strict=False).alias('rsi_14'),
            shared_rsi_expr('close', 21).fill_null(0.0).cast(cdt, strict=False).alias('rsi_21')
        ])

    def _calculate_macd_momentum(self):
        """
        计算MACD动量因子
        """
        cdt = self.data['close'].dtype

        # MACD 统一走共享公式层（标准 EMA 口径）
        exprs = macd_exprs('close', 12, 26, 9)
        self.data = self.data.with_columns([
            exprs['macd'].fill_null(0.0).cast(cdt, strict=False).alias('macd'),
            exprs['signal'].fill_null(0.0).cast(cdt, strict=False).alias('macd_signal'),
            exprs['hist'].fill_null(0.0).cast(cdt, strict=False).alias('macd_hist'),
        ])

    def _calculate_stochastic_momentum(self):
        """
        计算随机动量因子
        """
        cdt = self.data['close'].dtype

        # 计算RSV（前14日窗口，不含当日；最高价等于最低价时为0）
        highest = pl.col('high').rolling_max(14).shift(1)
        lowest = pl.col('low').rolling_min(14).shift(1)
        rsv = (
            pl.when(highest == lowest)
            .then(0.0)
            .otherwise((pl.col('close') - lowest) / (highest - lowest) * 100.0)
        )

        # 计算K（i==14时取50，之后 k = k*2/3 + rsv*1/3）
        self.data = self.data.with_columns(
            _seeded_kdj_expr(rsv, 14, 50.0).cast(cdt, strict=False).alias('kdj_k')
        )
        # 计算D（i==14时取50，之后 d = d*2/3 + k*1/3）
        self.data = self.data.with_columns(
            _seeded_kdj_expr(pl.col('kdj_k'), 14, 50.0).cast(cdt, strict=False).alias('kdj_d')
        )
        # 计算J
        self.data = self.data.with_columns(
            (3.0 * pl.col('kdj_k') - 2.0 * pl.col('kdj_d')).cast(cdt, strict=False).alias('kdj_j')
        )
