#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
价格动量因子计算模块
"""

import polars as pl
import numpy as np


def _seeded_ema_expr(col_name: str, period: int) -> pl.Expr:
    """
    与原 calculate_ema 逐位等价的EMA表达式：
    以第 period-1 个位置的前 period 个值的均值作为种子，之后按
    ema[i] = (values[i] - ema[i-1]) * multiplier + ema[i-1] 递推，
    multiplier = 2 / (period + 1)，种子位置之前为 0。
    """
    multiplier = 2.0 / (period + 1)
    idx = pl.int_range(pl.len())
    seeded = (
        pl.when(idx > period - 1).then(pl.col(col_name))
        .when(idx == period - 1).then(pl.col(col_name).rolling_mean(period))
        .otherwise(None)
    )
    return seeded.ewm_mean(alpha=multiplier, adjust=False, min_samples=1).fill_null(0.0)


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
        delta = pl.col('close').diff()
        gain = delta.clip(lower_bound=0.0)
        loss = (-delta).clip(lower_bound=0.0)

        def rsi_expr(period: int) -> pl.Expr:
            avg_gain = gain.rolling_mean(period)
            avg_loss = loss.rolling_mean(period)
            rs = avg_gain / avg_loss
            return (
                pl.when(avg_loss == 0)
                .then(100.0)
                .otherwise(100.0 - (100.0 / (1.0 + rs)))
                .fill_null(0.0)
            )

        # 添加到DataFrame
        self.data = self.data.with_columns([
            rsi_expr(14).cast(cdt, strict=False).alias('rsi_14'),
            rsi_expr(21).cast(cdt, strict=False).alias('rsi_21')
        ])

    def _calculate_macd_momentum(self):
        """
        计算MACD动量因子
        """
        cdt = self.data['close'].dtype

        # 计算EMA（与原 calculate_ema 等价：SMA种子 + 递推）
        self.data = self.data.with_columns([
            _seeded_ema_expr('close', 12).cast(cdt, strict=False).alias('_ema12'),
            _seeded_ema_expr('close', 26).cast(cdt, strict=False).alias('_ema26')
        ])

        # 计算MACD
        self.data = self.data.with_columns(
            (pl.col('_ema12') - pl.col('_ema26')).cast(cdt, strict=False).alias('macd')
        )
        self.data = self.data.with_columns(
            _seeded_ema_expr('macd', 9).cast(cdt, strict=False).alias('macd_signal')
        )
        self.data = self.data.with_columns(
            (pl.col('macd') - pl.col('macd_signal')).cast(cdt, strict=False).alias('macd_hist')
        )
        self.data = self.data.drop('_ema12', '_ema26')

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
