#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
技术指标因子计算模块
"""

import polars as pl
import numpy as np


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


class TechnicalFactors:
    """
    技术指标因子计算类
    """

    def __init__(self, data):
        """
        初始化技术指标因子计算类

        Args:
            data: Polars DataFrame，包含价格数据
        """
        self.data = data

    def calculate(self, **params) -> pl.DataFrame:
        """
        计算技术指标因子

        Args:
            **params: 计算参数

        Returns:
            pl.DataFrame: 包含计算结果的DataFrame
        """
        # 计算各种技术指标因子
        self._calculate_bollinger_bands()
        self._calculate_moving_average_crossovers()
        self._calculate_relative_strength()
        self._calculate_stochastic_oscillator()

        return self.data

    def _calculate_bollinger_bands(self):
        """
        计算布林带因子
        """
        cdt = self.data['close'].dtype
        close = pl.col('close')

        # 计算20日移动平均和标准差（前20日窗口，不含当日；ddof=0与np.std一致）
        ma20 = close.rolling_mean(20).shift(1)
        std20 = close.rolling_std(20, ddof=0).shift(1)
        upper_band = ma20 + 2.0 * std20
        lower_band = ma20 - 2.0 * std20
        bollinger_width = (upper_band - lower_band) / ma20
        bollinger_position = (close - ma20) / (upper_band - lower_band)

        # 添加到DataFrame
        self.data = self.data.with_columns([
            ma20.fill_null(0.0).cast(cdt, strict=False).alias('bollinger_ma20'),
            upper_band.fill_null(0.0).cast(cdt, strict=False).alias('bollinger_upper'),
            lower_band.fill_null(0.0).cast(cdt, strict=False).alias('bollinger_lower'),
            bollinger_width.fill_null(0.0).cast(cdt, strict=False).alias('bollinger_width'),
            bollinger_position.fill_null(0.0).cast(cdt, strict=False).alias('bollinger_position')
        ])

    def _calculate_moving_average_crossovers(self):
        """
        计算移动平均线交叉因子
        """
        cdt = self.data['close'].dtype
        close = pl.col('close')
        idx = pl.int_range(pl.len())

        # 计算不同周期的移动平均线（前N日均值，不含当日；与原实现一致仅在 i>=60 处非零）
        def ma(period: int) -> pl.Expr:
            return (
                pl.when(idx >= 60)
                .then(close.rolling_mean(period).shift(1))
                .otherwise(0.0)
            )

        self.data = self.data.with_columns([
            ma(5).cast(cdt, strict=False).alias('ma5'),
            ma(10).cast(cdt, strict=False).alias('ma10'),
            ma(20).cast(cdt, strict=False).alias('ma20'),
            ma(60).cast(cdt, strict=False).alias('ma60')
        ])

        # 计算移动平均线交叉信号（金叉1，死叉-1；i<60 时各均线为0，条件天然不成立）
        ma5 = pl.col('ma5')
        ma10 = pl.col('ma10')
        ma20 = pl.col('ma20')
        ma60 = pl.col('ma60')
        ma_crossover_5_20 = (
            pl.when((ma5 > ma20) & (ma5.shift(1) <= ma20.shift(1))).then(1.0)
            .when((ma5 < ma20) & (ma5.shift(1) >= ma20.shift(1))).then(-1.0)
            .otherwise(0.0)
        )
        ma_crossover_10_60 = (
            pl.when((ma10 > ma60) & (ma10.shift(1) <= ma60.shift(1))).then(1.0)
            .when((ma10 < ma60) & (ma10.shift(1) >= ma60.shift(1))).then(-1.0)
            .otherwise(0.0)
        )

        # 添加到DataFrame
        self.data = self.data.with_columns([
            ma_crossover_5_20.cast(cdt, strict=False).alias('ma_crossover_5_20'),
            ma_crossover_10_60.cast(cdt, strict=False).alias('ma_crossover_10_60')
        ])

    def _calculate_relative_strength(self):
        """
        计算相对强弱因子
        """
        cdt = self.data['close'].dtype
        delta = pl.col('close').diff()
        gain = delta.clip(lower_bound=0.0)
        loss = (-delta).clip(lower_bound=0.0)

        # 计算RSI（前14日均值，不含当日）
        avg_gain = gain.rolling_mean(14)
        avg_loss = loss.rolling_mean(14)
        rs = avg_gain / avg_loss
        rsi_raw = (
            pl.when(avg_loss == 0)
            .then(100.0)
            .otherwise(100.0 - (100.0 / (1.0 + rs)))
        )

        # 计算RSI动量（i>=28 处为 rsi_14[i] - rsi_14[i-14]，之前为0；
        # rsi_raw 在 i<14 处为 null，相减后仍为 null，fill_null(0) 与原实现一致）
        rsi_momentum = (rsi_raw - rsi_raw.shift(14)).fill_null(0.0)

        # 添加到DataFrame
        self.data = self.data.with_columns([
            rsi_raw.fill_null(0.0).cast(cdt, strict=False).alias('rsi_14'),
            rsi_momentum.cast(cdt, strict=False).alias('rsi_momentum')
        ])

    def _calculate_stochastic_oscillator(self):
        """
        计算随机振荡器因子
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

        # 计算K、D（i==14时取50，之后按 *2/3 + 输入*1/3 递推）
        self.data = self.data.with_columns(
            _seeded_kdj_expr(rsv, 14, 50.0).cast(cdt, strict=False).alias('kdj_k')
        )
        self.data = self.data.with_columns(
            _seeded_kdj_expr(pl.col('kdj_k'), 14, 50.0).cast(cdt, strict=False).alias('kdj_d')
        )
        self.data = self.data.with_columns(
            (3.0 * pl.col('kdj_k') - 2.0 * pl.col('kdj_d')).cast(cdt, strict=False).alias('kdj_j')
        )

        # 计算KDJ交叉信号（金叉1，死叉-1；i<15 时K/D为0或相等，条件天然不成立）
        k = pl.col('kdj_k')
        d = pl.col('kdj_d')
        kdj_crossover = (
            pl.when((k > d) & (k.shift(1) <= d.shift(1))).then(1.0)
            .when((k < d) & (k.shift(1) >= d.shift(1))).then(-1.0)
            .otherwise(0.0)
        )

        # 添加到DataFrame
        self.data = self.data.with_columns(
            kdj_crossover.cast(cdt, strict=False).alias('kdj_crossover')
        )
