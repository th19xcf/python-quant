#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
技术指标因子计算模块
"""

import polars as pl
import numpy as np

from src.alpha.factors.factor_registry import register_factor
from src.alpha.factors.partition import PartitionAware
from src.tech_analysis.indicators.formulas import kdj_exprs, rsi_expr as shared_rsi_expr


@register_factor('technical')
class TechnicalFactors(PartitionAware):
    """
    技术指标因子计算类
    """

    def __init__(self, data, partition_by=None):
        """
        初始化技术指标因子计算类

        Args:
            data: Polars DataFrame，包含价格数据
            partition_by: 标的代码列名；传 None 时为单标的序列
        """
        super().__init__(data, partition_by)

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
            self._p(ma20).fill_null(0.0).cast(cdt, strict=False).alias('bollinger_ma20'),
            self._p(upper_band).fill_null(0.0).cast(cdt, strict=False).alias('bollinger_upper'),
            self._p(lower_band).fill_null(0.0).cast(cdt, strict=False).alias('bollinger_lower'),
            self._p(bollinger_width).fill_null(0.0).cast(cdt, strict=False).alias('bollinger_width'),
            self._p(bollinger_position).fill_null(0.0).cast(cdt, strict=False).alias('bollinger_position')
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
            self._p(ma(5)).cast(cdt, strict=False).alias('ma5'),
            self._p(ma(10)).cast(cdt, strict=False).alias('ma10'),
            self._p(ma(20)).cast(cdt, strict=False).alias('ma20'),
            self._p(ma(60)).cast(cdt, strict=False).alias('ma60')
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
            self._p(ma_crossover_5_20).cast(cdt, strict=False).alias('ma_crossover_5_20'),
            self._p(ma_crossover_10_60).cast(cdt, strict=False).alias('ma_crossover_10_60')
        ])

    def _calculate_relative_strength(self):
        """
        计算相对强弱因子
        """
        cdt = self.data['close'].dtype

        # 计算RSI（共享公式层，Wilder 标准口径；前 13 行为 null，随后 fill_null 补 0）
        rsi_raw = shared_rsi_expr('close', 14)

        # 计算RSI动量（i>=28 处为 rsi_14[i] - rsi_14[i-14]，之前为0；
        # rsi_raw 在 i<14 处为 null，相减后仍为 null，fill_null(0) 与原实现一致）
        rsi_momentum = (rsi_raw - rsi_raw.shift(14)).fill_null(0.0)

        # 添加到DataFrame
        self.data = self.data.with_columns([
            self._p(rsi_raw).fill_null(0.0).cast(cdt, strict=False).alias('rsi_14'),
            self._p(rsi_momentum).cast(cdt, strict=False).alias('rsi_momentum')
        ])

    def _calculate_stochastic_oscillator(self):
        """
        计算随机振荡器因子（通达信 KDJ 口径，与图表同源）
        """
        cdt = self.data['close'].dtype

        kdj = kdj_exprs('high', 'low', 'close', window=14)
        self.data = self.data.with_columns([
            self._p(kdj['k']).cast(cdt, strict=False).alias('kdj_k'),
            self._p(kdj['d']).cast(cdt, strict=False).alias('kdj_d'),
            self._p(kdj['j']).cast(cdt, strict=False).alias('kdj_j'),
        ])

        # 计算KDJ交叉信号（金叉1，死叉-1）
        k = pl.col('kdj_k')
        d = pl.col('kdj_d')
        kdj_crossover = (
            pl.when((k > d) & (k.shift(1) <= d.shift(1))).then(1.0)
            .when((k < d) & (k.shift(1) >= d.shift(1))).then(-1.0)
            .otherwise(0.0)
        )

        # 添加到DataFrame
        self.data = self.data.with_columns(
            self._p(kdj_crossover).cast(cdt, strict=False).alias('kdj_crossover')
        )
