#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
波动率因子计算模块
"""

import polars as pl
import numpy as np

from src.alpha.factors.factor_registry import register_factor

_SQRT_252 = np.sqrt(252)
_GK_COEF = 2 * np.log(2) - 1


@register_factor('volatility')
class VolatilityFactors:
    """
    波动率因子计算类
    """

    def __init__(self, data):
        """
        初始化波动率因子计算类

        Args:
            data: Polars DataFrame，包含价格数据
        """
        self.data = data

    def calculate(self, **params) -> pl.DataFrame:
        """
        计算波动率因子

        Args:
            **params: 计算参数

        Returns:
            pl.DataFrame: 包含计算结果的DataFrame
        """
        # 计算各种波动率因子
        self._calculate_atr()
        self._calculate_historical_volatility()
        self._calculate_garman_klass_volatility()
        self._calculate_volatility_trend()

        return self.data

    def _calculate_atr(self):
        """
        计算平均真实波幅(ATR)
        """
        cdt = self.data['close'].dtype
        high = pl.col('high')
        low = pl.col('low')
        prev_close = pl.col('close').shift(1)

        # 计算真实波幅(TR)，首行为0与原实现一致
        tr = (
            pl.when(prev_close.is_not_null())
            .then(pl.max_horizontal(
                high - low,
                (high - prev_close).abs(),
                (low - prev_close).abs()
            ))
            .otherwise(0.0)
        )

        # 计算ATR（前N日均值，不含当日）
        self.data = self.data.with_columns([
            tr.rolling_mean(14).shift(1).fill_null(0.0).cast(cdt, strict=False).alias('atr_14'),
            tr.rolling_mean(21).shift(1).fill_null(0.0).cast(cdt, strict=False).alias('atr_21')
        ])

    def _calculate_historical_volatility(self):
        """
        计算历史波动率
        """
        cdt = self.data['close'].dtype
        close = pl.col('close')
        prev_close = close.shift(1)

        # 计算收益率，首行为0与原实现一致
        returns = (
            pl.when(prev_close.is_not_null())
            .then((close / prev_close).log())
            .otherwise(0.0)
        )

        # 计算历史波动率（前N日窗口，不含当日；ddof=0与np.std一致）
        volatility_20 = returns.rolling_std(20, ddof=0).shift(1) * _SQRT_252
        volatility_60 = returns.rolling_std(60, ddof=0).shift(1) * _SQRT_252

        # 添加到DataFrame
        self.data = self.data.with_columns([
            volatility_20.fill_null(0.0).cast(cdt, strict=False).alias('volatility_20'),
            volatility_60.fill_null(0.0).cast(cdt, strict=False).alias('volatility_60')
        ])

    def _calculate_garman_klass_volatility(self):
        """
        计算Garman-Klass波动率
        """
        cdt = self.data['close'].dtype

        # 计算Garman-Klass波动率，首行为0与原实现一致
        log_high_low = (pl.col('high') / pl.col('low')).log()
        log_close_open = (pl.col('close') / pl.col('open')).log()
        gk_raw = (0.5 * log_high_low * log_high_low - _GK_COEF * log_close_open * log_close_open).sqrt()
        idx = pl.int_range(pl.len())
        gk_volatility = pl.when(idx >= 1).then(gk_raw).otherwise(0.0)

        self.data = self.data.with_columns(
            gk_volatility.alias('_gk_volatility')
        )

        # 计算20日和60日移动平均（前N日均值，不含当日）
        gk = pl.col('_gk_volatility')
        self.data = self.data.with_columns([
            (gk.rolling_mean(20).shift(1) * _SQRT_252).fill_null(0.0).cast(cdt, strict=False).alias('gk_volatility_20'),
            (gk.rolling_mean(60).shift(1) * _SQRT_252).fill_null(0.0).cast(cdt, strict=False).alias('gk_volatility_60')
        ])
        self.data = self.data.drop('_gk_volatility')

    def _calculate_volatility_trend(self):
        """
        计算波动率趋势因子
        """
        cdt = self.data['close'].dtype
        close = pl.col('close')
        prev_close = close.shift(1)

        # 计算收益率，首行为0与原实现一致
        returns = (
            pl.when(prev_close.is_not_null())
            .then((close / prev_close).log())
            .otherwise(0.0)
        )

        # 计算波动率趋势（最近20日与之前40日的波动率对比，均不含当日）
        recent_vol = returns.rolling_std(20, ddof=0).shift(1)
        past_vol = returns.rolling_std(40, ddof=0).shift(21)
        volatility_trend = (
            pl.when(past_vol > 0)
            .then((recent_vol - past_vol) / past_vol)
            .otherwise(0.0)
        )

        # 添加到DataFrame
        self.data = self.data.with_columns(
            volatility_trend.cast(cdt, strict=False).alias('volatility_trend')
        )
