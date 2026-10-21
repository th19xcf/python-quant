#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
成交量因子计算模块
"""

import polars as pl
import numpy as np


class VolumeFactors:
    """
    成交量因子计算类
    """

    def __init__(self, data):
        """
        初始化成交量因子计算类

        Args:
            data: Polars DataFrame，包含价格和成交量数据
        """
        self.data = data

    def calculate(self, **params) -> pl.DataFrame:
        """
        计算成交量因子

        Args:
            **params: 计算参数

        Returns:
            pl.DataFrame: 包含计算结果的DataFrame
        """
        # 计算各种成交量因子
        self._calculate_volume_change()
        self._calculate_volume_momentum()
        self._calculate_on_balance_volume()
        self._calculate_volume_price_relationship()

        return self.data

    def _calculate_volume_change(self):
        """
        计算成交量变化因子
        """
        vdt = self.data['volume'].dtype
        vol = pl.col('volume')
        prev = vol.shift(1)

        # 计算成交量变化率（volume[i-1] <= 0 或首行为 0，与原实现一致）
        volume_change = (
            pl.when(prev > 0)
            .then((vol - prev) / prev)
            .otherwise(0.0)
        )
        self.data = self.data.with_columns(
            volume_change.cast(vdt, strict=False).alias('volume_change')
        )

        # 计算5日和20日平均成交量变化率（前N日均值，不含当日）
        vc = pl.col('volume_change')
        self.data = self.data.with_columns([
            vc.rolling_mean(5).shift(1).fill_null(0.0).cast(vdt, strict=False).alias('volume_change_5'),
            vc.rolling_mean(20).shift(1).fill_null(0.0).cast(vdt, strict=False).alias('volume_change_20')
        ])

    def _calculate_volume_momentum(self):
        """
        计算成交量动量因子
        """
        vdt = self.data['volume'].dtype
        vol = pl.col('volume')

        # 计算成交量动量
        volume_momentum_10 = (vol - vol.shift(10)) / vol.shift(10)
        volume_momentum_20 = (vol - vol.shift(20)) / vol.shift(20)

        # 添加到DataFrame
        self.data = self.data.with_columns([
            volume_momentum_10.fill_null(0.0).cast(vdt, strict=False).alias('volume_momentum_10'),
            volume_momentum_20.fill_null(0.0).cast(vdt, strict=False).alias('volume_momentum_20')
        ])

    def _calculate_on_balance_volume(self):
        """
        计算能量潮(OBV)因子
        """
        vdt = self.data['volume'].dtype
        vol = pl.col('volume')
        close = pl.col('close')
        prev_close = close.shift(1)

        # 计算OBV（符号化的成交量累加）
        signed_volume = (
            pl.when(close > prev_close).then(vol)
            .when(close < prev_close).then(-vol)
            .otherwise(pl.lit(0).cast(vdt))
        )
        obv = signed_volume.cum_sum()

        # 计算OBV的10日和20日移动平均（前N日均值，不含当日）
        self.data = self.data.with_columns([
            obv.cast(vdt, strict=False).alias('obv'),
            obv.rolling_mean(10).shift(1).fill_null(0.0).cast(vdt, strict=False).alias('obv_ma10'),
            obv.rolling_mean(20).shift(1).fill_null(0.0).cast(vdt, strict=False).alias('obv_ma20')
        ])

    def _calculate_volume_price_relationship(self):
        """
        计算量价关系因子
        """
        vdt = self.data['volume'].dtype
        vol = pl.col('volume')
        close = pl.col('close')
        prev_close = close.shift(1)

        # 计算价格变化
        price_change = (
            pl.when(prev_close.is_not_null())
            .then((close - prev_close) / prev_close)
            .otherwise(0.0)
        )

        # 计算量价配合度（前20日窗口，不含当日；窗口内方差为0时与原实现一致为NaN）
        idx = pl.int_range(pl.len())
        corr = pl.rolling_corr(vol, price_change, window_size=20).shift(1)
        volume_price_correlation = (
            pl.when(idx >= 20)
            .then(corr.fill_null(float('nan')))
            .otherwise(0.0)
        )

        # 计算成交量加权价格（窗口 max(0, i-20):i，不含当日）
        # 注：原实现为 np.zeros_like(close)，输出dtype跟随close
        cdt = self.data['close'].dtype
        weighted_sum = (close * vol).rolling_sum(20, min_samples=1).shift(1)
        volume_sum = vol.rolling_sum(20, min_samples=1).shift(1)
        volume_weighted_price = (
            pl.when(volume_sum > 0)
            .then(weighted_sum / volume_sum)
            .otherwise(0.0)
        )

        # 添加到DataFrame
        self.data = self.data.with_columns([
            volume_price_correlation.cast(vdt, strict=False).alias('volume_price_correlation'),
            volume_weighted_price.cast(cdt, strict=False).alias('volume_weighted_price')
        ])
