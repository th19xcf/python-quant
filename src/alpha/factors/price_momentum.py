#!/usr/bin/env python3

"""
价格动量因子计算模块
"""

import polars as pl

from src.alpha.factors.factor_registry import register_factor
from src.alpha.factors.partition import PartitionAware
from src.tech_analysis.indicators.formulas import (
    kdj_exprs,
    macd_exprs,
)
from src.tech_analysis.indicators.formulas import (
    rsi_expr as shared_rsi_expr,
)


@register_factor('price_momentum')
class PriceMomentumFactors(PartitionAware):
    """
    价格动量因子计算类
    """

    def __init__(self, data, partition_by=None):
        """
        初始化价格动量因子计算类

        Args:
            data: Polars DataFrame，包含价格数据
            partition_by: 标的代码列名；传 None 时为单标的序列
        """
        super().__init__(data, partition_by)

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
            self._p(momentum_1m).fill_null(0.0).cast(cdt, strict=False).alias('momentum_1m'),
            self._p(momentum_3m).fill_null(0.0).cast(cdt, strict=False).alias('momentum_3m'),
            self._p(momentum_6m).fill_null(0.0).cast(cdt, strict=False).alias('momentum_6m'),
            self._p(momentum_12m).fill_null(0.0).cast(cdt, strict=False).alias('momentum_12m')
        ])

    def _calculate_rsi_momentum(self):
        """
        计算RSI动量因子
        """
        cdt = self.data['close'].dtype

        # RSI 统一走共享公式层（Wilder 标准口径）
        # 添加到DataFrame
        self.data = self.data.with_columns([
            self._p(shared_rsi_expr('close', 14)).fill_null(0.0).cast(cdt, strict=False).alias('rsi_14'),
            self._p(shared_rsi_expr('close', 21)).fill_null(0.0).cast(cdt, strict=False).alias('rsi_21')
        ])

    def _calculate_macd_momentum(self):
        """
        计算MACD动量因子
        """
        cdt = self.data['close'].dtype

        # MACD 统一走共享公式层（标准 EMA 口径）
        exprs = macd_exprs('close', 12, 26, 9)
        self.data = self.data.with_columns([
            self._p(exprs['macd']).fill_null(0.0).cast(cdt, strict=False).alias('macd'),
            self._p(exprs['signal']).fill_null(0.0).cast(cdt, strict=False).alias('macd_signal'),
            self._p(exprs['hist']).fill_null(0.0).cast(cdt, strict=False).alias('macd_hist'),
        ])

    def _calculate_stochastic_momentum(self):
        """
        计算随机动量因子（通达信 KDJ 口径，与图表同源）
        """
        cdt = self.data['close'].dtype

        kdj = kdj_exprs('high', 'low', 'close', window=14)
        self.data = self.data.with_columns([
            self._p(kdj['k']).cast(cdt, strict=False).alias('kdj_k'),
            self._p(kdj['d']).cast(cdt, strict=False).alias('kdj_d'),
            self._p(kdj['j']).cast(cdt, strict=False).alias('kdj_j'),
        ])
