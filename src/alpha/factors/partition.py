#!/usr/bin/env python3

"""
因子层分区感知基类

设计取舍：指标公式仍统一收口在 tech_analysis/indicators/formulas.py，
分区能力通过表达式外层 .over(code_col) 实现，公式本身不感知标的，
因此不会给公式层引入"单标的/分区"双行为模式。

已实测：RSI / ATR（Wilder 递推）、MACD、KDJ（通达信 SMA 递推）在
.over('ts_code') 下与逐标的循环结果 bit 级一致，包括 wilder_expr 内部
pl.int_range(0, pl.len()) 在 over 上下文中按组求值这一关键点。
"""

import polars as pl


class PartitionAware:
    """为因子类提供可选的分区求值能力"""

    partition_by: str | None = None

    def __init__(self, data: pl.DataFrame, partition_by: str | None = None):
        """
        Args:
            data: Polars DataFrame
            partition_by: 标的代码列名；None 表示数据已是单标的序列，
                行为与改造前完全一致
        """
        self.data = data
        self.partition_by = partition_by

    def _p(self, expr: pl.Expr) -> pl.Expr:
        """
        按标的分区求值。

        注意：必须传入完整表达式（含 shift / rolling_* / cum_sum 等跨行算子），
        不能在 .over() 之后再叠加跨行算子，否则窗口会跨标的边界。
        """
        return expr.over(self.partition_by) if self.partition_by else expr
