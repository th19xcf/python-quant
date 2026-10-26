#!/usr/bin/env python3

"""
技术指标计算服务

UI 层不再直接依赖 src.tech_analysis，统一经由此服务调用。
"""

import polars as pl

from src.tech_analysis.technical_analyzer import TechnicalAnalyzer


def calculate_indicators_for_period(df: pl.DataFrame) -> pl.DataFrame:
    """
    为周线/月线等周期数据重新计算全部技术指标。

    Args:
        df: 基础 OHLCV 数据

    Returns:
        pl.DataFrame: 包含技术指标的数据
    """
    analyzer = TechnicalAnalyzer(df)
    return analyzer.calculate_all_indicators(data=df, return_polars=True)
