#!/usr/bin/env python3

"""
技术指标计算服务

UI 层不再直接依赖 src.tech_analysis，统一经由此服务调用。
"""

from typing import Any

import polars as pl

from src.tech_analysis.technical_analyzer import TechnicalAnalyzer


def calculate_indicators_for_period(df: pl.DataFrame, ts_code: str | None = None) -> pl.DataFrame:
    """
    为周线/月线等周期数据重新计算全部技术指标。

    Args:
        df: 基础 OHLCV 数据
        ts_code: 标的代码；传入后 HSL 可读本地真实换手率，缺失时回退估算口径

    Returns:
        pl.DataFrame: 包含技术指标的数据
    """
    analyzer = TechnicalAnalyzer(df)
    kwargs: dict[str, Any] = {}
    if ts_code:
        kwargs['ts_code'] = ts_code
    breadth = _load_breadth(df)
    if breadth is not None:
        kwargs['market_breadth'] = breadth
    return analyzer.calculate_all_indicators(data=df, return_polars=True, **kwargs)


def _load_breadth(df: pl.DataFrame) -> pl.DataFrame | None:
    """按数据日期范围加载本地 breadth 快照（与 UI 共用同一来源）。"""
    try:
        date_col = 'date' if 'date' in df.columns else (
            'trade_date' if 'trade_date' in df.columns else None)
        if date_col is None or df.height == 0:
            return None
        from src.data.services.breadth_service import get_breadth

        dates = df[date_col]
        return get_breadth(dates.min(), dates.max(), warmup=10)
    except Exception as exc:  # noqa: BLE001 - 兜底：失败时退化为无广度指标
        from loguru import logger

        logger.warning(f"加载本地涨跌家数快照失败，跳过涨跌家数相关指标: {exc}")
        return None
