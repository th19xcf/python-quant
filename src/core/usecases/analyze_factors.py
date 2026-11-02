#!/usr/bin/env python3

"""
analyze_factors —— 因子分析用例入口。

UI / CLI / 服务端统一经此调用，禁止直接依赖 src.quant.analysis.FactorAnalyzer。
"""

from __future__ import annotations

from typing import Any, Dict, List

from loguru import logger

from src.quant.analysis.factor_analyzer import FactorAnalyzer


def analyze_factors(
    stock_code: str,
    start_date: str,
    end_date: str,
    factors: List[str],
    analysis_type: str,
    window: int = 20,
    kdj_window: int = 14,
) -> Dict[str, Any]:
    """
    运行因子分析。

    Args:
        stock_code: 股票代码（如 '600519.SH'）。
        start_date: 开始日期 'YYYY-MM-DD'。
        end_date: 结束日期 'YYYY-MM-DD'。
        factors: 因子名称列表（MACD / RSI / BOLL / KDJ / MA / ATR / WR / CCI ...）。
        analysis_type: '因子有效性检验' | '因子相关性分析' | '因子组合分析'。
        window: 滚动窗口。
        kdj_window: KDJ 的 RSV 周期（默认 14）。

    Returns:
        dict: 分析结果（含 analysis_type / stock_code / start_date / end_date / factors）。

    Raises:
        ValueError: 参数缺失或不受支持时抛出。
    """
    if not stock_code:
        raise ValueError("analyze_factors: 股票代码不能为空")
    if not factors:
        raise ValueError("analyze_factors: 至少需要选择一个因子")

    result = FactorAnalyzer().run_analysis(
        stock_code=stock_code,
        start_date=start_date,
        end_date=end_date,
        factors=factors,
        analysis_type=analysis_type,
        window=window,
        kdj_window=kdj_window,
    )
    logger.info(
        f"因子分析完成: stock_code={stock_code}, analysis_type={analysis_type}, "
        f"factors={len(factors)}"
    )
    return result
