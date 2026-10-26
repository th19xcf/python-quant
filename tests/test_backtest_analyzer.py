#!/usr/bin/env python3

"""
测试回测结果分析和可视化功能
"""

import numpy as np
import polars as pl

from src.backtest.engine.backtest_engine import BacktestEngine
from src.backtest.performance.performance_analyzer import PerformanceAnalyzer
from src.backtest.strategies.trend_following_strategy import TrendFollowingStrategy


def _generate_sample_data(start_date: str = "2023-01-01", end_date: str = "2023-12-31") -> pl.DataFrame:
    """生成模拟股票数据"""
    np.random.seed(42)
    date_range = pl.date_range(
        start=pl.lit(start_date).str.to_date(),
        end=pl.lit(end_date).str.to_date(),
        interval="1d",
        eager=True
    )
    n = len(date_range)
    close = 100 * np.cumprod(1 + np.random.normal(0.001, 0.02, n))
    return pl.DataFrame({
        'date': date_range,
        'open': close * (1 + np.random.normal(0, 0.01, n)),
        'high': close * (1 + np.abs(np.random.normal(0, 0.015, n))),
        'low': close * (1 - np.abs(np.random.normal(0, 0.015, n))),
        'close': close,
        'volume': np.random.randint(1000000, 5000000, n)
    })


def test_backtest_analyzer():
    """
    测试回测结果分析器
    """
    print("=== 测试回测结果分析器 ===")

    # 生成模拟数据
    data = _generate_sample_data()

    # 创建回测引擎和策略
    engine = BacktestEngine(data)
    strategy = TrendFollowingStrategy()
    strategy.params['short_window'] = 5
    strategy.params['long_window'] = 20
    engine.set_strategy(strategy)

    # 运行回测
    result = engine.run_backtest(initial_capital=100000)
    print("回测完成，结果:", result)

    # 使用性能分析器分析回测结果
    analyzer = PerformanceAnalyzer()
    equity_curve = result.get('equity_curve', [])
    trades = result.get('trades', [])
    analysis_result = analyzer.analyze(equity_curve, trades)
    print("\n分析结果:", analysis_result)

    print("\n测试完成！")


if __name__ == "__main__":
    test_backtest_analyzer()
