#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
回测功能演示
"""

import numpy as np
import polars as pl

from src.backtest.engine.backtest_engine import BacktestEngine
from src.backtest.performance.performance_analyzer import PerformanceAnalyzer
from src.backtest.strategies.trend_following_strategy import TrendFollowingStrategy
from src.backtest.strategies.mean_reversion_strategy import MeanReversionStrategy
from src.backtest.strategies.multi_factor_strategy import MultiFactorStrategy


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


def _run_strategy_backtest(data, strategy, initial_capital=100000):
    """运行单个策略回测并返回格式化结果"""
    engine = BacktestEngine(data)
    engine.set_strategy(strategy)
    result = engine.run_backtest(initial_capital=initial_capital)

    # 使用性能分析器
    analyzer = PerformanceAnalyzer()
    performance = analyzer.analyze(result.get('equity_curve', []), result.get('trades', []))

    return {
        'strategy_name': result.get('strategy_name', strategy.name),
        'initial_capital': result.get('initial_capital', initial_capital),
        'final_equity': result.get('final_equity', 0),
        'total_return': result.get('total_return', 0),
        'annual_return': performance.get('annual_return', 0),
        'max_drawdown': performance.get('max_drawdown', 0),
        'sharpe_ratio': performance.get('sharpe_ratio', 0),
        'trade_count': len(result.get('trades', [])) // 2,
        'win_rate': performance.get('winning_rate', 0),
    }


def main():
    print("=" * 60)
    print("回测功能演示")
    print("=" * 60)
    print()

    # 生成模拟数据
    data = _generate_sample_data()

    # 1. 趋势跟踪策略回测
    print("【1】趋势跟踪策略回测")
    print("-" * 40)
    strategy1 = TrendFollowingStrategy()
    strategy1.params['short_window'] = 5
    strategy1.params['long_window'] = 20
    result1 = _run_strategy_backtest(data, strategy1)
    print(f"策略名称: {result1['strategy_name']}")
    print(f"初始资金: ¥{result1['initial_capital']:,.2f}")
    print(f"最终资金: ¥{result1['final_equity']:,.2f}")
    print(f"总收益率: {result1['total_return']:.2f}%")
    print(f"年化收益率: {result1['annual_return']:.2f}%")
    print(f"最大回撤: {result1['max_drawdown']:.2f}%")
    print(f"夏普比率: {result1['sharpe_ratio']:.2f}")
    print(f"交易次数: {result1['trade_count']}")
    print(f"胜率: {result1['win_rate']:.2f}%")
    print()

    # 2. 均值回归策略回测
    print("【2】均值回归策略回测")
    print("-" * 40)
    strategy2 = MeanReversionStrategy()
    strategy2.params['rsi_period'] = 14
    strategy2.params['overbought'] = 70
    strategy2.params['oversold'] = 30
    result2 = _run_strategy_backtest(data, strategy2)
    print(f"策略名称: {result2['strategy_name']}")
    print(f"初始资金: ¥{result2['initial_capital']:,.2f}")
    print(f"最终资金: ¥{result2['final_equity']:,.2f}")
    print(f"总收益率: {result2['total_return']:.2f}%")
    print(f"年化收益率: {result2['annual_return']:.2f}%")
    print(f"最大回撤: {result2['max_drawdown']:.2f}%")
    print(f"夏普比率: {result2['sharpe_ratio']:.2f}")
    print(f"交易次数: {result2['trade_count']}")
    print(f"胜率: {result2['win_rate']:.2f}%")
    print()

    # 3. 多因子策略回测
    print("【3】多因子策略回测")
    print("-" * 40)
    strategy3 = MultiFactorStrategy()
    result3 = _run_strategy_backtest(data, strategy3)
    print(f"策略名称: {result3['strategy_name']}")
    print(f"初始资金: ¥{result3['initial_capital']:,.2f}")
    print(f"最终资金: ¥{result3['final_equity']:,.2f}")
    print(f"总收益率: {result3['total_return']:.2f}%")
    print(f"年化收益率: {result3['annual_return']:.2f}%")
    print(f"最大回撤: {result3['max_drawdown']:.2f}%")
    print(f"夏普比率: {result3['sharpe_ratio']:.2f}")
    print(f"交易次数: {result3['trade_count']}")
    print(f"胜率: {result3['win_rate']:.2f}%")
    print()

    print("=" * 60)
    print("回测演示完成！")
    print("=" * 60)


if __name__ == "__main__":
    main()
