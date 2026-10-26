#!/usr/bin/env python3

"""
测试多因子策略功能
"""

import numpy as np
import pandas as pd
import polars as pl

from src.backtest.engine.portfolio_backtest_engine import PortfolioBacktestEngine
from src.backtest.strategies.multi_factor_strategy import MultiFactorStrategy


def generate_test_data(stock_code, start_date, end_date, volatility=0.02, trend=0.001):
    """
    生成测试数据

    Args:
        stock_code: 股票代码
        start_date: 开始日期
        end_date: 结束日期
        volatility: 波动率
        trend: 趋势

    Returns:
        pl.DataFrame: 测试数据
    """
    # 生成日期范围
    dates = pd.date_range(start=start_date, end=end_date, freq='B')

    # 生成价格数据
    np.random.seed(42)
    returns = np.random.normal(trend, volatility, len(dates))
    prices = [100.0]
    for ret in returns:
        prices.append(prices[-1] * (1 + ret))
    prices = prices[1:]

    # 生成成交量数据
    volumes = np.random.randint(100000, 1000000, len(dates))

    # 创建DataFrame
    data = {
        'date': dates,
        'open': [p * 0.999 for p in prices],
        'high': [p * 1.002 for p in prices],
        'low': [p * 0.998 for p in prices],
        'close': prices,
        'volume': volumes
    }

    return pl.DataFrame(data)


def test_multi_factor_strategy():
    """
    测试多因子策略功能
    """
    print("=== 测试多因子策略功能 ===")

    # 生成测试数据
    stock1_data = generate_test_data('600000.SH', '2023-01-01', '2023-12-31', volatility=0.02, trend=0.0005)
    stock2_data = generate_test_data('000001.SZ', '2023-01-01', '2023-12-31', volatility=0.025, trend=0.0003)

    # 创建数据字典
    data_dict = {
        '600000.SH': stock1_data,
        '000001.SZ': stock2_data
    }

    # 创建组合回测引擎
    engine = PortfolioBacktestEngine(data_dict)

    # 设置多因子策略
    # 股票1使用默认因子权重
    engine.set_strategy('600000.SH', MultiFactorStrategy())
    # 股票2使用自定义因子权重
    custom_weights = {'ma': 0.2, 'macd': 0.5, 'rsi': 0.3}
    engine.set_strategy('000001.SZ', MultiFactorStrategy(weights=custom_weights))

    # 运行回测
    print("运行多因子策略回测...")
    results = engine.run_backtest(initial_capital=1000000.0, allocation_strategy='equal')

    # 打印回测结果
    print("\n回测结果:")
    print(f"初始资金: {results['initial_capital']:.2f}")
    print(f"最终资金: {results['final_equity']:.2f}")
    print(f"总收益率: {results['total_return']:.2f}%")

    # 打印性能指标
    print("\n性能指标:")
    performance = results['performance']
    for key, value in performance.items():
        print(f"{key}: {value:.4f}")

    # 打印交易记录
    print("\n交易记录数量:", len(results['trades']))
    if results['trades']:
        print("前5条交易记录:")
        for trade in results['trades'][:5]:
            print(f"日期: {trade['date']}, 股票: {trade['stock_code']}, 信号: {trade['signal']}, 价格: {trade['price']:.2f}")

    print("\n多因子策略测试完成！")


if __name__ == "__main__":
    test_multi_factor_strategy()
