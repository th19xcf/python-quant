#!/usr/bin/env python3

"""
测试组合回测功能
"""

import numpy as np
import pandas as pd
import polars as pl
from loguru import logger

from src.backtest.engine.portfolio_backtest_engine import PortfolioBacktestEngine
from src.backtest.strategies.ma_strategy import MAStrategy
from src.backtest.strategies.macd_strategy import MACDStrategy


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


def test_portfolio_backtest():
    """
    测试组合回测功能
    """
    logger.info("=== 测试组合回测功能 ===")

    # 生成测试数据
    stock1_data = generate_test_data('600000.SH', '2023-01-01', '2023-12-31', volatility=0.02, trend=0.0005)
    stock2_data = generate_test_data('000001.SZ', '2023-01-01', '2023-12-31', volatility=0.025, trend=0.0003)
    stock3_data = generate_test_data('300001.SZ', '2023-01-01', '2023-12-31', volatility=0.03, trend=0.0008)

    # 创建数据字典
    data_dict = {
        '600000.SH': stock1_data,
        '000001.SZ': stock2_data,
        '300001.SZ': stock3_data
    }

    # 创建组合回测引擎
    engine = PortfolioBacktestEngine(data_dict)

    # 设置策略
    engine.set_strategy('600000.SH', MAStrategy(short_window=5, long_window=20))
    engine.set_strategy('000001.SZ', MACDStrategy())
    engine.set_strategy('300001.SZ', MAStrategy(short_window=10, long_window=30))

    # 运行回测
    logger.info("运行组合回测...")
    results = engine.run_backtest(initial_capital=1000000.0, allocation_strategy='equal')

    # 打印回测结果
    logger.info("\n回测结果:")
    logger.info(f"初始资金: {results['initial_capital']:.2f}")
    logger.info(f"最终资金: {results['final_equity']:.2f}")
    logger.info(f"总收益率: {results['total_return']:.2f}%")

    # 打印性能指标
    logger.info("\n性能指标:")
    performance = results['performance']
    for key, value in performance.items():
        logger.info(f"{key}: {value:.4f}")

    # 打印交易记录
    logger.info("{} {}", "\n交易记录数量:", len(results['trades']))
    if results['trades']:
        logger.info("前5条交易记录:")
        for trade in results['trades'][:5]:
            logger.info(f"日期: {trade['date']}, 股票: {trade['stock_code']}, 信号: {trade['signal']}, 价格: {trade['price']:.2f}")

    logger.info("\n组合回测测试完成！")


def test_portfolio_backtest_precomputes_indicators():
    """
    回归测试：组合引擎必须为每只标的预计算策略依赖的指标列。

    历史缺陷：组合引擎从未调用 _precompute_indicators，策略读不到 ma_* /
    macd / rsi，generate_signal 恒返回 hold，回测静默产生 0 笔交易。
    """
    days = 300
    dates = pd.date_range('2023-01-02', periods=days, freq='B')

    def _oscillating(phase):
        close = 100.0 + 10.0 * np.sin(np.arange(days) / 10.0 + phase)
        return pl.DataFrame({
            'date': dates,
            'open': close * 1.001,
            'high': close * 1.01,
            'low': close * 0.99,
            'close': close,
            'volume': np.full(days, 1_000_000.0),
        })

    data_dict = {'600000.SH': _oscillating(0.0), '000001.SZ': _oscillating(0.5)}
    engine = PortfolioBacktestEngine(data_dict)
    for code in data_dict:
        engine.set_strategy(code, MAStrategy(short_window=5, long_window=20))

    results = engine.run_backtest(initial_capital=1_000_000.0)

    # 1. 必须产生交易（预计算链路生效）
    assert len(results['trades']) > 0, '组合回测产生 0 笔交易：指标预计算链路可能断链'

    # 2. 交易必须归属已配置策略的标的，且买卖股数为整手
    open_by_date = {
        code: dict(zip(df['date'].to_list(), df['open'].to_list(), strict=False))
        for code, df in data_dict.items()
    }
    for trade in results['trades']:
        assert trade['stock_code'] in data_dict
        assert trade['signal'] in ('buy', 'sell')
        assert trade['shares'] % 100 == 0
        # 3. 成交价必须是该标的当日开盘价（验证日期索引与行数据对齐）
        assert abs(trade['price'] - open_by_date[trade['stock_code']][trade['date']]) < 1e-9

    # 4. 权益曲线覆盖全部交易日且按日期升序
    equity_dates = [row['date'] for row in results['equity_curve']]
    assert equity_dates == sorted(equity_dates)
    assert set(equity_dates) == set(dates)


if __name__ == "__main__":
    test_portfolio_backtest()
    test_portfolio_backtest_precomputes_indicators()
