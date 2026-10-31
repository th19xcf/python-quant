#!/usr/bin/env python3

"""
系统性能测试脚本
测试5年以上A股数据的高效处理能力
"""

import time

import numpy as np
import polars as pl
import pytest
from loguru import logger

from src.alpha.calculator.alpha_calculator import AlphaCalculator
from src.backtest.engine.backtest_engine import BacktestEngine
from src.backtest.strategies.trend_following_strategy import TrendFollowingStrategy
from src.data.managers.data_processor import DataProcessor
from src.recommendation.stock_recommender import StockRecommender


def generate_test_data(days: int = 1825, stocks: int = 1000) -> pl.DataFrame:
    """
    生成测试数据

    Args:
        days: 天数，默认1825天（约5年）
        stocks: 股票数量，默认1000只

    Returns:
        pl.DataFrame: 生成的测试数据
    """
    logger.info(f"生成{stocks}只股票，{days}天的测试数据...")

    # 生成日期范围
    import datetime
    start_date = datetime.datetime(2019, 1, 1)
    dates = [start_date + datetime.timedelta(days=i) for i in range(days)]

    # 生成股票数据
    data = []
    for stock_id in range(stocks):
        # 生成随机价格数据
        base_price = 10 + stock_id * 0.1
        price_series = np.random.normal(0, 0.02, days).cumsum() + base_price
        price_series = np.maximum(price_series, 1)  # 确保价格为正

        # 生成成交量数据
        volume_series = np.random.normal(1000000, 500000, days)
        volume_series = np.maximum(volume_series, 10000)  # 确保成交量为正

        # 生成每天的数据
        for i in range(days):
            data.append({
                'date': dates[i],
                'stock_code': f'STOCK{stock_id:04d}',
                'stock_name': f'股票{stock_id:04d}',
                'open': price_series[i] * (1 + np.random.normal(0, 0.01)),
                'high': price_series[i] * (1 + np.random.normal(0.01, 0.01)),
                'low': price_series[i] * (1 - np.random.normal(0.01, 0.01)),
                'close': price_series[i],
                'volume': volume_series[i],
                'amount': volume_series[i] * price_series[i]
            })

    # 转换为Polars DataFrame
    df = pl.DataFrame(data)
    logger.info(f"生成完成，数据形状: {df.shape}")
    return df


@pytest.fixture(scope="module")
def data() -> pl.DataFrame:
    """5个性能测试共用的测试数据（模块级，只生成一次）"""
    return generate_test_data()


def test_data_processing_performance(data: pl.DataFrame):
    """
    测试数据处理性能

    Args:
        data: 测试数据
    """
    logger.info("测试数据处理性能...")

    processor = DataProcessor({})

    # 测试数据清洗
    start_time = time.time()
    cleaned_data = processor.clean_data(data)
    cleanup_time = time.time() - start_time
    logger.info(f"数据清洗时间: {cleanup_time:.2f}秒")

    # 测试数据质量检查
    start_time = time.time()
    quality_report = processor.check_data_quality(cleaned_data)
    quality_time = time.time() - start_time
    logger.info(f"数据质量检查时间: {quality_time:.2f}秒")

    # 测试数据类型转换
    start_time = time.time()
    converted_data = processor.convert_data_type(cleaned_data, 'float32')
    convert_time = time.time() - start_time
    logger.info(f"数据类型转换时间: {convert_time:.2f}秒")

    # 功能性断言：清洗结果不能为空、不留空值与零成交量，转换后数值列为 float32
    assert isinstance(cleaned_data, pl.DataFrame)
    assert 0 < cleaned_data.height <= data.height
    assert quality_report['total_rows'] == cleaned_data.height
    assert quality_report['zero_volume_rows'] == 0
    assert all(count == 0 for count in quality_report['null_values'].values())
    assert isinstance(converted_data, pl.DataFrame)
    for column in ('open', 'high', 'low', 'close', 'volume', 'amount'):
        assert converted_data.schema[column] == pl.Float32

    # 粗粒度性能上限：仅拦截数量级级别的回归（1.8M 行远低于该阈值）
    assert cleanup_time < 60.0
    assert quality_time < 60.0
    assert convert_time < 60.0

    return {
        'cleanup_time': cleanup_time,
        'quality_time': quality_time,
        'convert_time': convert_time
    }


def _assert_factor_result(calculator, factor_type, columns_before, expected_height):
    """断言因子计算成功：返回表行数不变，且至少新增一列因子。"""
    result = calculator.calculate_factors([factor_type])
    assert isinstance(result, pl.DataFrame)
    assert result.height == expected_height
    assert factor_type in calculator.factor_results
    assert calculator.factor_results[factor_type] is not None
    assert len(result.columns) > columns_before


def test_alpha_factor_calculation(data: pl.DataFrame):
    """
    测试Alpha因子计算性能

    Args:
        data: 测试数据
    """
    logger.info("测试Alpha因子计算性能...")

    # 选择一只股票进行测试
    single_stock_data = data.filter(pl.col('stock_code') == 'STOCK0000')

    calculator = AlphaCalculator(single_stock_data)

    # 测试价格动量因子
    columns_before = len(calculator.pl_df.columns)
    start_time = time.time()
    _assert_factor_result(calculator, 'price_momentum', columns_before, single_stock_data.height)
    price_momentum_time = time.time() - start_time
    logger.info(f"价格动量因子计算时间: {price_momentum_time:.2f}秒")

    # 测试波动率因子
    columns_before = len(calculator.pl_df.columns)
    start_time = time.time()
    _assert_factor_result(calculator, 'volatility', columns_before, single_stock_data.height)
    volatility_time = time.time() - start_time
    logger.info(f"波动率因子计算时间: {volatility_time:.2f}秒")

    # 测试成交量因子
    columns_before = len(calculator.pl_df.columns)
    start_time = time.time()
    _assert_factor_result(calculator, 'volume', columns_before, single_stock_data.height)
    volume_time = time.time() - start_time
    logger.info(f"成交量因子计算时间: {volume_time:.2f}秒")

    # 测试技术指标因子
    columns_before = len(calculator.pl_df.columns)
    start_time = time.time()
    _assert_factor_result(calculator, 'technical', columns_before, single_stock_data.height)
    technical_time = time.time() - start_time
    logger.info(f"技术指标因子计算时间: {technical_time:.2f}秒")

    # 测试风险因子
    columns_before = len(calculator.pl_df.columns)
    start_time = time.time()
    _assert_factor_result(calculator, 'risk', columns_before, single_stock_data.height)
    risk_time = time.time() - start_time
    logger.info(f"风险因子计算时间: {risk_time:.2f}秒")

    # 风险因子应产出 beta / max_drawdown_20 / sharpe_ratio_20 三列
    assert {'beta', 'max_drawdown_20', 'sharpe_ratio_20'}.issubset(calculator.pl_df.columns)

    return {
        'price_momentum_time': price_momentum_time,
        'volatility_time': volatility_time,
        'volume_time': volume_time,
        'technical_time': technical_time,
        'risk_time': risk_time
    }


def test_backtest_performance(data: pl.DataFrame):
    """
    测试回测性能

    Args:
        data: 测试数据
    """
    logger.info("测试回测性能...")

    # 选择一只股票进行测试
    single_stock_data = data.filter(pl.col('stock_code') == 'STOCK0000')

    # 创建回测引擎
    engine = BacktestEngine(single_stock_data)

    # 设置策略
    strategy = TrendFollowingStrategy()
    engine.set_strategy(strategy)

    # 测试回测
    start_time = time.time()
    result = engine.run_backtest()
    backtest_time = time.time() - start_time
    logger.info(f"回测时间: {backtest_time:.2f}秒")

    # 测试参数优化
    start_time = time.time()
    param_ranges = {
        'short_window': [5, 10, 20],
        'long_window': [50, 100, 200]
    }
    optimization_result = engine.optimize_strategy(param_ranges, max_workers=4)
    optimization_time = time.time() - start_time
    logger.info(f"参数优化时间: {optimization_time:.2f}秒")

    # 回测结果断言：账户非空、资金回正、净值曲线与成交记录结构完整
    assert isinstance(result, dict) and result
    assert result['final_equity'] > 0
    assert isinstance(result['total_return'], (int, float))
    assert len(result['equity_curve']) > 0
    assert len(result['equity_curve']) <= single_stock_data.height
    assert isinstance(result['trades'], list)

    # 参数优化断言：目标函数正确、最佳参数来自给定候选集、候选组合全部完成
    assert isinstance(optimization_result, dict) and optimization_result
    assert optimization_result['objective'] == 'total_return'
    assert set(optimization_result['best_params']) == {'short_window', 'long_window'}
    assert optimization_result['best_params']['short_window'] in param_ranges['short_window']
    assert optimization_result['best_params']['long_window'] in param_ranges['long_window']
    assert isinstance(optimization_result['best_value'], (int, float))
    assert 1 <= len(optimization_result['optimization_results']) <= 9

    return {
        'backtest_time': backtest_time,
        'optimization_time': optimization_time
    }


def test_stock_recommendation(data: pl.DataFrame):
    """
    测试股票推荐性能

    Args:
        data: 测试数据
    """
    logger.info("测试股票推荐性能...")

    # 选择前100只股票进行测试
    test_data = data.filter(pl.col('stock_code').str.starts_with('STOCK00'))

    # 创建股票推荐器
    recommender = StockRecommender()

    # 测试股票推荐
    start_time = time.time()
    recommendations = recommender.recommend_stocks(test_data, top_n=20)
    recommendation_time = time.time() - start_time
    logger.info(f"股票推荐时间: {recommendation_time:.2f}秒")

    # 推荐结果断言：数量受 top_n 约束、评分降序、字段完整且信号取值合法
    assert isinstance(recommendations, list)
    assert 0 < len(recommendations) <= 20
    scores = [item['risk_adjusted_score'] for item in recommendations]
    assert scores == sorted(scores, reverse=True)
    for item in recommendations:
        assert item['signal'] in {'buy', 'hold', 'sell'}
        assert item['risk_level'] in {'低', '中', '高'}
        assert item['current_price'] > 0

    return {
        'recommendation_time': recommendation_time
    }


def test_memory_usage(data: pl.DataFrame):
    """
    测试内存使用情况

    Args:
        data: 测试数据
    """
    import os

    import psutil

    logger.info("测试内存使用情况...")

    process = psutil.Process(os.getpid())
    memory_before = process.memory_info().rss / 1024 / 1024  # MB
    logger.info(f"处理前内存使用: {memory_before:.2f} MB")

    # 执行一些数据处理操作
    processor = DataProcessor({})
    cleaned_data = processor.clean_data(data)
    converted_data = processor.convert_data_type(cleaned_data, 'float32')

    memory_after = process.memory_info().rss / 1024 / 1024  # MB
    logger.info(f"处理后内存使用: {memory_after:.2f} MB")
    logger.info(f"内存使用增加: {memory_after - memory_before:.2f} MB")

    # 内存断言：进程 RSS 非零、增量在数量级合理范围内（PSUtil 读数本身有抖动，故放宽上限）
    assert memory_before > 0
    assert memory_after > 0
    assert memory_after - memory_before < 4096
    assert isinstance(converted_data, pl.DataFrame)
    assert converted_data.height > 0
    assert converted_data.schema['close'] == pl.Float32

    return {
        'memory_before': memory_before,
        'memory_after': memory_after,
        'memory_increase': memory_after - memory_before
    }


def main():
    """
    主函数
    """
    logger.info("开始系统性能测试...")

    # 生成测试数据
    test_data = generate_test_data(days=1825, stocks=1000)

    # 测试数据处理性能
    data_processing_results = test_data_processing_performance(test_data)

    # 测试Alpha因子计算性能
    alpha_calculation_results = test_alpha_factor_calculation(test_data)

    # 测试回测性能
    backtest_results = test_backtest_performance(test_data)

    # 测试股票推荐性能
    recommendation_results = test_stock_recommendation(test_data)

    # 测试内存使用情况
    memory_results = test_memory_usage(test_data)

    # 输出测试结果
    logger.info("\n===== 性能测试结果 =====")
    logger.info("数据处理性能:")
    for key, value in data_processing_results.items():
        logger.info(f"  {key}: {value:.2f}秒")

    logger.info("\nAlpha因子计算性能:")
    for key, value in alpha_calculation_results.items():
        logger.info(f"  {key}: {value:.2f}秒")

    logger.info("\n回测性能:")
    for key, value in backtest_results.items():
        logger.info(f"  {key}: {value:.2f}秒")

    logger.info("\n股票推荐性能:")
    for key, value in recommendation_results.items():
        logger.info(f"  {key}: {value:.2f}秒")

    logger.info("\n内存使用情况:")
    for key, value in memory_results.items():
        logger.info(f"  {key}: {value:.2f} MB")

    logger.info("\n性能测试完成！")


if __name__ == "__main__":
    main()
