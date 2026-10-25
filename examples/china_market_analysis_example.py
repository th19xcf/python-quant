#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
中国股市分析示例

展示如何使用Quant项目的中国股市分析功能
"""

import sys
import os
from datetime import datetime, timedelta

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.services.data_service import DataService
from src.analysis.china_market_analysis import ChinaMarketAnalysis
from src.backtest.engine.china_backtest_engine import ChinaBacktestEngine
from src.backtest.strategies.china_strategies import LimitUpBreakoutStrategy, ChinaMAStrategy
from src.tech_analysis.indicators.china import calculate_tdx_atr, calculate_tdx_bias, calculate_tdx_cci, calculate_tdx_rsi


def main():
    """
    中国股市分析示例
    """
    print("===== 中国股市分析示例 =====")
    
    # 初始化数据服务
    from src.utils.config import Config
    from src.database.db_manager import DatabaseManager
    
    config = Config()
    db_manager = DatabaseManager(config)
    data_service = DataService(config, db_manager)
    
    # 初始化中国股市分析器
    china_analyzer = ChinaMarketAnalysis(data_service)
    
    # 设置日期范围
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    
    print(f"\n分析日期范围: {start_date} 到 {end_date}")
    
    # 1. 涨跌停分析示例
    print("\n1. 涨跌停分析示例")
    stock_code = "600519.SH"  # 贵州茅台
    limit_analysis = china_analyzer.analyze_limit_up_down(stock_code, start_date, end_date)
    if limit_analysis:
        print(f"股票代码: {limit_analysis['stock_code']}")
        print(f"总交易日: {limit_analysis['total_trading_days']}")
        print(f"涨停次数: {limit_analysis['limit_up_count']} ({limit_analysis['limit_up_rate']:.2f}%)")
        print(f"跌停次数: {limit_analysis['limit_down_count']} ({limit_analysis['limit_down_rate']:.2f}%)")
        print(f"最大连续涨停: {limit_analysis['max_consecutive_limit_up']}天")
        print(f"最大连续跌停: {limit_analysis['max_consecutive_limit_down']}天")
        print("涨停后表现:")
        print(f"  次日收益率: {limit_analysis['post_limit_up_performance']['next_day_return']:.2f}%")
        print(f"  3日收益率: {limit_analysis['post_limit_up_performance']['three_day_return']:.2f}%")
        print(f"  5日收益率: {limit_analysis['post_limit_up_performance']['five_day_return']:.2f}%")
    
    # 2. 板块轮动分析示例
    print("\n2. 板块轮动分析示例")
    sector_codes = ["000905.SH", "000906.SH", "000907.SH", "000908.SH"]  # 中证500行业指数
    sector_analysis = china_analyzer.analyze_sector_rotation(sector_codes, start_date, end_date)
    if sector_analysis:
        print(f"基准指数: {sector_analysis['benchmark']}")
        print("板块表现:")
        for sector_code, performance in sector_analysis['sector_performance'].items():
            print(f"  {sector_code}: 超额收益 {performance['total_excess_return']:.2f}%, 胜率 {performance['win_rate']:.2f}%")
    
    # 3. 市场情绪分析示例
    print("\n3. 市场情绪分析示例")
    index_codes = ["000001.SH", "399001.SZ", "399006.SZ"]  # 上证指数、深证成指、创业板指
    sentiment_analysis = china_analyzer.analyze_market_sentiment(index_codes, start_date, end_date)
    if sentiment_analysis:
        print("市场情绪:")
        for index_code, sentiment in sentiment_analysis['sentiment_indicators'].items():
            print(f"  {index_code}: {sentiment['sentiment_label']} (得分: {sentiment['sentiment_score']:.2f})")
            print(f"    RSI: {sentiment['rsi']:.2f}, 涨跌比: {sentiment['up_down_ratio']:.2f}, 波动率: {sentiment['volatility']:.2f}")
    
    # 4. 流动性分析示例
    print("\n4. 流动性分析示例")
    stock_codes = ["600519.SH", "000858.SZ", "601318.SH", "600036.SH"]  # 茅台、五粮液、中国平安、招商银行
    liquidity_analysis = china_analyzer.analyze_liquidity(stock_codes, start_date, end_date)
    if liquidity_analysis:
        print("股票流动性:")
        for stock_code, liquidity in liquidity_analysis['liquidity_indicators'].items():
            print(f"  {stock_code}: 流动性得分 {liquidity['liquidity_score']:.2f}")
            print(f"    平均成交额: {liquidity['avg_amount']/100000000:.2f}亿, 平均换手率: {liquidity['avg_turnover_rate']:.2f}%, 价格价差: {liquidity['price_spread']:.2f}%")
    
    # 5. 技术指标计算示例
    print("\n5. 技术指标计算示例")
    stock_data = data_service.get_stock_data(stock_code, start_date, end_date)
    if not stock_data.is_empty():
        # 计算通达信ATR指标
        data_with_atr = calculate_tdx_atr(stock_data)
        print(f"ATR指标 (最近值): {data_with_atr['atr'].tail(1).to_list()[0]:.4f}")
        
        # 计算通达信BIAS指标
        data_with_bias = calculate_tdx_bias(stock_data)
        print(f"BIAS指标 (最近值): {data_with_bias['bias'].tail(1).to_list()[0]:.4f}")
        
        # 计算通达信CCI指标
        data_with_cci = calculate_tdx_cci(stock_data)
        print(f"CCI指标 (最近值): {data_with_cci['cci'].tail(1).to_list()[0]:.4f}")
        
        # 计算通达信RSI指标
        data_with_rsi = calculate_tdx_rsi(stock_data)
        print(f"RSI指标 (最近值): {data_with_rsi['rsi'].tail(1).to_list()[0]:.4f}")
    
    # 6. 回测示例
    print("\n6. 回测示例")
    # 获取股票数据
    stock_data = data_service.get_stock_data("600519.SH", start_date, end_date)
    if not stock_data.is_empty():
        # 初始化中国股市回测引擎
        backtest_engine = ChinaBacktestEngine(stock_data)
        
        # 测试涨跌停突破策略
        strategy = LimitUpBreakoutStrategy()
        backtest_engine.set_strategy(strategy)
        backtest_result = backtest_engine.run_backtest(
            initial_capital=1000000
        )
    
    if 'backtest_result' in locals() and backtest_result:
        print("涨跌停突破策略回测结果:")
        print(f"总收益率: {backtest_result['total_return']:.2f}%")
        if 'performance' in backtest_result:
            print(f"年化收益率: {backtest_result['performance'].get('annual_return', 0):.2f}%")
            print(f"最大回撤: {backtest_result['performance'].get('max_drawdown', 0):.2f}%")
            print(f"夏普比率: {backtest_result['performance'].get('sharpe_ratio', 0):.2f}")
            print(f"胜率: {backtest_result['performance'].get('win_rate', 0):.2f}%")
    
    # 测试中国股市均线策略
    if not stock_data.is_empty():
        # 初始化中国股市回测引擎
        backtest_engine = ChinaBacktestEngine(stock_data)
        strategy = ChinaMAStrategy()
        backtest_engine.set_strategy(strategy)
        backtest_result = backtest_engine.run_backtest(
            initial_capital=1000000
        )
        
        if backtest_result:
            print("\n中国股市均线策略回测结果:")
            print(f"总收益率: {backtest_result['total_return']:.2f}%")
            if 'performance' in backtest_result:
                print(f"年化收益率: {backtest_result['performance'].get('annual_return', 0):.2f}%")
                print(f"最大回撤: {backtest_result['performance'].get('max_drawdown', 0):.2f}%")
                print(f"夏普比率: {backtest_result['performance'].get('sharpe_ratio', 0):.2f}")
                print(f"胜率: {backtest_result['performance'].get('win_rate', 0):.2f}%")
    
    print("\n===== 示例结束 =====")


if __name__ == "__main__":
    main()
