#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
策略回测引擎
"""

import polars as pl
import numpy as np
from datetime import datetime, timedelta

from src.utils.logger import logger


class BacktestEngine:
    """
    策略回测引擎
    """
    
    def __init__(self):
        """
        初始化回测引擎
        """
        pass
    
    def run_backtest(self, strategy_type, stock_code, start_date, end_date, initial_capital, 
                     short_ma=5, long_ma=20, rsi_period=14, rsi_overbought=70, rsi_oversold=30):
        """
        运行策略回测
        
        Args:
            strategy_type: 策略类型
            stock_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            initial_capital: 初始资金
            short_ma: 短期均线索引
            long_ma: 长期均线索引
            rsi_period: RSI周期
            rsi_overbought: RSI超买阈值
            rsi_oversold: RSI超卖阈值
            
        Returns:
            dict: 回测结果
        """
        try:
            # 生成模拟数据
            data = self._generate_sample_data(stock_code, start_date, end_date)
            
            # 根据策略类型运行回测
            if strategy_type == "趋势跟踪策略":
                result = self._run_trend_following_strategy(data, initial_capital, short_ma, long_ma)
            elif strategy_type == "均值回归策略":
                result = self._run_mean_reversion_strategy(data, initial_capital, rsi_period, rsi_overbought, rsi_oversold)
            elif strategy_type == "多因子策略":
                result = self._run_multi_factor_strategy(data, initial_capital)
            else:
                raise ValueError(f"不支持的策略类型: {strategy_type}")
            
            # 添加基本信息
            result.update({
                'strategy_type': strategy_type,
                'stock_code': stock_code,
                'start_date': start_date,
                'end_date': end_date,
                'initial_capital': initial_capital
            })
            
            return result
            
        except Exception as e:
            logger.error(f"回测失败: {e}")
            raise
    
    def _generate_sample_data(self, stock_code, start_date, end_date):
        """
        生成模拟数据
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            pl.DataFrame: 模拟数据
        """
        # 解析日期
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        # 生成日期序列
        dates = []
        current = start
        while current <= end:
            dates.append(current)
            current += timedelta(days=1)
        
        # 过滤掉非交易日（简单模拟，只保留周一到周五）
        trading_dates = [d for d in dates if d.weekday() < 5]
        
        # 生成价格数据
        np.random.seed(42)  # 固定种子以确保结果可复现
        base_price = 100.0
        prices = []
        price = base_price
        
        for _ in trading_dates:
            # 添加一些随机波动
            change = np.random.normal(0, 1)
            price = max(1, price + change)
            prices.append(price)
        
        # 生成成交量数据
        volumes = np.random.randint(100000, 1000000, size=len(trading_dates))
        
        # 创建DataFrame
        data = pl.DataFrame({
            'date': trading_dates,
            'open': [p * (1 + np.random.normal(0, 0.01)) for p in prices],
            'high': [p * (1 + np.random.normal(0, 0.02)) for p in prices],
            'low': [p * (1 - np.random.normal(0, 0.02)) for p in prices],
            'close': prices,
            'volume': volumes
        })
        
        return data
    
    def _run_trend_following_strategy(self, data, initial_capital, short_ma, long_ma):
        """
        运行趋势跟踪策略
        
        Args:
            data: 股票数据
            initial_capital: 初始资金
            short_ma: 短期均线索引
            long_ma: 长期均线索引
            
        Returns:
            dict: 回测结果
        """
        # 计算移动平均线
        data = data.with_columns([
            pl.col('close').rolling_mean(window_size=short_ma).alias(f'ma{short_ma}'),
            pl.col('close').rolling_mean(window_size=long_ma).alias(f'ma{long_ma}')
        ])
        
        # 生成信号
        data = data.with_columns([
            (pl.col(f'ma{short_ma}') > pl.col(f'ma{long_ma}')).alias('signal')
        ])
        
        # 回测
        capital = initial_capital
        position = 0
        trade_count = 0
        win_count = 0
        trades = []
        
        for i in range(1, len(data)):
            prev_signal = data[i-1]['signal'].item()
            current_signal = data[i]['signal'].item()
            current_price = data[i]['close'].item()
            
            # 金叉：买入
            if not prev_signal and current_signal:
                if position == 0:
                    position = capital / current_price
                    capital = 0
                    trade_count += 1
                    trades.append(('buy', current_price, data[i]['date']))
            # 死叉：卖出
            elif prev_signal and not current_signal:
                if position > 0:
                    capital = position * current_price
                    position = 0
                    # 计算收益
                    buy_price = trades[-1][1]
                    if current_price > buy_price:
                        win_count += 1
            
        # 最后一天如果有持仓，卖出
        if position > 0:
            final_price = data[-1]['close'].item()
            capital = position * final_price
            position = 0
            # 计算收益
            buy_price = trades[-1][1]
            if final_price > buy_price:
                win_count += 1
        
        # 计算指标
        total_return = (capital - initial_capital) / initial_capital * 100
        # 简单年化收益率计算
        days = (data[-1]['date'].item() - data[0]['date'].item()).days
        annual_return = total_return / (days / 365)
        
        # 计算最大回撤
        equity_curve = []
        temp_capital = initial_capital
        temp_position = 0
        for i in range(len(data)):
            if i > 0:
                prev_signal = data[i-1]['signal'].item()
                current_signal = data[i]['signal'].item()
                current_price = data[i]['close'].item()
                
                if not prev_signal and current_signal:
                    if temp_position == 0:
                        temp_position = temp_capital / current_price
                        temp_capital = 0
                elif prev_signal and not current_signal:
                    if temp_position > 0:
                        temp_capital = temp_position * current_price
                        temp_position = 0
            
            if temp_position > 0:
                equity = temp_position * data[i]['close'].item()
            else:
                equity = temp_capital
            equity_curve.append(equity)
        
        # 计算最大回撤
        peak = equity_curve[0]
        max_drawdown = 0
        for equity in equity_curve:
            if equity > peak:
                peak = equity
            drawdown = (peak - equity) / peak * 100
            if drawdown > max_drawdown:
                max_drawdown = drawdown
        
        # 计算夏普比率（简化版）
        returns = []
        for i in range(1, len(equity_curve)):
            daily_return = (equity_curve[i] - equity_curve[i-1]) / equity_curve[i-1]
            returns.append(daily_return)
        
        if returns:
            sharpe_ratio = np.mean(returns) / (np.std(returns) + 1e-10) * np.sqrt(252)
        else:
            sharpe_ratio = 0
        
        # 计算胜率
        win_rate = (win_count / trade_count * 100) if trade_count > 0 else 0
        
        return {
            'final_capital': capital,
            'total_return': total_return,
            'annual_return': annual_return,
            'max_drawdown': max_drawdown,
            'sharpe_ratio': sharpe_ratio,
            'trade_count': trade_count,
            'win_rate': win_rate
        }
    
    def _run_mean_reversion_strategy(self, data, initial_capital, rsi_period, rsi_overbought, rsi_oversold):
        """
        运行均值回归策略
        
        Args:
            data: 股票数据
            initial_capital: 初始资金
            rsi_period: RSI周期
            rsi_overbought: RSI超买阈值
            rsi_oversold: RSI超卖阈值
            
        Returns:
            dict: 回测结果
        """
        # 计算RSI
        delta = data['close'].diff()
        gain = delta.map_elements(lambda x: x if x > 0 else 0.0)
        loss = delta.map_elements(lambda x: -x if x < 0 else 0.0)
        
        avg_gain = gain.rolling_mean(window_size=rsi_period)
        avg_loss = loss.rolling_mean(window_size=rsi_period)
        
        rs = avg_gain / (avg_loss + 1e-10)
        rsi = 100 - (100 / (1 + rs))
        
        data = data.with_columns([rsi.alias('rsi')])
        
        # 回测
        capital = initial_capital
        position = 0
        trade_count = 0
        win_count = 0
        trades = []
        
        for i in range(rsi_period, len(data)):
            current_rsi = data[i]['rsi'].item()
            current_price = data[i]['close'].item()
            
            # RSI超卖：买入
            if current_rsi < rsi_oversold:
                if position == 0:
                    position = capital / current_price
                    capital = 0
                    trade_count += 1
                    trades.append(('buy', current_price, data[i]['date']))
            # RSI超买：卖出
            elif current_rsi > rsi_overbought:
                if position > 0:
                    capital = position * current_price
                    position = 0
                    # 计算收益
                    buy_price = trades[-1][1]
                    if current_price > buy_price:
                        win_count += 1
        
        # 最后一天如果有持仓，卖出
        if position > 0:
            final_price = data[-1]['close'].item()
            capital = position * final_price
            position = 0
            # 计算收益
            buy_price = trades[-1][1]
            if final_price > buy_price:
                win_count += 1
        
        # 计算指标
        total_return = (capital - initial_capital) / initial_capital * 100
        # 简单年化收益率计算
        days = (data[-1]['date'].item() - data[0]['date'].item()).days
        annual_return = total_return / (days / 365)
        
        # 计算最大回撤
        equity_curve = []
        temp_capital = initial_capital
        temp_position = 0
        for i in range(len(data)):
            if i >= rsi_period:
                current_rsi = data[i]['rsi'].item()
                current_price = data[i]['close'].item()
                
                if current_rsi < rsi_oversold:
                    if temp_position == 0:
                        temp_position = temp_capital / current_price
                        temp_capital = 0
                elif current_rsi > rsi_overbought:
                    if temp_position > 0:
                        temp_capital = temp_position * current_price
                        temp_position = 0
            
            if temp_position > 0:
                equity = temp_position * data[i]['close'].item()
            else:
                equity = temp_capital
            equity_curve.append(equity)
        
        # 计算最大回撤
        peak = equity_curve[0]
        max_drawdown = 0
        for equity in equity_curve:
            if equity > peak:
                peak = equity
            drawdown = (peak - equity) / peak * 100
            if drawdown > max_drawdown:
                max_drawdown = drawdown
        
        # 计算夏普比率（简化版）
        returns = []
        for i in range(1, len(equity_curve)):
            daily_return = (equity_curve[i] - equity_curve[i-1]) / equity_curve[i-1]
            returns.append(daily_return)
        
        if returns:
            sharpe_ratio = np.mean(returns) / (np.std(returns) + 1e-10) * np.sqrt(252)
        else:
            sharpe_ratio = 0
        
        # 计算胜率
        win_rate = (win_count / trade_count * 100) if trade_count > 0 else 0
        
        return {
            'final_capital': capital,
            'total_return': total_return,
            'annual_return': annual_return,
            'max_drawdown': max_drawdown,
            'sharpe_ratio': sharpe_ratio,
            'trade_count': trade_count,
            'win_rate': win_rate
        }
    
    def _run_multi_factor_strategy(self, data, initial_capital):
        """
        运行多因子策略
        
        Args:
            data: 股票数据
            initial_capital: 初始资金
            
        Returns:
            dict: 回测结果
        """
        # 计算多个因子
        # 1. 动量因子（10日收益率）
        momentum = data['close'].pct_change(10)
        
        # 2. 波动率因子（20日标准差）
        volatility = data['close'].rolling_std(window_size=20)
        
        # 3. 成交量因子（5日成交量变化）
        volume_change = data['volume'].pct_change(5)
        
        # 4. 移动平均线因子（5日与20日均线差）
        ma5 = data['close'].rolling_mean(window_size=5)
        ma20 = data['close'].rolling_mean(window_size=20)
        ma_diff = (ma5 - ma20) / ma20
        
        # 合并因子
        data = data.with_columns([
            momentum.alias('momentum'),
            volatility.alias('volatility'),
            volume_change.alias('volume_change'),
            ma_diff.alias('ma_diff')
        ])
        
        # 生成信号（简单示例：综合因子评分）
        # 这里使用简单的加权评分
        data = data.with_columns([
            (pl.col('momentum') * 0.3 + pl.col('ma_diff') * 0.3 + 
             (1 / (pl.col('volatility') + 1e-10)) * 0.2 + pl.col('volume_change') * 0.2).alias('score')
        ])
        
        # 回测
        capital = initial_capital
        position = 0
        trade_count = 0
        win_count = 0
        trades = []
        
        for i in range(20, len(data)):
            current_score = data[i]['score'].item()
            current_price = data[i]['close'].item()
            
            # 评分大于0.1：买入
            if current_score > 0.1:
                if position == 0:
                    position = capital / current_price
                    capital = 0
                    trade_count += 1
                    trades.append(('buy', current_price, data[i]['date']))
            # 评分小于-0.1：卖出
            elif current_score < -0.1:
                if position > 0:
                    capital = position * current_price
                    position = 0
                    # 计算收益
                    buy_price = trades[-1][1]
                    if current_price > buy_price:
                        win_count += 1
        
        # 最后一天如果有持仓，卖出
        if position > 0:
            final_price = data[-1]['close'].item()
            capital = position * final_price
            position = 0
            # 计算收益
            buy_price = trades[-1][1]
            if final_price > buy_price:
                win_count += 1
        
        # 计算指标
        total_return = (capital - initial_capital) / initial_capital * 100
        # 简单年化收益率计算
        days = (data[-1]['date'].item() - data[0]['date'].item()).days
        annual_return = total_return / (days / 365)
        
        # 计算最大回撤
        equity_curve = []
        temp_capital = initial_capital
        temp_position = 0
        for i in range(len(data)):
            if i >= 20:
                current_score = data[i]['score'].item()
                current_price = data[i]['close'].item()
                
                if current_score > 0.1:
                    if temp_position == 0:
                        temp_position = temp_capital / current_price
                        temp_capital = 0
                elif current_score < -0.1:
                    if temp_position > 0:
                        temp_capital = temp_position * current_price
                        temp_position = 0
            
            if temp_position > 0:
                equity = temp_position * data[i]['close'].item()
            else:
                equity = temp_capital
            equity_curve.append(equity)
        
        # 计算最大回撤
        peak = equity_curve[0]
        max_drawdown = 0
        for equity in equity_curve:
            if equity > peak:
                peak = equity
            drawdown = (peak - equity) / peak * 100
            if drawdown > max_drawdown:
                max_drawdown = drawdown
        
        # 计算夏普比率（简化版）
        returns = []
        for i in range(1, len(equity_curve)):
            daily_return = (equity_curve[i] - equity_curve[i-1]) / equity_curve[i-1]
            returns.append(daily_return)
        
        if returns:
            sharpe_ratio = np.mean(returns) / (np.std(returns) + 1e-10) * np.sqrt(252)
        else:
            sharpe_ratio = 0
        
        # 计算胜率
        win_rate = (win_count / trade_count * 100) if trade_count > 0 else 0
        
        return {
            'final_capital': capital,
            'total_return': total_return,
            'annual_return': annual_return,
            'max_drawdown': max_drawdown,
            'sharpe_ratio': sharpe_ratio,
            'trade_count': trade_count,
            'win_rate': win_rate
        }
