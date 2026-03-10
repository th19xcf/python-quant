#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
因子分析器
"""

import polars as pl
import numpy as np
from datetime import datetime, timedelta

from src.utils.logger import logger


class FactorAnalyzer:
    """
    因子分析器
    """
    
    def __init__(self):
        """
        初始化因子分析器
        """
        pass
    
    def run_analysis(self, stock_code, start_date, end_date, factors, analysis_type, window):
        """
        运行因子分析
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            factors: 因子列表
            analysis_type: 分析类型
            window: 滚动窗口
            
        Returns:
            dict: 分析结果
        """
        try:
            # 生成模拟数据
            data = self._generate_sample_data(stock_code, start_date, end_date)
            
            # 计算因子
            data = self._calculate_factors(data, factors)
            
            # 根据分析类型运行分析
            if analysis_type == "因子有效性检验":
                result = self._factor_validity_test(data, factors, window)
            elif analysis_type == "因子相关性分析":
                result = self._factor_correlation_analysis(data, factors)
            elif analysis_type == "因子组合分析":
                result = self._factor_combination_analysis(data, factors, window)
            else:
                raise ValueError(f"不支持的分析类型: {analysis_type}")
            
            # 添加基本信息
            result.update({
                'analysis_type': analysis_type,
                'stock_code': stock_code,
                'start_date': start_date,
                'end_date': end_date,
                'factors': factors
            })
            
            return result
            
        except Exception as e:
            logger.error(f"分析失败: {e}")
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
    
    def _calculate_factors(self, data, factors):
        """
        计算因子
        
        Args:
            data: 股票数据
            factors: 因子列表
            
        Returns:
            pl.DataFrame: 包含因子的数据
        """
        # 计算各种因子
        if 'MACD' in factors:
            # 计算MACD
            ema12 = data['close'].ewm_mean(span=12)
            ema26 = data['close'].ewm_mean(span=26)
            macd = ema12 - ema26
            signal = macd.ewm_mean(span=9)
            histogram = macd - signal
            data = data.with_columns([
                macd.alias('macd'),
                signal.alias('macd_signal'),
                histogram.alias('macd_hist')
            ])
        
        if 'RSI' in factors:
            # 计算RSI
            delta = data['close'].diff()
            gain = delta.apply(lambda x: x if x > 0 else 0)
            loss = delta.apply(lambda x: -x if x < 0 else 0)
            avg_gain = gain.rolling_mean(window_size=14)
            avg_loss = loss.rolling_mean(window_size=14)
            rs = avg_gain / (avg_loss + 1e-10)
            rsi = 100 - (100 / (1 + rs))
            data = data.with_columns([rsi.alias('rsi')])
        
        if 'BOLL' in factors:
            # 计算布林带
            ma20 = data['close'].rolling_mean(window_size=20)
            std20 = data['close'].rolling_std(window_size=20)
            upper = ma20 + 2 * std20
            lower = ma20 - 2 * std20
            data = data.with_columns([
                ma20.alias('boll_mid'),
                upper.alias('boll_upper'),
                lower.alias('boll_lower')
            ])
        
        if 'KDJ' in factors:
            # 计算KDJ
            low = data['low'].rolling_min(window_size=9)
            high = data['high'].rolling_max(window_size=9)
            rsv = (data['close'] - low) / (high - low + 1e-10) * 100
            k = rsv.ewm_mean(span=3)
            d = k.ewm_mean(span=3)
            j = 3 * k - 2 * d
            data = data.with_columns([
                k.alias('kdj_k'),
                d.alias('kdj_d'),
                j.alias('kdj_j')
            ])
        
        if 'MA' in factors:
            # 计算移动平均线
            data = data.with_columns([
                data['close'].rolling_mean(window_size=5).alias('ma5'),
                data['close'].rolling_mean(window_size=10).alias('ma10'),
                data['close'].rolling_mean(window_size=20).alias('ma20'),
                data['close'].rolling_mean(window_size=60).alias('ma60')
            ])
        
        if 'ATR' in factors:
            # 计算ATR
            tr1 = data['high'] - data['low']
            tr2 = abs(data['high'] - data['close'].shift(1))
            tr3 = abs(data['low'] - data['close'].shift(1))
            # 使用Polars的max方法计算最大值
            tr = pl.max_horizontal([tr1, tr2, tr3])
            atr = tr.rolling_mean(window_size=14)
            data = data.with_columns([atr.alias('atr')])
        
        if 'WR' in factors:
            # 计算威廉指标
            low = data['low'].rolling_min(window_size=14)
            high = data['high'].rolling_max(window_size=14)
            wr = (high - data['close']) / (high - low + 1e-10) * 100
            data = data.with_columns([wr.alias('wr')])
        
        if 'CCI' in factors:
            # 计算CCI
            typical_price = (data['high'] + data['low'] + data['close']) / 3
            ma = typical_price.rolling_mean(window_size=14)
            mean_dev = abs(typical_price - ma).rolling_mean(window_size=14)
            cci = (typical_price - ma) / (0.015 * mean_dev)
            data = data.with_columns([cci.alias('cci')])
        
        if 'PE' in factors:
            # 模拟市盈率
            pe = np.random.normal(15, 5, size=len(data))
            data = data.with_columns([pl.Series('pe', pe)])
        
        if 'PB' in factors:
            # 模拟市净率
            pb = np.random.normal(2, 0.5, size=len(data))
            data = data.with_columns([pl.Series('pb', pb)])
        
        if 'ROE' in factors:
            # 模拟净资产收益率
            roe = np.random.normal(10, 3, size=len(data))
            data = data.with_columns([pl.Series('roe', roe)])
        
        if 'EPS' in factors:
            # 模拟每股收益
            eps = np.random.normal(1, 0.3, size=len(data))
            data = data.with_columns([pl.Series('eps', eps)])
        
        if 'MOM1M' in factors:
            # 1个月动量
            mom1m = data['close'].pct_change(20)
            data = data.with_columns([mom1m.alias('mom1m')])
        
        if 'MOM3M' in factors:
            # 3个月动量
            mom3m = data['close'].pct_change(60)
            data = data.with_columns([mom3m.alias('mom3m')])
        
        if 'MOM6M' in factors:
            # 6个月动量
            mom6m = data['close'].pct_change(120)
            data = data.with_columns([mom6m.alias('mom6m')])
        
        if 'MOM12M' in factors:
            # 12个月动量
            mom12m = data['close'].pct_change(240)
            data = data.with_columns([mom12m.alias('mom12m')])
        
        return data
    
    def _factor_validity_test(self, data, factors, window):
        """
        因子有效性检验
        
        Args:
            data: 股票数据
            factors: 因子列表
            window: 滚动窗口
            
        Returns:
            dict: 分析结果
        """
        factor_results = {}
        
        # 计算收益率
        data = data.with_columns([data['close'].pct_change(1).alias('return')])
        
        for factor in factors:
            # 获取因子列名
            factor_col = factor.lower()
            if factor_col not in data.columns:
                continue
            
            # 计算因子与收益率的相关性（IC值）
            corr_matrix = data[[factor_col, 'return']].corr()
            ic = corr_matrix.get_column(factor_col)[1]
            
            # 计算分层测试
            # 将因子值分为5层
            data = data.with_columns([
                pl.col(factor_col).qcut(5).alias(f'{factor_col}_quantile')
            ])
            
            # 计算每层的平均收益率
            quantile_returns = data.groupby(f'{factor_col}_quantile').agg([
                pl.col('return').mean().alias('avg_return')
            ]).sort(f'{factor_col}_quantile')
            
            # 计算多空收益
            long_return = quantile_returns[-1]['avg_return'].item()
            short_return = quantile_returns[0]['avg_return'].item()
            long_short_return = long_return - short_return
            
            factor_results[factor] = {
                'ic': ic,
                'long_return': long_return,
                'short_return': short_return,
                'long_short_return': long_short_return
            }
        
        return {
            'factor_results': factor_results
        }
    
    def _factor_correlation_analysis(self, data, factors):
        """
        因子相关性分析
        
        Args:
            data: 股票数据
            factors: 因子列表
            
        Returns:
            dict: 分析结果
        """
        # 获取因子列
        factor_cols = []
        for factor in factors:
            factor_col = factor.lower()
            if factor_col in data.columns:
                factor_cols.append(factor_col)
        
        # 计算相关性矩阵
        if factor_cols:
            correlation_matrix = data[factor_cols].corr().to_numpy().tolist()
        else:
            correlation_matrix = []
        
        return {
            'correlation_matrix': correlation_matrix,
            'factor_columns': factor_cols
        }
    
    def _factor_combination_analysis(self, data, factors, window):
        """
        因子组合分析
        
        Args:
            data: 股票数据
            factors: 因子列表
            window: 滚动窗口
            
        Returns:
            dict: 分析结果
        """
        # 计算收益率
        data = data.with_columns([data['close'].pct_change(1).alias('return')])
        
        # 构建因子组合
        factor_cols = []
        for factor in factors:
            factor_col = factor.lower()
            if factor_col in data.columns:
                factor_cols.append(factor_col)
        
        # 简单等权组合
        if factor_cols:
            data = data.with_columns([
                pl.sum_horizontal(factor_cols).alias('factor_combination')
            ])
            
            # 计算组合因子与收益率的相关性
            corr_matrix = data[['factor_combination', 'return']].corr()
            ic = corr_matrix.get_column('factor_combination')[1]
            
            # 计算分层测试
            data = data.with_columns([
                pl.col('factor_combination').qcut(5).alias('factor_quantile')
            ])
            
            quantile_returns = data.groupby('factor_quantile').agg([
                pl.col('return').mean().alias('avg_return')
            ]).sort('factor_quantile')
            
            long_return = quantile_returns[-1]['avg_return'].item()
            short_return = quantile_returns[0]['avg_return'].item()
            long_short_return = long_return - short_return
        else:
            ic = 0
            long_return = 0
            short_return = 0
            long_short_return = 0
        
        return {
            'factor_combination': {
                'ic': ic,
                'long_return': long_return,
                'short_return': short_return,
                'long_short_return': long_short_return
            }
        }
