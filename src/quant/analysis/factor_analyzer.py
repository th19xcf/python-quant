#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
因子分析器
"""

import polars as pl
from datetime import datetime

from src.utils.logger import logger
from src.utils.config import get_config
from src.database.db_manager import DatabaseManager
from src.data.data_manager import DataManager


class FactorAnalyzer:
    """
    因子分析器
    """
    
    def __init__(self):
        """
        初始化因子分析器，按需创建数据管理器（延迟初始化避免纯计算场景报错）
        """
        self._data_manager = None

    @property
    def data_manager(self) -> DataManager:
        if self._data_manager is None:
            config = get_config()
            db_manager = DatabaseManager(config)
            self._data_manager = DataManager(config, db_manager)
        return self._data_manager
    
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
            # 从数据库/数据源获取真实行情，使用后复权消除分红送转干扰
            data = self.data_manager.get_stock_data(
                stock_code, start_date, end_date, adjustment_type='hfq'
            )

            if data.is_empty():
                raise ValueError(f'股票 {stock_code} 在 {start_date}~{end_date} 无可用行情数据')

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
            # 计算RSI：用Polars表达式替代已移除的Series.apply
            delta = pl.col('close').diff()
            gain = pl.when(delta > 0).then(delta).otherwise(0.0)
            loss = pl.when(delta < 0).then(-delta).otherwise(0.0)
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
        
        # 基本面因子：暂未接入真实数据源，跳过计算并在日志中提示
        fundamental_factors = ['PE', 'PB', 'ROE', 'EPS']
        missing = [f for f in fundamental_factors if f in factors]
        if missing:
            logger.warning(
                f"基本面因子 {missing} 暂未接入真实数据源，已跳过。"
                f"后续可对接 Tushare/BaoStock 基本面接口。"
            )
        
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
            quantile_returns = data.group_by(f'{factor_col}_quantile').agg([
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
            
            quantile_returns = data.group_by('factor_quantile').agg([
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
